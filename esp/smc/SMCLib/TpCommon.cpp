/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#pragma warning (disable : 4786)
// TpWrapper.cpp: implementation of the CTpWrapper class.
//
//////////////////////////////////////////////////////////////////////

#include "TpWrapper.hpp"
#include <stdio.h>
#include "workunit.hpp"
#include "exception_util.hpp"
#include "portlist.h"
#include "daqueue.hpp"
#include "dautils.hpp"
#include "dameta.hpp"

void CTpWrapper::appendTpMachine(double clientVersion, IConstEnvironment* constEnv, IConstInstanceInfo& instanceInfo, IArrayOf<IConstTpMachine>& machines)
{
    SCMStringBuffer name, networkAddress, description, directory;
    Owned<IEspTpMachine> machine = createTpMachine();
    Owned<IConstMachineInfo> machineInfo = instanceInfo.getMachine();
    machine->setName(machineInfo->getName(name).str());
    machine->setOS(machineInfo->getOS());
    machine->setNetaddress(machineInfo->getNetAddress(networkAddress).str());
    machine->setDirectory(instanceInfo.getDirectory(directory).str());
    machine->setPort(instanceInfo.getPort());
    machine->setType(eqSparkThorProcess); //for now, the appendTpMachine is only used for SparkThor.
    machines.append(*machine.getLink());
}

extern TPWRAPPER_API ISashaCommand* archiveOrRestoreWorkunits(StringArray& wuids, IProperties* params, bool archive, bool dfu)
{
    StringBuffer sashaAddress;
    if (params && params->hasProp("sashaServerIP"))
    {
        sashaAddress.set(params->queryProp("sashaServerIP"));
        sashaAddress.append(':').append(params->getPropInt("sashaServerPort", DEFAULT_SASHA_PORT));
    }
    else
        getSashaService(sashaAddress, dfu ? dfuwuArchiverType : wuArchiverType, true);

    SocketEndpoint ep(sashaAddress);
    Owned<INode> node = createINode(ep);
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(archive ? SCA_ARCHIVE : SCA_RESTORE);
    if (dfu)
        cmd->setDFU(true);

    ForEachItemIn(i, wuids)
        cmd->addId(wuids.item(i));

    if (!cmd->send(node, 1*60*1000))
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,
            "Sasha (%s) took too long to respond for Archive/restore workunit.",
            sashaAddress.str());
    return cmd.getClear();
}

extern TPWRAPPER_API bool matchNetAddressRequest(const char* netAddressReg, bool ipReq, IConstTpMachine& tpMachine)
{
    if (ipReq)
        return streq(netAddressReg, tpMachine.getNetaddress());
    return streq(netAddressReg, tpMachine.getConfigNetaddress());
}

extern TPWRAPPER_API void throwOrLogDropZoneLookUpError(int code, char const* format, ...) __attribute__((format(printf, 2, 3)));
TPWRAPPER_API void throwOrLogDropZoneLookUpError(int code, char const* format, ...)
{
    va_list args;
    va_start(args, format);
#ifdef _CONTAINERIZED
    throw makeStringExceptionVA(code, format, args);
#else
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (env->isDropZoneRestrictionEnabled())
        throw makeStringExceptionVA(code, format, args);

    VALOG(MCdebugInfo, format, args);
#endif
    va_end(args);
}

StringBuffer &findDropZonePlaneName(const char *host, const char *path, StringBuffer &planeName)
{
    //Call findDropZonePlane() to resolve plane by hostname. Shouldn't resolve plane
    //by hostname in containerized but kept for backward compatibility for now.
    Owned<IPropertyTree> plane = findDropZonePlane(path, host, true, false);
    if (plane)
        planeName.append(plane->queryProp("@name"));
    else
        throwOrLogDropZoneLookUpError(ECLWATCH_INVALID_INPUT, "DropZone not found for host '%s' path '%s'.", host, path);
    return planeName;
}

//If the dropZoneName is empty, find out the dropzone based on the host and path. In bare-metal and
//isDropZoneRestrictionEnabled()==false, the dropzone may not be found.
//If the dropZoneName is not empty, find out the dropzone based on the dropZoneName and validate the host and path.
extern TPWRAPPER_API IPropertyTree* getDropZoneAndValidateHostAndPath(const char* dropZoneName, const char* host, const char* path)
{
    StringBuffer pathToCheck(path);
    addPathSepChar(pathToCheck);

    const char* hostToCheck = nullptr;
    if (isContainerized() && strsame(host, "localhost"))
        hostToCheck = nullptr; // "localhost" is a placeholder for mounted dropzones that have no hosts.
    else
        hostToCheck = host;

    Owned<IPropertyTree> dropZone;
    if (isEmptyString(dropZoneName))
    {
        dropZone.setown(findDropZonePlane(pathToCheck, hostToCheck, isIPAddress(hostToCheck), false));
        if (!dropZone)
            throwOrLogDropZoneLookUpError(ECLWATCH_INVALID_INPUT, "DropZone not found for host '%s' path '%s'.",
                isEmptyString(host) ? "unspecified" : host, isEmptyString(path) ? "unspecified" : path);
    }
    else
    {
        dropZone.setown(getDropZonePlane(dropZoneName));
        if (!dropZone)
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "DropZone '%s' not found.", dropZoneName);
        if (!isEmptyString(hostToCheck) && !isHostInPlane(dropZone, hostToCheck, isIPAddress(hostToCheck)))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "The host '%s' is not valid for dropzone %s.", host, dropZoneName);
        if (!isEmptyString(pathToCheck) && !isPathInPlane(dropZone, pathToCheck))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "The path '%s' is not valid for dropzone %s.", path, dropZoneName);
    }
    return dropZone.getClear();
}

static SecAccessFlags getDropZoneScopePermissions(IEspContext& context, const IPropertyTree* dropZone, const char* dropZonePath)
{
    //If the dropZonePath is an absolute path, change it to a relative path.
    if (isAbsolutePath(dropZonePath))
    {
        const char* relativePath = getRelativePath(dropZonePath, dropZone->queryProp("@prefix"));
        if (nullptr == relativePath)
            throw makeStringExceptionV(-1, "Invalid DropZone path %s.", dropZonePath);
        dropZonePath = relativePath;
    }

    Owned<IUserDescriptor> userDesc = createUserDescriptor();
    userDesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());
    return queryDistributedFileDirectory().getDropZoneScopePermissions(dropZone->queryProp("@name"), dropZonePath, userDesc);
}

static SecAccessFlags getDZPathScopePermissions(IEspContext& context, const char* dropZoneName, const char* dropZonePath, const char* dropZoneHost)
{
    Owned<IPropertyTree> dropZone;
    if (isEmptyString(dropZoneName))
    {
        dropZone.setown(findDropZonePlane(dropZonePath, dropZoneHost, true, false));
        if (!dropZone)
        {
            throwOrLogDropZoneLookUpError(ECLWATCH_INVALID_INPUT, "getDZPathScopePermissions(): DropZone not found for host '%s' path '%s'.",
                isEmptyString(dropZoneHost) ? "unspecified" : dropZoneHost, isEmptyString(dropZonePath) ? "unspecified" : dropZonePath);
            return SecAccess_Full;
        }
    }
    else
    {
        dropZone.setown(getDropZonePlane(dropZoneName));
        if (!dropZone)
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "getDZPathScopePermissions(): DropZone %s not found.", dropZoneName);
    }

    return getDropZoneScopePermissions(context, dropZone, dropZonePath);
}

static SecAccessFlags getDZFileScopePermissions(IEspContext& context, const char* dropZoneName, const char* dropZonePath,
    const char* dropZoneHost)
{
    StringBuffer dir, fileName;
    splitFilename(dropZonePath, &dir, &dir, nullptr, nullptr);
    dropZonePath = dir.str();
    return getDZPathScopePermissions(context, dropZoneName, dropZonePath, dropZoneHost);
}

static SecAccessFlags getLegacyDZPhysicalPerms(IEspContext& context, const char* dropZoneName, const char* dropZoneHost,
    const char* dropZoneFile, bool isPath)
{
    if (isEmptyString(dropZoneHost) && isEmptyString(dropZoneName))
        throw makeStringException(ECLWATCH_INVALID_INPUT, "Neither dropzone plane or host specified.");

    Owned<IUserDescriptor> userDesc = createUserDescriptor();
    userDesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());

    StringBuffer host;
    if (isEmptyString(dropZoneHost))
    {
        Owned<IPropertyTree> plane = getDropZonePlane(dropZoneName);
        if (getPlaneHost(host, plane, 0))
            dropZoneHost = host.str();
        else
            dropZoneHost = "localhost";
    }

    CDfsLogicalFileName dlfn;
    SocketEndpoint ep(dropZoneHost);
    dlfn.setExternal(ep, dropZoneFile);

    StringBuffer scopes;
    const char *scopesToCheck = nullptr;
    if (isPath)
        scopesToCheck = dlfn.get();
    else
    {
        dlfn.getScopes(scopes);
        scopesToCheck = scopes;
    }
    return queryDistributedFileDirectory().getFScopePermissions(scopesToCheck, userDesc);
}

//Get dropzone file permission for a dropzone file path (the path does NOT contain a file name).
//This function (and the validate functions below) calls the getDZPathScopePermissions(). In the
//getDZPathScopePermissions(), the isDropZoneRestrictionEnabled() will be used for bare-metal if
//a dropzone cannot be found based on host and path.
extern TPWRAPPER_API SecAccessFlags getDZPathScopePermsAndLegacyPhysicalPerms(IEspContext &context, const char *dropZoneName, const char *path,
    const char *host, SecAccessFlags permissionReq)
{
    SecAccessFlags permission = getDZPathScopePermissions(context, dropZoneName, path, host);
    if ((permission < permissionReq) && getGlobalConfigSP()->getPropBool("expert/@failOverToLegacyPhysicalPerms", !isContainerized()))
        permission = getLegacyDZPhysicalPerms(context, dropZoneName, host, path, true);
    return permission;
}

//Validate dropzone file permission for a dropzone file path (the path does NOT contain a file name)
extern TPWRAPPER_API void validateDZPathScopePermsAndLegacyPhysicalPerms(IEspContext &context, const char *dropZoneName, const char *path,
    const char *host, SecAccessFlags permissionReq)
{
    SecAccessFlags permission = getDZPathScopePermsAndLegacyPhysicalPerms(context, dropZoneName, path, host, permissionReq);
    if (permission < permissionReq)
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone %s %s not allowed for user %s (permission:%s). Read Access Required.",
            dropZoneName, path, context.queryUserId(), getSecAccessFlagName(permission));
}

//Validate dropzone file permission for a dropzone file path (the path DOES contain a file name)
extern TPWRAPPER_API void validateDZFileScopePermsAndLegacyPhysicalPerms(IEspContext& context, const char* dropZoneName, const char* path,
    const char* host, SecAccessFlags permissionReq)
{
    SecAccessFlags permission = getDZFileScopePermissions(context, dropZoneName, path, host);
    if ((permission < permissionReq) && getGlobalConfigSP()->getPropBool("expert/@failOverToLegacyPhysicalPerms", !isContainerized()))
        permission = getLegacyDZPhysicalPerms(context, dropZoneName, host, path, false);
    if (permission < permissionReq)
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone %s %s not allowed for user %s (permission:%s). %s permission Required.",
            dropZoneName, path, context.queryUserId(), getSecAccessFlagName(permission), getSecAccessFlagName(permissionReq));
}

//This function is called when uploading a file to a dropzone or downloading a file to a dropzone.
//1. both host and filePath should not be empty.
//2. the filePath should not contain bad file path.
//3. call the getDropZoneAndValidateHostAndPath().
//4. if the dropzone name is available, validate dropzone file permission. Should be available unless bare-metal and isDropZoneRestrictionEnabled()==false.
extern TPWRAPPER_API void validateDropZoneReq(IEspContext& ctx, const char* dropZoneName, const char* host, const char* filePath, SecAccessFlags permissionReq)
{
    if (isEmptyString(host) || isEmptyString(filePath)) //The host and filePath must be specified for accessing a DropZone file.
        throw makeStringException(ECLWATCH_INVALID_INPUT, "Host or file path not defined.");

    if (containsRelPaths(filePath)) //Detect a path like: /home/lexis/runtime/var/lib/HPCCSystems/mydropzone/../../../
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid path %s", filePath);

    Owned<IPropertyTree> dropZone = getDropZoneAndValidateHostAndPath(dropZoneName, host, filePath);
    if (dropZone)
        dropZoneName = dropZone->queryProp("@name");

    if (!isEmptyString(dropZoneName))
        validateDZPathScopePermsAndLegacyPhysicalPerms(ctx, dropZoneName, filePath, host, permissionReq);
}

//This function is called when accessing a dropzone file from the WsFileIO service (the fileNameWithRelPath is a relative path).
//1. the targetDZNameOrHost could be a dropzone name or dropzone host. Find out the dropzone.
//   If the targetDZNameOrHost is a dropzone name and the hostReq is not empty, validate the hostReq inside the dropzone.
//   If the dropzone cannot be found based on the targetDZNameOrHost, throw an exception.
//2. the fileNameWithRelPath should not contain bad file path.
//3. validate dropzone file permission.
//4. set the dlfn using dropzone name and fileNameWithRelPath.
extern TPWRAPPER_API void validateDropZoneAccess(IEspContext& context, const char* targetDZNameOrHost, const char* hostReq, SecAccessFlags permissionReq,
    const char* fileNameWithRelPath, CDfsLogicalFileName& dlfn)
{
    if (containsRelPaths(fileNameWithRelPath)) //Detect a path like: a/../../../f
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid file path %s", fileNameWithRelPath);

    Owned<IPropertyTree> dropZone = getDropZonePlane(targetDZNameOrHost);
    if (!dropZone) //The targetDZNameOrHost could be a dropzone host.
        dropZone.setown(findDropZonePlane(nullptr, targetDZNameOrHost, true, true));
    else if (!isEmptyString(hostReq))
    {
        if (!isHostInPlane(dropZone, hostReq, true))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Host %s is not valid DropZone plane %s", hostReq, targetDZNameOrHost);
    }

    //If the dropZonePath is an absolute path, change it to a relative path.
    if (isAbsolutePath(fileNameWithRelPath))
    {
        const char* relativePath = getRelativePath(fileNameWithRelPath, dropZone->queryProp("@prefix"));
        if (nullptr == relativePath)
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid DropZone path %s.", fileNameWithRelPath);
        fileNameWithRelPath = relativePath;
    }

    const char *dropZoneName = dropZone->queryProp("@name");
    SecAccessFlags permission = getDZFileScopePermissions(context, dropZoneName, fileNameWithRelPath, hostReq);
    if ((permission < permissionReq) && getGlobalConfigSP()->getPropBool("expert/@failOverToLegacyPhysicalPerms", !isContainerized()))
    {
        StringBuffer fullPath(dropZone->queryProp("@prefix"));
        addNonEmptyPathSepChar(fullPath).append(fileNameWithRelPath);
        permission = getLegacyDZPhysicalPerms(context, dropZoneName, hostReq, fullPath, false);
    }
    if (permission < permissionReq)
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone %s %s not allowed for user %s (permission:%s). %s Access Required.",
            dropZoneName, fileNameWithRelPath, context.queryUserId(), getSecAccessFlagName(permission), getSecAccessFlagName(permissionReq));
    dlfn.setPlaneExternal(dropZoneName, fileNameWithRelPath);
}
