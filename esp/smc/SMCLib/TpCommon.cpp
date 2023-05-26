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

extern TPWRAPPER_API IStringIterator* getContainerTargetClusters(const char* processType, const char* processName)
{
    Owned<CStringArrayIterator> ret = new CStringArrayIterator;
    Owned<IPropertyTreeIterator> queues = getComponentConfigSP()->getElements("queues");
    ForEach(*queues)
    {
        IPropertyTree& queue = queues->query();
        if (!isEmptyString(processType))
        {
            const char* type = queue.queryProp("@type");
            if (isEmptyString(type) || !strieq(type, processType))
                continue;
        }
        const char* qName = queue.queryProp("@name");
        if (isEmptyString(qName))
            continue;

        if (!isEmptyString(processName) && !strieq(qName, processName))
            continue;

        ret->append_unique(qName);
    }
    if (!isEmptyString(processType) && !strieq("roxie", processType))
        return ret.getClear();

    Owned<IPropertyTreeIterator> services = getGlobalConfigSP()->getElements("services[@type='roxie']");
    ForEach(*services)
    {
        IPropertyTree& service = services->query();
        const char* targetName = service.queryProp("@target");
        if (isEmptyString(targetName))
            continue;

        if (!isEmptyString(processName) && !strieq(targetName, processName))
            continue;

        ret->append_unique(targetName);
    }
    return ret.getClear();
}

extern TPWRAPPER_API bool matchNetAddressRequest(const char* netAddressReg, bool ipReq, IConstTpMachine& tpMachine)
{
    if (ipReq)
        return streq(netAddressReg, tpMachine.getNetaddress());
    return streq(netAddressReg, tpMachine.getConfigNetaddress());
}

extern TPWRAPPER_API StringBuffer &findDropZonePlaneName(const char* host, const char* path, StringBuffer& planeName)
{
    //Call findDropZonePlane() to resolve plane by hostname. Shouldn't resolve plane
    //by hostname in containerized but kept for backward compatibility for now.
    Owned<IPropertyTree> plane = findDropZonePlane(path, host, true, false);
    if (plane)
        planeName.append(plane->queryProp("@name"));
    else
    {
        Owned<IException> e = makeStringExceptionV(ECLWATCH_INVALID_INPUT, "DropZone not found for host '%s' path '%s'.", host, path);
#ifndef _CONTAINERIZED
        // In bare-metal, if environment.conf is configured with useDropZoneRestriction=false, issue warning only
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> env = factory->openEnvironment();
        if (!env->isDropZoneRestrictionEnabled())
        {
            WARNLOG(e);
            return planeName; // NB: not filled
        }
#endif
        throw e.getClear();
    }
    return planeName;
}

static SecAccessFlags getDropZoneScopePermissions(IEspContext& context, const IPropertyTree* dropZone, const char* dropZonePath)
{
    if (isEmptyString(dropZonePath))
        throw makeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "getDropZoneScopePermissions(): DropZone path must be specified.");

    //If the dropZonePath is an absolute path, change it to a relative path.
    StringBuffer s;
    const char* prefix = dropZone->queryProp("@prefix");
    const char* name = dropZone->queryProp("@name");
    if (hasPrefix(dropZonePath, prefix, true))
    {
        const char* p = dropZonePath + strlen(prefix);
        if (!*p || !isPathSepChar(p[0]))
            addPathSepChar(s);
        s.append(p);
        dropZonePath = s.str();
    }

    Owned<IUserDescriptor> userDesc = createUserDescriptor();
    userDesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());
    return queryDistributedFileDirectory().getDropZoneScopePermissions(name, dropZonePath, userDesc);
}

extern TPWRAPPER_API SecAccessFlags getDZPathScopePermissions(IEspContext& context, const char* dropZoneName, const char* dropZonePath)
{
    if (isEmptyString(dropZonePath))
        throw makeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "getDZPathScopePermissions(): DropZone path must be specified.");

    Owned<IPropertyTree> dropZone = getDropZonePlane(dropZoneName);
    if (!dropZone)
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "getDZPathScopePermissions(): DropZone %s not found.", dropZoneName);

    return getDropZoneScopePermissions(context, dropZone, dropZonePath);
}

extern TPWRAPPER_API void checkDZPathScopePermissions(IEspContext& context, const char* dropZoneName, const char* dropZonePath, SecAccessFlags accessReq)
{
    SecAccessFlags access = getDZPathScopePermissions(context, dropZoneName, dropZonePath);
    if (access < accessReq)
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone Scope %s %s not allowed for user %s (permission:%s). %s Access Required.",
            dropZoneName, dropZonePath, context.queryUserId(), getSecAccessFlagName(access), getSecAccessFlagName(accessReq));
}

extern TPWRAPPER_API SecAccessFlags getDZFileScopePermissions(IEspContext& context, const char* dropZoneName, const char* dropZoneFilePath)
{
    StringBuffer dir, fileName;
    splitFilename(dropZoneFilePath, &dir, &dir, nullptr, nullptr);
    dropZoneFilePath = dir.str();
    return getDZPathScopePermissions(context, dropZoneName, dropZoneFilePath);
}

extern TPWRAPPER_API void checkDZFileScopePermissions(IEspContext& context, const char* dropZoneName, const char* dropZoneFilePath, SecAccessFlags accessReq)
{
    SecAccessFlags access = getDZFileScopePermissions(context, dropZoneName, dropZoneFilePath);
    if (access < accessReq)
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone Scope %s for file %s not allowed for user %s (permission:%s). %s Access Required.",
            dropZoneName, dropZoneFilePath, context.queryUserId(), getSecAccessFlagName(access), getSecAccessFlagName(accessReq));
}

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
    const char *dropZoneName = dropZone->queryProp("@name");
    SecAccessFlags permission = getDZFileScopePermissions(context, dropZoneName, fileNameWithRelPath);
    if (permission < permissionReq)
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone Scope %s %s not allowed for user %s (permission:%s). %s Access Required.",
            dropZoneName, fileNameWithRelPath, context.queryUserId(), getSecAccessFlagName(permission), getSecAccessFlagName(permissionReq));
    dlfn.setPlaneExternal(dropZoneName, fileNameWithRelPath);
}

extern TPWRAPPER_API void checkDropZoneInput(IEspContext& context, StringBuffer& dropZoneName, const char* host, const char* path)
{
    if (dropZoneName.isEmpty())
        findDropZonePlaneName(host, path, dropZoneName);
    if (!dropZoneName.isEmpty()) // must be true, unless bare-metal and isDropZoneRestrictionEnabled()==false
    {
        Owned<IPropertyTree> dropZone = getDropZonePlane(dropZoneName);
        if (!dropZone)
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Unknown landing zone: %s", dropZoneName.str());
        if (isContainerized() && strsame("localhost", host)) //ECLWatch may send "localhost" for dropzones without hosts.
            host = nullptr;
        else if (!isEmptyString(host) && !isHostInPlane(dropZone, host, false))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Host '%s' is not valid for dropzone '%s'", host, dropZoneName.str());
        if (!isPathInPlane(dropZone, path))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Path '%s' is not valid for dropzone '%s'", path, dropZoneName.str());
    }
}

extern TPWRAPPER_API void checkDropZoneInputAndAccess(IEspContext& context, StringBuffer& dropZoneName, const char* host, const char* path, SecAccessFlags accessReq)
{
    checkDropZoneInput(context, dropZoneName, host, path);
    if (!dropZoneName.isEmpty()) // must be true, unless bare-metal and isDropZoneRestrictionEnabled()==false
        checkDZPathScopePermissions(context, dropZoneName, path, accessReq);
}

