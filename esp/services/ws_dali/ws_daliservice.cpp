/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems.

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

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

#include "ws_daliservice.hpp"
#include "jlib.hpp"
#include "dautils.hpp"
#include "daadmin.hpp"

using namespace daadmin;

#define REQPATH_EXPORTSDSDATA "/WSDali/Export"

const char* daliFolder = "tempdalifiles" PATHSEPSTR;
const unsigned daliFolderLength = strlen(daliFolder);

void CWSDaliEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
    espProcess.set(process);
}

int CWSDaliSoapBindingEx::onGet(CHttpRequest* request, CHttpResponse* response)
{
    try
    {
#ifdef _USE_OPENLDAP
        request->queryContext()->ensureSuperUser(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Access denied, administrators only.");
#endif
        if (wsdService->isDaliDetached())
            throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Dali detached.");

        StringBuffer path;
        request->getPath(path);

        if (!strnicmp(path.str(), REQPATH_EXPORTSDSDATA, sizeof(REQPATH_EXPORTSDSDATA) - 1))
        {
            exportSDSData(request, response);
            return 0;
        }
    }
    catch(IException* e)
    {
        onGetException(*request->queryContext(), request, response, *e);
        FORWARDEXCEPTION(*request->queryContext(), e,  ECLWATCH_INTERNAL_ERROR);
    }

    return CWSDaliSoapBinding::onGet(request,response);
}

void CWSDaliSoapBindingEx::exportSDSData(CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer path, xpath, safeReq;
    request->getParameter("Path", path);
    request->getParameter("Safe", safeReq);
    Owned<IRemoteConnection> conn = connectXPathOrFile(path, strToBool(safeReq), xpath);
    if (!conn)
        throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Failed to connect Dali.");

    Owned<IPropertyTree> root = conn->getRoot();

    Owned<IFile> workingDir = createIFile(daliFolder);
    if (!workingDir->exists())
        workingDir->createDirectory();

    StringBuffer peer, outFileNameWithPath;
    VStringBuffer prefix("sds_for_%s", request->getPeer(peer).str());
    Owned<IFileIO> io = createUniqueFile(daliFolder, prefix, nullptr, outFileNameWithPath, IFOcreaterw);
    { //Force the fios to finish
        Owned<IFileIOStream> fios = createBufferedIOStream(io);
        toXML(root, *fios);
    }

    VStringBuffer headerStr("attachment;filename=%s", outFileNameWithPath.str() + daliFolderLength);
    IEspContext* context = request->queryContext();
    context->addCustomerHeader("Content-disposition", headerStr.str());

    response->setContent(createIOStream(io));
    response->setContentType(HTTP_TYPE_OCTET_STREAM);
    response->send();

    io.clear();
    removeFileTraceIfFail(outFileNameWithPath);
}

void CWSDaliEx::checkAccess(IEspContext& context)
{
#ifdef _USE_OPENLDAP
    context.ensureSuperUser(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Access denied, administrators only.");
#endif
    if (isDaliDetached())
        throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Dali detached.");
}

bool CWSDaliEx::onSetValue(IEspContext& context, IEspSetValueRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");
        const char* value = req.getValue();
        if (isEmptyString(value))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data value not specified.");

        StringBuffer oldValue, result;
        setValue(path, value, oldValue);
        if (oldValue.isEmpty())
            result.appendf("Changed %s to '%s'", path, value);
        else
            result.appendf("Changed %s from '%s' to '%s'", path, oldValue.str(), value);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetValue(IEspContext& context, IEspGetValueRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        StringBuffer value;
        getValue(path, value);
        resp.setResult(value);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onImport(IEspContext& context, IEspImportRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* xml = req.getXML();
        const char* path = req.getPath();
        if (isEmptyString(xml))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data XML not specified.");
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        StringBuffer result;
        if (importFromXML(path, xml, req.getAdd(), result))
            result.appendf(" Branch %s loaded.", path);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDelete(IEspContext& context, IEspDeleteRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        StringBuffer result;
        erase(path, false, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onAdd(IEspContext& context, IEspAddRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");
        const char* value = req.getValue();

        StringBuffer result;
        add(path, isEmptyString(value) ? nullptr : value, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onCount(IEspContext& context, IEspCountRequest& req, IEspCountResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        resp.setResult(count(path));
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

IUserDescriptor* CWSDaliEx::createUserDesc(IEspContext& context)
{
    StringBuffer username;
    context.getUserID(username);
    if (username.isEmpty())
        return nullptr;

    Owned<IUserDescriptor> userdesc = createUserDescriptor();
    userdesc->set(username.str(), context.queryPassword(), context.querySignature());
    return userdesc.getClear();
}

bool CWSDaliEx::onDFSLS(IEspContext& context, IEspDFSLSRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        StringBuffer options;
        if (req.getRecursively())
            options.append("r");
        if (!req.getPathAndNameOnly())
            options.append("l");
        if (req.getIncludeSubFileInfo())
            options.append("s");

        StringBuffer result;
        dfsLs(req.getName(), options, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetDFSCSV(IEspContext& context, IEspGetDFSCSVRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfscsv(req.getLogicalNameMask(), userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSExists(IEspContext& context, IEspDFSExistsRequest& req, IEspBooleanResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);
        resp.setResult(dfsexists(fileName, userDesc) == 0 ? true : false);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetLogicalFile(IEspContext& context, IEspGetLogicalFileRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfsfile(fileName, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetLogicalFilePart(IEspContext& context, IEspGetLogicalFilePartRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");
        if (req.getPartNumber_isNull())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Part number not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfspart(fileName, userDesc, req.getPartNumber(), result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSetLogicalFilePartAttr(IEspContext& context, IEspSetLogicalFilePartAttrRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");
        const char* attr = req.getAttr();
        if (isEmptyString(attr))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Part attribute name not specified.");
        if (req.getPartNumber_isNull())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Part number not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        setdfspartattr(fileName, req.getPartNumber(), attr, req.getValue(), userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetDFSMap(IEspContext& context, IEspGetDFSMapRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfsmap(fileName, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetDFSParents(IEspContext& context, IEspGetDFSParentsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfsparents(fileName, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSCheck(IEspContext& context, IEspDFSCheckRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfsCheck(result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSetProtected(IEspContext& context, IEspSetProtectedRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);
                
        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        const char* callerId = req.getCallerId();
        if (isEmptyString(callerId))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Caller Id not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        setprotect(fileName, callerId, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSetUnprotected(IEspContext& context, IEspSetUnprotectedRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        const char* callerId = req.getCallerId();
        if (isEmptyString(callerId))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Caller Id not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        unprotect(fileName, callerId, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetProtectedList(IEspContext& context, IEspGetProtectedListRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        const char* callerId = req.getCallerId();
        if (isEmptyString(callerId))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Caller Id not specified.");

        StringBuffer result;
        listprotect(fileName, callerId, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSReplication(IEspContext& context, IEspDFSReplicationRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* clusterMask = req.getClusterMask();
        if (isEmptyString(clusterMask))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Cluster Mask not specified.");

        const char* lfnMask = req.getLogicalNameMask();
        if (isEmptyString(lfnMask))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Logical File Mask not specified.");

        if (req.getRedundancyCount_isNull())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Redundancy Count not specified");
        unsigned redundancy = req.getRedundancyCount();
        
        if (req.getDryRun_isNull())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Dryrun not set");
        bool dryrun = req.getDryRun();

        StringBuffer result;
        dfsreplication(clusterMask, lfnMask, redundancy, dryrun, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onNormalizeFileNames(IEspContext& context, IEspNormalizeFileNamesRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Name not specified.");

        StringBuffer result;
        normalizeFileNames(userDesc, name, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onCleanScopes(IEspContext& context, IEspCleanScopesRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        cleanscopes(userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSScopes(IEspContext& context, IEspDFSScopesRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Name not specified.");


        StringBuffer result;
        dfsscopes(name, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSMeta(IEspContext& context, IEspDFSMetaRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        const char* name = req.getFileName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "FileName not specified.");

        bool includeStorage = req.getIncludeStorage();
        if (req.getIncludeStorage_isNull())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Include Storage not specified.");

        StringBuffer result;
        dfsmeta(name,userDesc,includeStorage,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSGroup(IEspContext& context, IEspDFSGroupRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);


        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Name not specified.");


        const char* outputFilename = req.getOutputFileName();
        if (isEmptyString(outputFilename))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Output File Name not specified.");

        StringBuffer result;
        dfsGroup(name,outputFilename,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onClusterGroup(IEspContext& context, IEspClusterGroupRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);


        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Name not specified.");


        const char* outputFilename = req.getOutputFileName();
        if (isEmptyString(outputFilename))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Output File Name not specified.");

        StringBuffer result;
        clusterGroup(name,outputFilename,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSUnlink(IEspContext& context, IEspDFSUnlinkRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        const char* name = req.getLinkName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Link Name not specified.");

        StringBuffer result;
        dfsunlink(name,userDesc,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSVerify(IEspContext& context, IEspDFSVerifyRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Link Name not specified.");

        StringBuffer result;
        dfsverify(name,NULL,userDesc,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onCheckSuperFile(IEspContext& context, IEspCheckSuperFileRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Name not specified.");

        bool fix = req.getFix();
        if (req.getFix_isNull())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Fix not specified.");

        StringBuffer result;
        checksuperfile(name,result,fix);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onCheckSubFile(IEspContext& context, IEspCheckSubFileRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Name not specified.");

        StringBuffer result;
        checksubfile(name,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onListExpires(IEspContext& context, IEspListExpiresRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        const char* name = req.getLinkedFileNameMask();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Linked File Name Mask not specified.");

        StringBuffer result;
        listexpires(name,userDesc,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onListRelationships(IEspContext& context, IEspListRelationshipsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* primary = req.getPrimary();
        if (isEmptyString(primary))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Primary not specified.");

        const char* secondary = req.getSecondary();
        if (isEmptyString(secondary))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Secondary not specified.");


        StringBuffer result;
        listrelationships(primary,secondary,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSPerm(IEspContext& context, IEspDFSPermRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Name not specified.");

        StringBuffer result;
        dfsperm(name,userDesc,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSCompRatio(IEspContext& context, IEspDFSCompRatioRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        const char* name = req.getName();
        if (isEmptyString(name))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Name not specified.");

        StringBuffer result;
        dfscompratio(name,userDesc,result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
