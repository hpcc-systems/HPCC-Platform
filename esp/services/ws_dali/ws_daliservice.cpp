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
