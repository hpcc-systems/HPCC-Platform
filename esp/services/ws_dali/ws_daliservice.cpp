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
#include "dasds.hpp"
#include "dadfs.hpp"

#define REQPATH_EXPORTSDSDATA "/WSDali/Export"

const char* daliFolder = "tempdalifiles" PATHSEPSTR;
const unsigned daliFolderLength = strlen(daliFolder);
static unsigned daliConnectTimeoutMs = 5000;

void CWSDaliEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
    espProcess.set(process);
}

bool CWSDaliEx::onImport(IEspContext& context, IEspImportRequest& req, IEspImportResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        context.ensureSuperUser(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Access denied, administrators only.");
#endif
        if (isDaliDetached())
            throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Dali detached.");

        const char* xml = req.getXML();
        const char* path = req.getPath();
        if (isEmptyString(xml))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data XML not specified.");
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        importSDSData(xml, nullptr, path, req.getAdd());
        resp.setMessage("Dali updated.");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDelete(IEspContext& context, IEspDeleteRequest& req, IEspDeleteResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        context.ensureSuperUser(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Access denied, administrators only.");
#endif
        if (isDaliDetached())
            throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Dali detached.");

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        StringBuffer head, tmp;
        const char* tail = splitpath(path, head, tmp);
        path = remLeading(path);
        Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), RTM_LOCK_WRITE, daliConnectTimeoutMs);
        if (!conn)
            throw makeStringExceptionV(ECLWATCH_INTERNAL_ERROR, "Could not connect to %s", head.str());
        if (!req.getNoBackup())
            backupSDSData(conn->queryRoot(), path, tail);
        conn->close(true);
        resp.setMessage("Dali updated.");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void CWSDaliEx::importSDSData(const char* dataXML, const char* dataFile, const char* path, bool add)
{
    StringBuffer head, tmp;
    const char* tail = splitpath(path, head, tmp);
    path = remLeading(path);
    if (!add)
    {
        Owned<IRemoteConnection> bconn = querySDS().connect(path, myProcessSession(), RTM_LOCK_READ|RTM_SUB, daliConnectTimeoutMs);
        if (bconn)
            backupSDSData(bconn->queryRoot(), path, tail);
    }

    Owned<IRemoteConnection> conn = querySDS().connect(head, myProcessSession(), RTM_LOCK_WRITE, daliConnectTimeoutMs);
    if (!conn)
        throw makeStringExceptionV(ECLWATCH_CANNOT_CONNECT_DALI, "Could not connect to %s", path);

    StringAttr newtail; // must be declared outside the following if
    IPropertyTree* root = conn->queryRoot();
    if (!add)
    {
        root->removeProp(tail);

        //If replacing a qualified branch then remove the qualifiers before calling addProp
        const char* qualifier = strchr(tail, '[');
        if (qualifier)
        {
            newtail.set(tail, qualifier-tail);
            tail = newtail;
        }
    }

    Owned<IPropertyTree> branch;
    if (!isEmptyString(dataXML))
        branch.setown(createPTreeFromXMLString(dataXML));
    else
    {
        branch.setown(createPTreeFromXMLFile(dataFile));
        removeFileTraceIfFail(dataFile);
    }

#ifndef _CONTAINERIZED
    Owned<IPropertyTree> oldEnvironment;
    if (streq(path, "Environment"))
        oldEnvironment.setown(createPTreeFromIPT(root));
#endif
    root->addPropTree(tail, LINK(branch));
    conn->close();
    PROGLOG("Branch %s imported", path);

#ifndef _CONTAINERIZED
    if (streq(path, "Environment"))
     {
        PROGLOG("Refreshing cluster groups from Environment");
        StringBuffer response;
        initClusterGroups(false, response, oldEnvironment);
        if (!response.isEmpty())
            PROGLOG("updating Environment via import path=%s : %s", path, response.str());
    }
#endif
}

void CWSDaliEx::backupSDSData(IPropertyTree* srcPTree, const char* srcPath, const char* prefix)
{
    StringBuffer bakName;
    Owned<IFileIO> io = createUniqueFile(daliFolder, prefix, "bak", bakName);
    PROGLOG("Saving backup of %s to %s", srcPath, bakName.str());
    Owned<IFileIOStream> fstream = createBufferedIOStream(io);
    toXML(srcPTree, *fstream);         // formatted (default)
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

int CWSDaliSoapBindingEx::onStartUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringArray fileNames, files;
    VStringBuffer source("WsDali::%s()", method);
    Owned<IMultiException> me = MakeMultiException(source);
    try
    {
#ifdef _USE_OPENLDAP
        request->queryContext()->ensureSuperUser(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Access denied, administrators only.");
#endif
        if (wsdService->isDaliDetached())
            throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Dali detached.");

        if (strieq(method, "Import"))
        {
            StringBuffer path, add;
            request->getParameter("Path", path);
            request->getParameter("Add", add);
            if (isEmptyString(path))
                throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

            request->readContentToFiles(nullptr, daliFolder, fileNames);
            unsigned count = fileNames.ordinality();
            if (count == 0)
                throw makeStringException(ECLWATCH_INVALID_INPUT, "Failed to read upload content.");
            if (count > 1)
                throw makeStringException(ECLWATCH_INVALID_INPUT, "Only one file is allowed.");

            VStringBuffer fileName("%s%s", daliFolder, fileNames.item(0));
            wsdService->importSDSData(nullptr, fileName, path, strToBool(add));
        }
        else
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "WsWorkunits::%s does not support the upload_ option.", method);
    }
    catch (IException* e)
    {
        me->append(*e);
    }
    catch (...)
    {
        me->append(*makeStringException(ECLWATCH_INTERNAL_ERROR, "Unknown Exception"));
    }
    return onFinishUpload(ctx, request, response, serv, method, fileNames, files, me);
}
