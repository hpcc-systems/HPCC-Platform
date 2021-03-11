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
#include "daadmin.hpp"

#define REQPATH_EXPORTSDSDATA "/WSDali/Export"

const char* daliFolder = "tempdalifiles" PATHSEPSTR;
const unsigned daliFolderLength = strlen(daliFolder);

void CWSDaliEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
    espProcess.set(process);
}

bool CWSDaliEx::onGetValue(IEspContext& context, IEspGetValueRequest& req, IEspGetValueResponse& resp)
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

        StringBuffer result;
        get(path, &result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
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
