/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "ws_fsService.hpp"
#include "ws_fsBinding.hpp"
#include "TpWrapper.hpp"
#include "environment.hpp"
#include "jwrapper.hpp"
#include "dfuwu.hpp"
#include "dadfs.hpp"
#include "exception_util.hpp"

#define FILE_SPRAY_URL      "FileSprayAccess"
#define FILE_DESPRAY_URL    "FileDesprayAccess"

int CFileSpraySoapBindingEx::onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    bool permission = true;
    bool bDownloadFile = false;
    bool bProcess;
    StringBuffer sourceLogicalFile;
    StringBuffer methodbuf;
    StringBuffer submethod;
    StringBuffer xsltFileName(getCFD());
    xsltFileName.append("smc_xslt/");

    if (stricmp(method, "SprayFixedInput")==0)
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            permission = false;

        bProcess = true;
        xsltFileName.append("fs_sprayForm.xslt");
        methodbuf.append("SprayFixed");
    }
    else if(stricmp(method, "SprayVariableInput")==0)
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            permission = false;

        bProcess = true;
        xsltFileName.append("fs_sprayForm.xslt");
        methodbuf.append("SprayVariable");
        request->getParameter("submethod", submethod);
    }
    else if (stricmp(method, "DesprayInput")==0)
    {
        if (!context.validateFeatureAccess(FILE_DESPRAY_URL, SecAccess_Write, false))
            permission = false;

        request->getParameter("sourceLogicalName", sourceLogicalFile);
        xsltFileName.append("fs_desprayCopyForm.xslt");
        methodbuf.append("Despray");
        bProcess = true;
    }
    else if (stricmp(method, "CopyInput") == 0)
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            permission = false;

        request->getParameter("sourceLogicalName", sourceLogicalFile);
        xsltFileName.append("fs_desprayCopyForm.xslt");
        methodbuf.append("Copy");
        bProcess = true;
    }
    else if (stricmp(method, "RenameInput") == 0)
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            permission = false;

        request->getParameter("sourceLogicalName", sourceLogicalFile);
        xsltFileName.append("fs_renameForm.xslt");
        methodbuf.append("Rename");
        bProcess = true;
    }
    else if (stricmp(method, "DownloadFile") == 0)
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Full, false))
            permission = false;

        downloadFile(context, request, response);
        bDownloadFile = true;
        bProcess = true;
    }
    else
        bProcess = false;

    if (bProcess)
    {
        if (bDownloadFile)
            return 0;

        StringBuffer xml;
        Owned<IProperties> params(createProperties());
        if (!permission)
        {
            params->setProp("@method", methodbuf.str());
            xml.append("<Environment><ErrorMessage>Permission denied.</ErrorMessage></Environment>");
        }
        else
        {
            if(submethod.length() > 0)
                params->setProp("@submethod", submethod.str());
            params->setProp("@method", methodbuf.str());

            if (*sourceLogicalFile.str())
            {
                params->setProp("@sourceLogicalName", sourceLogicalFile.str());

                Owned<IUserDescriptor> userdesc;
                StringBuffer username;
                context.getUserID(username);
                if(username.length() > 0)
                {
                    const char* passwd = context.queryPassword();
                    userdesc.setown(createUserDescriptor());
                    userdesc->set(username.str(), passwd);

                    try 
                    {
                        if (stricmp(method, "CopyInput") == 0)
                        {
                            Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(sourceLogicalFile.str(), userdesc.get());
                            if(!df)
                            {
                                throw MakeStringException(ECLWATCH_FILE_NOT_EXIST,"Could not find file %s.",sourceLogicalFile.str());
                            }
            
                            const char *kind = df->queryAttributes().queryProp("@kind");
                            if (kind && strcmp(kind,"key")==0)
                            {
                                params->setProp("@compressflag", 0);
                            }
                            else if(df->isCompressed())
                            {
                                params->setProp("@compressflag", 2);
                            }
                            else
                            {
                                params->setProp("@compressflag", 1);
                            }
                        }
                    }
                    catch (IException *E)
                    {
                        Owned<IXslProcessor> xslp = getXslProcessor();
                        if (!xslp)
                            throw E;

                        Owned<IMultiException> me = MakeMultiException();
                        me->append(*E);
                        response->handleExceptions(xslp, me, "FileSpray", method, StringBuffer(getCFD()).append("./smc_xslt/exceptions.xslt").str());
                        return 0;
                    }
                }
            }
            else
            {
                params->setProp("@compressflag", 1);
            }

            StringBuffer wuid;
            request->getParameter("wuid", wuid);
            Owned<IPropertyTree> pTree = createPTreeForXslt(method, wuid.str());
            toXML(pTree, xml, false);
        }
    
        IProperties* requestparams = request->queryParameters();
        if(requestparams && requestparams->hasProp("rawxml_"))
        {
            response->setContent(xml.str());
            response->setContentType(HTTP_TYPE_APPLICATION_XML);
        }
        else{
            StringBuffer htmlbuf;
            xsltTransform(xml.str(), xsltFileName.str(), params, htmlbuf);
            response->setContent(htmlbuf.str());
            response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);
        }

        response->send();
        return 0;
    }
    else
        return CFileSpraySoapBinding::onGetInstantQuery(context, request, response, service, method);
}

IPropertyTree* CFileSpraySoapBindingEx::createPTreeForXslt(const char* method, const char* dfuwuid)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
#if 0
    Owned<IConstEnvironment> m_constEnv = factory->openEnvironment();
#else
    Owned<IConstEnvironment> m_constEnv = factory->openEnvironmentByFile();
#endif
    Owned<IPropertyTree> pEnvRoot = &m_constEnv->getPTree();
    IPropertyTree* pEnvSoftware = pEnvRoot->queryPropTree("Software");

    Owned<IPropertyTree> pRoot = createPTreeFromXMLString("<Environment/>");
    IPropertyTree* pSoftware = pRoot->addPropTree("Software", createPTree("Software"));

    if (pEnvSoftware)
    {
        StringBuffer dfuwuidSourcePartIP, wuxml;
        if(dfuwuid && *dfuwuid)
        {
            Owned<IDFUWorkUnitFactory> dfuwu_factory = getDFUWorkUnitFactory();
            Owned<IConstDFUWorkUnit> dfuwu = dfuwu_factory->openWorkUnit(dfuwuid, false);
            if(dfuwu)
            {   
                dfuwu->toXML(wuxml);

                Owned<IPropertyTree> wu = createPTreeFromXMLString(wuxml.str());
                if (wu)
                {
                    const char* ip = wu->queryProp("Source/Part/@node");
                    if (ip && *ip)
                        dfuwuidSourcePartIP.append(ip);
                }
            }
        }

        Owned<IPropertyTreeIterator> it = pEnvSoftware->getElements("DropZone");
        ForEach(*it)
        {
            IPropertyTree* pDropZone = pSoftware->addPropTree("DropZone", &it->get());
            //get IP Address of the computer associated with this drop zone
            const char* pszComputer = it->query().queryProp("@computer");
            if (!strcmp(pszComputer, "."))
                pszComputer = "localhost";

            StringBuffer xpath;
            xpath.appendf("Hardware/Computer[@name='%s']/@netAddress", pszComputer);

            StringBuffer sNetAddr;
            const char* pszNetAddr = pEnvRoot->queryProp(xpath.str());
            if (strcmp(pszNetAddr, "."))
            {       
                sNetAddr.append(pszNetAddr);
            }
            else
            {
                StringBuffer ipStr;
                IpAddress ipaddr = queryHostIP();
                ipaddr.getIpText(ipStr);
                if (ipStr.length() > 0)
                {
#ifdef MACHINE_IP
                    sNetAddr.append(MACHINE_IP);
#else
                    sNetAddr.append(ipStr.str());
#endif
                }
            }
            pDropZone->addProp("@netAddress", sNetAddr.str());
            if ((dfuwuidSourcePartIP.length() > 0) && (sNetAddr.length() > 0))
            {
                IpAddress ip1(dfuwuidSourcePartIP.str()), ip2(sNetAddr.str());
                if (ip1.ipequals(ip2))              
                    pDropZone->addProp("@sourceNode", "1");
            }

            Owned<IConstMachineInfo> machine;
            if (strcmp(pszNetAddr, "."))
                machine.setown(m_constEnv->getMachineByAddress(sNetAddr.str()));
            else
            {
                machine.setown(m_constEnv->getMachineByAddress(pszNetAddr));
                if (!machine)
                    machine.setown(m_constEnv->getMachineByAddress(sNetAddr.str()));
            }

            if (machine)
            {
                //int os = machine->getOS();
                StringBuffer dir;
                pDropZone->getProp("@directory", dir);
                if (machine->getOS() == MachineOsLinux || machine->getOS() == MachineOsSolaris)
                {         
                    dir.replace('\\', '/');//replace all '\\' by '/'
                    pDropZone->setProp("@linux", "true");
                }
                else
                {       
                    dir.replace('/', '\\');
                    dir.replace('$', ':');
                }
                pDropZone->setProp("@directory", dir);
            }
        }

        it.setown(pEnvSoftware->getElements("ThorCluster"));
        ForEach(*it)
            pSoftware->addPropTree("ThorCluster", &it->get());

        it.setown(pEnvSoftware->getElements("EclAgentProcess"));
        ForEach(*it)
        {
            IPropertyTree &cluster = it->query();
            const char* name = cluster.queryProp("@name");
            if (!name||!*name)
                continue;
            
            unsigned ins = 0;
            Owned<IPropertyTreeIterator> insts = cluster.getElements("Instance");
            ForEach(*insts) 
            {
                const char *na = insts->query().queryProp("@netAddress");
                if (!na || !*na) 
                {
                    insts->query().setProp("@gname", name);
                    continue;
                }
                
                SocketEndpoint ep(na);
                if (ep.isNull())
                    continue;

                ins++;
                StringBuffer gname("hthor__");
                //StringBuffer gname;
                gname.append(name);
                if (ins>1)
                    gname.append('_').append(ins);

                insts->query().setProp("@gname", gname.str());

            }

            pSoftware->addPropTree("EclAgentProcess", &it->get());
        }

        if (stricmp(method, "CopyInput") == 0) //Limit for this method only
        {
            it.setown(pEnvSoftware->getElements("RoxieCluster"));
            ForEach(*it)
                pSoftware->addPropTree("RoxieCluster", &it->get());
        }

        if (wuxml.length() > 0)
            pSoftware->addPropTree("DfuWorkunit", createPTreeFromXMLString(wuxml.str()));
    }
    return pRoot.getClear();
}


void CFileSpraySoapBindingEx::xsltTransform(const char* xml, const char* sheet, IProperties *params, StringBuffer& ret)
{
    StringBuffer xsl;
    if (!checkFileExists(sheet))
        throw MakeStringException(ECLWATCH_FILE_NOT_EXIST, "Cannot open stylesheet %s",sheet);

    Owned<IXslProcessor> proc  = getXslProcessor();
    Owned<IXslTransform> trans = proc->createXslTransform();

    trans->setXmlSource(xml, strlen(xml));
    trans->loadXslFromFile(sheet);

    if (params)
    {
        Owned<IPropertyIterator> it = params->getIterator();
        for (it->first(); it->isValid(); it->next())
        {
            const char *key = it->getPropKey();
            //set parameter in the XSL transform skipping over the @ prefix, if any
            const char* paramName = *key == '@' ? key+1 : key;
            trans->setParameter(paramName, StringBuffer().append('\'').append(params->queryProp(key)).append('\'').str());
        }
    }

    trans->transform(ret);
}

void CFileSpraySoapBindingEx::downloadFile(IEspContext &context, CHttpRequest* request, CHttpResponse* response)
{
    try
    {
        StringBuffer netAddressStr, osStr, pathStr, nameStr;
        request->getParameter("NetAddress", netAddressStr);
        request->getParameter("OS", osStr);
        request->getParameter("Path", pathStr);
        request->getParameter("Name", nameStr);
        
#if 0
        StringArray files;
        IProperties* params = request->queryParameters();
        Owned<IPropertyIterator> iter = params->getIterator();
        if (iter && iter->first())
        {
            while (iter->isValid())
            {
                const char *keyname=iter->getPropKey();
                if (!keyname || strncmp(keyname, "Names", 5))
                    continue;

                files.append(params->queryProp(iter->getPropKey()));
                iter->next();
            }
        }
#endif

        if (netAddressStr.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Network address not specified.");

        if (pathStr.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Path not specified.");

        if (nameStr.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"File name not specified.");

        char pathSep = '/';
        if ((osStr.length() > 1) && (atoi(osStr.str())== OS_WINDOWS))
        {
            pathSep = '\\';
        }

        pathStr.replace(pathSep=='\\'?'/':'\\', pathSep);
        if (*(pathStr.str() + pathStr.length() -1) != pathSep)
            pathStr.append( pathSep );

        StringBuffer fullName;
        fullName.appendf("%s%s", pathStr.str(), nameStr.str());

        StringBuffer headerStr("attachment;");
        headerStr.appendf("filename=%s", nameStr.str());

        RemoteFilename rfn;
        rfn.setRemotePath(fullName.str());
        SocketEndpoint ep(netAddressStr.str());
        rfn.setIp(ep);

        Owned<IFile> rFile = createIFile(rfn);
        if (!rFile)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.",fullName.str());

        OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
        if (!rIO)
            throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.",fullName.str());

        IFileIOStream* ioS = createIOStream(rIO);
        context.addCustomerHeader("Content-disposition", headerStr.str());
        response->setContent(ioS);  
        response->setContentType(HTTP_TYPE_OCTET_STREAM);
        response->send();
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return;
}

int CFileSpraySoapBindingEx::onFinishUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response,   const char *service, const char *method)
{
    StringBuffer newUrl, netAddress, path;

    request->getParameter("NetAddress", netAddress);
    request->getParameter("Path", path);

    newUrl.appendf("/FileSpray/DropZoneFiles?NetAddress=%s&Path=%s", netAddress.str(), path.str());
    response->redirect(*request, newUrl.str());

    return 0;
}
