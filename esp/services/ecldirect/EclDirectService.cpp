/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#pragma warning (disable : 4786)

#include "EclDirectService.hpp"
#include "EclDirectBinding.hpp"

#include "daclient.hpp"
#include "workunit.hpp"
#include "fileview.hpp"
#include "wuwebview.hpp"
#include "xsdparser.hpp"
#include "xpp/XmlPullParser.h"

void CEclDirectEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    Owned<IPropertyTree> srvcfg = cfg->getPropTree(xpath.str());

    if (!srvcfg)
    {
        IERRLOG("EclDirect: Configuration Error: unable to load configuration");
        throw MakeStringException(-1, "EclDirect: Configuration Error: unable to load configuration");
    }

    srvcfg->getProp("ClusterName", m_clustername);

    m_def_timeout = srvcfg->getPropInt("WuTimeout", 60000);
    m_deleteworkunits = cfg->getPropBool("DeleteWorkUnits", false);
}

bool CEclDirectEx::onRunEcl(IEspContext &context, IEspRunEclRequest & req, IEspRunEclResponse & resp)
{
    bool sec_access = (context.querySecManager()!=NULL);
    Owned <IWorkUnitFactory> factory;
    if (sec_access)
        factory.setown(getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser()));
    else
        factory.setown(getWorkUnitFactory());

    Owned <IWorkUnit> workunit = factory->createWorkUnit(NULL, "ECL-Direct", "user");
    Owned<IWUQuery> query = workunit->updateQuery();
    query->setQueryText(req.getEclText());
    query.clear();

    StringBuffer user;
    context.getUserID(user);
    
    if (user.length()==0)
        user.append(req.getUserName());

    StringBuffer pw;
    context.getPassword(pw);

    if (pw.length()==0)
        pw.append(req.getPassword());
    
    workunit->setUser((user.length()) ? user.str() : "user");

    const char* clustername = req.getCluster();
    if (!clustername || *clustername==0 || !stricmp(clustername, "default")) 
        clustername = m_clustername.str();

    workunit->setClusterName(clustername);
    if (req.getLimitResults())
        workunit->setResultLimit(100);

    const char* snapshot = req.getSnapshot();
    if (snapshot && *snapshot)
        workunit->setSnapshot(snapshot);

    // Execute it
    SCMStringBuffer wuid;
    
    workunit->getWuid(wuid);
    workunit->setAction(WUActionRun);
    workunit->setState(WUStateSubmitted);
    workunit.clear();

    if (sec_access)
        secSubmitWorkUnit(wuid.str(), *context.querySecManager(), *context.queryUser());
    else
        submitWorkUnit(wuid.str(), user.str(), pw.str());

    if (waitForWorkUnitToComplete(wuid.str(), m_def_timeout))
    {
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
    
        SCMStringBuffer resultXML;
        getFullWorkUnitResultsXML(user.str(), pw.str(), cw.get(), resultXML, false);
        resp.setResults(resultXML.str());

        cw.clear();

        try
        {
            if (m_deleteworkunits)
                factory->deleteWorkUnit(wuid.str());
        }
        catch (...)
        {
        }
    }
    else
    {
        // Don't delete these ones...
        DBGLOG("WorkUnit %s timed out", wuid.str());
        
        StringBuffer result;
        result.appendf("<Exception><Source>ESP</Source><Message>Timed out waiting for job to complete: %s</Message></Exception>", wuid.str());
        resp.setResults(result.str());
    }

    DBGLOG("EclDirect Request served");
    return true;
}

bool CEclDirectEx::onRunEclEx(IEspContext &context, IEspRunEclExRequest & req, IEspRunEclExResponse & resp)
{
    const char* eclText = req.getEclText();
    if (!eclText || !*eclText)
    {
        resp.setResults("<Exception><Source>ESP</Source><Message>No Ecl code</Message></Exception>");
        return true;
    }

    StringBuffer user, pw;
    context.getUserID(user);
    context.getPassword(pw);

    Owned <IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned <IWorkUnit> workunit = factory->createWorkUnit(NULL, "ECL-Direct", (user.length()) ? user.str() : "user");
    workunit->setUser((user.length()) ? user.str() : "user");

    Owned<IWUQuery> query = workunit->updateQuery();
    query->setQueryText(eclText);
    query.clear();

    const char* clustername = req.getCluster();
    if (!clustername || *clustername==0 || !stricmp(clustername, "default"))
        clustername = m_clustername.str();
    workunit->setClusterName(clustername);

    const char* snapshot = req.getSnapshot();
    if (snapshot && *snapshot)
        workunit->setSnapshot(snapshot);

    if (req.getLimitResults())
        workunit->setResultLimit(100);

    // Execute it
    SCMStringBuffer wuid;
    workunit->getWuid(wuid);
    workunit->setAction(WUActionRun);
    workunit->setState(WUStateSubmitted);
    workunit.clear();

    if (context.querySecManager()!=NULL)
        secSubmitWorkUnit(wuid.str(), *context.querySecManager(), *context.queryUser());
    else
        submitWorkUnit(wuid.str(), user.str(), pw.str());

    if (!waitForWorkUnitToComplete(wuid.str(), m_def_timeout))
    {
        StringBuffer result;
        result.appendf("<Exception><Source>ESP</Source><Message>Timed out waiting for job to complete: %s</Message></Exception>", wuid.str());
        resp.setResults(result.str());
        return true;
    }

    StringBuffer results;
    const char* outputFormat = req.getOutputFormat();
    Owned<IWuWebView> web = createWuWebView(wuid.str(), NULL, getCFD(), true);
    if (!web)
    {
        results.appendf("<Exception><Source>ESP</Source><Message>Failed loading result workunit %s</Message></Exception>", wuid.str());
    }
    else if (!outputFormat || !streq(outputFormat, "TABLE"))
    {
        StringBuffer resultXML;
        unsigned xmlflags = WWV_ADD_SOAP | WWV_ADD_RESULTS_TAG | WWV_ADD_RESPONSE_TAG | WWV_INCL_NAMESPACES | WWV_INCL_GENERATED_NAMESPACES | WWV_USE_DISPLAY_XSLT;
        if (outputFormat && streq(outputFormat, "XML"))
            xmlflags |= WWV_OMIT_SCHEMAS;
        web->expandResults(results, xmlflags);
    }
    else
    {
        StringBuffer xsltfile(getCFD());
        xsltfile.append("xslt/wsecl3_result.xslt");
        web->applyResultsXSLT(xsltfile.str(), results);
    }
    resp.setResults(results.str());

    try
    {
        if (m_deleteworkunits)
            factory->deleteWorkUnit(wuid.str());
    }
    catch (...)
    {
    }

    return true;
}

void CEclDirectSoapBindingEx::initFromEnv()
{
    try
    {
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironmentByFile();
        Owned<IPropertyTree> root = &constEnv->getPTree();
        if (!root)
            throw MakeStringException(20045, "Failed to get environment information");

        Owned<IPropertyTreeIterator> clusters= root->getElements("Software/Topology/Cluster");
        ForEach(*clusters)
        {
            IPropertyTree &cluster = clusters->query();                 
            const char* name = cluster.queryProp("@name");
            if (!name||!*name)
                continue;
            m_clusterNames.append(name);
        }

        //m_useEclRepository is set to true if an Ecl repository can be used
        m_useEclRepository = false;
    }
    catch (IException *e)
    {
        StringBuffer msg;
        ERRLOG("Exception getting environment information (%d) -- %s", e->errorCode(), e->errorMessage(msg).str());
    }
    catch (...)
    {
        ERRLOG("Unknown Exception getting environment information");
    }

}

void xsltTransform(const char* xml, const char* sheet, IProperties *params, StringBuffer& ret)
{
    StringBuffer xsl;
    if(!checkFileExists(sheet))
        throw MakeStringException(-1, "Could not find stylesheet %s.",sheet);
    Owned<IXslProcessor> proc = getXslProcessor();
    Owned<IXslTransform> trans = proc->createXslTransform();
    trans->setXmlSource(xml, strlen(xml));
    trans->loadXslFromFile(sheet);
    trans->copyParameters(params);
    trans->transform(ret);
}

int CEclDirectSoapBindingEx::onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;

    if (!qualifyServiceName(context, serv, method, serviceQName, &methodQName))
    {
        return onGetNotFound(context, request,  response, serv);
    }
    else
    {
        StringBuffer xml;
        StringBuffer xslt;

        xml.append("<RunEclEx>");
        if (strieq(method, "runeclex"))
        {
            xml.append("<EclWatchRunEcl>");
        }
        else
        {
            xml.append("<RunEcl>");
        }
        
        ForEachItemIn(indC, m_clusterNames)
        {
            const char *cluster = m_clusterNames.item(indC);
            if (cluster && *cluster)
                appendXMLTag(xml, "Cluster", cluster);
        }

        StringBuffer buf;
        appendXMLTag(xml, "ServiceQName", serviceQName.str());
        appendXMLTag(xml, "MethodQName",methodQName.str());
        appendXMLTag(xml, "ClientVersion", buf.appendf("%g",context.getClientVersion()).str());
        if (m_useEclRepository)
            appendXMLTag(xml, "UseEclRepository", "Yes");

        if (strieq(method, "runeclex"))
        {
            xml.append("</EclWatchRunEcl>");
        }
        else
        {
            xml.append("</RunEcl>");
        }
        xml.append("</RunEclEx>");

        xslt.append(getCFD()).append("./smc_xslt/run_ecl.xslt");

        if (xslt.length() && xml.length())
        {
            StringBuffer html;
            xsltTransform(xml.str(), xslt.str(), NULL, html);
            response->setContent(html.str());
            response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);
            response->send();
        }
    }

    return 0;
}

int CEclDirectSoapBindingEx::getMethodHelp(IEspContext &context, const char *serv, const char *method, StringBuffer &page)
{
    return 0;
}

//overide the default behavior when the basic service method url is accessed (http://host:port/EclDirect/RunEcl)
//      by default that is a query with no parameters, but we can just display the form
//      instead of the user having to add "?form" to the end
int CEclDirectSoapBindingEx::onGetService(IEspContext &context, CHttpRequest* request,   CHttpResponse* response, const char *serv, const char *method, const char *pathex)
{
    StringBuffer propStr;
    IProperties *props = request->queryParameters();

    //can't just get properties count?
    if (props && props->hasProp("eclText"))
        return onGetQuery(context, request, response, serv, method);

    return onGetXForm(context, request, response,   serv, method);
}

int CEclDirectSoapBindingEx::getWsdlMessages(IEspContext &context, StringBuffer &content, const char *service, const char *method)
{
    bool allMethods = (method==NULL || *method==0);
    if (allMethods || Utils::strcasecmp(method, "RunEcl")==0)
    {
        content.append("<message name=\"RunEclSoapIn\">");
        content.append("<part name=\"parameters\" element=\"tns:RunEclRequest\"/>");
        content.append("</message>");

        content.append("<message name=\"RunEclSoapOut\">");
        content.append("<part name=\"parameters\" element=\"tns:RunEclResponse\"/>");
        content.append("</message>");

        content.append("<message name=\"RunEclHttpGetIn\">");
        content.append("<part name=\"parameters\" element=\"tns:string\"/>");
        content.append("</message>");

        content.append("<message name=\"RunEclHttpGetOut\">");
        content.append("<part name=\"parameters\" element=\"tns:string\"/>");
        content.append("</message>");

        content.append("<message name=\"RunEclHttpPostIn\">");
        content.append("<part name=\"parameters\" element=\"tns:string\"/>");
        content.append("</message>");

        content.append("<message name=\"RunEclHttpPostOut\">");
        content.append("<part name=\"parameters\" element=\"tns:string\"/>");
        content.append("</message>");

    }
    return 0;
}

int CEclDirectSoapBindingEx::getWsdlBindings(IEspContext &context, StringBuffer &content, const char *service, const char *method)
{
    bool allMethods = (method==NULL || *method==0);
    content.append("<binding name=\"EclDirectServiceSoap\" type=\"tns:EclDirectServiceSoap\">");
    content.append("<soap:binding transport=\"http://schemas.xmlsoap.org/soap/http\" style=\"document\"/>");

    if (allMethods || Utils::strcasecmp(method, "RunEcl")==0)
    {
        content.append("<operation name=\"RunEcl\">");
        content.append("<soap:operation soapAction=\"urn:hpccsystems:ws:ecldirect:runecl\" style=\"document\"/>");
        content.append("<input>");
        content.append("<soap:body use=\"literal\"/>");
        content.append("</input>");
        content.append("<output><soap:body use=\"literal\"/></output>");
        content.append("</operation>");

    }
    content.append("</binding>");
    content.append("<binding name=\"EclDirectServiceHttpGet\" type=\"tns:EclDirectServiceHttpGet\">");
    content.append("<http:binding verb=\"GET\"/>");

    if (allMethods || Utils::strcasecmp(method, "RunEcl")==0)
    {
        content.append("<operation name=\"RunEcl\">");
        content.append("<http:operation location=\"/RunEcl\"/>");
        content.append("<input><http:urlEncoded/></input>");
        content.append("<output><mime:mimeXml part=\"Body\"/></output>");
        content.append("</operation>");

    }
    content.append("</binding>");
    content.append("<binding name=\"EclDirectServiceHttpPost\" type=\"tns:EclDirectServiceHttpPost\">");
    content.append("<http:binding verb=\"POST\"/>");

    if (allMethods || Utils::strcasecmp(method, "RunEcl")==0)
    {
        content.append("<operation name=\"RunEcl\">");
        content.append("<http:operation location=\"/RunEcl\"/>");
        content.append("<input><mime:content type=\"application/x-www-form-urlencoded\"/></input>");
        content.append("<output><mime:mimeXml part=\"Body\"/></output>");
        content.append("</operation>");

    }
    content.append("</binding>");
    return 0;
}

int CEclDirectSoapBindingEx::onGet(CHttpRequest* request, CHttpResponse* response)
{
    try
    {
        Owned<IEspEclDirect> iserv = (IEspEclDirect*)getService();
        if(iserv == NULL)
        {
            StringBuffer respStr;
            respStr.append("Service not available");
            response->setContent(respStr.str());
            response->setContentType("text/html");
            response->send();
            return 0;
        }

        sub_service sstype = sub_serv_unknown;
        StringBuffer path, pathEx, serviceName, methodName;
        request->getPath(path);
        request->getEspPathInfo(sstype, &pathEx, &serviceName, &methodName, false);

        if((sstype != sub_serv_form) && !strnicmp(path.str(), "/EclDirect/RunEclEx", 19))
        {
            IEspContext& context = *request->queryContext();
            Owned<CRunEclExRequest> esp_request = new CRunEclExRequest(&context, "EclDirectEx", request->queryParameters(), request->queryAttachments());
            IEspRunEclExRequest& req= *esp_request.get();

            CRunEclExResponse* resp = new CRunEclExResponse("EclDirect");
            Owned<CSoapResponseBinding> esp_response;
            esp_response.setown(resp);
            iserv->onRunEclEx(*request->queryContext(), req, *resp);
            if (esp_response.get())
            {
                const char *result = resp->getResults();
                if (result && *result)
                {
                    const char* outputFormat = req.getOutputFormat();
                    if (!outputFormat || streq(outputFormat, "XML") || streq(outputFormat, "EXTENDEDXML"))
                    {
                        response->setContentType("text/xml; charset=UTF-8");
                    }
                    else
                    {
                        response->setContentType("text/html; charset=utf-8");
                    }
                    response->setContent(result);
                }
                else
                {
                    response->setContent("No result in Ecl execution.");
                    response->setContentType("text/html");
                }
            }
            else
            {
                response->setContent("Failed in Ecl execution.");
                response->setContentType("text/html");
            }

            response->setStatus(HTTP_STATUS_OK);
            response->send();
            return 0;
        }
    }
    catch (IException *e)
    {
        StringBuffer msg;
        ERRLOG("Exception processing HTTP Get request in EclDirect: (%d) -- %s", e->errorCode(), e->errorMessage(msg).str());
    }
    catch (...)
    {
        ERRLOG("Unknown Exception processing HTTP Get request in EclDirect");
    }

    return CEclDirectSoapBinding::onGet(request,response);
}
