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
            if (!name||!*name || cluster.hasProp("RoxieCluster[1]"))
                continue;
            m_clusterNames.append(name);
        }
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
        StringBuffer page;

        page.append(
            "<html>"
                "<head>"
                    "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                    "<title>ESP Service form</title>"
                    "<script language=\"JavaScript\" src=\"files_/calendar_xs.js\"></script>"
                    "<script language=\"JavaScript\" src=\"files_/hint.js\"></script>"
                "</head>"
                "<body>"
                    "<p align=\"center\" />"
                    "<table cellSpacing=\"0\" cellPadding=\"1\" width=\"90%\" bgColor=\"#4775FF\" border=\"0\">"
                        "<tbody>"
                            "<tr align=\"middle\" bgColor=\"#4775FF\">"
                                "<td height=\"23\">"
                                    "<p align=\"left\">"
                                        "<font color=\"#efefef\">");
                                page.appendf("<b>%s [Version %g]</b>", serviceQName.str(), context.getClientVersion());
                                page.append("</font>"
                                    "</p>"
                                "</td>"
                            "</tr>"
                            "<tr bgColor=\"#CBE5FF\">"
                                "<td height=\"3\">"
                                    "<p align=\"left\">");
                                            
                                page.appendf("<b>&gt; %s</b>", methodQName.str());
                                page.append("</p>"
                                "</td>"
                            "</tr>"
                            "<TR bgColor=\"#666666\">"
                                "<TABLE cellSpacing=\"0\" width=\"90%\" bgColor=\"#efefef\" border=\"0\">"
                                    "<TBODY>"
                                        "<TR>"
                                            "<TD vAlign=\"center\" align=\"left\">"
                                                "<p align=\"left\"><br/>");
                                                getMethodDescription(context, serviceQName.str(), methodQName.str(), page);
                                                page.append("</p>"
                                            "</TD>"
                                        "</TR>"
                                    "</TBODY>"
                                "</TABLE>"
                            "</TR>"
                            "<TR bgColor=\"#666666\">"
                                "<TABLE cellSpacing=\"0\" width=\"90%\" bgColor=\"#efefef\" border=\"0\">"
                                    "<TBODY>"
                                        "<TR>"
                                            "<TD vAlign=\"center\" align=\"left\">");
                                                getMethodHtmlForm(context, request, serviceQName.str(), methodQName.str(), page, true);
                                                page.append("</TD>"
                                        "</TR>"
                                    "</TBODY>"
                                "</TABLE>"
                            "</TR>"
                        "</tbody>"
                    "</table>"
                    "<BR />"
                "</body>"
            "</html>");

        response->setContent(page.str());
        response->setContentType("text/html");
    }
    
    response->send();

    return 0;
}

int CEclDirectSoapBindingEx::getMethodHtmlForm(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &form, bool bIncludeFormTag)
{
    if (Utils::strcasecmp(method, "runecl")==0)
    {
        if (bIncludeFormTag)
            form.append("<form method=\"POST\" action=\"/EclDirect/RunEcl\">");

        form.append("<p align=\"left\">");
        form.append("<b>ECL Text: </b><br/><textarea name=\"eclText\" rows=\"20\" cols=\"80\" ></textarea>");
        form.append("<br/><input type=\"button\" value=\"options &gt;&gt;\" onclick=\"if (option_span.style.display=='block'){option_span.style.display='none';value='options &gt;&gt;';} else {option_span.style.display='block'; value='&lt;&lt; options';}\"/>");
        
        form.append("<span id=\"option_span\" style=\"display:none\">");


        form.append("<br/><b>Cluster: </b><br/>");
        form.append("<select name=\"cluster\" >");
        form.append("<option value=\"default\" selected=\"1\" /> default");
        ForEachItemIn(indC, m_clusterNames)
        {
            const char *cluster = m_clusterNames.item(indC);
            form.appendf("<option value=\"%s\"/>%s", cluster, cluster);
        }
        form.append("</select><br/>");
        form.append("<br/><b>Repository Label (Legacy): </b><br/><input type=\"text\" name=\"snapshot\"/><br/>");
            
        form.append("<br/><input type=\"checkbox\" name=\"limitResults\" checked=\"1\" value=\"1\"/> Limit Result Count to 100.");
        form.append("<br/><input type=\"checkbox\" name=\"rawxml_\" /> Output Xml?");
        form.append("</span>");
        
        form.append("<br/><input type=\"submit\" value=\"Submit\" name=\"S1\" />");
        form.append("</p>");

        if (bIncludeFormTag)
            form.append("</form>");
    }
    else
        return CEclDirectSoapBinding::getMethodHtmlForm(context, request, serv, method, form, bIncludeFormTag);

    return 0;
}

int CEclDirectSoapBindingEx::getMethodDescription(IEspContext &context, const char *serv, const char *method, StringBuffer &page)
{
    if (Utils::strcasecmp(method, "RunEcl")==0)
    {
        page.append("Submit ECL text for execution.");
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
