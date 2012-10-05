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

#pragma warning (disable : 4786)

#include "EclDirectService.hpp"

#include "workunit.hpp"
#include "fileview.hpp"
#include "wuwebview.hpp"

#define ECLDIRECT_ACCESS "EclDirectAccess"

struct EclDirectWUExceptions
{
    EclDirectWUExceptions(IConstWorkUnit& cw);
    operator IArrayOf<IEspECLDirectException>&() { return errors; }

private:
    IArrayOf<IEspECLDirectException> errors;
};

EclDirectWUExceptions::EclDirectWUExceptions(IConstWorkUnit& cw)
{
    Owned<IConstWUExceptionIterator> it = &cw.getExceptions();
    ForEach(*it)
    {
        SCMStringBuffer s;
        Owned<IEspECLDirectException> e= createECLDirectException();
        IConstWUException &item = it->query();
        e->setCode(item.getExceptionCode());
        e->setSource(item.getExceptionSource(s).str());
        e->setMessage(item.getExceptionMessage(s).str());
        e->setFileName(item.getExceptionFileName(s).str());
        e->setLineNo(item.getExceptionLineNo());
        e->setColumn(item.getExceptionColumn());

        switch (it->query().getSeverity())
        {
            default:
            case ExceptionSeverityError:
                e->setSeverity("Error");
                break;
            case ExceptionSeverityWarning:
                e->setSeverity("Warning");
                break;
            case ExceptionSeverityInformation:
                e->setSeverity("Info");
                break;
        }

        errors.append(*e.getLink());
    }
}

void CEclDirectEx::refreshValidClusters()
{
    validClusters.kill();
    Owned<IStringIterator> it = getTargetClusters(NULL, NULL);
    ForEach(*it)
    {
        SCMStringBuffer s;
        IStringVal &val = it->str(s);
        if (!validClusters.getValue(val.str()))
            validClusters.setValue(val.str(), true);
    }
}

bool CEclDirectEx::isValidCluster(const char *cluster)
{
    CriticalBlock block(crit);
    if (validClusters.getValue(cluster))
        return true;
    if (validateTargetClusterName(cluster))
    {
        refreshValidClusters();
        return true;
    }
    return false;
}

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

    srvcfg->getProp("ClusterName", defaultCluster);

    defaultWait = srvcfg->getPropInt("WuTimeout", 60000);
    deleteWorkunits = cfg->getPropBool("DeleteWorkUnits", false);

    refreshValidClusters();
}

CEclDirectSoapBindingEx::CEclDirectSoapBindingEx(IPropertyTree* cfg, const char *binding, const char *process):CEclDirectSoapBinding(cfg, binding, process)
{
    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name='%s']", process);
    IPropertyTree *procTree = cfg->queryPropTree(xpath.str());
    if (!procTree)
        throw MakeStringException(-1, "EclDirect Configuration Error: unable to find process");

    xpath.set("EspBinding[@name='").append(binding).append("']/@port");
    int port = procTree->getPropInt(xpath.str());
    if (port)
    {
        xpath.set("EspBinding[@type='ws_workunitsSoapBinding'][@port='").append(port).append("']");
        redirect = procTree->hasProp(xpath.str());
    }

    SCMStringBuffer s;
    Owned<IStringIterator> it = getTargetClusters(NULL, NULL);
    ForEach(*it)
        clusters.append(it->str(s).str());
    supportRepository = false;
}

inline void deleteEclDirectWorkunit(IWorkUnitFactory *factory, const char *wuid)
{
    try
    {
        factory->deleteWorkUnit(wuid);
    }
    catch (IException *e)
    {
        DBGLOG(e, "EclDirect Failed to delete workunit");
    }
    catch (...)
    {
    }
}

bool CEclDirectEx::onRunEcl(IEspContext &context, IEspRunEclRequest & req, IEspRunEclResponse & resp)
{
    if (!context.validateFeatureAccess(ECLDIRECT_ACCESS, SecAccess_Full, false))
        throw MakeStringException(-1, "EclDirect access permission denied.");

    StringBuffer user;
    if (!context.getUserID(user).length())
        user.append(req.getUserName());

    Owned <IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned <IWorkUnit> workunit;
    if (!user.length())
        workunit.setown(factory->createWorkUnit(NULL, "ECL-Direct", ""));
    else
    {
        workunit.setown(factory->createWorkUnit(NULL, "ECL-Direct", user.str()));
        workunit->setUser(user.str());
    }

    Owned<IWUQuery> query = workunit->updateQuery();
    query->setQueryText(req.getEclText());
    query.clear();

    const char* clustername = req.getCluster();
    if (!clustername || !*clustername || strieq(clustername, "default"))
        clustername = defaultCluster.str();

    if (!clustername || !*clustername)
        throw MakeStringException(-1, "No Cluster Specified");

    if (!isValidCluster(clustername))
        throw MakeStringException(-1, "Invalid TargetCluster %s Specified", clustername);

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

    submitWorkUnit(wuid.str(), context.querySecManager(), context.queryUser());

    if (waitForWorkUnitToComplete(wuid.str(), defaultWait))
    {
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);

        SCMStringBuffer resultXML;
        getFullWorkUnitResultsXML(context.queryUserId(), context.queryPassword(), cw.get(), resultXML);
        resp.setResults(resultXML.str());

        cw.clear();

        if (deleteWorkunits)
            deleteEclDirectWorkunit(factory, wuid.str());
    }
    else
    {
        // Don't delete these ones...
        DBGLOG("WorkUnit %s timed out", wuid.str());
        
        StringBuffer result;
        result.appendf("<Exception><Source>ESP</Source><Message>Timed out waiting for job to complete: %s</Message></Exception>", wuid.str());
        resp.setResults(result.str());
    }

    return true;
}

bool CEclDirectEx::onRunEclEx(IEspContext &context, IEspRunEclExRequest & req, IEspRunEclExResponse & resp)
{
    if (!context.validateFeatureAccess(ECLDIRECT_ACCESS, SecAccess_Full, false))
        throw MakeStringException(-1, "EclDirect access permission denied.");

    const char* eclText = req.getEclText();
    if (!eclText || !*eclText)
    {
        resp.setResults("<Exception><Source>ESP</Source><Message>No Ecl Text provided</Message></Exception>");
        return true;
    }

    StringBuffer user;
    if (!context.getUserID(user).length())
        user.append(req.getUserName());

    Owned <IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned <IWorkUnit> workunit;
    if (!user.length())
        workunit.setown(factory->createWorkUnit(NULL, "ECL-Direct", ""));
    else
    {
        workunit.setown(factory->createWorkUnit(NULL, "ECL-Direct", user.str()));
        workunit->setUser(user.str());
    }

    Owned<IWUQuery> query = workunit->updateQuery();
    query->setQueryText(eclText);
    query.clear();

    const char* cluster = req.getCluster();
    if (!cluster || !*cluster || !stricmp(cluster, "default"))
        cluster = defaultCluster.str();

    if (!cluster || !*cluster)
        throw MakeStringException(-1, "No Cluster Specified");

    if (!isValidCluster(cluster))
        throw MakeStringException(-1, "Invalid TargetCluster %s Specified", cluster);

    workunit->setClusterName(cluster);

    const char* snapshot = req.getSnapshot();
    if (snapshot && *snapshot)
        workunit->setSnapshot(snapshot);

    if (req.getResultLimit())
        workunit->setResultLimit(req.getResultLimit());

    // Execute it
    SCMStringBuffer wuid;
    workunit->getWuid(wuid);
    workunit->setAction(WUActionRun);
    workunit->setState(WUStateSubmitted);
    workunit.clear();

    resp.setWuid(wuid.str());

    submitWorkUnit(wuid.str(), context.querySecManager(), context.queryUser());

    if (!waitForWorkUnitToComplete(wuid.str(), (req.getWait_isNull()) ? defaultWait : req.getWait()))
    {
        StringBuffer result;
        result.appendf("<Exception><Source>ESP</Source><Message>Timed out waiting for job to complete: %s</Message></Exception>", wuid.str());
        resp.setResults(result.str());
        return true;
    }

    if (!deleteWorkunits && context.queryRequestParameters()->hasProp("redirect"))
    {
        StringBuffer url("/WsWorkunits/WUInfo?Wuid=");
        resp.setRedirectUrl(url.append(wuid).str());
        return true;
    }

    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
    EclDirectWUExceptions errors(*cw);
    resp.setErrors(errors);

    if (req.getIncludeResults())
    {
        StringBuffer results;
        CRunEclExFormat outputFormat = req.getFormat();
        Owned<IWuWebView> web = createWuWebView(wuid.str(), NULL, getCFD(), true);
        if (!web)
            results.appendf("<Exception><Source>ESP</Source><Message>Failed loading result workunit %s</Message></Exception>", wuid.str());
        else if (outputFormat == CRunEclExFormat_Table)
        {
            StringBuffer xsltfile(getCFD());
            web->applyResultsXSLT(xsltfile.append("xslt/wsecl3_result.xslt").str(), results);
        }
        else
        {
            unsigned xmlflags = 0;
            if (outputFormat != CRunEclExFormat_ExtendedXml)
                xmlflags |= WWV_OMIT_SCHEMAS;
            if (context.queryRequestParameters()->hasProp("display_xslt"))
                xmlflags |= WWV_USE_DISPLAY_XSLT;
            else
                xmlflags |= WWV_OMIT_XML_DECLARATION;
            web->expandResults(results, xmlflags);
        }
        resp.setResults(results.str());
    }

    if (req.getIncludeGraphs())
    {
        Owned<IConstWUGraphIterator> it = &cw->getGraphs(GraphTypeAny);
        StringBuffer xgmml("<Graphs>");
        SCMStringBuffer s;
        ForEach(*it)
            xgmml.append(it->query().getXGMML(s, true).str());
        xgmml.append("</Graphs>");
        resp.setGraphsXGMML(xgmml.str());
    }

    if (deleteWorkunits)
        deleteEclDirectWorkunit(factory, wuid.str());

    return true;
}

static void xsltTransform(const char* xml, const char* sheet, IProperties *params, StringBuffer& ret)
{
    if(!checkFileExists(sheet))
        throw MakeStringException(-1, "Could not find stylesheet %s.",sheet);
    Owned<IXslProcessor> proc = getXslProcessor();
    Owned<IXslTransform> trans = proc->createXslTransform();
    trans->setXmlSource(xml, strlen(xml));
    trans->loadXslFromFile(sheet);
    trans->copyParameters(params);
    trans->transform(ret);
}

int CEclDirectSoapBindingEx::sendRunEclExForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer xml;
    xml.append("<RunEclEx clientVersion='").append(context.getClientVersion()).append("'>");
    appendXMLTag(xml, "UseEclRepository", (supportRepository) ? "Yes" : "No");
    appendXMLTag(xml, "Redirect", (redirect) ? "Yes" : "No");
    appendXMLTag(xml, "IncludeResults", (redirect) ? "No" : "Yes");
    ForEachItemIn(i, clusters)
        appendXMLTag(xml, "Cluster", clusters.item(i));
    xml.append("</RunEclEx>");

    StringBuffer xslt(getCFD());
    xslt.append("./smc_xslt/run_ecl.xslt");

    StringBuffer html;
    xsltTransform(xml.str(), xslt.str(), NULL, html);
    response->setContent(html.str());
    response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);
    response->send();

    return 0;
}

inline const char *runEclExFormatMimeType(CRunEclExFormat format)
{
    if (format == CRunEclExFormat_Table)
        return "text/html; charset=utf-8";
    return "application/xml; charset=utf-8";
}

int CEclDirectSoapBindingEx::onGet(CHttpRequest* request, CHttpResponse* response)
{
    const char *path = request->queryPath();
    if (strieq(path, "/EclDirect/RunEclEx/Form"))
        return sendRunEclExForm(*request->queryContext(), request, response);

    if(strieq(path, "/EclDirect/RunEclEx/DisplayResult"))
    {
        IEspContext& context = *request->queryContext();
        request->queryParameters()->setProp("display_xslt", 1);

        CRunEclExRequest reqObj(&context, "EclDirect", request->queryParameters(), request->queryAttachments());
        CRunEclExResponse respObj("EclDirect");
        theService->onRunEclEx(context, *QUERYINTERFACE(&reqObj, IEspRunEclExRequest), *QUERYINTERFACE(&respObj, IEspRunEclExResponse));

        const char *result = respObj.getResults();
        if (result && *result)
        {
            response->setContent(result);
            response->setContentType(runEclExFormatMimeType(reqObj.getFormat()));
        }
        else
        {
            response->setContent("No result in Ecl execution.");
            response->setContentType("text/html");
        }

        response->setStatus(HTTP_STATUS_OK);
        response->send();
        return 0;
    }

    return CEclDirectSoapBinding::onGet(request,response);
}
