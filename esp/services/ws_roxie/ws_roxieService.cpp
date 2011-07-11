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

#include <string>
#include <iostream>
#include <sstream>
#include <fstream>

#include "ws_roxieService.hpp"
#include "jstring.hpp"
#include "dalienv.hpp"
#include "espxslt.hpp"

void writeFile(const char * path, const char * txt)
{
    std::ofstream outFile(path);
    outFile << txt; 
}

const char * const rcXmlFile = "rc.xml";

void CRoxieEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    StringBuffer xpath;
    
    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/DaliServers", process, service);
    cfg->getProp(xpath.str(), daliServers_);

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/QueueServer", process, service);
    cfg->getProp(xpath.str(), queueServer_);

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/EclServer", process, service);
    cfg->getProp(xpath.str(), eclServer_);

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/RoxieServer", process, service);
    cfg->getProp(xpath.str(), roxieServer_);


    rc_ = new RoxieConfig(rcXmlFile);
    StringBuffer prop;
    rc_->loadProp(prop.appendf("daliServers=%s", daliServers_.str()).str());
    DBGLOG(prop.str());
    prop.clear();
    rc_->loadProp(prop.appendf("queue=%s", queueServer_.str()).str());
    DBGLOG(prop.str());
    prop.clear();
    rc_->loadProp(prop.appendf("timeout=%i", 30).str());
    DBGLOG(prop.str());
    prop.clear();
    rc_->loadProp(prop.appendf("roxieServer=%s", roxieServer_.str()).str());
    DBGLOG(prop.str());

    if (!daliClientActive())
    {
        if(daliServers_.length() == 0)
        {
            throw MakeStringException(-1, "Please specify daliServers in the config file");
        }
        Owned<IGroup> serverGroup = createIGroup(daliServers_.str(), DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_EspServer);
        setPasswordsFromSDS();
    }
}

//  ===============================================================================================
/*
bool CRoxieEx::onIndex(IEspContext &context, IEspIndexRequest &req, IEspIndexResponse &resp)
{
    return true;
}

bool CRoxieEx::onContent(IEspContext &context, IEspContentRequest &req, IEspContentResponse &resp)
{
    Owned<IPropertyTree> Index = createPTree("Index", false);
    IPropertyTree * Contents = Index->addPropTree("Contents", createPTree("Contents", false));

    IPropertyTree * Content = Contents->addPropTree("Content", createPTree("Content", false));
    Content->setProp("Label", "Local Configuration");
    Content->setProp("URL", "GetQueryList?");

    Content = Contents->addPropTree("Content", createPTree("Content", false));
    Content->setProp("Label", "Publish Doxie Query");
    Content->setProp("URL", "GetDoxieList?");

    Content = Contents->addPropTree("Content", createPTree("Content", false));
    Content->setProp("Label", "Add Query");
    Content->setProp("URL", "AddQuery?");

    Content = Contents->addPropTree("Content", createPTree("Content", false));
    Content->setProp("Label", "View Config XML");
    Content->setProp("URL", "DebugXML?");

    StringBuffer tmp;
    resp.setContentResult(toXML(Index, tmp).str());
    return true;
}

bool CRoxieEx::onGetQueryList(IEspContext &context, IEspGetQueryListRequest &req, IEspGetQueryListResponse &resp)
{
    StringBuffer xml, status, result;
    rc_->saveXML(xml);
    rc_->getStatus(status);
    GenerateCurrentXml(xml.str(), status.str(), result);
    resp.setGetQueryListResult(result.str());
    return true;
}

bool CRoxieEx::onAddQuery(IEspContext &context, IEspAddQueryRequest &req, IEspAddQueryResponse &resp)
{
    Owned<IAttributeMetaDataResolver> attrResolver =  createEclAttributeResolver(eclServer_.str()); 
    const char * module = "Doxie";

    StringArray items, infos;
    if (attrResolver->getIndex("GOSMITH","PASSWORD", module, "SOAP", items, infos))
    {
        Owned<IPropertyTree> moduleTree = createPTree("Module", false);
        moduleTree->setProp("@Label", module);
        for (int i = 0; i < items.ordinality(); ++i)
        {
            IPropertyTree *attr = moduleTree->addPropTree("Attribute", createPTree("Attribute", false)); 
            attr->setProp("@Label", items.item(i));
            attr->setProp("Description", infos.item(i));
        }
        StringBuffer xml;
        toXML(moduleTree, xml);
        resp.setModules(xml.str());
    }

    const char * label = req.getLabel();
    const char * ecl = req.getEcl();
    if (strcmp(label, "") != 0 && strcmp(ecl, "") != 0)
    {
        try
        {
            rc_->addQuery(label, ecl);
            StringBuffer s;
            rc_->saveXML(s);
            writeFile(rcXmlFile, s.str());
            resp.setMessage("");
        }
        catch(IException *e)
        {
            StringBuffer errMsg;
            resp.setMessage(e->errorMessage(errMsg).str());
        }
    }
    else
    {
        resp.setMessage("Label + Ecl cannot be blank!");
    }
    return true;
}

bool CRoxieEx::onAddAttribute(IEspContext &context, IEspAddAttributeRequest &req, IEspAddAttributeResponse &resp)
{
    StringBuffer label, ecl;
    label.appendf("%s", req.getAttributeLabel());
    ecl.appendf("%s.%s();", req.getModuleLabel(), req.getAttributeLabel());
    if (strcmp(label.str(), "") != 0 && strcmp(ecl.str(), "") != 0)
    {
        try
        {
            rc_->addQuery(label.str(), ecl.str());
            StringBuffer s;
            rc_->saveXML(s);
            writeFile(rcXmlFile, s.str());
            resp.setMessage("");
        }
        catch(IException *e)
        {
            StringBuffer errMsg;
            resp.setMessage(e->errorMessage(errMsg).str());
        }
    }
    else
    {
        resp.setMessage("Label + Ecl cannot be blank!");
    }
    return true;
}

bool CRoxieEx::onRemoveQuery(IEspContext &context, IEspRemoveQueryRequest &req, IEspRemoveQueryResponse &resp)
{
    const char * label = req.getLabel();
    unsigned int version = req.getVersion();
    if (strcmp(label, "") != 0 && version > 0)
    {
        try
        {
            rc_->removeQuery(label, version);
            StringBuffer s;
            rc_->saveXML(s);
            writeFile(rcXmlFile, s.str());
            resp.setMessage("");
        }
        catch(IException *e)
        {
            StringBuffer errMsg;
            resp.setMessage(e->errorMessage(errMsg).str());
        }
    }
    else
    {
        resp.setMessage("Label + Version cannot be blank!");
    }
    return true;
}

bool CRoxieEx::onPublishQuery(IEspContext &context, IEspPublishQueryRequest &req, IEspPublishQueryResponse &resp)
{
    const char * label = req.getLabel();
    unsigned int version = req.getVersion();
    if (strcmp(label, "") != 0 && version > 0)
    {
        try
        {
            rc_->publishQuery(label, version);
            resp.setMessage("");
        }
        catch(IException *e)
        {
            StringBuffer errMsg;
            resp.setMessage(e->errorMessage(errMsg).str());
        }
    }
    else
    {
        resp.setMessage("Label + Version cannot be blank!");
    }
    return true;
}

bool CRoxieEx::onPublishConfiguration(IEspContext &context, IEspPublishConfigurationRequest &req, IEspPublishConfigurationResponse &resp)
{
    try
    {
        StringBuffer result;
        rc_->publishConfiguration(result);
        resp.setMessage("");
    }
    catch(IException *e)
    {
        StringBuffer errMsg;
        resp.setMessage(e->errorMessage(errMsg).str());
    }
    return true;
}

bool CRoxieEx::onTestQuery(IEspContext &context, IEspTestQueryRequest &req, IEspTestQueryResponse &resp)
{
    const char * label = req.getLabel();
    unsigned int version = req.getVersion();
    if (strcmp(label, "") != 0 && version > 0)
    {
        try
        {
            StringBuffer root, result;
            root.appendf("%s.%i", req.getLabel(), req.getVersion());
            rc_->testQuery(label, version, req.getBody(), result);
            resp.setCode(0);
            resp.setMessage("");
            resp.setResult(result.str());
            resp.setRoot(root.str());
            resp.setBody(req.getBody());
        }
        catch(IException *e)
        {
            StringBuffer errMsg;
            resp.setMessage(e->errorMessage(errMsg).str());
        }
    }
    else
    {
        resp.setMessage("Label + Version cannot be blank!");
    }
    return true;
}

bool CRoxieEx::onRemoveDll(IEspContext &context, IEspRemoveDllRequest &req, IEspRemoveDllResponse &resp)
{
    try
    {
        StringBuffer result;
        RoxieSocket rs(roxieServer_.str());
        rs.removeFile(req.getDll(), result);
        resp.setMessage("");
    }
    catch(IException *e)
    {
        StringBuffer errMsg;
        resp.setMessage(e->errorMessage(errMsg).str());
    }
    return true;
}

bool CRoxieEx::onDebugXML(IEspContext &context, IEspDebugXMLRequest &req, IEspDebugXMLResponse &resp)
{
    StringBuffer s, s2;
    rc_->saveXML(s);
    encodeXML(s.str(), s2);
    resp.setDebugXMLResult(s2.str());
    return true;
}

bool CRoxieEx::onSetActiveVersion(IEspContext &context, IEspSetActiveVersionRequest &req, IEspSetActiveVersionResponse &resp)
{
    return false;
}

bool CRoxieEx::onGetActiveVersion(IEspContext &context, IEspGetActiveVersionRequest &req, IEspGetActiveVersionResponse &resp)
{
    return false;
}

bool CRoxieEx::onGetQueryVersionList(IEspContext &context, IEspGetQueryVersionListRequest &req, IEspGetQueryVersionListResponse &resp)
{
    return false;
}

bool CRoxieEx::onGetResources(IEspContext &context, IEspGetResourcesRequest &req, IEspGetResourcesResponse &resp)
{
    return false;
}

bool CRoxieEx::onAppendQuery(IEspContext &context, IEspAppendQueryRequest &req, IEspAppendQueryResponse &resp)
{
    return false;
}


bool CRoxieEx::onRealignQuery(IEspContext &context, IEspRealignQueryRequest &req, IEspRealignQueryResponse &resp)
{
    return false;
}

//  ===============================================================================================
bool CRoxieEx::onGetFileList(IEspContext &context, IEspGetFileListRequest &req, IEspGetFileListResponse &resp)
{
    return false;
}

bool CRoxieEx::onGetFileVersionList(IEspContext &context, IEspGetFileVersionListRequest &req, IEspGetFileVersionListResponse &resp)
{
    return false;
}

bool CRoxieEx::onAppendFile(IEspContext &context, IEspAppendFileRequest &req, IEspAppendFileResponse &resp)
{
    return false;
}

bool CRoxieEx::onRemoveFile(IEspContext &context, IEspRemoveFileRequest &req, IEspRemoveFileResponse &resp)
{
    return false;
}

bool CRoxieEx::onSetRedundancy(IEspContext &context, IEspSetRedundancyRequest &req, IEspSetRedundancyResponse &resp)
{
    return false;
}

bool CRoxieEx::onGetRedundancy(IEspContext &context, IEspGetRedundancyRequest &req, IEspGetRedundancyResponse &resp)
{
    return false;
}

//  ===============================================================================================

void CRoxieEx::GenerateCurrentXml(const char * localXml, const char * statusXml, StringBuffer &resp)
{
//  DBGLOG(localXml);
//  writeFile("c:\\roxie.xml", statusXml);
//  DBGLOG(statusXml);
    Owned<IPropertyTree> local = createPTreeFromXMLString(localXml, false);
    Owned<IPropertyTree> status = createPTreeFromXMLString(statusXml, false);

    Owned<IPropertyTreeIterator> itr = status->getElements("Summary/File");
    ForEach(*itr)
    {
        IPropertyTree & file = itr->query();
        StringBuffer dll, xpath;
        file.getProp("@name", dll);
        xpath.appendf("Query[@dll=\"%s\"]", dll.str());
        if (!local->hasProp(xpath.str()))
        {
            IPropertyTree * orphan = local->addPropTree("Orphan", createPTree("Orphan", false));
            orphan->setProp("@dll", dll.str());
            if (file.hasProp("@locked") && strcmp(file.queryProp("@locked"), "true") == 0)
                orphan->setProp("@locked", "true");
            orphan->setProp("@count", file.queryProp("@count"));
        }
        else
        {
            Owned<IPropertyTree> localFile = local->getPropTree(xpath.str());
            localFile->setProp("@published", "true");
            localFile->setProp("@count", file.queryProp("@count"));
        }
    }

    StringBuffer xml;
    toXML(local, xml);
//  DBGLOG(xml.str());
    resp.append(xml.str());
}
*/
