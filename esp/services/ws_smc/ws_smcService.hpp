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

#ifndef _ESPWIZ_WsSMC_HPP__
#define _ESPWIZ_WsSMC_HPP__

#include "ws_smc_esp.ipp"
#include "TpWrapper.hpp"
#include "WUXMLInfo.hpp"

class CWsSMCEx : public CWsSMC
{
    long m_counter;
    CTpWrapper m_ClusterStatus;
    CWUXMLInfo m_WuidInfo;

    StringBuffer m_ChatURL;
    StringBuffer m_Banner;
    StringBuffer m_BannerSize;
    StringBuffer m_BannerColor;
    StringBuffer m_BannerScroll;
    int m_BannerAction;
    bool m_EnableChatURL;

public:
    IMPLEMENT_IINTERFACE;
    CWsSMCEx(){}
    virtual ~CWsSMCEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    bool onMoveJobDown(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onMoveJobUp(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onMoveJobBack(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onMoveJobFront(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onRemoveJob(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onStopQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp);
    bool onResumeQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp);
    bool onPauseQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp);
    bool onClearQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp);
    bool onIndex(IEspContext &context, IEspSMCIndexRequest &req, IEspSMCIndexResponse &resp);
    bool onActivity(IEspContext &context, IEspActivityRequest &req, IEspActivityResponse &resp);
    bool onSetJobPriority(IEspContext &context, IEspSMCPriorityRequest &req, IEspSMCPriorityResponse &resp);
    bool onGetThorQueueAvailability(IEspContext &context, IEspGetThorQueueAvailabilityRequest &req, IEspGetThorQueueAvailabilityResponse& resp);
    bool onSetBanner(IEspContext &context, IEspSetBannerRequest &req, IEspSetBannerResponse& resp);
    bool onNotInCommunityEdition(IEspContext &context, IEspNotInCommunityEditionRequest &req, IEspNotInCommunityEditionResponse &resp);

private:
    void addCapabilities( IPropertyTree* pFeatureNode, const char* access, 
                                 IArrayOf<IEspCapability>& capabilities);
    bool isInWuList(IArrayOf<IEspActiveWorkunit>& aws, const char* wuid);
};


class CWsSMCSoapBindingEx : public CWsSMCSoapBinding
{
    StringBuffer m_authType, m_portalURL;

public:
    CWsSMCSoapBindingEx(){}
    CWsSMCSoapBindingEx(IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL):CWsSMCSoapBinding(cfg, bindname, procname)
	{
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/Authentication/@method", procname);
        const char* method = cfg->queryProp(xpath);
        if (method && *method)
            m_authType.append(method);
        xpath.clear().appendf("Software/EspProcess[@name='%s']/@portalurl", procname);
        const char* portalURL = cfg->queryProp(xpath.str());
        if (portalURL && *portalURL)
            m_portalURL.append(portalURL);
	}

    virtual ~CWsSMCSoapBindingEx(){}

    virtual int onGetRoot(IEspContext &context, CHttpRequest* request,  CHttpResponse* response)
    {
        return  onGetInstantQuery(context, request, response, "WsSMC", "Activity");
    }

    virtual int onGetIndex(IEspContext &context, CHttpRequest* request,  CHttpResponse* response, const char *service)
    {
        return  onGetInstantQuery(context, request, response, "WsSMC", "Activity");
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        bool isFF = false;
        StringBuffer browserUserAgent;
        context.getUseragent(browserUserAgent);
        if ((browserUserAgent.length() > 0) && strstr(browserUserAgent.str(), "Firefox"))
            isFF = true;

        StringBuffer path = "/WsSMC/NotInCommunityEdition?form_";
        if (m_portalURL.length() > 0)
            path.appendf("&EEPortal=%s", m_portalURL.str());

		data.setProp("@appName", "EclWatch");
        data.setProp("@start_page", "/WsSMC/Activity");

        IPropertyTree *folderSMC = ensureNavFolder(data, "Clusters", "Clusters");
        ensureNavLink(*folderSMC, "Activity", "/WsSMC/Activity", "Change Password");
        ensureNavLink(*folderSMC, "Scheduler", "/WsWorkunits/WUShowScheduled", "Show Scheduled WUs");

        IPropertyTree *folderTopology = ensureNavFolder(data, "Topology", "Topology");
        ensureNavLink(*folderTopology, "Target Clusters", "/WsTopology/TpTargetClusterQuery?Type=ROOT", "Target Clusters");
        ensureNavLink(*folderTopology, "Cluster Processes", "/WsTopology/TpClusterQuery?Type=ROOT", "Cluster Processes");
        ensureNavLink(*folderTopology, "System Servers", "/WsTopology/TpServiceQuery?Type=ALLSERVICES", "System Servers");

		IPropertyTree *folderWorkunit = ensureNavFolder(data, "ECL Workunits", "ECL Workunits");
        ensureNavLink(*folderWorkunit, "Search", "/WsWorkunits/WUQuery?form_", "Search Workunits");
        ensureNavLink(*folderWorkunit, "Browse", "/WsWorkunits/WUQuery", "Browse Workunits");

        IPropertyTree *folderQueryset = ensureNavFolder(data, "Query Sets", "Queryset Management");
        ensureNavLink(*folderQueryset, "Browse", "/WsWorkunits/WUQuerySets", "Browse Querysets");

        IPropertyTree *folderDFUWorkunit = ensureNavFolder(data, "DFU Workunits", "DFU Workunits");
        ensureNavLink(*folderDFUWorkunit, "Search", "/FileSpray/DFUWUSearch", "Search Workunits");
        ensureNavLink(*folderDFUWorkunit, "Browse", "/FileSpray/GetDFUWorkunits", "Browse Workunits");

        IPropertyTree *folderDFUFile = ensureNavFolder(data, "DFU Files", "DFU Files");
        ensureNavLink(*folderDFUFile, "Upload/download File", "/FileSpray/DropZoneFiles", "Upload/download File");
        ensureNavLink(*folderDFUFile, "View Data File", "/WsDfu/DFUGetDataColumns?ChooseFile=1", "View Data File");
        ensureNavLink(*folderDFUFile, "Search File Relationships", "/WsRoxieQuery/FileRelationSearch", "Search File Relationships");
        ensureNavLink(*folderDFUFile, "Browse Space Usage", "/WsDfu/DFUSpace", "Browse Space Usage");
        ensureNavLink(*folderDFUFile, "Search Logical Files", "/WsDfu/DFUSearch", "Search Logical Files");
        ensureNavLink(*folderDFUFile, "Browse Logical Files", "/WsDfu/DFUQuery", "Browse Logical Files");
        ensureNavLink(*folderDFUFile, "Browse Files by Scope", "/WsDfu/DFUFileView", "Browse Files by Scope");
        ensureNavLink(*folderDFUFile, "Spray Fixed", "/FileSpray/SprayFixedInput", "Spray Fixed");
        ensureNavLink(*folderDFUFile, "Spray CSV", "/FileSpray/SprayVariableInput?submethod=csv", "Spray CSV");
        ensureNavLink(*folderDFUFile, "Spray XML", "/FileSpray/SprayVariableInput?submethod=xml", "Spray XML");
        ensureNavLink(*folderDFUFile, "Remote Copy", "/FileSpray/CopyInput", "Remote Copy");
        ensureNavLink(*folderDFUFile, "XRef", "/WsDFUXRef/DFUXRefList", "XRef");

        IPropertyTree *folderRoxieQuery = ensureNavFolder(data, "Roxie Queries", "Roxie Queries");
        ensureNavLink(*folderRoxieQuery, "Search Roxie Queries", "/WsRoxieQuery/RoxieQuerySearch", "Search Roxie Queries");
        ensureNavLink(*folderRoxieQuery, "Search Roxie Files",path.str(), "Search Roxie Files");
        ensureNavLink(*folderRoxieQuery, "View Roxie Files", path.str(), "View Roxie Files");

        IPropertyTree *folderResource = ensureNavFolder(data, "Resources", "HPCC Resources");
        ensureNavLink(*folderResource, "Browse", "/WsRoxieQuery/BrowseResources", "List HPCC Resources for Download");

        IPropertyTree *folderAccount = ensureNavFolder(data, "My Account", "My Account");
        ensureNavLink(*folderAccount, "Change Password", path.str(), "Change Password");
        if (!isFF)
            ensureNavLink(*folderAccount, "Relogin", path.str(), "Relogin");
        ensureNavLink(*folderAccount, "Who Am I", path.str(), "WhoAmI");

        IPropertyTree *folderPermission = ensureNavFolder(data, "Users/Permissions", "Permissions");
        ensureNavLink(*folderPermission, "Users", path.str(), "Users");
        ensureNavLink(*folderPermission, "Groups", path.str(), "Groups");
        ensureNavLink(*folderPermission, "Permissions", path.str(), "Permissions");
    }
    virtual void getNavSettings(int &width, bool &resizable, bool &scroll)
    {
        width=168;
        resizable=false;
        scroll=true;
    }

    virtual int onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
};

#endif //_ESPWIZ_WsSMC_HPP__

