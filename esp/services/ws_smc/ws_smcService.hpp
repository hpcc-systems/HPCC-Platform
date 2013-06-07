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

#ifndef _ESPWIZ_WsSMC_HPP__
#define _ESPWIZ_WsSMC_HPP__

#include "ws_smc_esp.ipp"
#include "TpWrapper.hpp"
#include "WUXMLInfo.hpp"

enum BulletType
{
    bulletNONE = 0,
    bulletOrange = 1,
    bulletYellow = 2,
    bulletWhite = 3,
    bulletGreen = 4,
    bulletError = 5
};

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
    StringBuffer m_PortalURL;
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

    virtual bool onBrowseResources(IEspContext &context, IEspBrowseResourcesRequest & req, IEspBrowseResourcesResponse & resp);
    virtual bool onRoxieControlCmd(IEspContext &context, IEspRoxieControlCmdRequest &req, IEspRoxieControlCmdResponse &resp);

private:
    void addCapabilities( IPropertyTree* pFeatureNode, const char* access, 
                                 IArrayOf<IEspCapability>& capabilities);
    void addToThorClusterList(IArrayOf<IEspThorCluster>& clusters, IEspThorCluster* cluster, const char* sortBy, bool descending);
    void addToRoxieClusterList(IArrayOf<IEspRoxieCluster>& clusters, IEspRoxieCluster* cluster, const char* sortBy, bool descending);
    void addServerJobQueue(IArrayOf<IEspServerJobQueue>& jobQueues, const char* queueName, const char* serverName, const char* serverType);
    void addServerJobQueue(IArrayOf<IEspServerJobQueue>& jobQueues, const char* queueName, const char* queueState, const char* serverName, const char* serverType);
    void getQueueState(int runningJobsInQueue, StringBuffer& queueState, BulletType& colorType);
    void readClusterTypeAndQueueName(CConstWUClusterInfoArray& clusters, const char* clusterName, StringBuffer& clusterType, SCMStringBuffer& clusterQueue);
    void addRunningWUs(IEspContext &context, IPropertyTree& node, CConstWUClusterInfoArray& clusters,
                   IArrayOf<IEspActiveWorkunit>& aws, BoolHash& uniqueWUIDs,
                   StringArray& runningQueueNames, int* runningJobsInQueue);
    void readBannerAndChatRequest(IEspContext& context, IEspActivityRequest &req, IEspActivityResponse& resp);
    void setBannerAndChatData(double version, IEspActivityResponse& resp);
    void getServersAndWUs(IEspContext &context, IEspActivityRequest &req, IEspActivityResponse& resp, double version,
        IPropertyTree* envRoot, CConstWUClusterInfoArray& clusters);
};


class CWsSMCSoapBindingEx : public CWsSMCSoapBinding
{
    StringBuffer m_portalURL;
public:
    CWsSMCSoapBindingEx(){}
    CWsSMCSoapBindingEx(IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL):CWsSMCSoapBinding(cfg, bindname, procname)
    {
        if (!procname || !*procname)
            return;

        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/@portalurl", procname);
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
        data.setProp("@appName", "EclWatch");
        data.setProp("@start_page", "/WsSMC/Activity");
        IPropertyTree *folder = ensureNavFolder(data, "Clusters", NULL, NULL, false, 1);
        ensureNavLink(*folder, "Activity", "/WsSMC/Activity", "Display Activity on all target clusters in an environment", NULL, NULL, 1);
        ensureNavLink(*folder, "Scheduler", "/WsWorkunits/WUShowScheduled", "Access the ECL Scheduler to view and manage scheduled workunits or events", NULL, NULL, 2);
#ifndef USE_RESOURCE
        if (m_portalURL.length() > 0)
        {
            IPropertyTree *folderTools = ensureNavFolder(data, "Resources", NULL, NULL, false, 8);
            ensureNavLink(*folderTools, "Browse", "/WsSMC/BrowseResources", "Browse a list of resources available for download, such as the ECL IDE, documentation, examples, etc. These are only available if optional packages are installed on the ESP Server.", NULL, NULL, 1);
        }
#else
        IPropertyTree *folderTools = ensureNavFolder(data, "Resources", NULL, NULL, false, 8);
        ensureNavLink(*folderTools, "Browse", "/WsSMC/BrowseResources", "Browse a list of resources available for download, such as the ECL IDE, documentation, examples, etc. These are only available if optional packages are installed on the ESP Server.", NULL, NULL, 1);
#endif
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

