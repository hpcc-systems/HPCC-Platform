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

#ifndef _ESPWIZ_WsRoxieQuery_HPP__
#define _ESPWIZ_WsRoxieQuery_HPP__

#ifdef _WIN32
    #define FILEVIEW_API __declspec(dllimport)
#else
    #define FILEVIEW_API
#endif

#include "fileview.hpp"
#include "fvrelate.hpp"

#include "roxiecommlibscm.hpp"
#include "roxiemanagerscm.hpp"

#include <list>
#include "WUWrapper.hpp"

#include "ws_roxiequery_esp.ipp"

class CWsRoxieQuerySoapBindingEx : public CWsRoxieQuerySoapBinding
{
    StringBuffer m_portalURL;
public:
    CWsRoxieQuerySoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsRoxieQuerySoapBinding(cfg, name, process, llevel)
    {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/@portalurl", process);
        const char* portalURL = cfg->queryProp(xpath.str());
        if (portalURL && *portalURL)
            m_portalURL.append(portalURL);
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        IPropertyTree *folder = ensureNavFolder(data, "Roxie Queries", NULL, NULL, false, 7);
        StringBuffer path = "/WsSMC/NotInCommunityEdition?form_";
        if (m_portalURL.length() > 0)
            path.appendf("&EEPortal=%s", m_portalURL.str());
        ensureNavLink(*folder, "Search Roxie Files",path.str(), "Search Roxie Files", NULL, NULL, 2);
        ensureNavLink(*folder, "View Roxie Files", path.str(), "View Roxie Files", NULL, NULL, 3);
    }
};


class CWsRoxieQueryEx : public CWsRoxieQuery
{
private:
    Owned<IXslProcessor> m_xsl;
    Mutex m_superfilemutex;

    StringBuffer m_attsXSLT;
    StringBuffer m_graphStatsXSLT;

public:
    IMPLEMENT_IINTERFACE;
    virtual ~CWsRoxieQueryEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    bool onRoxieQuerySearch(IEspContext &context, IEspRoxieQuerySearchRequest & req, IEspRoxieQuerySearchResponse & resp);
    bool onRoxieQueryList(IEspContext &context, IEspRoxieQueryListRequest &req, IEspRoxieQueryListResponse &resp);
    bool onQueryDetails(IEspContext &context, IEspRoxieQueryDetailsRequest & req, IEspRoxieQueryDetailsResponse & resp);
    bool onGVCAjaxGraph(IEspContext &context, IEspGVCAjaxGraphRequest &req, IEspGVCAjaxGraphResponse &resp);
    bool onShowGVCGraph(IEspContext &context, IEspShowGVCGraphRequest &req, IEspShowGVCGraphResponse &resp);
    bool onRoxieQueryProcessGraph(IEspContext &context, IEspRoxieQueryProcessGraphRequest &req, IEspRoxieQueryProcessGraphResponse &resp);

private:
    void addToQueryString(StringBuffer &queryString, const char *name, const char *value);
    void addToQueryStringFromInt(StringBuffer &queryString, const char *name, int value);
    void getClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress, int& port);

    bool getAllRoxieQueries(IEspContext &context, const char* cluster, const char* suspended, const char* sortBy, bool descending, __int64 displayEnd, IArrayOf<IEspRoxieQuery>& RoxieQueryList, __int64& totalFiles);
    IPropertyTree* getXgmmlGraph(const char *queryName, SocketEndpoint &ep, const char* graphName);
    void enumerateGraphs(const char *queryName, SocketEndpoint &ep, StringArray& graphNames);
};

#endif //_ESPWIZ_WsRoxieQuery_HPP__

