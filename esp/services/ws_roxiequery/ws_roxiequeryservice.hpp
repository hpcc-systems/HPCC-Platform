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
        IPropertyTree *folder = ensureNavFolder(data, "Roxie Queries", "Roxie Queries", NULL, false, 7);
        ensureNavLink(*folder, "Search Roxie Queries", "/WsRoxieQuery/RoxieQuerySearch", "Search Roxie Queries", NULL, NULL, 1);

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

private:
    void addToQueryString(StringBuffer &queryString, const char *name, const char *value);
    void addToQueryStringFromInt(StringBuffer &queryString, const char *name, int value);
    void getClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress, int& port);

    bool getAllRoxieQueries(IEspContext &context, const char* cluster, const char* suspended, const char* sortBy, bool descending, __int64 displayEnd, IArrayOf<IEspRoxieQuery>& RoxieQueryList, __int64& totalFiles);
};

#endif //_ESPWIZ_WsRoxieQuery_HPP__

