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

#include "WsDeployService.hpp"
#include "deploy.hpp"
#include "jwrapper.hpp"



class CWsDeployBindingEx : public CWsDeploySoapBinding
{
public:
    CWsDeployBindingEx(http_soap_log_level level=hsl_none) : CWsDeploySoapBinding(level)
    {
    }
    
    CWsDeployBindingEx(IPropertyTree* cfg, const char *bindname, const char *procname, http_soap_log_level level=hsl_none) : CWsDeploySoapBinding(cfg, bindname, procname, level)
    {
    }

    virtual void getNavSettings(int &width, bool &resizable, bool &scroll)
    {
        width=475;
        resizable=true; 
        scroll=true;
    }

    void getNavigationData(IEspContext &context, IPropertyTree& data)
    {
        Owned<CWsDeployEx> pSvc = dynamic_cast<CWsDeployEx*>(getService());
        if (pSvc)
            pSvc->getNavigationData(context, &data);
    }

    void getDynNavData(IEspContext &context, IProperties *params, IPropertyTree & data)
    {
        if (!params)
            return;
    }

    int onGetRoot(IEspContext &context, CHttpRequest* request,  CHttpResponse* response)
    {
        return onGetInstantQuery(context, request, response, "WsDeploy", "Init");
    }

    virtual const char* getRootPage()
    {
        return "files/configmgr.html";
    }


private:
};



CWsDeploySoapBinding* createWsDeploySoapBinding(IPropertyTree *cfg, const char *name, const char *process)
{
   return new CWsDeployBindingEx(cfg, name, process);
}
