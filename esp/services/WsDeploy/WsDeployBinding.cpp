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

    virtual const char* getRootPage(IEspContext* ctx)
    {
        return "files/configmgr.html";
    }


private:
};



CWsDeploySoapBinding* createWsDeploySoapBinding(IPropertyTree *cfg, const char *name, const char *process)
{
   return new CWsDeployBindingEx(cfg, name, process);
}
