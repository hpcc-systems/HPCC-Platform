/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#include "ws_configService.hpp"
#include "jwrapper.hpp"



class Cws_configBindingEx : public Cws_configSoapBinding
{
public:
    Cws_configBindingEx(http_soap_log_level level=hsl_none) : Cws_configSoapBinding(level)
    {
    }
    
    Cws_configBindingEx(IPropertyTree* cfg, const char *bindname, const char *procname, http_soap_log_level level=hsl_none) : Cws_configSoapBinding(cfg, bindname, procname, level)
    {
    }

    virtual void getNavSettings(int &width, bool &resizable, bool &scroll)
    {
        width=200;
        resizable=true; 
        scroll=true;
    }

    void getNavigationData(IEspContext &context, IPropertyTree& data)
    {
        if (queryComponentConfig().getPropBool("@api_only"))
        {
            CHttpSoapBinding::getNavigationData(context, data);
            return;
        }

        Owned<Cws_configEx> pSvc = dynamic_cast<Cws_configEx*>(getService());
        if (pSvc)
            pSvc->getNavigationData(&data);
    }

    void getDynNavData(IEspContext &context, IProperties *params, IPropertyTree & data)
    {
        if (!params)
            return;

    }

    int onGetRoot(IEspContext &context, CHttpRequest* request,  CHttpResponse* response)
    {
        return onGetInstantQuery(context, request, response, "ws_config", "Init");
    }
private:
};



Cws_configSoapBinding* createws_configSoapBinding(IPropertyTree *cfg, const char *name, const char *process)
{
   return new Cws_configBindingEx(cfg, name, process);
}
