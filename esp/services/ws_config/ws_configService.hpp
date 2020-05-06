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

#ifndef _ESPWIZ_WS_CONFIG_HPP__
#define _ESPWIZ_WS_CONFIG_HPP__

#include "ws_config_esp.ipp"
#include "environment.hpp"

//==========================================================================================



class Cws_configEx : public Cws_config
{
public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual ~Cws_configEx();

    virtual bool onGetConfigAccess(IEspContext &context, IEspConfigAccessRequest& req, IEspConfigAccessResponse& resp);

private:
};



class Cws_configSoapBindingEx : public Cws_configSoapBinding
{
public:
    Cws_configSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) 
      : Cws_configSoapBinding(cfg, name, process, llevel){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        if (queryComponentConfig().getPropBool("@api_only"))
        {
            CHttpSoapBinding::getNavigationData(context, data);
            return;
        }
    }
};

#endif //_ESPWIZ_WS_CONFIG_HPP__

