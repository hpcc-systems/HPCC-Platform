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

#ifndef _ESPWIZ_ws_packageprocess_HPP__
#define _ESPWIZ_ws_packageprocess_HPP__

#include "ws_packageprocess_esp.ipp"



class CWsPackageProcessSoapBindingEx : public CWsPackageProcessSoapBinding
{
public:
    CWsPackageProcessSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsPackageProcessSoapBinding(cfg, name, process, llevel)
    {
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        //Add navigation link here
    }
};


class CWsPackageProcessEx : public CWsPackageProcess
{
public:
    IMPLEMENT_IINTERFACE;
    virtual ~CWsPackageProcessEx(){};

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    virtual bool onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp);
    virtual bool onAddPackage(IEspContext &context, IEspAddPackageRequest &req, IEspAddPackageResponse &resp);
    virtual bool onDeletePackage(IEspContext &context, IEspDeletePackageRequest &req, IEspDeletePackageResponse &resp);
    virtual bool onActivatePackage(IEspContext &context, IEspActivatePackageRequest &req, IEspActivatePackageResponse &resp);
    virtual bool onDeActivatePackage(IEspContext &context, IEspDeActivatePackageRequest &req, IEspDeActivatePackageResponse &resp);
    virtual bool onListPackage(IEspContext &context, IEspListPackageRequest &req, IEspListPackageResponse &resp);
    virtual bool onGetPackage(IEspContext &context, IEspGetPackageRequest &req, IEspGetPackageResponse &resp);
    virtual bool onValidatePackage(IEspContext &context, IEspValidatePackageRequest &req, IEspValidatePackageResponse &resp);
};

#endif //_ESPWIZ_ws_packageprocess_HPP__

