/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems.

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

#ifndef _ESPWIZ_ws_dali_HPP__
#define _ESPWIZ_ws_dali_HPP__

#include "ws_dali_esp.ipp"
#include "exception_util.hpp"

class CWSDaliEx : public CWSDali
{
    StringAttr espProcess;
    std::atomic<bool> daliDetached{false};

public:
    IMPLEMENT_IINTERFACE;

    bool isDaliDetached()
    {
        return daliDetached;
    }

    virtual bool attachServiceToDali() override
    {
        daliDetached = false;
        return true;
    }

    virtual bool detachServiceFromDali() override
    {
        daliDetached = true;
        return true;
    }

    virtual void init(IPropertyTree* cfg, const char* process, const char* service) override;
    virtual bool onGetValue(IEspContext& context, IEspGetValueRequest& req, IEspGetValueResponse& resp) override;
};

class CWSDaliSoapBindingEx : public CWSDaliSoapBinding
{
    CWSDaliEx* wsdService = nullptr;

    void exportSDSData(CHttpRequest* request, CHttpResponse* response);

public:
    CWSDaliSoapBindingEx(http_soap_log_level level = hsl_none) : CWSDaliSoapBinding(level) { }
    CWSDaliSoapBindingEx(IPropertyTree* cfg, const char* bindName, const char* procName, http_soap_log_level level = hsl_none)
        : CWSDaliSoapBinding(cfg, bindName, procName, level) { }

    virtual int onGet(CHttpRequest* request, CHttpResponse* response) override;

    virtual void addService(const char* name, const char* host, unsigned short port, IEspService& service) override
    {
        wsdService = dynamic_cast<CWSDaliEx*>(&service);
        CWSDaliSoapBinding::addService(name, host, port, service);
    }
};

#endif //_ESPWIZ_ws_dali_HPP__
