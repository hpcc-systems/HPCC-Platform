/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#ifndef _ESPWIZ_Esdl_SVC_Engine_HPP__
#define _ESPWIZ_Esdl_SVC_Engine_HPP__

#include "esdl_binding.hpp"
#include "wsexcept.hpp"
#include "dasds.hpp"

class CEsdlSvcEngine : public EsdlServiceImpl
{
private:
    CriticalSection trxIdCritSec;

public:
    Owned<IPropertyTree> m_service_ctx;

    IMPLEMENT_IINTERFACE;
    CEsdlSvcEngine(){}

    ~CEsdlSvcEngine();

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    void generateTransactionId(IEspContext & context, StringBuffer & trxid);
    virtual IPropertyTree *createTargetContext(IEspContext &context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *req_pt);
    virtual void esdl_log(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *tgtcfg, IPropertyTree *tgtctx, IPropertyTree *req_pt, const char *xmlresp, const char *logdata, unsigned int timetaken);
};

class CEsdlSvcEngineSoapBindingEx : public EsdlBindingImpl
{
public:

public:
    IMPLEMENT_IINTERFACE;

    CEsdlSvcEngineSoapBindingEx();
    CEsdlSvcEngineSoapBindingEx(IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL);

    ~CEsdlSvcEngineSoapBindingEx()
    {
    }

    virtual void addService(const char * name, const char * host, unsigned short port, IEspService & service)
    {
         m_pESDLService = dynamic_cast<CEsdlSvcEngine*>(&service);
        EsdlBindingImpl::addService(name, host, port, service);
    }

    virtual const char *queryServiceType(){return  m_pESDLService->getServiceType();}
    virtual void handleSoapRequestException(IException *e, const char *source);
};

#endif //_ESPWIZ_Esdl_SVC_Engine_HPP__
