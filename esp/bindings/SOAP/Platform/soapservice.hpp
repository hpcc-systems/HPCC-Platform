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

#ifndef _SOAPSERVICE_HPP__
#define _SOAPSERVICE_HPP__

//Jlib
#include "jiface.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"

//ESP Bindings
#include "SOAP/Platform/soapmessage.hpp"
#include "http/platform/httpservice.hpp"



class CSoapService : implements ISoapService, public CInterface
{
private:
    Owned<IEspSoapBinding> m_soapbinding;
    IEspContainer* m_container;

public:
    IMPLEMENT_IINTERFACE;

    CSoapService(IEspSoapBinding* soapbinding)
    {
        m_soapbinding.set(soapbinding);
        m_container = NULL;
    };
    //CSoapService(CHttpSoapBinding* httpsoapbinding){m_httpsoapbinding.set(httpsoapbinding);};
    virtual ~CSoapService() {};
    virtual int processRequest(ISoapMessage &req, ISoapMessage& resp);
    virtual int processHeader(CHeader* header, IEspContext* ctx);
    virtual int onPost() {return 0;};

    virtual void setContainer(IEspContainer *container){m_container = container;}
    IEspContainer * queryContainer(){return m_container;}
    
    virtual const char * getServiceType() {return "SeiSoap";};

    bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
   {
      return true;
   }

    bool subscribeServiceToDali() { return false; }
    bool unsubscribeServiceFromDali() { return false; }
    bool detachServiceFromDali() { return false; }
    bool attachServiceToDali() { return false; }
    bool canDetachFromDali() { return false; }
};


#endif
