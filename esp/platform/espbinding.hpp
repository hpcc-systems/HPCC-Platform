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

#ifndef _ESPBINDING_HPP__
#define _ESPBINDING_HPP__

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
//#include "IAEsp.hpp"
#include "soapesp.hpp"

#include "espprotocol.hpp"
#include "../bindings/http/platform/httpbinding.hpp"

class esp_http_decl CEspBinding : public CInterface,
    implements IEspSoapBinding
{
protected:
    IEspProtocol* m_protocol;
    Owned<IEspService> m_service;
    IEspContainer* m_container;

public:
    IMPLEMENT_IINTERFACE;
    
    CEspBinding()
    {
        m_container = NULL;
        //m_service = NULL;
        m_protocol = NULL;
    }
    virtual ~CEspBinding(){}

    virtual const char * getRpcType(){return "Unknown";}
    virtual const char * getTransportType(){return "Unknown";}


    IPropertyTree *ensureNavMenu(IPropertyTree &root, const char *name);
    IPropertyTree *ensureNavMenuItem(IPropertyTree &root, const char *name, const char *tooltip, const char *action);
    IPropertyTree *ensureNavFolder(IPropertyTree &root, const char *name, const char *tooltip, const char *menu=NULL, bool sort=false, unsigned relPosition = 0);
    IPropertyTree *ensureNavDynFolder(IPropertyTree &root, const char *name, const char *tooltip, const char *parms, const char *menu=NULL);
    IPropertyTree *addNavException(IPropertyTree &root, const char *message=NULL, int code=0, const char *source=NULL);

    IPropertyTree *ensureNavLink(IPropertyTree &folder, const char *name, const char *path, const char *tooltip, const char *menu=NULL, const char *navPath=NULL, unsigned relPosition = 0, bool force = false);
    virtual void getNavigationData(IEspContext &context, IPropertyTree & data);
    virtual void getDynNavData(IEspContext &context, IProperties *params, IPropertyTree & data);
    virtual bool showSchemaLinks() { return false; }
    virtual void addService(const char * name, const char * host, unsigned short port, IEspService & service)
    {
        m_service.set(&service);
    }

    virtual void addProtocol(const char * name, IEspProtocol & prot)
    {
        m_protocol = &prot;
    }

    virtual ISocketSelectNotify * queryListener()
    {
        if (m_protocol)
            return dynamic_cast<ISocketSelectNotify*>(m_protocol);
        IERRLOG("protocol is NULL");
        return NULL;
    }

    virtual int run(){return 0;}
    virtual int stop(){return 0;}

    virtual void setContainer(IEspContainer * ic)
    {
        m_container = ic;
    }

    IEspContainer *queryContainer(IEspContainer * ic)
    {
        return m_container;
    }

    virtual int processRequest(IRpcMessage* rpc_call, IRpcMessage* rpc_response)
    {
        return 0;
    }

    virtual IEspService *getService()
    {
        return m_service.getLink();
    }

    virtual bool isValidServiceName(IEspContext & context, const char * name){return false;}
    virtual bool qualifyServiceName(IEspContext & context, const char * servname, const char * methname, StringBuffer & servQName, StringBuffer * methQName){return false;}
    virtual IRpcRequestBinding *createReqBinding(IEspContext &context, IHttpMessage *request, const char *service, const char *method){return NULL;}
    virtual bool isDynamicBinding() const { return false; }
    virtual bool isBound() const { return false; }
};

#endif

