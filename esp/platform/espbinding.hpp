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
    IPropertyTree *ensureNavFolder(IPropertyTree &root, const char *name, const char *tooltip, const char *menu=NULL, bool sort=false);
    IPropertyTree *ensureNavDynFolder(IPropertyTree &root, const char *name, const char *tooltip, const char *parms, const char *menu=NULL);
    IPropertyTree *addNavException(IPropertyTree &root, const char *message=NULL, int code=0, const char *source=NULL);

    IPropertyTree *ensureNavLink(IPropertyTree &folder, const char *name, const char *path, const char *tooltip, const char *menu=NULL, const char *navPath=NULL);
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
        DBGLOG("protocol is NULL");
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

};

#endif

