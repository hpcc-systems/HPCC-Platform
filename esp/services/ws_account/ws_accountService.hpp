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

#ifndef _ESPWIZ_ws_account_HPP__
#define _ESPWIZ_ws_account_HPP__

#pragma warning( disable : 4786)

//JLib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"

//ESP Bindings
#include "SOAP/Platform/soapmessage.hpp"
#include "SOAP/Platform/soapmacro.hpp"
#include "SOAP/Platform/soapservice.hpp"
#include "SOAP/Platform/soapparam.hpp"
#include "SOAP/client/soapclient.hpp"

class Cws_accountSoapBinding : public CHttpSoapBinding
{
    StringBuffer m_authType, m_portalURL;
    Owned<IXslProcessor> xslp;
public:
    Cws_accountSoapBinding(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CHttpSoapBinding(cfg, name, process, llevel)
    {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/@portalurl", process);
        const char* portalURL = cfg->queryProp(xpath.str());
        if (portalURL && *portalURL)
            m_portalURL.append(portalURL);
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        bool isFF = false;
        StringBuffer browserUserAgent;
        context.getUseragent(browserUserAgent);
        if ((browserUserAgent.length() > 0) && strstr(browserUserAgent.str(), "Firefox"))
            isFF = true;

        IPropertyTree *folder = ensureNavFolder(data, "My Account", "My Account");
        StringBuffer path = "/WsSMC/NotInCommunityEdition?form_";
        if (m_portalURL.length() > 0)
            path.appendf("&EEPortal=%s", m_portalURL.str());

        ensureNavLink(*folder, "Change Password", path.str(), "Change Password");
        if (!isFF)
            ensureNavLink(*folder, "Relogin", path.str(), "Relogin");
        ensureNavLink(*folder, "Who Am I", path.str(), "WhoAmI");
    }

    int getQualifiedNames(IEspContext& ctx, MethodInfoArray & methods)
    {
        return methods.ordinality();
    }
    void setXslProcessor(IInterface *xslp_){xslp.set(dynamic_cast<IXslProcessor *>(xslp_));}
};


class Cws_account : public CInterface,
    implements IEspService
{
private:
    IEspContainer* m_container;

public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *cfg, const char *process, const char *service) {};
    virtual bool init(const char * service, const char * type, IPropertyTree * cfg, const char * process)
    {
        return true;
    }
    virtual void setContainer(IEspContainer *c)
    {
        m_container = c;
    }
    virtual IEspContainer *queryContainer()
    {
        return m_container;
    }
    virtual const char* getServiceType(){return "ws_account";}
};

#endif //_ESPWIZ_ws_account_HPP__

