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

#ifndef _ESPWIZ_ws_access_HPP__
#define _ESPWIZ_ws_access_HPP__

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
#include "ldapsecurity.ipp"

class Cws_accessSoapBinding : public CHttpSoapBinding
{
    StringBuffer m_authType, m_portalURL;
    Owned<IXslProcessor> xslp;

public:
    Cws_accessSoapBinding(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CHttpSoapBinding(cfg, name, process, llevel)
    {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/Authentication/@method", process);
        const char* method = cfg->queryProp(xpath);
        if (method && *method)
            m_authType.append(method);
        xpath.clear().appendf("Software/EspProcess[@name='%s']/@portalurl", process);
        const char* portalURL = cfg->queryProp(xpath.str());
        if (portalURL && *portalURL)
            m_portalURL.append(portalURL);
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        StringBuffer path = "/WsSMC/NotInCommunityEdition?form_";
        if (m_portalURL.length() > 0)
            path.appendf("&EEPortal=%s", m_portalURL.str());

        IPropertyTree *folder = ensureNavFolder(data, "Users/Permissions", NULL);
        ensureNavLink(*folder, "Users", path.str(), "Manage Users and permissions");
        ensureNavLink(*folder, "Groups", path.str(), "Manage Groups and permissions");
        ensureNavLink(*folder, "Permissions", path.str(), "Manage Permissions");
    }

    int getQualifiedNames(IEspContext& ctx, MethodInfoArray & methods)
    {
        return methods.ordinality();
    }
    void setXslProcessor(IInterface *xslp_){xslp.set(dynamic_cast<IXslProcessor *>(xslp_));}
};

class Cws_access : public CInterface,
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
    virtual const char* getServiceType(){return "ws_access";}
};

#endif //_ESPWIZ_ws_access_HPP__

