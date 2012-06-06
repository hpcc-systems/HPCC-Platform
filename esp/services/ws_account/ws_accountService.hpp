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

#include "ws_account_esp.ipp"

class Cws_accountSoapBindingEx : public Cws_accountSoapBinding
{
    StringBuffer m_authType;

public:
    Cws_accountSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : Cws_accountSoapBinding(cfg, name, process, llevel)
    {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/Authentication/@method", process);
        const char* method = cfg->queryProp(xpath);
        if (method && *method)
            m_authType.append(method);
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
#ifdef _USE_OPENLDAP
        bool isFF = false;
        StringBuffer browserUserAgent;
        context.getUseragent(browserUserAgent);
        if ((browserUserAgent.length() > 0) && strstr(browserUserAgent.str(), "Firefox"))
            isFF = true;

        IPropertyTree *folder = ensureNavFolder(data, "My Account", "My Account");

        const char* build_level = getBuildLevel();
        if (!stricmp(m_authType.str(), "none") || !stricmp(m_authType.str(), "local"))
        {
            ensureNavLink(*folder, "My Account", "/Ws_Access/SecurityNotEnabled?form_", "My Account", NULL, NULL, 0, true);//Force the menu to use this setting
            ensureNavLink(*folder, "Change Password", "/Ws_Access/SecurityNotEnabled?form_", "Change Password", NULL, NULL, 0, true);//Force the menu to use this setting
        }
        else
        {
            ensureNavLink(*folder, "My Account", "/Ws_Account/MyAccount", "MyAccount", NULL, NULL, 0, true);//Force the menu to use this setting
            ensureNavLink(*folder, "Change Password", "/Ws_Account/UpdateUserInput", "Change Password", NULL, NULL, 0, true);//Force the menu to use this setting
        }
#endif
    }
};

class Cws_accountEx : public Cws_account
{
public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

#ifdef _USE_OPENLDAP
    virtual bool onUpdateUser(IEspContext &context, IEspUpdateUserRequest &req, IEspUpdateUserResponse &resp);
    virtual bool onUpdateUserInput(IEspContext &context, IEspUpdateUserInputRequest &req, IEspUpdateUserInputResponse &resp);
    virtual bool onMyAccount(IEspContext &context, IEspMyAccountRequest &req, IEspMyAccountResponse &resp);
#else
    virtual bool onUpdateUser(IEspContext &context, IEspUpdateUserRequest &req, IEspUpdateUserResponse &resp) {return true;};
    virtual bool onUpdateUserInput(IEspContext &context, IEspUpdateUserInputRequest &req, IEspUpdateUserInputResponse &resp) {return true;};
    virtual bool onMyAccount(IEspContext &context, IEspMyAccountRequest &req, IEspMyAccountResponse &resp) {return true;};
#endif
    virtual bool onVerifyUser(IEspContext &context, IEspVerifyUserRequest &req, IEspVerifyUserResponse &resp);
};

#endif //_ESPWIZ_ws_account_HPP__

