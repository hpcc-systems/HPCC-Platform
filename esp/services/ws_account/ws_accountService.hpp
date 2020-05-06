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
        if (queryComponentConfig().getPropBool("@api_only"))
        {
            CHttpSoapBinding::getNavigationData(context, data);
            return;
        }
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

