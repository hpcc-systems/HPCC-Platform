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
            if (!isFF)
                ensureNavLink(*folder, "Relogin", "/Ws_Access/SecurityNotEnabled?form_", "Relogin", NULL, NULL, 0, true);//Force the menu to use this setting
            else
                ensureNavLink(*folder, "Relogin", "/Ws_Access/FirefoxNotSupport?form_", "Relogin", NULL, NULL, 0, true);//Force the menu to use this setting
        }
        else
        {
            ensureNavLink(*folder, "My Account", "/Ws_Account/MyAccount", "MyAccount", NULL, NULL, 0, true);//Force the menu to use this setting
            ensureNavLink(*folder, "Change Password", "/Ws_Account/UpdateUserInput", "Change Password", NULL, NULL, 0, true);//Force the menu to use this setting
            if (!isFF)
                ensureNavLink(*folder, "Relogin", "/Ws_Account/LogoutUser", "Relogin", NULL, NULL, 0, true);//Force the menu to use this setting
            else
                ensureNavLink(*folder, "Relogin", "/Ws_Access/FirefoxNotSupport?form_", "Relogin", NULL, NULL, 0, true);//Force the menu to use this setting
        }
#endif
    }

#ifdef _USE_OPENLDAP
    int onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
    {
        if(!stricmp(method, "LogoutUser")||!stricmp(method, "LogoutUserRequest"))
        {
            CEspCookie* logincookie = request->queryCookie("RELOGIN");
            if(logincookie == NULL || stricmp(logincookie->getValue(), "1") == 0)
            {
                response->addCookie(new CEspCookie("RELOGIN", "0"));

                StringBuffer content(
                "<html xmlns=\"http://www.w3.org/1999/xhtml\">"
                    "<head>"
                        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>"
                        "<title>Enterprise Services Platform</title>"
                    "</head>"
                    "<body onLoad=\"location.href='/ws_account/LogoutUserCancel'\">"
                    "</body>"
                "</html>");

                response->sendBasicChallenge("ESP", content.str());
            }
            else
            {
                response->addCookie(new CEspCookie("RELOGIN", "1"));
                response->setContentType("text/html; charset=UTF-8");
                StringBuffer content(
                "<html xmlns=\"http://www.w3.org/1999/xhtml\">"
                    "<head>"
                        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>"
                        "<title>Enterprise Services Platform</title>"
                    "</head>"
                    "<body>"
                    "<br/><b>Relogin successful, you're now logged in as ");
                content.append(context.queryUserId()).append(
                    "</b>"
                    "</body>"
                    "</html>");

                response->setContent(content.str());
                response->send();
            }

            return 0;
        }
        else if(!stricmp(method, "LogoutUserCancel")||!stricmp(method, "LogoutUserRequest"))
        {
            CEspCookie* logincookie = request->queryCookie("RELOGIN");
            response->addCookie(new CEspCookie("RELOGIN", "1"));
            response->setContentType("text/html; charset=UTF-8");
            StringBuffer content(
            "<html xmlns=\"http://www.w3.org/1999/xhtml\">"
                "<head>"
                    "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>"
                    "<title>Enterprise Services Platform</title>"
                    "<script type='text/javascript'>"
                        "function closeWin() { top.opener=top; top.close(); }"
                    "</script>"
                "</head>"
                "<body onload=\"javascript:closeWin();\">"
                    "<br/><b>Relogin canceled, you're now still logged in as ");
            content.append(context.queryUserId()).append(
                "</b>"
                "</body>"
            "</html>");

            response->setContent(content.str());
            response->send();
            return 0;
        }
        else
            return Cws_accountSoapBinding::onGetInstantQuery(context, request, response, service, method);
    }
#endif
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

