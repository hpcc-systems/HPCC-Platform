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

#ifndef _EclDirectBinding_HPP__
#define _EclDirectBinding_HPP__

#include "ecldirect_esp.ipp"
#include "environment.hpp"


typedef MapStringTo<int> DedupeList;


class CEclDirectSoapBindingEx : public CEclDirectSoapBinding
{
private:
    StringArray m_clusterNames;
    StringArray m_eclQueueNames;
    bool        m_useEclRepository;

public:
    CEclDirectSoapBindingEx()
    {
        initFromEnv();
    }
    
    CEclDirectSoapBindingEx(IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL):CEclDirectSoapBinding(cfg, bindname, procname)
    {
        initFromEnv();
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        IPropertyTree *folder = ensureNavFolder(data, "ECL", "Run Ecl code and review Ecl workunits", NULL, false, 2);
        ensureNavLink(*folder, "Run Ecl", "/EclDirect/RunEclEx?form_", "Submit ECL text for execution", NULL, NULL, 3);
    }

    int onGetXForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
    {
        return onGetForm(context, request, response, service, method);
    }

    virtual int onGet(CHttpRequest* request, CHttpResponse* response);

    void initFromEnv();

    int onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method);

    virtual int getMethodHelp(IEspContext &context, const char *serv, const char *method, StringBuffer &page);

    //overide the default behavior when the basic service method url is accessed (http://host:port/EclDirect/RunEcl)
    //      by default that is a query with no parameters, but we can just display the form
    //      instead of the user having to add "?form" to the end
    virtual int onGetService(IEspContext &context, CHttpRequest* request,   CHttpResponse* response, const char *serv, const char *method, const char *pathex);
    int getWsdlMessages(IEspContext &context, StringBuffer &content, const char *service, const char *method);
    int getWsdlBindings(IEspContext &context, StringBuffer &content, const char *service, const char *method);
};

#endif //_EclDirectBinding_HPP__

