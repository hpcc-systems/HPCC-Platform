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

#ifndef _EclDirectService_HPP__
#define _EclDirectService_HPP__

#include "ecldirect_esp.ipp"

class CEclDirectEx : public CEclDirect
{
private:
    StringBuffer defaultCluster;
    int defaultWait;
    bool deleteWorkunits;
    BoolHash validClusters;
    CriticalSection crit;

public:
   IMPLEMENT_IINTERFACE;

    CEclDirectEx() : defaultWait(0){}

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    void refreshValidClusters();
    bool isValidCluster(const char *cluster);

    bool onRunEcl(IEspContext &context, IEspRunEclRequest &req, IEspRunEclResponse &resp);
    bool onRunEclEx(IEspContext &context, IEspRunEclExRequest &req, IEspRunEclExResponse &resp);
};

class CEclDirectSoapBindingEx : public CEclDirectSoapBinding
{
private:
    CEclDirectEx *theService;

    StringArray clusters;
    bool supportRepository;
    bool redirect;

public:
    CEclDirectSoapBindingEx(IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL);

    void addService(const char *name, const char *host, unsigned short port, IEspService &service)
    {
        theService = dynamic_cast<CEclDirectEx*>(&service);
        CEspBinding::addService(name, host, port, service);
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        IPropertyTree *folder = ensureNavFolder(data, "ECL", "Run Ecl code and review Ecl workunits", NULL, false, 2);
        ensureNavLink(*folder, "Run Ecl", "/EclDirect/RunEclEx/Form", "Submit ECL text for execution", NULL, NULL, 3);
        ensureNavLink(*folder, "ECL Playground", "/esp/files/ECLPlayground.htm", "ECL Editor, Executor, Graph and Result Viewer", NULL, NULL, 4);
    }

    virtual int onGet(CHttpRequest* request, CHttpResponse* response);
    int sendRunEclExForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response);
};

#endif //_EclDirectService_HPP__
