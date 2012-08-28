/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    }

    virtual int onGet(CHttpRequest* request, CHttpResponse* response);
    int sendRunEclExForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response);
};

#endif //_EclDirectService_HPP__
