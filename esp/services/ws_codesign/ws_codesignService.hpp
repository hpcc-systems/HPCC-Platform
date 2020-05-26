/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef _WS_CODESIGNSERVICE_HPP_
#define _WS_CODESIGNSERVICE_HPP_

#include "ws_codesign_esp.ipp"

class Cws_codesignEx : public Cws_codesign
{
private:
    Owned<IPropertyTree> m_serviceCfg;
    void clearPassphrase(const char* key);
public:
    IMPLEMENT_IINTERFACE

    Cws_codesignEx();
    virtual ~Cws_codesignEx();
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual bool onSign(IEspContext &context, IEspSignRequest &req, IEspSignResponse &resp);
    virtual bool onListUserIDs(IEspContext &context, IEspListUserIDsRequest &req, IEspListUserIDsResponse &resp);
};

#endif // _WS_CODESIGNSERVICE_HPP_
