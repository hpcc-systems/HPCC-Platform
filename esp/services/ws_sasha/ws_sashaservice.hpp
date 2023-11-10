/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems.

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

#ifndef _ESPWIZ_ws_sasha_HPP__
#define _ESPWIZ_ws_sasha_HPP__

#include "ws_sasha_esp.ipp"
#include "exception_util.hpp"
#include "jlib.hpp"
#include "TpWrapper.hpp"
#include "sacmd.hpp"

class CWSSashaEx : public CWSSasha
{
    ISashaCommand* setSashaCommand(SashaCommandAction action, bool dfu, SocketEndpoint& serverep);

public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree* cfg, const char* process, const char* service) override;

    virtual bool onGetVersion(IEspContext& context, IEspGetVersionRequest& req, IEspResultResponse& resp) override;
    virtual bool onListWU(IEspContext& context, IEspListWURequest& req, IEspResultResponse& resp) override;
    virtual bool onArchiveWU(IEspContext& context, IEspArchiveWURequest& req, IEspResultResponse& resp) override;
    virtual bool onRestoreWU(IEspContext& context, IEspRestoreWURequest& req, IEspResultResponse& resp) override;
    virtual bool onXREF(IEspContext& context, IEspXREFRequest& req, IEspResultResponse& resp) override;
};

#endif //_ESPWIZ_ws_sasha_HPP__
