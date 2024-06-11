/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems.

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

#ifndef _WsSYSINFOLOG_HPP_
#define _WsSYSINFOLOG_HPP_

#include "ws_sysinfolog_esp.ipp"
#include "sysinfologger.hpp"

class Cws_sysinfologEx : public Cws_sysinfolog
{
private:
    void populateMessageFromLoggerMsg(IEspSysInfoMessage* espMsg, IConstSysInfoLoggerMsg* loggerMsg);
    void parseFilters(IEspGetMessagesRequest &req, Owned<ISysInfoLoggerMsgFilter>& filter);
    bool parseDateTime(const char* dateTimeStr, timestamp_type& ts);

public:
    Cws_sysinfologEx();
    virtual ~Cws_sysinfologEx();

    virtual void init(IPropertyTree *cfg, const char *process, const char *service) override;

    virtual bool onGetMessages(IEspContext &context, IEspGetMessagesRequest &req, IEspGetMessagesResponse &resp) override;
    virtual bool onGetMessageByID(IEspContext &context, IEspGetMessageByIDRequest &req, IEspGetMessageByIDResponse &resp) override;
    virtual bool onHideMessage(IEspContext &context, IEspHideMessageRequest &req, IEspHideMessageResponse &resp) override;
    virtual bool onUnhideMessage(IEspContext &context, IEspUnhideMessageRequest &req, IEspUnhideMessageResponse &resp) override;
};

#endif
