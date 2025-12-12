/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems.

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

#pragma warning (disable : 4786)

#include "WsSysInfoLogService.hpp"
#include "exception_util.hpp"
#include "jptree.hpp"
#include "jtime.hpp"
#include "jmisc.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"
#include "daclient.hpp"
#include "jstring.hpp"

#ifdef _WIN32
#include <stdlib.h>
#else
#include <stdlib.h>
#define _strtoui64(str, endptr, base) strtoull(str, endptr, base)
#endif

Cws_sysinfologEx::Cws_sysinfologEx()
{
}

Cws_sysinfologEx::~Cws_sysinfologEx()
{
}

void Cws_sysinfologEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if(!daliClientActive())
    {
        ERRLOG("No Dali Connection Active.");
        return;
    }

    Cws_sysinfolog::init(cfg, process, service);
}

void Cws_sysinfologEx::populateMessageFromLoggerMsg(IEspSysInfoMessage* espMsg, IConstSysInfoLoggerMsg* loggerMsg)
{
    // Set message ID
    StringBuffer msgIdStr;
    msgIdStr.append(loggerMsg->queryLogMsgId());
    espMsg->setID(msgIdStr.str());

    // Set message text
    espMsg->setMessage(loggerMsg->queryMsg());

    // Convert LogMsgClass to MessageType using standard HPCC function
    LogMsgClass msgClass = loggerMsg->queryClass();
    espMsg->setType(LogMsgClassToVarString(msgClass));

    // Set component/source
    espMsg->setComponent(loggerMsg->querySource());

    // Set timestamp
    timestamp_type ts = loggerMsg->queryTimeStamp();
    CDateTime dt;
    dt.set(ts);
    StringBuffer timeStr;
    dt.getString(timeStr);
    espMsg->setCreatedAt(timeStr.str());

    // Set active status (opposite of hidden)
    espMsg->setIsActive(!loggerMsg->queryIsHidden());
}

void Cws_sysinfologEx::parseFilters(IEspGetMessagesRequest &req, Owned<ISysInfoLoggerMsgFilter>& filter)
{
    // Handle MessageID filter first (creates specific filter)
    const char* messageID = req.getMessageID();
    if (!isEmptyString(messageID))
    {
        unsigned __int64 msgId = _strtoui64(messageID, nullptr, 10);
        filter.setown(createSysInfoLoggerMsgFilter(msgId, nullptr));
    }
    else
    {
        filter.setown(createSysInfoLoggerMsgFilter(nullptr));
    }

    // Set component/source filter if provided
    const char* component = req.getComponent();
    if (!isEmptyString(component))
        filter->setMatchSource(component);

    // Set visibility filters
    if (req.getActiveOnly())
        filter->setVisibleOnly();
    else if (req.getHiddenOnly())
        filter->setHiddenOnly();

    // Values for date range filter
    unsigned startYear = 0, startMonth = 0, startDay = 0;
    unsigned endYear = 0, endMonth = 0, endDay = 0;
    bool hasDateRange = false;

    // Validate and set start date if provided
    if (req.getStartYear() > 0)
    {
        if (req.getStartMonth() <= 12 && req.getStartDay() >= 1 && req.getStartDay() <= 31)
        {
            startYear = req.getStartYear();
            startMonth = req.getStartMonth();
            startDay = req.getStartDay();
            hasDateRange = true;
        }
    }
    
    // Validate and set end date if provided
    if (req.getEndYear() > 0)
    {
        if (req.getEndMonth() <= 12 && req.getEndDay() >= 1 && req.getEndDay() <= 31)
        {
            endYear = req.getEndYear();
            endMonth = req.getEndMonth();
            endDay = req.getEndDay();
            hasDateRange = true;
        }
    }

    if (hasDateRange)
        filter->setDateRange(startYear, startMonth, startDay, endYear, endMonth, endDay);

    // Set specific timestamp matching
    const char* matchTimeStr = req.getMatchTimeStamp();
    if (!isEmptyString(matchTimeStr))
    {
        try
        {
            CDateTime dt;
            dt.setString(matchTimeStr);
            timestamp_type ts = dt.getSimple();
            filter->setMatchTimeStamp(ts);
        }
        catch (...)
        {
            // Invalid timestamp format, ignore
        }
    }

    // Convert MessageType to LogMsgClass
    CMessageType enumType = req.getType();
    if (enumType != MessageType_Undefined)
    {
        LogMsgClass msgClass = MSGCLS_unknown;
        switch (enumType)
        {
            case CMessageType_Disaster:
                msgClass = MSGCLS_disaster;
                break;
            case CMessageType_Error:
                msgClass = MSGCLS_error;
                break;
            case CMessageType_Warning:
                msgClass = MSGCLS_warning;
                break;
            case CMessageType_Information:
                msgClass = MSGCLS_information;
                break;
            case CMessageType_Progress:
                msgClass = MSGCLS_progress;
                break;
            case CMessageType_Metric:
                msgClass = MSGCLS_metric;
                break;
            case CMessageType_Event:
                msgClass = MSGCLS_event;
                break;
            default:
                msgClass = MSGCLS_unknown;
                break;
        }

        if (msgClass != MSGCLS_unknown)
            filter->setMatchMsgClass(msgClass);
    }
}

bool Cws_sysinfologEx::onGetMessages(IEspContext &context, IEspGetMessagesRequest &req, IEspGetMessagesResponse &resp)
{
    try
    {
        Owned<ISysInfoLoggerMsgFilter> filter;
        parseFilters(req, filter);

        Owned<ISysInfoLoggerMsgIterator> iter = createSysInfoLoggerMsgIterator(filter, false);

        IArrayOf<IEspSysInfoMessage> messages;
        int offset = req.getOffset();
        int limit = req.getLimit();
        int currentIndex = 0;

        ForEach(*iter)
        {
            if (currentIndex < offset)
            {
                currentIndex++;
                continue;
            }
            if (limit > 0 && messages.length() >= limit)
                break;

            ISysInfoLoggerMsg& loggerMsg = iter->query();
            Owned<IEspSysInfoMessage> msg = createSysInfoMessage();
            populateMessageFromLoggerMsg(msg, &loggerMsg);

            messages.append(*msg.getClear());
            currentIndex++;
        }
        resp.setMessages(messages);

        return true;
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
}

bool Cws_sysinfologEx::onGetMessageByID(IEspContext &context, IEspGetMessageByIDRequest &req, IEspGetMessageByIDResponse &resp)
{
    try
    {
        const char* messageIDStr = req.getMessageID();
        if (isEmptyString(messageIDStr))
            throw makeStringException(-1, "Message ID is required");

        unsigned __int64 messageID = _strtoui64(messageIDStr, nullptr, 10);

        Owned<ISysInfoLoggerMsgFilter> filter = createSysInfoLoggerMsgFilter(messageID, nullptr);
        Owned<ISysInfoLoggerMsgIterator> iter = createSysInfoLoggerMsgIterator(filter, false);

        if (iter->first())
        {
            ISysInfoLoggerMsg& loggerMsg = iter->query();
            Owned<IEspSysInfoMessage> msg = createSysInfoMessage();
            populateMessageFromLoggerMsg(msg, &loggerMsg);
            resp.setMessage(*msg.getClear());
        }
        else
        {
            // Return empty response instead of throwing exception
            // Client can check if Message is set to determine if found
        }

        return true;
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
}

bool Cws_sysinfologEx::onHideMessage(IEspContext &context, IEspHideMessageRequest &req, IEspHideMessageResponse &resp)
{
    try
    {
        const char* messageIDStr = req.getMessageID();
        if (isEmptyString(messageIDStr))
            throw makeStringException(-1, "Message ID is required");

        unsigned __int64 messageID = _strtoui64(messageIDStr, nullptr, 10);

        if (hideLogSysInfoMsg(messageID))
            resp.setStatus("Message hidden successfully");
        else
            resp.setStatus("Failed to hide message or message not found");

        return true;
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
}

bool Cws_sysinfologEx::onUnhideMessage(IEspContext &context, IEspUnhideMessageRequest &req, IEspUnhideMessageResponse &resp)
{
    try
    {
        const char* messageIDStr = req.getMessageID();
        if (isEmptyString(messageIDStr))
            throw makeStringException(-1, "Message ID is required");

        unsigned __int64 messageID = _strtoui64(messageIDStr, nullptr, 10);

        if (unhideLogSysInfoMsg(messageID))
            resp.setStatus("Message unhidden successfully");
        else
            resp.setStatus("Failed to unhide message or message not found");

        return true;
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
}
