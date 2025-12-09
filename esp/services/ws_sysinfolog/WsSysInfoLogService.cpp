/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless isRequired by applicable law or agreed to in writing, software
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
#include <stdlib.h>

#ifndef _WIN32
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
    unsigned __int64 msgId = 0;
    if (getMessageIdFromReq(req, false, msgId))
        filter.setown(createSysInfoLoggerMsgFilter(msgId, nullptr));
    else
        filter.setown(createSysInfoLoggerMsgFilter(nullptr));

    const char* component = req.getComponent();
    if (!isEmptyString(component))
        filter->setMatchSource(component);

    if (req.getActiveOnly() && req.getHiddenOnly())
        throw makeStringException(-1, "Cannot set both ActiveOnly and HiddenOnly filters");

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
        if (req.getStartMonth() >= 1 && req.getStartMonth() <= 12 && req.getStartDay() >= 1 && req.getStartDay() <= 31)
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
        if (req.getEndMonth() >= 1 && req.getEndMonth() <= 12 && req.getEndDay() >= 1 && req.getEndDay() <= 31)
        {
            endYear = req.getEndYear();
            endMonth = req.getEndMonth();
            endDay = req.getEndDay();
            hasDateRange = true;
        }
    }

    if (hasDateRange) // there's additional validation in the setDateRange method
        filter->setDateRange(startYear, startMonth, startDay, endYear, endMonth, endDay);

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
            throw makeStringExceptionV(-1, "Timestamp is invalid: %s", matchTimeStr);
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
                throw makeStringExceptionV(-1, "Unknown MessageType %s", req.getTypeAsString());
        }

        if (msgClass != MSGCLS_unknown)
            filter->setMatchMsgClass(msgClass);
    }
}

template <typename ESPReqObj>
bool Cws_sysinfologEx::getMessageIdFromReq(ESPReqObj &req, bool isRequired, unsigned __int64 & messageId) const
{
    const char* messageIDStr = req.getMessageID();
    if (isEmptyString(messageIDStr))
    {
        if (isRequired)
            throw makeStringException(-1, "Message ID is required");
        return false;
    }

    unsigned __int64 messageID = _strtoui64(messageIDStr, nullptr, 10);
    if (messageID == 0)
    {
        if (isRequired)
            throw makeStringException(-1, "Invalid Message ID");
        return false;
    }
    
    messageId = messageID;
    return true;
}

bool Cws_sysinfologEx::onGetMessages(IEspContext &context, IEspGetMessagesRequest &req, IEspGetMessagesResponse &resp)
{
    IArrayOf<IEspSysInfoMessage> messages;
    try
    {
        Owned<ISysInfoLoggerMsgFilter> filter;
        parseFilters(req, filter);

        Owned<ISysInfoLoggerMsgIterator> iter = createSysInfoLoggerMsgIterator(filter, false);

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
            if (limit > 0 && messages.length() >= (unsigned int)limit)
                break;

            ISysInfoLoggerMsg& loggerMsg = iter->query();
            Owned<IEspSysInfoMessage> msg = createSysInfoMessage();
            populateMessageFromLoggerMsg(msg, &loggerMsg);

            messages.append(*msg.getClear());
            currentIndex++;
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    resp.setMessages(messages);

    return true;
}

bool Cws_sysinfologEx::onGetMessageByID(IEspContext &context, IEspGetMessageByIDRequest &req, IEspGetMessageByIDResponse &resp)
{
    try
    {
        unsigned __int64 messageID = 0;

        getMessageIdFromReq(req, true, messageID);

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
            throw makeStringExceptionV(-1, "Message with ID %llu not found", messageID);
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;}

bool Cws_sysinfologEx::onHideMessage(IEspContext &context, IEspHideMessageRequest &req, IEspHideMessageResponse &resp)
{
    try
    {
        unsigned __int64 messageID = 0;
        getMessageIdFromReq(req, true, messageID);

        if (hideLogSysInfoMsg(messageID))
            resp.setStatus("Message hidden successfully");
        else
            resp.setStatus("Failed to hide message or message not found");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    
    return true;
}

bool Cws_sysinfologEx::onUnhideMessage(IEspContext &context, IEspUnhideMessageRequest &req, IEspUnhideMessageResponse &resp)
{
    try
    {
        unsigned __int64 messageID = 0;
        getMessageIdFromReq(req, true, messageID);

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
