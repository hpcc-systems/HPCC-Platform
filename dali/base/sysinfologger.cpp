/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#include "sysinfologger.hpp"
#include "jutil.hpp"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5 minutes
#define SYS_INFO_VERSION "1.0"

#define SYS_INFO_ROOT  "/SysLogs"
#define ATTR_NEXTSEQN  "@nextSeqNum"
#define ATTR_VERSION   "@version"
#define MSG_NODE       "msg"
#define ATTR_SEQNUM    "@seqNum"
#define ATTR_TIMESTAMP "@ts"
#define ATTR_AUDIENCE  "@audience"
#define ATTR_CLASS     "@class"
#define ATTR_CODE      "@code"
#define ATTR_HIDDEN    "@hidden"
#define ATTR_SOURCE    "@source"

static constexpr unsigned __int64 nonSysInfoLogMsgMask = 0x80000000;

static void extractDate(timestamp_type ts, unsigned & year, unsigned & month, unsigned & day)
{
    CDateTime timeStamp;
    timeStamp.setTimeStamp(ts);
    timeStamp.getDate(year, month, day);
}

unsigned __int64 makeMessageId(unsigned year, unsigned month, unsigned day, unsigned seqN, bool nonSysInfoLogMsg)
{
    // bits 0-4 = day, bits 5-8 = month, bits 9-22 = year
    // bit 23-30 unused
    // bit 31 = (flag) unset means SysInfoLogger managed message, set means message managed elsewhere
    // bits 32-63 = sequence number
    return ((unsigned __int64) seqN)<<32 | (nonSysInfoLogMsg?nonSysInfoLogMsgMask:0) | year<<9 | month<<5 | day;
}

unsigned __int64 makeMessageId(unsigned __int64 ts, unsigned seqN, bool nonSysInfoLogMsg)
{
    unsigned year, month, day;
    extractDate(ts, year, month, day);
    return makeMessageId(year, month, day, seqN, nonSysInfoLogMsg);
}

static bool decodeMessageId(unsigned __int64 msgId, unsigned & year, unsigned & month, unsigned & day, unsigned & seqn)
{
    day = msgId & 0x1F;
    month = (msgId>>5) & 0x0F;
    year = (msgId>>9) & 0x3FFF;
    seqn = msgId>>32;
    return msgId & nonSysInfoLogMsgMask;
}

class CSysInfoLoggerMsg : implements ISysInfoLoggerMsg
{
    Owned<IPropertyTree> msgPtree;
    bool updateable = false;

    inline void ensureUpdateable()
    {
        if (!updateable)
            throw makeStringException(-1, "Unable to update ISysInfoLoggerMsg");
    }

public:
    CSysInfoLoggerMsg()
    {
        msgPtree.setown(createPTree(MSG_NODE));
    }
    CSysInfoLoggerMsg(unsigned seqn, const LogMsgCategory & cat, LogMsgCode code, const char * source, const char * msg, timestamp_type ts, bool hidden)
    {
        msgPtree.setown(createPTree(MSG_NODE));
        msgPtree->setPropInt64(ATTR_SEQNUM, seqn);
        msgPtree->setPropBool(ATTR_HIDDEN, hidden);
        msgPtree->setPropInt64(ATTR_TIMESTAMP, ts);
        msgPtree->setPropInt(ATTR_CODE, code);
        msgPtree->setProp(ATTR_AUDIENCE, LogMsgAudienceToFixString(cat.queryAudience()));
        msgPtree->setProp(ATTR_CLASS, LogMsgClassToFixString(cat.queryClass()));
        msgPtree->setProp(ATTR_SOURCE, source);
        msgPtree->setProp(".", msg);
    }
    CSysInfoLoggerMsg & set(IPropertyTree * ptree, bool _updateable)
    {
        msgPtree.setown(ptree);
        updateable = _updateable;
        return * this;
    }
    virtual bool queryIsHidden() const override
    {
        return msgPtree->getPropBool(ATTR_HIDDEN, false);
    }
    virtual timestamp_type queryTimeStamp() const override
    {
        return msgPtree->getPropInt64(ATTR_TIMESTAMP);
    }
    virtual const char * querySource() const override
    {
        if (msgPtree->hasProp(ATTR_SOURCE))
            return msgPtree->queryProp(ATTR_SOURCE);
        else
            return "Unknown";
    }
    virtual LogMsgCode queryLogMsgCode() const override
    {
        return msgPtree->getPropInt(ATTR_CODE, -1);
    }
    virtual LogMsgAudience queryAudience() const override
    {
        if (msgPtree->hasProp(ATTR_AUDIENCE))
            return LogMsgAudFromAbbrev(msgPtree->queryProp(ATTR_AUDIENCE));
        else
            return MSGAUD_unknown;
    }
    virtual LogMsgClass queryClass() const override
    {
        if (msgPtree->hasProp(ATTR_CLASS))
            return LogMsgClassFromAbbrev(msgPtree->queryProp(ATTR_CLASS));
        else
            return MSGCLS_unknown;
    }
    virtual unsigned __int64 queryLogMsgId() const override
    {
        return makeMessageId(queryTimeStamp(), msgPtree->getPropInt64(ATTR_SEQNUM, 0));
    }
    virtual const char * queryMsg() const override
    {
        const char *msg = msgPtree->queryProp(nullptr);
        return msg ? msg : "";
    }
    virtual void setHidden(bool _hidden) override
    {
        ensureUpdateable();
        msgPtree->setPropBool(ATTR_HIDDEN, _hidden);
    }
    virtual StringBuffer & getXpath(StringBuffer & xpath) const override
    {
        unsigned year, month, day;
        extractDate(queryTimeStamp(), year, month, day);
        unsigned __int64 seqn = msgPtree->getPropInt64(ATTR_SEQNUM, 0);
        xpath.appendf("m%04u%02u/d%02u/" MSG_NODE "[" ATTR_SEQNUM "='%" I64F "u']", year, month, day, seqn);
        return xpath;
    }
    IPropertyTree * getTree()
    {
        return msgPtree.getLink();
    }
};

class CSysInfoLoggerMsgFilter : public CSimpleInterfaceOf<ISysInfoLoggerMsgFilter>
{
    // (For numeric fields: match only if it has a non-zero value)
    bool hiddenOnly = false;
    bool visibleOnly = false;
    timestamp_type matchTimeStamp = 0;
    StringAttr matchSource; // only matchSource when not empty
    LogMsgCode matchCode = 0;
    LogMsgAudience matchAudience = MSGAUD_all;
    LogMsgClass matchClass = MSGCLS_all;
    bool haveDateRange = false;
    unsigned matchEndYear = 0;
    unsigned matchEndMonth = 0;
    unsigned matchEndDay = 0;
    unsigned matchStartYear = 0;
    unsigned matchStartMonth = 0;
    unsigned matchStartDay = 0;
    unsigned matchId = 0;

public:
    CSysInfoLoggerMsgFilter(const char *_source): matchSource(_source)
    {
    }
    CSysInfoLoggerMsgFilter(unsigned __int64 msgId, const char *_source): matchSource(_source)
    {
        setMatchMsgId(msgId);
    }
    CSysInfoLoggerMsgFilter(bool _visibleOnly, bool _hiddenOnly, unsigned _year, unsigned _month, unsigned _day, const char *_source) :
                            hiddenOnly(_hiddenOnly), visibleOnly(_visibleOnly), matchSource(_source)
    {
        if (hiddenOnly && visibleOnly)
            throw makeStringException(-1, "ISysInfoLoggerMsgFilter: cannot filter by both hiddenOnly and visibleOnly");
        setDateRange(_year, _month, _day, _year, _month, _day);
    }
    virtual void setHiddenOnly() override
    {
        hiddenOnly = true;
    }
    virtual void setVisibleOnly() override
    {
        visibleOnly = true;
    }
    virtual void setMatchTimeStamp(timestamp_type ts) override
    {
        matchTimeStamp = ts;
    }
    virtual void setMatchSource(const char * source) override
    {
        matchSource.set(source);
    }
    virtual void setMatchCode(LogMsgCode code) override
    {
        matchCode = code;
    }
    virtual void setMatchAudience(LogMsgAudience audience) override
    {
        matchAudience = audience;
    }
    virtual void setMatchMsgClass(LogMsgClass msgClass) override
    {
        matchClass = msgClass;
    }
    virtual void setMatchMsgId(unsigned __int64 msgId) override
    {
        unsigned year, month, day, seqn;
        bool nonSysInfoLogMsg = decodeMessageId(msgId, year, month, day, seqn);
        if (nonSysInfoLogMsg)
            throw makeStringExceptionV(-1, "Message id: %" I64F "u cannot be processed by SysInfoLogger", msgId);
        if (year==0 || month==0 || day==0 || seqn==0)
            throw makeStringExceptionV(-1,"ISysInfoLoggerMsgFilter::setMatchMsgId invalid message id: %" I64F "u", msgId);
        matchEndYear = matchStartYear = year;
        matchEndMonth = matchStartMonth = month;
        matchEndDay = matchStartDay = day;
        matchId = seqn;
        haveDateRange = false;
    }
    virtual void setDateRange(unsigned startYear, unsigned startMonth, unsigned startDay, unsigned endYear, unsigned endMonth, unsigned endDay) override
    {
        if ( (startDay && (!startMonth||!startYear)) ||
             (endDay && (!endMonth||!endYear)) )
            throw makeStringException(-1, "ISysInfoLoggerMsgFilter: month and year must be provided when filtering by day");
        if ((!startYear && startMonth) || (!endYear && endMonth))
            throw makeStringException(-1, "ISysInfoLoggerMsgFilter: year must be provided when filtering by month");
        // Make sure starts are on or before end dates
        if ( (startYear > endYear) || (startMonth && (startYear == endYear && startMonth > endMonth))
             || (startDay && (startYear == endYear && startMonth == endMonth && startDay > endDay)) )
            throw makeStringExceptionV(-1, "ISysInfoLoggerMsgFilter: invalid date range: %04u-%02u-%02u to %04u-%02u-%02u", startYear, startMonth, startDay, endYear, endMonth, endDay);
        matchEndYear = endYear;
        matchEndMonth = endMonth;
        matchEndDay = endDay;
        matchStartYear = startYear;
        matchStartMonth = startMonth;
        matchStartDay = startDay;
        if (matchEndYear||matchStartYear)
            haveDateRange = (matchStartYear<matchEndYear)||(matchStartMonth<matchEndMonth)||(matchStartDay<matchEndDay);
        else
            haveDateRange = false;
    }
    virtual void setOlderThanDate(unsigned year, unsigned month, unsigned day) override
    {
        assert(year);
        matchEndYear = year;
        matchEndMonth = month;
        matchEndDay = day;
        matchStartYear = 0;
        matchStartMonth = 0;
        matchStartDay = 0;
        haveDateRange = true;
    }
    virtual bool hasDateRange() const override
    {
        return haveDateRange;
    }
    virtual bool isInDateRange(timestamp_type ts) const override
    {
        unsigned tyear, tmonth, tday;
        extractDate(ts, tyear, tmonth, tday);

        if (matchStartYear)
        {
            if (tyear<matchStartYear)
                return false;
            if (tyear==matchStartYear)
            {
                if (matchStartMonth)
                {
                    if (tmonth<matchStartMonth)
                        return false;
                    if (tmonth==matchStartMonth)
                    {
                        if (matchStartDay && tday<matchStartDay)
                            return false;
                    }
                }
            }
        }
        if (matchEndYear)
        {
            if (tyear>matchEndYear)
                return false;
            if (tyear==matchEndYear)
            {
                if (matchEndMonth)
                {
                    if (tmonth>matchEndMonth)
                        return false;
                    if (tmonth==matchEndMonth)
                    {
                        if (matchEndDay && tday>matchEndDay)
                            return false;
                    }
                }
            }
        }
        return true;
    }
    virtual bool queryHiddenOnly() const override
    {
        return hiddenOnly;
    }
    virtual bool queryVisibleOnly() const override
    {
        return visibleOnly;
    }
    virtual timestamp_type queryMatchTimeStamp() const override
    {
        return matchTimeStamp;
    }
    virtual unsigned queryStartYear() const override
    {
        return matchStartYear;
    }
    virtual unsigned queryStartMonth() const override
    {
        return matchStartMonth;
    }
    virtual unsigned queryStartDay() const override
    {
        return matchStartDay;
    }
    virtual unsigned queryEndYear() const override
    {
        return matchEndYear;
    }
    virtual unsigned queryEndMonth() const override
    {
        return matchEndMonth;
    }
    virtual unsigned queryEndDay() const override
    {
        return matchEndDay;
    }
    virtual const char * queryMatchSource() const override
    {
        return matchSource.str();
    }
    virtual LogMsgCode queryMatchCode() const override
    {
        return matchCode;
    }
    virtual LogMsgAudience queryMatchAudience() const override
    {
        return matchAudience;
    }
    virtual LogMsgClass queryMatchClass() const override
    {
        return matchClass;
    }
    virtual StringBuffer & getQualifierXPathFilter(StringBuffer & xpath) const override
    {
        bool fullDayMatch=false;
        bool hardMatchYear = matchStartYear && (matchStartYear==matchEndYear);
        bool hardMatchMonth = matchStartMonth && (matchStartMonth==matchEndMonth);
        if (hardMatchYear && hardMatchMonth)
        {
            // future: optimize when month unknown with "m%04u*"
            xpath.appendf("m%04u%02u", matchStartYear, matchStartMonth);
            if (matchStartDay==matchEndDay)
            {
                xpath.appendf("/d%02u", matchStartDay);
                fullDayMatch = true;
            }
        }
        if (fullDayMatch)
            xpath.appendf("/" MSG_NODE);
        else
            xpath.appendf("//" MSG_NODE);
        if (hiddenOnly)
            xpath.append("[" ATTR_HIDDEN "='1')]");
        if (visibleOnly)
            xpath.append("[" ATTR_HIDDEN "='0')]");
        if (!matchSource.isEmpty())
            xpath.appendf("[" ATTR_SOURCE "='%s']", matchSource.str());
        if (matchCode)
            xpath.appendf("[" ATTR_CODE "='%d']", (int)matchCode);
        if (matchAudience!=MSGAUD_all)
            xpath.appendf("[" ATTR_AUDIENCE "='%s']", LogMsgAudienceToFixString(matchAudience));
        if (matchClass!=MSGCLS_all)
            xpath.appendf("[" ATTR_CLASS "='%s']", LogMsgClassToFixString(matchClass));
        if (matchId)
            xpath.appendf("[" ATTR_SEQNUM "='%u']", matchId);
        if (matchTimeStamp)
            xpath.appendf("[" ATTR_TIMESTAMP "='%" I64F "u']", matchTimeStamp);
        return xpath;
    }
};

ISysInfoLoggerMsgFilter * createSysInfoLoggerMsgFilter(const char *source)
{
    return new CSysInfoLoggerMsgFilter(source);
}

ISysInfoLoggerMsgFilter * createSysInfoLoggerMsgFilter(unsigned __int64 msgId, const char *source)
{
    return new CSysInfoLoggerMsgFilter(msgId, source);
}

class CSysInfoLoggerMsgIterator : public CSimpleInterfaceOf<ISysInfoLoggerMsgIterator>
{
    // N.b. IRemoteConnection exists for the duration of the iterator so if this iterator exists for too long, it could cause
    // performance issues for other clients:  consider caching some messages and releasing connection (and reopening as necessary).
    Linked<IRemoteConnection> conn;
    Linked<IConstSysInfoLoggerMsgFilter> filter;
    bool updateable = false;
    Owned<IPropertyTreeIterator> msgIter;
    CSysInfoLoggerMsg infoMsg;

    bool ensureMatch()
    {
        if (filter->hasDateRange())
        {
            for (; msgIter->isValid(); msgIter->next())
            {
                timestamp_type ts = msgIter->query().getPropInt64(ATTR_TIMESTAMP, 0);
                if (filter->isInDateRange(ts))
                    return true;
            }
            return false;
        }
        return msgIter->isValid();
    }

public:
    CSysInfoLoggerMsgIterator(IRemoteConnection *_conn, IConstSysInfoLoggerMsgFilter * _filter, bool _updateable = false) : conn(_conn), filter(_filter), updateable(_updateable)
    {
    }
    CSysInfoLoggerMsg & queryInfoLoggerMsg()
    {
        return infoMsg.set(&(msgIter->get()), updateable);
    }
    virtual ISysInfoLoggerMsg & query() override
    {
        return queryInfoLoggerMsg();
    }
    virtual bool first() override
    {
        StringBuffer xpath;
        filter->getQualifierXPathFilter(xpath);

        msgIter.setown(conn->queryRoot()->getElements(xpath.str()));
        if (!msgIter->first())
            return false;
        return ensureMatch();
    }
    virtual bool next() override
    {
        if (!msgIter->next())
            return false;
        return ensureMatch();
    }
    virtual bool isValid() override
    {
        return msgIter ? msgIter->isValid() : false;
    }
};
class NullSysInfoLoggerMsgIterator : public CSimpleInterfaceOf<ISysInfoLoggerMsgIterator>
{
public:
    virtual ISysInfoLoggerMsg & query() override
    {
        UNIMPLEMENTED;
    }
    virtual bool first() override
    {
        return false;
    }
    virtual bool next() override
    {
        return false;
    }
    virtual bool isValid() override
    {
        return false;
    }
};

ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(IRemoteConnection * conn, IConstSysInfoLoggerMsgFilter * msgFilter, bool updateable)
{
    return new CSysInfoLoggerMsgIterator(conn, msgFilter, updateable);
}

ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(IConstSysInfoLoggerMsgFilter * msgFilter, bool updateable)
{
    unsigned mode = updateable ? RTM_LOCK_WRITE : RTM_LOCK_READ;
    Owned<IRemoteConnection> conn = querySDS().connect(SYS_INFO_ROOT, myProcessSession(), mode, SDS_LOCK_TIMEOUT);
    if (!conn)
        return new NullSysInfoLoggerMsgIterator();
    return createSysInfoLoggerMsgIterator(conn, msgFilter, updateable);
}

ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(IRemoteConnection * conn, bool visibleOnly, bool hiddenOnly, unsigned year, unsigned month, unsigned day, const char *source, bool updateable)
{
    Owned<CSysInfoLoggerMsgFilter> filter = new CSysInfoLoggerMsgFilter(visibleOnly, hiddenOnly, year, month, day, source);
    return createSysInfoLoggerMsgIterator(conn, filter, updateable);
}

ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(bool visibleOnly, bool hiddenOnly, unsigned year, unsigned month, unsigned day, const char *source, bool updateable)
{
    Owned<CSysInfoLoggerMsgFilter> filter = new CSysInfoLoggerMsgFilter(visibleOnly, hiddenOnly, year, month, day, source);
    return createSysInfoLoggerMsgIterator(filter, updateable);
}


// returns messageId
unsigned __int64 logSysInfoError(const LogMsgCategory & cat, LogMsgCode code, const char *source, const char * msg, timestamp_type ts)
{
    if (ts==0)
        ts = getTimeStampNowValue();

    if (isEmptyString(source))
        source = "unknown";
    Owned<IRemoteConnection> conn = querySDS().connect(SYS_INFO_ROOT, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw makeStringExceptionV(-1, "logSysInfoLogger: unable to create connection to '%s'", SYS_INFO_ROOT);

    IPropertyTree * root = conn->queryRoot();
    unsigned seqn = root->getPropInt(ATTR_NEXTSEQN, 1);
    if (seqn==UINT_MAX)  // wrap id to reuse id numbers (shouldn't wrap but no harm in doing this for safety)
        seqn=1;
    root->setPropInt(ATTR_NEXTSEQN, seqn+1);

    StringBuffer xpath;
    unsigned year, month, day;
    extractDate(ts, year, month, day);
    xpath.appendf("%s/m%04u%02u/d%02u/%s", SYS_INFO_ROOT, year, month, day, MSG_NODE);
    Owned<IRemoteConnection> connMsgRoot = querySDS().connect(xpath.str(), myProcessSession(), RTM_CREATE_ADD, SDS_LOCK_TIMEOUT);
    if (!connMsgRoot)
        throw makeStringExceptionV(-1, "logSysInfoLogger: unable to create connection to '%s'", xpath.str());
    IPropertyTree * msgPT = connMsgRoot->queryRoot();

    CSysInfoLoggerMsg sysInfoMsg(seqn, cat, code, source, msg, ts, false);
    msgPT->setPropTree(nullptr, sysInfoMsg.getTree());
    msgPT->setProp(".", msg); // previous setPropTree doesn't set the node value
    return makeMessageId(ts, seqn);
}

unsigned updateMessage(IConstSysInfoLoggerMsgFilter * msgFilter, std::function<void (ISysInfoLoggerMsg &)> updateOp)
{
    unsigned count = 0;
    Owned<ISysInfoLoggerMsgIterator> iter = createSysInfoLoggerMsgIterator(msgFilter, true);
    ForEach(*iter)
    {
        ISysInfoLoggerMsg & sysInfoMsg = iter->query();
        updateOp(sysInfoMsg);
        ++count;
    }
    return count;
}

unsigned updateMessage(unsigned __int64 msgId, const char *source, std::function<void (ISysInfoLoggerMsg &)> updateOp)
{
    Owned<IConstSysInfoLoggerMsgFilter> msgFilter = createSysInfoLoggerMsgFilter(msgId, source);
    return updateMessage(msgFilter, updateOp);
}

unsigned hideLogSysInfoMsg(IConstSysInfoLoggerMsgFilter * msgFilter)
{
    return updateMessage(msgFilter, [](ISysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.setHidden(true);});
}

bool hideLogSysInfoMsg(unsigned __int64 msgId, const char *source)
{
    return updateMessage(msgId, source, [](ISysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.setHidden(true);})==1;
}

unsigned unhideLogSysInfoMsg(IConstSysInfoLoggerMsgFilter * msgFilter)
{
    return updateMessage(msgFilter, [](ISysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.setHidden(false);});
}

bool unhideLogSysInfoMsg(unsigned __int64 msgId, const char *source)
{
    return updateMessage(msgId, source, [](ISysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.setHidden(false);})==1;
}

unsigned deleteLogSysInfoMsg(IConstSysInfoLoggerMsgFilter * msgFilter)
{
    std::vector<std::string> deleteXpathList;
    Owned<IRemoteConnection> conn = querySDS().connect(SYS_INFO_ROOT, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    {
        Owned<ISysInfoLoggerMsgIterator> iter = createSysInfoLoggerMsgIterator(conn, msgFilter, false);
        ForEach(*iter)
        {
            IConstSysInfoLoggerMsg & sysInfoMsg = iter->query();
            StringBuffer xpath;
            sysInfoMsg.getXpath(xpath);
            deleteXpathList.push_back(xpath.str());
        }
    }
    IPropertyTree * root = conn->queryRoot();
    unsigned count = 0;
    for (auto & xpath: deleteXpathList)
    {
        if (root->removeProp(xpath.c_str()))
            ++count;
    }
    return count;
}

bool deleteLogSysInfoMsg(unsigned __int64 msgId, const char *source)
{
    Owned<ISysInfoLoggerMsgFilter> msgFilter = createSysInfoLoggerMsgFilter(msgId, source);
    return deleteLogSysInfoMsg(msgFilter);
}

unsigned deleteOlderThanLogSysInfoMsg(bool visibleOnly, bool hiddenOnly, unsigned year, unsigned month, unsigned day, const char *source)
{
    if (!year && month)
        throw makeStringExceptionV(-1, "deleteOlderThanLogSysInfoMsg: year must be provided if month is specified (year=%u, month=%u, day=%u)", year, month, day);
    if (!month && day)
        throw makeStringExceptionV(-1, "deleteOlderThanLogSysInfoMsg: month must be provided if day is specified (year=%u, month=%u, day=%u)", year, month, day);
    if (month>12)
        throw makeStringExceptionV(-1, "deleteOlderThanLogSysInfoMsg: invalid month(year=%u, month=%u, day=%u)", year, month, day);
    if (day>31)
        throw makeStringExceptionV(-1, "deleteOlderThanLogSysInfoMsg: invalid day(year=%u, month=%u, day=%u)", year, month, day);
    // With visibleOnly/hiddenOnly option, use createSysInfoLoggerMsgFilter()
    if (visibleOnly || hiddenOnly || day)
    {
        Owned<ISysInfoLoggerMsgFilter> msgFilter = createSysInfoLoggerMsgFilter(source);
        if (hiddenOnly)
            msgFilter->setHiddenOnly();
        if (visibleOnly)
            msgFilter->setVisibleOnly();
        if (source)
            msgFilter->setMatchSource(source);
        msgFilter->setOlderThanDate(year, month, day);
        return deleteLogSysInfoMsg(msgFilter);
    }

    // With only date range, use this quicker method to remove whole subtrees
    Owned<IRemoteConnection> conn = querySDS().connect(SYS_INFO_ROOT, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        return 0;

    std::vector<std::string> deleteXpathList;
    IPropertyTree * root = conn->queryRoot();
    // future: optimize by getting only minimum set of subtrees to delete and get sorted elements(so search can stop earlier)
    Owned<IPropertyTreeIterator> monthIter = root->getElements("*");
    Owned<IException> innerException; //only first exception record/reported
    ForEach(*monthIter)
    {
        IPropertyTree & monthPT = monthIter->query();
        if (year==0)
            deleteXpathList.push_back(monthPT.queryName());
        else
        {
            unsigned msgYear = 0, msgMonth = 0;
            const char *p = monthPT.queryName(); // should be in format 'myyyydd'
            if (*p++ == 'm')
            {
                msgYear = readDigits(p, 4, false);
                msgMonth = readDigits(p, 2, false);
            }
            if (msgYear == 0 || msgMonth == 0)
            {
                if (!innerException)
                    innerException.setown(makeStringExceptionV(-1, "child of " SYS_INFO_ROOT " is invalid: %s", monthPT.queryName()));
                continue;
            }
            if (msgYear > year)
                continue;
            if (msgYear < year)
                deleteXpathList.push_back(monthPT.queryName());
            else
            {
                // msgYear matches year in this section
                if (msgMonth > month)
                    continue;
                else if (msgMonth < month)
                    deleteXpathList.push_back(monthPT.queryName());
                else // msgMonth==month
                {
                    Owned<IPropertyTreeIterator> dayIter = monthPT.getElements("*");
                    ForEach(*dayIter)
                    {
                        IPropertyTree & dayPT = dayIter->query();
                        unsigned msgDay = 0;
                        const char * d = dayPT.queryName();
                        if (*d++ == 'd')
                            msgDay = readDigits(d, 2);
                        if (msgDay == 0)
                        {
                            if (!innerException)
                                innerException.setown(makeStringExceptionV(-1, "child of " SYS_INFO_ROOT "/%s is invalid: %s", monthPT.queryName(), dayPT.queryName()));
                            continue;
                        }
                        if (day && (msgDay >= day))
                            continue;

                        VStringBuffer xpath("%s/%s", monthPT.queryName(), dayPT.queryName());
                        deleteXpathList.push_back(xpath.str());
                    }
                }
            }
        }
    }
    unsigned count = 0;
    for (auto & xpath: deleteXpathList)
    {
        if (root->removeProp(xpath.c_str()))
            ++count;
    }

    if (innerException) // allow items to be deleted even if there is an exception
        throw innerException.getClear();

    return count;
}

class DaliMsgLoggerHandler : public CSimpleInterfaceOf<ILogMsgHandler>
{
public:
    DaliMsgLoggerHandler(unsigned _messageFields=MSGFIELD_all) : messageFields(_messageFields)
    {
    }
    virtual void handleMessage(const LogMsg & msg) override
    {
        LogMsgSysInfo sysInfo = msg.querySysInfo();
        time_t timeNum = sysInfo.queryTime();
        unsigned __int64 ts = sysInfo.queryTime() * 1000000 + sysInfo.queryUSecs();
        logSysInfoError(msg.queryCategory(), msg.queryCode(), queryComponentName(), msg.queryText(), ts);
    }
    virtual bool needsPrep() const override
    {
        return false;
    }
    virtual void prep() override
    {
    }
    virtual unsigned queryMessageFields() const override
    {
        return messageFields;
    }
    virtual void setMessageFields(unsigned _fields = MSGFIELD_all) override
    {
        messageFields = _fields;
    }
    virtual void addToPTree(IPropertyTree * parent) const override
    {
        IPropertyTree * handlerTree = createPTree(ipt_caseInsensitive);
        handlerTree->setProp("@type", "globalmessages");
        handlerTree->setPropInt("@fields", messageFields);
        parent->addPropTree("handler", handlerTree);
    }
    virtual int flush() override
    {
        return 0;
    }
    virtual bool getLogName(StringBuffer &name) const override
    {
        return false;
    }
    virtual offset_t getLogPosition(StringBuffer &logFileName) const override
    {
        return 0;
    }
private:
    unsigned messageFields = MSGFIELD_all;
};

static Owned<ILogMsgHandler> msgHandler;

void useDaliForOperatorMessages(bool use)
{
    if (use)
    {
        if (!msgHandler)
        {
            msgHandler.setown(getDaliMsgLoggerHandler());
            Owned<ILogMsgFilter> operatorFilter = getCategoryLogMsgFilter(MSGAUD_operator,
                                                                    MSGCLS_disaster|MSGCLS_error|MSGCLS_warning,
                                                                    WarnMsgThreshold);
            queryLogMsgManager()->addMonitor(msgHandler, operatorFilter);
        }
    }
    else if (msgHandler)
    {
        queryLogMsgManager()->removeMonitor(msgHandler);
        msgHandler.clear();
    }
}

ILogMsgHandler * getDaliMsgLoggerHandler()
{
    return new DaliMsgLoggerHandler();
}
