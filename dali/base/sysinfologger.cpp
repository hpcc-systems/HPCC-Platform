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
#include "daclient.hpp"
#include "jutil.hpp"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5 minutes
#define SYS_INFO_VERSION "1.0"

#define SYS_INFO_ROOT  "/SysLogs"
#define ATTR_LASTID    "@lastId"
#define ATTR_VERSION   "@version"
#define MSG_NODE       "msg"
#define ATTR_ID        "@id"
#define ATTR_TIMESTAMP "@ts"
#define ATTR_AUDIENCE  "@audience"
#define ATTR_CLASS     "@class"
#define ATTR_CODE      "@code"
#define ATTR_HIDDEN    "@hidden"
#define ATTR_SOURCE    "@source"

static void extractDate(unsigned __int64 ts, unsigned & year, unsigned & month, unsigned & day)
{
    CDateTime timeStamp;
    timeStamp.setTimeStamp(ts);
    timeStamp.getDate(year, month, day);
}

static unsigned __int64 makeMessageId(unsigned year, unsigned month, unsigned day, unsigned id)
{
    return id<<21 | year<<9 | month<<5 | day;
}

static unsigned __int64 makeMessageId(unsigned __int64 ts, unsigned id)
{
    unsigned year, month, day;
    extractDate(ts, year, month, day);
    return makeMessageId(year, month, day, id);
}

static void decodeMessageId(unsigned __int64 msgId, unsigned & year, unsigned & month, unsigned & day, unsigned & id)
{
    day = msgId & 0x1F;
    month = (msgId>>5) & 0x0F;
    year = (msgId>>9) & 0xFFF;
    id = (msgId>>21);
}

class CSysInfoLoggerMsg : implements ISysInfoLoggerMsg
{
    Owned<IPropertyTree> msgPtree;
    IPropertyTree * root;
    bool updateable = false;

    inline void ensureUpdateable()
    {
        if (!updateable)
            throw makeStringException(-1, "Unable to update ISysInfoLoggerMsg");
    }

public:
    CSysInfoLoggerMsg & set(IPropertyTree * ptree, IPropertyTree * _root, bool _updateable)
    {
        msgPtree.setown(ptree);
        root = _root;
        updateable = _updateable;
        return * this;
    }
    virtual bool queryIsHidden() const override
    {
        return msgPtree->getPropBool(ATTR_HIDDEN, false);
    }
    virtual unsigned __int64 queryTimeStamp() const override
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
        return makeMessageId(queryTimeStamp(), msgPtree->getPropInt64(ATTR_ID, 0));
    }
    virtual const char * queryMsg() const override
    {
        const char *msg = msgPtree->queryProp(nullptr);
        return msg ? msg : "";
    }
    virtual void setHidden(bool _hidden)
    {
        ensureUpdateable();
        msgPtree->setPropBool(ATTR_HIDDEN, _hidden);
    }
    virtual void remove()
    {
        ensureUpdateable();
        unsigned year, month, day;
        extractDate(queryTimeStamp(), year, month, day);
        VStringBuffer xpath("m%04u%02u/d%02u", year, month, day);
        IPropertyTree * parent = root->queryPropTree(xpath.str());
        parent->removeTree(msgPtree);
    }
    static IPropertyTree * createMsgPTree(unsigned id, const LogMsgCategory & cat, LogMsgCode code, const char * source, const char * msg, unsigned __int64 ts, bool hidden)
    {
        Owned<IPropertyTree> msgPtree = createPTree(MSG_NODE);
        msgPtree->setPropInt64(ATTR_ID, id);
        msgPtree->setPropBool(ATTR_HIDDEN, false);
        msgPtree->setPropInt64(ATTR_TIMESTAMP, ts);
        msgPtree->setPropInt(ATTR_CODE, code);
        msgPtree->setProp(ATTR_AUDIENCE, LogMsgAudienceToFixString(cat.queryAudience()));
        msgPtree->setProp(ATTR_CLASS, LogMsgClassToFixString(cat.queryClass()));
        msgPtree->setProp(ATTR_SOURCE, source);
        msgPtree->setProp(".", msg);
        return msgPtree.getClear();
    }
};

class CSysInfoLoggerMsgFilter : public CInterfaceOf<ISysInfoLoggerMsgFilter>
{
    // (For numeric fields, match only if it has a non-zero value)
    bool hiddenOnly = false;
    bool visibleOnly = false;
    unsigned __int64 matchTimeStamp = 0;
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
    CSysInfoLoggerMsgFilter()
    {
    }
    CSysInfoLoggerMsgFilter(unsigned __int64 msgId)
    {
        CSysInfoLoggerMsgFilter::setMatchMsgId(msgId);
    }
    CSysInfoLoggerMsgFilter(bool _visibleOnly, bool _hiddenOnly, unsigned _year, unsigned _month, unsigned _day) :
                            visibleOnly(_visibleOnly), hiddenOnly(_hiddenOnly),
                            matchEndYear(_year), matchEndMonth(_month), matchEndDay(_day),
                            matchStartYear(_year), matchStartMonth(_month), matchStartDay(_day)
    {
        if (hiddenOnly && visibleOnly)
            throw makeStringExceptionV(-1, "ISysInfoLoggerMsgFilter: cannot filter by both hiddenOnly and visibleOnly");
        if (!_month && _day)
            throw makeStringExceptionV(-1, "ISysInfoLoggerMsgFilter: month and year must be provided when filtering by day");
        if (!_year && _month)
            throw makeStringExceptionV(-1, "ISysInfoLoggerMsgFilter: year must be provided when filtering by month");
       haveDateRange = matchEndYear||matchStartYear||matchStartMonth|matchEndMonth||matchStartDay||matchEndDay;
    }
    virtual void setHiddenOnly() override
    {
        hiddenOnly = true;
    }
    virtual void setVisibleOnly() override
    {
        visibleOnly = true;
    }
    virtual void setMatchTimeStamp(unsigned __int64 ts) override
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
        unsigned year, month, day, id;
        decodeMessageId(msgId, year, month, day, id);
        if (year==0 || month==0 || day==0 || id==0)
            throw makeStringExceptionV(-1,"ISysInfoLoggerMsgFilter::setMatchMsgId invalid argument: %" I64F "u", msgId);
        matchEndYear = matchStartYear = year;
        matchEndMonth = matchStartMonth = month;
        matchEndDay = matchStartDay = day;
        matchId = id;
        haveDateRange = false;
    }
    virtual void setDateRange(unsigned startYear, unsigned startMonth, unsigned startDay, unsigned endYear, unsigned endMonth, unsigned endDay) override
    {
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
    virtual bool isInDateRange(unsigned __int64 ts) const override
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
    virtual unsigned __int64 queryMatchTimeStamp() const override
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
    virtual unsigned queryMatchYear() const override
    {
        return (matchStartYear==matchEndYear ? matchStartYear : 0);
    }
    virtual unsigned queryMatchMonth() const override
    {
        return (matchStartMonth==matchEndMonth ? matchStartMonth : 0);
    }
    virtual unsigned queryMatchDay() const override
    {
        return (matchStartDay==matchEndDay ? matchStartDay : 0);
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
    virtual unsigned __int64 queryMatchMsgId() const override
    {
        if (queryMatchYear() && queryMatchMonth() && queryMatchDay() && matchId)
        {
            return makeMessageId(queryMatchYear(), queryMatchMonth(), queryMatchDay(), matchId);
        }
        return 0;
    }
    virtual StringBuffer & getQualifierXPathFilter(StringBuffer & xpath) const override
    {
        if (queryMatchYear() && queryMatchMonth())
        {
            xpath.appendf("m%04u%02u", queryMatchYear(), queryMatchMonth());
            if (queryMatchDay())
                xpath.appendf("/d%02u", queryMatchDay());
        }
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
            xpath.appendf("[" ATTR_ID "='%u']", matchId);
        if (matchTimeStamp)
            xpath.appendf("[" ATTR_TIMESTAMP "='%" I64F "u']", matchTimeStamp);
        return xpath;
    }
};

ISysInfoLoggerMsgFilter * createSysInfoLoggerMsgFilter()
{
    return new CSysInfoLoggerMsgFilter();
}

ISysInfoLoggerMsgFilter * createSysInfoLoggerMsgFilter(unsigned __int64 msgId)
{
    return new CSysInfoLoggerMsgFilter(msgId);
}

class CSysInfoLoggerMsgIterator : public CInterfaceOf<ISysInfoLoggerMsgIterator>
{
    Linked<ISysInfoLoggerMsgFilter> filter;
    Owned<IRemoteConnection> conn;
    bool updateable = false;
    Owned<IPropertyTree> root;
    Owned<IPropertyTreeIterator> msgIter;
    CSysInfoLoggerMsg infoMsg;

    bool ensureMatch()
    {
        if (filter->hasDateRange())
        {
            for (; msgIter->isValid(); msgIter->next())
            {
                unsigned __int64 ts = msgIter->query().getPropInt64(ATTR_TIMESTAMP, 0);
                if (filter->isInDateRange(ts))
                    return true;
            }
        }
        return msgIter->isValid();
    }

public:
    CSysInfoLoggerMsgIterator(ISysInfoLoggerMsgFilter * _filter, bool _updateable=false) : filter(_filter), updateable(_updateable)
    {
    }
    CSysInfoLoggerMsg & queryInfoLoggerMsg()
    {
        return infoMsg.set(&(msgIter->get()), root.get(), updateable);
    }
    virtual ISysInfoLoggerMsg & query() override
    {
        return queryInfoLoggerMsg();
    }
    virtual bool first() override
    {
        unsigned mode = RTM_LOCK_READ | (updateable?RTM_LOCK_WRITE : 0);
        conn.setown(querySDS().connect(SYS_INFO_ROOT, myProcessSession(), mode, SDS_LOCK_TIMEOUT));
        if (!conn)
            return false;
        root.setown(conn->getRoot());

        StringBuffer xpath;
        filter->getQualifierXPathFilter(xpath);

        msgIter.setown(root->getElements(xpath.str()));
        if (!msgIter->first())
            return false;
        return ensureMatch();
    }
    virtual bool next() override
    {
        if (msgIter->next())
            return false;
        return ensureMatch();
    }
    virtual bool isValid() override
    {
        return msgIter ? msgIter->isValid() : false;
    }
};

ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(bool visibleOnly, bool hiddenOnly, unsigned year, unsigned month, unsigned day)
{
    Owned<CSysInfoLoggerMsgFilter> filter = new CSysInfoLoggerMsgFilter(visibleOnly, hiddenOnly, year, month, day);
    return new CSysInfoLoggerMsgIterator(filter.getClear(), false);
}

ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(ISysInfoLoggerMsgFilter * msgFilter)
{
    return new CSysInfoLoggerMsgIterator(msgFilter, false);
}

// returns messageId
unsigned __int64 logSysInfoError(const LogMsgCategory & cat, LogMsgCode code, const char *source, const char * msg, unsigned __int64 ts)
{
    if (ts==0)
        ts = getTimeStampNowValue();

    if (isEmptyString(source))
        source = "unknown";
    Owned<IRemoteConnection> conn = querySDS().connect(SYS_INFO_ROOT, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw makeStringExceptionV(-1, "logSysInfoLogger: unable to create connection to '%s'", SYS_INFO_ROOT);

    StringBuffer xpath;
    unsigned year, month, day;
    extractDate(ts, year, month, day);
    xpath.appendf("m%04u%02u/d%02u[" ATTR_VERSION "='%s']", year, month, day, SYS_INFO_VERSION);

    unsigned id = 0;
    IPropertyTree * dayPT = conn->queryRoot()->queryPropTree(xpath.str());
    if (dayPT)
    {
        id = dayPT->getPropInt(ATTR_LASTID, 0);
    }
    else
    {
        xpath.setf("m%04u%02u", year, month);
        IPropertyTree * monthPT = conn->queryRoot()->queryPropTree(xpath.str());
        if (!monthPT)
            monthPT = conn->queryRoot()->addPropTree(xpath.str());
        xpath.setf("d%02u", day);
        dayPT = monthPT->addPropTree(xpath.str());
        dayPT->addProp(ATTR_VERSION, SYS_INFO_VERSION);
    }
    dayPT->setPropInt(ATTR_LASTID, ++id);
    dayPT->addPropTree(MSG_NODE, CSysInfoLoggerMsg::createMsgPTree(id, cat, code, source, msg, ts, false));

    return makeMessageId(ts, id);
}

unsigned updateMessage(ISysInfoLoggerMsgFilter * msgFilter, std::function<void (CSysInfoLoggerMsg &)> updateOp)
{
    unsigned count = 0;
    Owned<CSysInfoLoggerMsgIterator> iter = new CSysInfoLoggerMsgIterator(msgFilter, true);
    ForEach(*iter)
    {
        CSysInfoLoggerMsg & sysInfoMsg = iter->queryInfoLoggerMsg();
        updateOp(sysInfoMsg);
        ++count;
    }
    return count;
}

unsigned updateMessage(unsigned __int64 msgId, std::function<void (CSysInfoLoggerMsg &)> updateOp)
{
    Owned<ISysInfoLoggerMsgFilter> msgFilter = createSysInfoLoggerMsgFilter(msgId);
    return updateMessage(msgFilter, updateOp);
}

unsigned hideLogSysInfoMsg(ISysInfoLoggerMsgFilter * msgFilter)
{
    return updateMessage(msgFilter, [](CSysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.setHidden(true);});
}

bool hideLogSysInfoMsg(unsigned __int64 msgId)
{
    return updateMessage(msgId, [](CSysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.setHidden(true);})==1;
}

unsigned unhideLogSysInfoMsg(ISysInfoLoggerMsgFilter * msgFilter)
{
    return updateMessage(msgFilter, [](CSysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.setHidden(false);});
}

bool unhideLogSysInfoMsg(unsigned __int64 msgId)
{
    return updateMessage(msgId, [](CSysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.setHidden(false);})==1;
}

unsigned deleteLogSysInfoMsg(ISysInfoLoggerMsgFilter * msgFilter)
{
    return updateMessage(msgFilter, [](CSysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.remove();});
}

bool deleteLogSysInfoMsg(unsigned __int64 msgId)
{
    return updateMessage(msgId, [](CSysInfoLoggerMsg & sysInfoMsg){sysInfoMsg.remove();})==1;
}

unsigned deleteOlderThanLogSysInfoMsg(bool visibleOnly, bool hiddenOnly, unsigned year, unsigned month, unsigned day)
{
    // With visibleOnly/hiddenOnly option, use createSysInfoLoggerMsgFilter()
    if (visibleOnly || hiddenOnly)
    {
        unsigned count = 0;
        Owned<ISysInfoLoggerMsgFilter> msgFilter = createSysInfoLoggerMsgFilter();
        if (hiddenOnly)
            msgFilter->setHiddenOnly();
        if (visibleOnly)
            msgFilter->setVisibleOnly();
        msgFilter->setOlderThanDate(year, month, day);
        return deleteLogSysInfoMsg(msgFilter);
    }
    // With only date range, use this quicker method to remove whole subtrees
    unsigned count = 0;
    Owned<IRemoteConnection> conn = querySDS().connect(SYS_INFO_ROOT, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        return 0;

    Owned<IPropertyTreeIterator> monthIter = conn->queryRoot()->getElements("*");
    ForEach(*monthIter)
    {
        IPropertyTree & monthPT = monthIter->query();
        if (year==0)
        {
            conn->queryRoot()->removeTree(&monthPT);
        }
        else
        {
            unsigned msgYear = 0, msgMonth = 0;
            const char *p = monthPT.queryName(); // should be in format 'myyyydd'
            if (*p++ == 'm')
            {
                msgYear = readDigits(p, 4);
                msgMonth = readDigits(p, 2);
            }
            if (msgYear == 0 || msgMonth == 0)
                throw makeStringExceptionV(-1, "child of " SYS_INFO_ROOT " is invalid: %s", monthPT.queryName());
            if (msgYear > year)
                continue;
            if (msgYear == year && (msgMonth > month))
                continue;
            if (msgMonth < month)
            {
                conn->queryRoot()->removeTree(&monthPT);
                ++count;
            }
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
                        throw makeStringExceptionV(-1, "child of " SYS_INFO_ROOT "/%s is invalid: %s", monthPT.queryName(), dayPT.queryName());
                    if (day && (msgDay >= day))
                        continue;

                    monthPT.removeTree(&dayPT);
                    ++count;
                }
            }
        }
    }
    return count;
}
