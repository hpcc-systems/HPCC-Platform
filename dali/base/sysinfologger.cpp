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

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <algorithm>
#endif

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5 minutes
#define SYS_INFO_VERSION "1.0"

#define SYS_INFO_ROOT  "/SysLogs"
#define ATTR_VERSION   "@version"
#define MSG_NODE       "msg"
#define ATTR_MSGID     "@msgID"
#define ATTR_TIMESTAMP "@ts"
#define ATTR_AUDIENCE  "@audience"
#define ATTR_CLASS     "@class"
#define ATTR_CODE      "@code"
#define ATTR_HIDDEN    "@hidden"
#define ATTR_SOURCE    "@source"


static unsigned readDigits(char const * & p, unsigned numDigits)
{
    unsigned num = 0;
    for (unsigned n=0; n<numDigits; ++n, ++p)
    {
        if (!isdigit(*p))
            return 0;
        num = num * 10 + (*p - '0');
    }
    return num;
}

class CSysInfoLoggerMsg : implements ISysInfoLoggerMsg
{
    Owned<IPropertyTree> msgPtree = nullptr;
    Linked<IPropertyTree> root = nullptr;
    bool updateable = false;

    inline void ensureUpdateable()
    {
        if (!updateable)
            throw makeStringExceptionV(-1, "Unable to update ISysInfoLoggerMsg");
    }

public:
    CSysInfoLoggerMsg & set(IPropertyTree & ptree, IPropertyTree & _root, bool _updateable)
    {
        msgPtree.setown(&ptree);
        root.set(LINK(&_root));
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
        return msgPtree->getPropInt64(ATTR_MSGID, 0);
    }
    virtual const char * queryMsg() const override
    {
        if (msgPtree->hasProp("."))
            return msgPtree->queryProp(".");
        else
            return "";
    }
    virtual void setHidden(bool _hidden)
    {
        ensureUpdateable();
        msgPtree->setPropBool(ATTR_HIDDEN, _hidden);
    }
    virtual void remove()
    {
        ensureUpdateable();
        const char *rootName = root->queryName();
        assertex(rootName);

        // if parent is not a 'day', then will need to get the day tree
        if (rootName[0]!='d')
        {
            Owned<IPropertyTree> parent;
            unsigned year, month, day;
            CDateTime timeStamp;
            timeStamp.setTimeStamp(queryTimeStamp());
            timeStamp.getDate(year, month, day);

            if (rootName[0]=='m') // have month, so need the 'day' child
            {
                VStringBuffer xpath("d%02u", day);
                parent.setown(root->getPropTree(xpath.str()));
            }
            else // will need to specify for month/day child
            {
                VStringBuffer xpath("m%04u%02u/d%02u", year, month, day);
                parent.setown(root->getPropTree(xpath.str()));
            }
            assertex(parent.get());
            parent->removeTree(msgPtree);
        }
        else
            root->removeTree(msgPtree);
    }
    static IPropertyTree * createMsgPTree(LogMsgId msgId, const LogMsgCategory & cat, LogMsgCode code, const char * source, const char * msg, unsigned __int64 ts, bool hidden)
    {
        Owned<IPropertyTree> msgPtree = createPTree(MSG_NODE);
        msgPtree->setPropInt64(ATTR_MSGID, msgId);
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
    // (For numeric fields, match only if it has non-zero value)
    bool hiddenOnly = false;
    bool visibleOnly = false;
    unsigned __int64 matchTimeStamp = 0;
    StringAttr matchSource; // only matchSource when not empty
    LogMsgCode matchCode = 0;
    LogMsgAudience matchAudience = MSGAUD_all;
    LogMsgClass matchClass = MSGCLS_all;
    unsigned __int64 matchMsgId = 0;
    bool haveDateRange = false;
    unsigned matchEndYear = 0;
    unsigned matchEndMonth = 0;
    unsigned matchEndDay = 0;
    unsigned matchStartYear = 0;
    unsigned matchStartMonth = 0;
    unsigned matchStartDay = 0;

public:
    CSysInfoLoggerMsgFilter()
    {
    }
    CSysInfoLoggerMsgFilter(unsigned __int64 msgId, unsigned __int64 ts) : matchMsgId(msgId), matchTimeStamp(ts)
    {
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
        matchMsgId = msgId;
    }
    virtual void setDateRange(unsigned startYear, unsigned startMonth, unsigned startDay, unsigned endYear, unsigned endMonth, unsigned endDay) override
    {
        matchEndYear = endYear;
        matchEndMonth = endMonth;
        matchEndDay = endDay;
        matchStartYear = startYear;
        matchStartMonth = startMonth;
        matchStartDay = startDay;
        haveDateRange = matchEndYear||matchStartYear||matchStartMonth|matchEndMonth||matchStartDay||matchEndDay;
    }
    virtual void setOlderThanDate(unsigned year, unsigned month, unsigned day) override
    {
        matchEndYear = year;
        matchEndMonth = month;
        matchEndDay = day;
        matchStartYear = 0;
        matchStartMonth = 0;
        matchStartDay = 0;
        haveDateRange = matchEndYear||matchStartYear||matchStartMonth|matchEndMonth||matchStartDay||matchEndDay;
    }
    virtual bool hasDateRange() const override
    {
        return haveDateRange;
    }
    virtual bool isInDateRange(unsigned __int64 ts) const override
    {
        CDateTime dt;
        dt.setTimeStamp(ts);
        unsigned tyear, tmonth, tday;
        dt.getDate(tyear, tmonth, tday);
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
    };
    virtual unsigned __int64 queryMatchMsgId() const override
    {
        return matchMsgId;
    };

};

ISysInfoLoggerMsgFilter * createSysInfoLoggerMsgFilter()
{
    return new CSysInfoLoggerMsgFilter();
}

class CSysInfoLoggerMsgIterator : public CInterface, implements ISysInfoLoggerMsgIterator
{
    Linked<ISysInfoLoggerMsgFilter> filter;
    Owned<IRemoteConnection> conn = nullptr;
    bool updateable = false;
    Owned<IPropertyTree> root = nullptr;
    Owned<IPropertyTreeIterator> msgIter = nullptr;
    CSysInfoLoggerMsg infoMsg;

    bool ensureMatch()
    {
        if (filter->hasDateRange())
        {
            for (; msgIter && msgIter->isValid(); msgIter->next())
            {
                unsigned __int64 ts = msgIter->query().getPropInt64(ATTR_TIMESTAMP, 0);
                if(!ts || !filter->isInDateRange(ts))
                    continue;
                break;
            }
        }
        return msgIter->isValid();
    }

public:
    IMPLEMENT_IINTERFACE;

    CSysInfoLoggerMsgIterator(ISysInfoLoggerMsgFilter * _filter, bool _updateable=false) : filter(_filter), updateable(_updateable)
    {
    }
    ~CSysInfoLoggerMsgIterator()
    {
        if (conn)
        {
            root.clear();
            msgIter.clear();
            conn->close();
            conn.clear();
        }
    }
    CSysInfoLoggerMsg & queryInfoLoggerMsg()
    {
        return infoMsg.set(msgIter->get(), *(root.getLink()), updateable);
    }
    virtual ISysInfoLoggerMsg & query() override
    {
        return queryInfoLoggerMsg();
    }
    virtual bool first() override
    {
        StringBuffer xpath(SYS_INFO_ROOT);
        if(filter->queryMatchYear() && filter->queryMatchMonth())
        {
            xpath.appendf("/m%04u%02u", filter->queryMatchYear(), filter->queryMatchMonth());
            if (filter->queryMatchDay())
                xpath.appendf("/d%02u", filter->queryMatchDay());
        }
        unsigned mode = updateable ? RTM_LOCK_WRITE : RTM_LOCK_READ;
        printf("querySDS().connect first() connecting\n");
        conn.setown(querySDS().connect(xpath.str(), myProcessSession(), mode, SDS_LOCK_TIMEOUT));
        if (!conn)
            return false;
        printf("querySDS().connect first() connected\n");

        xpath.set("//" MSG_NODE);
        if (filter->queryHiddenOnly())
            xpath.append("[" ATTR_HIDDEN "='1')]");
        if (filter->queryVisibleOnly())
            xpath.append("[" ATTR_HIDDEN "='0')]");
        if (!isEmptyString(filter->queryMatchSource()))
            xpath.appendf("[" ATTR_SOURCE "='%s']", filter->queryMatchSource());
        if (filter->queryMatchCode())
            xpath.appendf("[" ATTR_CODE "='%d']", (int)filter->queryMatchCode());
        if (filter->queryMatchAudience()!=MSGAUD_all)
            xpath.appendf("[" ATTR_AUDIENCE "='%s']", LogMsgAudienceToFixString(filter->queryMatchAudience()));
        if (filter->queryMatchClass()!=MSGCLS_all)
            xpath.appendf("[" ATTR_CLASS "='%s']", LogMsgClassToFixString(filter->queryMatchClass()));
        if (filter->queryMatchMsgId())
            xpath.appendf("[" ATTR_MSGID "='%" I64F "u']", filter->queryMatchMsgId());
        if (filter->queryMatchTimeStamp())
            xpath.appendf("[" ATTR_TIMESTAMP "='%" I64F "u']", filter->queryMatchTimeStamp());
        root.setown(conn->getRoot());
        msgIter.setown(root->getElements(xpath.str()));
        msgIter->first();
        return ensureMatch();
    }
    virtual bool next() override
    {
        msgIter->next();
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

ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(unsigned __int64 msgId, unsigned __int64 ts)
{
    Owned<CSysInfoLoggerMsgFilter> filter = new CSysInfoLoggerMsgFilter(msgId, ts);
    return new CSysInfoLoggerMsgIterator(filter.getClear(), false);
}

ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(ISysInfoLoggerMsgFilter * msgFilter)
{
    return new CSysInfoLoggerMsgIterator(msgFilter, false);
}

void logSysInfoError(const LogMsgId msgId, const LogMsgCategory & cat, LogMsgCode code, const char *source, const char * msg, unsigned __int64 ts)
{
    if (ts==0)
        ts = getTimeStampNowValue();

    if (isEmptyString(source))
        source = "unknown";

    StringBuffer xpath(SYS_INFO_ROOT);
    unsigned year, month, day;
    CDateTime timeStamp;
    timeStamp.setTimeStamp(ts);
    timeStamp.getDate(year, month, day);
    xpath.appendf("/m%04u%02u/d%02u", year, month, day);

    printf("logSysInfoError msgID %u\n", msgId);
    printf("querySDS().connect connecting\n");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw makeStringExceptionV(-1, "logSysInfoError: unable to create connection to '%s'", xpath.str());
    printf("querySDS().connect connected\n");
    IPropertyTree * root = conn->queryRoot();
    if (!root->hasProp(ATTR_VERSION))
        root->addProp(ATTR_VERSION, SYS_INFO_VERSION);

    root->addPropTree(MSG_NODE, CSysInfoLoggerMsg::createMsgPTree(msgId, cat, code, source, msg, ts, false));
    conn->close();
};

unsigned hideLogSysInfoMsg(ISysInfoLoggerMsgFilter * msgFilter)
{
    unsigned count = 0;
    Owned<CSysInfoLoggerMsgIterator> iter = new CSysInfoLoggerMsgIterator(msgFilter, true);
    ForEach(*iter)
    {
        CSysInfoLoggerMsg & sysInfoMsg = iter->queryInfoLoggerMsg();
        sysInfoMsg.setHidden(true);
        ++count;
    }
    return count;
}

unsigned deleteLogSysInfoMsg(ISysInfoLoggerMsgFilter * msgFilter)
{
    unsigned count = 0;
    Owned<CSysInfoLoggerMsgIterator> iter = new CSysInfoLoggerMsgIterator(msgFilter, true);
    ForEach(*iter)
    {
        CSysInfoLoggerMsg & sysInfoMsg = iter->queryInfoLoggerMsg();
        sysInfoMsg.remove();
        ++count;
    }
    return count;
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
    printf("querySDS().connect deleteOlderThanLogSysInfoMsg connecting\n");
    Owned<IRemoteConnection> conn = querySDS().connect(SYS_INFO_ROOT, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        return 0;
    printf("querySDS().connect deleteOlderThanLogSysInfoMsg connected\n");

    Owned<IPropertyTreeIterator> monthIter = conn->queryRoot()->getElements("./*");
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
                msgMonth= readDigits(p, 2);
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
                Owned<IPropertyTreeIterator> dayIter = monthPT.getElements("./*");
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
    conn->close();
    return count;
}
