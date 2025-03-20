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

#ifndef SYSINFOLOGGER
#define SYSINFOLOGGER

#include "jlog.hpp"
#include "jutil.hpp"
#include "daclient.hpp"

#ifdef DALI_EXPORTS
 #define SYSINFO_API DECL_EXPORT
#else
 #define SYSINFO_API DECL_IMPORT
#endif

interface IConstSysInfoLoggerMsg
{
    virtual bool queryIsHidden() const = 0;
    virtual timestamp_type queryTimeStamp() const = 0;
    virtual const char * querySource() const = 0;
    virtual LogMsgCode queryLogMsgCode() const = 0;
    virtual LogMsgAudience queryAudience() const = 0;
    virtual LogMsgClass queryClass() const = 0;
    virtual unsigned __int64 queryLogMsgId() const = 0;
    virtual const char * queryMsg() const = 0;
    virtual StringBuffer & getXpath(StringBuffer & xpath) const = 0;
};

interface ISysInfoLoggerMsg : public IConstSysInfoLoggerMsg
{
    virtual void setHidden(bool _hidden) = 0;
};

interface IConstSysInfoLoggerMsgFilter : public IInterface
{
    virtual bool hasDateRange() const = 0;
    virtual bool isInDateRange(timestamp_type ts) const = 0;
    virtual bool queryHiddenOnly() const = 0;
    virtual bool queryVisibleOnly() const = 0;
    virtual timestamp_type queryMatchTimeStamp() const = 0;
    virtual unsigned queryStartYear() const = 0;
    virtual unsigned queryStartMonth() const = 0;
    virtual unsigned queryStartDay() const = 0;
    virtual unsigned queryEndYear() const = 0;
    virtual unsigned queryEndMonth() const = 0;
    virtual unsigned queryEndDay() const = 0;
    virtual const char * queryMatchSource() const = 0;
    virtual LogMsgCode queryMatchCode() const = 0;
    virtual LogMsgAudience queryMatchAudience() const = 0;
    virtual LogMsgClass queryMatchClass() const = 0;
    virtual StringBuffer & getQualifierXPathFilter(StringBuffer & xpath) const = 0;
};

interface ISysInfoLoggerMsgFilter : extends IConstSysInfoLoggerMsgFilter
{
    virtual void setHiddenOnly() = 0;
    virtual void setVisibleOnly() = 0;
    virtual void setMatchTimeStamp(timestamp_type ts) = 0;
    virtual void setMatchSource(const char * source) = 0;
    virtual void setMatchCode(LogMsgCode code) = 0;
    virtual void setMatchAudience(LogMsgAudience audience) = 0;
    virtual void setMatchMsgClass(LogMsgClass msgClass) = 0;
    virtual void setMatchMsgId(unsigned __int64 msgId) = 0;
    virtual void setDateRange(unsigned startYear, unsigned startMonth, unsigned startDay, unsigned endYear, unsigned endMonth, unsigned endDay) = 0;
    virtual void setOlderThanDate(unsigned year, unsigned month, unsigned day) = 0;
};

typedef IIteratorOf<ISysInfoLoggerMsg> ISysInfoLoggerMsgIterator;

SYSINFO_API ISysInfoLoggerMsgFilter * createSysInfoLoggerMsgFilter(const char *source=nullptr);
SYSINFO_API ISysInfoLoggerMsgFilter * createSysInfoLoggerMsgFilter(unsigned __int64 msgId, const char *source=nullptr);
SYSINFO_API ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(IRemoteConnection * conn, IConstSysInfoLoggerMsgFilter * msgFilter, bool updateable = false);
SYSINFO_API ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(IConstSysInfoLoggerMsgFilter * msgFilter, bool updateable = false);
SYSINFO_API ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(IRemoteConnection * conn, bool visibleOnly, bool hiddenOnly, unsigned year, unsigned month, unsigned day, const char *source, bool updateable = false);
SYSINFO_API ISysInfoLoggerMsgIterator * createSysInfoLoggerMsgIterator(bool visibleOnly, bool hiddenOnly, unsigned year, unsigned month, unsigned day, const char *source, bool updateable = false);

SYSINFO_API unsigned __int64 logSysInfoError(const LogMsgCategory & cat, LogMsgCode code, const char *source, const char * msg, unsigned __int64 ts);
SYSINFO_API unsigned hideLogSysInfoMsg(IConstSysInfoLoggerMsgFilter * msgFilter);
SYSINFO_API bool hideLogSysInfoMsg(unsigned __int64 msgId, const char *source=nullptr);
SYSINFO_API unsigned unhideLogSysInfoMsg(IConstSysInfoLoggerMsgFilter * msgFilter, const char *source=nullptr);
SYSINFO_API bool unhideLogSysInfoMsg(unsigned __int64 msgId, const char *source=nullptr);
SYSINFO_API unsigned deleteLogSysInfoMsg(IConstSysInfoLoggerMsgFilter * msgFilter);
SYSINFO_API bool deleteLogSysInfoMsg(unsigned __int64 msgId, const char *source=nullptr);
SYSINFO_API unsigned deleteOlderThanLogSysInfoMsg(bool visibleOnly, bool hiddenOnly, unsigned year, unsigned month, unsigned day, const char *source=nullptr);

/* makeMessageId - Create a message id from date, sequence number and nonSysInfoLogMsg flag to uniquely identify a message
 - nonSysInfoLogMsg flag is used to identify whether or not the message is managed by SysInfoLogger */
SYSINFO_API unsigned __int64 makeMessageId(unsigned __int64 ts, unsigned seqN, bool nonSysInfoLogMsg=false);
SYSINFO_API unsigned __int64 makeMessageId(unsigned year, unsigned month, unsigned day, unsigned seqN, bool nonSysInfoLogMsg=false);
SYSINFO_API ILogMsgHandler * getDaliMsgLoggerHandler();
SYSINFO_API void useDaliForOperatorMessages(bool use);
#endif
