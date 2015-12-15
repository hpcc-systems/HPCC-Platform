/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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



#ifndef JLOG_HPP
#define JLOG_HPP

// Control whether XML reports have newlines between category/system/job/text sections
#ifdef _DEBUG
#define LOG_MSG_NEWLINE
#endif

#include "stdio.h"
#include "time.h"
#include "jiface.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jutil.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"


// ENUMS, TYPEDEFS, CONSTS ETC.

// Enums, typedefs, and consts for LogMsgCategory, plus enum-to-string functions

// When changing this enum, be sure to update (a) the string functions, and (b) NUM value

typedef MessageAudience LogMsgAudience;

#define MSGAUDNUM 8

inline const char * LogMsgAudienceToVarString(LogMsgAudience audience)
{
    switch(audience)
    {
    case MSGAUD_operator:
        return("Operator");
    case MSGAUD_user:
        return("User");
    case MSGAUD_monitor:
        return("Monitor");
    case MSGAUD_performance:
        return("Performance");
    case MSGAUD_internal:
        return("Internal");
    case MSGAUD_programmer:
        return("Programmer");
    case MSGAUD_legacy:
        return("Legacy");
    case MSGAUD_audit:
        return("Audit");
    default:
        return("UNKNOWN");
    }
}

inline const char * LogMsgAudienceToFixString(LogMsgAudience audience)
{
    switch(audience)
    {
    case MSGAUD_operator:
        return("Operator ");
    case MSGAUD_user:
        return("User     ");
    case MSGAUD_monitor:
        return("Monitor  ");
    case MSGAUD_performance:
        return("Perf.    ");
    case MSGAUD_internal:
        return("Internal ");
    case MSGAUD_programmer:
        return("Prog.    ");
    case MSGAUD_legacy:
        return("Legacy   ");
    case MSGAUD_audit:
        return("Audit    ");
    default:
        return("UNKNOWN  ");
    }
}

// When changing this enum, be sure to update (a) the string functions, and (b) NUM value

typedef enum
{
    MSGCLS_unknown     = 0x00,
    MSGCLS_disaster    = 0x01,
    MSGCLS_error       = 0x02,
    MSGCLS_warning     = 0x04,
    MSGCLS_information = 0x08,
    MSGCLS_progress    = 0x10,
    MSGCLS_legacy      = 0x20,
    MSGCLS_event       = 0x40,
    MSGCLS_all         = 0xFF
} LogMsgClass;

#define MSGCLSNUM 7

inline const char * LogMsgClassToVarString(LogMsgClass msgClass)
{
    switch(msgClass)
    {
    case MSGCLS_disaster:
        return("Disaster");
    case MSGCLS_error:
        return("Error");
    case MSGCLS_warning:
        return("Warning");
    case MSGCLS_information:
        return("Information");
    case MSGCLS_progress:
        return("Progress");
    case MSGCLS_legacy:
        return("Legacy");
    case MSGCLS_event:
        return("Event");
    default:
        return("UNKNOWN");
    }
}

inline const char * LogMsgClassToFixString(LogMsgClass msgClass)
{
    switch(msgClass)
    {
    case MSGCLS_disaster:
        return("Disaster ");
    case MSGCLS_error:
        return("Error    ");
    case MSGCLS_warning:
        return("Warning  ");
    case MSGCLS_information:
        return("Inform.  ");
    case MSGCLS_progress:
        return("Progress ");
    case MSGCLS_legacy:
        return("Legacy   ");
    case MSGCLS_event:
        return("Event    ");
    default:
        return("UNKNOWN  ");
    }
}

typedef unsigned LogMsgDetail;
#define DefaultDetail   100
#define TopDetail (LogMsgDetail)-1

// Typedef for LogMsgSysInfo

typedef unsigned LogMsgId;

// Typedefs and consts for LogMsgJobInfo

typedef unsigned __int64 LogMsgJobId;
typedef unsigned __int64 LogMsgUserId;
typedef unsigned __int64 LogMsgSessionId;
#define UnknownJob (LogMsgJobId)-1
#define UnknownUser (LogMsgUserId)-1
#define UnknownSession (LogMsgSessionId)-1

// Other enums, typedefs, and consts

typedef int LogMsgCode;
#define NoLogMsgCode -1

// When changing this enum, be sure to update (a) the string function, and (b) the abbrev function

typedef enum
{
    MSGFIELD_audience    = 0x000001,
    MSGFIELD_class       = 0x000002,
    MSGFIELD_detail      = 0x000004,
    MSGFIELD_allCategory = 0x000007,
    MSGFIELD_msgID       = 0x000008,
    MSGFIELD_time        = 0x000010,
    MSGFIELD_date        = 0x000020,
    MSGFIELD_timeDate    = 0x000030,
    MSGFIELD_process     = 0x000040,
    MSGFIELD_thread      = 0x000080,
    MSGFIELD_node        = 0x000100,
    MSGFIELD_allSysInfo  = 0x00F1F8,
    MSGFIELD_job         = 0x000200,
    MSGFIELD_user        = 0x000400,
    MSGFIELD_session     = 0x000800,
    MSGFIELD_allJobInfo  = 0x000E00,
    MSGFIELD_code        = 0x001000,
    MSGFIELD_milliTime   = 0x002000,
    MSGFIELD_microTime   = 0x004000,
    MSGFIELD_nanoTime    = 0x008000,  // Not supported
    MSGFIELD_component   = 0x010000,
    MSGFIELD_quote       = 0x020000,
    MSGFIELD_prefix      = 0x040000,
    MSGFIELD_last        = 0x040000,
    MSGFIELD_all         = 0xFFFFFF
} LogMsgField;

#ifdef _WIN32
#define MSGFIELD_STANDARD LogMsgField(MSGFIELD_timeDate | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code | MSGFIELD_quote | MSGFIELD_prefix)
#else
#define MSGFIELD_STANDARD LogMsgField(MSGFIELD_timeDate | MSGFIELD_milliTime | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code | MSGFIELD_quote | MSGFIELD_prefix)
#endif

inline const char * LogMsgFieldToString(LogMsgField field)
{
    switch(field)
    {
    case MSGFIELD_audience:
        return("Audience");
    case MSGFIELD_class:
        return("Class");
    case MSGFIELD_detail:
        return("Detail");
    case MSGFIELD_msgID:
        return("Message ID");
    case MSGFIELD_time:
        return("Time");
    case MSGFIELD_date:
        return("Date");
    case MSGFIELD_process:
        return("Process ID");
    case MSGFIELD_thread:
        return("Thread ID");
    case MSGFIELD_node:
        return("Node");
    case MSGFIELD_job:
        return("Job");
    case MSGFIELD_user:
        return("User");
    case MSGFIELD_session:
        return("Session");
    case MSGFIELD_code:
        return("Code");
    case MSGFIELD_milliTime:
        return("Timer (milli)");
    case MSGFIELD_microTime:
        return("Timer (micro)");
    case MSGFIELD_nanoTime:
        return("Timer (nano)");
    case MSGFIELD_component:
        return("Component");
    case MSGFIELD_quote:
        return("Quote");
    default:
        return("UNKNOWN");
    }
}

inline unsigned LogMsgFieldFromAbbrev(char const * abbrev)
{
    if(strnicmp(abbrev, "AUD", 3)==0)
        return MSGFIELD_audience;
    if(strnicmp(abbrev, "CLS", 3)==0)
        return MSGFIELD_class;
    if(strnicmp(abbrev, "DET", 3)==0)
        return MSGFIELD_detail;
    if(strnicmp(abbrev, "MID", 3)==0)
        return MSGFIELD_msgID;
    if(strnicmp(abbrev, "TIM", 3)==0)
        return MSGFIELD_time;
    if(strnicmp(abbrev, "DAT", 3)==0)
        return MSGFIELD_date;
    if(strnicmp(abbrev, "PID", 3)==0)
        return MSGFIELD_process;
    if(strnicmp(abbrev, "TID", 3)==0)
        return MSGFIELD_thread;
    if(strnicmp(abbrev, "NOD", 3)==0)
        return MSGFIELD_node;
    if(strnicmp(abbrev, "JOB", 3)==0)
        return MSGFIELD_job;
    if(strnicmp(abbrev, "USE", 3)==0)
        return MSGFIELD_user;
    if(strnicmp(abbrev, "SES", 3)==0)
        return MSGFIELD_session;
    if(strnicmp(abbrev, "COD", 3)==0)
        return MSGFIELD_code;
    if(strnicmp(abbrev, "MLT", 3)==0)
        return MSGFIELD_milliTime;
    if(strnicmp(abbrev, "MCT", 3)==0)
        return MSGFIELD_microTime;
    if(strnicmp(abbrev, "NNT", 3)==0)
        return MSGFIELD_nanoTime;
    if(strnicmp(abbrev, "COM", 3)==0)
        return MSGFIELD_component;
    if(strnicmp(abbrev, "QUO", 3)==0)
        return MSGFIELD_quote;
    if(strnicmp(abbrev, "PFX", 3)==0)
        return MSGFIELD_prefix;
    if(strnicmp(abbrev, "ALL", 3)==0)
        return MSGFIELD_all;
    if(strnicmp(abbrev, "STD", 3)==0)
        return MSGFIELD_STANDARD;
    return 0;
}

// This function parses strings such as "AUD+CLS+DET+COD" and "STD+MIT-PID", and is used for fields attribute in XML handler descriptions

inline unsigned LogMsgFieldsFromAbbrevs(char const * abbrevs)
{
    unsigned fields = 0;
    bool negate = false;
    bool more = true;
    while(more)
    {
        if(strlen(abbrevs)<3) break;
        unsigned field = LogMsgFieldFromAbbrev(abbrevs);
        if(field)
        {
            if(negate)
                fields &= ~field;
            else
                fields |= field;
        }
        switch(abbrevs[3])
        {
        case '+':
            negate = false;
            abbrevs += 4;
            break;
        case '-':
            negate = true;
            abbrevs += 4;
            break;
        default:
            more = false;
        }
    }
    return fields;
}

inline char const * msgPrefix(LogMsgClass msgClass)
{
    switch(msgClass)
    {
    case MSGCLS_error:
        return "ERROR: ";
    case MSGCLS_warning:
        return "WARNING: ";
    default:
        return "";
    }
}

// LOG MESSAGE CLASS AND ITS COMPONENTS

// Info about category of log message, provided by user (this info is static, chosen during coding)

class jlib_decl LogMsgCategory
{
public:
    LogMsgCategory(LogMsgAudience _audience = MSGAUD_unknown, LogMsgClass _class = MSGCLS_unknown, LogMsgDetail _detail = DefaultDetail) : audience(_audience), msgClass(_class), detail(_detail) {}
    inline LogMsgAudience     queryAudience() const { return audience; }
    inline LogMsgClass        queryClass() const { return msgClass; }
    inline LogMsgDetail       queryDetail() const { return detail; }
    void                      serialize(MemoryBuffer & out) const { out.append(audience).append(msgClass).append(detail); }
    void                      deserialize(MemoryBuffer & in)
    {
        unsigned a, c, d; in.read(a).read(c).read(d);
        audience = (LogMsgAudience) a;
        msgClass = (LogMsgClass) c;
        detail = (LogMsgDetail) d;
    }
    LogMsgCategory const      operator ()(unsigned newDetail) const { return LogMsgCategory(audience, msgClass, newDetail); }
private:
    LogMsgAudience            audience;
    LogMsgClass               msgClass;
    LogMsgDetail              detail;
};

// Info about log message determined automatically by system

class jlib_decl LogMsgSysInfo
{
public:
    LogMsgSysInfo(LogMsgId _id = (LogMsgId)-1, unsigned port = 0, LogMsgSessionId session = UnknownSession);
    inline LogMsgId           queryMsgID() const { return id; }
#ifdef _WIN32
    inline time_t             queryTime() const { return timeStarted; }
    inline unsigned           queryUSecs() const { return 0; }
#else
    inline time_t             queryTime() const { return timeStarted.tv_sec; }
    inline unsigned           queryUSecs() const { return (unsigned)timeStarted.tv_usec; }
#endif
    inline unsigned           queryProcessID() const { return processID; }
    inline unsigned           queryThreadID() const { return threadID; }
    inline LogMsgSessionId    querySessionID() const { return sessionID; }
    inline const SocketEndpoint * queryNode() const { return &node; }
    void                      serialize(MemoryBuffer & out) const;
    void                      deserialize(MemoryBuffer & in);
private:
    LogMsgId                  id;
#ifdef _WIN32
    time_t                     timeStarted;
#else
    struct timeval            timeStarted;
#endif
    unsigned                  processID;
    unsigned                  threadID;
    LogMsgSessionId           sessionID;
    SocketEndpoint            node;
};

// Info about job generating log message, provided by user (this info is dynamic, determined at run-time)

class jlib_decl LogMsgJobInfo
{
public:
    LogMsgJobInfo(LogMsgJobId _job = UnknownJob, LogMsgUserId _user = UnknownUser) : jobID(_job), userID(_user) {}
    inline LogMsgJobId        queryJobID() const { return jobID; }
    inline LogMsgUserId       queryUserID() const { return userID; }
    void                      serialize(MemoryBuffer & out) const { out.append(jobID).append(userID); }
    void                      deserialize(MemoryBuffer & in) { in.read(jobID).read(userID); }
private:
    LogMsgJobId               jobID;
    LogMsgUserId              userID;
};

class jlib_decl LogMsg : public CInterface
{
public:
    LogMsg() : category(), sysInfo(), jobInfo(), remoteFlag(false) {}
    LogMsg(const LogMsgCategory & _cat, LogMsgId _id, const LogMsgJobInfo & _jobInfo, LogMsgCode _code, const char * _text, unsigned _compo, unsigned port, LogMsgSessionId session) : category(_cat), sysInfo(_id, port, session), jobInfo(_jobInfo), msgCode(_code), component(_compo), remoteFlag(false) { text.append(_text); }
    LogMsg(const LogMsgCategory & _cat, LogMsgId _id, const LogMsgJobInfo & _jobInfo, LogMsgCode _code, const char * format, va_list args,
           unsigned _compo, unsigned port, LogMsgSessionId session)  __attribute__((format(printf,6, 0)))
    : category(_cat), sysInfo(_id, port, session), jobInfo(_jobInfo), msgCode(_code), component(_compo), remoteFlag(false) { text.valist_appendf(format, args); }
    LogMsg(MemoryBuffer & in) { deserialize(in, false); }
    StringBuffer &            toStringPlain(StringBuffer & out, unsigned fields = MSGFIELD_all) const;
    StringBuffer &            toStringXML(StringBuffer & out, unsigned fields = MSGFIELD_all) const;
    StringBuffer &            toStringTable(StringBuffer & out, unsigned fields = MSGFIELD_all) const;
    static StringBuffer &     toStringTableHead(StringBuffer & out, unsigned fields = MSGFIELD_all);
    void                      fprintPlain(FILE * handle, unsigned fields = MSGFIELD_all) const;
    void                      fprintXML(FILE * handle, unsigned fields = MSGFIELD_all) const;
    void                      fprintTable(FILE * handle, unsigned fields = MSGFIELD_all) const;
    static void               fprintTableHead(FILE * handle, unsigned fields = MSGFIELD_all);
    inline const LogMsgCategory  queryCategory() const { return category; }
    inline const LogMsgSysInfo & querySysInfo() const { return sysInfo; }
    inline const LogMsgJobInfo & queryJobInfo() const { return jobInfo; }
    inline unsigned           queryComponent() const { return component; }
    inline LogMsgCode         queryCode() const { return msgCode; }
    inline const char *       queryText() const { return text.str(); }
    void                      serialize(MemoryBuffer & out) const { category.serialize(out); sysInfo.serialize(out); jobInfo.serialize(out); out.append(msgCode); text.serialize(out); }
    void                      deserialize(MemoryBuffer & in, bool remote = false) { category.deserialize(in); sysInfo.deserialize(in); jobInfo.deserialize(in); in.read(msgCode); text.clear(); text.deserialize(in); remoteFlag = remote; }
    bool                      queryRemoteFlag() const { return remoteFlag; }
protected:
    LogMsgCategory            category;
    LogMsgSysInfo             sysInfo;
    LogMsgJobInfo             jobInfo;
    LogMsgCode                msgCode;
    unsigned                  component;
    StringBuffer              text;
    bool                      remoteFlag;
};

// INTERFACES

// Filter for log messages --- contains method to accept or reject messages

interface jlib_decl ILogMsgFilter : public IInterface
{
 public:
    virtual bool              includeMessage(const LogMsg & msg) const = 0;
    virtual bool              mayIncludeCategory(const LogMsgCategory & cat) const = 0;
    virtual unsigned          queryAudienceMask() const = 0;
    virtual unsigned          queryClassMask() const = 0;
    virtual LogMsgDetail      queryMaxDetail() const = 0;
    virtual bool              isCategoryFilter() const { return false; }
    virtual void              serialize(MemoryBuffer & out, bool preserveLocal) const = 0;
    virtual void              addToPTree(IPropertyTree * tree) const = 0;
    virtual bool              queryLocalFlag() const { return false; }
};

// Handler for log messages --- contains method to write or send messages

interface jlib_decl ILogMsgHandler : public IInterface
{
 public:
    virtual void              handleMessage(const LogMsg & msg) const = 0;
    virtual bool              needsPrep() const = 0;
    virtual void              prep() = 0;
    virtual unsigned          queryMessageFields() const = 0;
    virtual void              setMessageFields(unsigned _fields = MSGFIELD_all) = 0;
    virtual void              addToPTree(IPropertyTree * parent) const = 0;
    virtual int               flush() { return 0; }
    virtual char const *      disable() { return 0; }
    virtual void              enable() {}
    virtual bool              getLogName(StringBuffer &name) const = 0;
};

// Class on manager's list of children which sends new filters to children, and holds thread which receives log messages

class jlib_decl ILogMsgLinkToChild : public IInterface
{
public:
    virtual void              sendFilter(ILogMsgFilter * filter) const = 0;
    virtual void              sendFilterOwn(ILogMsgFilter * filter) const = 0;
    virtual void              connect() = 0;
    virtual void              disconnect() = 0;
    virtual bool              queryConnected() const = 0;
    virtual void              markDisconnected() = 0;
};

// Manager to receive log messages, filter, and pass to handlers

interface jlib_decl ILogMsgListener : public IInterface
{
    virtual bool              addMonitor(ILogMsgHandler * handler, ILogMsgFilter * filter) = 0;
    virtual bool              addMonitorOwn(ILogMsgHandler * handler, ILogMsgFilter * filter) = 0;
    virtual bool              removeMonitor(ILogMsgHandler * handler) = 0;
    typedef bool HandlerTest(ILogMsgHandler * handler);
    virtual unsigned          removeMonitorsMatching(HandlerTest & test) = 0;
    virtual void              removeAllMonitors() = 0;
    virtual bool              isActiveMonitor(const ILogMsgHandler * handler) const = 0;
    virtual ILogMsgFilter *   queryMonitorFilter(const ILogMsgHandler * handler) const = 0;
    virtual ILogMsgFilter *   getMonitorFilter(const ILogMsgHandler * handler) const = 0;
    virtual bool              changeMonitorFilter(const ILogMsgHandler * handler, ILogMsgFilter * newFilter) = 0;
    virtual bool              changeMonitorFilterOwn(const ILogMsgHandler * handler, ILogMsgFilter * newFilter) = 0;
    virtual void              prepAllHandlers() const = 0;
    virtual void              addChildOwn(ILogMsgLinkToChild * child) = 0;
    virtual void              removeChild(ILogMsgLinkToChild * child) = 0;
    virtual void              removeAllChildren() = 0;
    virtual ILogMsgFilter *   getCompoundFilter(bool locked = false) const = 0;
    virtual void              suspendChildren() = 0;
    virtual void              unsuspendChildren() = 0;
    virtual bool              addMonitorToPTree(const ILogMsgHandler * handler, IPropertyTree * tree) const = 0;
    virtual void              addAllMonitorsToPTree(IPropertyTree * tree) const = 0;
    virtual void              setPort(unsigned _port) = 0;
    virtual unsigned          queryPort() const = 0;
    virtual void              setSession(LogMsgSessionId _session) = 0;
    virtual LogMsgSessionId   querySession() const = 0;
};

interface jlib_decl ILogMsgManager : public ILogMsgListener
{
 public:    
    virtual void              enterQueueingMode() = 0;
    virtual void              setQueueBlockingLimit(unsigned lim) = 0;
    virtual void              setQueueDroppingLimit(unsigned lim, unsigned numToDrop) = 0;
    virtual void              resetQueueLimit() = 0;
    virtual bool              flushQueue(unsigned timeout) = 0;
    virtual void              resetMonitors() = 0;
    virtual void              report(const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 3, 4))) = 0;
    virtual void              report_va(const LogMsgCategory & cat, const char * format, va_list args) = 0;
    virtual void              report(const LogMsgCategory & cat, LogMsgCode code , const char * format, ...) __attribute__((format(printf, 4, 5))) = 0;
    virtual void              report_va(const LogMsgCategory & cat, LogMsgCode code , const char * format, va_list args) = 0;
    virtual void              report(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL) = 0;
    virtual void              report(unsigned compo, const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 4, 5))) = 0;
    virtual void              report_va(unsigned compo, const LogMsgCategory & cat, const char * format, va_list args) = 0;
    virtual void              report(unsigned compo, const LogMsgCategory & cat, LogMsgCode code , const char * format, ...) __attribute__((format(printf, 5, 6))) = 0;
    virtual void              report_va(unsigned compo, const LogMsgCategory & cat, LogMsgCode code , const char * format, va_list args) = 0;
    virtual void              report(unsigned compo, const LogMsgCategory & cat, const IException * e, const char * prefix = NULL) = 0;
    virtual void              report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...) __attribute__((format(printf, 4, 5))) = 0;
    virtual void              report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args) = 0;
    virtual void              report(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code , const char * format, ...) __attribute__((format(printf, 5, 6))) = 0;
    virtual void              report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code , const char * format, va_list args) = 0;
    virtual void              report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * e, const char * prefix = NULL) = 0;
    virtual void              report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...) __attribute__((format(printf, 5, 6))) = 0;
    virtual void              report_va(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args) = 0;
    virtual void              report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code , const char * format, ...) __attribute__((format(printf, 6, 7))) = 0;
    virtual void              report_va(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code , const char * format, va_list args) = 0;
    virtual void              report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * e, const char * prefix = NULL) = 0;
    virtual void              report(const LogMsg & msg) const = 0;
    virtual LogMsgId          getNextID() = 0;
    virtual bool              rejectsCategory(const LogMsgCategory & cat) const = 0;
};

// CONCRETE CLASSES

// Class which mimics the report methods of a manager, registering an additional component field

class jlib_decl LogMsgComponentReporter
{
public:
    LogMsgComponentReporter(unsigned _compo) : component(_compo) {}
    void                  report(const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 3, 4)));
    void                  report_va(const LogMsgCategory & cat, const char * format, va_list args);
    void                  report(const LogMsgCategory & cat, LogMsgCode code, const char * format, ...) __attribute__((format(printf, 4, 5)));
    void                  report_va(const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args);
    void                  report(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL);
    void                  report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...) __attribute__((format(printf, 4, 5)));
    void                  report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args);
    void                  report(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, ...) __attribute__((format(printf, 5, 6)));
    void                  report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, va_list args);
    void                  report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * e, const char * prefix = NULL);
    void                  report(const LogMsg & msg);
private:
    unsigned                  component;
};

// Class which mimics the report methods of a manager, prepending the given file and line (intended only for use in the FLLOG macro, below)

class jlib_decl LogMsgPrepender
{
public:
    LogMsgPrepender(LogMsgComponentReporter * r, char const * f, unsigned l) : reporter(r), file(f), line(l) { }
    void                      report(const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 3, 4)));
    void                      report_va(const LogMsgCategory & cat, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, LogMsgCode code, const char * format, ...) __attribute__((format(printf, 4, 5)));
    void                      report_va(const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL);
    void                      report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...) __attribute__((format(printf, 4, 5)));
    void                      report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, ...) __attribute__((format(printf, 5, 6)));
    void                      report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * e, const char * prefix = NULL);
    IException *              report(IException * e, const char * prefix = NULL, LogMsgClass cls = MSGCLS_error); // uses MCexception(e, cls), unknownJob, handy for EXCLOG
private:
    LogMsgComponentReporter * reporter;
    char const *              file;
    unsigned                  line;
};

// FUNCTIONS, DATA, AND MACROS

// Function to get filters and handlers

extern jlib_decl ILogMsgFilter * getDefaultLogMsgFilter();
extern jlib_decl ILogMsgFilter * getPassAllLogMsgFilter();
extern jlib_decl ILogMsgFilter * getLocalLogMsgFilter();
extern jlib_decl ILogMsgFilter * getPassNoneLogMsgFilter();
extern jlib_decl ILogMsgFilter * queryPassAllLogMsgFilter();
extern jlib_decl ILogMsgFilter * queryLocalLogMsgFilter();
extern jlib_decl ILogMsgFilter * queryPassNoneLogMsgFilter();
extern jlib_decl ILogMsgFilter * getCategoryLogMsgFilter(unsigned audiences = MSGAUD_all, unsigned classes = MSGCLS_all, LogMsgDetail maxDetail = TopDetail, bool local = false);
extern jlib_decl ILogMsgFilter * getPIDLogMsgFilter(unsigned pid, bool local = false);
extern jlib_decl ILogMsgFilter * getTIDLogMsgFilter(unsigned tid, bool local = false);
extern jlib_decl ILogMsgFilter * getNodeLogMsgFilter(const char * name, unsigned port = 0, bool local = false);
extern jlib_decl ILogMsgFilter * getNodeLogMsgFilter(const IpAddress & ip, unsigned port = 0, bool local = false);
extern jlib_decl ILogMsgFilter * getNodeLogMsgFilter(unsigned port, bool local = false);
extern jlib_decl ILogMsgFilter * getIpLogMsgFilter(const char * name, bool local = false);
extern jlib_decl ILogMsgFilter * getIpLogMsgFilter(const IpAddress & ip, bool local = false);
extern jlib_decl ILogMsgFilter * getIpLogMsgFilter(bool local = false);
extern jlib_decl ILogMsgFilter * getJobLogMsgFilter(LogMsgJobId job, bool local = false);
extern jlib_decl ILogMsgFilter * getUserLogMsgFilter(LogMsgUserId user, bool local = false);
extern jlib_decl ILogMsgFilter * getSessionLogMsgFilter(LogMsgSessionId session, bool local = false);
extern jlib_decl ILogMsgFilter * getComponentLogMsgFilter(unsigned component, bool local = false);
extern jlib_decl ILogMsgFilter * getRegexLogMsgFilter(const char *regex, bool local = false);
extern jlib_decl ILogMsgFilter * getNotLogMsgFilter(ILogMsgFilter * arg);
extern jlib_decl ILogMsgFilter * getNotLogMsgFilterOwn(ILogMsgFilter * arg);
extern jlib_decl ILogMsgFilter * getAndLogMsgFilter(ILogMsgFilter * arg1, ILogMsgFilter * arg2);
extern jlib_decl ILogMsgFilter * getAndLogMsgFilterOwn(ILogMsgFilter * arg1, ILogMsgFilter * arg2);
extern jlib_decl ILogMsgFilter * getOrLogMsgFilter(ILogMsgFilter * arg1, ILogMsgFilter * arg2);
extern jlib_decl ILogMsgFilter * getOrLogMsgFilterOwn(ILogMsgFilter * arg1, ILogMsgFilter * arg2);
extern jlib_decl ILogMsgFilter * getSwitchLogMsgFilterOwn(ILogMsgFilter * switchFilter, ILogMsgFilter * yesFilter, ILogMsgFilter * noFilter);

extern jlib_decl ILogMsgHandler * getHandleLogMsgHandler(FILE * handle = stderr, unsigned fields = MSGFIELD_all, bool writeXML = false);
extern jlib_decl ILogMsgHandler * getFileLogMsgHandler(const char * filename, const char * headertext = 0, unsigned fields = MSGFIELD_all, bool writeXML = true, bool append = false, bool flushes = true);
extern jlib_decl ILogMsgHandler * getRollingFileLogMsgHandler(const char * filebase, const char * fileextn, unsigned fields = MSGFIELD_all, bool append = false, bool flushes = true, const char *initialName = NULL, const char *alias = NULL, bool daily = false);
extern jlib_decl ILogMsgHandler * getBinLogMsgHandler(const char * filename, bool append = false);

// Function to install switch filter into a monitor, switch some messages to new filter whilst leaving rest to previous filter

extern jlib_decl void installLogMsgFilterSwitch(ILogMsgHandler * handler, ILogMsgFilter * switchFilter, ILogMsgFilter * newFilter);

// Functions to make standard handlers and catagory filters and add to manager

extern jlib_decl ILogMsgHandler * attachStandardFileLogMsgMonitor(const char * filename, const char * headertext = 0, unsigned fields = MSGFIELD_all, unsigned audiences = MSGAUD_all, unsigned classes = MSGCLS_all, LogMsgDetail detail = TopDetail, bool writeXML = true, bool append = false, bool flushes = true, bool local = false);
extern jlib_decl ILogMsgHandler * attachStandardBinLogMsgMonitor(const char * filename, unsigned audiences = MSGAUD_all, unsigned classes = MSGCLS_all, LogMsgDetail detail = TopDetail, bool append = false, bool local = false);
extern jlib_decl ILogMsgHandler * attachStandardHandleLogMsgMonitor(FILE * handle = stderr, unsigned fields = MSGFIELD_all, unsigned audiences = MSGAUD_all, unsigned classes = MSGCLS_all, LogMsgDetail detail = TopDetail, bool writeXML = false, bool local = false);

// Function to construct filter from serialized and XML forms, and construct handler from XML form, and attach monitor(s) from XML form

extern jlib_decl ILogMsgFilter * getDeserializedLogMsgFilter(MemoryBuffer & in);
extern jlib_decl ILogMsgFilter * getLogMsgFilterFromPTree(IPropertyTree * tree);
extern jlib_decl ILogMsgHandler * getLogMsgHandlerFromPTree(IPropertyTree * tree);
extern jlib_decl ILogMsgHandler * attachLogMsgMonitorFromPTree(IPropertyTree * tree);     // Takes tree containing <handler> and <filter> elements
extern jlib_decl void attachManyLogMsgMonitorsFromPTree(IPropertyTree * tree);            // Takes tree containing many <monitor> elements

// Extern standard categories and unknown jobInfo

extern jlib_decl const LogMsgCategory MCdisaster;
extern jlib_decl const LogMsgCategory MCuserError;
#define MCerror MCuserError
extern jlib_decl const LogMsgCategory MCoperatorError;
extern jlib_decl const LogMsgCategory MCinternalError;
extern jlib_decl const LogMsgCategory MCuserWarning;
#define MCwarning MCuserWarning
extern jlib_decl const LogMsgCategory MCoperatorWarning;
extern jlib_decl const LogMsgCategory MCinternalWarning;
// MCexception(e, cls) is now a convenient function to get a message category with its audience taken from an exception
inline LogMsgCategory MCexception(IException * e, LogMsgClass cls = MSGCLS_error) { return LogMsgCategory((e)->errorAudience(),cls); }
extern jlib_decl const LogMsgCategory MCuserProgress;
#define MCprogress MCuserProgress
extern jlib_decl const LogMsgCategory MCoperatorProgress;
extern jlib_decl const LogMsgCategory MCdebugProgress;
extern jlib_decl const LogMsgCategory MCdebugInfo;
extern jlib_decl const LogMsgCategory MCstats;
extern jlib_decl const LogMsgCategory MCevent;
extern jlib_decl const LogMsgCategory MClegacy;

extern jlib_decl const LogMsgJobInfo unknownJob;

// Function to return manager, standard handler and the reporters, and the handler's message fields

extern jlib_decl ILogMsgManager * queryLogMsgManager();
extern jlib_decl ILogMsgHandler * queryStderrLogMsgHandler();
extern jlib_decl LogMsgComponentReporter * queryLogMsgComponentReporter(unsigned compo);

extern jlib_decl ILogMsgManager * createLogMsgManager(); // use with care! (needed by mplog listener facility)

// Macros to make logging as simple as possible

#ifdef LOGMSGCOMPONENT
#define LOGMSGREPORTER queryLogMsgComponentReporter(LOGMSGCOMPONENT)
#define FLLOG LogMsgPrepender(LOGMSGREPORTER, __FILE__, __LINE__).report
#else // LOGMSGCOMPONENT
#define LOGMSGREPORTER queryLogMsgManager()
#define FLLOG LogMsgPrepender(NULL, __FILE__, __LINE__).report
#endif // LOGMSGCOMPONENT

#ifdef LOGMSGCOMPONENT
#else // LOGMSGCOMPONENT
#endif // LOGMSGCOMPONENT

#ifdef _PROFILING_WITH_TRUETIME_
//It can't cope with the macro definition..... at least v6.5 can't.
inline void LOG(const LogMsg & msg)
{
    LOGMSGREPORTER->report(msg);
}
void LOG(const LogMsgCategory & cat, const char * format, ...) __attribute__((format(printf, 2, 3)));
inline void LOG(const LogMsgCategory & cat, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    LOGMSGREPORTER->report_va(cat, format, args);
    va_end(args);
}
void LOG(const LogMsgCategory & cat, LogMsgCode code , const char * format, ...) __attribute__((format(printf, 3, 4)));
inline void LOG(const LogMsgCategory & cat, LogMsgCode code , const char * format, ...)
{
    va_list args;
    va_start(args, format);
    LOGMSGREPORTER->report_va(cat, code , format, args);
    va_end(args);
}
inline void LOG(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...) __attribute__((format(printf, 3, 4)));
inline void LOG(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    LOGMSGREPORTER->report_va(cat, job, format, args);
    va_end(args);
}
inline void LOG(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code , const char * format, ...) __attribute__((format(printf, 4, 5)));
inline void LOG(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code , const char * format, ...)
{
    va_list args;
    va_start(args, format);
    LOGMSGREPORTER->report_va(cat, job, code , format, args);
    va_end(args);
}
inline void LOG(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL)
{
    LOGMSGREPORTER->report(cat, e, prefix);
}
inline void LOG(const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * e, const char * prefix = NULL)
{
    LOGMSGREPORTER->report(cat, job, e, prefix);
}
inline void VALOG(const LogMsgCategory & cat, const char * format, va_list args)
{
    LOGMSGREPORTER->report_va(cat, format, args);
}
inline void VALOG(const LogMsgCategory & cat, LogMsgCode code , const char * format, va_list args)
{
    LOGMSGREPORTER->report_va(cat, code , format, args);
}
inline void VALOG(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args)
{
    LOGMSGREPORTER->report_va(cat, job, format, args);
}
inline void VALOG(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code , const char * format, va_list args)
{
    LOGMSGREPORTER->report_va(cat, job, code , format, args);
}
#else
#define LOG LOGMSGREPORTER->report
#define VALOG LOGMSGREPORTER->report_va
#endif

#define INTLOG(category, job, expr) LOGMSGREPORTER->report(category, job, #expr"=%d", expr)
#define OCTLOG(category, job, expr) LOGMSGREPORTER->report(category, job, #expr"=0%o", expr)
#define HEXLOG(category, job, expr) LOGMSGREPORTER->report(category, job, #expr"=0x%X", expr)
#define DBLLOG(category, job, expr) LOGMSGREPORTER->report(category, job, #expr"=%lg", expr)
#define CHRLOG(category, job, expr) LOGMSGREPORTER->report(category, job, #expr"=%c", expr)
#define STRLOG(category, job, expr) LOGMSGREPORTER->report(category, job, #expr"='%s'", expr)
#define TOSTRLOG(category, job, prefix, func) { if (!REJECTLOG(category)) { StringBuffer buff; func(buff); LOGMSGREPORTER->report(category, job, prefix"'%s'", buff.str()); } }
#define EVENTLOG(code, extra) LOGMSGREPORTER->report(MCevent, unknownJob, code, extra)

inline void DBGLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void DBGLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdebugInfo, unknownJob, format, args);
    va_end(args);
}

inline void DISLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void DISLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdisaster, unknownJob, format, args);
    va_end(args);
}

inline void ERRLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void ERRLOG(char const * format, ...) 
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserError, unknownJob, format, args);
    va_end(args);
}

inline void OERRLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void OERRLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCoperatorError, unknownJob, format, args);
    va_end(args);
}

inline void IERRLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void IERRLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCinternalError, unknownJob, format, args);
    va_end(args);
}

inline void WARNLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void WARNLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserWarning, unknownJob, format, args);
    va_end(args);
}

inline void PROGLOG(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void PROGLOG(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCprogress, unknownJob, format, args);
    va_end(args);
}

inline void DBGLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void DBGLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdebugInfo, unknownJob, code, format, args);
    va_end(args);
}

inline void DISLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void DISLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCdisaster, unknownJob, code, format, args);
    va_end(args);
}

inline void ERRLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void ERRLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserError, unknownJob, code, format, args);
    va_end(args);
}

inline void IERRLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void IERRLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCinternalError, unknownJob, code, format, args);
    va_end(args);
}

inline void WARNLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void WARNLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCuserWarning, unknownJob, code, format, args);
    va_end(args);
}

inline void PROGLOG(LogMsgCode code, char const * format, ...) __attribute__((format(printf, 2, 3)));
inline void PROGLOG(LogMsgCode code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCprogress, unknownJob, code, format, args);
    va_end(args);
}

inline IException *DBGLOG(IException *except, const char *prefix=NULL)
{
    LOG(MCdebugInfo, except, prefix);
    return except;
}

inline IException *DISLOG(IException *except, const char *prefix=NULL)
{
    LOG(MCdisaster, except, prefix);
    return except;
}

#define EXCLOG FLLOG

#define FILELOG attachStandardFileLogMsgMonitor
#define BINLOG attachStandardBinLogMsgMonitor
#define HANDLELOG attachStandardHandleLogMsgMonitor
inline void removeLog() { queryLogMsgManager()->removeAllMonitors(); queryLogMsgManager()->removeAllChildren(); }
inline void resetLog()  { queryLogMsgManager()->resetMonitors();     queryLogMsgManager()->removeAllChildren(); }
#define PREPLOG queryLogMsgManager()->prepAllHandlers
#define REJECTLOG queryLogMsgManager()->rejectsCategory

#define AUDIT_TYPES_BEGIN typedef enum {
#define MAKE_AUDIT_TYPE(name, type, categoryid, eventid, level) AUDIT_TYPE_##name,
#define AUDIT_TYPES_END NUM_AUDIT_TYPES } AuditType;
#include "jelogtype.hpp"
#undef AUDIT_TYPES_BEGIN
#undef MAKE_AUDIT_TYPE
#undef AUDIT_TYPES_END

class jlib_decl ISysLogEventLogger : public IInterface
{
public:
    virtual bool              log(AuditType auditType, char const * msg) = 0;
    virtual bool              log(AuditType auditType, char const * msg, size32_t datasize, void const * data) = 0;
};

extern jlib_decl ISysLogEventLogger * querySysLogEventLogger();
extern jlib_decl ILogMsgHandler * getSysLogMsgHandler(unsigned fields = MSGFIELD_all);
extern jlib_decl void UseSysLogForOperatorMessages(bool use=true);

#define SYSLOG querySysLogEventLogger()->log
#define AUDIT SYSLOG                               // bwd compatibility

extern jlib_decl void AuditSystemAccess(const char *userid, bool success, char const * msg,...) __attribute__((format(printf, 3, 4)));

/***************************************************************************/
/* The simplest logging commands are:                                      */
/*   DBGLOG(format, ...) [for temporary logs while debugging, MCdebugInfo] */
/*   DISLOG(format, ...) [for disasters, MCdisaster]                       */
/*   ERRLOG(format, ...) [for errors reported to user, MCuserError]        */
/*   OERRLOG(format, ...) [for errors reported to operator]                */
/*   IERRLOG(format, ...) [for errors reported to internal & programmer]   */
/*   WARNLOG(format, ...) [for warnings reported to user, MCuserWarning]   */
/*   PROGLOG(format, ...) [for progress logs to user, MCprogress]          */
/* There are equivalent commands which take a LogMsgCode:                  */
/*   DBGLOG(code, format, ...)                                             */
/*   DISLOG(code, format, ...)                                             */
/*   ERRLOG(code, format, ...)                                             */
/*   OERRLOG(code, format, ...)                                            */
/*   IERRLOG(code, format, ...)                                            */
/*   WARNLOG(code, format, ...)                                            */
/*   PROGLOG(code, format, ...)                                            */
/* This takes code, message, & audience from an exception, class is error  */
/*   EXCLOG(exception, prefix)                                             */
/* The more general logging command has the following forms:               */
/*   LOG(category, format, ...)                                            */
/*   LOG(category, code, format, ...)                                      */
/*   LOG(category, job, format, ...)                                       */
/*   LOG(category, job, code, format, ...)                                 */
/* In the first and second cases, the common unknownJob is used.           */
/* There are also forms to log exceptions:                                 */
/*   LOG(category, exception)                                              */
/*   LOG(category, exception, prefix)                                      */
/*   LOG(category, job, exception)                                         */
/*   LOG(category, job, exception, prefix)                                 */
/* The off-the-shelf categories, MCdebugInfo et al, are listed above.      */
/* Their detail level can be modified from the default as MCdebugInfo(50). */
/***************************************************************************/

interface jlib_decl IContextLogger : extends IInterface
{
    void CTXLOG(const char *format, ...) const  __attribute__((format(printf, 2, 3)));
    virtual void CTXLOGva(const char *format, va_list args) const __attribute__((format(printf,2,0))) = 0;
    void logOperatorException(IException *E, const char *file, unsigned line, const char *format, ...) const  __attribute__((format(printf, 5, 6)));
    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const __attribute__((format(printf,5,0))) = 0;
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value) const = 0;
    virtual void mergeStats(const CRuntimeStatisticCollection &from) const = 0;
    virtual unsigned queryTraceLevel() const = 0;
};

extern jlib_decl const IContextLogger &queryDummyContextLogger();

//---------------------------------------------------------------------------

interface IComponentLogFileCreator : extends IInterface
{
    //IComponentLogFileCreator set methods
    virtual void setExtension(const char * _ext) = 0;       //log filename extension (eg ".log")
    virtual void setPrefix(const char * _prefix) = 0;       //filename prefix (eg "master")
    virtual void setName(const char * _name) = 0;           //log filename, overrides default of component name (without extension)
    virtual void setPostfix(const char * _postfix) = 0;     //filename postfix (eg "coalesce")
    virtual void setCreateAliasFile(bool _create) = 0;      //controls creation of hardlink alias file
    virtual void setAliasName(const char * _aliasName) = 0; //alias file name, overrides default of component name
    virtual void setLogDirSubdir(const char * _subdir) = 0; //subdir be appended to config log dir (eg "server" or "audit")
    virtual void setRolling(const bool _rolls) = 0;         //daily rollover to new file
    virtual void setCompleteFilespec(const char * _fs) = 0; //Full filespec (path/fn.ext), overrides everything else

    //ILogMsgHandler fields
    virtual void setAppend(const bool _append) = 0;         //append to existing logfile
    virtual void setFlushes(const bool _flushes) = 0;       //automatically flush
    virtual void setMsgFields(const unsigned _fields) = 0;  //fields/columns to be included in log

    //ILogMsgFilter fields
    virtual void setMsgAudiences(const unsigned _audiences) = 0;    //log audience
    virtual void setMsgClasses(const unsigned _classes) = 0;        //message class
    virtual void setMaxDetail(const LogMsgDetail _maxDetail) = 0;   //message detail
    virtual void setLocal(const bool _local) = 0;                   //local logging

    //query methods (not valid until logging started)
    virtual const char * queryLogDir() const = 0;           //Location of component logfile
    virtual const char * queryLogFileSpec() const = 0;      //Full log filespec
    virtual const char * queryAliasFileSpec() const = 0;    //Full alias filespec, if created

    virtual ILogMsgHandler * beginLogging() = 0;    //begin logging to specified file(s)
};

extern jlib_decl IComponentLogFileCreator * createComponentLogFileCreator(IPropertyTree * _properties, const char *_component);
extern jlib_decl IComponentLogFileCreator * createComponentLogFileCreator(const char *_logDir, const char *_component);
extern jlib_decl IComponentLogFileCreator * createComponentLogFileCreator(const char *_component);
#endif
