/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


#ifndef JLOG_IPP
#define JLOG_IPP

#include <algorithm>
#include "jmutex.hpp"
#include "jlog.hpp"
#include "jiface.hpp"
#include "jarray.hpp"
#include "jsocket.hpp"
#include "jptree.hpp"
#include "jfile.hpp"
#include "jqueue.tpp"
#include "jregexp.hpp"

static unsigned const defaultMsgQueueLimit = 256;
static LogMsgCategory const dropWarningCategory(MSGAUD_operator, MSGCLS_error, 0);

// Initial size of StringBuffer used to build output in LogMsg::toString methods

#define LOG_MSG_FORMAT_BUFFER_LENGTH 1024

// Enum used when serializing IFilter to show derived class

enum
{
    MSGFILTER_passall,
    MSGFILTER_passlocal,
    MSGFILTER_passnone,
    MSGFILTER_category,
    MSGFILTER_pid,
    MSGFILTER_tid,
    MSGFILTER_node,
    MSGFILTER_ip,
    MSGFILTER_job,
    MSGFILTER_user,
    MSGFILTER_session,
    MSGFILTER_component,
    MSGFILTER_not,
    MSGFILTER_and,
    MSGFILTER_or,
    MSGFILTER_switch,
    MSGFILTER_regex
    };



// Implementations of filter which pass all or no messages

//MORE: This would benefit from more code moved into this base class
class CLogMsgFilter : implements ILogMsgFilter, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
};


class PassAllLogMsgFilter : public CLogMsgFilter
{
public:
    bool                      includeMessage(const LogMsg & msg) const { return true; }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_passall); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return false; }
};

class PassLocalLogMsgFilter : public CLogMsgFilter
{
public:
    bool                      includeMessage(const LogMsg & msg) const { return !msg.queryRemoteFlag(); }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(preserveLocal ? MSGFILTER_passlocal : MSGFILTER_passall); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return true; }
};

class PassNoneLogMsgFilter : public CLogMsgFilter
{
public:
    bool                      includeMessage(const LogMsg & msg) const { return false; }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return false; }
    unsigned                  queryAudienceMask() const { return 0; }
    unsigned                  queryClassMask() const { return 0; }
    LogMsgDetail              queryMaxDetail() const { return 0; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_passnone); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return true; }
};

// Implementation of filter which passes messages by category masks

class CategoryLogMsgFilter : public CLogMsgFilter
{
public:
    CategoryLogMsgFilter(unsigned _aMask, unsigned _cMask, LogMsgDetail _dMax, bool local) : audienceMask(_aMask), classMask(_cMask), maxDetail(_dMax), localFlag(local) {}
    CategoryLogMsgFilter(MemoryBuffer & in) { in.read(audienceMask); in.read(classMask); in.read(maxDetail); in.read(localFlag); }
    CategoryLogMsgFilter(IPropertyTree * tree) { audienceMask = tree->getPropInt("@audience", MSGAUD_all); classMask = tree->getPropInt("@class", MSGCLS_all); maxDetail = tree->getPropInt("@detail", TopDetail); localFlag = tree->hasProp("@local"); }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return includeCategory(msg.queryCategory()); }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return includeCategory(cat); }
    unsigned                  queryAudienceMask() const { return audienceMask; }
    unsigned                  queryClassMask() const { return classMask; }
    LogMsgDetail              queryMaxDetail() const { return maxDetail; }
    bool                      isCategoryFilter() const { return true; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_category).append(audienceMask).append(classMask).append(maxDetail); out.append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    void                      orWithFilter(const ILogMsgFilter * filter);
    void                      reset();
    bool                      includeCategory(const LogMsgCategory & category) const { return (category.queryAudience() & audienceMask) && (category.queryClass() & classMask) && (category.queryDetail() <= maxDetail); }
    bool                      queryLocalFlag() const { return localFlag; }
protected:
    unsigned                  audienceMask;
    unsigned                  classMask;
    LogMsgDetail              maxDetail;
    bool                      localFlag;
};

// Implementations of filters using sysInfo

class PIDLogMsgFilter : public CLogMsgFilter
{
public:
    PIDLogMsgFilter(unsigned _pid, bool local) : pid(_pid), localFlag(local) {}
    PIDLogMsgFilter(MemoryBuffer & in) { in.read(pid); in.read(localFlag); }
    PIDLogMsgFilter(IPropertyTree * tree) { pid = tree->getPropInt("@pid", 0); localFlag = tree->hasProp("@local"); }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return msg.querySysInfo().queryProcessID() == pid; }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_pid).append(pid).append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    unsigned                  pid;
    bool                      localFlag;
};

class TIDLogMsgFilter : public CLogMsgFilter
{
public:
    TIDLogMsgFilter(unsigned _tid, bool local) : tid(_tid), localFlag(local) {}
    TIDLogMsgFilter(MemoryBuffer & in) { in.read(tid); in.read(localFlag); }
    TIDLogMsgFilter(IPropertyTree * tree) { tid = tree->getPropInt("@tid", 0); localFlag = tree->hasProp("@local"); }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return msg.querySysInfo().queryThreadID() == tid; }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_tid).append(tid).append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    unsigned                  tid;
    bool                      localFlag;
};

class NodeLogMsgFilter : public CLogMsgFilter
{
public:
    NodeLogMsgFilter(const char * name, unsigned port, bool local) : node(name, port), localFlag(local) {}
    NodeLogMsgFilter(const IpAddress & ip, unsigned port, bool local) : node(port, ip), localFlag(local) {}
    NodeLogMsgFilter(unsigned port, bool local) : node(port), localFlag(local) {}
    NodeLogMsgFilter(MemoryBuffer & in) { node.deserialize(in); in.read(localFlag); }
    NodeLogMsgFilter(IPropertyTree * tree)
    {
        StringBuffer buff;
        tree->getProp("@ip", buff);
        node.set(buff.str(), tree->getPropInt("@port", 0));
        localFlag = tree->hasProp("@local");
    }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return msg.querySysInfo().queryNode()->equals(node); }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_node); node.serialize(out); out.append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    SocketEndpoint            node;
    bool localFlag;
};

class IpLogMsgFilter : public CLogMsgFilter
{
public:
    IpLogMsgFilter(const char * name, bool local) : ip(name), localFlag(local) {}
    IpLogMsgFilter(const IpAddress & _ip, bool local) : localFlag(local), ip(_ip) 
    {
    }
    IpLogMsgFilter(bool local) : localFlag(local) { GetHostIp(ip); }
    IpLogMsgFilter(MemoryBuffer & in) { ip.ipdeserialize(in); in.read(localFlag); }
    IpLogMsgFilter(IPropertyTree * tree) : ip(tree->queryProp("@ip")) { localFlag = tree->hasProp("@local"); }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return msg.querySysInfo().queryNode()->ipequals(ip); }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_ip); ip.ipserialize(out); out.append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    IpAddress                 ip;
    bool                      localFlag;
};

class SessionLogMsgFilter : public CLogMsgFilter
{
public:
    SessionLogMsgFilter(LogMsgSessionId _session, bool local) : session(_session), localFlag(local) {}
    SessionLogMsgFilter(MemoryBuffer & in) { in.read(session); in.read(localFlag); }
    SessionLogMsgFilter(IPropertyTree * tree) { session = tree->getPropInt64("@session", UnknownSession); localFlag = tree->hasProp("@local"); }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return msg.querySysInfo().querySessionID() == session; }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_session).append(session).append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    LogMsgSessionId           session;
    bool                      localFlag;
};

// Implementations of filters using job info

class JobLogMsgFilter : public CLogMsgFilter
{
public:
    JobLogMsgFilter(LogMsgJobId _job, bool local) : job(_job), localFlag(local) {}
    JobLogMsgFilter(MemoryBuffer & in) { in.read(job); in.read(localFlag); }
    JobLogMsgFilter(IPropertyTree * tree) { job = tree->getPropInt64("@job", UnknownJob); localFlag = tree->hasProp("@local"); }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return msg.queryJobInfo().queryJobID() == job; }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_job).append(job).append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    LogMsgJobId               job;
    bool                      localFlag;
};

class UserLogMsgFilter : public CLogMsgFilter
{
public:
    UserLogMsgFilter(LogMsgUserId _user, bool local) : user(_user), localFlag(local) {}
    UserLogMsgFilter(MemoryBuffer & in) { in.read(user); in.read(localFlag); }
    UserLogMsgFilter(IPropertyTree * tree) { user = tree->getPropInt64("@user", UnknownUser); localFlag = tree->hasProp("@local"); }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return msg.queryJobInfo().queryUserID() == user; }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_user).append(user).append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    LogMsgUserId              user;
    bool                      localFlag;
};

// Implementation of filter using component

class ComponentLogMsgFilter : public CLogMsgFilter
{
public:
    ComponentLogMsgFilter(unsigned _compo, bool local) : component(_compo), localFlag(local) {}
    ComponentLogMsgFilter(MemoryBuffer & in) { in.read(component); in.read(localFlag); }
    ComponentLogMsgFilter(IPropertyTree * tree) { component = tree->getPropInt("@component", 0); localFlag = tree->hasProp("@local"); }

    bool                      includeMessage(const LogMsg & msg) const { if(localFlag && msg.queryRemoteFlag()) return false; return msg.queryComponent() == component; }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_component).append(component).append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    unsigned                  component;
    bool                      localFlag;
};

// Implementation of filter using regex

class RegexLogMsgFilter : public CLogMsgFilter
{
public:
    RegexLogMsgFilter(const char *_regexText, bool local) : regexText(_regexText), regex(_regexText), localFlag(local) {}
    RegexLogMsgFilter(MemoryBuffer & in) { in.read(regexText); in.read(localFlag); regex.init(regexText);}
    RegexLogMsgFilter(IPropertyTree * tree) { regexText.set(tree->queryProp("@regex")); localFlag = tree->hasProp("@local"); regex.init(regexText);}

    bool                      includeMessage(const LogMsg & msg) const;
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_regex).append(regexText).append(localFlag && preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return localFlag; }
private:
    mutable SpinLock          lock; // Regexpr is not threadsafe when called from multiple threads
    StringAttr                regexText;
    RegExpr                   regex;
    bool                      localFlag;
};
// Implementations of filters to do logic

class NotLogMsgFilter : public CLogMsgFilter
{
public:
    NotLogMsgFilter(ILogMsgFilter * _arg) : arg(_arg) {}
    NotLogMsgFilter(MemoryBuffer & in) { arg.setown(getDeserializedLogMsgFilter(in)); }
    NotLogMsgFilter(IPropertyTree * tree) { Owned<IPropertyTree> nottree = tree->getPropTree("filter"); arg.setown(getLogMsgFilterFromPTree(nottree)); }

    bool                      includeMessage(const LogMsg & msg) const { return !(arg->includeMessage(msg)); }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return true; }     // can't just invert
    unsigned                  queryAudienceMask() const { return MSGAUD_all; }
    unsigned                  queryClassMask() const { return MSGCLS_all; }
    LogMsgDetail              queryMaxDetail() const { return TopDetail; }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const
    {
        if(preserveLocal)
        {
            out.append(MSGFILTER_not);
            arg->serialize(out, true);
        }
        else
            out.append(MSGFILTER_passall);     // sadly have to lose data here, could transmit a few too many messages because of this
    }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return false; }
private:
    Linked<ILogMsgFilter>     arg;
};

class AndLogMsgFilter : public CLogMsgFilter
{
public:
    AndLogMsgFilter(ILogMsgFilter * _arg1, ILogMsgFilter * _arg2) : arg1(_arg1), arg2(_arg2) {}
    AndLogMsgFilter(MemoryBuffer & in) { arg1.setown(getDeserializedLogMsgFilter(in)); arg2.setown(getDeserializedLogMsgFilter(in)); }
    AndLogMsgFilter(IPropertyTree * tree) 
    { 
        Owned <IPropertyTreeIterator> iter = tree->getElements("filter");
        ForEach(*iter) 
        {
            ILogMsgFilter *filter = getLogMsgFilterFromPTree(&(iter->query()));
            if (!arg1.get())
                arg1.setown(filter);
            else if (!arg2.get())
                arg2.setown(filter);
            else
                arg2.setown(getAndLogMsgFilterOwn(arg2.getClear(), filter));
        }
    }

    bool                      includeMessage(const LogMsg & msg) const { return (arg1->includeMessage(msg)) && (arg2->includeMessage(msg)); }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return (arg1->mayIncludeCategory(cat)) && (arg2->mayIncludeCategory(cat)); }
    unsigned                  queryAudienceMask() const { return arg1->queryAudienceMask() & arg2->queryAudienceMask(); }
    unsigned                  queryClassMask() const { return arg1->queryClassMask() & arg2->queryClassMask(); }
    LogMsgDetail              queryMaxDetail() const { return std::min(arg1->queryMaxDetail(), arg2->queryMaxDetail()); }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_and); arg1->serialize(out, preserveLocal); arg2->serialize(out, preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return arg1->queryLocalFlag() || arg2->queryLocalFlag(); }
private:
    Linked<ILogMsgFilter>     arg1;
    Linked<ILogMsgFilter>     arg2;
};

class OrLogMsgFilter : public CLogMsgFilter
{
public:
    OrLogMsgFilter(ILogMsgFilter * _arg1, ILogMsgFilter * _arg2) : arg1(_arg1), arg2(_arg2) {}
    OrLogMsgFilter(MemoryBuffer & in) { arg1.setown(getDeserializedLogMsgFilter(in)); arg2.setown(getDeserializedLogMsgFilter(in)); }
    OrLogMsgFilter(IPropertyTree * tree) 
    { 
        Owned <IPropertyTreeIterator> iter = tree->getElements("filter");
        ForEach(*iter) 
        {
            ILogMsgFilter *filter = getLogMsgFilterFromPTree(&(iter->query()));
            if (!arg1.get())
                arg1.setown(filter);
            else if (!arg2.get())
                arg2.setown(filter);
            else
                arg2.setown(getOrLogMsgFilterOwn(arg2.getClear(), filter));
        }
    }

    bool                      includeMessage(const LogMsg & msg) const { return (arg1->includeMessage(msg)) || (arg2->includeMessage(msg)); }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const { return (arg1->mayIncludeCategory(cat)) || (arg2->mayIncludeCategory(cat)); }
    unsigned                  queryAudienceMask() const { return arg1->queryAudienceMask() | arg2->queryAudienceMask(); }
    unsigned                  queryClassMask() const { return arg1->queryClassMask() | arg2->queryClassMask(); }
    LogMsgDetail              queryMaxDetail() const { return std::max(arg1->queryMaxDetail(), arg2->queryMaxDetail()); }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const { out.append(MSGFILTER_or); arg1->serialize(out, preserveLocal); arg2->serialize(out, preserveLocal); }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return arg1->queryLocalFlag() && arg2->queryLocalFlag(); }
private:
    Linked<ILogMsgFilter>     arg1;
    Linked<ILogMsgFilter>     arg2;
};

class SwitchLogMsgFilter : public CLogMsgFilter
{
public:
    SwitchLogMsgFilter(ILogMsgFilter * _cond, ILogMsgFilter * _yes, ILogMsgFilter * _no) : cond(_cond), yes(_yes), no(_no) {}
    SwitchLogMsgFilter(MemoryBuffer & in)
    {
        cond.setown(getDeserializedLogMsgFilter(in));
        yes.setown(getDeserializedLogMsgFilter(in));
        no.setown(getDeserializedLogMsgFilter(in));
    }
    SwitchLogMsgFilter(IPropertyTree * tree) 
    {
        cond.setown(getLogMsgFilterFromPTree(tree->queryPropTree("filter[1]")));
        yes.setown(getLogMsgFilterFromPTree(tree->queryPropTree("filter[2]")));
        no.setown(getLogMsgFilterFromPTree(tree->queryPropTree("filter[3]")));
    }

    bool                      includeMessage(const LogMsg & msg) const
    {
        bool swtch = cond->includeMessage(msg);
        return swtch ? yes->includeMessage(msg) : no->includeMessage(msg);
    }
    bool                      mayIncludeCategory(const LogMsgCategory & cat) const
    {
        return yes->mayIncludeCategory(cat) || no->mayIncludeCategory(cat);
    }
    unsigned                  queryAudienceMask() const { return yes->queryAudienceMask() | no->queryAudienceMask(); }
    unsigned                  queryClassMask() const { return yes->queryClassMask() | no->queryClassMask(); }
    LogMsgDetail              queryMaxDetail() const { return std::max(yes->queryMaxDetail(), no->queryMaxDetail()); }
    void                      serialize(MemoryBuffer & out, bool preserveLocal) const
    {
        if(preserveLocal)
        {
            out.append(MSGFILTER_switch);
            cond->serialize(out, true);
            yes->serialize(out, true);
            no->serialize(out, true);
        }
        else
        {
            out.append(MSGFILTER_or);
            out.append(MSGFILTER_and);
            cond->serialize(out, false);
            yes->serialize(out, false);
            no->serialize(out, false);
        }
    }
    void                      addToPTree(IPropertyTree * tree) const;
    bool                      queryLocalFlag() const { return yes->queryLocalFlag() && no->queryLocalFlag(); }
private:
    Linked<ILogMsgFilter>     cond;
    Linked<ILogMsgFilter>     yes;
    Linked<ILogMsgFilter>     no;
};

// Implementations of handlers which writes selected fields to file handle (XML and table output)

class HandleLogMsgHandler : public ILogMsgHandler
{
public:
    HandleLogMsgHandler(FILE * _handle, unsigned _fields) : handle(_handle), messageFields(_fields) {}
    virtual ~HandleLogMsgHandler() { flush(); }

    unsigned                  queryMessageFields() const { return messageFields; }
    void                      setMessageFields(unsigned _fields) { messageFields = _fields; }
    int                       flush() { CriticalBlock block(crit); return fflush(handle); }
    char const *              disable() { crit.enter(); return "HANDLER"; }
    void                      enable() { crit.leave(); }
    bool                      getLogName(StringBuffer &name) const { return false; }
protected:
    FILE *                    handle;
    unsigned                  messageFields;
    mutable CriticalSection   crit;
};

class HandleLogMsgHandlerXML : public CInterface, implements HandleLogMsgHandler
{
public:
    HandleLogMsgHandlerXML(FILE * _handle, unsigned _fields) : HandleLogMsgHandler(_handle, _fields) {}
    IMPLEMENT_IINTERFACE;
    void                      handleMessage(const LogMsg & msg) const { CriticalBlock block(crit); msg.fprintXML(handle, messageFields); }
    bool                      needsPrep() const { return false; }
    void                      prep() {}
    void                      addToPTree(IPropertyTree * tree) const;
};

class HandleLogMsgHandlerTable : public CInterface, implements HandleLogMsgHandler
{
public:
    HandleLogMsgHandlerTable(FILE * _handle, unsigned _fields) : HandleLogMsgHandler(_handle, _fields), prepped(false) {}
    IMPLEMENT_IINTERFACE;
    void                      handleMessage(const LogMsg & msg) const { CriticalBlock block(crit); msg.fprintTable(handle, messageFields); }
    bool                      needsPrep() const { return !prepped; }
    void                      prep() { CriticalBlock block(crit); LogMsg::fprintTableHead(handle, messageFields); prepped = true; }
    void                      addToPTree(IPropertyTree * tree) const;
private:
    bool                      prepped;
};

// Implementations of handlers which writes selected fields to named file (XML and table output)

class FileLogMsgHandler : public ILogMsgHandler
{
public:
    FileLogMsgHandler(const char * _filename, const char * _headerText = 0, unsigned _fields = MSGFIELD_all, bool _append = false, bool _flushes = true);
    virtual ~FileLogMsgHandler();
    unsigned                  queryMessageFields() const { return messageFields; }
    void                      setMessageFields(unsigned _fields) { messageFields = _fields; }
    int                       flush() { CriticalBlock block(crit); return fflush(handle); }
    char const *              disable();
    void                      enable();
    bool                      getLogName(StringBuffer &name) const { name.append(filename); return true; }
                
protected:
    FILE *                    handle;
    unsigned                  messageFields;
    StringAttr                filename;
    StringAttr                headerText;
    bool                      append;
    bool                      flushes;
    mutable CriticalSection   crit;
};

class FileLogMsgHandlerXML : public CInterface, implements FileLogMsgHandler
{
public:
    FileLogMsgHandlerXML(const char * _filename, const char * _headerText = 0, unsigned _fields = MSGFIELD_all, bool _append = false, bool _flushes = true) : FileLogMsgHandler(_filename, _headerText, _fields, _append, _flushes) {}
    IMPLEMENT_IINTERFACE;
    void                      handleMessage(const LogMsg & msg) const { CriticalBlock block(crit); msg.fprintXML(handle, messageFields); if(flushes) fflush(handle); }
    bool                      needsPrep() const { return false; }
    void                      prep() {}
    void                      addToPTree(IPropertyTree * tree) const;
};

class FileLogMsgHandlerTable : public CInterface, implements FileLogMsgHandler
{
public:
    FileLogMsgHandlerTable(const char * _filename, const char * _headerText = 0, unsigned _fields = MSGFIELD_all, bool _append = false, bool _flushes = true) : FileLogMsgHandler(_filename, _headerText, _fields, _append, _flushes), prepped(false) {}
    IMPLEMENT_IINTERFACE;
    void                      handleMessage(const LogMsg & msg) const { CriticalBlock block(crit); msg.fprintTable(handle, messageFields); if(flushes) fflush(handle); }
    bool                      needsPrep() const { return !prepped; }
    void                      prep() { CriticalBlock block(crit); LogMsg::fprintTableHead(handle, messageFields); prepped = true; }
    void                      addToPTree(IPropertyTree * tree) const;
private:
    bool                      prepped;
};

class RollingFileLogMsgHandler : public CInterface, implements ILogMsgHandler
{
public:
    RollingFileLogMsgHandler(const char * _filebase, const char * _fileextn, unsigned _fields = MSGFIELD_all, bool _append = false, bool _flushes = true, const char *initialName = NULL, const char *alias = NULL, bool daily = false);
    virtual ~RollingFileLogMsgHandler();
    IMPLEMENT_IINTERFACE;
    void                      handleMessage(const LogMsg & msg) const
    {
        CriticalBlock block(crit);
        checkRollover();
        msg.fprintTable(handle, messageFields);
        if(flushes) fflush(handle);
    }
    bool                      needsPrep() const { return false; }
    void                      prep() {}
    unsigned                  queryMessageFields() const { return messageFields; }
    void                      setMessageFields(unsigned _fields) { messageFields = _fields; }
    void                      addToPTree(IPropertyTree * tree) const;
    int                       flush() { CriticalBlock block(crit); return fflush(handle); }
    char const *              disable();
    void                      enable();
    bool                      getLogName(StringBuffer &name) const { name.append(filename); return true; }
protected:
    void                      checkRollover() const;
    void                      doRollover(bool daily, const char *forceName = NULL) const;
protected:
    mutable FILE *            handle;
    unsigned                  messageFields;
    StringAttr                alias;
    StringAttr                filebase;
    StringAttr                fileextn;
    mutable StringBuffer      filename;
    bool                      append;
    bool                      flushes;
    mutable CriticalSection   crit;
    mutable struct tm         startTime;
};

// Implementation of handler which writes message to file in binary form

class BinLogMsgHandler : public CInterface, implements ILogMsgHandler
{
public:
    BinLogMsgHandler(const char * _filename, bool _append = false);
    virtual ~BinLogMsgHandler();
    IMPLEMENT_IINTERFACE;
    void                      handleMessage(const LogMsg & msg) const;
    bool                      needsPrep() const { return false; }
    void                      prep() {}
    void                      addToPTree(IPropertyTree * tree) const;
    unsigned                  queryMessageFields() const { return MSGFIELD_all; }
    void                      setMessageFields(unsigned _fields) {}
    int                       flush() { return 0; }
    char const *              disable();
    void                      enable();
    bool                      getLogName(StringBuffer &name) const { name.append(filename); return true; }
protected:
    StringAttr                filename;
    bool                      append;
    OwnedIFile                file;
    OwnedIFileIO              fio;
    OwnedIFileIOStream        fstr;
    mutable MemoryBuffer      mbuff;
    mutable size32_t              msglen;
    mutable CriticalSection   crit;
};

// Implementation of handler which uses the audit event logger

class SysLogMsgHandler : public CInterface, implements ILogMsgHandler
{
public:
    SysLogMsgHandler(ISysLogEventLogger * _logger, unsigned _fields) : logger(_logger), fields(_fields) {}
    IMPLEMENT_IINTERFACE;
    void                      handleMessage(const LogMsg & msg) const;
    bool                      needsPrep() const { return false; }
    void                      prep() {}
    void                      addToPTree(IPropertyTree * tree) const;
    unsigned                  queryMessageFields() const { return fields; }
    void                      setMessageFields(unsigned _fields) { fields = _fields; }
    int                       flush() { return 0; }
    char const *              disable() { return "Audit"; }
    void                      enable() {}
    bool                      getLogName(StringBuffer &name) const { return false; }
protected:
    ISysLogEventLogger *      logger;
    unsigned                  fields;
};

// Monitor to watch for messages --- pairs up a filter with a handler

class LogMsgMonitor : public CInterface
{
public:
    LogMsgMonitor(ILogMsgFilter * _filter, ILogMsgHandler * _handler) : filter(_filter), handler(_handler) {}
    void                      processMessage(const LogMsg & msg) const { if(filter->includeMessage(msg)) handler->handleMessage(msg); }
    ILogMsgFilter *           queryFilter() const { return filter; }
    ILogMsgFilter *           getFilter() const { return LINK(filter); }
    ILogMsgHandler *          queryHandler() const { return handler; }
    ILogMsgHandler *          getHandler() const { return LINK(handler); }
    void                      setFilter(ILogMsgFilter * newFilter) { filter.set(newFilter); }
    void                      setFilterOwn(ILogMsgFilter * newFilter) { filter.setown(newFilter); }
    void                      addToPTree(IPropertyTree * tree) const;
private:
    Linked<ILogMsgFilter>     filter;
    Linked<ILogMsgHandler>    handler;
};

class DropLogMsg;

// Implementation of logging manager

class CLogMsgManager : public CInterface, implements ILogMsgManager
{
private:
    class MsgProcessor : public Thread
    {
    public:
        MsgProcessor(CLogMsgManager * _owner) : Thread("CLogMsgManager::MsgProcessor"), owner(_owner), more(true), droppingLimit(0) {}
        void push(LogMsg * msg);
        virtual int run();
        void notify(LogMsg * msg);
        void stop();
        void setBlockingLimit(unsigned lim);
        void setDroppingLimit(unsigned lim, unsigned num);
        void resetLimit();
        bool flush(unsigned timeout);

    private:
        void drop();

    private:
        CLogMsgManager * owner;
        bool more;
        CallbackInterThreadQueueOf<LogMsg, MsgProcessor, false> q;
        unsigned droppingLimit;
        unsigned numToDrop;
        Mutex pullCycleMutex;
    };
    Owned<MsgProcessor> processor;
    friend class MsgProcessor;
    friend class DropLogMsg;

public:
    CLogMsgManager() : prefilter(0, 0, 0, false), suspendedChildren(false), port(0), session(UnknownSession) { atomic_set(&nextID, 0); }
    ~CLogMsgManager();
    IMPLEMENT_IINTERFACE;
    void                      enterQueueingMode();
    void                      setQueueBlockingLimit(unsigned lim);
    void                      setQueueDroppingLimit(unsigned lim, unsigned numToDrop);
    void                      resetQueueLimit();
    bool                      flushQueue(unsigned timeout) { if(processor) return processor->flush(timeout); else return true; }
    void                      report(const LogMsgCategory & cat, const char * format, ...);
    void                      report_va(const LogMsgCategory & cat, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, LogMsgCode code, const char * format, ...);
    void                      report_va(const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, const IException * e, const char * prefix = NULL);
    void                      report(unsigned compo, const LogMsgCategory & cat, const char * format, ...);
    void                      report_va(unsigned compo, const LogMsgCategory & cat, const char * format, va_list args);
    void                      report(unsigned compo, const LogMsgCategory & cat, LogMsgCode code, const char * format, ...);
    void                      report_va(unsigned compo, const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args);
    void                      report(unsigned compo, const LogMsgCategory & cat, const IException * e, const char * prefix = NULL);
    void                      report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...);
    void                      report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, ...);
    void                      report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, va_list args);
    void                      report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * e, const char * prefix = NULL);
    void                      report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...);
    void                      report_va(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args);
    void                      report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, ...);
    void                      report_va(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, va_list args);
    void                      report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * e, const char * prefix = NULL);
    void                      report(const LogMsg & msg) const { if(prefilter.includeCategory(msg.queryCategory())) doReport(msg); }
    bool                      addMonitor(ILogMsgHandler * handler, ILogMsgFilter * filter);
    bool                      addMonitorOwn(ILogMsgHandler * handler, ILogMsgFilter * filter);
    bool                      removeMonitor(ILogMsgHandler * handler);
    unsigned                  removeMonitorsMatching(HandlerTest & test);
    void                      removeAllMonitors();
    void                      resetMonitors();
    ILogMsgFilter *           queryMonitorFilter(const ILogMsgHandler * handler) const;
    ILogMsgFilter *           getMonitorFilter(const ILogMsgHandler * handler) const { return LINK(queryMonitorFilter(handler)); }
    bool                      isActiveMonitor(const ILogMsgHandler * handler) const { return (find(handler) != NotFound); }
    bool                      changeMonitorFilter(const ILogMsgHandler * handler, ILogMsgFilter * newFilter);
    bool                      changeMonitorFilterOwn(const ILogMsgHandler * handler, ILogMsgFilter * newFilter) { bool ret = changeMonitorFilter(handler, newFilter); newFilter->Release(); return ret; }
    LogMsgId                  getNextID() { return static_cast<LogMsgId>(atomic_add_exchange(&nextID, 1)); }
    void                      prepAllHandlers() const;
    void                      addChildOwn(ILogMsgLinkToChild * child) { WriteLockBlock block(childLock); children.append(*child); }
    void                      removeChild(ILogMsgLinkToChild * child) { WriteLockBlock block(childLock); children.remove(findChild(child)); }
    void                      removeAllChildren() { WriteLockBlock block(childLock); children.kill(); }
    ILogMsgFilter *           getCompoundFilter(bool locked = false) const;
    void                      suspendChildren() { suspendedChildren = true; }
    void                      unsuspendChildren() { suspendedChildren = false; sendFilterToChildren(); }
    bool                      addMonitorToPTree(const ILogMsgHandler * handler, IPropertyTree * tree) const;
    void                      addAllMonitorsToPTree(IPropertyTree * tree) const;
    void                      setPort(unsigned _port) { port = _port; }
    unsigned                  queryPort() const { return port; }
    void                      setSession(LogMsgSessionId _session) { session = _session; }
    LogMsgSessionId           querySession() const { return session; }
    bool                      rejectsCategory(const LogMsgCategory & cat) const;

private:
    void                      sendFilterToChildren(bool locked = false) const;
    aindex_t                  find(const ILogMsgHandler * handler) const;
    void                      buildPrefilter();
    void                      pushMsg(LogMsg * msg);
    void                      doReport(const LogMsg & msg) const;
    void                      panic(char const * reason) const;
    aindex_t                  findChild(ILogMsgLinkToChild * child) const;

private:
    CIArrayOf<LogMsgMonitor>  monitors;
    mutable ReadWriteLock     monitorLock;
    CategoryLogMsgFilter      prefilter;
    atomic_t                  nextID;
    IArrayOf<ILogMsgLinkToChild> children;
    mutable ReadWriteLock     childLock;
    bool                      suspendedChildren;
    unsigned                  port;
    LogMsgSessionId           session;
    CriticalSection           modeLock;
};

// Message indicating messages have been dropped

class DropLogMsg : public LogMsg
{
public:
    DropLogMsg(CLogMsgManager * owner, LogMsgId id, unsigned _count) : LogMsg(dropWarningCategory, id, unknownJob, NoLogMsgCode, "MISSING LOG MESSAGES: ", 0, owner->port, owner->session), count(_count)
    {
        text.append("message queue length exceeded, dropped ").append(count).append(" messages");
    }
    unsigned queryCount() const { return count; }

private:
    unsigned count;
};

class CSysLogEventLogger : implements ISysLogEventLogger, public CInterface
{
public:
    CSysLogEventLogger();
    ~CSysLogEventLogger();
    IMPLEMENT_IINTERFACE;
    virtual bool              log(AuditType auditType, char const * msg) { return log(auditType, msg, 0, 0); }
    virtual bool              log(AuditType auditType, char const * msg, size32_t datasize, void const * data);
private:
#ifdef _WIN32
    bool                      win32Report(unsigned eventtype, unsigned category, unsigned eventid, const char * msg, size32_t datasize, const void * data);
    HANDLE                    hEventLog;
#else
    bool                      linuxReport(int level, const char * msg, size32_t datasize, const void * data);
    void                      openDataLog();
    int                       writeDataLog(size32_t datasize, byte const * data);
    bool                      dataLogUsed;
    char *                    dataLogName;
    int                       dataLogFile;
    CriticalSection           dataLogLock;
#endif
};

// Standard filters, handlers, manager, and audit event logger

extern PassAllLogMsgFilter * thePassAllFilter;
extern PassLocalLogMsgFilter * thePassLocalFilter;
extern PassNoneLogMsgFilter * thePassNoneFilter;
extern HandleLogMsgHandlerTable * theStderrHandler;
extern CLogMsgManager * theManager;
extern CSysLogEventLogger * theSysLogEventLogger;

#endif
