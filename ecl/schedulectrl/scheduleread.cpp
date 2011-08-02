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
#include "scheduleread.hpp"
#include "schedulectrl.ipp"
#include "jptree.hpp"
#include "jexcept.hpp"
#include "dasds.hpp"

class CScheduleWuidIterator : public CInterface
{
public:
    CScheduleWuidIterator(IPropertyTree * textBranch) : branch(textBranch)
    {
        iter.setown(branch->getElements("*"));
        iter->first();
    }
    bool isValidWuid() const { return iter->isValid(); }
    bool nextWuid() { return iter->next(); }
    StringBuffer & getWuid(StringBuffer & out) const { return ncnameUnescape(iter->query().queryName(), out); }

    bool queryUpdateLatest(char const * baseXpath, CDateTime const & dtNow) const
    {
        StringBuffer xpath;
        xpath.append(baseXpath).append('/').append(iter->query().queryName());
        Owned<IRemoteConnection> connection = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE | RTM_CREATE_QUERY, connectionTimeout);
        IPropertyTree * root = connection->queryRoot();
        if(root->hasProp("@latest"))
        {
            CDateTime dtThen;
            dtThen.setString(root->queryProp("@latest"));
            if(dtNow <= dtThen)
                return false;
        }
        StringBuffer timestr;
        dtNow.getString(timestr);
        root->setProp("@latest", timestr.str());
        return true;
    }
private:
    Owned<IPropertyTree> branch;
    Owned<IPropertyTreeIterator> iter;
};

class CScheduleEventTextIterator : public CInterface
{
public:
    CScheduleEventTextIterator(IPropertyTree * nameBranch) : branch(nameBranch)
    {
        iter.setown(branch->getElements("*"));
        iter->first();
        if(iter->isValid())
            child.setown(new CScheduleWuidIterator(&iter->get()));
    }
    bool isValidWuid() const { return (child && child->isValidWuid()); }
    bool nextWuid() { return (child && child->nextWuid()); }
    bool isValidEventText() const { return iter->isValid(); }
    bool nextEventText()
    {
        if(iter->next())
        {
            child.setown(new CScheduleWuidIterator(&iter->get()));
            return true;
        }
        else
        {
            child.clear();
            return false;
        }
    }
    StringBuffer & getWuid(StringBuffer & out) const { return child->getWuid(out); }
    StringBuffer & getEventText(StringBuffer & out) const { return ncnameUnescape(iter->query().queryName(), out); }

    bool queryUpdateLatest(char const * baseXpath, CDateTime const & dt) const
    {
        StringBuffer xpath;
        xpath.append(baseXpath).append('/').append(iter->query().queryName());
        return child->queryUpdateLatest(xpath.str(), dt);
    }

private:
    Owned<IPropertyTree> branch;
    Owned<IPropertyTreeIterator> iter;
    Owned<CScheduleWuidIterator> child;
};

class ScheduleReaderBase : implements IInterface
{
public:
    class SubscriptionProxy : public CInterface, implements ISDSSubscription
    {
    public:
        SubscriptionProxy(ScheduleReaderBase * _owner) : owner(_owner) {}
        IMPLEMENT_IINTERFACE;
        virtual void notify(SubscriptionId id, char const * xpath, SDSNotifyFlags flags, unsigned valueLen = 0, const void * valueData = NULL)
        {
            owner->notify();
        }
    private:
        ScheduleReaderBase * owner;
    };

    ScheduleReaderBase(IScheduleSubscriber * _subscriber) : uptodate(true), linkCount(0), subscriber(_subscriber), subsId(0) {}

    virtual ~ScheduleReaderBase() { if(subsProxy) querySDS().unsubscribe(subsId); }

    void link()
    {
        CriticalBlock block(crit);
        linkCount++;
    }

    void unlink()
    {
        CriticalBlock block(crit);
        linkCount--;
        if(!uptodate)
        {
            read(false);
            uptodate = true;
        }
    }

    void notify()
    {
        CriticalBlock block(crit);
        if(linkCount==0)
        {
            read(false);
            uptodate = true;
            if(subscriber)
                subscriber->notify();
        }
        else
        {
            uptodate = false;
        }
    }

protected:
    virtual void read(bool subscribeAfter) = 0;

    void subscribe(char const * xpath)
    {
        assertex(subsId == 0);
        subsProxy.setown(new SubscriptionProxy(this));
        subsId = querySDS().subscribe(xpath, *subsProxy.get(), true);
    }

private:
    bool uptodate;
    CriticalSection crit;
    unsigned linkCount;
    Owned<IScheduleSubscriber> subscriber;
    Owned<SubscriptionProxy> subsProxy;
    SubscriptionId subsId;
};

class CScheduleEventNameIterator : public CInterface, implements IScheduleReaderIterator
{
public:
    CScheduleEventNameIterator(IPropertyTree * scheduleBranch, char const * _baseXpath, ScheduleReaderBase * _owner) : branch(scheduleBranch), baseXpath(_baseXpath), owner(_owner)
    {
        owner->link();
        iter.setown(branch->getElements("*"));
        iter->first();
        if(iter->isValid())
            child.setown(new CScheduleEventTextIterator(&iter->get()));
    }
    virtual ~CScheduleEventNameIterator() { owner->unlink(); }
    IMPLEMENT_IINTERFACE;
    virtual bool isValidWuid() const { return (child && child->isValidWuid()); }
    virtual bool nextWuid() { return (child && child->nextWuid()); }
    virtual bool isValidEventText() const { return (child && child->isValidEventText()); }
    virtual bool nextEventText() { return (child && child->nextEventText()); }
    virtual bool isValidEventName() const { return iter->isValid(); }
    virtual bool nextEventName()
    {
        if(iter->next())
        {
            child.setown(new CScheduleEventTextIterator(&iter->get()));
            return true;
        }
        else
        {
            child.clear();
            return false;
        }
    }
    virtual StringBuffer & getWuid(StringBuffer & out) const { return child->getWuid(out); }
    virtual StringBuffer & getEventText(StringBuffer & out) const { return child->getEventText(out); }
    virtual StringBuffer & getEventName(StringBuffer & out) const { return ncnameUnescape(iter->query().queryName(), out); }

    virtual bool queryUpdateLatest(CDateTime const & dt) const
    {
        StringBuffer xpath;
        xpath.append(baseXpath).append('/').append(iter->query().queryName());
        return child->queryUpdateLatest(xpath.str(), dt);
    }

private:
    Owned<IPropertyTree> branch;
    StringAttr baseXpath; // /Schedule/servername
    Owned<IPropertyTreeIterator> iter;
    Owned<CScheduleEventTextIterator> child;
    Owned<ScheduleReaderBase> owner;
};

class CScheduleSingleEventNameIterator : public CInterface, implements IScheduleReaderIterator
{
public:
    CScheduleSingleEventNameIterator(char const * eventName, IPropertyTree * nameBranch, char const * _baseXpath, ScheduleReaderBase * _owner) : name(eventName), owner(_owner)
    {
        owner->link();
        child.setown(new CScheduleEventTextIterator(nameBranch));
    }
    ~CScheduleSingleEventNameIterator() { owner->unlink(); }
    IMPLEMENT_IINTERFACE;
    virtual bool isValidWuid() const { return (child && child->isValidWuid()); }
    virtual bool nextWuid() { return (child && child->nextWuid()); }
    virtual bool isValidEventText() const { return (child && child->isValidEventText()); }
    virtual bool nextEventText() { return (child && child->nextEventText()); }
    virtual bool isValidEventName() const { return child.get() != NULL; }
    virtual bool nextEventName() { child.clear(); return false; }
    virtual StringBuffer & getWuid(StringBuffer & out) const { return child->getWuid(out); }
    virtual StringBuffer & getEventText(StringBuffer & out) const { return child->getEventText(out); }
    virtual StringBuffer & getEventName(StringBuffer & out) const { if(child) out.append(name.get()); return out; }
    virtual bool queryUpdateLatest(CDateTime const & dt) const { return child->queryUpdateLatest(baseXpath.get(), dt); }
private:
    StringAttr name;
    Owned<ScheduleReaderBase> owner;
    StringAttr baseXpath; // /Schedule/server/name
    Owned<CScheduleEventTextIterator> child;
};

class CScheduleSingleEventTextIterator : public CInterface, implements IScheduleReaderIterator
{
public:
    CScheduleSingleEventTextIterator(char const * eventName, char const * eventText, IPropertyTree * textBranch, char const * _baseXpath, ScheduleReaderBase * _owner) : name(eventName), text(eventText), baseXpath(_baseXpath), owner(_owner)
    {
        owner->link();
        child.setown(new CScheduleWuidIterator(textBranch));
    }
    ~CScheduleSingleEventTextIterator() { owner->unlink(); }
    IMPLEMENT_IINTERFACE;
    virtual bool isValidWuid() const { return (child && child->isValidWuid()); }
    virtual bool nextWuid() { return (child && child->nextWuid()); }
    virtual bool isValidEventText() const { return child.get() != NULL; }
    virtual bool nextEventText() { child.clear(); return false; }
    virtual bool isValidEventName() const { return child.get() != NULL; }
    virtual bool nextEventName() { child.clear(); return false; }
    virtual StringBuffer & getWuid(StringBuffer & out) const { return child->getWuid(out); }
    virtual StringBuffer & getEventText(StringBuffer & out) const { if(child) out.append(text.get()); return out; }
    virtual StringBuffer & getEventName(StringBuffer & out) const { if(child) out.append(name.get()); return out; }
    virtual bool queryUpdateLatest(CDateTime const & dt) const { return child->queryUpdateLatest(baseXpath.get(), dt); }
private:
    StringAttr name;
    StringAttr text;
    StringAttr baseXpath; // /Scheduler/server/name/text
    Owned<ScheduleReaderBase> owner;
    Owned<CScheduleWuidIterator> child;
};

class CScheduleNullEventNameIterator : public CInterface, implements IScheduleReaderIterator
{
public:
    CScheduleNullEventNameIterator() {}
    IMPLEMENT_IINTERFACE;
    virtual bool isValidEventName() const { return false; }
    virtual bool isValidEventText() const { return false; }
    virtual bool isValidWuid() const { return false; }
    virtual bool nextEventName() { return false; }
    virtual bool nextEventText() { return false; }
    virtual bool nextWuid() { return false; }
    virtual StringBuffer & getEventName(StringBuffer & out) const { throwUnexpected(); return out; }
    virtual StringBuffer & getEventText(StringBuffer & out) const { throwUnexpected(); return out; }
    virtual StringBuffer & getWuid(StringBuffer & out) const { throwUnexpected(); return out; }
    virtual bool queryUpdateLatest(CDateTime const & dt) const { throwUnexpected(); return false; }
};

class CRootScheduleReader : public CInterface, public ScheduleReaderBase, implements IScheduleReader
{
public:
    CRootScheduleReader(char const * _serverName, bool subscribe, IScheduleSubscriber * _subscriber) : ScheduleReaderBase(_subscriber), serverName(_serverName)
    {
        rootPath.append("/Schedule/").append(serverName.get());
        read(subscribe);
    }

    IMPLEMENT_IINTERFACE;
    virtual IScheduleReaderIterator * getIterator(char const * eventName, char const * eventText)
    {
        if(eventName)
        {
            StringBuffer xpath;
            ncnameEscape(eventName, xpath);
            StringBuffer childPath;
            childPath.append(rootPath).append('/').append(xpath.str());
            Owned<IPropertyTree> nameBranch(scheduleBranch->getPropTree(xpath.str()));
            if(!nameBranch)
                return new CScheduleNullEventNameIterator();
            if(eventText)
            {
                ncnameEscape(eventText, xpath.clear());
                Owned<IPropertyTree> textBranch(nameBranch->getPropTree(xpath.str()));
                if(!textBranch)
                    return new CScheduleNullEventNameIterator();
                childPath.append('/').append(xpath);
                return new CScheduleSingleEventTextIterator(eventName, eventText, textBranch.getLink(), childPath.str(), LINK(this));
            }
            else
            {
                return new CScheduleSingleEventNameIterator(eventName, nameBranch.getLink(), childPath.str(), LINK(this));
            }
        }
        else
        {
            return new CScheduleEventNameIterator(scheduleBranch.getLink(), rootPath.str(), LINK(this));
        }
    }

protected:
    void read(bool subscribeAfter)
    {
        Owned<IRemoteConnection> connection = querySDS().connect(rootPath.str(), myProcessSession(), RTM_LOCK_READ | RTM_CREATE_QUERY, connectionTimeout);
        Owned<IPropertyTree> root(connection->queryRoot()->getBranch("."));
        if(root)
            scheduleBranch.setown(createPTreeFromIPT(root));
        if(subscribeAfter)
            subscribe(rootPath.str());
    }

private:
    StringAttr serverName;
    StringBuffer rootPath;
    Owned<IPropertyTree> scheduleBranch;
};

class CBranchScheduleReader : public CInterface, public ScheduleReaderBase, implements IScheduleReader
{
public:
    CBranchScheduleReader(char const * _serverName, char const * _eventName, bool subscribe, IScheduleSubscriber * _subscriber) : ScheduleReaderBase(_subscriber), serverName(_serverName), eventName(_eventName)
    {
        rootPath.append("/Schedule/").append(serverName.get());
        ncnameEscape(_eventName, xpath);
        fullPath.append(rootPath).append('/').append(xpath.str());
        read(subscribe);
    }
    IMPLEMENT_IINTERFACE;
    virtual IScheduleReaderIterator * getIterator(char const * _eventName, char const * eventText)
    {
        if((!nameBranch) || (_eventName && (strcmp(_eventName, eventName.get())!=0)))
            return new CScheduleNullEventNameIterator();
        if(eventText)
        {
            StringBuffer xpath;
            ncnameEscape(eventText, xpath);
            Owned<IPropertyTree> textBranch(nameBranch->getPropTree(xpath.str()));
            if(!textBranch)
                return new CScheduleNullEventNameIterator();
            StringBuffer childPath;
            childPath.append(fullPath).append('/').append(xpath);
            return new CScheduleSingleEventTextIterator(_eventName, eventText, textBranch.getLink(), childPath.str(), LINK(this));
        }
        else
        {
            return new CScheduleSingleEventNameIterator(eventName.get(), nameBranch.getLink(), fullPath.str(), LINK(this));
        }
    }

protected:
    void read(bool subscribeAfter)
    {
        Owned<IRemoteConnection> connection = querySDS().connect(rootPath.str(), myProcessSession(), RTM_LOCK_READ | RTM_CREATE_QUERY, connectionTimeout);
        Owned<IPropertyTree> root(connection->queryRoot()->getBranch(xpath.str()));
        if(root)
            nameBranch.setown(createPTreeFromIPT(root));
        if(subscribeAfter)
            subscribe(fullPath.str());
    }

private:
    StringAttr serverName;
    StringAttr eventName;
    StringBuffer rootPath; // /Schedule/server
    StringBuffer xpath; // name
    StringBuffer fullPath; // /Schedule/server/name
    Owned<IPropertyTree> nameBranch;
};

IScheduleReader * getScheduleReader(char const * serverName, char const * eventName)
{
    if(eventName)
        return new CBranchScheduleReader(serverName, eventName, false, NULL);
    else
        return new CRootScheduleReader(serverName, false, NULL);
}

IScheduleReader * getSubscribingScheduleReader(char const * serverName, IScheduleSubscriber * subscriber, char const * eventName)
{
    if(eventName)
        return new CBranchScheduleReader(serverName, eventName, true, subscriber);
    else
        return new CRootScheduleReader(serverName, true, subscriber);
}

class CSchedulerListIterator : public CInterface, implements ISchedulerListIterator
{
public:
    CSchedulerListIterator()
    {
        conn.setown(querySDS().connect("/Schedulers", myProcessSession(), RTM_LOCK_READ, connectionTimeout));
        if(conn)
        {
            root.setown(conn->queryRoot()->getBranch("."));
            iter.setown(root->getElements("*"));
        }
    }
    IMPLEMENT_IINTERFACE;
    virtual void first() { if(iter) iter->first(); }
    virtual bool isValid() const { return (iter && iter->isValid()); }
    virtual bool next() { return (iter && iter->next()); }
    virtual char const * query() const { return (iter ? iter->query().queryName() : NULL); }
private:
    Owned<IRemoteConnection> conn;
    Owned<IPropertyTree> root;
    Owned<IPropertyTreeIterator> iter;
};

ISchedulerListIterator * getSchedulerList()
{
    return new CSchedulerListIterator();
}
