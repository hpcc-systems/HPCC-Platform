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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "schedulectrl.hpp"
#include "scheduleread.hpp"
#include "eventqueue.hpp"
#include "daclient.hpp"

void usage(int exitval = 2)
{
    printf("Usage:\n"
           "scheduleadmin help\n"
           "scheduleadmin <DALI> add <WUID>\n"
           "scheduleadmin <DALI> remove <WUID>\n"
           "scheduleadmin <DALI> removeall\n"
           "scheduleadmin <DALI> servers\n"
           "scheduleadmin <DALI> list <eclserver> (<event name>)\n"
           "scheduleadmin <DALI> monitor <eclserver> (<event name>)\n"
           "scheduleadmin <DALI> cleanup\n"
           "scheduleadmin <DALI> push eventname eventtext (<wuid>)\n"
#if _DEBUG
           "scheduleadmin <DALI> testpull <eclserver queue>\n"
#endif
           );
    releaseAtoms();
    ExitModuleObjects();
    exit(exitval);
}

class AdminScheduleMonitor : public CInterface
{
private:
    class SubscriptionProxy : public CInterface, implements IScheduleSubscriber
    {
    public:
        SubscriptionProxy(AdminScheduleMonitor const * _owner) : owner(_owner) {}
        IMPLEMENT_IINTERFACE;
        virtual void notify() { owner->notify(); }
    private:
        AdminScheduleMonitor const * owner;
    };

public:
    AdminScheduleMonitor(char const * serverName, char const * eventName, bool subscribe)
    {
        if(subscribe)
        {
            subsProxy.setown(new SubscriptionProxy(this));
            reader.setown(getSubscribingScheduleReader(serverName, LINK(subsProxy), eventName));
        }
        else
        {
            reader.setown(getScheduleReader(serverName, eventName));
        }
    }
    ~AdminScheduleMonitor() { if(subsProxy) printf("...done\n"); }
    void dump() const
    {
        dumpTime();
        Owned<IScheduleReaderIterator> iter(reader->getIterator());
        StringBuffer buff;
        while(iter->isValidEventName())
        {
            printf("%s\n", iter->getEventName(buff.clear()).str());
            while(iter->isValidEventText())
            {
                printf("    %s\n", iter->getEventText(buff.clear()).str());
                while(iter->isValidWuid())
                {
                    printf("        %s\n", iter->getWuid(buff.clear()).str());
                    iter->nextWuid();
                }
                iter->nextEventText();
            }
            iter->nextEventName();
        }
        if(subsProxy) printf("monitoring...\n");
    }

    void notify() const
    {
        printf("\n----------------------------------------\n\n");
        dump();
    }

private:
    void dumpTime() const
    {
        CDateTime time;
        StringBuffer tstr;
        time.setNow();
        time.getString(tstr, false);
        printf("%s\n\n", tstr.str());
    }
    
private:
    Owned<SubscriptionProxy> subsProxy;
    Owned<IScheduleReader> reader;
};

class AdminScheduleEventTester : public CInterface, implements IExceptionHandler
{
public:
    class EventExecutor : public CInterface, implements IScheduleEventExecutor
    {
    public:
        EventExecutor() {}
        IMPLEMENT_IINTERFACE;
        virtual void execute(char const * wuid, char const * name, char const * text)
        {
            CDateTime nowdt;
            StringBuffer nowstr;
            nowdt.setNow();
            nowdt.getString(nowstr);
            printf("Pass event to workunit %s: name=%s text=%s (at %s)\n", wuid, name, text, nowstr.str());
        }
    };

    AdminScheduleEventTester(char const * _serverName) : serverName(_serverName) {}

    void run()
    {
        printf("watching event queue...\n");
        Owned<EventExecutor> executor(new EventExecutor);
        Owned<IScheduleEventProcessor> processor(getScheduleEventProcessor(serverName.get(), LINK(executor), this));
        processor->start();
        getchar();
        processor->stop();
        printf("...done\n");
    }

    virtual bool fireException(IException *e) { StringBuffer msg; ERRLOG("Scheduler error (skipping event): %d: %s", e->errorCode(), e->errorMessage(msg).str()); e->Release(); return true; }
private:
    StringAttr serverName;
};

interface IScheduleTask : extends IInterface
{
public:
    virtual void doit() = 0;
};

class AddScheduleTask : public CInterface, implements IScheduleTask
{
public:
    AddScheduleTask(char const * _wuid) : wuid(_wuid) {}
    IMPLEMENT_IINTERFACE;
    virtual void doit () { scheduleWorkUnit(wuid.get()); }
private:
    StringAttr wuid;
};

class RemoveScheduleTask : public CInterface, implements IScheduleTask
{
public:
    RemoveScheduleTask(char const * _wuid) : wuid(_wuid) {}
    IMPLEMENT_IINTERFACE;
    virtual void doit () { descheduleWorkUnit(wuid.get()); }
private:
    StringAttr wuid;
};

class RemoveAllScheduleTask : public CInterface, implements IScheduleTask
{
public:
    RemoveAllScheduleTask() {}
    IMPLEMENT_IINTERFACE;
    virtual void doit () { descheduleAllWorkUnits(); }
};

class ListServersScheduleTask : public CInterface, implements IScheduleTask
{
public:
    ListServersScheduleTask() {}
    IMPLEMENT_IINTERFACE;
    virtual void doit()
    {
        Owned<ISchedulerListIterator> iter = getSchedulerList();
        for(iter->first(); iter->isValid(); iter->next())
            printf("%s\n", iter->query());
    }
};

class ReadNamedScheduleTask : public CInterface, implements IScheduleTask
{
public:
    ReadNamedScheduleTask(char const * _serverName, char const * _eventName, bool _subscribe) : serverName(_serverName), eventName(_eventName), subscribe(_subscribe) {}
    IMPLEMENT_IINTERFACE;
    virtual void doit() { Owned<AdminScheduleMonitor> monitor(new AdminScheduleMonitor(serverName.get(), eventName.get(), subscribe)); monitor->dump(); if(subscribe) getchar(); }
private:
    StringAttr serverName;
    StringAttr eventName;
    bool subscribe;
};

class ReadFullScheduleTask : public CInterface, implements IScheduleTask
{
public:
    ReadFullScheduleTask(char const * _serverName, bool _subscribe) : serverName(_serverName), subscribe(_subscribe) {}
    IMPLEMENT_IINTERFACE;
    virtual void doit() { Owned<AdminScheduleMonitor> monitor(new AdminScheduleMonitor(serverName.get(), NULL, subscribe)); monitor->dump(); if(subscribe) getchar(); }
private:
    StringAttr serverName;
    bool subscribe;
};

class CleanupScheduleTask : public CInterface, implements IScheduleTask
{
public:
    CleanupScheduleTask() {}
    IMPLEMENT_IINTERFACE;
    virtual void doit() { cleanupWorkUnitSchedule(); }
};

class PushScheduleTask : public CInterface, implements IScheduleTask
{
public:
    PushScheduleTask(char const * _name, char const * _text, const char * _target) 
        : name(_name), text(_text), target(_target) {}
    IMPLEMENT_IINTERFACE;
    virtual void doit()
    {
        Owned<IScheduleEventPusher> pusher(getScheduleEventPusher());
        unsigned count = pusher->push(name.get(), text.get(), target.get());
        PROGLOG("Pushed event to %u active schedulers", count);
    }
private:
    StringAttr name;
    StringAttr text;
    StringAttr target;
};

#ifdef _DEBUG

class TestPullScheduleTask : public CInterface, implements IScheduleTask
{
public:
    TestPullScheduleTask(char const * _serverName) : serverName(_serverName) {}
    IMPLEMENT_IINTERFACE;
    virtual void doit() { Owned<AdminScheduleEventTester> tester(new AdminScheduleEventTester(serverName.get())); tester->run(); }
private:
    StringAttr serverName;
};

#endif

int main(int argc, char * const * argv)
{
    if((argc==2) && (stricmp(argv[1], "help")==0))
        usage(0);
    if(argc<3) usage();
    Owned<IScheduleTask> task;
    try
    {
        char const * cmd = argv[2];
        if(stricmp(cmd, "add")==0)
            if(argc==4)
                task.setown(new AddScheduleTask(argv[3]));
            else
                usage();
        else if(stricmp(cmd, "remove")==0)
            if(argc==4)
                task.setown(new RemoveScheduleTask(argv[3]));
            else
                usage();
        else if(stricmp(cmd, "removeall")==0)
            if(argc==3)
                task.setown(new RemoveAllScheduleTask());
            else
                usage();
        else if(stricmp(cmd, "servers")==0)
            if(argc==3)
                task.setown(new ListServersScheduleTask());
            else
                usage();
        else if(stricmp(cmd, "list")==0)
        {
            if(argc==5)
                task.setown(new ReadNamedScheduleTask(argv[3], argv[4], false));
            else if(argc==4)
                task.setown(new ReadFullScheduleTask(argv[3], false));
            else
                usage();
        }
        else if(stricmp(cmd, "monitor")==0)
        {
            if(argc==5)
                task.setown(new ReadNamedScheduleTask(argv[3], argv[4], true));
            else if(argc==4)
                task.setown(new ReadFullScheduleTask(argv[3], true));
            else
                usage();
        }
        else if(stricmp(cmd, "cleanup")==0)
            if(argc==3)
                task.setown(new CleanupScheduleTask());
            else
                usage();
        else if(stricmp(cmd, "push")==0)
            if(argc==5)
                task.setown(new PushScheduleTask(argv[3], argv[4], NULL));
            else if(argc==6)
                task.setown(new PushScheduleTask(argv[3], argv[4], argv[5]));
            else
                usage();
#ifdef _DEBUG
        else if(stricmp(cmd, "testpull")==0)
            if(argc==4)
                task.setown(new TestPullScheduleTask(argv[3]));
            else
                usage();
#endif
        else
            usage();
        Owned<IGroup> serverGroup = createIGroup(argv[1], DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_Other); //PG MORE: right value
        task->doit();
        closedownClientProcess();
    }
    catch(IException * e)
    {
        EXCLOG(e);
        e->Release();
    }
    releaseAtoms();
    ExitModuleObjects();
    return 0;
}
