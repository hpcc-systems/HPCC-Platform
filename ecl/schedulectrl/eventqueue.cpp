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
#include "jthread.hpp"
#include "jregexp.hpp"
#include "jtime.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "eventqueue.hpp"
#include "scheduleread.hpp"
#include "schedulectrl.ipp"

class CScheduleEventPusher : public CInterface, implements IScheduleEventPusher
{
public:
    CScheduleEventPusher()
    {
        qconn.setown(createNamedQueueConnection(0));
    }
    IMPLEMENT_IINTERFACE;

    virtual unsigned push(char const * name, char const * text, const char * target)
    {
        if(strcmp(name, "CRON")==0) throw MakeStringException(0, "Schedule event pusher: Illegally attempted to push CRON event");
        Owned<IRemoteConnection> conn = querySDS().connect("Schedulers", myProcessSession(), RTM_LOCK_READ, connectionTimeout);
        if(!conn) return 0;
        MemoryBuffer buff;
        buff.append(name).append(text).append(target);
        Owned<IPropertyTree> root(conn->queryRoot()->getBranch("."));
        Owned<IPropertyTreeIterator> iter = root->getElements("*");
        StringBuffer queueName;
        unsigned count = 0;
        for(iter->first(); iter->isValid(); iter->next())
        {
            queueName.clear().append(iter->query().queryName()).append(".schedule");
            Owned<IQueueChannel> channel;
            channel.setown(qconn->open(queueName.str()));
            channel->put(buff);
            ++count;
        }
        return count;
    }

private:
    Owned<INamedQueueConnection> qconn;
};

class CScheduleEventProcessor;

class CSchedulePuller : public Thread
{
public:
    CSchedulePuller(char const * serverName, CScheduleEventProcessor * _processor) : Thread("CSchedulePuller"), processor(_processor), more(true)
    {
        qconn.setown(createNamedQueueConnection(0));
        StringBuffer queueName;
        queueName.clear().append(serverName).append(".schedule");
        channel.setown(qconn->open(queueName.str()));
        PROGLOG("Scheduler[%s]: listening on queue %s", serverName, queueName.str());
    }
    
    virtual int run();

    void stop()
    {
        more = false;
        channel->cancelGet();
        join();
    }

private:
    StringAttr name;
    CScheduleEventProcessor * processor;
    Owned<INamedQueueConnection> qconn;
    Owned<IQueueChannel> channel;
    bool more;
};

class CScheduleTimer : public Thread
{
private:
    class SubscriptionProxy : public CInterface, implements IScheduleSubscriber
    {
    public:
        SubscriptionProxy(CScheduleTimer * _owner) : owner(_owner) {}
        IMPLEMENT_IINTERFACE;
        virtual void notify() { owner->notify(); }
    private:
        CScheduleTimer * owner;
    };

public:
    CScheduleTimer(char const * serverName, CScheduleEventProcessor * _processor) : Thread("CScheduleTimer"), processor(_processor), more(true)
    {
        subsProxy.setown(new SubscriptionProxy(this));
        schedule.setown(getSubscribingScheduleReader(serverName, LINK(subsProxy), "CRON"));
        crontab.setown(createCronTable());
        Owned<IScheduleReaderIterator> iter(schedule->getIterator());
        StringBuffer spec;
        while(iter->isValidEventText())
        {
            if(iter->isValidWuid())
            {
                iter->getEventText(spec.clear());
                try
                {
                    crontab->add(spec.str(), spec.str());
                }
                catch(IException * e)
                {
                    e->Release(); // ignore bad cron specs (cannot communicate error back to user at this stage!)
                }
            }
            iter->nextEventText();
        }
        PROGLOG("Scheduler[%s]: timer activated", serverName);
    }

    virtual int run();

    void stop()
    {
        more = false;
        waiter.signal();
        join();
    }

    void notify()
    {
        Owned<ICronTable::Transaction> frame(crontab->getTransactionFrame());
        if(!frame) return;
        frame->removeall();
        Owned<IScheduleReaderIterator> iter(schedule->getIterator());
        StringBuffer spec;
        while(iter->isValidEventText())
        {
            if(iter->isValidWuid())
            {
                iter->getEventText(spec.clear());
                try
                {
                    if(frame->unremove(spec.str())==0)
                        frame->add(spec.str(), spec.str());
                }
                catch(IException * e)
                {
                    e->Release(); // ignore bad cron specs (cannot communicate error back to user at this stage!)
                }
            }
            iter->nextEventText();
        }
        frame->commit();
        waiter.signal();
    }

private:
    CScheduleEventProcessor * processor;
    Owned<SubscriptionProxy> subsProxy;
    Owned<IScheduleReader> schedule;
    Owned<ICronTable> crontab;
    bool more;
    Semaphore waiter;
};

class CScheduleEventProcessor : public CInterface, implements IScheduleEventProcessor
{
public:
    CScheduleEventProcessor(char const *_serverName, IScheduleEventExecutor * _executor, IExceptionHandler * _handler) 
      : serverName(_serverName), executor(_executor), handler(_handler)
    {
        schedule.setown(getSubscribingScheduleReader(_serverName, NULL, NULL));
        puller.setown(new CSchedulePuller(_serverName, this));
        timer.setown(new CScheduleTimer(_serverName, this));
    }
    IMPLEMENT_IINTERFACE;

    virtual void start()
    {
        puller->start(false);
        timer->start(false);
    }

    virtual void stop()
    {
        timer->stop();
        puller->stop();
    }

    void handle(MemoryBuffer & buff)
    {
        StringAttr name;
        StringAttr text;
        StringAttr target;
        buff.read(name).read(text);
        //Backward compatibility for the moment.
        if (buff.remaining())
            buff.read(target);
        PROGLOG("Scheduler[%s]: received event [%s/%s] @%s", serverName.get(), name.get(), text.get(), target.get());
        if(strcmp(name.get(), "CRON")==0)
        {
            OWARNLOG("Scheduler[%s]: Unexpectedly got external CRON event (ignoring)", serverName.get());
            return;
        }
        bool checkWuid = (target && *target);
        Owned<IScheduleReaderIterator> iter(schedule->getIterator(name));
        StringBuffer pattern, wuid;
        while(iter->isValidEventText())
        {
            //MORE: If the target is specified it may be best to check it first, but still only do the wildmatch once
            iter->getEventText(pattern.clear());
            if(WildMatch(text, pattern.str()), true)
            {
                PROGLOG("Scheduler[%s]: event matched schedule item [%s/%s]", serverName.get(), name.get(), pattern.str());
                while(iter->isValidWuid())
                {
                    iter->getWuid(wuid.clear());
                    if (!checkWuid || (strcmp(wuid, target) == 0))
                    {
                        PROGLOG("Scheduler[%s]: pushing event to workunit %s", serverName.get(), wuid.str());
                        execute(name, text, wuid.str());
                    }
                    iter->nextWuid();
                }
            }
            iter->nextEventText();
        }
    }

    void handleCron(char const * spec, CDateTime const & dt)
    {
        Owned<IScheduleReaderIterator> iter(schedule->getIterator("CRON", spec));
        PROGLOG("Scheduler[%s]: cron triggered [%s]", serverName.get(), spec);
        StringBuffer wuid;
        while(iter->isValidWuid())
        {
            iter->getWuid(wuid.clear());
            if(iter->queryUpdateLatest(dt))
            {
                PROGLOG("Scheduler[%s]: pushing cron to workunit %s", serverName.get(), wuid.str());
                execute("CRON", spec, wuid.str());
            }
            iter->nextWuid();
        }
    }

    IExceptionHandler * queryExceptionHandler() { return handler; }

private:

    void execute(char const * name, char const * text, char const * wuid)
    {
        executor->execute(wuid, name, text);
    }

private:
    StringAttr serverName;
    Owned<IScheduleEventExecutor> executor;
    IExceptionHandler * handler;
    Owned<IScheduleReader> schedule;
    Owned<CSchedulePuller> puller;
    Owned<CScheduleTimer> timer;
};

int CSchedulePuller::run()
{
    MemoryBuffer buff;
    while(more)
        try
        {
            if(channel->get(buff))
                processor->handle(buff);
        }
        catch(IException * e)
        {
            if(!processor->queryExceptionHandler()->fireException(e))
                return 0;
        }
    return 0;
}

int CScheduleTimer::run()
{
    CDateTime nowdt;
    nowdt.setNow();
    while(more)
    {
        CDateTime nextdt;
        StringArray crons;
        unsigned count;
        count = crontab->next(nowdt, nextdt, crons);
        if(count)
        {
            nowdt.setNow();
            if(nextdt > nowdt)
            {
                unsigned time = (unsigned)(1000*(nextdt.getSimple()-nowdt.getSimple()));
                waiter.wait(time);
            }
            nowdt.setNow();
            if(more && (nowdt >= nextdt)) //!more indicated abort, !(nowdt >= nextdt) indicates schedule notify
                for(unsigned i=0; i<count; i++)
                    processor->handleCron(crons.item(i), nowdt);
        }
        else
        {
            waiter.wait();
            nowdt.setNow();
        }
    }
    return 0;
}

extern SCHEDULECTRL_API IScheduleEventProcessor * getScheduleEventProcessor(char const * serverName, IScheduleEventExecutor * executor, IExceptionHandler * handler) { return new CScheduleEventProcessor(serverName, executor, handler); }
extern SCHEDULECTRL_API IScheduleEventPusher * getScheduleEventPusher() { return new CScheduleEventPusher(); }
