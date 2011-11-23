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

#ifndef __PQUEUE_HPP
#define __PQUEUE_HPP

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning(disable : 4284 ) // return type for '...::operator ->' is '...' (ie; not a UDT or reference to a UDT.  Will produce errors if applied using infix notation)
#endif

#include "jlibplus.hpp"

#include "jmutex.hpp"
#include "jthread.hpp"
#include "jmisc.hpp"
#include <list>
#include <vector>


namespace esp
{

template<typename T> class WaitQueue: public CInterface, protected Mutex, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    WaitQueue(): counter(), stopped(false), waiting(0)
    {
    }

    ~WaitQueue()
    {
        stop();
        synchronized block(*this); 
        while(waiting)
        {
            counter.signal(waiting); // actually need only one and only once
            wait(INFINITE);
        }
    }

    unsigned size()
    {
        synchronized block(*this);
        return queue.size();
    }

    T get(unsigned timeout=INFINITE)
    {
        synchronized block(*this);
        for(;;)
        {
            if(stopped)
                return 0;
            if(queue.size())
                break;
            if(!wait(timeout))
                return 0;
        }
        T item=queue.front();
        queue.pop_front();
        return item;
    }

    bool put(const T& item)
    {
        synchronized block(*this);
        if(stopped)
            return true;
        queue.push_back(item);
        counter.signal();
        return waiting>0;
    }

    void stop()
    {
        synchronized block(*this);
        stopped=true;
        queue.clear();
        counter.signal(waiting);
    }

    bool isStopped()
    {
        synchronized block(*this);
        return stopped;
    }
    
private:

    bool wait(unsigned timeout)
    {
        bool ret=false;
        waiting++;

        int locked = unlockAll();
        ret=counter.wait(timeout);
        lockAll(locked);

        waiting--;
        return ret;
    }

    Semaphore counter;
    std::list<T> queue;
    volatile unsigned waiting;
    volatile bool stopped; //need event
};

interface ITask: extends IInterface
{
    virtual int run()=0;
    virtual bool stop()=0;
};

interface IErrorListener: extends IInterface
{
    virtual void reportError(const char* err,...) __attribute__((format(printf, 2, 3))) =0;
};

class TaskQueue
{
public:
    TaskQueue(size32_t _maxsize,IErrorListener* _err=0): maxsize(_maxsize), err(_err) 
    {
    }

    ~TaskQueue()
    {
        stop();
        join();
    }

    void put(ITask* task)
    {
        bool needthread=!queue.put(task);
        if(needthread)
        {
            synchronized block(mworkers);
            if(workers.size()<maxsize)
            {
                workers.push_back(new WorkerThread(*this));
                workers.back()->start();
            }
//            PrintLog("%d threads",workers.size());
        }
    }

    void stop()
    {
        queue.stop();

        synchronized block(mworkers); 
        for(Workers::iterator it=workers.begin();it!=workers.end();it++)
            (*it)->stop(); // no good if threads did not clean up
    }

    void join()
    {
        synchronized block(mworkers);
        while(!workers.empty())
        {
            mworkers.wait();
        }

    }

    void setErrorListener(IErrorListener* _err)
    {
        err.set(_err);
    }

    void reportError(const char* e)
    {
        if(err)
        {
            synchronized block(merr);
            err->reportError(e);
        }
    }

private:

    class WorkerThread: public Thread
    {
    public:
        WorkerThread(TaskQueue& _tq): tq(_tq), stopped(false) 
        {
        }

        virtual int run()
        {
            for(;;)
            {
                
                try
                {
                    task.setown(tq.queue.get(1000).get());
                    if(stopped || !task)
                        break;
                    task->run();
                }
                catch (IException *E)
                {
                    StringBuffer err;
                    E->errorMessage(err);
                    tq.reportError(err.str());
                    E->Release();
                }
                catch (...)
                {
                    tq.reportError("Unknown exception ");
                }
                task.clear();
            }
            Release(); // There should be one more
            return 0;
        }
        

        bool stop()
        {
            stopped=true;
            Linked<ITask> t(task.get());
            return t ? t->stop() : true;
        }

        virtual void beforeDispose()
        {
            tq.remove(this);
        }

    private:
        TaskQueue& tq;
        volatile bool stopped;
        Owned<ITask> task;
    };

    void remove(WorkerThread* th)
    {
        synchronized block(mworkers);
        workers.remove(th);
        if(workers.empty())
            mworkers.notifyAll();
    }

 
    WaitQueue<Linked<ITask> > queue;

    size32_t maxsize;
    friend WorkerThread;
    typedef std::list<WorkerThread*> Workers;
    Workers workers;
    Monitor mworkers;
    Linked<IErrorListener> err;
    Mutex merr;
};
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif
