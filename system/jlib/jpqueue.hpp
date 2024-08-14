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


#ifndef __PQUEUE_HPP
#define __PQUEUE_HPP

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning(disable : 4284 ) // return type for '...::operator ->' is '...' (ie; not a UDT or reference to a UDT.  Will produce errors if applied using infix notation)
#endif

#include "jmutex.hpp"
#include "jthread.hpp"
#include "jmisc.hpp"
#include <list>
#include <vector>

template<typename T> class WaitQueue: public CInterface
{
public:
    WaitQueue(): counter(), waiting(0), stopped(false)
    {
    }

    ~WaitQueue()
    {
        stop();
        synchronized block(mutex); 
        while(waiting)
        {
            counter.signal(waiting); // actually need only one and only once
            wait(INFINITE);
        }
    }

    unsigned size()
    {
        synchronized block(mutex);
        return queue.size();
    }

    T get(unsigned timeout=INFINITE)
    {
        synchronized block(mutex);
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
        synchronized block(mutex);
        if(stopped)
            return true;
        queue.push_back(item);
        counter.signal();
        return waiting>0;
    }

    void stop()
    {
        synchronized block(mutex);
        stopped=true;
        queue.clear();
        counter.signal(waiting);
    }

    bool isStopped()
    {
        synchronized block(mutex);
        return stopped;
    }
    
private:

    bool wait(unsigned timeout)
    {
        bool ret=false;
        waiting++;

        mutex.unlock();
        ret=counter.wait(timeout);
        mutex.lock();

        waiting--;
        return ret;
    }

    Mutex mutex;
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
    virtual void reportError(const char* err,...)=0;
};

//MORE: This class is barely used - I think it is only used by some ancient windows specific code.
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
            MonitorBlock block(mworkers);
            if(workers.size()<maxsize)
            {
                workers.push_back(new WorkerThread(*this));
                workers.back()->start(false);
            }
//            DBGLOG("%d threads",workers.size());
        }
    }

    void stop()
    {
        queue.stop();

        MonitorBlock block(mworkers); 
        for(Workers::iterator it=workers.begin();it!=workers.end();it++)
            (*it)->stop(); // no good if threads did not clean up
    }

    void join()
    {
        MonitorBlock block(mworkers);
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
        MonitorBlock block(mworkers);
        workers.remove(th);
        if(workers.empty())
            mworkers.notifyAll();
    }

 
    WaitQueue<Linked<ITask> > queue;

    size32_t maxsize;
    friend class WorkerThread;
    typedef std::list<WorkerThread*> Workers;
    Workers workers;
    Monitor mworkers;
    Linked<IErrorListener> err;
    Mutex merr;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif
