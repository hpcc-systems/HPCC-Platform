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
#include "jlib.hpp"
#include "jfile.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jqueue.tpp"
#include "jregexp.hpp"
#include "jlog.ipp"
#include "jisem.hpp"
#include "jtask.hpp"

#include <assert.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/resource.h>
#endif
#ifdef __linux__
#include <sys/prctl.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#endif

#define LINUX_STACKSIZE_CAP (0x200000)

//#define NO_CATCHALL

//static __thread ThreadTermFunc threadTerminationHook;
static thread_local std::vector<ThreadTermFunc> threadTermHooks;
static std::vector<ThreadTermFunc> mainThreadTermHooks;

static struct MainThreadIdHelper
{
    ThreadId tid;
    MainThreadIdHelper()
    {
        tid = GetCurrentThreadId();
    }
} mainThreadIdHelper;

extern bool isMainThread()
{
    return GetCurrentThreadId() == mainThreadIdHelper.tid;
}

/*
 * NB: Thread termination hook functions are tracked using a thread local vector (threadTermHooks).
 * However, hook functions installed on the main thread must be tracked separately in a non thread local vector (mainThreadTermHooks).
 * This is because thread local variables are destroyed before atexit functions are called and therefore before ModuleExitObjects().
 * The hooks tracked by mainThreadTermHooks are called by the MODULE_EXIT below.
 */
void addThreadTermFunc(ThreadTermFunc onTerm)
{
    auto &termHooks = (GetCurrentThreadId() == mainThreadIdHelper.tid) ? mainThreadTermHooks : threadTermHooks;
    for (auto hook: termHooks)
    {
        if (hook==onTerm)
            return;
    }
    termHooks.push_back(onTerm);
}

void callThreadTerminationHooks(bool isPooled)
{
    std::vector<ThreadTermFunc> keepHooks;

    auto &termHooks = (GetCurrentThreadId() == mainThreadIdHelper.tid) ? mainThreadTermHooks : threadTermHooks;
    for (auto hook: termHooks)
    {
        if ((*hook)(isPooled) && isPooled)
           keepHooks.push_back(hook);
    }
    termHooks.swap(keepHooks);
}

PointerArray *exceptionHandlers = NULL;
MODULE_INIT(INIT_PRIORITY_JTHREAD)
{
    exceptionHandlers = new PointerArray();
    return true;
}
MODULE_EXIT()
{
    callThreadTerminationHooks(false);
    delete exceptionHandlers;
}

void addThreadExceptionHandler(IExceptionHandler *handler)
{
    assertex(exceptionHandlers); // have to ensure MODULE_INIT has appropriate priority.
    exceptionHandlers->append(handler);
}

void removeThreadExceptionHandler(IExceptionHandler *handler)
{
    exceptionHandlers->zap(handler);
}

static bool SEHHandling = false;
void enableThreadSEH() { SEHHandling=true; }
void disableThreadSEH() { SEHHandling=false; } // only prevents new threads from having SEH handler, no mech. for turning off existing threads SEH handling.


static std::atomic<unsigned> threadCount;
static size32_t defaultThreadStackSize=0;

/*
The following variables are used to avoid a race condition between thread termination and join().  There are two situations:
i) thread is free running. join will never be called.
ii) thread will be joined

The code in the thread termination code signals a semaphore to indicate it is complete, and then releases the thread
object.  This ensures the semaphore has not been destroyed before it is signalled, and that threads that are mot joined
are cleaned up correctly.

The code in join waits for the semaphore to be signalled, and then continues.  If the thread object is a contained
member the thread object may be destroyed before the release call in the thread termination code - which would lead to
memory corruption.

Options to prevent this:

* Added the thread to a list before the signal(), and removed after the Release().
  The joining thread can wake up immediately, but then needs to loop until the thread is not on the list.  (This
  involved getting a critical section).
  [This is the previous implementation.]

* The termination code holds the critical section for the { signal(), Release() } duration.
  The joining thread checks it can get that critical section before continuing.  The joining thread may wake up, try
  and get the critical section, and then block because the other thread hasn't released it yet, but this is better than
  the previous situation which would spin-loop.  The join will be blocked by all terminating threads - in a signal,
  rather than purely user code.
  [This is the new implementation]

* Terminating thread conditionally signals.
  The terminating thread could call Release(), and only signal() the semaphore if the object was not destroyed.  This would cope
  with the common joining pattern, but would fail if another object held onto a reference to the thread but didn't call join().
  Potentially more efficient, but more scope for accidental bugs.
*/
static CriticalSection ThreadDestroyLock;


#ifdef _WIN32
extern void *EnableSEHtranslation();

unsigned WINAPI Thread::_threadmain(LPVOID v)
#else
void *Thread::_threadmain(void *v)
#endif
{
    Thread * t = (Thread *)v;
    restoreThreadContext(t->savedCtx);
#ifdef _WIN32
    if (SEHHandling) 
        EnableSEHtranslation();
#else
    t->tidlog = threadLogID();
#endif
    int ret = t->begin();
    {
        try
        {
            CriticalBlock block(ThreadDestroyLock);
            t->stopped.signal();
            t->Release();
        }
        catch (...) {
            PROGLOG("thread release exception");
            throw;
        }
    }
#if defined(_WIN32)
    return ret;
#else
    return (void *) (memsize_t)ret;
#endif
}

// JCSMORE - should have a setPriority(), unsupported under _WIN32
void Thread::adjustPriority(int delta)
{
    if (delta < -2)
        prioritydelta = -2;
    else if (delta > 2)
        prioritydelta = 2;
    else
        prioritydelta = delta;

    if (alive)
    {
#if defined(_WIN32)
        int priority;
        switch (delta)
        {
        case -2: priority = THREAD_PRIORITY_LOWEST; break;
        case -1: priority = THREAD_PRIORITY_BELOW_NORMAL; break;
        case  0: priority = THREAD_PRIORITY_NORMAL; break;
        case +1: priority = THREAD_PRIORITY_ABOVE_NORMAL; break;
        case +2: priority = THREAD_PRIORITY_HIGHEST; break;
        }

        SetThreadPriority(hThread, priority);
#else
        //MORE - What control is there?
        int policy;
        sched_param param;
        int rc;
        if (( rc = pthread_getschedparam(threadid, &policy, &param)) != 0) 
            DBGLOG("pthread_getschedparam error: %d", rc);
        switch (delta)
        {
        // JCS - doubtful whether these good values...
        case -2: param.sched_priority =   0; policy =SCHED_OTHER; break;
        case -1: param.sched_priority =   0; policy =SCHED_OTHER; break;
        case  0: param.sched_priority =   0; policy =SCHED_OTHER; break;
        case +1: param.sched_priority =  (sched_get_priority_max(SCHED_RR)-sched_get_priority_min(SCHED_RR))/2; policy =SCHED_RR;       break;
        case +2: param.sched_priority =  sched_get_priority_max(SCHED_RR); policy =SCHED_RR;    break;
        }
        if(( rc = pthread_setschedparam(threadid, policy, &param)) != 0) 
            DBGLOG("pthread_setschedparam error: %d policy=%i pr=%i id=%" I64F "u PID=%i", rc,policy,param.sched_priority,(unsigned __int64) threadid,getpid());
        else
            DBGLOG("priority set id=%" I64F "u policy=%i pri=%i PID=%i",(unsigned __int64) threadid,policy,param.sched_priority,getpid());
#endif
    }
}

void Thread::adjustNiceLevel()
{
#if defined(_WIN32)
    int priority;
    if(nicelevel < -15)
        priority = THREAD_PRIORITY_TIME_CRITICAL;
    else if(nicelevel >= -15 && nicelevel < -10)
        priority = THREAD_PRIORITY_HIGHEST;
    else if(nicelevel >= -10 && nicelevel < 0)
        priority = THREAD_PRIORITY_ABOVE_NORMAL;
    else if(nicelevel == 0)
        priority = THREAD_PRIORITY_NORMAL;
    else if(nicelevel > 0 && nicelevel <= 10)
        priority = THREAD_PRIORITY_BELOW_NORMAL;
    else if(nicelevel > 10 && nicelevel <= 15)
        priority = THREAD_PRIORITY_LOWEST;
    else if(nicelevel >15)
        priority = THREAD_PRIORITY_IDLE;
    SetThreadPriority(hThread, priority);
#elif defined(__linux__)
    setpriority(PRIO_PROCESS, 0, nicelevel);
#else
    UNIMPLEMENTED;
#endif
}

bool Thread::isCurrentThread() const
{
    return GetCurrentThreadId() == threadid;
}

// _nicelevel ranges from -20 to 19, the higher the nice level, the less cpu time the thread will get.
void Thread::setNice(int _nicelevel)
{
    if (_nicelevel < -20 || _nicelevel > 19)
        throw MakeStringException(0, "nice level should be between -20 and 19");

    if(alive)
        throw MakeStringException(0, "nice can only be set before the thread is started.");
    
    nicelevel = _nicelevel;
}

void Thread::setStackSize(size32_t size)
{
    stacksize = (unsigned short)(size/0x1000);
}

void Thread::setDefaultStackSize(size32_t size)
{
    defaultThreadStackSize = size; // has no effect under windows (though may be used for calculations later)
}

int Thread::begin()
{
    if(nicelevel)
        adjustNiceLevel();
#ifndef _WIN32
    starting.signal();
    suspend.wait();
#endif
    int ret=-1;
    try {
        ret = run();
    }
    catch (IException *e)
    {
        handleException(e);
    }
#ifndef NO_CATCHALL
    catch (...)
    {
        handleException(MakeStringException(0, "Unknown exception in Thread %s", getName()));
    }
#endif
    callThreadTerminationHooks(false);
#ifdef _WIN32
#ifndef _DEBUG
    CloseHandle(hThread);   // leak handle when debugging, 
                            // fixes some lockups/crashes in the debugger when lots of threads being created
#endif
    hThread = NULL;
#endif
    //alive = false;        // not safe here
    return ret;
}

void Thread::handleException(IException *e)
{
    assertex(exceptionHandlers);
    if (exceptionHandlers->ordinality() == 0)
    {
        if (!dynamic_cast<InterruptedSemaphoreException *>(e))
            PrintExceptionLog(e,getName());
        //throw; // don't rethrow unhandled, preferable over alternative of causing process death
        e->Release();
    }
    else
    {
        PrintExceptionLog(e,getName());
        bool handled = false;
        ForEachItemIn(ie, *exceptionHandlers)
        {
            IExceptionHandler *handler = (IExceptionHandler *) exceptionHandlers->item(ie);
            handled = handler->fireException(e) || handled;
        }
        if (!handled) 
        {
            // if nothing choose to handle it.
            EXCLOG(e, NULL);
            //throw e; // don't rethrow unhandled, preferable over alternative of causing process death
        }
        e->Release();
    }
}

void Thread::init(const char *_name)
{
#ifdef _WIN32
    hThread = NULL;
#endif
    threadid = 0;
    tidlog = 0;
    alive = false;
    cthreadname.set(_name);
    prioritydelta = 0;
    nicelevel = 0;
    stacksize = 0; // default is EXE default stack size  (set by /STACK)
}

void Thread::captureThreadLoggingInfo()
{
    ::saveThreadContext(savedCtx);
}

void Thread::start(bool inheritThreadContext)
{
    if (inheritThreadContext)
        captureThreadLoggingInfo();
    if (alive) {
        IWARNLOG("Thread::start(%s) - Thread already started!",getName());
        PrintStackReport();
#ifdef _DEBUG
        throw MakeStringException(-1,"Thread::start(%s) - Thread already started!",getName());
#endif
        return;
    }
    Link();
    startRelease();
}

StringBuffer & Thread::getInfo(StringBuffer &str)
{
    const char * status = alive ? "" : "Stopped ";
    str.appendf("%8" I64F "X %6" I64F "d %u: %s%s",(__int64)threadid,(__int64)threadid,tidlog,status,getName());
    return str;
}

void Thread::startRelease()
{
    assertex(!alive);
    stopped.reinit(0); // just in case restarting
#ifdef _WIN32
    hThread = (HANDLE)_beginthreadex(NULL, 0x1000*(unsigned)stacksize, Thread::_threadmain, this, CREATE_SUSPENDED, (unsigned *)&threadid);
    if (!hThread || !threadid)
    {
        Release();
        throw makeOsException(GetLastError());
    }
#else
    int status;
    unsigned numretrys = 8;
    unsigned delay = 1000;
    for (;;) {
        pthread_attr_t attr;
        status = pthread_attr_init(&attr);
        if (status)
        {
            IERRLOG("pthread_attr_init returns %d",status);
            throw makeOsException(status);
        }
        pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (stacksize)
            pthread_attr_setstacksize(&attr, (unsigned)stacksize*0x1000);
        else if (defaultThreadStackSize)
            pthread_attr_setstacksize(&attr, defaultThreadStackSize);
        else {
#ifndef __64BIT__       // no need to cap 64bit
            size_t defss=0;
            pthread_attr_getstacksize(&attr, &defss);
            if (defss>LINUX_STACKSIZE_CAP)
                pthread_attr_setstacksize(&attr, LINUX_STACKSIZE_CAP);
#endif
        }
        sched_param param;
        pthread_attr_getschedparam(&attr, &param);
        param.sched_priority = 0;
        pthread_attr_setschedparam(&attr, &param);
        pthread_attr_setschedpolicy(&attr, SCHED_OTHER);
        pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
        status = pthread_create(&threadid, &attr, Thread::_threadmain, this);
        pthread_attr_destroy(&attr);
        if ((status==EAGAIN)||(status==EINTR)) {
            if (numretrys--==0)
                break;
            IWARNLOG("pthread_create(%d): Out of threads, retrying...",status);
            Sleep(delay);
            delay *= 2;
        }
        else 
            break;
    } 
    if (status) {
        threadid = 0;
        Release();
        IERRLOG("pthread_create returns %d",status);
        PrintStackReport();
        PrintMemoryReport();
        throw makeOsException(status);
    }
    unsigned retryCount = 10;
    for (;;)
    {
        if (starting.wait(1000*10))
            break;
        else if (0 == --retryCount)
            throw MakeStringException(-1, "Thread::start(%s) failed", getName());
        IWARNLOG("Thread::start(%s) stalled, waiting to start, retrying", getName());
    }
#endif
    alive = true;
    if (prioritydelta)
        adjustPriority(prioritydelta);
    threadCount++;
#ifdef _WIN32
    DWORD count = ResumeThread(hThread);
    assertex(count == 1);
#else
    suspend.signal();
#endif
}


bool Thread::join(unsigned timeout)
{
    if (!alive&&!threadid) {
#ifdef _DEBUG
        PROGLOG("join on unstarted thread!");
        PrintStackReport();
#endif
        return true;
    }
    if (!stopped.wait(timeout)) 
        return false;
    if (!alive)         // already joined
    {
        stopped.signal();
        return true;
    }

    {
        //Enter and leave the critical section to ensure that the terminating thread has called Release() - it has
        //already signalled the stopped semaphore
        CriticalBlock block(ThreadDestroyLock);
    }

#ifdef _DEBUG
    int c = getLinkCount();
    if (c>=DEAD_PSEUDO_COUNT) {
        PROGLOG("Dead/Dying thread joined! %d",c);
        PrintStackReport();
    }
#endif

    alive = false;      // should be safe here
    stopped.signal();  // signal stopped again, to prevent any parallel call from blocking.
    return true;
}



Thread::~Thread()
{
#ifdef _DEBUG
    if (alive) {
        if (!stopped.wait(0)) { // see if fell out of threadmain and signal stopped
            PROGLOG("Live thread killed! %s",getName());
            PrintStackReport();
        }
        // don't need to resignal as we are on way out
    }
#endif
    threadCount--;
//  DBGLOG("Thread %x (%s) destroyed\n", threadid, cthreadname.str());
}

unsigned getThreadCount()
{
    return threadCount;
}

// CThreadedPersistent

CThreadedPersistent::CThreadedPersistent(const char *name, IThreaded *_owner) : athread(*this, name), owner(_owner), state(s_ready)
{
    halt = false;
    athread.start(false);
}

CThreadedPersistent::~CThreadedPersistent()
{
    join(INFINITE, false);
    halt = true;
    sem.signal();
    athread.join();
}

void CThreadedPersistent::threadmain()
{
    for (;;)
    {
        sem.wait();
        if (halt)
            break;
        try
        {
            restoreThreadContext(athread.savedCtx);
            owner->threadmain();
            // Note we do NOT call the thread reset hook here - these threads are expected to be able to preserve state, I think
        }
        catch (IException *e)
        {
            VStringBuffer errMsg("CThreadedPersistent (%s)", athread.getName());
            EXCLOG(e, errMsg.str());
            exception.setown(e);
            joinSem.signal(); // leave in running state, signal to join to handle
            continue;
        }
        unsigned expected = s_running;
        if (!state.compare_exchange_strong(expected, s_ready))
        {
            expected = s_joining;
            if (state.compare_exchange_strong(expected, s_ready))
                joinSem.signal();
        }
    }
}

void CThreadedPersistent::start(bool inheritThreadContext)
{
    unsigned expected = s_ready;
    if (!state.compare_exchange_strong(expected, s_running))
    {
        VStringBuffer msg("CThreadedPersistent::start(%s) - not ready", athread.getName());
        IWARNLOG("%s", msg.str());
        PrintStackReport();
        throw MakeStringExceptionDirect(-1, msg.str());
    }
    if (inheritThreadContext)
        athread.captureThreadLoggingInfo();
    sem.signal();
}

bool CThreadedPersistent::join(unsigned timeout, bool throwException)
{
    unsigned expected = s_running;
    if (state.compare_exchange_strong(expected, s_joining))
    {
        if (!joinSem.wait(timeout))
        {
            unsigned expected = s_joining;
            if (state.compare_exchange_strong(expected, s_running)) // if still joining, restore running state
                return false;
            // if here, threadmain() set s_ready after timeout and has or will signal
            if (!joinSem.wait(60000) && throwException) // should be instant
                throwUnexpected();
            return true;
        }
        if (throwException && exception.get())
        {
            // switch back to ready state and throw
            Owned<IException> e = exception.getClear();
            unsigned expected = s_joining;
            if (!state.compare_exchange_strong(expected, s_ready))
                throwUnexpected();
            throw e.getClear();
        }
    }
    return true;
}

//class CAsyncFor

void CAsyncFor::For(unsigned num,unsigned maxatonce,bool abortFollowingException, bool shuffled)
{
    if (num <= 1)
    {
        if (num == 1)
            Do(0);
        return;
    }
    AtomicShared<IException> e;
    Owned<IShuffledIterator> shuffler;
    if (shuffled) {
        shuffler.setown(createShuffledIterator(num));
        shuffler->first(); // prime (needed to make thread safe)
    }
    unsigned i;
    if (maxatonce==1) { // no need for threads
        for (i=0;i<num;i++) {
            unsigned idx = shuffled?shuffler->lookup(i):i;
            try {
                Do(idx);
            }
            catch (IException * _e)
            {
                e.setownIfNull(_e);
                if (abortFollowingException) 
                    break;
            }
        }
    }
    else {
        if (maxatonce==0 || maxatonce > num)
            maxatonce = num;
        if (maxatonce < num)
        {
            class cdothread: public Thread
            {
            public:
                Semaphore &ready;
                AtomicShared<IException> &erre;
                unsigned idx;
                CAsyncFor *self;
                cdothread(CAsyncFor *_self,unsigned _idx,Semaphore &_ready,AtomicShared<IException> &_e)
                    : Thread("CAsyncFor"),ready(_ready),erre(_e)
                {
                    idx = _idx;
                    self = _self;
                }
                int run()
                {
                    try {
                        self->Do(idx);
                    }
                    catch (IException * _e)
                    {
                        erre.setownIfNull(_e);
                    }
    #ifndef NO_CATCHALL
                    catch (...)
                    {
                        erre.setownIfNull(MakeStringException(0, "Unknown exception in Thread %s", getName()));
                    }
    #endif
                    ready.signal();
                    return 0;
                }
            };
            Semaphore ready;
            for (i=0;(i<num)&&(i<maxatonce);i++)
                ready.signal();
            IArrayOf<Thread> started;
            started.ensureCapacity(num);
            for (i=0;i<num;i++) {
                ready.wait();
                if (abortFollowingException && e.isSet()) break;
                Owned<Thread> thread = new cdothread(this,shuffled?shuffler->lookup(i):i,ready,e);
                thread->start(true);
                started.append(*thread.getClear());
            }
            ForEachItemIn(idx, started)
            {
                started.item(idx).join();
            }
        }
        else
        {
            // Common case of execute all at once can be optimized a little
            // Note that shuffle and abortFollowingException are meaningless when executing all at once
            class cdothread: public Thread
            {
            public:
                AtomicShared<IException> &erre;
                unsigned idx;
                CAsyncFor *self;
                cdothread(CAsyncFor *_self,unsigned _idx,AtomicShared<IException>&_e)
                    : Thread("CAsyncFor"),erre(_e)
                {
                    idx = _idx;
                    self = _self;
                }
                int run()
                {
                    try {
                        self->Do(idx);
                    }
                    catch (IException * _e)
                    {
                        erre.setownIfNull(_e);
                    }
    #ifndef NO_CATCHALL
                    catch (...)
                    {
                        erre.setownIfNull(MakeStringException(0, "Unknown exception in Thread %s", getName()));
                    }
    #endif
                    return 0;
                }
            };
            IArrayOf<Thread> started;
            started.ensureCapacity(num);
            for (i=0;i<num-1;i++)
            {
                Owned<Thread> thread = new cdothread(this,i,e);
                thread->start(true);
                started.append(*thread.getClear());
            }

            try {
                Do(num-1);
            }
            catch (IException * _e)
            {
                e.setownIfNull(_e);
            }
#ifndef NO_CATCHALL
            catch (...)
            {
                e.setownIfNull(MakeStringException(0, "Unknown exception in main Thread"));
            }
#endif

            ForEachItemIn(idx, started)
            {
                started.item(idx).join();
            }
        }
    }
    if (e.isSet())
        throw e.getClear();
}

void CAsyncFor::TaskFor(unsigned num, ITaskScheduler & scheduler)
{
    if (num <= 1)
    {
        if (num == 1)
            Do(0);
        return;
    }

    Owned<CCompletionTask> completed = new CCompletionTask(scheduler);
    for (unsigned i=0; i < num; i++)
        completed->spawn([i, this]() { Do(i); });

    completed->decAndWait();
}

// ---------------------------------------------------------------------------
// Thread Pools
// ---------------------------------------------------------------------------


class CPooledThreadWrapper;


class CThreadPoolBase
{
public:
    CThreadPoolBase(bool _inheritThreadContext)
    : inheritThreadContext(_inheritThreadContext)
    {
    }
    virtual ~CThreadPoolBase() {}
protected: friend class CPooledThreadWrapper;
    IExceptionHandler *exceptionHandler = nullptr;
    CriticalSection crit;
    StringAttr poolname;
    Semaphore donesem;
    PointerArray waitingsems;
    UnsignedArray waitingids;
    std::atomic<bool> stopall{false};
    const bool inheritThreadContext;
    unsigned defaultmax = 0;
    unsigned targetpoolsize = 0;
    unsigned delay = 0;
    Semaphore availsem;
    std::atomic_uint numrunning{0};
    virtual void notifyStarted(CPooledThreadWrapper *item)=0;
    virtual bool notifyStopped(CPooledThreadWrapper *item)=0;
};


class CPooledThreadWrapper: public Thread
{
    PooledThreadHandle handle;
    IPooledThread *thread;
    Semaphore sem;
    CThreadPoolBase &parent;
    StringAttr runningName;
public:
    CPooledThreadWrapper(CThreadPoolBase &_parent,
                         PooledThreadHandle _handle,
                         IPooledThread *_thread) // takes ownership of thread
        : Thread(StringBuffer("Member of thread pool: ").append(_parent.poolname).str()), parent(_parent)
    {
        thread = _thread;
        handle = _handle;
        runningName.set(_parent.poolname); 
    }

    ~CPooledThreadWrapper()
    {
        thread->Release();
    }

    void setName(const char *name) { runningName.set(name); }
    void setHandle(PooledThreadHandle _handle) { handle = _handle; }
    PooledThreadHandle queryHandle() { return handle; }
    IPooledThread &queryThread() { return *thread; }
    void setThread(IPooledThread *_thread) { thread = _thread; } // takes ownership
    bool isStopped() { return (handle==0); }
    PooledThreadHandle markStopped()
    {
        PooledThreadHandle ret=handle;
        handle = 0;
        if (ret) // JCSMORE - I can't see how handle can not be set if here..
            parent.numrunning--;
        return ret;
    }
    void markStarted()
    {
        parent.numrunning++;
    }

    int run()
    {
        do
        {
            sem.wait();
            {
                CriticalBlock block(parent.crit); // to synchronize
                if (parent.stopall)
                    break;
            }
            if (parent.inheritThreadContext)
                restoreThreadContext(savedCtx);
            parent.notifyStarted(this);
            try
            {
                cthreadname.swapWith(runningName); // swap running name and threadname
                thread->threadmain();
                cthreadname.swapWith(runningName); // swap back
            }
            catch (IException *e)
            {
                cthreadname.swapWith(runningName); // swap back
                handleException(e);
            }
#ifndef NO_CATCHALL
            catch (...)
            {
                cthreadname.swapWith(runningName); // swap back
                handleException(MakeStringException(0, "Unknown exception in Thread from pool %s", parent.poolname.get()));
            }
#endif
            callThreadTerminationHooks(true);    // Reset any per-thread state.
        } while (parent.notifyStopped(this));
        return 0;
    }

    void cycle()
    {
        sem.signal();
    }

    void go(void *param)
    {
        thread->init(param);
        cycle();
    }

    bool stop()
    {
        if (handle)
            return thread->stop();
        return true;
    }

    void handleException(IException *e)
    {
        CriticalBlock block(parent.crit);
        PrintExceptionLog(e,parent.poolname.get());
        if (!parent.exceptionHandler||!parent.exceptionHandler->fireException(e)) {
        }
        e->Release();
    }


};


class CPooledThreadIterator: implements IPooledThreadIterator, public CInterface
{
    unsigned current;
public:
    IArrayOf<IPooledThread> threads;
    IMPLEMENT_IINTERFACE;
    CPooledThreadIterator()
    {
        current = 0;
    }

    bool first()
    {
        current = 0;
        return threads.isItem(current);
    }

    bool next()
    {
        current++;
        return threads.isItem(current);
    }

    bool isValid()
    {
        return threads.isItem(current);
    }

    IPooledThread & query()
    {
        return threads.item(current);
    }
};




class CThreadPool: public CThreadPoolBase, implements IThreadPool, public CInterface
{
    IArrayOf<CPooledThreadWrapper> threadwrappers;
    PooledThreadHandle nextid;
    IThreadFactory *factory;
    unsigned stacksize;
    unsigned timeoutOnRelease;
    unsigned traceStartDelayPeriod = 0;
    int niceValue = 0;
    unsigned startsInPeriod = 0;
    cycle_t startDelayInPeriod = 0;
    CCycleTimer overAllTimer;

    PooledThreadHandle _start(void *param,const char *name, bool noBlock, unsigned timeout=0)
    {
        CCycleTimer startTimer;
        bool waited = false;
        bool timedout = false;
        if (defaultmax)
        {
            waited = !availsem.wait(0);
            if (noBlock)
                timedout = waited;
            else if (waited)
                timedout = !availsem.wait(timeout>0?timeout:delay);
        }
        PooledThreadHandle ret;
        {
            CriticalBlock block(crit);
            if (timedout)
            {
                if (!availsem.wait(0)) // make sure take allocated sem if has become available
                {
                    if (noBlock || timeout > 0)
                        throw MakeStringException(0, "No threads available in pool %s", poolname.get());
                    IWARNLOG("Pool limit exceeded for %s", poolname.get());
                }
            }
            if (traceStartDelayPeriod)
            {
                ++startsInPeriod;
                if (waited)
                    startDelayInPeriod += startTimer.elapsedCycles();
                if (overAllTimer.elapsedCycles() >= queryOneSecCycles()*traceStartDelayPeriod) // check avg. delay per minute
                {
                    double totalDelayMs = (static_cast<double>(cycle_to_nanosec(startDelayInPeriod)))/1000000;
                    double avgDelayMs = (static_cast<double>(cycle_to_nanosec(startDelayInPeriod/startsInPeriod)))/1000000;
                    unsigned totalElapsedSecs = overAllTimer.elapsedMs()/1000;
                    PROGLOG("%s: %u threads started in last %u seconds, total delay = %0.2f milliseconds, average delay = %0.2f milliseconds, currently running = %u", poolname.get(), startsInPeriod, totalElapsedSecs, totalDelayMs, avgDelayMs, runningCount());
                    startsInPeriod = 0;
                    startDelayInPeriod = 0;
                    overAllTimer.reset();
                }
            }
            CPooledThreadWrapper &t = allocThread();
            if (name)
                t.setName(name);
            if (inheritThreadContext)
                t.captureThreadLoggingInfo();
            t.go(param);
            ret = t.queryHandle();
        }
        Sleep(0);
        return ret;
    }

public:
    IMPLEMENT_IINTERFACE;
    CThreadPool(IThreadFactory *_factory,bool _inheritThreadContext, IExceptionHandler *_exceptionHandler,const char *_poolname,unsigned _defaultmax, unsigned _delay, unsigned _stacksize, unsigned _timeoutOnRelease, unsigned _targetpoolsize)
    : CThreadPoolBase(_inheritThreadContext)
    {
        poolname.set(_poolname);
        factory = LINK(_factory);
        exceptionHandler = _exceptionHandler;
        nextid = 1;
        stopall = false;
        defaultmax = _defaultmax;
        delay = _delay;
        if (defaultmax)
            availsem.signal(defaultmax);
        stacksize = _stacksize;
        timeoutOnRelease = _timeoutOnRelease;
        targetpoolsize = _targetpoolsize?_targetpoolsize:defaultmax;
    }

    ~CThreadPool()
    {
        stopAll(true);
        if (!joinAll(true, timeoutOnRelease))
            IWARNLOG("%s; timedout[%d] waiting for threads in pool", poolname.get(), timeoutOnRelease);
        CriticalBlock block(crit);
        bool first=true;
        ForEachItemIn(i,threadwrappers)
        {
            CPooledThreadWrapper &t = threadwrappers.item(i);
            if (!t.isStopped())
            {
                if (first)
                {
                    IWARNLOG("Threads still active: ");
                    first = false;
                }
                StringBuffer threadInfo;
                PROGLOG("Active thread: %s, info: %s", t.getName(), t.getInfo(threadInfo).str());
            }
        }       
        factory->Release();
    }

    CPooledThreadWrapper &allocThread()
    {   // called in critical section
        PooledThreadHandle newid=nextid++;
        if (newid==0)
            newid=nextid++;
        ForEachItemIn(i,threadwrappers) {
            CPooledThreadWrapper &it = threadwrappers.item(i);
            if (it.isStopped()) {
                it.setHandle(newid);
                if (!it.queryThread().canReuse()) {
                    it.queryThread().Release();
                    it.setThread(factory->createNew());
                }
                return it;
            }
        }
        CPooledThreadWrapper &ret = *new CPooledThreadWrapper(*this,newid,factory->createNew());
        if (stacksize)
            ret.setStackSize(stacksize);
        if (niceValue)
            ret.setNice(niceValue);
        ret.start(false);
        threadwrappers.append(ret);
        return ret;
    }

    CPooledThreadWrapper *findThread(PooledThreadHandle handle)
    {   // called in critical section
        ForEachItemIn(i,threadwrappers) {
            CPooledThreadWrapper &it = threadwrappers.item(i);
            if (it.queryHandle()==handle)
                return &it;
        }
        return NULL;
    }

    PooledThreadHandle startNoBlock(void *param)
    {
        return _start(param, NULL, true);
    }

    PooledThreadHandle start(void *param)
    {
        return _start(param, NULL, false);
    }

    PooledThreadHandle start(void *param,const char *name)
    {
        return _start(param, name, false);
    }

    PooledThreadHandle start(void *param,const char *name, unsigned timeout)
    {
        return _start(param, name, false, timeout);
    }

    bool stop(PooledThreadHandle handle)
    {
        CriticalBlock block(crit);
        CPooledThreadWrapper *t = findThread(handle);
        if (t)
            return t->stop();
        return true;    // already stopped
    }

    bool stopAll(bool tryall=false)
    {
        availsem.signal(1000);
        availsem.wait();
        CriticalBlock block(crit);
        bool ret=true;
        ForEachItemIn(i,threadwrappers) {
            CPooledThreadWrapper &it = threadwrappers.item(i);
            if (!it.stop()) {
                ret = false;
                if (!tryall)
                    break;
            }
        }
        return ret;
    }

    bool joinWait(CPooledThreadWrapper &t,unsigned timeout)
    {
        // called in critical section
        if (t.isStopped())
            return true;
        Semaphore sem;
        waitingsems.append(&sem);
        waitingids.append(t.queryHandle());
        crit.leave();
        bool ret = sem.wait(timeout);
        crit.enter();
        unsigned i = waitingsems.find(&sem);
        if (i!=NotFound) {
            waitingids.remove(i);
            waitingsems.remove(i);
        }
        return ret;
    }

    bool join(PooledThreadHandle handle,unsigned timeout=INFINITE)
    {
        CriticalBlock block(crit);
        CPooledThreadWrapper *t = findThread(handle);
        if (!t)
            return true;    // already stopped
        return joinWait(*t,timeout);
    }

    virtual bool joinAll(bool del,unsigned timeout=INFINITE)
    { // note timeout is for each join
        CriticalBlock block(crit);
        IArrayOf<CPooledThreadWrapper> tojoin;
        ForEachItemIn(i1,threadwrappers) {
            CPooledThreadWrapper &it = threadwrappers.item(i1);
            it.Link();
            tojoin.append(it);
        }
            
        ForEachItemIn(i2,tojoin) 
            if (!joinWait(tojoin.item(i2),timeout))  
                return false;
        if (del) {
            stopall = true;
            ForEachItemIn(i3,tojoin) 
                tojoin.item(i3).cycle();
            {
                CriticalUnblock unblock(crit);
                ForEachItemIn(i4,tojoin) 
                    tojoin.item(i4).join();
            }
            threadwrappers.kill();
            stopall = false;
        }
        return true;
    }

    IPooledThreadIterator *running()
    {
        CriticalBlock block(crit);
        CPooledThreadIterator *ret = new CPooledThreadIterator;
        ForEachItemIn(i,threadwrappers) {
            CPooledThreadWrapper &it = threadwrappers.item(i);
            if (!it.isStopped()) {
                IPooledThread &t = it.queryThread();
                t.Link();
                ret->threads.append(t);
            }
        }
        return ret;
    }

    unsigned runningCount()
    {
        return numrunning;
    }

    void notifyStarted(CPooledThreadWrapper *item)
    {
        item->markStarted();
    }
    bool notifyStopped(CPooledThreadWrapper *item)
    {
        CriticalBlock block(crit);
        PooledThreadHandle myid = item->markStopped();
        ForEachItemIn(i1,waitingids) { // tell anyone waiting
            if (waitingids.item(i1)==myid)
                ((Semaphore *)waitingsems.item(i1))->signal();
        }
        bool ret = true;
        if (defaultmax) {
            unsigned n=threadwrappers.ordinality();
            for (unsigned i2=targetpoolsize;i2<n;i2++) {        // only check excess for efficiency
                if (item==&threadwrappers.item(i2)) {
                    threadwrappers.remove(i2);
                    ret = false;
                    break;
                }
            }
            availsem.signal();
        }
        return ret;
    }
    void setStartDelayTracing(unsigned secs)
    {
        traceStartDelayPeriod = secs;
    }
    void setNiceValue(int value)
    {
        niceValue = value;
    }
    bool waitAvailable(unsigned timeout)
    {
        if (!defaultmax)
            return true;
        if (availsem.wait(timeout))
        {
            availsem.signal();
            return true;
        }
        return false;
    }
};


IThreadPool *createThreadPool(const char *poolname,IThreadFactory *factory,bool inheritThreadContext, IExceptionHandler *exceptionHandler,unsigned defaultmax, unsigned delay, unsigned stacksize, unsigned timeoutOnRelease, unsigned targetpoolsize)
{
    return new CThreadPool(factory,inheritThreadContext,exceptionHandler,poolname,defaultmax,delay,stacksize,timeoutOnRelease,targetpoolsize);
}

//=======================================================================================================

static void CheckAllowedProgram(const char *prog,const char *allowed)
{
    if (!prog||!allowed||(strcmp(allowed,"*")==0))
        return;
    StringBuffer head;
    bool inq = false;
    // note don't have to be too worried about odd quoting as matching fixed list
    while (*prog&&((*prog!=' ')||inq)) {
        if (*prog=='"')
            inq = !inq;
        head.append(*(prog++));
    }
    StringArray list;
    list.appendList(allowed, ",");
    ForEachItemIn(i,list) {
        if (WildMatch(head.str(),list.item(i)))
            return;
    }
    AERRLOG("Unauthorized pipe program(%s)",head.str());
    throw MakeStringException(-1,"Unauthorized pipe program(%s)",head.str());
}

class CPipeProcessException : public CSimpleInterfaceOf<IPipeProcessException>
{
    int errCode;
    StringAttr msg;
    MessageAudience audience;
public:
    CPipeProcessException(int _errCode, const char *_msg, MessageAudience _audience = MSGAUD_user) : errCode(_errCode), msg(_msg), audience(_audience)
    {
    }
    virtual int errorCode() const override { return errCode; }
    virtual StringBuffer & errorMessage(StringBuffer &str) const override
    {
        if (msg)
            str.append(msg).append(", ");
        return str.append(strerror(errCode));
    }
    MessageAudience errorAudience() const { return audience; }
};

IPipeProcessException *createPipeErrnoException(int code, const char *msg)
{
    return new CPipeProcessException(code, msg);
}

IPipeProcessException *createPipeErrnoExceptionV(int code, const char *msg, ...)
{
    StringBuffer eStr;
    va_list args;
    va_start(args, msg);
    eStr.limited_valist_appendf(1024, msg, args);
    va_end(args);
    return new CPipeProcessException(code, eStr.str());
}


class CSimplePipeStream: implements ISimpleReadStream, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CSimplePipeStream(IPipeProcess *_pipe, bool _isStderr) : pipe(_pipe), isStderr(_isStderr) {}
    virtual size32_t read(size32_t sz, void * data)
    {
        if (isStderr)
            return pipe->readError(sz, data);
        else
            return pipe->read(sz, data);
    }
private:
    Owned<IPipeProcess> pipe;
    bool isStderr;
};

#ifdef _WIN32


class CWindowsPipeProcess: implements IPipeProcess, public CInterface
{
    HANDLE pipeProcess;
    HANDLE hInput;
    HANDLE hOutput;
    HANDLE hError;
    StringAttr title;
    unsigned retcode;
    CriticalSection sect;
    bool aborted;
    StringAttr allowedprogs;
    StringArray env;

public:
    IMPLEMENT_IINTERFACE;

    CWindowsPipeProcess(const char *_allowedprogs)
        : allowedprogs(_allowedprogs)
    {
        pipeProcess = (HANDLE)-1;
        hInput=(HANDLE)-1;
        hOutput=(HANDLE)-1;
        hError=(HANDLE)-1;
        retcode = (unsigned)-1;
        aborted = false;
    }
    ~CWindowsPipeProcess()
    {
        kill();
    }

    void kill()
    {
        doCloseInput();
        doCloseOutput();
        doCloseError();
        if (pipeProcess != (HANDLE)-1) {
            CloseHandle(pipeProcess);
            pipeProcess = (HANDLE)-1;
        }
    }

    bool run(const char *_title,const char *prog,const char *dir,bool hasinput,bool hasoutput,bool haserror, size32_t stderrbufsize,bool newProcessGroup)
    {
        // size32_t stderrbufsize ignored as not required (I think)
        CriticalBlock block(sect);
        kill();
        title.clear();
        if (_title) {
            title.set(_title);
            PROGLOG("%s: Creating PIPE process : %s", title.get(), prog);
        }
        CheckAllowedProgram(prog,allowedprogs);
        SECURITY_ATTRIBUTES sa;
        sa.nLength = sizeof(SECURITY_ATTRIBUTES); 
        sa.bInheritHandle = TRUE; 
        sa.lpSecurityDescriptor = NULL; 
        HANDLE hProgOutput=(HANDLE)-1;
        HANDLE hProgInput=(HANDLE)-1;
        HANDLE hProgError=(HANDLE)-1;
        
        HANDLE h;
        //NB: Create a pipe handles that are not inherited our end 
        if (hasinput) {
            CreatePipe(&hProgInput,&h,&sa,0);       
            DuplicateHandle(GetCurrentProcess(),h, GetCurrentProcess(), &hInput, 0, FALSE, DUPLICATE_SAME_ACCESS);
            CloseHandle(h);
        }
        if (hasoutput) {
            CreatePipe(&h,&hProgOutput,&sa,0);      
            DuplicateHandle(GetCurrentProcess(),h, GetCurrentProcess(), &hOutput, 0, FALSE, DUPLICATE_SAME_ACCESS);
            CloseHandle(h);
        }
        if (haserror) {
            CreatePipe(&h,&hProgError,&sa,0);       
            DuplicateHandle(GetCurrentProcess(),h, GetCurrentProcess(), &hError, 0, FALSE, DUPLICATE_SAME_ACCESS);
            CloseHandle(h);
        }

        STARTUPINFO StartupInfo;
        _clear(StartupInfo);
        StartupInfo.cb = sizeof(StartupInfo);
        StartupInfo.wShowWindow = SW_HIDE;
        StartupInfo.dwFlags = STARTF_USESTDHANDLES|STARTF_USESHOWWINDOW ;
        StartupInfo.hStdOutput = hasoutput?hProgOutput:GetStdHandle(STD_OUTPUT_HANDLE);
        StartupInfo.hStdError  = haserror?hProgError:GetStdHandle(STD_ERROR_HANDLE);
        StartupInfo.hStdInput  = hasinput?hProgInput:GetStdHandle(STD_INPUT_HANDLE);
        
        PROCESS_INFORMATION ProcessInformation;

        // MORE - should create a new environment block that is copy of parent's, then set all the values in env array, and pass it

        if (!CreateProcess(NULL, (char *)prog, NULL,NULL,TRUE,0,NULL, dir&&*dir?dir:NULL, &StartupInfo,&ProcessInformation)) {
            if (_title) {
                StringBuffer errstr;
                formatSystemError(errstr, GetLastError());
                OERRLOG("%s: PIPE process '%s' failed: %s", title.get(), prog, errstr.str());
            }
            return false;
        }
        pipeProcess = ProcessInformation.hProcess;
        CloseHandle(ProcessInformation.hThread);
        if (hasoutput)
            CloseHandle(hProgOutput);
        if (hasinput)
            CloseHandle(hProgInput);
        if (haserror)
            CloseHandle(hProgError);
        return true;
    }

    virtual void setenv(const char *var, const char *value)
    {
        assertex(var);
        if (!value)
            value = "";
        VStringBuffer s("%s=%s", var, value);
        aindex_t found = lookupEnv(s);
        if (found != NotFound)
            env.replace(s, found);
        else
            env.append(s);
    }

    aindex_t lookupEnv(const char *_s)
    {
        ForEachItemIn(idx, env)
        {
            const char *v = env.item(idx);
            const char *s = _s;
            while (*s && *v && *s==*v)
            {
                if (*s=='=')
                    return idx;
                s++;
                v++;
            }
        }
        return NotFound;
    }

    size32_t read(size32_t sz, void *buf)
    {
        DWORD sizeRead;
        if (!ReadFile(hOutput, buf, sz, &sizeRead, NULL)) {
            //raise error here
            if(aborted)
                return 0;
            int err=GetLastError();
            switch(err)
            {
            case ERROR_HANDLE_EOF:
            case ERROR_BROKEN_PIPE:
            case ERROR_NO_DATA:
                return 0;
            default:
                aborted = true;
                IException *e = makeOsExceptionV(err, "Pipe: ReadFile failed (size %d)", sz);
                PrintExceptionLog(e, NULL);
                throw e;
            }
        }
        return aborted?0:((size32_t)sizeRead);
    }
    ISimpleReadStream *getOutputStream()
    {
        return new CSimplePipeStream(LINK(this), false);
    }
    size32_t readError(size32_t sz, void *buf)
    {
        DWORD sizeRead;
        if (!ReadFile(hError, buf, sz, &sizeRead, NULL)) {
            //raise error here
            if(aborted)
                return 0;
            int err=GetLastError();
            switch(err)
            {
            case ERROR_HANDLE_EOF:
            case ERROR_BROKEN_PIPE:
            case ERROR_NO_DATA:
                return 0;
            default:
                aborted = true;
                IException *e = makeOsExceptionV(err, "Pipe: ReadError failed (size %d)", sz);
                PrintExceptionLog(e, NULL);
                throw e;
            }
        }
        return aborted?0:((size32_t)sizeRead);
    }
    ISimpleReadStream *getErrorStream()
    {
        return new CSimplePipeStream(LINK(this), true);
    }
    size32_t write(size32_t sz, const void *buf)
    {
        DWORD sizeWritten;
        if (!WriteFile(hInput, buf, sz, &sizeWritten, NULL)) {
            int err=GetLastError();
            if ((err==ERROR_HANDLE_EOF)||aborted)
                sizeWritten = 0;
            else {
                IException *e = makeOsExceptionV(err, "Pipe: WriteFile failed (size %d)", sz);
                PrintExceptionLog(e, NULL);
                throw e;
            }
        }
        return aborted?((size32_t)-1):((size32_t)sizeWritten);
    }
    unsigned wait()
    {
        CriticalBlock block(sect);
        if (pipeProcess != (HANDLE)-1) {
            if (title.length())
                PROGLOG("%s: Pipe: Waiting for process to complete %d",title.get(),(unsigned)pipeProcess);

            {
                CriticalUnblock unblock(sect);
                WaitForSingleObject(pipeProcess, INFINITE);
            }
            if (pipeProcess != (HANDLE)-1) {
                GetExitCodeProcess(pipeProcess,(LPDWORD)&retcode);  // already got if notified
                CloseHandle(pipeProcess);
                pipeProcess = (HANDLE)-1;
            }
            if (title.length())
                PROGLOG("%s: Pipe: process complete",title.get());
        }
        return retcode;
    }

    unsigned wait(unsigned timeoutms, bool &timedout)
    {
        CriticalBlock block(sect);
        timedout = false;
        if (pipeProcess != (HANDLE)-1) {
            if (title.length())
                PROGLOG("%s: Pipe: Waiting for process to complete %d",title.get(),(unsigned)pipeProcess);

            {
                CriticalUnblock unblock(sect);
                if (WaitForSingleObject(pipeProcess, timeoutms)!=WAIT_OBJECT_0) {
                    timedout = true;
                    return retcode;
                }
            }
            if (pipeProcess != (HANDLE)-1) {
                GetExitCodeProcess(pipeProcess,(LPDWORD)&retcode);  // already got if notified
                CloseHandle(pipeProcess);
                pipeProcess = (HANDLE)-1;
            }
            if (title.length())
                PROGLOG("%s: Pipe: process complete",title.get());
        }
        return retcode;
    }


    void notifyTerminated(HANDLE pid,unsigned _retcode)
    {
        CriticalBlock block(sect);
        if ((pid!=(HANDLE)-1)&&(pid==pipeProcess)) {
            retcode = _retcode;
            pipeProcess = (HANDLE)-1;
        }
    }
    

    void doCloseInput()
    {
        CriticalBlock block(sect);
        if (hInput != (HANDLE)-1) {
            CloseHandle(hInput);
            hInput = (HANDLE)-1;
        }
    }

    void doCloseOutput()
    {
        CriticalBlock block(sect);
        if (hOutput != (HANDLE)-1) {
            CloseHandle(hOutput);
            hOutput = (HANDLE)-1;
        }
    }

    void doCloseError()
    {
        CriticalBlock block(sect);
        if (hError != (HANDLE)-1) {
            CloseHandle(hError);
            hError = (HANDLE)-1;
        }
    }

    void closeInput()
    {
        doCloseInput();
    }

    void closeOutput()
    {
        doCloseOutput();
    }

    void closeError()
    {
        doCloseError();
    }

    void abort()
    {
        CriticalBlock block(sect);
        if (pipeProcess != (HANDLE)-1) {
            if (title.length())
                PROGLOG("%s: Pipe Aborting",title.get());
            aborted = true;
            //doCloseOutput();                  // seems to work better without this
            doCloseInput();
            {
                CriticalUnblock unblock(sect);
                Sleep(100);
            }
            try {       // this code is problematic for some reason
                if (pipeProcess != (HANDLE)-1) {
                    TerminateProcess(pipeProcess, 255);
                    CloseHandle(pipeProcess);
                    pipeProcess = (HANDLE)-1;
                }
            }
            catch (...) {
                // ignore errors
            }
            if (title.length())
                PROGLOG("%s: Pipe Aborted",title.get());
        }
    }
    bool hasInput()
    {
        return hInput!=(HANDLE)-1;
    }
    bool hasOutput()
    {
        return hOutput!=(HANDLE)-1;
    }
    bool hasError()
    {
        return hError!=(HANDLE)-1;
    }
    HANDLE getProcessHandle()
    {
        return pipeProcess;
    }
    void setAllowTrace() override {}
};

IPipeProcess *createPipeProcess(const char *allowedprogs)
{
    return new CWindowsPipeProcess(allowedprogs);
}



#else

class CIgnoreSIGPIPE
{
public:
    CIgnoreSIGPIPE()
    {
        oact.sa_handler = SIG_IGN;
        struct sigaction act;
        sigset_t blockset;
        sigemptyset(&blockset);
        act.sa_mask = blockset;
        act.sa_handler = SIG_IGN;
        act.sa_flags = 0;
        sigaction(SIGPIPE, &act, &oact);
    }

    ~CIgnoreSIGPIPE()
    {
        if (oact.sa_handler != SIG_IGN)
            sigaction(SIGPIPE, &oact, NULL);
    }

private:
    struct sigaction oact;
};

#define WHITESPACE " \t\n\r"

static unsigned dowaitpid(HANDLE pid, int mode)
{
    while (pid != (HANDLE)-1) {
        int stat=-1;
        int ret = waitpid(pid, &stat, mode);
        if (ret>0) 
        {
            if (WIFEXITED(stat))
                return WEXITSTATUS(stat);
            else if (WIFSIGNALED(stat))
            {
                OERRLOG("Program was terminated by signal %u", (unsigned) WTERMSIG(stat));
                if (WTERMSIG(stat)==SIGPIPE)
                    return 0;
                return 254;
            }
            else
            {
                return 254;
            }
        }
        if (ret==0)
            break;
        int err = errno;
        if (err == ECHILD) 
            break;
        if (err!=EINTR) {
            OERRLOG("dowait failed with errcode %d",err);
            return (unsigned)-1;
        }       
    }
    return 0;
}

#ifdef __APPLE__
extern char **environ;
#endif

static CriticalSection runsect; // single thread process start to avoid forked handle open/closes interleaving
class CLinuxPipeProcess: implements IPipeProcess, public CInterface
{

    class cForkThread: public Thread
    {
        CLinuxPipeProcess *parent;
    public:
        cForkThread(CLinuxPipeProcess *_parent)
        {
            parent = _parent;
        }
        int run()
        {
            parent->run();
            return 0;
        }

    };

    Owned<cForkThread> forkthread;

    class cStdErrorBufferThread: public Thread
    {
        MemoryAttr buf;
        size32_t bufsize;
        Semaphore stopsem;
        CriticalSection &sect;
        int &hError;
    public:
        cStdErrorBufferThread(size32_t maxbufsize,int &_hError,CriticalSection &_sect)
            : sect(_sect), hError(_hError)
        {
            buf.allocate(maxbufsize);
            bufsize = 0;
        }
        int run()
        {
            while (!stopsem.wait(1000)) {
                CriticalBlock block(sect); 
                if (hError!=(HANDLE)-1) { // hmm who did that
                    fcntl(hError,F_SETFL,O_NONBLOCK); // make sure non-blocking
                    if (bufsize<buf.length()) {
                        size32_t sizeRead = (size32_t)::read(hError, (byte *)buf.bufferBase()+bufsize, buf.length()-bufsize); 
                        if ((int)sizeRead>0) { 
                            bufsize += sizeRead;
                        }
                    }
                    else { // flush (to avoid process blocking)
                        byte tmp[1024];
                        size32_t totsz = 0;
                        for (unsigned i=0;i<1024;i++) {
                            size32_t sz = (size32_t)::read(hError, tmp, sizeof(tmp));
                            if ((int)sz<=0)
                                break;
                            totsz+=sz;
                        }
                        if (totsz)
                            IWARNLOG("Lost %d bytes of stderr output",totsz);
                    }
                }

            }
            if (hError!=(HANDLE)-1) { // hmm who did that
                fcntl(hError,F_SETFL,0); // read any remaining data in blocking mode
                while (bufsize<buf.length()) {
                    size32_t sizeRead = (size32_t)::read(hError, (byte *)buf.bufferBase()+bufsize, buf.length()-bufsize);
                    if ((int)sizeRead>0)
                        bufsize += sizeRead;
                    else
                        break;
                }
            }
            return 0;
        }
        void stop()
        {
            stopsem.signal();
            Thread::join();
        }

    size32_t read(size32_t sz,void *out) 
        {
            CriticalBlock block(sect); 
            if (bufsize<sz) 
                sz = bufsize;
            if (sz>0) {
                memcpy(out,buf.bufferBase(),sz);
                if (sz!=bufsize) {
                    bufsize -= sz;
                    memmove(buf.bufferBase(),(byte *)buf.bufferBase()+sz,bufsize); // not ideal but hopefully not large
                }
                else
                    bufsize = 0;
            }
            return sz;
        }

    } *stderrbufferthread;

protected: friend class PipeWriterThread;
    HANDLE pipeProcess;
    HANDLE hInput;
    HANDLE hOutput;
    HANDLE hError;
    bool hasinput;
    bool hasoutput;
    bool haserror;
    bool newProcessGroup;
    StringAttr title;
    StringAttr cmd;
    StringAttr prog;
    StringAttr dir;
    int retcode;
    CriticalSection sect;
    Semaphore started;
    bool aborted;
    MemoryBuffer stderrbuf;
    size32_t stderrbufsize;
    StringAttr allowedprogs;
    StringArray env;
    bool allowTrace = false;

    void clearUtilityThreads(bool clearStderr)
    {
        Owned<cForkThread> ft;
        cStdErrorBufferThread *et;
        {
            CriticalBlock block(sect); // clear forkthread and optionally stderrbufferthread
            ft.setown(forkthread.getClear());
            et = stderrbufferthread;
            if (clearStderr)
                stderrbufferthread = nullptr;
        }
        if (ft)
        {
            ft->join();
            ft.clear();
        }
        if (et)
        {
            et->stop();
            if (clearStderr)
                delete et;
        }
    }
public:
    IMPLEMENT_IINTERFACE;

    CLinuxPipeProcess(const char *_allowedprogs)
        : allowedprogs(_allowedprogs)
    {
        pipeProcess = (HANDLE)-1;
        hInput=(HANDLE)-1;
        hOutput=(HANDLE)-1;
        hError=(HANDLE)-1;
        retcode = -1;
        aborted = false;
        stderrbufferthread = NULL;
        newProcessGroup = false;
    }
    ~CLinuxPipeProcess()
    {
        kill();
    }

    void kill() 
    {
        closeInput();
        closeOutput();
        closeError();
        clearUtilityThreads(true);
    }


    char **splitargs(const char *line,unsigned &argc)
    {
        char *buf = strdup(line);
        // first count params       (this probably could be improved)
        char *s = buf;
        argc = 0;
        while (readarg(s))
            argc++;
        free(buf);
        size32_t l = strlen(line)+1;
        size32_t al = (argc+1)*sizeof(char *);
        char **argv = (char **)malloc(al+l);
        argv[argc] = NULL;
        s = ((char *)argv)+al;
        memcpy(s,line,l);
        for (unsigned i=0;i<argc;i++) 
            argv[i] = readarg(s);
        return argv;
    }


    void run()
    {
        int inpipe[2];
        int outpipe[2];
        int errpipe[2];
        if (aborted ||
            (hasinput && (::pipe(inpipe)==-1)) ||
            (hasoutput && (::pipe(outpipe)==-1)) ||
            (haserror && (::pipe(errpipe)==-1)))
        {
            retcode = START_FAILURE;
            started.signal();
            throw makeOsException(errno);
        }
        ConstPointerArrayOf<char> envp;
        if (env.length())
        {
            ForEachItemIn(idx, env)
            {
                envp.append(env.item(idx));
            }
            // Now add any existing environment variables that were not overridden
            char **e = getSystemEnv();
            while (*e)
            {
                const char *entry = *e++;
                if (lookupEnv(entry) != NotFound)
                    continue;
                envp.append(entry);
            }
            envp.append(nullptr);
        }
#ifdef __linux__
        sem_t *mutex = nullptr;
        int shmid=0;
        if (allowTrace)
        {
            shmid = shmget(0, sizeof(sem_t)*2, IPC_CREAT | SHM_R | SHM_W);
            assertex(shmid>=0);
            mutex = (sem_t *) shmat(shmid, NULL, 0);
            assertex(mutex);
            int r = sem_init(&mutex[0], 1, 0);
            assertex(r==0);
            r = sem_init(&mutex[1], 1, 0);
            assertex(r==0);
        }
#endif

        /* NB: Important to call splitargs (which calls malloc) before the fork()
         * and not in the child process. Because performing malloc in the child
         * process, which then calls exec() can cause problems for TBB malloc proxy.
         */
        unsigned argc;
        char **argv=splitargs(prog,argc);
        for (;;)
        {
            pipeProcess = (HANDLE)fork();
            if (pipeProcess!=(HANDLE)-1) 
                break;
            if (errno!=EAGAIN) {
                if (hasinput) {
                    close(inpipe[0]);
                    close(inpipe[1]);
                }
                if (hasoutput) {
                    close(outpipe[0]);
                    close(outpipe[1]);
                }
                if (haserror) {
                    close(errpipe[0]);
                    close(errpipe[1]);
                }
                retcode = START_FAILURE;
                started.signal();
                free(argv);
                return;
            }
        }
        // NOTE - from here to the execvp/_exit call, we must only call "signal-safe" functions, that do not do any memory allocation
        // fork() only clones the one thread from parent process, meaning any mutexes/semaphores protecting multi-threaded access
        // to (for example) malloc cannot be relied upon not to be locked, and will never be unlocked if they are.
        if (pipeProcess)
        {
#ifdef __linux__
            if (allowTrace)
            {
                prctl(PR_SET_PTRACER, pipeProcess, 0, 0, 0);
                sem_post(&mutex[0]);
                sem_wait(&mutex[1]);
                sem_destroy(&mutex[0]);
                sem_destroy(&mutex[1]);
                shmdt(mutex);
                shmctl(shmid, IPC_RMID, NULL);
            }
#endif
        }
        else
        { // child
#ifdef __linux__
            if (allowTrace)
            {
                sem_wait(&mutex[0]);
                sem_post(&mutex[1]);
                shmdt(mutex);
            }
#endif
            if (newProcessGroup)//Force the child process into its own process group, so we can terminate it and its children.
                setpgid(0,0);
            if (hasinput) {
                dup2(inpipe[0],0);
                close(inpipe[0]);
                close(inpipe[1]);
            }
            if (hasoutput) {
                dup2(outpipe[1],1);
                close(outpipe[0]);
                close(outpipe[1]);
            }
            if (haserror) {
                dup2(errpipe[1],2);
                close(errpipe[0]);
                close(errpipe[1]);
            }

            if (dir.get() && chdir(dir) == -1)
            {
                if (haserror)
                {
                    fprintf(stderr, "ERROR: CLinuxPipeProcess::run: could not change dir to %s", dir.str());
                    fflush(stderr);
                }
                _exit(START_FAILURE);    // must be _exit!!
            }
            if (envp.length())
                environ = (char **) envp.detach();
            execvp(argv[0], argv);
            if (haserror)
            {
                fprintf(stderr, "ERROR: %d: exec failed: %s, %s", errno, prog.str(), strerror(errno));
                fflush(stderr);
            }
            _exit(START_FAILURE);    // must be _exit!!
        }
        free(argv);
        if (hasinput) 
            close(inpipe[0]);
        if (hasoutput) 
            close(outpipe[1]);
        if (haserror)
            close(errpipe[1]);
        hInput = hasinput?inpipe[1]:((HANDLE)-1);
        hOutput = hasoutput?outpipe[0]:((HANDLE)-1);
        hError = haserror?errpipe[0]:((HANDLE)-1);
        started.signal();
        retcode = dowaitpid(pipeProcess, 0);
        if (retcode==START_FAILURE) 
            closeOutput();
    }

    bool run(const char *_title,const char *_prog,const char *_dir,bool _hasinput,bool _hasoutput, bool _haserror, size32_t stderrbufsize, bool _newProcessGroup)
    {
        CriticalBlock runblock(runsect);
        kill();
        CriticalBlock block(sect); 
        hasinput = _hasinput;
        hasoutput = _hasoutput;
        haserror = _haserror;
        newProcessGroup = _newProcessGroup;
        title.clear();
        prog.set(_prog);
        dir.set(_dir);
        if (_title)
        {
            title.set(_title);
            StringBuffer envText;
            ForEachItemIn(idx, env)
            {
                const auto & cur = env.item(idx);
                envText.append(" ").append(cur);
            }

            PROGLOG("%s: Creating PIPE program process : '%s' - hasinput=%d, hasoutput=%d stderrbufsize=%d [%s] in (%s)", title.get(), prog.get(),(int)hasinput, (int)hasoutput, stderrbufsize, envText.str(), _dir ? _dir : "<cwd>");
        }
        CheckAllowedProgram(prog,allowedprogs);
        retcode = 0;
        if (forkthread)
        {
            {
                CriticalUnblock unblock(sect); 
                forkthread->join();
            }
            forkthread.clear();
        }
        forkthread.setown(new cForkThread(this));
        forkthread->start(true);
        bool joined = false;
        {
            CriticalUnblock unblock(sect); 
            started.wait();
            joined = forkthread->join(50); // give a chance to fail
        }
        // only check retcode if we were able to join
        if ( (joined) && (retcode==START_FAILURE) )
        {
            DBGLOG("%s: PIPE process '%s' failed to start", title.get()?title.get():"CLinuxPipeProcess", prog.get());
            forkthread.clear();
            return false;
        }
        if (stderrbufsize)
        {
            if (stderrbufferthread)
            {
                stderrbufferthread->stop();
                delete stderrbufferthread;
            }
            stderrbufferthread = new cStdErrorBufferThread(stderrbufsize,hError,sect);
            stderrbufferthread->start(true);
        }
        return true;
    }

    virtual void setenv(const char *var, const char *value)
    {
        assertex(var);
        if (!value)
            value = "";
        VStringBuffer s("%s=%s", var, value);
        aindex_t found = lookupEnv(s);
        if (found != NotFound)
            env.replace(s, found);
        else
            env.append(s);
    }

    aindex_t lookupEnv(const char *_s)
    {
        ForEachItemIn(idx, env)
        {
            const char *v = env.item(idx);
            const char *s = _s;
            while (*s && *v && *s==*v)
            {
                if (*s=='=')
                    return idx;
                s++;
                v++;
            }
        }
        return NotFound;
    }

    size32_t read(size32_t sz, void *buf)
    {
        CriticalBlock block(sect); 
        if (aborted)
            return 0;
        if (hOutput==(HANDLE)-1)
            return 0;
        size32_t sizeRead;
        for (;;) {
            {
                CriticalUnblock unblock(sect); 
                sizeRead = (size32_t)::read(hOutput, buf, sz);
            }
            if (sizeRead!=(size32_t)-1) 
                break;
            if (aborted)
                break;
            if (errno!=EINTR) {
                aborted = true;
                throw createPipeErrnoExceptionV(errno,"Pipe: read failed (size %d)", sz);
            }
        }
        return aborted?0:((size32_t)sizeRead);
    }

    ISimpleReadStream *getOutputStream()
    {
        return new CSimplePipeStream(LINK(this), false);
    }
    
    size32_t write(size32_t sz, const void *buf)
    {
        CriticalBlock block(sect); 
        CIgnoreSIGPIPE ignoresigpipe;

        if (aborted)
            return (size32_t)-1;
        if (hInput==(HANDLE)-1)
            return 0;
        size32_t sizeWritten;
        for (;;) {
            {
                CriticalUnblock unblock(sect); 
                sizeWritten = (size32_t)::write(hInput, buf, sz);
            }
            if (sizeWritten!=(size32_t)-1) 
                break;
            if (aborted) 
                break;
            if (errno!=EINTR) {
                throw createPipeErrnoExceptionV(errno, "Pipe: write failed (size %d)", sz);
            }
        }
        return aborted?((size32_t)-1):((size32_t)sizeWritten);
    }

    size32_t readError(size32_t sz, void *buf)
    {
        CriticalBlock block(sect); 
        if (stderrbufferthread) 
            return stderrbufferthread->read(sz,buf);
        if (aborted)
            return 0;
        if (hError==(HANDLE)-1)
            return 0;
        size32_t sizeRead;
        for (;;) {
            {
                CriticalUnblock unblock(sect); 
                sizeRead = (size32_t)::read(hError, buf, sz);
            }
            if (sizeRead!=(size32_t)-1) 
                break;
            if (aborted)
                break;
            if (errno!=EINTR) {
                aborted = true;
                throw createPipeErrnoExceptionV(errno, "Pipe: readError failed (size %d)", sz);
            }
        }
        return aborted?0:((size32_t)sizeRead);
    }

    ISimpleReadStream *getErrorStream()
    {
        return new CSimplePipeStream(LINK(this), true);
    }

    void notifyTerminated(HANDLE pid,unsigned _retcode)
    {
        CriticalBlock block(sect); 
        if (((int)pid>0)&&(pid==pipeProcess)) {
            retcode = _retcode;
            pipeProcess = (HANDLE)-1;
        }
    }

    unsigned wait()
    {
        bool timedout;
        return wait(INFINITE, timedout);
    }

    unsigned wait(unsigned timeoutms, bool &timedout)
    {
        timedout = false;
        if (INFINITE != timeoutms)
        {
            CriticalBlock block(sect);
            if (forkthread)
            {
                {
                    CriticalUnblock unblock(sect);
                    if (!forkthread->join(timeoutms))
                    {
                        timedout = true;
                        return retcode;
                    }
                }
            }
        }
        // NOTE - we don't clear stderrbufferthread here, since we want to be able to still read the buffered data
        clearUtilityThreads(false); // NB: will recall forkthread->join(), but doesn't matter
        if (pipeProcess != (HANDLE)-1)
        {
            if (title.length())
                PROGLOG("%s: Pipe: process %d complete %d", title.get(), pipeProcess, retcode);
            pipeProcess = (HANDLE)-1;
        }
        return retcode;
    }

    void closeOutput()
    {
        CriticalBlock block(sect);
        if (hOutput != (HANDLE)-1) {
            ::close(hOutput);
            hOutput = (HANDLE)-1;
        }
    }
    
    void closeInput()
    {
        CriticalBlock block(sect);
        if (hInput != (HANDLE)-1) {
            ::close(hInput);
            hInput = (HANDLE)-1;
        }
    }

    void closeError()
    {
        CriticalBlock block(sect);
        if (hError != (HANDLE)-1) {
            ::close(hError);
            hError = (HANDLE)-1;
        }
    }

    void abort()
    {
        CriticalBlock block(sect);
        aborted = true;
        if (pipeProcess != (HANDLE)-1) {
            if (title.length())
                PROGLOG("%s: Pipe Aborting",title.get());
            closeInput();
            if (forkthread)
            {
                CriticalUnblock unblock(sect);
                forkthread->join(1000);
            }
            if (pipeProcess != (HANDLE)-1) {
                if (title.length())
                    PROGLOG("%s: Forcibly killing pipe process %d",title.get(),pipeProcess);
                if (newProcessGroup)
                    ::kill(-pipeProcess,SIGKILL);
                else
                    ::kill(pipeProcess,SIGKILL);            // if this doesn't kill it we are in trouble
                CriticalUnblock unblock(sect);
                wait();
            }
            if (title.length())
                PROGLOG("%s: Pipe Aborted",title.get());
            retcode = -1;
            forkthread.clear();

        }
    }
    
    bool hasInput()
    {
        CriticalBlock block(sect);
        return hInput!=(HANDLE)-1;
    }
    
    bool hasOutput()
    {
        CriticalBlock block(sect);
        return hOutput!=(HANDLE)-1;
    }

    bool hasError()
    {
        CriticalBlock block(sect);
        return hError!=(HANDLE)-1;
    }

    HANDLE getProcessHandle()
    {
        CriticalBlock block(sect);
        return pipeProcess;
    }
    
    void setAllowTrace() override
    {
        allowTrace = true;
    }
};


IPipeProcess *createPipeProcess(const char *allowedprogs)
{
    return new CLinuxPipeProcess(allowedprogs);
}


#endif

// Worker thread




class CWorkQueueThread: implements IWorkQueueThread, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CriticalSection crit;
    unsigned persisttime;

    class cWorkerThread: public Thread
    {
        unsigned persisttime;
        CWorkQueueThread *parent;
        CriticalSection &crit;
    public:
        cWorkerThread(CWorkQueueThread *_parent,CriticalSection &_crit,unsigned _persisttime)
            : crit(_crit)
        {
            parent = _parent;
            persisttime = _persisttime;
        }
        QueueOf<IWorkQueueItem,false> queue;
        Semaphore sem;  

        int run()
        {
            for (;;) {
                IWorkQueueItem * work;
                bool wr = sem.wait(persisttime);
                {
                    CriticalBlock block(crit);
                    if (!wr) {
                        wr = sem.wait(0);               // catch race
                        if (!wr) 
                            break; // timed out
                    }
                    work = queue.dequeue();
                }
                if (!work)
                    break;

                try {
                    //If the thread context needs to be preserved - it should be done inside the IWorkQueueItem implementation.
                    work->execute();
                    work->Release();
                }
                catch (IException *e)
                {
                    EXCLOG(e,"CWorkQueueThread item execute");
                    e->Release();
                }
            }
            return 0;
        }
        
    };
    Owned<cWorkerThread> worker;

    CWorkQueueThread(unsigned _persisttime)
    {
        persisttime = _persisttime;
    }

    ~CWorkQueueThread()
    {
        wait();
    }

    void post(IWorkQueueItem *packet)
    {
        CriticalBlock block(crit);
        if (!worker) {
            worker.setown(new cWorkerThread(this,crit,persisttime));
            worker->start(false);
        }
        worker->queue.enqueue(packet);
        worker->sem.signal();
    }

    void wait()
    {
        Owned<cWorkerThread> wt;
        {
            CriticalBlock block(crit);
            if (worker) {
                worker->queue.enqueue(NULL);
                worker->sem.signal();
                wt.swap(worker);
            }
        }

        if (wt)
            wt->join();
    }

    unsigned pending()
    {
        CriticalBlock block(crit);
        unsigned ret = 0;
        if (worker) 
            ret = worker->queue.ordinality();
        return ret;
    }
};


IWorkQueueThread *createWorkQueueThread(unsigned persisttime)
{
    return new CWorkQueueThread(persisttime);
}

unsigned threadLogID()  // for use in logging
{
#if defined(__APPLE__)
     return pthread_mach_thread_np(pthread_self());
#elif !defined(_WIN32)
#ifdef SYS_gettid
    return (unsigned) (memsize_t) syscall(SYS_gettid);
#endif
#endif
    return (unsigned)(memsize_t) GetCurrentThreadId(); // truncated in 64bit
}

void PerfTracer::setInterval(double _interval)
{
    interval = _interval;
}

void PerfTracer::start()
{
#ifdef __linux__
    dostart(1000000);
#else
    UNIMPLEMENTED;
#endif
}

void PerfTracer::dostart(unsigned seconds)
{
#ifdef __linux__
    pipe.setown(createPipeProcess());
    pipe->setAllowTrace();
    VStringBuffer cmd("doperf %u %u %f", GetCurrentProcessId(), seconds, interval);
    if (!pipe->run(nullptr, cmd, ".", false, true, false, 1024*1024))
    {
        pipe.clear();
        throw makeStringException(0, "Failed to run doperf");
    }
#else
    UNIMPLEMENTED;
#endif
}

void PerfTracer::stop()
{
#ifdef __linux__
    assertex(pipe);
    ::kill(pipe->getProcessHandle(), SIGINT);
    dostop();
#else
    UNIMPLEMENTED;
#endif
}

void PerfTracer::traceFor(unsigned seconds)
{
#ifdef __linux__
    dostart(seconds);
    dostop();
#else
    UNIMPLEMENTED;
#endif
}

void PerfTracer::dostop()
{
#ifdef __linux__
    char buf[1024];
    while (true)
    {
        size32_t read = pipe->read(sizeof(buf), buf);
        if (!read)
            break;
        result.append(read, buf);
    }
    pipe->wait();
#else
    UNIMPLEMENTED;
#endif
}

//---------------------------------------------------------------------------------------------------------------------

//Global defaults used to initialize thread local variables
static TraceFlags defaultTraceFlags = TraceFlags::Standard;

// NOTE - extern thread_local variables are very inefficient - don't be tempted to expose the variables directly

static thread_local LogMsgJobId defaultJobId = UnknownJob;
static thread_local TraceFlags threadTraceFlags = TraceFlags::Standard;
static thread_local const IContextLogger *default_thread_logctx = nullptr;
static thread_local ISpan * threadActiveSpan = nullptr;

void saveThreadContext(SavedThreadContext & saveCtx)
{
    saveCtx.jobId = defaultJobId;
    saveCtx.logctx = default_thread_logctx;
    saveCtx.traceFlags = threadTraceFlags;
    saveCtx.activeSpan = threadActiveSpan;
}

void restoreThreadContext(const SavedThreadContext & saveCtx)
{
    // Note - as implemented the thread default job info is determined by what the global one was when the thread was created.
    // There is an alternative interpretation, that an unset thread-local one should default to whatever the global one is at the time the thread one is used.
    // In practice I doubt there's a lot of difference as global one is likely to be set once at program startup
    defaultJobId = saveCtx.jobId;
    default_thread_logctx = saveCtx.logctx;
    threadTraceFlags = saveCtx.traceFlags;
    threadActiveSpan = saveCtx.activeSpan;
}

LogMsgJobId queryThreadedJobId()
{
    return defaultJobId;
}

void setDefaultJobId(LogMsgJobId id)
{
    defaultJobId = id;
}

const IContextLogger * queryThreadedContextLogger()
{
    return default_thread_logctx;
}

ISpan * queryThreadedActiveSpan()
{
    ISpan * result = threadActiveSpan;
    if (!result)
        result = queryNullSpan();
    return result;
}

ISpan * setThreadedActiveSpan(ISpan * span)
{
    ISpan * ret = threadActiveSpan;
    threadActiveSpan = span;
    return ret;
}

//---------------------------

bool doTrace(TraceFlags featureFlag, TraceFlags level)
{
    if ((threadTraceFlags & TraceFlags::LevelMask) < level)
        return false;
    return (threadTraceFlags & featureFlag) == featureFlag;
}

void updateTraceFlags(TraceFlags flag, bool global)
{
    if (global)
        defaultTraceFlags = flag;
    threadTraceFlags = flag;
}

TraceFlags queryTraceFlags()
{
    return threadTraceFlags;
}

TraceFlags queryDefaultTraceFlags()
{
    return defaultTraceFlags;
}

//---------------------------

LogContextScope::LogContextScope(const IContextLogger *ctx)
{
    prevFlags = threadTraceFlags;
    prev = default_thread_logctx;
    default_thread_logctx = ctx;
}
LogContextScope::LogContextScope(const IContextLogger *ctx, TraceFlags traceFlags)
{
    prevFlags = threadTraceFlags;
    threadTraceFlags = traceFlags;
    prev = default_thread_logctx;
    default_thread_logctx = ctx;
}
LogContextScope::~LogContextScope()
{
    default_thread_logctx = prev;
    threadTraceFlags = prevFlags;
}
