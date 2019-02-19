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

#define LINUX_STACKSIZE_CAP (0x200000)

//#define NO_CATCHALL

static __thread ThreadTermFunc threadTerminationHook;

ThreadTermFunc addThreadTermFunc(ThreadTermFunc onTerm)
{
    ThreadTermFunc old = threadTerminationHook;
    threadTerminationHook = onTerm;
    return old;
}

void callThreadTerminationHooks()
{
    if (threadTerminationHook)
    {
        (*threadTerminationHook)();
        threadTerminationHook = NULL;
    }
}

PointerArray *exceptionHandlers = NULL;
MODULE_INIT(INIT_PRIORITY_JTHREAD)
{
    if (threadTerminationHook)
        (*threadTerminationHook)();  // May be too late :(
    exceptionHandlers = new PointerArray();
    return true;
}
MODULE_EXIT()
{
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


static ICopyArrayOf<Thread> ThreadList;
static CriticalSection ThreadListSem;
static size32_t defaultThreadStackSize=0;
static ICopyArrayOf<Thread> ThreadDestroyList;
static SpinLock ThreadDestroyListLock;




#ifdef _WIN32
extern void *EnableSEHtranslation();

unsigned WINAPI Thread::_threadmain(LPVOID v)
#else
void *Thread::_threadmain(void *v)
#endif
{
    Thread * t = (Thread *)v;
#ifdef _WIN32
    if (SEHHandling) 
        EnableSEHtranslation();
#else
    t->tidlog = threadLogID();
#endif
    int ret = t->begin();
    char *&threadname = t->cthreadname.threadname;
    if (threadname) {
        memsize_t l=strlen(threadname);
        char *newname = (char *)malloc(l+8+1);
        memcpy(newname,"Stopped ",8);
        memcpy(newname+8,threadname,l+1);
        char *oldname = threadname;
        threadname = newname;
        free(oldname);
    }
    {
        // need to ensure joining thread does not race with us to release
        t->Link();  // extra safety link
        {
            SpinBlock block(ThreadDestroyListLock);
            ThreadDestroyList.append(*t);
        }
        try {
            t->stopped.signal();        
            if (t->Release()) {
                PROGLOG("extra unlinked thread");
                PrintStackReport();
            }
            else
                t->Release(); 
        }
        catch (...) {
            PROGLOG("thread release exception");
            throw;
        }
        {
            SpinBlock block(ThreadDestroyListLock);
            ThreadDestroyList.zap(*t);  // hopefully won't get too big (i.e. one entry!)
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
    callThreadTerminationHooks();
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
    cthreadname.threadname = (NULL == _name) ? NULL : strdup(_name);
    ithreadname = &cthreadname;
    prioritydelta = 0;
    nicelevel = 0;
    stacksize = 0; // default is EXE default stack size  (set by /STACK)
}

void Thread::start()
{
    if (alive) {
        WARNLOG("Thread::start(%s) - Thread already started!",getName());
        PrintStackReport();
#ifdef _DEBUG
        throw MakeStringException(-1,"Thread::start(%s) - Thread already started!",getName());
#endif
        return;
    }
    Link();
    startRelease();
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
        pthread_attr_init(&attr);
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
        status = pthread_create(&threadid, &attr, Thread::_threadmain, this);
        if ((status==EAGAIN)||(status==EINTR)) {
            if (numretrys--==0)
                break;
            WARNLOG("pthread_create(%d): Out of threads, retrying...",status);
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
        StringBuffer s;
        getThreadList(s);
        IERRLOG("Running threads:\n %s",s.str());
        throw makeOsException(status);
    }
    unsigned retryCount = 10;
    for (;;)
    {
        if (starting.wait(1000*10))
            break;
        else if (0 == --retryCount)
            throw MakeStringException(-1, "Thread::start(%s) failed", getName());
        WARNLOG("Thread::start(%s) stalled, waiting to start, retrying", getName());
    }
#endif
    alive = true;
    if (prioritydelta)
        adjustPriority(prioritydelta);

    {
        CriticalBlock block(ThreadListSem);
        ThreadList.zap(*this);  // just in case restarting
        ThreadList.append(*this);
    }
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
    unsigned st = 0;
    for (;;) {                                              // this is to prevent race with destroy
                                                        // (because Thread objects are not always link counted!)
        {
            SpinBlock block(ThreadDestroyListLock);
            if (ThreadDestroyList.find(*this)==NotFound)
                break;
        }
#ifdef _DEBUG
        if (st==10)     
            PROGLOG("Thread::join race");   
#endif
        Sleep(st);      // switch back to exiting thread (not very elegant!)
        st++;
        if (st>10)
            st = 10; // note must be non-zero for high priority threads
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
    ithreadname = &cthreadname; // safer (as derived classes destroyed)
#ifdef _DEBUG
    if (alive) {
        if (!stopped.wait(0)) { // see if fell out of threadmain and signal stopped
            PROGLOG("Live thread killed! %s",getName());
            PrintStackReport();
        }
        // don't need to resignal as we are on way out
    }
#endif
    Link();
    
//  DBGLOG("Thread %x (%s) destroyed\n", threadid, threadname);
    {
        CriticalBlock block(ThreadListSem);
        ThreadList.zap(*this);
    }
    free(cthreadname.threadname);
    cthreadname.threadname = NULL;
}

unsigned getThreadCount()
{
    CriticalBlock block(ThreadListSem);
    return ThreadList.ordinality();
}

StringBuffer & getThreadList(StringBuffer &str)
{
    CriticalBlock block(ThreadListSem);
    ForEachItemIn(i,ThreadList) {
        Thread &item=ThreadList.item(i);
        item.getInfo(str).append("\n");
    }
    return str;
}

StringBuffer &getThreadName(int thandle,unsigned tid,StringBuffer &name)
{
    CriticalBlock block(ThreadListSem);
    bool found=false;
    ForEachItemIn(i,ThreadList) {
        Thread &item=ThreadList.item(i);
        int h; 
        unsigned t;
        const char *s = item.getLogInfo(h,t);
        if (s&&*s&&((thandle==0)||(h==thandle))&&((tid==0)||(t==tid))) {
            if (found) {
                name.clear();
                break;  // only return if unambiguous
            }
            name.append(s);
            found = true;
        }
    }
    return name;
}


// CThreadedPersistent

CThreadedPersistent::CThreadedPersistent(const char *name, IThreaded *_owner) : athread(*this, name), owner(_owner), state(s_ready)
{
    halt = false;
    athread.start();
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

void CThreadedPersistent::start()
{
    unsigned expected = s_ready;
    if (!state.compare_exchange_strong(expected, s_running))
    {
        VStringBuffer msg("CThreadedPersistent::start(%s) - not ready", athread.getName());
        WARNLOG("%s", msg.str());
        PrintStackReport();
        throw MakeStringExceptionDirect(-1, msg.str());
    }
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
    Mutex errmutex;
    IException *e=NULL;
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
                if (e)
                    _e->Release();  // only return first
                else
                    e = _e;
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
                Mutex *errmutex;
                Semaphore &ready;
                IException *&erre;
                unsigned idx;
                CAsyncFor *self;
                cdothread(CAsyncFor *_self,unsigned _idx,Semaphore &_ready,Mutex *_errmutex,IException *&_e)
                    : Thread("CAsyncFor"),ready(_ready),erre(_e)
                {
                    errmutex =_errmutex;
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
                        synchronized block(*errmutex);
                        if (erre)
                            _e->Release();  // only return first
                        else
                            erre = _e;
                    }
    #ifndef NO_CATCHALL
                    catch (...)
                    {
                        synchronized block(*errmutex);
                        if (!erre)
                            erre = MakeStringException(0, "Unknown exception in Thread %s", getName());
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
            started.ensure(num);
            for (i=0;i<num;i++) {
                ready.wait();
                if (abortFollowingException && e) break;
                Owned<Thread> thread = new cdothread(this,shuffled?shuffler->lookup(i):i,ready,&errmutex,e);
                thread->start();
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
                Mutex *errmutex;
                IException *&erre;
                unsigned idx;
                CAsyncFor *self;
                cdothread(CAsyncFor *_self,unsigned _idx,Mutex *_errmutex,IException *&_e)
                    : Thread("CAsyncFor"),erre(_e)
                {
                    errmutex =_errmutex;
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
                        synchronized block(*errmutex);
                        if (erre)
                            _e->Release();  // only return first
                        else
                            erre = _e;
                    }
    #ifndef NO_CATCHALL
                    catch (...)
                    {
                        synchronized block(*errmutex);
                        if (!erre)
                            erre = MakeStringException(0, "Unknown exception in Thread %s", getName());
                    }
    #endif
                    return 0;
                }
            };
            IArrayOf<Thread> started;
            started.ensure(num);
            for (i=0;i<num-1;i++)
            {
                Owned<Thread> thread = new cdothread(this,i,&errmutex,e);
                thread->start();
                started.append(*thread.getClear());
            }
            Do(num-1);
            ForEachItemIn(idx, started)
            {
                started.item(idx).join();
            }
        }
    }
    if (e)
        throw e;
}

//---------------------------------------------------------------------------------------------------------------------

class CSimpleFunctionThread : public Thread
{
    std::function<void()> func;
public:
    inline CSimpleFunctionThread(std::function<void()> _func) : Thread("TaskProcessor"), func(_func) { }
    virtual int run()
    {
        func();
        return 1;
    }
};

void asyncStart(IThreaded & threaded)
{
    CThreaded * thread = new CThreaded("AsyncStart", &threaded);
    thread->startRelease();
}

void asyncStart(const char * name, IThreaded & threaded)
{
    CThreaded * thread = new CThreaded(name, &threaded);
    thread->startRelease();
}

//Experimental - is this a useful function to replace some uses of IThreaded?
void asyncStart(std::function<void()> func)
{
    (new CSimpleFunctionThread(func))->startRelease();
}

// ---------------------------------------------------------------------------
// Thread Pools
// ---------------------------------------------------------------------------


class CPooledThreadWrapper;


class CThreadPoolBase
{
public:
    virtual ~CThreadPoolBase() {}
protected: friend class CPooledThreadWrapper;
    IExceptionHandler *exceptionHandler;
    CriticalSection crit;
    StringAttr poolname;
    int donewaiting;
    Semaphore donesem;
    PointerArray waitingsems;
    UnsignedArray waitingids;
    bool stopall;
    unsigned defaultmax;
    unsigned targetpoolsize;
    unsigned delay;
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
    char *runningname;
public:
    CPooledThreadWrapper(CThreadPoolBase &_parent,
                         PooledThreadHandle _handle,
                         IPooledThread *_thread) // takes ownership of thread
        : Thread(StringBuffer("Member of thread pool: ").append(_parent.poolname).str()), parent(_parent)
    {
        thread = _thread;
        handle = _handle;
        runningname = strdup(_parent.poolname); 
    }

    ~CPooledThreadWrapper()
    {
        thread->Release();
        free(runningname);
    }

    void setName(const char *name) { free(runningname); runningname=strdup(name); }
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
            parent.notifyStarted(this);
            try
            {
                char *&threadname = cthreadname.threadname;
                char *temp = threadname;    // swap running name and threadname
                threadname = runningname;
                runningname = temp;
                thread->threadmain();
                temp = threadname;  // and back
                threadname = runningname;
                runningname = temp;
            }
            catch (IException *e)
            {
                char *&threadname = cthreadname.threadname;
                char *temp = threadname;    // swap back
                threadname = runningname;
                runningname = temp;
                handleException(e);
            }
#ifndef NO_CATCHALL
            catch (...)
            {
                char *&threadname = cthreadname.threadname;
                char *temp = threadname;    // swap back
                threadname = runningname;
                runningname = temp;
                handleException(MakeStringException(0, "Unknown exception in Thread from pool %s", parent.poolname.get()));
            }
#endif
            callThreadTerminationHooks();    // Reset any pre-thread state.
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
    CIArrayOf<CPooledThreadWrapper> threadwrappers;
    PooledThreadHandle nextid;
    IThreadFactory *factory;
    unsigned stacksize;
    unsigned timeoutOnRelease;
    unsigned traceStartDelayPeriod;
    unsigned startsInPeriod;
    cycle_t startDelayInPeriod;
    CCycleTimer overAllTimer;

    PooledThreadHandle _start(void *param,const char *name, bool noBlock, unsigned timeout=0)
    {
        CCycleTimer startTimer;
        bool timedout = defaultmax && !availsem.wait(noBlock ? 0 : (timeout>0?timeout:delay));
        PooledThreadHandle ret;
        {
            CriticalBlock block(crit);
            if (timedout)
            {
                if (!availsem.wait(0)) {  // make sure take allocated sem if has become available
                    if (noBlock || timeout > 0)
                        throw MakeStringException(0, "No threads available in pool %s", poolname.get());
                    WARNLOG("Pool limit exceeded for %s", poolname.get());
                }
                else
                    timedout = false;
            }
            if (traceStartDelayPeriod)
            {
                ++startsInPeriod;
                if (timedout)
                {
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
            }
            CPooledThreadWrapper &t = allocThread();
            if (name)
                t.setName(name);
            t.go(param);
            ret = t.queryHandle();
        }
        Sleep(0);
        return ret;
    }

public:
    IMPLEMENT_IINTERFACE;
    CThreadPool(IThreadFactory *_factory,IExceptionHandler *_exceptionHandler,const char *_poolname,unsigned _defaultmax, unsigned _delay, unsigned _stacksize, unsigned _timeoutOnRelease, unsigned _targetpoolsize)
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
        traceStartDelayPeriod = 0;
        startsInPeriod = 0;
        startDelayInPeriod = 0;
    }

    ~CThreadPool()
    {
        stopAll(true);
        if (!joinAll(true, timeoutOnRelease))
            WARNLOG("%s; timedout[%d] waiting for threads in pool", poolname.get(), timeoutOnRelease);
        CriticalBlock block(crit);
        bool first=true;
        ForEachItemIn(i,threadwrappers)
        {
            CPooledThreadWrapper &t = threadwrappers.item(i);
            if (!t.isStopped())
            {
                if (first)
                {
                    WARNLOG("Threads still active: ");
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
        ret.start();
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

    PooledThreadHandle startNoBlock(void *param,const char *name)
    {
        return _start(param, name, true);
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
        CIArrayOf<CPooledThreadWrapper> tojoin;
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
};


IThreadPool *createThreadPool(const char *poolname,IThreadFactory *factory,IExceptionHandler *exceptionHandler,unsigned defaultmax, unsigned delay, unsigned stacksize, unsigned timeoutOnRelease, unsigned targetpoolsize)
{
    return new CThreadPool(factory,exceptionHandler,poolname,defaultmax,delay,stacksize,timeoutOnRelease,targetpoolsize);
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
    OERRLOG("Unauthorized pipe program(%s)",head.str());
    throw MakeStringException(-1,"Unauthorized pipe program(%s)",head.str());
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
    StringArray envVars;
    StringArray envValues;

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

        // MORE - should create a new environment block that is copy of parent's, then set all the values in envVars/envValues, and pass it

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
        envVars.append(var);
        envValues.append(value);
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
        return aborted?((size32_t)-1):((size32_t)sizeRead);
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
        return aborted?((size32_t)-1):((size32_t)sizeRead);
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
                            WARNLOG("Lost %d bytes of stderr output",totsz);
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
    StringArray envVars;
    StringArray envValues;

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
        if (hasinput)
            if (::pipe(inpipe)==-1)
                throw makeOsException(errno);
        if (hasoutput)
            if (::pipe(outpipe)==-1)
                throw makeOsException(errno);
        if (haserror)
            if (::pipe(errpipe)==-1)
                throw makeOsException(errno);

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
        if (pipeProcess==0) { // child
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

            if (dir.get()) {
                if (chdir(dir) == -1)
                    throw MakeStringException(-1, "CLinuxPipeProcess::run: could not change dir to %s", dir.get());
            }
            ForEachItemIn(idx, envVars)
            {
                ::setenv(envVars.item(idx), envValues.item(idx), 1);
            }
            execvp(argv[0],argv);
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
        if (_title) {
            title.set(_title);
            PROGLOG("%s: Creating PIPE program process : '%s' - hasinput=%d, hasoutput=%d stderrbufsize=%d", title.get(), prog.get(),(int)hasinput, (int)hasoutput, stderrbufsize);
        }
        CheckAllowedProgram(prog,allowedprogs);
        retcode = 0;
        if (forkthread) {
            {
                CriticalUnblock unblock(sect); 
                forkthread->join();
            }
            forkthread.clear();
        }
        forkthread.setown(new cForkThread(this));
        forkthread->start();
        {
            CriticalUnblock unblock(sect); 
            started.wait();
            forkthread->join(50); // give a chance to fail
        }
        if (retcode==START_FAILURE) {   
            DBGLOG("%s: PIPE process '%s' failed to start", title.get()?title.get():"CLinuxPipeProcess", prog.get());
            forkthread.clear();
            return false;
        }
        if (stderrbufsize) {
            if (stderrbufferthread) {
                stderrbufferthread->stop();
                delete stderrbufferthread;
            }
            stderrbufferthread = new cStdErrorBufferThread(stderrbufsize,hError,sect);
            stderrbufferthread->start();
        }
        return true;
    }

    virtual void setenv(const char *var, const char *value)
    {
        assertex(var);
        if (!value)
            value = "";
        envVars.append(var);
        envValues.append(value);
    }
    
    size32_t read(size32_t sz, void *buf)
    {
        CriticalBlock block(sect); 
        if (aborted)
            return (size32_t)-1;
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
                throw makeErrnoExceptionV(errno,"Pipe: read failed (size %d)", sz);
            }
        }
        return aborted?((size32_t)-1):((size32_t)sizeRead);
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
                throw makeErrnoExceptionV(errno, "Pipe: write failed (size %d)", sz);
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
            return (size32_t)-1;
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
                throw makeErrnoExceptionV(errno, "Pipe: readError failed (size %d)", sz);
            }
        }
        return aborted?((size32_t)-1):((size32_t)sizeRead);
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
        if (pipeProcess != (HANDLE)-1) {
            if (title.length())
                PROGLOG("%s: Pipe Aborting",title.get());
            aborted = true;
            closeInput();
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
                    work->execute();
                    work->Release();
                }
                catch (IException *e)
                {
                    EXCLOG(e,"CWorkQueueThread item execute");
                    e->Release();
                }
            }
            CriticalBlock block(crit);
            parent->worker=NULL;    // this should be safe
            return 0;
        }
        
    } *worker;

    CWorkQueueThread(unsigned _persisttime)
    {
        persisttime = _persisttime;
        worker = NULL;
    }

    ~CWorkQueueThread()
    {
        wait();
    }

    void post(IWorkQueueItem *packet)
    {
        CriticalBlock block(crit);
        if (!worker) {
            worker = new cWorkerThread(this,crit,persisttime);
            worker->startRelease();
        }
        worker->queue.enqueue(packet);
        worker->sem.signal();
    }

    void wait()
    {
        CriticalBlock block(crit);
        if (worker) {
            worker->queue.enqueue(NULL);
            worker->sem.signal();
            Linked<cWorkerThread> wt;
            wt.set(worker);
            CriticalUnblock unblock(crit);
            wt->join();
        }
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
