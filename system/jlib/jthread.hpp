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



#ifndef __JTHREAD__
#define __JTHREAD__

#include "jiface.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "jhash.hpp"
#include <functional>
#include "jtrace.hpp"

#ifdef _WIN32
#define DEFAULT_THREAD_PRIORITY THREAD_PRIORITY_NORMAL
#else
// no thread priority handling?
#endif

// Functions used to reset thread-local context variables, when a threadpool starts
//NOTE: Currently activeSpan is not linked in the SavedThreadContext.
//That will not cause any problems as long as the lifetime of the thread executing with that saved span value is less than the lifetime of the span.
//It would be trivial to use a linked pointer inside the savedthread context, at the expense of a little inefficiency.
//Revisit when this feature starts being used in anger.
typedef unsigned __int64 LogMsgJobId;
interface IContextLogger;
interface ISpan;
struct jlib_decl SavedThreadContext
{
    const IContextLogger * logctx = nullptr;
    LogMsgJobId jobId = (LogMsgJobId)-1;
    TraceFlags traceFlags = queryDefaultTraceFlags();
    ISpan * activeSpan = nullptr;
};

extern void restoreThreadContext(const SavedThreadContext & saveCtx);
extern void saveThreadContext(SavedThreadContext & saveCtx);

extern jlib_decl ISpan * queryThreadedActiveSpan();
extern jlib_decl ISpan * setThreadedActiveSpan(ISpan * span);

//--------------------------------------------------------------

interface jlib_decl IThread : public IInterface
{
    virtual void start(bool inheritThreadContext) = 0;
    virtual int run() = 0;
};

interface jlib_decl IThreadName 
{
    virtual const char *get()=0;
};


extern jlib_decl bool isMainThread();
extern jlib_decl void addThreadExceptionHandler(IExceptionHandler *handler);
extern jlib_decl void removeThreadExceptionHandler(IExceptionHandler *handler);
extern jlib_decl void enableThreadSEH();
extern jlib_decl void disableThreadSEH();

extern jlib_decl unsigned threadLogID();  // for use in logging

// A function registered via addThreadTermFunc will be called when the thread that registered that function
// terminates. Such a function should call on to the previously registered function (if any) - generally you
// would expect to store that value in thread-local storage.
// This can be used to ensure that thread-specific objects can be properly destructed.
// Note that threadpools also call the thread termination hook when each thread's threadmain function terminates,
// so the hook function should clear any variables if necessary rather than assuming that they will be cleared
// at thread startup time.

typedef bool (*ThreadTermFunc)(bool isPooled);
extern jlib_decl void addThreadTermFunc(ThreadTermFunc onTerm);
extern jlib_decl void callThreadTerminationHooks(bool isPooled);

//An exception safe way of ensuring that the thread termination hooks are called.
class jlib_decl QueryTerminationCleanup
{
    bool isPooled;
public:
    inline QueryTerminationCleanup(bool _isPooled) : isPooled(_isPooled) { }
    inline ~QueryTerminationCleanup() { callThreadTerminationHooks(isPooled); }
};

class jlib_decl Thread : public CInterface, public IThread
{
friend class CThreadedPersistent;
private:
    ThreadId threadid;
    unsigned short stacksize; // in 4K blocks
    int prioritydelta;
    int nicelevel;

    bool alive;
    unsigned tidlog;
#ifdef _WIN32
    HANDLE hThread;
    static unsigned WINAPI _threadmain(LPVOID v);
#else
    static void *_threadmain(void *v);
#endif
    virtual int begin();
    void init(const char *name);
    void handleException(IException *e);
    void adjustNiceLevel();

protected:
    StringAttr cthreadname;
    SavedThreadContext savedCtx;
public:
#ifndef _WIN32
    Semaphore suspend;
    Semaphore starting;
#endif
    Semaphore stopped;

    IMPLEMENT_IINTERFACE;

    Thread(const char *_name) { init(_name); }
    Thread() { init(NULL); }
    ~Thread();

    void adjustPriority(int delta);
    bool isCurrentThread() const;
    void setNice(int nicelevel);
    void setStackSize(size32_t size);               // required stack size in bytes - called before start() (obviously)
    const char *getName() { return cthreadname.isEmpty() ? "unknown" : cthreadname.str(); }
    bool isAlive() { return alive; }
    bool join(unsigned timeout=INFINITE);
    void captureThreadLoggingInfo();                // Capture current thread logging context to be used by this thread when started

    virtual void start(bool inheritThreadContext);
    virtual void startRelease();        

    StringBuffer &getInfo(StringBuffer &str);
    const char *getLogInfo(int &thandle,unsigned &tid) { 
#ifdef _WIN32
        thandle = (int)(memsize_t)hThread;
#elif defined __FreeBSD__ || defined __APPLE__
        thandle = (int)(memsize_t)threadid;
#else
        thandle = (int)threadid;
#endif
        tid = tidlog;
        return getName(); 
    }

    // run method not implemented - concrete derived classes must do so
    static  void setDefaultStackSize(size32_t size);    // NB under windows requires linker setting (/stack:)
};

interface IThreaded
{
    virtual void threadmain() = 0;
protected:
    virtual ~IThreaded() {}
};

// utility class, useful for containing a thread
class CThreaded : public Thread
{
    IThreaded *owner;
public:
    inline CThreaded(const char *name, IThreaded *_owner) : Thread(name), owner(_owner) { }
    inline CThreaded(const char *name) : Thread(name) { owner = NULL; }
    inline void init(IThreaded *_owner, bool inheritThreadContext) { owner = _owner; start(inheritThreadContext); }
    virtual int run() { owner->threadmain(); return 1; }
};

// Similar to above, but the underlying thread always remains running. This can make repeated start + join's significantly quicker
class jlib_decl CThreadedPersistent
{
    class CAThread : public Thread
    {
        CThreadedPersistent &owner;
    public:
        CAThread(CThreadedPersistent &_owner, const char *name) : Thread(name), owner(_owner) { }
        virtual int run() { owner.threadmain(); return 1; }
    } athread;
    Owned<IException> exception;
    IThreaded *owner;
    Semaphore sem, joinSem;
    std::atomic_uint state;
    bool halt;
    enum ThreadStates { s_ready, s_running, s_joining };
    void threadmain();
public:
    CThreadedPersistent(const char *name, IThreaded *_owner);
    ~CThreadedPersistent();
    void start(bool inheritThreadContext);
    bool join(unsigned timeout, bool throwException = true);
};


// Asynchronous 'for' utility class
// see HRPCUTIL.CPP for example of usage
interface ITaskScheduler;
class jlib_decl CAsyncFor
{
public:
    void For(unsigned num,unsigned maxatonce,bool abortFollowingException=false,bool shuffled=false);
    void TaskFor(unsigned num, ITaskScheduler & scheduler);
    virtual void Do(unsigned idx=0)=0;
};

template <typename AsyncFunc>
class CAsyncForFunc final : public CAsyncFor
{
public:
    CAsyncForFunc(AsyncFunc _func) : func(_func) {}
    virtual void Do(unsigned idx=0) { func(idx); }
private:
    AsyncFunc func;
};

//Utility functions for executing a lambda function in parallel, but allow the number of concurrent iterations
//action on exception, and shuffling to be controlled.
template <typename AsyncFunc>
inline void asyncFor(unsigned num, unsigned maxAtOnce, bool abortFollowingException, bool shuffled, AsyncFunc func)
{
    CAsyncForFunc<AsyncFunc> async(func);
    async.For(num, maxAtOnce, abortFollowingException, shuffled);
}
template <typename AsyncFunc>
inline void asyncFor(unsigned num, unsigned maxAtOnce, bool abortFollowingException, AsyncFunc func)
{
    asyncFor(num, maxAtOnce, abortFollowingException, false, func);
}
template <typename AsyncFunc>
inline void asyncFor(unsigned num, unsigned maxAtOnce, AsyncFunc func)
{
    asyncFor(num, maxAtOnce, false, false, func);
}
template <typename AsyncFunc>
inline void asyncFor(unsigned num, AsyncFunc func)
{
    asyncFor(num, num, false, false, func);
}

// ---------------------------------------------------------------------------
// Thread Pools
// ---------------------------------------------------------------------------

interface IPooledThread: extends IInterface                 // base class for deriving pooled thread (alternative to Thread)
{
public:
    virtual void init(void *param) = 0;                    // called before threadmain started (from within start)
    virtual void threadmain() = 0;                         // where threads code goes (param is passed from start)
    virtual bool stop() = 0;                               // called to cause threadmain to return, returns false if request rejected
    virtual bool canReuse() const = 0;                     // return true if object can be re-used (after stopped), otherwise released
};

interface IThreadFactory: extends IInterface                // factory for creating new pooled instances (called when pool empty)
{
       virtual IPooledThread *createNew()=0;
};

typedef IIteratorOf<IPooledThread> IPooledThreadIterator;
typedef unsigned PooledThreadHandle;

interface IThreadPool : extends IInterface
{
        virtual PooledThreadHandle start(void *param)=0;    // starts a new thread reuses stopped pool entries
        virtual PooledThreadHandle start(void *param,const char *name)=0;   // starts a new thread reuses stopped pool entries 
        virtual PooledThreadHandle start(void *param,const char *name,unsigned timeout)=0;  // starts a new thread reuses stopped pool entries, throws exception if can't start within timeout 
        virtual bool stop(PooledThreadHandle handle)=0;     // initiates stop on specified thread (may return false)
        virtual bool stopAll(bool tryall=false)=0;          // initiates stop on all threads, if tryall continues even if one or more fails
        virtual bool join(PooledThreadHandle handle,unsigned timeout=INFINITE)=0;
                                                            // waits for a single thread to terminate
        virtual bool joinAll(bool del=true,unsigned timeout=INFINITE)=0;    // waits for all threads in thread pool to terminate
                                                                            // if del true frees all pooled threads
        virtual IPooledThreadIterator *running()=0;                 // return an iterator for all currently running threads
        virtual unsigned runningCount()=0;                  // number of currently running threads
        virtual PooledThreadHandle startNoBlock(void *param)=0; // starts a new thread if it can do so without blocking, else throws exception
        virtual void setStartDelayTracing(unsigned secs) = 0;        // set start delay tracing period
        virtual void setNiceValue(int value) = 0;                    // set priority for thread
        virtual bool waitAvailable(unsigned timeout) = 0;            // wait until a pool member is available
};

extern jlib_decl IThreadPool *createThreadPool(
                                const char *poolname,       // trace name of pool
                                IThreadFactory *factory,    // factory for creating new thread instances
                                bool inheritThreadContext,  // Are threads run independent of the calling context(false), or within the calling context(true)
                                IExceptionHandler *exceptionHandler, // optional exception handler
                                unsigned defaultmax=50,     // maximum number of threads before starts blocking
                                unsigned delay=1000,        // maximum delay on each block
                                unsigned stacksize=0,       // stack size (bytes) 0 is default
                                unsigned timeoutOnRelease=INFINITE, // maximum time waited for thread to terminate on releasing pool
                                unsigned targetpoolsize=0           // target maximum size of pool (default same as defaultmax)   
                             );

extern jlib_decl unsigned getThreadCount();

// Simple pipe process support
interface ISimpleReadStream;

#define START_FAILURE (199) // return code if program cannot be started

interface IPipeProcessException : extends IException
{
};

extern jlib_decl IPipeProcessException *createPipeErrnoException(int code, const char *msg);
extern jlib_decl IPipeProcessException *createPipeErrnoExceptionV(int code, const char *msg, ...) __attribute__((format(printf, 2, 3)));

interface IPipeProcess: extends IInterface
{
    virtual bool run(const char *title,const char *prog, const char *dir,
                       bool hasinput,bool hasoutput,bool haserror=false,
                       size32_t stderrbufsize=0,                      // set to non-zero to automatically buffer stderror output
                       bool newProcessGroup=false) __attribute__ ((warn_unused_result)) = 0;
    virtual bool hasInput() = 0;                                    // i.e. can write to pipe
    virtual size32_t write(size32_t sz, const void *buffer) = 0;    // write pipe process standard output
    virtual bool hasOutput() = 0;                                   // i.e. can read from pipe
    virtual size32_t read(size32_t sz, void *buffer) = 0;           // read from pipe process standard output
    virtual ISimpleReadStream *getOutputStream() = 0;               // read from pipe process standard output
    virtual bool hasError() = 0;                                    // i.e. can read from pipe stderr
    virtual size32_t readError(size32_t sz, void *buffer) = 0;      // read from pipe process standard error
    virtual ISimpleReadStream *getErrorStream() = 0;                // read from pipe process standard error

    virtual unsigned wait() = 0;                                    // returns return code
    virtual unsigned wait(unsigned timeoutms, bool &timedout) = 0;  // sets timedout to true if times out
    virtual void closeInput() = 0;                                  // indicate finished input to pipe
    virtual void closeOutput() = 0;                                 // indicate finished reading from pipe (generally called automatically)
    virtual void closeError() = 0;                                  // indicate finished reading from pipe stderr
    virtual void abort() = 0;
    virtual void notifyTerminated(HANDLE pid,unsigned retcode) = 0; // internal
    virtual HANDLE getProcessHandle() = 0;                          // used to auto kill
    virtual void setenv(const char *var, const char *value) = 0;  // Set a value to be passed in the called process environment
    virtual void setAllowTrace() = 0;                               // Allow child process to trace me
};

extern jlib_decl IPipeProcess *createPipeProcess(const char *allowedprograms=NULL);

//--------------------------------------------------------

class jlib_decl PerfTracer
{
    Owned<IPipeProcess> pipe;
    double interval = 0.2;
    StringBuffer result;
public:
    void start();
    void stop();
    void traceFor(unsigned seconds);
    StringBuffer &queryResult() { return result; }
    void setInterval(double _interval);  // In seconds
private:
    void dostart(unsigned seconds);
    void dostop();
};

//--------------------------------------------------------

interface IWorkQueueItem: extends IInterface
{
    virtual void execute()=0;
};

interface IWorkQueueThread: extends IInterface
{
    virtual void post(IWorkQueueItem *item)=0;  // takes ownership of item
    virtual void wait()=0;
    virtual unsigned pending()=0;
};

// Simple lightweight async worker queue
// internally thread persists for specified time waiting before self destroying
extern jlib_decl IWorkQueueThread *createWorkQueueThread(unsigned persisttime=1000*60);

//--- Functions that manage the current thread state =- for context, tracing, spans etc.

extern jlib_decl LogMsgJobId queryThreadedJobId();
extern jlib_decl void setDefaultJobId(LogMsgJobId id);

extern const IContextLogger * queryThreadedContextLogger();

// Temporarily modify the trace context and/or flags for the current thread, for the lifetime of the LogContextScope object
class jlib_decl LogContextScope
{
public:
    LogContextScope(const IContextLogger *ctx);
    LogContextScope(const IContextLogger *ctx, TraceFlags traceFlags);
    ~LogContextScope();

    const IContextLogger *prev;
    TraceFlags prevFlags;
};


#endif
