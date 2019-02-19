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


//#define MINIMALTRACE // a more readable version of the trace file (IMO)
//#define LOGCLOCK

#include "platform.h"
#include "stdio.h"
#include <time.h>
#include "jmisc.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "jstring.hpp"
#include "jdebug.hpp"

#ifdef _WIN32
#include <direct.h>
#else
#include <sys/wait.h>
#include <pwd.h>
#endif

#ifdef LOGCLOCK
#define MSGFIELD_PRINTLOG MSGFIELD_timeDate | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code | MSGFIELD_milliTime
#else
#define MSGFIELD_PRINTLOG MSGFIELD_timeDate | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code
#endif

#define FPRINTF(s) { fprintf(stdlog, "%s", s); \
                      if (logFile) fprintf(logFile, "%s", s); }

void _rev(size32_t len, void * _ptr)
{ 
    byte * ptr = (byte *)_ptr;
    while (len > 1) 
    { 
        byte t = *ptr; 
        *ptr = ptr[--len]; 
        ptr[len] = t; 
        len--; 
        ptr++; 
    }
}

Mutex printMutex;
FILE *logFile;
FILE *stdlog = stderr;
HiresTimer logTimer;
class CStdLogIntercept: public ILogIntercept
{
    bool nl;
#ifdef MINIMALTRACE
    time_t lastt;
    unsigned lasttpid;
#endif
    unsigned detail;

public:
    CStdLogIntercept()
    {
#ifdef MINIMALTRACE
        lasttpid = 0;
        lastt = 0;
#endif
        nl = true;
        detail = _LOG_TIME | _LOG_PID | _LOG_TID;
#ifdef LOGCLOCK
        detail |= _LOG_CLOCK;
#endif
        queryStderrLogMsgHandler()->setMessageFields(detail);
    }
    unsigned setLogDetail(unsigned i)
    {
        unsigned ret = detail;
        detail = i;
        return ret;
    }
    void print(const char *str)
    {
        if (nl) {
            char timeStamp[32];
            time_t tNow;
            time(&tNow);
#ifdef _WIN32
            struct tm ltNow = *localtime(&tNow);
#else
            struct tm ltNow;
            localtime_r(&tNow, &ltNow);
#endif
#ifdef MINIMALTRACE
            unsigned tpid = GetCurrentThreadId();
            if (((detail & _LOG_TID) && tpid!=lasttpid)||((detail & _LOG_TIME) && memcmp(&tNow,&lastt,sizeof(tNow))!=0))
            {
                lasttpid = tpid;
                lastt = tNow;
                FPRINTF("[");
                if (detail & _LOG_TIME)
                {
                    strftime(timeStamp, 32, "%H:%M:%S ", &ltNow);
                    FPRINTF(timeStamp);
                }
                if (detail & _LOG_TID)
                {
                    fprintf(stdlog, "TID=%d ", tpid);
                    if (logFile)
                        fprintf(logFile, "TID=%d ", tpid);
                }
                FPRINTF("]\n");
            }
#else
            if (detail & _LOG_TIME)
            {
                strftime(timeStamp, 32, "%m/%d/%y %H:%M:%S ", &ltNow);
                FPRINTF(timeStamp);
            }
            if (detail & _LOG_CLOCK)
            {
                unsigned t=msTick();
                fprintf(stdlog, "%u ", t);
                if (logFile)
                    fprintf(logFile, "%u ", t);
            }
            if (detail & _LOG_HIRESCLOCK)
            {
                unsigned clock = usTick();
                fprintf(stdlog, " TICK=%u ", clock);
                if (logFile)
                    fprintf(logFile, " TICK=%u ", clock);
            }
#if defined(_WIN32) 
            if (detail & _LOG_TID)
            {
                fprintf(stdlog, "TID=%d ", GetCurrentThreadId());
                if (logFile)
                    fprintf(logFile, "TID=%d ", GetCurrentThreadId());
            }
            if (detail & _LOG_PID)
            {
                fprintf(stdlog, "PID=%d ", GetCurrentProcessId());
                if (logFile)
                    fprintf(logFile, "PID=%d ", GetCurrentProcessId());
            }
#else
            if (detail & _LOG_PID)
            {
                fprintf(stdlog, "PID=%d ", getpid());
                if (logFile)
                    fprintf(logFile, "PID=%d ", getpid());
            }
#endif
            if (detail & (_LOG_PID|_LOG_TID|_LOG_TIME))
            {
                fprintf(stdlog, "- ");
                if (logFile)
                    fprintf(logFile, "- ");
            }
#endif
        }
        if (str && *str)
        {
            nl = str[strlen(str)-1]=='\n';
            FPRINTF(str);
        }
        else
            nl = false;
        fflush(stdlog);
        if (logFile)
        {
            fflush(logFile);
        }
    }
    void close()
    {
        if (logFile) {
            fclose(logFile);
            logFile = 0;
        }
    }

};

static ILogIntercept *logintercept = NULL;

jlib_decl ILogIntercept* interceptLog(ILogIntercept *intercept)
{
    ILogIntercept *old=logintercept;
    logintercept = intercept;
    return old;
}

jlib_decl void openLogFile(StringBuffer & resolvedFS, const char *filename, unsigned detail, bool enterQueueMode, bool append)
{
    if(enterQueueMode)
        queryLogMsgManager()->enterQueueingMode();
    Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(".", 0);
    lf->setCreateAliasFile(false);
    lf->setLocal(true);
    lf->setRolling(false);
    lf->setAppend(append);
    lf->setCompleteFilespec(filename);//user specified log filespec
    lf->setMaxDetail(detail ? detail : DefaultDetail);
    lf->setMsgFields(MSGFIELD_timeDate | MSGFIELD_msgID | MSGFIELD_process | MSGFIELD_thread | MSGFIELD_code);
    lf->beginLogging();
    resolvedFS.set(lf->queryLogFileSpec());
}

jlib_decl void PrintLogDirect(const char *msg)
{
    LOG(MClegacy, unknownJob, "%s", msg);
}

jlib_decl int PrintLog(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    VALOG(MClegacy, unknownJob, fmt, args);
    va_end(args);
    return 0;
}

jlib_decl void SPrintLog(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    VALOG(MClegacy, unknownJob, fmt, args);
    va_end(args);
}

StringBuffer &addFileTimestamp(StringBuffer &fname, bool daily)
{
    time_t tNow;
    time(&tNow);
    char timeStamp[32];

#ifdef _WIN32
    struct tm *ltNow;
    ltNow = localtime(&tNow);
    strftime(timeStamp, 32, daily ? ".%Y_%m_%d" : ".%Y_%m_%d_%H_%M_%S", ltNow);
#else
    struct tm ltNow;
    localtime_r(&tNow, &ltNow);
    strftime(timeStamp, 32, daily ? ".%Y_%m_%d" : ".%Y_%m_%d_%H_%M_%S", &ltNow);
#endif
    return fname.append(timeStamp);
}

#define MAX_TRY_SIZE 0x8000000 // 256 MB

jlib_decl void PrintMemoryStatusLog()
{
#ifdef _WIN32
    MEMORYSTATUS mS;
    GlobalMemoryStatus(&mS);
    LOG(MCdebugInfo, unknownJob, "Available Physical Memory = %dK", (unsigned)(mS.dwAvailPhys/1024));
#ifdef FRAGMENTATION_CHECK
    // see if fragmented
    size32_t sz = MAX_TRY_SIZE;
    while (sz) {
        if (sz<mS.dwAvailPhys/4) {
            void *p=malloc(sz);
            if (p) {
                free(p);
                break;
            }
        }
        sz /= 2;
    }
    sz *= 2;
    if ((sz<MAX_TRY_SIZE)&&(sz<mS.dwAvailPhys/4)) {
        LOG(MCdebugInfo, unknownJob, "WARNING: Could not allocate block size %d", sz);
       _HEAPINFO hinfo;
       int heapstatus;
       hinfo._pentry = NULL;
       size32_t max=0;
       unsigned fragments=0;
       while( ( heapstatus = _heapwalk( &hinfo ) ) == _HEAPOK ) { 
           if (hinfo._useflag != _USEDENTRY) {
                if (hinfo._size>max)
                    max = hinfo._size;
                fragments++;
           }
       }
       LOG(MCdebugInfo, unknownJob, "Largest unused fragment = %d", max);
       LOG(MCdebugInfo, unknownJob, "Number of fragments = %d", fragments);
    }
#endif
#endif
}


static class _init_jmisc_class
{
public:
    ~_init_jmisc_class()
    {
        if (logFile)
            fclose(logFile);
    }
} _init_jmisc;

FILE *xfopen(const char *path, const char *mode)
{
    char *s, *e, *p;
    p = s = strdup(path);
    e = s+strlen(path);
    bool alt=false;
    for (; p<e; p++)
    {
#ifdef _WIN32
        if (*p == '/')
#else
        if (*p == '\\')
#endif
        {
            alt = true;
            *p = PATHSEPCHAR;
        }
    }
    if (alt) 
        printf("XFOPEN ALTERED FILENAME FROM:%s TO %s\n", path, s);
    FILE *file = ::fopen(s, mode);
    free(s);
    return file;
}

const char * queryCcLogName()
{
  const char* logFileName = getenv("ECL_CCLOG");
  if (!logFileName)
  {
    logFileName = "cclog.txt";
  }
  return logFileName;
  //More does output get redirected here?
}

StringBuffer& queryCcLogName(const char* wuid, StringBuffer& logname)
{
    if(!wuid || !*wuid)
        logname.append(queryCcLogName());
    else
    {
        const char* presetName = getenv("ECL_CCLOG");
        if (presetName && *presetName)
            logname.append(presetName);
        else
            logname.append(wuid).trim().append(".cclog.txt");
    }

    return logname;
}

jlib_decl char* readarg(char*& curptr)
{
    char *cur = curptr;
    if(cur == NULL || *cur == 0)
        return NULL;

    while(*cur == ' ' || *cur == '\t') {
        cur++;
        if (!*cur) {
            curptr = cur;
            return NULL;
        }
    }

    char quote = 0;
    if(*cur == '\'' || *cur == '"')
    {
        quote = *cur;
        cur++;
    }
    char *ret = cur;

    if (quote) 
    {
        while (*cur && *cur!=quote)
            cur++;
    }
    else 
    {
        do {
            cur++;
        } while(*cur && *cur != ' ' && *cur != '\t');
    }

    if(*cur != 0)
    {
        *cur = 0;
        cur++;
    }
    curptr = cur;
    return ret;
}

#ifdef _WIN32
bool invoke_program(const char *command_line, DWORD &runcode, bool wait, const char *outfile, HANDLE *rethandle, bool throwException, bool newProcessGroup)
{
    runcode = 0;
    if (rethandle)
        *rethandle = 0;
    if(!command_line || !*command_line)
        return false;
    STARTUPINFO StartupInfo;
    SECURITY_ATTRIBUTES security;

    _clear(security);
    security.nLength = sizeof(security);
    security.bInheritHandle = TRUE;

    _clear(StartupInfo);
    StartupInfo.cb = sizeof(StartupInfo);
    HANDLE outh = NULL;
    if (outfile&&*outfile) {
        outh = CreateFile(outfile,GENERIC_READ|GENERIC_WRITE,FILE_SHARE_READ|FILE_SHARE_WRITE,&security,CREATE_ALWAYS,FILE_ATTRIBUTE_NORMAL,NULL);
        if (outh == INVALID_HANDLE_VALUE)
        {
            OERRLOG("Cannot create file '%s' error code %d",outfile,(int)GetLastError());
            return false;
        }
    }
    if (outh) {
        if (!DuplicateHandle(GetCurrentProcess(),outh,GetCurrentProcess(),&StartupInfo.hStdOutput, 0, TRUE, DUPLICATE_SAME_ACCESS) || 
             !DuplicateHandle(GetCurrentProcess(),outh,GetCurrentProcess(),&StartupInfo.hStdError, 0, TRUE, DUPLICATE_SAME_ACCESS))
        {
            OERRLOG("Execution of \"%s\" failed, DuplicateHandle error = %d",command_line, (int)GetLastError());
            CloseHandle(outh);
            return false;
        }

        StartupInfo.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
        StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        CloseHandle(outh);
    }
    PROCESS_INFORMATION ProcessInformation;
    bool ok = CreateProcess(NULL,(char *)command_line,NULL,NULL,TRUE,0,NULL,NULL,&StartupInfo,&ProcessInformation)!=0;
    if (ok) {
        if (wait)
        {
            WaitForSingleObject(ProcessInformation.hProcess,INFINITE);
            GetExitCodeProcess(ProcessInformation.hProcess, &runcode);
        }
        CloseHandle(ProcessInformation.hThread);
        if (rethandle) {
            *rethandle = (HANDLE)ProcessInformation.hProcess;
            if (wait)
                CloseHandle(ProcessInformation.hProcess); 
        }
        else
            CloseHandle(ProcessInformation.hProcess); 
    }
    else
    {
        int lastError = (int)GetLastError();      // for debugging
        //print out why create process failed
        OERRLOG("Execution of \"%s\" failed, error = %d",command_line,lastError);
    }
    if (outh)
    {
        CloseHandle(StartupInfo.hStdOutput);
        CloseHandle(StartupInfo.hStdError);
    }

    return ok;
}

bool wait_program(HANDLE handle,DWORD &runcode,bool block)
{
    runcode = (DWORD)-1;
    if ((handle==NULL)||(handle==(HANDLE)-1))  
        return true; // actually it failed
    int ret = WaitForSingleObject(handle,block?INFINITE:0);
    int err = GetLastError();
    if (ret==WAIT_OBJECT_0) {
        GetExitCodeProcess(handle, &runcode);
        CloseHandle(handle);
        return true;
    }
    return false;
}

jlib_decl bool interrupt_program(HANDLE handle, bool stopChildren, int signum)
{
    if (signum==0)
        return TerminateProcess(handle,1)!=FALSE;
    OERRLOG("interrupt_program signal %d not supported in windows",signum);
    return false;
}

void close_program(HANDLE handle)
{
    CloseHandle(handle);
}


#else
bool invoke_program(const char *command_line, DWORD &runcode, bool wait, const char *outfile, HANDLE *rethandle, bool throwException, bool newProcessGroup)
{
    runcode = 0;
    if (rethandle)
        *rethandle = 0;
    if(!command_line || !*command_line)
        return false;
  
    pid_t pid = fork();
    if (pid == 0) 
    {
        //Force the child process into its own process group, so we can terminate it and its children.
        if (newProcessGroup)
            setpgid(0,0);
        if (outfile&&*outfile) {
            int outh = open(outfile, O_CREAT|O_WRONLY|O_TRUNC, S_IRUSR|S_IWUSR);
            if(outh >= 0)
            {
                dup2(outh, STDOUT_FILENO);
                dup2(outh, STDERR_FILENO);
                close(outh);
            }
            else  {
                OERRLOG("invoke_program: could not open %s",outfile);
                return false;
            }
        }

        int size = 10;
        char **args;
        char **argptr = args = (char **) malloc(sizeof(args) * size);
        char **over = argptr+size; 

        char *cl = strdup(command_line);
        char *curptr = cl;

        *argptr = readarg(curptr);
        while (*argptr != NULL)
        {
            argptr++;
            if (argptr==over)
            {
                args = (char **) realloc(args, sizeof(args) * size * 2);
                argptr = args+size;
                size *= 2;
                over = args+size; 
            }
            *argptr = readarg(curptr);
        }
        // JCSMORE - do I not need to free args, if successful exec?

        if (execvp(*args, args))
        {
            //print out why create process failed
            int err = errno;
            OERRLOG("invoke_program(%s) failed with error(%d): %s",command_line,err,strerror(err));
        }
        assertex(0); // don't expect to get here!
        _exit(0);
    }
    else if (pid < 0)
    {
        StringBuffer s("fork of \"");
        s.append(command_line).append("\" failed. fork() returned:");
        if (errno == EAGAIN)
            s.append("EAGAIN");
        else if (errno == ENOMEM)
            s.append("ENOMEM");
        else
            s.append(errno);

        OERRLOG("%s",s.str());
        if(throwException)
            throw MakeStringExceptionDirect(-1, s.str());
        return false;
    }

    if (rethandle)
        *rethandle = (HANDLE)pid;
    if (wait)
    {
        int retv;
        while (1)
        {
            auto wpid = waitpid(pid, &retv, 0);
            if (wpid == pid)
                break;
            if (errno != EINTR)
            {
                OERRLOG("invoke_program(%s): wait failed (%d, %d, %d)",command_line,(int) wpid, retv, errno);
                return false;
            }
        }
        if (!WIFEXITED(retv)) //did not exit normally
        {
            int err = errno;
            OERRLOG("invoke_program(%s): failed.",command_line);
            OERRLOG("The process was killed by signal %d%s.",(int)WTERMSIG(retv),WCOREDUMP(retv)?" - core dumped":"");
            OERRLOG("Last system error is %s",strerror(err));
        }

        runcode = WEXITSTATUS(retv);
    }
    return true; // it did run even if signalled
}


bool wait_program(HANDLE handle,DWORD &runcode,bool block)
{
    pid_t pid = (pid_t)handle;
    runcode = (DWORD)-1;
    if ((int)pid<=0)
        return true; // actually it failed
    int retv;
    pid_t ret = waitpid(pid, &retv, block?0:WNOHANG);
    if (ret == (pid_t)-1) {
        IERRLOG("wait_program: wait failed (%d)",errno);
        return true; // actually it failed but assume finished
    }
    else if (ret==0)
        return false;

    runcode = WEXITSTATUS(retv);
    return true;
}


bool interrupt_program(HANDLE handle, bool stopChildren, int signum)
{
    if (signum == 0)
        signum = SIGINT;

    pid_t pid = (pid_t)handle;
    if ((int)pid<=0)
        return false;

    //If we need to also stop child processes then kill the process group (same as the pid)
    //Note: This will not apply to grand-children started by the children by calling invoke_program()
    //since they will have a different process group
    if (stopChildren)
        pid = -pid;
    return (kill(pid, signum)==0);
}

void close_program(HANDLE handle)
{
    // not needed in linux
}

#endif

#ifndef _WIN32
bool CopyFile(const char *file, const char *newfile, bool fail)
{
    struct stat s;
    if ((fail) && (0 == stat((char *)newfile, &s))) return false;
    FILE *in=fopen(file,"rb"), *out=fopen(newfile,"wb");
    try
    {
        if (!in)
            throw MakeStringException(-1, "failed to open %s for copy",file);
        if (!out)
            throw MakeStringException(-1, "failed to create %s",newfile);
        char b[1024];
        while (true)
        {
            int c=fread(b,1,sizeof(b),in);
            if (!c) break;
            if (!fwrite(b,c,1,out)) throw MakeStringException(-1, "failed to copy file %s to %s",file,newfile);
        }
        fclose(in);
        fclose(out);
        stat((char *)file, &s);
        chmod(newfile, s.st_mode);
    } catch (...)
    {
        if (in)  fclose(in);
        if (out) fclose(out);
        return false;
    }

    return true;
}
#endif

//========================================================================================================================

static bool hadAbortSignal = false;
static bool handlerInstalled = false;
CriticalSection abortCrit;

class AbortHandlerInfo : public CInterface
{
public:
    ThreadId installer;
    AbortHandler handler;
    SimpleAbortHandler shandler;
    IAbortHandler *ihandler;
    AbortHandlerInfo(AbortHandler _handler)
    {
        handler = _handler;
        shandler = NULL;
        ihandler = NULL;
        installer = GetCurrentThreadId();
    }
    AbortHandlerInfo(SimpleAbortHandler _handler)
    {
        handler = NULL;
        shandler = _handler;
        ihandler = NULL;
        installer = GetCurrentThreadId();
    }
    AbortHandlerInfo(IAbortHandler *_ihandler)
    {
        handler = NULL;
        shandler = NULL;
        ihandler = _ihandler;
        installer = GetCurrentThreadId();
    }

    bool handle(ahType type)
    {
#ifndef _WIN32
        if (installer == GetCurrentThreadId())
#endif
        {
//          DBGLOG("handle abort %x", GetCurrentThreadId());
            if (handler)
                return handler(type);
            else if (shandler)
                return shandler();
            else
                return ihandler->onAbort();
        }
#ifndef _WIN32
        else
            return false;
#endif
    }
};

CIArrayOf<AbortHandlerInfo> handlers;

bool notifyOnAbort(ahType type)
{
//  DBGLOG("notifyOnAbort %x", GetCurrentThreadId());
//      CriticalBlock c(abortCrit); You would think that this was needed, but it locks up.
//      If it needs to be threadsafe, have to clone the list or something
    bool doExit = false;
    ForEachItemInRev(idx, handlers)
    {
        if (handlers.item(idx).handle(type))
            doExit = true;
    }
//  DBGLOG("notifyOnAbort returning %d", (int) doExit);
    return doExit;
}

#ifdef _WIN32
BOOL WINAPI WindowsAbortHandler ( DWORD dwCtrlType ) 
{ 
    switch( dwCtrlType ) 
    { 
        case CTRL_BREAK_EVENT:  // use Ctrl+C or Ctrl+Break to simulate 
        case CTRL_C_EVENT:      // SERVICE_CONTROL_STOP in debug mode 
        case CTRL_CLOSE_EVENT:
        {
            hadAbortSignal = true;
            bool doExit = notifyOnAbort(ahInterrupt);
            return !doExit; 
        }
        case CTRL_LOGOFF_EVENT:
        case CTRL_SHUTDOWN_EVENT:
            hadAbortSignal = true;
            notifyOnAbort(ahTerminate);
            return FALSE;
    }
    return FALSE; 
} 

BOOL WINAPI ModuleExitHandler ( DWORD dwCtrlType ) 
{ 
    switch( dwCtrlType ) 
    { 
        case CTRL_BREAK_EVENT:  // use Ctrl+C or Ctrl+Break to simulate 
        case CTRL_C_EVENT:      // SERVICE_CONTROL_STOP in debug mode 
        case CTRL_CLOSE_EVENT:
        case CTRL_LOGOFF_EVENT:
        case CTRL_SHUTDOWN_EVENT:
            ExitModuleObjects();
    }
    return FALSE; 
} 
#elif defined(__linux__)
static void UnixAbortHandler(int signo)
{
    ahType type = ahInterrupt;
    if (SIGTERM == signo)
            type = ahTerminate;

    hadAbortSignal = true;
    if (handlers.length()==0 || notifyOnAbort(type))
    {
        _exit(0);
    }
}
#endif

void queryInstallAbortHandler()
{
    if (handlerInstalled)
        return;

#if defined(_WIN32)
    SetConsoleCtrlHandler( WindowsAbortHandler, TRUE ); 
#elif defined(__linux__)
    struct sigaction action;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_RESTART;
    action.sa_handler = (void(*)(int))UnixAbortHandler;
    if (sigaction(SIGINT,  &action, NULL) == -1 ||
        sigaction(SIGQUIT, &action, NULL) == -1 || 
        sigaction(SIGTERM, &action, NULL) == -1)
    {
        perror("sigaction in queryInstallAbortHandler failed");
    }
#endif
    handlerInstalled = true;
}

void queryUninstallAbortHandler()
{
    if (handlers.ordinality())
        return;

#if defined(_WIN32)
    if (handlerInstalled)
    {
        SetConsoleCtrlHandler( WindowsAbortHandler, FALSE);
        handlerInstalled = false;
    }
#else
    // Don't uninstall - we always want one for the module exit support
#endif
}

MODULE_INIT(INIT_PRIORITY_JMISC2)
{
#if defined(_WIN32)
    // NOTE: handlers are called in LIFO order and hence any handler that returns false
    // (e.g CTRL-C not wanting to abort)) will stop this handler being called also (correctly).
    SetConsoleCtrlHandler( ModuleExitHandler, TRUE);
#elif defined(__linux__)
    queryInstallAbortHandler();
#endif
    return true;
}


void addAbortHandler(AbortHandler handler)
{
    CriticalBlock c(abortCrit);
    queryInstallAbortHandler();
    handlers.append(*new AbortHandlerInfo(handler));
}

void addAbortHandler(SimpleAbortHandler handler)
{
    CriticalBlock c(abortCrit);
    queryInstallAbortHandler();
    handlers.append(*new AbortHandlerInfo(handler));
}

void addAbortHandler(IAbortHandler & handler)
{
    CriticalBlock c(abortCrit);
    queryInstallAbortHandler();
    handlers.append(*new AbortHandlerInfo(&handler));
}

void removeAbortHandler(AbortHandler handler)
{
    CriticalBlock c(abortCrit);
    ForEachItemInRev(idx, handlers)
    {
        if (handlers.item(idx).handler == handler)
        {
            handlers.remove(idx);
            break;
        }
    }
    queryUninstallAbortHandler();
}

void removeAbortHandler(SimpleAbortHandler handler)
{
    CriticalBlock c(abortCrit);
    ForEachItemInRev(idx, handlers)
    {
        if (handlers.item(idx).shandler == handler)
        {
            handlers.remove(idx);
            break;
        }
    }
    queryUninstallAbortHandler();
}

void removeAbortHandler(IAbortHandler & handler)
{
    CriticalBlock c(abortCrit);
    ForEachItemInRev(idx, handlers)
    {
        if (handlers.item(idx).ihandler == &handler)
        {
            handlers.remove(idx);
            break;
        }
    }
    queryUninstallAbortHandler();
}

bool isAborting()
{
    return hadAbortSignal;
}

void throwAbortException()
{
    throw MakeStringException(JLIBERR_UserAbort, "Operation aborted by user");
}

void throwExceptionIfAborting() 
{ 
    if (isAborting())
        throwAbortException(); 
}

//========================================================================================================================




StringBuffer & hexdump2string(byte const * in, size32_t inSize, StringBuffer & out)
{
    out.append("[");
    byte last = 0;
    unsigned seq = 1;
    for(unsigned i=0; i<inSize; ++i)
    {
        if((i>0) && (in[i]==last))
        {
            ++seq;
        }
        else
        {
            if(seq>1)
            {
                if(seq==2)
                    out.appendf(" %02X", last);
                else
                    out.appendf("x%u", seq);
                seq = 1;
            }
            out.appendf(" %02X", in[i]);
            last = in[i];
        }
    }
    if(seq>1)
        out.appendf("x%u", seq);
    out.append(" ]");
    return out;
}

jlib_decl bool getHomeDir(StringBuffer & homepath)
{
#ifdef _WIN32
    const char *home = getenv("APPDATA");
    // Not the 'official' way - which changes with every windows version
    // but should work well enough for us (and avoids sorting out windows include mess)
#else
    const char *home = getenv("HOME");
    if (!home)
    {
        struct passwd *pw = getpwuid(getuid());
        home = pw->pw_dir;
    }
#endif
    if (!home)
        return false;
    homepath.append(home);
    return true;
}

#ifdef _WIN32
char *mkdtemp(char *_template)
{
    if (!_template || strlen(_template) < 6 || !streq(_template+strlen(_template)-6, "XXXXXX"))
    {
        errno = EINVAL;
        return nullptr;
    }
    char * tail = _template + strlen(_template) - 6;
    for (int i = 0; i < 100; i++)
    {
        snprintf(tail, 7, "%06d", fastRand());
        if (!_mkdir(_template))
            return _template;
        if (errno != EEXIST)
            return nullptr;
    }
    errno = EINVAL;
    return nullptr;
}
#endif
