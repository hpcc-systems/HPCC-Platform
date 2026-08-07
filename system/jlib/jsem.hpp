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



#ifndef __JSEM__
#define __JSEM__

#include "jiface.hpp"
#include "jprotrace.hpp"
#include "jeventconst.hpp"

typedef unsigned sync_uid_type;

// Introduce a constexpr function for stripping the path from the filename:line text to reduce memory usage.
constexpr const char * extractTail(const char * text)
{
    const char * result = text;
    for (const char * p = text; *p; p++)
        if (*p == '/' || *p == '\\')
            result = p + 1;
    return result;
}

#define SYNC_STRINGIZE2(x)   #x
#define SYNC_STRINGIZE(x)    SYNC_STRINGIZE2(x)
#define SYNC_LOCATION        (extractTail(__FILE__ ":" SYNC_STRINGIZE(__LINE__)))
//Use SYNC_UNNAMED for Semaphores or Mutexes that are uninteresting - e.g. singletons, or other rarely used locks.
#define SYNC_UNNAMED         nullptr

#ifdef _WIN32

class jlib_decl Semaphore
{
protected:
    inline void noteEvent(EventType event)
    {
#ifdef PROTRACE_SEMAPHORES
        if (trackUnnamedLocks || likely(syncid))
            protraceRecord(static_cast<unsigned>(event), syncid);
#endif
    }
    Semaphore([[maybe_unused]] const char *syncName, const char *name)
    {
        hSem = CreateSemaphore(NULL, 0, 0x7fffffff, name);
#ifdef PROTRACE_SEMAPHORES
        if (trackUnnamedLocks || syncName)
            syncid = protrace::note_semaphore(syncName);
#endif
    }
public:
    Semaphore([[maybe_unused]] const char *syncName, unsigned initialCount = 0U)
    {
        hSem = CreateSemaphore(NULL, initialCount, 0x7fffffff, NULL);
#ifdef PROTRACE_SEMAPHORES
        if (trackUnnamedLocks || syncName)
            syncid = protrace::note_semaphore(syncName);
#endif
    }


    ~Semaphore()
    {
        CloseHandle(hSem);
    }

    bool tryWait()
    {
        noteEvent(EventSemWait);
        bool acquired = (WaitForSingleObject(hSem, 0) == WAIT_OBJECT_0);
        if (acquired)
            noteEvent(EventSemAcquire);
        else
            noteEvent(EventSemWaitTimeout);
        return acquired;
    }

    void wait()
    {
        noteEvent(EventSemWait);
        WaitForSingleObject(hSem, INFINITE);
        noteEvent(EventSemAcquire);
    }

    void reinit(unsigned initialCount=0U)
    {
        CloseHandle(hSem);
        hSem = CreateSemaphore(NULL, initialCount, 0x7fffffff, NULL);
    }

    bool wait(unsigned timeout)
    {
        noteEvent(EventSemWait);
        bool acquired = (WaitForSingleObject(hSem, (timeout==(unsigned)-1)?INFINITE:timeout)==WAIT_OBJECT_0);
        if (acquired)
            noteEvent(EventSemAcquire);
        else
            noteEvent(EventSemWaitTimeout);
        return acquired;
    }

    void signal()
    {
        ReleaseSemaphore(hSem,1,NULL);
        noteEvent(EventSemSignal);
    }

    void signal(unsigned count)
    {
        ReleaseSemaphore(hSem,count,NULL);
        for (unsigned i=0; i<count; i++)
            noteEvent(EventSemSignal);
    }
protected:
    HANDLE hSem;
#ifdef PROTRACE_SEMAPHORES
    sync_uid_type syncid = 0;
#endif

};

#else

#include <semaphore.h>

void jlib_decl getEndTime(timespec & abs, unsigned timeout);

#ifdef __APPLE__
 // sem_timedwait is not available in OSX, so continue to use old code
 #define USE_OLD_SEMAPHORE_CODE
#endif

class jlib_decl Semaphore
{
public:
    inline void noteEvent(EventType event)
    {
#ifdef PROTRACE_SEMAPHORES
    if (trackUnnamedLocks || likely(syncid))
        protraceRecord(static_cast<unsigned>(event), syncid);
#endif
    }
    Semaphore([[maybe_unused]] const char *syncName, unsigned initialCount=0U);
    ~Semaphore();
    bool tryWait();
    void wait();
    bool wait(unsigned timeout); // in msecs
    void signal();
    void signal(unsigned count);
    void reinit(unsigned initialCount=0U);
#ifndef USE_OLD_SEMAPHORE_CODE
protected:
    sem_t sem;
#else
protected:
    void init();
protected:
    MutexId mx;
    pthread_cond_t cond;
    int count;
#endif
#ifdef PROTRACE_SEMAPHORES
    sync_uid_type syncid = 0;
#endif
};

#endif

#endif
