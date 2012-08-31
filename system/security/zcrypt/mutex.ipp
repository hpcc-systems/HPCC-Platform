/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

// Documentation for this file can be found at http://mgweb.mg.seisint.com/WebHelp/jlib/html/jmutex_hpp.html
#ifndef _MUTEX_IPP__
#define _MUTEX_IPP__

#include <assert.h>
#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#define ThreadId DWORD
#define MutexId HANDLE

class ZMutex
{
public:
    ZMutex()
    {
        mutex = CreateMutex(NULL, FALSE, NULL);
        lockcount = 0;
        owner = 0;
    }
    ZMutex(const char *name)
    {
        mutex = CreateMutex(NULL, FALSE, name);
        lockcount = 0;
        owner = 0;
    }
    ~ZMutex()
    {
        if (owner != 0)
            printf("Warning - Owned mutex destroyed"); // can't use PrintLog here!
        CloseHandle(mutex);
    }
    void lock()
    {
        WaitForSingleObject(mutex, INFINITE);
        if (lockcount) {
            if(owner!=GetCurrentThreadId())     // I think only way this can happen is with unhandled thread exception
                lockcount = 0;                  // (don't assert as unhandled error may get lost)
        }
        lockcount++;
        owner=GetCurrentThreadId();
    }
    bool lockWait(long timeout)
    {
        if (WaitForSingleObject(mutex, timeout)!=WAIT_OBJECT_0)
            return false;
        if (lockcount) {
            if(owner!=GetCurrentThreadId())     // I think only way this can happen is with unhandled thread exception
                lockcount = 0;                  // (don't assert as unhandled error may get lost)
        }
        lockcount++;
        owner=GetCurrentThreadId();
        return true;
    }
    void unlock()
    {
        --lockcount;
        if (lockcount==0)
            owner = 0;
        ReleaseMutex(mutex);
    }

protected:
    MutexId mutex;
    ThreadId owner;
    int unlockAll()
    {
        int ret = lockcount;
        int lc = ret;
        while (lc--)
            unlock();
        return ret;
    }
    void lockAll(int count)
    {
        while (count--)
            lock();
    }
    
private:
    int lockcount;
};

#else // posix
#include <sys/time.h>
#include <errno.h>
#include <pthread.h>

#define ThreadId pthread_t
#define MutexId pthread_mutex_t

class ZMutex
{
public:
    ZMutex();
    ~ZMutex();
    void lock();
    bool lockWait(long timeout);
    void unlock();
protected:
    MutexId mutex;
    ThreadId owner;
    int unlockAll();
    void lockAll(int);
private:
    int lockcount;
    pthread_cond_t lock_free;
};

#endif

class zsynchronized
{
private:
    ZMutex &mutex;
    void throwLockException(unsigned timeout);
public:
    zsynchronized(ZMutex &m) : mutex(m) { mutex.lock(); };
    zsynchronized(ZMutex &m,long timeout) : mutex(m) { if(!mutex.lockWait(timeout)) throwLockException(timeout);  }
    ~zsynchronized() { mutex.unlock(); };
};

#endif
