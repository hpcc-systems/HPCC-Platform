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
