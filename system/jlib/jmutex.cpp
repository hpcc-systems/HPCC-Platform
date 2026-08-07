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

#include "platform.h"
#include "jmutex.hpp"
#include "jsuperhash.hpp"
#include "jmisc.hpp"
#include "jfile.hpp"
#include "jerror.hpp"

#include <stdio.h>
#include <assert.h>

//===========================================================================
#ifndef _WIN32

LegacyMutex::LegacyMutex([[maybe_unused]] const char *syncName)
{
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&lock_free, NULL);
    owner = 0;
    lockcount = 0;
#ifdef PROTRACE_LOCKS
    if (trackUnnamedLocks || syncName)
        syncid = protrace::note_lock(syncName);
#endif
}

LegacyMutex::~LegacyMutex()
{
    pthread_cond_destroy(&lock_free);
    pthread_mutex_destroy(&mutex);
}

void LegacyMutex::lock()
{
    noteEvent(EventLockWait);
    pthread_mutex_lock(&mutex);
    while ((owner!=0) && !pthread_equal(owner, pthread_self()))
        pthread_cond_wait(&lock_free, &mutex);
    if (lockcount++==0)
        owner = pthread_self();
    pthread_mutex_unlock(&mutex);
    noteEvent(EventLockAcquire);
}

bool LegacyMutex::lockWait(unsigned timeout)
{
    if (timeout==(unsigned)-1) {
        lock();
        return true;
    }
    noteEvent(EventLockWait);
    pthread_mutex_lock(&mutex);
    bool first=true;
    while ((owner!=0) && !pthread_equal(owner, pthread_self())) {
        timespec abs;
        if (first) {
            getEndTime(abs, timeout);
            first = false;
        }
        if (pthread_cond_timedwait(&lock_free, &mutex, &abs)==ETIMEDOUT) {
            pthread_mutex_unlock(&mutex);
            noteEvent(EventLockWaitTimeout);
            return false;
        }
    }
    if (lockcount++==0)
        owner = pthread_self();
    pthread_mutex_unlock(&mutex);
    noteEvent(EventLockAcquire);
    return true;
}

void LegacyMutex::unlock()
{
    pthread_mutex_lock(&mutex);
#ifdef _DEBUG
    assertex(pthread_equal(owner, pthread_self()));
#endif 
    if (--lockcount==0)
    {
        owner = 0;
        pthread_cond_signal(&lock_free);
    }
    pthread_mutex_unlock(&mutex);
    noteEvent(EventLockRelease);
}

void LegacyMutex::lockAll(int count)
{
    //MORE: I have no idea what events to record here - only used by Monitor
    // which is only used by TaskQueue, which is barely used.
    if (count) {
        pthread_mutex_lock(&mutex);
        while ((owner!=0) && !pthread_equal(owner, pthread_self()))
            pthread_cond_wait(&lock_free, &mutex);
        lockcount = count;
        owner = pthread_self();
        pthread_mutex_unlock(&mutex);
    }
}

int LegacyMutex::unlockAll()
{
    pthread_mutex_lock(&mutex);
    int ret = lockcount;
    if (lockcount!=0) {
#ifdef _DEBUG
        assertex(pthread_equal(owner, pthread_self()));
#endif
        lockcount = 0;
        owner = 0;
        pthread_cond_signal(&lock_free);
    }
    pthread_mutex_unlock(&mutex);
    return ret;
}

inline bool read_data(int fd, void *buf, size_t nbytes) 
{
    size32_t nread = 0;
    while (nread<nbytes) {
        size32_t rd = read(fd, (char *)buf + nread, nbytes-nread);
        if ((int)rd>=0)
            nread += rd;
        else if (errno != EINTR) 
            return false;
    }
    return true;
}

inline bool write_data(int fd, const void *buf, size_t nbytes) 
{
    size32_t nwritten= 0;
    while (nwritten<nbytes) {
        size32_t wr = write(fd, (const char *)buf + nwritten, nbytes-nwritten);
        if ((int)wr>=0)
            nwritten += wr;
        else if (errno != EINTR) 
            return false;
    }
    return true;
}

constexpr unsigned MUTEX_POLLTIME=(1000*15);

static bool lock_file(const char *lfpath) 
{
    unsigned attempt = 0;
    while (attempt < 3) {
        char lckcontents[12];
        int fd = open(lfpath, O_RDWR | O_CREAT | O_EXCL, S_IRWXU);
        if (fd==-1) {
            if (errno != EEXIST) 
                break;
            fd = open(lfpath, O_RDONLY);
            if (fd==-1) 
                break;
            bool ok = read_data(fd, lckcontents, sizeof(lckcontents)-1);
            close(fd);
            if (ok) {
                lckcontents[sizeof(lckcontents)-1] = 0;
                int pid = atoi(lckcontents);
                if (pid==getpid())
                    return true;
                if (kill(pid, 0) == -1) {
                    if (errno != ESRCH) 
                        return false;
                    unlink(lfpath);
                    continue;
                }
            }
            Sleep(1000);
            attempt++;
        }
        else {
            sprintf(lckcontents,"%10d\n",(int)getpid());
            bool ok = write_data(fd, lckcontents, sizeof(lckcontents)-1);
            close(fd);
            if (!ok)
                break;
        }
    }
    return false;
}

static void unlock_file(const char *lfpath) 
{
    for (unsigned attempt=0;attempt<10;attempt++) {
        if (unlink(lfpath)>=0)
            return;
        attempt++;
        Sleep(500);
    }
    IERRLOG("NamedMutex cannot unlock file (%d)",errno);
}

static CriticalSection lockPrefixCS(SYNC_LOCATION);
static StringBuffer lockPrefix;
NamedMutex::NamedMutex([[maybe_unused]] const char *syncName, const char *name)
{
#ifdef PROTRACE_LOCKS
    if (trackUnnamedLocks || syncName)
        syncid = protrace::note_lock(syncName);
#endif
    {
        CriticalBlock b(lockPrefixCS);
        if (0 == lockPrefix.length())
        {
            if (getConfigurationDirectory(NULL, "lock", NULL, NULL, lockPrefix))
                addPathSepChar(lockPrefix);
            else
                WARNLOG("Failed to get lock directory from environment");
            lockPrefix.append("JLIBMUTEX_");
        }
    }
    StringBuffer tmp(lockPrefix);
    tmp.append(name);
    mutexfname = tmp.detach();
}

NamedMutex::~NamedMutex()
{
    free(mutexfname);
}

void NamedMutex::lock()
{
    noteEvent(EventLockWait);
    // first lock locally
    threadmutex.lock();
    // then lock globally
    for (;;) {
        if (lock_file(mutexfname))
        {
            noteEvent(EventLockAcquire);
            return;
        }
        Sleep(MUTEX_POLLTIME);
    }
}

bool NamedMutex::lockWait(unsigned timeout)
{
    noteEvent(EventLockWait);
    unsigned t = msTick();
    // first lock locally
    if (!threadmutex.lockWait(timeout))
    {
        noteEvent(EventLockWaitTimeout);
        return false;
    }
    // then lock globally
    for (;;) {
        if (lock_file(mutexfname))
        {
            noteEvent(EventLockAcquire);
            return true;
        }
        unsigned elapsed = msTick()-t;
        if (elapsed>=timeout) {
            threadmutex.unlock();
            break;
        }
        Sleep((timeout-elapsed)>MUTEX_POLLTIME ? MUTEX_POLLTIME : (timeout-elapsed));
    }

    noteEvent(EventLockWaitTimeout);
    return false;
}

void NamedMutex::unlock()
{
    // assumed held
    unlock_file(mutexfname);
    threadmutex.unlock();
    noteEvent(EventLockRelease);
}
#endif

bool TimedMutex::lockWait(unsigned timeout)
{
    if (timeout==(unsigned)-1) {
        lock();
        return true;
    }
    noteEvent(EventLockWait);
    std::chrono::milliseconds ms(timeout);
    bool locked = mutex.try_lock_for(ms);
    if (locked)
        noteEvent(EventLockAcquire);
    else
        noteEvent(EventLockWaitTimeout);
    return locked;
}

void TimedMutexBlock::throwLockException(unsigned timeout)
{
    throw MakeStringException(JLIBERR_SystemCanNotLockD,"Can not lock - %d",timeout);
}



//===========================================================================

void Monitor::wait()
{
    assertex(mutex.owner==GetCurrentThreadId());
    waiting++;
    void *cur = last;
    last = &cur;
    while (1) {
        int locked = mutex.unlockAll();
        sem->wait();
        mutex.lockAll(locked);
        if (cur==NULL) { // i.e. first in
            void **p=(void **)&last;
            while (*p!=&cur)
                p = (void **)*p;
            *p = NULL; // reset so next picks up
            break;
        }
        sem->signal();
    }
}

void Monitor::notify()
{   // should always be locked
    assertex(mutex.owner==GetCurrentThreadId());
    if (waiting)
    {
        waiting--;
        sem->signal();
    }
}

void Monitor::notifyAll()
{   // should always be locked
    assertex(mutex.owner==GetCurrentThreadId());
    if (waiting)
    {
        sem->signal(waiting);
        waiting = 0;
    }
}

//==================================================================================

#ifdef USE_PTHREAD_RWLOCK

bool ReadWriteLock::lockRead(unsigned timeout)
{
    if (timeout == (unsigned)-1)
    {
        lockRead();
        return true;
    }

    if (pthread_rwlock_tryrdlock(&rwlock) == 0)
        return true;

    timespec endtime;
    getEndTime(endtime, timeout);
    return (pthread_rwlock_timedrdlock(&rwlock, &endtime) == 0);
}

bool ReadWriteLock::lockWrite(unsigned timeout)
{
    if (timeout == (unsigned)-1)
    {
        lockWrite();
        return true;
    }

    if (pthread_rwlock_trywrlock(&rwlock) == 0)
        return true;

    timespec endtime;
    getEndTime(endtime, timeout);
    return (pthread_rwlock_timedwrlock(&rwlock, &endtime) == 0);
}
#endif

//==================================================================================

#ifdef USECHECKEDCRITICALSECTIONS
CheckedReadLockBlock::CheckedReadLockBlock(ReadWriteLock &l, unsigned timeout, const char *fname,unsigned lnum) : lock(l)
{
    for (;;)
    {
        if (lock.lockRead(timeout))
            break;
        PROGLOG("CheckedReadLockBlock timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

CheckedWriteLockBlock::CheckedWriteLockBlock(ReadWriteLock &l, unsigned timeout, const char *fname,unsigned lnum) : lock(l)
{
    for (;;)
    {
        if (lock.lockWrite(timeout))
            break;
        PROGLOG("CheckedWriteLockBlock timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

void checkedReadLockEnter(ReadWriteLock &lock, unsigned timeout, const char *fname, unsigned lnum)
{
    for (;;)
    {
        if (lock.lockRead(timeout))
            break;
        PROGLOG("checkedReadLockEnter timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

void checkedWriteLockEnter(ReadWriteLock &lock, unsigned timeout, const char *fname, unsigned lnum)
{
    for (;;)
    {
        if (lock.lockWrite(timeout))
            break;
        PROGLOG("checkedWriteLockEnter timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

//==================================================================================

void checkedCritEnter(CheckedCriticalSection &crit, unsigned timeout, const char *fname, unsigned lnum)
{
    for (;;)
    {
        if (crit.lockWait(timeout))
            break;
        PROGLOG("checkedCritEnter timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}
void checkedCritLeave(CheckedCriticalSection &crit)
{
    crit.unlock();
}

CheckedCriticalBlock::CheckedCriticalBlock(CheckedCriticalSection &c, unsigned timeout, const char *fname,unsigned lnum) 
    : crit(c)       
{ 
    for (;;)
    {
        if (crit.lockWait(timeout))
            break;
        PROGLOG("CheckedCriticalBlock timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

CheckedCriticalUnblock::~CheckedCriticalUnblock()
{ 
    for (;;)
    {
        if (crit.lockWait(timeout))
            break;
        PROGLOG("CheckedCriticalUnblock timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}
#endif

void ThreadYield()
{   // works for SCHED_RR threads (<spit>) also
#ifdef _WIN32
    Sleep(0);
#else
    pthread_t self = pthread_self();
    int policy;
    sched_param param;
    pthread_getschedparam(self, &policy, &param);
    if (policy==SCHED_RR) {
        int saveprio = param.sched_priority;
        param.sched_priority = 0;
        pthread_setschedparam(self, SCHED_OTHER, &param);
        param.sched_priority = saveprio;
        sched_yield();
        pthread_setschedparam(self, policy, &param);
    }
    else
        sched_yield();
#endif
}

void spinUntilReady(std::atomic_uint &value)
{
    unsigned i = 0;
    const unsigned maxSpins = 10;
    while (value.load(std::memory_order_relaxed))
    {
        if (i++ == maxSpins)
        {
            i = 0;
            ThreadYield();
        }
    }
}
