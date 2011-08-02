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


#include "platform.h"
#include "jmutex.hpp"
#include "jsuperhash.hpp"
#include "jmisc.hpp"

#include <stdio.h>
#include <assert.h>

//===========================================================================
#ifndef _WIN32

Mutex::Mutex()
{
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&lock_free, NULL);
    owner = 0;
    lockcount = 0;
}

Mutex::~Mutex()
{
    pthread_cond_destroy(&lock_free);
    pthread_mutex_destroy(&mutex);
}

void Mutex::lock()
{
    pthread_mutex_lock(&mutex);
    while ((owner!=0) && !pthread_equal(owner, pthread_self()))
        pthread_cond_wait(&lock_free, &mutex);
    if (lockcount++==0)
        owner = pthread_self();
    pthread_mutex_unlock(&mutex);
}

bool Mutex::lockWait(unsigned timeout)
{
    if (timeout==(unsigned)-1) {
        lock();
        return true;
    }
    pthread_mutex_lock(&mutex);
    bool first=true;
    while ((owner!=0) && !pthread_equal(owner, pthread_self())) {
        timespec abs;
        if (first) {
            timeval cur;
            gettimeofday(&cur, NULL);
            unsigned tsec=(timeout/1000);
            abs.tv_sec = cur.tv_sec + tsec;
            abs.tv_nsec = (cur.tv_usec + timeout%1000*1000)*1000;
            if (abs.tv_nsec>=1000000000) {
                abs.tv_nsec-=1000000000;
                abs.tv_sec++;
            }
            first = false;
        }
        if (pthread_cond_timedwait(&lock_free, &mutex, &abs)==ETIMEDOUT) {
            pthread_mutex_unlock(&mutex);
            return false;
        }
    }
    if (lockcount++==0)
        owner = pthread_self();
    pthread_mutex_unlock(&mutex);
    return true;
}

void Mutex::unlock()
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
}

void Mutex::lockAll(int count)
{
    if (count) {
        pthread_mutex_lock(&mutex);
        while ((owner!=0) && !pthread_equal(owner, pthread_self()))
            pthread_cond_wait(&lock_free, &mutex);
        lockcount = count;
        owner = pthread_self();
        pthread_mutex_unlock(&mutex);
    }
}

int Mutex::unlockAll()
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

#define POLLTIME (1000*15)

static bool lock_file(const char *lfpath) 
{
    unsigned attempt = 0;
    int err = 0;
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
    unsigned attempt = 0;
    for (unsigned attempt=0;attempt<10;attempt++) {
        if (unlink(lfpath)>=0)
            return;
        attempt++;
        Sleep(500);
    }
    ERRLOG("NamedMutex cannot unlock file (%d)",errno);
}

NamedMutex::NamedMutex(const char *name)
{
    const char * pfx = "/var/lock/JLIBMUTEX_";
    mutexfname = (char *)malloc(strlen(name)+strlen(pfx)+1);
    strcpy(mutexfname,pfx);
    strcat(mutexfname,name);
}
    
NamedMutex::~NamedMutex()
{
    free(mutexfname);
}

void NamedMutex::lock()
{
    // first lock locally
    threadmutex.lock();
    // then lock globally
    loop {
        if (lock_file(mutexfname))
            return;
        Sleep(POLLTIME);
    }
}

bool NamedMutex::lockWait(unsigned timeout)
{
    unsigned t = msTick();
    // first lock locally
    if (!threadmutex.lockWait(timeout))
        return false;
    // then lock globally
    loop {
        if (lock_file(mutexfname))
            return true;
        unsigned elapsed = msTick()-t;
        if (elapsed>=timeout) {
            threadmutex.unlock();
            break;
        }
        Sleep((timeout-elapsed)>POLLTIME?POLLTIME:(timeout-elapsed));
    }
    return false;

}

void NamedMutex::unlock()
{
    // assumed held
    unlock_file(mutexfname);
    threadmutex.unlock();
}



#endif

void synchronized::throwLockException(unsigned timeout)
{
    throw MakeStringException(0,"Can not lock - %d",timeout);
}



//===========================================================================

void Monitor::wait()
{
    assertex(owner==GetCurrentThreadId());
    waiting++;
    void *cur = last;
    last = &cur;
    while (1) {
        int locked = unlockAll();
        sem->wait();
        lockAll(locked);
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
    assertex(owner==GetCurrentThreadId());
    if (waiting)
    {
        waiting--;
        sem->signal();
    }
}

void Monitor::notifyAll()
{   // should always be locked
    assertex(owner==GetCurrentThreadId());
    if (waiting)
    {
        sem->signal(waiting);
        waiting = 0;
    }
}

//==================================================================================

#ifdef USECHECKEDCRITICALSECTIONS
CheckedReadLockBlock::CheckedReadLockBlock(ReadWriteLock &l, unsigned timeout, const char *fname,unsigned lnum) : lock(l)
{
    loop
    {
        if (lock.lockRead(timeout))
            break;
        PROGLOG("CheckedReadLockBlock timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

CheckedWriteLockBlock::CheckedWriteLockBlock(ReadWriteLock &l, unsigned timeout, const char *fname,unsigned lnum) : lock(l)
{
    loop
    {
        if (lock.lockWrite(timeout))
            break;
        PROGLOG("CheckedWriteLockBlock timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

void checkedReadLockEnter(ReadWriteLock &lock, unsigned timeout, const char *fname, unsigned lnum)
{
    loop
    {
        if (lock.lockRead(timeout))
            break;
        PROGLOG("checkedReadLockEnter timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

void checkedWriteLockEnter(ReadWriteLock &lock, unsigned timeout, const char *fname, unsigned lnum)
{
    loop
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
    loop
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
    loop
    {
        if (crit.lockWait(timeout))
            break;
        PROGLOG("CheckedCriticalBlock timeout %s(%d)",fname,lnum);
        PrintStackReport();
    }
}

CheckedCriticalUnblock::~CheckedCriticalUnblock()
{ 
    loop
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
