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
            getEndTime(abs, timeout);
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

static CriticalBlockInstrumentation *critBlocks[10000];
static RelaxedAtomic<unsigned> nextCritBlock;

int compareCI (const void * a, const void * b) 
{
   return - (*(CriticalBlockInstrumentation **)a)->compare(*(CriticalBlockInstrumentation **)b);
}

MODULE_EXIT()
{
    unsigned numCritBlocks = nextCritBlock;
    qsort(critBlocks, numCritBlocks, sizeof(CriticalBlockInstrumentation*), compareCI);
    for (unsigned i = 0; i < numCritBlocks; i++)
    {
        critBlocks[i]->describe(i < 10);
    }
}

thread_local CriticalBlockInstrumentation *__cbinst = nullptr;

std::size_t CriticalBlockInstrumentation::StackHash::operator()(const Stack& k) const
{
    return hashc((const byte *) k.stack, sizeof(k.stack), 0);
}

std::string humanTime(unsigned long cycles)
{
    if (!cycles)
        return "0 ns";
    unsigned long v = cycle_to_nanosec(cycles);
    if (v < 1000)
        return std::to_string(v) + " ns";
    else if (v < 1000000)
        return std::to_string(v/1000) + " us";
    else
        return std::to_string(v/1000000) + " ms";
}

CriticalBlockInstrumentation::CriticalBlockInstrumentation(const char *_file, const char *_func, unsigned _line)
: file(strdup(_file)), func(strdup(_func)), line(_line), waitCycles(0), holdCycles(0), uncontended(0), contended(0)
{
    unsigned idx = nextCritBlock++;
    if (idx < 10000)
        critBlocks[idx] = this;
}

void CriticalBlockInstrumentation::addStat(cycle_t wait, cycle_t hold, bool wasContended)
{
#ifdef HAS_BACKTRACE
    void *lbtBuff[numStackEntries+3];
    unsigned nFrames = backtrace(lbtBuff, numStackEntries+3);
    NonReentrantSpinBlock b(lock);
    if (nFrames == numStackEntries+3)
    {
        Stack *a = (Stack *) (lbtBuff+3);
        stacks[*a]++;
    }
#else
    NonReentrantSpinBlock b(lock);
#endif
    if (wasContended)
        contended++;
    else
        uncontended++;
    waitCycles += wait;
    holdCycles += hold;
}

CriticalBlockInstrumentation::~CriticalBlockInstrumentation() 
{
}

void CriticalBlockInstrumentation::describe(bool includeStack) const
{
    DBGLOG("CritBlock %s (%s:%u): wait %s hold %s, entered %u times, contended %u times", func, strrchr(file, PATHSEPCHAR)+1, line, humanTime(waitCycles).c_str(), humanTime(holdCycles).c_str(), contended+uncontended, contended);
    if (includeStack)
    {
        std::vector<std::pair<Stack, int>> sorted_stacks(stacks.begin(), stacks.end());
        std::sort(sorted_stacks.begin(), sorted_stacks.end(), [](auto &left, auto &right) { return left.second > right.second; });
        unsigned printed = 0;
        for (auto &stack: sorted_stacks)
        {
            if (printed == 3)
                break;
            DBGLOG("Stack appeared %u times:", stack.second);
            char** strs = backtrace_symbols(stack.first.stack, 4);
            for (unsigned i = 0; i < 4; ++i)
                DBGLOG("%s", strs[i]);
            free(strs);
            printed++;
        }
    }
}

int CriticalBlockInstrumentation::compare(const CriticalBlockInstrumentation *other) const
{
    if (waitCycles < other->waitCycles)
        return -1;
    if (waitCycles > other->waitCycles)
        return 1;
    if (holdCycles < other->holdCycles)
        return -1;
    if (holdCycles > other->holdCycles)
        return 1;
    if (contended < other->contended)
        return -1;
    if (contended > other->contended)
        return 1;
    if (uncontended < other->uncontended)
        return -1;
    if (uncontended > other->uncontended)
        return 1;
    return 0;
}

InstrumentedCriticalBlock::InstrumentedCriticalBlock(InstrumentedCriticalSection &c) : crit(c)
{
    inst = __cbinst;
    start = get_cycles_now();
    c.enter();
    in = get_cycles_now();
}

InstrumentedCriticalBlock::~InstrumentedCriticalBlock()
{
    bool wasContended = crit.leave();
    cycle_t out = get_cycles_now();
    inst->addStat(in-start, out-in, wasContended);
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

static CriticalSection lockPrefixCS;
static StringBuffer lockPrefix;
NamedMutex::NamedMutex(const char *name)
{
    {
        CriticalBlock b(lockPrefixCS);
        if (0 == lockPrefix.length())
        {
            if (!getConfigurationDirectory(NULL, "lock", NULL, NULL, lockPrefix))
                WARNLOG("Failed to get lock directory from environment");
        }
        addPathSepChar(lockPrefix);
        lockPrefix.append("JLIBMUTEX_");
    }
    StringBuffer tmp(lockPrefix);
    tmp.append("JLIBMUTEX_").append(name);
    mutexfname = tmp.detach();
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
    for (;;) {
        if (lock_file(mutexfname))
            return;
        Sleep(MUTEX_POLLTIME);
    }
}

bool NamedMutex::lockWait(unsigned timeout)
{
    unsigned t = msTick();
    // first lock locally
    if (!threadmutex.lockWait(timeout))
        return false;
    // then lock globally
    for (;;) {
        if (lock_file(mutexfname))
            return true;
        unsigned elapsed = msTick()-t;
        if (elapsed>=timeout) {
            threadmutex.unlock();
            break;
        }
        Sleep((timeout-elapsed)>MUTEX_POLLTIME ? MUTEX_POLLTIME : (timeout-elapsed));
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
