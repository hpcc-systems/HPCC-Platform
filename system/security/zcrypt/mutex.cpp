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

#include "mutex.ipp"

#include <stdio.h>
#include <assert.h>
#include <string>
using namespace std;

#ifndef _WIN32
#include <pthread.h>

ZMutex::ZMutex()
{
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&lock_free, NULL);
    owner = 0;
    lockcount = 0;
}

ZMutex::~ZMutex()
{
    pthread_cond_destroy(&lock_free);
    pthread_mutex_destroy(&mutex);
}

void ZMutex::lock()
{
    int rc = pthread_mutex_lock(&mutex);
    if (rc != 0)
        assert(!"Unexpected error in ZMutex pthread_mutex_lock()");
    while ((owner!=0) && !pthread_equal(owner, pthread_self()))
    {
        rc = pthread_cond_wait(&lock_free, &mutex);
        if (rc != 0)
            assert(!"Unexpected error in ZMutex pthread_cond_wait()");
    }
    if (lockcount++==0)
        owner = pthread_self();
    rc = pthread_mutex_unlock(&mutex);
    if (rc != 0)
        assert(!"Unexpected error in ZMutex pthread_mutex_unlock()");
}

bool ZMutex::lockWait(long timeout)
{
    if (timeout==-1) {
        lock();
        return true;
    }
    timespec abs;
    timeval cur;
    gettimeofday(&cur, NULL);
    abs.tv_sec = cur.tv_sec + timeout/1000;
    abs.tv_nsec = (cur.tv_usec + timeout%1000*1000)*1000;
    if (abs.tv_nsec>=1000000000) {
        abs.tv_nsec-=1000000000;
        abs.tv_sec++;
    }
    int rc = pthread_mutex_lock(&mutex);
    if (rc != 0)
        assert(!"Unexpected error in ZMutex pthread_mutex_lock()");
    while ((owner!=0) && !pthread_equal(owner, pthread_self())) {
        if (pthread_cond_timedwait(&lock_free, &mutex, &abs)==ETIMEDOUT) {
            rc = pthread_mutex_unlock(&mutex);
            if (rc != 0)
                assert(!"Unexpected error in ZMutex pthread_mutex_unlock()");
            return false;
        }
    }
    if (lockcount++==0)
        owner = pthread_self();
    rc = pthread_mutex_unlock(&mutex);
    if (rc != 0)
        assert(!"Unexpected error in ZMutex pthread_mutex_unlock()");
    return true;
}

void ZMutex::unlock()
{
    int rc = pthread_mutex_lock(&mutex);
    if (rc != 0)
        assert(!"Unexpected error in ZMutex pthread_mutex_lock()");
    if (--lockcount==0)
    {
        rc = pthread_cond_signal(&lock_free);
        if (rc != 0)
            assert(!"Unexpected error in ZMutex pthread_cond_signal()");
        owner = 0;
    }
    rc = pthread_mutex_unlock(&mutex);
    if (rc != 0)
        assert(!"Unexpected error in ZMutex pthread_mutex_unlock()");
}

void ZMutex::lockAll(int count)
{
    if (count) {
        int rc = pthread_mutex_lock(&mutex);
        if (rc != 0)
            assert(!"Unexpected error in ZMutex pthread_mutex_lock()");
        while ((owner!=0) && !pthread_equal(owner, pthread_self()))
            pthread_cond_wait(&lock_free, &mutex);
        lockcount = count;
        owner = pthread_self();
        rc = pthread_mutex_unlock(&mutex);
        if (rc != 0)
            assert(!"Unexpected error in ZMutex pthread_mutex_unlock()");
    }
}

int ZMutex::unlockAll()
{
    int rc = pthread_mutex_lock(&mutex);
    if (rc != 0)
        assert(!"Unexpected error in ZMutex pthread_mutex_lock()");
    int ret = lockcount;
    if (lockcount!=0) {
        rc = pthread_cond_signal(&lock_free);
        if (rc != 0)
            assert(!"Unexpected error in ZMutex pthread_cond_signal()");
        lockcount = 0;
        owner = 0;
    }
    rc = pthread_mutex_unlock(&mutex);
    if (rc != 0)
        assert(!"Unexpected error in ZMutex pthread_mutex_unlock()");
    return ret;
}


#endif

void zsynchronized::throwLockException(unsigned timeout)
{
    char exbuf[256];
    sprintf(exbuf, "Can not lock - %d",timeout);
    throw string(exbuf);
}


