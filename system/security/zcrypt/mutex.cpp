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
    pthread_mutex_lock(&mutex);
    while ((owner!=0) && !pthread_equal(owner, pthread_self()))
        pthread_cond_wait(&lock_free, &mutex);
    if (lockcount++==0)
        owner = pthread_self();
    pthread_mutex_unlock(&mutex);
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
    pthread_mutex_lock(&mutex);
    while ((owner!=0) && !pthread_equal(owner, pthread_self())) {
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

void ZMutex::unlock()
{
    pthread_mutex_lock(&mutex);
    if (--lockcount==0)
    {
        pthread_cond_signal(&lock_free);
        owner = 0;
    }
    pthread_mutex_unlock(&mutex);
}

void ZMutex::lockAll(int count)
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

int ZMutex::unlockAll()
{
    pthread_mutex_lock(&mutex);
    int ret = lockcount;
    if (lockcount!=0) {
        pthread_cond_signal(&lock_free);
        lockcount = 0;
        owner = 0;
    }
    pthread_mutex_unlock(&mutex);
    return ret;
}


#endif

void zsynchronized::throwLockException(unsigned timeout)
{
    char exbuf[256];
    snprintf(exbuf,256,"Can not lock - %d",timeout);
    throw string(exbuf);
}


