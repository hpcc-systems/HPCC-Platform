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
    sprintf(exbuf, "Can not lock - %d",timeout);
    throw string(exbuf);
}


