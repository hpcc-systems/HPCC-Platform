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


#include "jlib.hpp"
#include "jsem.hpp"
#include "jisem.hpp"
#include "jmutex.hpp"

#ifndef _WIN32

#include <sys/time.h>


Semaphore::Semaphore(unsigned initialCount)
{
    init();
    count = initialCount;
}

#if 0 // not supported
Semaphore::Semaphore(const char *name)
{
    //MORE - ignores the name...
    init();
    count = 0;
}
#endif

Semaphore::~Semaphore()
{
    pthread_mutex_destroy(&mx);
    pthread_cond_destroy(&cond);
}

void Semaphore::init()
{
    pthread_mutex_init(&mx, NULL);
    pthread_cond_init(&cond, NULL);
}

void Semaphore::reinit(unsigned initialCount)
{
    pthread_mutex_lock(&mx);
    count = initialCount;
    pthread_mutex_unlock(&mx);
}

void Semaphore::wait()
{
    pthread_mutex_lock(&mx);
    if (--count<0) 
        pthread_cond_wait(&cond, &mx);
    pthread_mutex_unlock(&mx);
}

bool Semaphore::wait(unsigned timeout)
{
    if (timeout==(unsigned)-1) {
        wait();
        return true;
    }
    pthread_mutex_lock(&mx);
    if (--count<0) {
        timespec abs;
        timeval cur;
        gettimeofday(&cur, NULL);
        abs.tv_sec = cur.tv_sec + timeout/1000;
        abs.tv_nsec = (cur.tv_usec + timeout%1000*1000)*1000;
        if (abs.tv_nsec>=1000000000) {
            abs.tv_nsec-=1000000000;
            abs.tv_sec++;
        }
        if (pthread_cond_timedwait(&cond, &mx, &abs)==ETIMEDOUT) {
            count++;        // not waiting
            pthread_mutex_unlock(&mx);
            return false;
        }
    }
    pthread_mutex_unlock(&mx);
    return true;
}


void Semaphore::signal()
{
    pthread_mutex_lock(&mx);
    if (count++<0)                      // only signal if someone waiting
        pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mx);
}

void Semaphore::signal(unsigned n)
{   
    pthread_mutex_lock(&mx);
    while ((count<0)&&n) {
        count++;
        pthread_cond_signal(&cond); // this shouldn't switch til mutex unlocked
        n--;
    }
    count += n;
    pthread_mutex_unlock(&mx);
}

#endif

