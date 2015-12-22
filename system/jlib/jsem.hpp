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

#ifdef _WIN32

class jlib_decl Semaphore
{
protected:
    Semaphore(const char *name)
    {
        hSem = CreateSemaphore(NULL, 0, 0x7fffffff, name);
    }
public:
    Semaphore(unsigned initialCount = 0U)
    {
        hSem = CreateSemaphore(NULL, initialCount, 0x7fffffff, NULL);
    }


    ~Semaphore()
    {
        CloseHandle(hSem);
    }

    bool tryWait()
    {
        return (WaitForSingleObject(hSem, 0) == WAIT_OBJECT_0);
    }

    void wait()
    {
        WaitForSingleObject(hSem, INFINITE);
    }

    void reinit(unsigned initialCount=0U)
    {
        CloseHandle(hSem);
        hSem = CreateSemaphore(NULL, initialCount, 0x7fffffff, NULL);
    }

    bool wait(unsigned timeout)
    {
        return (WaitForSingleObject(hSem, (timeout==(unsigned)-1)?INFINITE:timeout)==WAIT_OBJECT_0);
    }

    void signal()
    {
        ReleaseSemaphore(hSem,1,NULL);
    }

    void signal(unsigned count)
    {
        ReleaseSemaphore(hSem,count,NULL);
    }
protected:
    HANDLE hSem;

};

class jlib_decl Semaphore_Array
{

public:
    Semaphore_Array(int num_sem, unsigned initialCount = 0U)
    { hSem = new HANDLE [num_sem];
      sem_count=num_sem;
        for (int i=0; i<num_sem; i++) {
            hSem[i] = CreateSemaphore(NULL, initialCount, 0x7fffffff, NULL);
        }
    }


    ~Semaphore_Array()
    { for (int i =0; i< sem_count; i++) {
          CloseHandle(hSem[i]);
        }
        delete [] hSem;
    }

    int wait_one_of()
    {
        return WaitForMultipleObjects(sem_count,hSem, false, INFINITE);
    }

    int wait_one_of(unsigned timeout)
    {
        return WaitForMultipleObjects(sem_count,hSem, false, (timeout==(unsigned)-1)?INFINITE:timeout);
    }

    void signal(int i)
    {
        ReleaseSemaphore(hSem[i],1,NULL);
    }

    void signal(int i,unsigned count)
    {
        ReleaseSemaphore(hSem[i],count,NULL);
    }
protected:
    HANDLE * hSem;
    int sem_count;

};
#else

#include <semaphore.h>

#ifdef __APPLE__
 // sem_timedwait is not available in OSX, so continue to use old code
 #define USE_OLD_SEMAPHORE_CODE
#endif

class jlib_decl Semaphore
{
public:
    Semaphore(unsigned initialCount=0U);
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
};

#endif

#endif
