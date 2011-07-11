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



#ifndef __JSEM__
#define __JSEM__

#include "jexpdef.hpp"

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

    void wait()
    {
        WaitForSingleObject(hSem, INFINITE);
    }

    void reinit(unsigned initialCount)
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

class jlib_decl Semaphore
{
public:
    Semaphore(unsigned initialCount=0U);
    ~Semaphore();
    void wait();
    bool wait(unsigned timeout); // in msecs
    void signal();
    void signal(unsigned count);
    void reinit(unsigned initialCount=0U);
protected:
    void init();
protected:
    MutexId mx;
    pthread_cond_t cond;
    int count;
};


#endif

#endif
