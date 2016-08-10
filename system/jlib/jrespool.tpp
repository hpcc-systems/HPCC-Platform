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


#ifndef __JRESPOOL_TPP
#define __JRESPOOL_TPP

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jarray.hpp"
#include "jmutex.hpp"
#include "jsem.hpp"

template <typename T> interface IResourceFactory: implements IInterface
{
    virtual T* createResource() = 0;
};

// T should implement IInterface
template <typename T> class CResourcePool : public CInterface
{
public:
    CResourcePool()
    {
        numresources = 0;
        resources = NULL;
    }
    
    CResourcePool(unsigned size,IResourceFactory<T>* fac): factory(fac)
    {
        numresources = size;
        resources = (T **)calloc(sizeof(T *),numresources);
    }

    ~CResourcePool()
    {
        for (unsigned i=0;i<numresources;i++) 
            ::Release(resources[i]);
        free(resources);
    }

    void init(unsigned size,IResourceFactory<T>* fac)
    {
        CriticalBlock b(crit); 
        if(resources)
        {
            for (unsigned i=0;i<numresources;i++) 
                ::Release(resources[i]);
            free(resources);
        }
        numresources = size;
        resources = (T **)calloc(sizeof(T *),numresources);
        factory.set(fac);
    }

    T * get(long timeout=0)
    {   
        const long interval=1000;

        for(;;)
        {
            {
                CriticalBlock b(crit); 
                for (unsigned i=0;i<numresources;i++) 
                {
                    T * &it = resources[i];
                    if(it == NULL)
                        it = factory->createResource();
                    else if (it->IsShared())
                        continue;
                    it->Link();
                    return it;
                }
            }

            long slp=timeout!=INFINITE && timeout<interval ? timeout : interval;
            if(slp<=0)
                break;

            long start=msTick();
            long elapsed=sem.wait(slp) ? (msTick()-start) : slp;

            if(timeout!=INFINITE)
                timeout-=elapsed;
        }

        throw MakeStringException(1, "Run out of resources");
    }

    void release()
    {
        sem.signal();
    }
    
protected:
    CriticalSection crit;
    Semaphore sem;
    unsigned numresources;
    T **resources;
    Linked<IResourceFactory<T> > factory;
};




#endif
