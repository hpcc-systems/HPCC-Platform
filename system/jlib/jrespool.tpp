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
template <typename T> class CResourcePool : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

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
