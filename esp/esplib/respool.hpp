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

#ifndef __RESPOOL_HPP
#define __RESPOOL_HPP

#include "jwrapper.hpp"
#include "jexcept.hpp"
#include <vector>

namespace esp
{

template <typename T> interface IResourceFactory: implements IInterface
{
    virtual T* createResource() = 0;
};

// T should implement IInterface
template <typename T> class ResourcePool : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    ResourcePool()
    {
    }
    
    ResourcePool(unsigned size,IResourceFactory<T>* fac): resources(size), factory(fac)
    {
    }

    void init(unsigned size,IResourceFactory<T>* fac)
    {
        CriticalBlock b(crit); 
        resources.clear();
        resources.resize(size);
        factory.set(fac);
    }

    Linked<T> get(long timeout=0)
    {   
        const long interval=1000;

        for(;;)
        {
            {
                CriticalBlock b(crit); 
                typename std::vector<Linked<T> >::iterator it;
                for(it=resources.begin();it!=resources.end();it++)
                {
                    if(it->get() == NULL)
                    {
                        Owned<T> e = factory->createResource();
                        if(e)
                        {
                            it->set(e.get());
                            return e;
                        }
                    }
                    else if(!it->get()->IsShared())
                    {
                        return *it;
                    }
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

    void clearOne(T* t)
    {
        if(!t)
            return;

        {
        CriticalBlock b(crit); 
        typename std::vector<Linked<T> >::iterator it;
        for(it=resources.begin();it!=resources.end();it++)
        {
            if(it->get() && it->get() == t)
            {
                it->clear();
                break;
            }
        }
        }
    }

    void clearAll()
    {
        CriticalBlock b(crit); 
        typename std::vector<Linked<T> >::iterator it;
        for(it=resources.begin();it!=resources.end();it++)
        {
            if(it->get())
            {
                it->clear();
            }
        }
    }
    
protected:
    CriticalSection crit;
    Semaphore sem;
    std::vector<Linked<T> > resources;
    Linked<IResourceFactory<T> > factory;
};



}

#endif
