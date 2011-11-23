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
