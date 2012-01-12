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


#ifndef JMALLOC_HPP
#define JMALLOC_HPP

#include "jexpdef.hpp"
#include "jiface.hpp"

interface IAllocator: extends IInterface
{
    virtual void * allocMem(size32_t sz)=0;
    virtual void   freeMem(void *)=0;
    virtual void * reallocMem(void *prev,size32_t sz)=0;
    virtual size32_t usableSize(const void *)=0;  
    virtual memsize_t totalAllocated()=0;   // amount allocated from OS
    virtual memsize_t totalMax()=0;         // total that can be allocated from OS
    virtual memsize_t totalRemaining()=0;   // maximum remaining
    virtual void checkPtrValid(const void * ptr)=0;   // for debugging only
    virtual void logMemLeaks(bool logdata)=0; 
    virtual void * allocMem2(size32_t sz, size32_t &usablesz)=0;               // returns size actually allocated 
    virtual void * reallocMem2(void *prev,size32_t sz, size32_t &usablesz)=0;   // returns size actually allocated 
    virtual size32_t roundupSize(size32_t sz)=0; // returns usable size would round up to

};

extern jlib_decl IAllocator *createMemoryAllocator(
    memsize_t maxtotal,                         // maximum to allocate from OS
    size32_t maxsuballocsize=1024,              // set to 0 to disable suballocation
    memsize_t mintotal=0x100000*16,             // above this amount free to OS
    bool avoidReallocFragmentation=true         // whether reallocMem should avoid fragmenting memory by copying where needed 
);

extern jlib_decl IAllocator *createGuardedMemoryAllocator( // for debug only!
    memsize_t maxtotal,                         // maximum to allocate from OS
    size32_t maxsuballocsize=1024,              // set to 0 to disable suballocation
    memsize_t mintotal=0x100000*16,             // above this amount free to OS
    bool avoidReallocFragmentation=true,            // whether reallocMem should avoid fragmenting memory by copying where needed 
    size32_t guardsize= 32
);


extern jlib_decl void testAllocator();

#endif
