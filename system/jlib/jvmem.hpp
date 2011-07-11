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


#ifndef JVMEM_HPP
#define JVMEM_HPP

#include "jexpdef.hpp"
#include "jiface.hpp"

interface IVMAllocator: extends IInterface
{
    virtual void *alloc(size32_t sz)=0;             // allocates multiples of VMPAGESIZE (i.e. rounds up)
    virtual bool dealloc(void *p,size32_t sz)=0;    // size must be the size allocated
    virtual offset_t allocated() const = 0;         // total allocated
};

#define VMPAGESIZE     (0x1000) 
#define VMPAGEMASK     (VMPAGESIZE-1) 
#define VMPAGEROUND(s) (((s)+VMPAGEMASK)&~VMPAGEMASK)


extern  jlib_decl IVMAllocator *createVMAllocator(const char *_filename,offset_t size);


class  jlib_decl CVMLargeMemoryAllocator: public CLargeMemoryAllocator
{
    virtual void allocchunkmem();
    virtual void disposechunkmem();
    Linked<IVMAllocator> vm;
public:
    CVMLargeMemoryAllocator(
                          IVMAllocator *vm, 
                          size32_t _totalmax,
                          size32_t _chunkmin,
                          bool _throwexception);
    virtual ~CVMLargeMemoryAllocator() { reset(); } // call reset before destroyed! 
    
};



#endif
