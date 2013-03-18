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


#ifndef JVMEM_HPP
#define JVMEM_HPP

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
