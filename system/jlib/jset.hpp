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



#ifndef __JSET__
#define __JSET__

#include "jiface.hpp"



interface jlib_decl IBitSet : public IInterface 
{
    virtual void set(unsigned n,bool val=true)      = 0;
    virtual bool invert(unsigned n)                 = 0;            // returns inverted value
    virtual bool test(unsigned n)                   = 0;
    virtual bool testSet(unsigned n,bool val=true)  = 0;            // returns prev val
    virtual unsigned scan(unsigned from,bool tst)       = 0;        // returns index of first = val >= from
    virtual unsigned scanInvert(unsigned from,bool tst) = 0;        // like scan but inverts bit as well
    virtual void incl(unsigned lo, unsigned hi)     = 0;
    virtual void excl(unsigned lo, unsigned hi)     = 0;
    virtual void reset() = 0;
    virtual void serialize(MemoryBuffer &buffer) const = 0;
};

extern jlib_decl IBitSet *deserializeIBitSet(MemoryBuffer &mb);

// Simple BitSet // 0 based, all intermediate items exist, operations threadsafe and atomic
extern jlib_decl IBitSet *createBitSet(); 




#endif
