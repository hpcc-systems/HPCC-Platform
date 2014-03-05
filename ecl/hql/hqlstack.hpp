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
#ifndef _HQLSTACK_HPP_
#define _HQLSTACK_HPP_

#include "jlib.hpp"
#include "jstream.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "deftype.hpp"
#include "defvalue.hpp"
#include "hqlexpr.hpp"

#define DEFAULTSTACKSIZE 256
#define INCREMENTALSIZE  128
#define MAXARGS          32

#ifdef __64BIT__
#define ALIGNMENT 8
#define REGSIZE 8
#define MMXREGSIZE 8
#define MAXFPREGS 8
#else
#define ALIGNMENT 4
#endif
#define align(x)  ((x + ALIGNMENT - 1) & ~(ALIGNMENT-1))

class FuncCallStack {
private:
    unsigned   tos;
    unsigned   sp;
    char*      stackbuf;
    char*      toFree[MAXARGS];
    int        numToFree;
#ifdef __64BIT__
    // ARMFIX: If this is related to VFP registers in procedure call
    // than both ARM32 and ARM64 use it and we'll need to account for it
    double      fpRegs[MAXFPREGS];
    unsigned    numFpRegs;
#endif
public:
    FuncCallStack(int size = DEFAULTSTACKSIZE);
    virtual ~FuncCallStack();

    unsigned getSp();
    char* getMem();
#ifdef __64BIT__
    void * getFloatMem() { return numFpRegs?&fpRegs:NULL; }
#endif

    int push(ITypeInfo* argType, IValue* paramValue);
    int push(char* & val);
    int pushPtr(void * val);
    int pushRef(unsigned& val);
    int pushRef(char*& val);
    void assure(int inclen);

protected:
    int push(unsigned len, const void * data);
};



#endif
