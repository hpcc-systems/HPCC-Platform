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

#if defined (_ARCH_X86_64_)
 #define ALIGNMENT 8
 #define REGSIZE 8
 #define MAXFPREGS 8
 #define REGPARAMS 6
 #define EVEN_STACK_ALIGNMENT
#elif defined (_ARCH_X86_)
 #define ALIGNMENT 4
 #define REGSIZE 4
#elif defined (_ARCH_PPC64EL_)
 #define ALIGNMENT 8
 #define REGSIZE 8
 #define MAXFPREGS 8
 #define REGPARAMS 6
 #define EVEN_STACK_ALIGNMENT
#elif defined (_ARCH_ARM64_)
 #define ALIGNMENT 8
 #define ALIGN_USES_ELEMENTSIZE
 #define REGSIZE 8
 #define REGPARAMS 8
 #define ODD_STACK_ALIGNMENT
 #if defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 5)) \
     && defined(__ARM_EABI__) && !defined(__ARM_PCS_VFP) && !defined(__ARM_PCS)
  #error "Can't identify floating point calling conventions.\nPlease ensure that your toolchain defines __ARM_PCS or __ARM_PCS_VFP."
 #endif
 #if defined(__ARM_PCS_VFP)
  #define MAXFPREGS 8 // d0-d7
 #endif
#elif defined (_ARCH_ARM32_)
 #define ALIGNMENT 4
 #define ALIGN_USES_ELEMENTSIZE
 #define REGSIZE 4
 #define REGPARAMS 4
 #define ODD_STACK_ALIGNMENT
 #if defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 5)) \
     && defined(__ARM_EABI__) && !defined(__ARM_PCS_VFP) && !defined(__ARM_PCS)
  #error "Can't identify floating point calling conventions.\nPlease ensure that your toolchain defines __ARM_PCS or __ARM_PCS_VFP."
 #endif
 #if defined(__ARM_PCS_VFP)
  #define MAXFPREGS 8 // Floating point parameters passed in v0-v7
 #endif
#endif

class FuncCallStack {
private:
    unsigned   tos;
    unsigned   sp;
    char*      stackbuf;
    char*      toFree[MAXARGS];
    int        numToFree;
#ifdef MAXFPREGS
 #ifdef FPREG_FIXEDSIZE
    double      fpRegs[MAXFPREGS];
 #else
    union {
        double d;
        float f;
    } fpRegs[MAXFPREGS];
    unsigned fpSizes[MAXFPREGS];
 #endif
    unsigned    numFpRegs;
#endif
    unsigned align(unsigned size);
public:
    FuncCallStack(int size = DEFAULTSTACKSIZE);
    virtual ~FuncCallStack();

    unsigned getSp();
    char* getMem();
#ifdef MAXFPREGS
    void * getFloatMem() { return numFpRegs?&fpRegs:NULL; }
 #ifndef FPREG_FIXEDSIZE
    unsigned *getFloatSizes() { return numFpRegs?fpSizes:NULL; }
 #endif
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
