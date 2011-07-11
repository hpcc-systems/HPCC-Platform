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
    double      fpRegs[MAXFPREGS];
    unsigned    numFpRegs;
#endif
public:
    FuncCallStack();
    FuncCallStack(int size);
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
