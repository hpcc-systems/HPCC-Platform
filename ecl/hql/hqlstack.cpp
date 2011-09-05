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
#include <stdlib.h>
#include <string.h>
#include "hqlstack.hpp"

FuncCallStack::FuncCallStack() {
    sp = 0;
    tos = DEFAULTSTACKSIZE;
    stackbuf = (char *)malloc(tos);
    numToFree = 0;
#ifdef __64BIT__
    numFpRegs = 0;
    for (unsigned i=0;i<MAXFPREGS;i++)
        fpRegs[i] = 0.0;
#endif
}

FuncCallStack::FuncCallStack(int size) {
    if(size < DEFAULTSTACKSIZE)
        size = DEFAULTSTACKSIZE;
    sp = 0;
    tos = size;
    stackbuf = (char*) malloc(tos);
    numToFree = 0;
#ifdef __64BIT__
    numFpRegs = 0;
    for (unsigned i=0;i<MAXFPREGS;i++)
        fpRegs[i] = 0.0;
#endif
}

FuncCallStack::~FuncCallStack() {
    if(stackbuf) {
        free(stackbuf);
    }
    
    // Free memory used by string/data parameters
    for(int i = 0; i < numToFree; i++) {
        if(toFree[i]) {
            free(toFree[i]);
        }
    }
}

unsigned FuncCallStack::getSp(){ 
    return sp;
}

char* FuncCallStack::getMem() {
    return stackbuf;
}

int FuncCallStack::push(unsigned len, const void * data) 
{
    int incsize = len;
    int inclen = align(incsize);
    assure(inclen);
    memcpy(stackbuf + sp, data, incsize);
    memset(stackbuf+sp+incsize, 0, inclen - incsize);
    sp += inclen;
    return sp;
}



int FuncCallStack::push(ITypeInfo* argType, IValue* paramValue) 
{
    unsigned len = 0;
    char* str;
    int incsize;
    int inclen;

    Owned<IValue> castParam = paramValue->castTo(argType);
    if(!castParam) {
        PrintLog("Failed to cast paramValue to argType in FuncCallStack::push");
        return -1;
    }

    switch (argType->getTypeCode()) 
    {
    case type_string:
    case type_data:
        getStringFromIValue(len, str, castParam);
        // For STRINGn, len doesn't need to be passed in.
        if(argType->getSize() == UNKNOWN_LENGTH) {
            push(sizeof(unsigned), &len);
        }
        push(sizeof(char *), &str);
        if(numToFree < MAXARGS) {
            toFree[numToFree++] = str;
        }
        break;
    case type_varstring:
        getStringFromIValue(len, str, castParam);
        push(sizeof(char *), &str);
        if(numToFree < MAXARGS) {
            toFree[numToFree++] = str;
        }
        break;
    case type_qstring:
    case type_unicode:
    case type_utf8:
        {
            unsigned argSize = castParam->getSize();
            const void * text = castParam->queryValue();
            str = (char *)malloc(argSize);
            memcpy(str, text, argSize);

            // For STRINGn, len doens't need to be passed in.
            if(argType->getSize() == UNKNOWN_LENGTH)
            {
                len = castParam->queryType()->getStringLen();
                push(sizeof(unsigned), &len);
            }
            push(sizeof(char *), &str);
            if(numToFree < MAXARGS) {
                toFree[numToFree++] = str;
            }
        }
        break;
    case type_varunicode:
        UNIMPLEMENTED;
    case type_real:
#ifdef __64BIT__
        if (numFpRegs==MAXFPREGS) {
            PrintLog("Too many floating point registers needed in FuncCallStack::push");
            return -1;
        }
        char tempbuf[MMXREGSIZE];
        castParam->toMem(tempbuf);
        if (argType->getSize()<=4)
            fpRegs[numFpRegs++] = *(float *)&tempbuf;
        else
            fpRegs[numFpRegs++] = *(double *)&tempbuf;
        break;
#else
    // fall through
#endif
    case type_boolean:
    case type_int:
    case type_decimal:
    case type_date:
    case type_char:
    case type_enumerated:
    case type_swapint:
    case type_packedint:
        incsize = argType->getSize();
        inclen = align(incsize);
        assure(inclen);
        castParam->toMem(stackbuf+sp);
        memset(stackbuf+sp+incsize, 0, inclen - incsize);
        sp += inclen;
        break;
    default:
        //code isn't here to pass sets/datasets to external functions....
        return -1;
    }

    return sp;
}

int FuncCallStack::pushPtr(void * val)
{
    return push(sizeof(void *), &val);
}

int FuncCallStack::push(char* & val) {
    return push(sizeof(char *), &val);
}


int FuncCallStack::pushRef(unsigned& val) {
    unsigned* valRef = &val;
    return push(sizeof(unsigned *), &valRef);
}

int FuncCallStack::pushRef(char*& val) {
    char** valRef = &val;
    return push(sizeof(char **), &valRef);
}

void FuncCallStack::assure(int inclen) {
    if(sp + inclen >= tos) {
        tos += INCREMENTALSIZE;
        stackbuf = (char *)realloc(stackbuf, tos);
    }
}
