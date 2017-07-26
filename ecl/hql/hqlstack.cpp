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
#include <stdlib.h>
#include <string.h>
#include "jlib.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
#include "rtlrecord.hpp"
#include "rtldynfield.hpp"
#include "hqlstack.hpp"
#include "hqlir.hpp"
#include "hqlutil.hpp"

/**
 * class CDynamicOutputMetaData
 *
 * An implementation of IOutputMetaData for use with a dynamically-created record type info structure
 *
 */

class CDynamicOutputMetaData : public COutputMetaData
{
public:
    CDynamicOutputMetaData(const RtlRecordTypeInfo & fields) : typeInfo(fields)
    {
    }

    virtual const RtlTypeInfo * queryTypeInfo() const { return &typeInfo; }
    virtual size32_t getRecordSize(const void * row)
    {
        //Allocate a temporary offset array on the stack to avoid runtime overhead.
        const RtlRecord &offsetInformation = queryRecordAccessor(true);
        unsigned numOffsets = offsetInformation.getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow offsetCalculator(offsetInformation, row, numOffsets, variableOffsets);
        return offsetCalculator.getRecordSize();
    }

    virtual size32_t getFixedSize() const
    {
        return queryRecordAccessor(true).getFixedSize();
    }
    // returns 0 for variable row size
    virtual size32_t getMinRecordSize() const
    {
        return queryRecordAccessor(true).getMinRecordSize();
    }

    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) { throwUnexpected(); }

protected:
    const RtlTypeInfo &typeInfo;
};

FuncCallStack::FuncCallStack(bool _hasMeta, int size)
{
    hasMeta = _hasMeta;
    if(size < DEFAULTSTACKSIZE)
        size = DEFAULTSTACKSIZE;
    sp = 0;
    tos = size;
    stackbuf = (char*) malloc(tos);
    numToFree = 0;
#ifdef MAXFPREGS
    numFpRegs = 0;
    for (unsigned i=0;i<MAXFPREGS;i++)
    {
#ifdef FPREG_FIXEDSIZE
        fpRegs[i] = 0.0;
#else
        fpRegs[i].d = 0.0;
        fpSizes[i] = 8;
#endif
    }
#endif
}

FuncCallStack::~FuncCallStack() {
    if(stackbuf) {
        free(stackbuf);
    }
    
    // Free memory used by string/data parameters
    for(int i = 0; i < numToFree; i++) {
        free(toFree[i]);
    }
}

unsigned FuncCallStack::align(unsigned size)
{
#ifdef ALIGN_USES_ELEMENTSIZE
    unsigned boundary = (size < ALIGNMENT) ? ALIGNMENT : size;
#else
    unsigned boundary = ALIGNMENT;
#endif
    return ((size + boundary - 1) & ~(boundary-1));
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



int FuncCallStack::push(ITypeInfo* argType, IHqlExpression* curParam)
{
    unsigned len = 0;
    char* str;
    int incsize;
    int inclen;

    IValue * paramValue = curParam->queryValue();
    Owned<IValue> castParam;
    if (paramValue) // Not all constants have a paramValue - null, all, constant records etc
    {
        castParam.setown(paramValue->castTo(argType));
        if(!castParam)
        {
            PrintLog("Failed to cast paramValue to argType in FuncCallStack::push");
            return -1;
        }
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
#ifdef MAXFPREGS
        if (numFpRegs==MAXFPREGS) {
            PrintLog("Too many floating point registers needed in FuncCallStack::push");
            return -1;
        }
        char tempbuf[sizeof(double)];
        castParam->toMem(tempbuf);
#ifdef FPREG_FIXEDSIZE
        if (argType->getSize()<=4)
            fpRegs[numFpRegs++] = *(float *)&tempbuf;
        else
            fpRegs[numFpRegs++] = *(double *)&tempbuf;
#else
        // Variable size FP registers as on arm/x64
        if (argType->getSize()<=4)
            fpRegs[numFpRegs].f = *(float *)&tempbuf;
        else
            fpRegs[numFpRegs].d = *(double *)&tempbuf;
        fpSizes[numFpRegs++] = argType->getSize();
#endif
        break;
#else
    // fall through if no hw regs used for params
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
    case type_row:
    {
        if (hasMeta)
        {
            try
            {
                pushMeta(curParam->queryRecordType());
            }
            catch (IException *E)
            {
                ::Release(E);
                return -1;
            }
        }
        if (curParam->getOperator()==no_null)
        {
            // MORE - check type matches
            MemoryBuffer out;
            createConstantNullRow(out, curParam->queryRecord());
            str = (char *) out.detach();
            push(sizeof(char *), &str);
            if(numToFree < MAXARGS)
                toFree[numToFree++] = str;
        }
        else
            return -1;
        break;
    }
    case type_record:
    {
        try
        {
            pushMeta(curParam->queryRecordType());
        }
        catch (IException *E)
        {
            ::Release(E);
            return -1;
        }
        break;
    }
    default:
        EclIR::dump_ir(curParam);
        //code isn't here to pass sets/datasets to external functions....
        return -1;
    }

    return sp;
}

int FuncCallStack::pushMeta(ITypeInfo *type)
{
    if (!deserializer)
        deserializer.setown(createRtlFieldTypeDeserializer());
    const RtlTypeInfo *typeInfo = buildRtlType(*deserializer.get(), type);
    CDynamicOutputMetaData * meta = new CDynamicOutputMetaData(* static_cast<const RtlRecordTypeInfo *>(typeInfo));
    metas.append(*meta);
    return pushPtr(meta);
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
