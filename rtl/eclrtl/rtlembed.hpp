/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

#ifndef rtlembed_hpp
#define rtlembed_hpp

// NOTE - not included from generated code (see rtlfield.hpp)

#include "eclrtl.hpp"
#include "nbcd.hpp"

class NullFieldProcessor : public CInterfaceOf<IFieldProcessor>
{
public:
    NullFieldProcessor(const RtlFieldInfo * field)
    : intResult(0), uintResult(0), boolResult(false), doubleResult(0),
      unicodeResult(NULL), stringResult(NULL), resultChars(0), fieldExpected(field)
    {
        if (field && field->initializer)
        {
            field->process(field->initializer, field->initializer, *this);
            assertex(fieldExpected==NULL);
        }
    }
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        stringResult = value;
        resultChars = len;
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        boolResult = value;
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        stringResult = (const char *) value;
        resultChars = len;
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        intResult = value;
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        uintResult = value;
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        doubleResult = value;
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        checkExpected(field);
        decimalResult.setDecimal(digits, precision, value);
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        checkExpected(field);
        decimalResult.setDecimal(digits, precision, value);
    }
    virtual void processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        unicodeResult = value;
        resultChars = chars;
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        stringResult = value;
        resultChars = len;
    }
    virtual void processUtf8(unsigned chars, const char *value, const RtlFieldInfo * field)
    {
        checkExpected(field);
        stringResult = value;
        resultChars = chars;
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
    {
        UNIMPLEMENTED;
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
    {
        throwUnexpected();
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
    __int64 intResult;
    __uint64 uintResult;
    bool boolResult;
    double doubleResult;
    Decimal decimalResult;
    const char *stringResult;
    const UChar *unicodeResult;
    unsigned resultChars;
protected:
    const RtlFieldInfo * fieldExpected;
    void checkExpected(const RtlFieldInfo * field)
    {
        assertex(field==fieldExpected);
        fieldExpected = NULL;
    }

};

#endif
