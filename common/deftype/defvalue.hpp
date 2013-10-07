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

#ifndef _DEFVALUE_INCL
#define _DEFVALUE_INCL

#include "deftype.hpp"

#ifdef _WIN32
#ifdef DEFTYPE_EXPORTS
#define DEFTYPE_API __declspec(dllexport)
#else
#define DEFTYPE_API __declspec(dllimport)
#endif
#else
#define DEFTYPE_API
#endif

#define DEFVALUE_MALLOC_FAILED 701  //Unable to allocate requested memory


interface IValue : public serializable
{
public:
    virtual ITypeInfo *getType() = 0;
    virtual ITypeInfo *queryType() const = 0;
    virtual type_t getTypeCode() = 0;
    virtual size32_t getSize() = 0;

    virtual IValue * castTo(ITypeInfo * type) = 0;
    virtual const char *generateCPP(StringBuffer & out, CompilerType compiler = DEFAULT_COMPILER) = 0;
    virtual const char *generateECL(StringBuffer & out) = 0;
    virtual int compare(IValue * other) = 0;
    virtual int compare(const void *mem) = 0;
    virtual int rangeCompare(ITypeInfo * targetType) = 0;                       // can the value be represented in the type -1-too small,0-yes,+1-too big

    virtual const void *queryValue() const = 0;
    virtual bool getBoolValue() = 0;    
    virtual __int64 getIntValue() = 0;  
    virtual double getRealValue() = 0;
    virtual const char *getStringValue(StringBuffer & out) = 0; 
    virtual void * getUCharStringValue(unsigned len, void * out) = 0; //copies into buffer of outlen UChars at out; null-terminates if there is room
    virtual const char *getUTF8Value(StringBuffer & out) = 0;
    virtual const char *getCodepageValue(StringBuffer & out, char const * codepage) = 0; // codepages: see system/icu/include/codepages.txt
    virtual void pushDecimalValue() = 0;
    virtual void toMem(void * ptr) = 0;

    virtual unsigned getHash(unsigned initval)=0;
};

typedef Owned<IValue> OwnedIValue;

extern DEFTYPE_API void serializeValue(MemoryBuffer & target, IValue * value);
extern DEFTYPE_API IValue * deserializeValue(MemoryBuffer & source);

extern DEFTYPE_API void appendValueToBuffer(MemoryBuffer & mem, IValue * value);

extern DEFTYPE_API IValue * createValueFromMem(ITypeInfo * type, const void * ptr);

extern DEFTYPE_API IValue * createStringValue(const char * value, unsigned size);
extern DEFTYPE_API IValue * createStringValue(const char * value, ITypeInfo *type);
extern DEFTYPE_API IValue * createStringValue(const char *val, ITypeInfo *type, size32_t srcLength, ICharsetInfo *srcCharset);
extern DEFTYPE_API IValue * createUnicodeValue(char const * value, unsigned size, char const * locale, bool utf8, bool unescape = false); // size is length of ascii or utf8 string; locale is like fr_BE_EURO
extern DEFTYPE_API IValue * createUnicodeValue(char const * value, ITypeInfo *type); // only for ascii string, use above for utf8
extern DEFTYPE_API IValue * createUnicodeValue(char const * value, ITypeInfo *type, unsigned srclen); // only for ascii string
extern DEFTYPE_API IValue * createUnicodeValue(size32_t len, const void * text, ITypeInfo * type);
extern DEFTYPE_API IValue * createVarUnicodeValue(char const * value, unsigned size, char const * locale, bool utf8, bool unescape = false); // size is length of ascii or utf8 string; locale is like fr_BE_EURO
extern DEFTYPE_API IValue * createVarUnicodeValue(size32_t len, const void * text, ITypeInfo * type);
extern DEFTYPE_API IValue * createUtf8Value(char const * value, ITypeInfo *type);
extern DEFTYPE_API IValue * createUtf8Value(unsigned srclen, char const * value, ITypeInfo *type);
extern DEFTYPE_API IValue * createDataValue(const char * value, unsigned size);
extern DEFTYPE_API IValue * createDataValue(const char * value, ITypeInfo *type);
extern DEFTYPE_API IValue * createQStringValue(unsigned len, const char * value, ITypeInfo *type);
extern DEFTYPE_API IValue * createVarStringValue(unsigned len, const char * value, ITypeInfo *type);
extern DEFTYPE_API IValue * createVarStringValue(const char *val, ITypeInfo *type, int srcLength, ICharsetInfo * srcCharset);
extern DEFTYPE_API IValue * createPackedStringValue(const char *val, unsigned size);
extern DEFTYPE_API IValue * createPackedStringValue(const char *val, ITypeInfo *type);
extern DEFTYPE_API IValue * createCharValue(char value, bool caseSensitive);
extern DEFTYPE_API IValue * createCharValue(char value, ITypeInfo *type);
extern DEFTYPE_API IValue * createIntValue(__int64 value, unsigned size, bool isSigned);
extern DEFTYPE_API IValue * createIntValue(__int64 value, ITypeInfo * type);
extern DEFTYPE_API IValue * createTruncIntValue(__int64 value, unsigned size, bool isSigned);
extern DEFTYPE_API IValue * createTruncIntValue(__int64 value, ITypeInfo * type);
extern DEFTYPE_API IValue * createPackedIntValue(__int64 value, ITypeInfo * type);
extern DEFTYPE_API IValue * createRealValue(double value, unsigned size);
extern DEFTYPE_API IValue * createRealValue(double value, ITypeInfo * type);
extern DEFTYPE_API IValue * createDecimalValue(void * value, ITypeInfo * type);
extern DEFTYPE_API IValue * createDecimalValueFromStack(ITypeInfo * type);
extern DEFTYPE_API IValue * createBoolValue(bool value);
extern DEFTYPE_API IValue * createEnumValue(int value, ITypeInfo * type);
extern DEFTYPE_API IValue * createBitfieldValue(__int64 value, ITypeInfo * type);

extern DEFTYPE_API IValue * createMinIntValue(__int64 value);
extern DEFTYPE_API IValue * createMinUIntValue(unsigned __int64 value);

extern DEFTYPE_API IValue * addValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * subtractValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * multiplyValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * divideValues(IValue * left, IValue * right, byte dbz);
extern DEFTYPE_API IValue * modulusValues(IValue * left, IValue * right, byte dbz);
extern DEFTYPE_API IValue * powerValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * shiftLeftValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * shiftRightValues(IValue * left, IValue * right);

extern DEFTYPE_API IValue * substringValue(IValue * v, IValue *lower, IValue *higher);
extern DEFTYPE_API IValue * trimStringValue(IValue * v, char typecode);
extern DEFTYPE_API IValue * concatValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * negateValue(IValue * v);
extern DEFTYPE_API IValue * expValue(IValue * v);
extern DEFTYPE_API IValue * truncateValue(IValue * v);
extern DEFTYPE_API IValue * lnValue(IValue * v);
extern DEFTYPE_API IValue * sinValue(IValue * v);
extern DEFTYPE_API IValue * cosValue(IValue * v);
extern DEFTYPE_API IValue * tanValue(IValue * v);
extern DEFTYPE_API IValue * sinhValue(IValue * v);
extern DEFTYPE_API IValue * coshValue(IValue * v);
extern DEFTYPE_API IValue * tanhValue(IValue * v);
extern DEFTYPE_API IValue * asinValue(IValue * v);
extern DEFTYPE_API IValue * acosValue(IValue * v);
extern DEFTYPE_API IValue * atanValue(IValue * v);
extern DEFTYPE_API IValue * atan2Value(IValue * y, IValue* x);
extern DEFTYPE_API IValue * log10Value(IValue * v);
extern DEFTYPE_API IValue * sqrtValue(IValue * v);
extern DEFTYPE_API IValue * absValue(IValue * v);
extern DEFTYPE_API IValue * roundValue(IValue * v);
extern DEFTYPE_API IValue * roundToValue(IValue * v, int places);
extern DEFTYPE_API IValue * roundUpValue(IValue * v);
extern DEFTYPE_API IValue * binaryAndValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * binaryOrValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * binaryXorValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * logicalAndValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * logicalOrValues(IValue * left, IValue * right);

extern DEFTYPE_API int orderValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * equalValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * notEqualValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * lessValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * lessEqualValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * greaterValues(IValue * left, IValue * right);
extern DEFTYPE_API IValue * greaterEqualValues(IValue * left, IValue * right);
extern DEFTYPE_API void getStringFromIValue(unsigned & len, char* & str, IValue* val);
extern DEFTYPE_API unsigned getMinimumIntegerSize(unsigned __int64 value, bool isSigned);
#endif
