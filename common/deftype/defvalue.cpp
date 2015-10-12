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

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"

#include "deftype.ipp"
#include "defvalue.ipp"

#include <stdio.h>
#include <math.h>
#include <limits.h>

#include "rtlbcd.hpp"
#include "eclrtl_imp.hpp"

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #undef new
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

BoolValue *BoolValue::trueconst;
BoolValue *BoolValue::falseconst;
static IAtom * asciiAtom;
static IAtom * ebcdicAtom;

MODULE_INIT(INIT_PRIORITY_DEFVALUE)
{
    asciiAtom = createLowerCaseAtom("ascii");
    ebcdicAtom = createLowerCaseAtom("ebcdic");
    BoolValue::trueconst = new BoolValue(true);
    BoolValue::falseconst = new BoolValue(false);
    return true;
}
MODULE_EXIT()
{
    ::Release(BoolValue::trueconst);
    ::Release(BoolValue::falseconst);
}

union RealUnion
{
    float            r4;
    double           r8;
};

//===========================================================================

static double powerOfTen[] = {1e0, 1e1, 1e2, 1e3, 1e4,
                       1e5, 1e6, 1e7, 1e8, 1e9, 
                       1e10, 1e11, 1e12, 1e13, 1e14,
                       1e15, 1e16, 1e17, 1e18, 1e19, 
                       1e20, 1e21, 1e22, 1e23, 1e24,
                       1e25, 1e26, 1e27, 1e28, 1e29, 
                       1e30, 1e31, 1e32 };

static unsigned __int64 maxUIntValue[]  =   { I64C(0), I64C(0xFF), I64C(0xFFFF), I64C(0xFFFFFF), I64C(0xFFFFFFFF), I64C(0xFFFFFFFFFF), I64C(0xFFFFFFFFFFFF), I64C(0xFFFFFFFFFFFFFF), U64C(0xFFFFFFFFFFFFFFFF) };
static __int64 maxIntValue[] =  { I64C(0), I64C(0x7F), I64C(0x7FFF), I64C(0x7FFFFF), I64C(0x7FFFFFFF), I64C(0x7FFFFFFFFF), I64C(0x7FFFFFFFFFFF), I64C(0x7FFFFFFFFFFFFF), I64C(0x7FFFFFFFFFFFFFFF) };

int rangeCompare(double value, ITypeInfo * targetType)
{
    if (targetType->isSigned())
    {
        switch (targetType->getTypeCode())
        {
        case type_decimal:
            if (value <= -powerOfTen[targetType->getDigits()-targetType->getPrecision()])
                return -1;
            if (value >= powerOfTen[targetType->getDigits()-targetType->getPrecision()])
                return +1;
            break;
        case type_int:
        case type_swapint:
            if (value < 0)
            {
                if (-value > (double)maxIntValue[targetType->getSize()]+1)
                    return -1;
            }
            if (value > (double)maxIntValue[targetType->getSize()])
                return +1;
            break;
        case type_packedint:
            return rangeCompare(value, targetType->queryPromotedType());
        }
    }
    else
    {
        if (value < 0)
            return -1;
        switch (targetType->getTypeCode())
        {
        case type_decimal:
            if (value >= powerOfTen[targetType->getDigits()-targetType->getPrecision()])
                return +1;
            break;
        case type_int:
        case type_swapint:
            if (value > (double)maxUIntValue[targetType->getSize()])
                return +1;
            break;
        case type_packedint:
            return rangeCompare(value, targetType->queryPromotedType());
        case type_bitfield:
            {
                unsigned __int64 maxValue = (1 << targetType->getBitSize()) - 1;
                if (value > maxValue)
                    return +1;
                break;
            }
        }
    }
    return 0;
}

inline unsigned getTargetLength(ITypeInfo * type, unsigned dft)
{
    unsigned length = type->getStringLen();
    if (length == UNKNOWN_LENGTH)
        length = dft;
    return length;
}

//===========================================================================

CValue::CValue(ITypeInfo *_type)
{
    type = _type;
    assertThrow(type);
}

CValue::~CValue()
{
    type->Release();
}

int CValue::compare(const void *mem)
{
    type->Link();
    IValue *to = createValueFromMem(type, mem);
    int ret = compare(to);
    to->Release();
    return ret;
}

double CValue::getRealValue()
{
    return (double)getIntValue();
}

type_t CValue::getTypeCode(void)
{
    return type->getTypeCode();
}

size32_t CValue::getSize()
{
    return type->getSize();
}

ITypeInfo *CValue::queryType() const
{
    return type;
}

ITypeInfo *CValue::getType()
{
    return LINK(type);
}

IValue * CValue::doCastTo(unsigned osize, const char * text, ITypeInfo *t)
{
    assertex(t == queryUnqualifiedType(t));
    switch (t->getTypeCode())
    {
    case type_string:
        return createStringValue(text, LINK(t), osize, type->queryCharset());
    case type_varstring:
        return createVarStringValue(text, LINK(t), osize, type->queryCharset());
    case type_data:
        return t->castFrom(osize, text);        //NB: Must not go through translation in default case
    case type_char:
        return createCharValue(text[0], LINK(t));
    case type_int:
    case type_swapint:
    case type_packedint:
        break;  // treat these using the default action in the type.
    case type_boolean:
        return createBoolValue(getBoolValue());
    case type_unicode:
    case type_varunicode:
        {
            rtlDataAttr buff(osize * sizeof(UChar));
            UChar * target = buff.getustr();
            rtlCodepageToUnicode(osize, target, osize, text, type->queryCharset()->queryCodepageName());
            return t->castFrom(osize, target);
        }
    case type_utf8:
        {
            unsigned tLen = getTargetLength(t, osize);
            rtlDataAttr buff(tLen * 4);
            char * target = buff.getstr();
            rtlCodepageToUtf8(tLen, target, osize, text, type->queryCharset()->queryCodepageName());
            return createUtf8Value(tLen, target, LINK(t));
        }
    }

    Owned<ICharsetInfo> charset = getCharset(asciiAtom);
    ITranslationInfo * translation = queryDefaultTranslation(charset, type->queryCharset());
    if (translation)
    {
        StringBuffer translated;
        translation->translate(translated, osize, text);
        return t->castFrom(osize, translated.str());
    }
    return t->castFrom(osize, text);
}

//===========================================================================

VarStringValue::VarStringValue(unsigned len, const char *v, ITypeInfo *_type) : CValue(_type)
{
    unsigned typeLen = type->getStringLen();
    assertex(typeLen != UNKNOWN_LENGTH);
    if (len >= typeLen)
        val.set(v, typeLen);
    else
    {
        char * temp = (char *)checked_malloc(typeLen+1, DEFVALUE_MALLOC_FAILED);
        memcpy(temp, v, len);
        temp[len] = 0;
        val.set(temp, typeLen);
        free(temp);
    }
}

const char *VarStringValue::generateECL(StringBuffer &out)
{
    return appendStringAsQuotedECL(out, val.length(), val).str();
}

const char *VarStringValue::generateCPP(StringBuffer &out, CompilerType compiler)
{
    unsigned len = val.length();
    return appendStringAsQuotedCPP(out, len, val.get(), len>120).str();
}

void VarStringValue::toMem(void *target)
{
    unsigned copyLen = val.length();
    unsigned typeLen = type->getSize();
    //assertThrow(typeLen);
    if(typeLen > 0) {
        if (copyLen >= typeLen)
            copyLen = typeLen - 1;
    
        memcpy(target, val.get(), copyLen);
        memset((char *)target+copyLen, 0, (typeLen-copyLen));
    }
    else {
        memcpy(target, val.get(), copyLen + 1);
    }
}

unsigned VarStringValue::getHash(unsigned initval)
{
    return hashc((unsigned char *) val.get(), val.length(), initval);
}

int VarStringValue::compare(IValue *_to)
{
    assertThrow(_to->getTypeCode()==type->getTypeCode());
    VarStringValue *to = (VarStringValue *) _to;
    return rtlCompareVStrVStr(val.get(), to->val.get());
}

int VarStringValue::compare(const void *mem)
{
    return rtlCompareVStrVStr(val.get(), (const char *)mem);
}

IValue *VarStringValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    return doCastTo(val.length(), (const char *)val.get(), t);
}

const void *VarStringValue::queryValue() const
{
    return (const void *) val.get();
}

bool VarStringValue::getBoolValue()
{
    return val.length() != 0;
}

__int64 VarStringValue::getIntValue()
{
    return rtlVStrToInt8(val.get());
}

const char *VarStringValue::getStringValue(StringBuffer &out)
{
    out.append(val);
    return out.str();
}

void VarStringValue::pushDecimalValue(void)
{
    DecPushCString((char*)val.get());
}

void VarStringValue::serialize(MemoryBuffer &tgt)
{
    tgt.append(val);
}

void VarStringValue::deserialize(MemoryBuffer &src)
{
    src.read(val);
}

IValue * createVarStringValue(unsigned len, const char * value, ITypeInfo *type)
{
    if (type->getSize() != UNKNOWN_LENGTH)
        return new VarStringValue(len, value, type);
    ITypeInfo * newType = getStretchedType(len, type);
    type->Release();
    return new VarStringValue(len, value, newType);
}


IValue *createVarStringValue(const char *val, ITypeInfo *type, int srcLength, ICharsetInfo * srcCharset)
{
    ITranslationInfo * translation = queryDefaultTranslation(type->queryCharset(), srcCharset);
    if(translation)
    {
        StringBuffer translated;
        translation->translate(translated, srcLength, val);
        return createVarStringValue(srcLength, translated.str(), type);
    }
    else
        return createVarStringValue(srcLength, val, type);
}

int VarStringValue::rangeCompare(ITypeInfo * targetType)
{
    //MORE - should probably check for unpadded length, and check the type as well.
    return 0;
}

//===========================================================================

MemoryValue::MemoryValue(const void *v, ITypeInfo *_type) : CValue(_type)
{
    assertex(_type->getSize() != UNKNOWN_LENGTH);
    val.set(_type->getSize(), v);
}

MemoryValue::MemoryValue(ITypeInfo *_type) : CValue(_type)
{
}

void MemoryValue::toMem(void *target)
{
    size32_t size = getSize();
    assertThrow(val.length()>=size);
    memcpy(target, val.get(), size);
}

unsigned MemoryValue::getHash(unsigned initval)
{
    size32_t size = getSize();
    assertThrow(val.length()>=size);
    return hashc((unsigned char *) val.get(), size, initval);
}

int MemoryValue::compare(IValue * to)
{
    ITypeInfo * toType = to->queryType();
    assertex(toType->getTypeCode()==type->getTypeCode());
    return rtlCompareDataData(type->getSize(), (const char *)val.get(), toType->getSize(), (const char *)to->queryValue());
}

int MemoryValue::compare(const void *mem)
{
    return memcmp(val.get(), (const char *) mem, type->getSize());
}

const char *MemoryValue::generateCPP(StringBuffer &out, CompilerType compiler)
{
    unsigned size = getSize();
    return appendStringAsQuotedCPP(out, size, (const char *)val.get(), size>120).str();
}

bool MemoryValue::getBoolValue()
{
    //return true if any non zero character
    const char * cur = (const char *)val.get();
    unsigned len = getSize();
    while (len--)
    {
        if (*cur++ != 0)
            return true;
    }
    return false;
}

void MemoryValue::pushDecimalValue()
{
    DecPushUlong(0);
}

void MemoryValue::serialize(MemoryBuffer &tgt)
{
    tgt.append(val.length());
    tgt.append(val.length(), val.get());
}

void MemoryValue::deserialize(MemoryBuffer &src)
{
    size32_t size;
    src.read(size);
    void *mem = checked_malloc(size, DEFVALUE_MALLOC_FAILED);
    assertex(mem);
    src.read(size, mem);
    val.setOwn(size, mem);
}

int MemoryValue::rangeCompare(ITypeInfo * targetType)
{
    //MORE - should probably check for unpadded length, and check the type as well.
    return 0;
}

//===========================================================================

StringValue::StringValue(const char *v, ITypeInfo *_type) : MemoryValue(_type)
{
    //store a null terminated string for ease of conversion
    unsigned len = _type->getSize();
    assertex(len != UNKNOWN_LENGTH);
    char * temp = (char *)val.allocate(len+1);
    memcpy(temp, v, len);
    temp[len] = 0;
}

const char *StringValue::generateECL(StringBuffer &out)
{
    return appendStringAsQuotedECL(out, type->getSize(), (const char *)val.get()).str();
}

int StringValue::compare(IValue * to)
{
    ITypeInfo * toType = to->queryType();
    assertex(toType->getTypeCode()==type->getTypeCode());
    return rtlCompareStrStr(type->getSize(), (const char *)val.get(), toType->getSize(), (const char *)to->queryValue());
}

int StringValue::compare(const void *mem)
{
//  if (type->isCaseSensitive())
        return memcmp(val.get(), (const char *) mem, type->getSize());
//  return memicmp(val.get(), (const char *) mem, type->getSize());
}


IValue *StringValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    const char * str = (const char *)val.get();
    if (tc == type_data)
        return t->castFrom(type->getSize(), str);        //NB: Must not go through translation in default case

    ICharsetInfo * srcCharset = type->queryCharset();
    Owned<ICharsetInfo> asciiCharset = getCharset(asciiAtom);
    if (queryDefaultTranslation(asciiCharset, srcCharset))
    {
        Owned<ITypeInfo> asciiType = getAsciiType(type);
        Owned<IValue> asciiValue = createStringValue(str, LINK(asciiType), type->getStringLen(), srcCharset);
        return asciiValue->castTo(t);
    }

    return doCastTo(type->getStringLen(), str, t);
}

const void *StringValue::queryValue() const
{
    return (const void *) val.get();
}

bool StringValue::getBoolValue()
{
    //return true if any character is non space
    //return true if any non zero character
    const char * cur = (const char *)val.get();
    unsigned len = type->getSize();
    char fill = type->queryCharset()->queryFillChar();
    while (len--)
    {
        if (*cur++ != fill)
            return true;
    }
    return false;
}

__int64 StringValue::getIntValue()
{
    return rtlStrToInt8(val.length(), (const char *)val.get());
}

const char *StringValue::getStringValue(StringBuffer &out)
{
    out.append(type->getSize(), (const char *)val.get());
    return out.str();
}

const char *StringValue::getUTF8Value(StringBuffer &out)
{
    ICharsetInfo * srcCharset = type->queryCharset();
    Owned<ICharsetInfo> asciiCharset = getCharset(asciiAtom);
    if (queryDefaultTranslation(asciiCharset, srcCharset))
    {
        Owned<ITypeInfo> asciiType = getAsciiType(type);
        Owned<IValue> asciiValue = castTo(asciiType);
        return asciiValue->getUTF8Value(out);
    }

    rtlDataAttr temp;
    unsigned bufflen;
    rtlStrToUtf8X(bufflen, temp.refstr(), type->getSize(), (const char *)val.get());
    out.append(rtlUtf8Size(bufflen, temp.getstr()), temp.getstr());
    return out.str();
}

void StringValue::pushDecimalValue()
{
    DecPushString(type->getSize(),(char *)val.get());
}

IValue *createStringValue(const char *val, unsigned size)
{
    return createStringValue(val, makeStringType(size, NULL, NULL));
}

IValue *createStringValue(const char *val, ITypeInfo *type)
{
    assertex(type->getSize() != UNKNOWN_LENGTH);
    if (type->getSize() == UNKNOWN_LENGTH)
    {
        ITypeInfo * newType = getStretchedType((size32_t)strlen(val), type);
        type->Release();
        return new StringValue(val, newType);
    }
    return new StringValue(val, type);
}

IValue *createStringValue(const char *val, ITypeInfo *type, size32_t srcLength, ICharsetInfo *srcCharset)
{
    ITranslationInfo * translation = queryDefaultTranslation(type->queryCharset(), srcCharset);
    size32_t tgtLength = type->getSize();
    if (tgtLength == UNKNOWN_LENGTH)
    {
        ITypeInfo * newType = getStretchedType(srcLength, type);
        type->Release();
        type = newType;
        tgtLength = srcLength;
    }
    if(translation)
    {
        StringBuffer translated;
        translation->translate(translated, srcLength, val);
        if(tgtLength > srcLength)
            translated.appendN(tgtLength-srcLength, type->queryCharset()->queryFillChar());
        return new StringValue(translated.str(), type);
    }
    else if (tgtLength > srcLength)
    {
        char * extended = (char *)checked_malloc(tgtLength, DEFVALUE_MALLOC_FAILED);
        memcpy(extended, val, srcLength);
        memset(extended+srcLength, type->queryCharset()->queryFillChar(), tgtLength-srcLength);
        IValue * ret = new StringValue(extended, type);
        free(extended);
        return ret;
    }
    else
        return new StringValue(val, type);

}

//==========================================================================

UnicodeValue::UnicodeValue(UChar const * _value, ITypeInfo * _type) : MemoryValue(_type)
{
    unsigned len = _type->getSize();
    assertex(len != UNKNOWN_LENGTH);
    val.set(len, _value);
}

const void *UnicodeValue::queryValue() const
{
    return (const void *) val.get();
}

const char *UnicodeValue::generateCPP(StringBuffer &out, CompilerType compiler)
{
    out.append("((UChar *)");
    MemoryValue::generateCPP(out, compiler);
    return out.append(")").str();
}

const char *UnicodeValue::generateECL(StringBuffer &out)
{
    char * buff;
    unsigned bufflen;
    rtlUnicodeToUtf8X(bufflen, buff, type->getStringLen(), (UChar const *)val.get());
    out.append("U'");
    appendUtf8AsECL(out, rtlUtf8Size(bufflen, buff), buff);
    out.append('\'');
    rtlFree(buff);
    return out.str();
}

IValue *UnicodeValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    size32_t olen = type->getStringLen();
    UChar const * uchars = (const UChar *)val.get();

    switch (tc)
    {
    case type_unicode:
        if ((t->getSize() == UNKNOWN_LENGTH) && (type->queryLocale() == t->queryLocale()))
            return LINK(this);
        return createUnicodeValue(uchars, olen, LINK(t));
    case type_string:
        {
            char * buff;
            unsigned bufflen;
            rtlUnicodeToCodepageX(bufflen, buff, olen, uchars, t->queryCharset()->queryCodepageName());
            Owned<IValue> temp = createStringValue(buff, makeStringType(bufflen, LINK(t->queryCharset()), 0));
            rtlFree(buff);
            return temp->castTo(t);
        }
    }

    return t->castFrom(olen, uchars);
}

int UnicodeValue::compare(IValue * to)
{
    ITypeInfo * toType = to->queryType();
    assertex(toType->getTypeCode()==type->getTypeCode());
    assertex(haveCommonLocale(type, toType));
    char const * locale = str(getCommonLocale(type, toType));
    return rtlCompareUnicodeUnicode(type->getStringLen(), (const UChar *)val.get(), toType->getStringLen(), (const UChar *)to->queryValue(), locale);
}

int UnicodeValue::compare(const void *mem)
{
    size32_t len = type->getStringLen();
    return rtlCompareUnicodeUnicode(len, (const UChar *)val.get(), len, (const UChar *)mem, str(type->queryLocale()));
}

bool UnicodeValue::getBoolValue()
{
    return rtlUnicodeToBool(type->getStringLen(), (UChar const *)val.get());
}

__int64 UnicodeValue::getIntValue()
{
    return rtlUnicodeToInt8(type->getStringLen(), (UChar const *)val.get());
}

void UnicodeValue::pushDecimalValue()
{
    rtlDecPushUnicode(type->getStringLen(), (UChar const *)val.get());
}

const char *UnicodeValue::getStringValue(StringBuffer &out)
{
    return getCodepageValue(out, "US-ASCII");
}

void * UnicodeValue::getUCharStringValue(unsigned len, void * out)
{
    unsigned vallen = val.length()/2;
    if(vallen > len)
        vallen = len;
    memcpy(out, val.get(), vallen*2);
    if(len > vallen)
        ((UChar *)out)[vallen] = 0x0000;
    return out;
}

const char *UnicodeValue::getUTF8Value(StringBuffer &out)
{
    return getCodepageValue(out, "UTF-8");
}

const char *UnicodeValue::getCodepageValue(StringBuffer &out, char const * codepage)
{
    char * buff;
    unsigned bufflen;
    rtlUnicodeToCodepageX(bufflen, buff, val.length()/2, (UChar const *)val.get(), codepage);
    out.append(bufflen, buff);
    rtlFree(buff);
    return out.str();
}

IValue *createUnicodeValue(char const * value, unsigned size, char const * locale, bool utf8, bool unescape)
{
    UChar * buff = 0;
    unsigned bufflen = 0;
    if(unescape)
        rtlCodepageToUnicodeXUnescape(bufflen, buff, size, value, utf8 ? "UTF-8" : "US-ASCII");
    else
        rtlCodepageToUnicodeX(bufflen, buff, size, value, utf8 ? "UTF-8" : "US-ASCII");
    IValue * ret = new UnicodeValue(buff, makeUnicodeType(bufflen, createLowerCaseAtom(locale)));
    rtlFree(buff);
    return ret;
}

IValue *createUtf8Value(size32_t len, char const * value, char const * locale, bool unescape)
{
    if (unescape)
    {
        rtlDataAttr temp;
        size32_t newlen = 0;
        size32_t size = rtlUtf8Size(len, value);
        rtlCodepageToUtf8XUnescape(newlen, temp.refstr(), size, value, "UTF-8");

        ITypeInfo * type = makeUtf8Type(newlen, createLowerCaseAtom(locale));
        return createUtf8Value(temp.getstr(), type);
    }
    else
    {
        ITypeInfo * type = makeUtf8Type(len, createLowerCaseAtom(locale));
        return createUtf8Value(value, type);
    }
}

IValue *createUnicodeValue(char const * value, ITypeInfo * type)
{
    if(type->getSize() == UNKNOWN_LENGTH)
    {
        type->Release();
        return createUnicodeValue(value, (size32_t)strlen(value), str(type->queryLocale()), false);
    }
    return createUnicodeValue(value, type, type->getStringLen());
}

IValue *createUnicodeValue(char const * value, ITypeInfo * type, unsigned srclen)
{
    if(type->getSize() == UNKNOWN_LENGTH)
    {
        type->Release();
        return createUnicodeValue(value, srclen, str(type->queryLocale()), false);
    }
    UChar * buff = (UChar *)checked_malloc(type->getSize(), DEFVALUE_MALLOC_FAILED);
    rtlCodepageToUnicode(type->getStringLen(), buff, srclen, value, "US-ASCII");
    IValue * ret = new UnicodeValue(buff, type);
    free(buff);
    return ret;
}

IValue * createUnicodeValue(UChar const * text, size32_t len, ITypeInfo * type)
{
    size32_t nlen = type->getStringLen();
    if(nlen == UNKNOWN_LENGTH)
    {
        ITypeInfo * newType = getStretchedType(len, type);
        type->Release();
        type = newType;
        nlen = len;
    }
    if (nlen <= len)
        return new UnicodeValue(text, type);

    UChar * buff = 0;
    unsigned bufflen = 0;
    rtlUnicodeSubStrFTX(bufflen, buff, len, text, 1, nlen);
    assertex(bufflen == nlen);
    IValue * ret = new UnicodeValue(buff, type);
    rtlFree(buff);
    return ret;
}

IValue * createUnicodeValue(size32_t len, const void * text, ITypeInfo * type)
{
    return createUnicodeValue((const UChar *)text, len, type);
}


//===========================================================================

void UnicodeAttr::set(UChar const * _text, unsigned _len)
{
    free(text);
    text = (UChar *) checked_malloc((_len+1)*2, DEFVALUE_MALLOC_FAILED);
    memcpy(text, _text, _len*2);
    text[_len] = 0x0000;
}

void UnicodeAttr::setown(UChar * _text)
{
    free(text);
    text = _text;
}

VarUnicodeValue::VarUnicodeValue(unsigned len, const UChar * v, ITypeInfo * _type) : CValue(_type)
{
    unsigned typeLen = type->getStringLen();
    assertex(typeLen != UNKNOWN_LENGTH);
    if (len >= typeLen)
        val.set(v, typeLen);
    else
    {
        UChar * temp = (UChar *)checked_malloc((typeLen+1)*2, DEFVALUE_MALLOC_FAILED);
        memcpy(temp, v, len*2);
        temp[len] = 0;
        val.set(temp, typeLen);
        free(temp);
    }
}

const void * VarUnicodeValue::queryValue() const
{
    return val.get();
}

const char * VarUnicodeValue::generateCPP(StringBuffer & out, CompilerType compiler)
{
    out.append("(UChar *)");
    unsigned len = val.length()*2+1; // not pretty, but adds one null, so that string is double-null-terminated as required
    return appendStringAsQuotedCPP(out, len, (char const *)val.get(), len>120).str();
}

const char * VarUnicodeValue::generateECL(StringBuffer & out)
{
    char * buff;
    unsigned bufflen;
    rtlUnicodeToUtf8X(bufflen, buff, val.length(), val.get());
    out.append("U'");
    appendUtf8AsECL(out, rtlUtf8Size(bufflen, buff), buff);
    out.append('\'');
    rtlFree(buff);
    return out.str();
}

IValue * VarUnicodeValue::castTo(ITypeInfo * t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    size32_t olen = val.length();
    UChar const * uchars = (UChar const *)val.get();
    switch (tc)
    {
    case type_string:
        {
            char * buff;
            unsigned bufflen;
            rtlUnicodeToCodepageX(bufflen, buff, olen, uchars, t->queryCharset()->queryCodepageName());
            Owned<IValue> temp = createStringValue(buff, makeStringType(bufflen, LINK(t->queryCharset()), 0));
            rtlFree(buff);
            return temp->castTo(t);
        }
    }

    return t->castFrom(olen, uchars);
}

int VarUnicodeValue::compare(IValue * to)
{
    ITypeInfo * toType = to->queryType();
    assertex(toType->getTypeCode()==type->getTypeCode());
    assertex(haveCommonLocale(type, toType));
    char const * locale = str(getCommonLocale(type, toType));
    UChar const * rhs = (UChar const *) to->queryValue();
    return rtlCompareUnicodeUnicode(val.length(), val.get(), rtlUnicodeStrlen(rhs), rhs, locale);
}

int VarUnicodeValue::compare(const void * mem)
{
    UChar const * rhs = (UChar const *) mem;
    return rtlCompareUnicodeUnicode(val.length(), val.get(), rtlUnicodeStrlen(rhs), rhs, str(type->queryLocale()));
}

bool VarUnicodeValue::getBoolValue()
{
    return rtlUnicodeToBool(val.length(), val.get());
}

__int64 VarUnicodeValue::getIntValue()
{
    return rtlUnicodeToInt8(val.length(), val.get());
}

const char * VarUnicodeValue::getStringValue(StringBuffer & out)
{
    return getCodepageValue(out, "US-ASCII");
}   

void * VarUnicodeValue::getUCharStringValue(unsigned len, void * out)
{
    unsigned vallen = val.length();
    if(vallen > len)
        vallen = len;
    memcpy(out, val.get(), vallen*2);
    if(len > vallen)
        ((UChar *)out)[vallen] = 0x0000;
    return out;
}

const char * VarUnicodeValue::getUTF8Value(StringBuffer & out)
{
    return getCodepageValue(out, "UTF-8");
}

const char * VarUnicodeValue::getCodepageValue(StringBuffer & out, char const * codepage)
{
    char * buff;
    unsigned bufflen;
    rtlUnicodeToCodepageX(bufflen, buff, val.length(), val.get(), codepage);
    out.append(bufflen, buff);
    rtlFree(buff);
    return out.str();
}

void VarUnicodeValue::pushDecimalValue()
{
    rtlDecPushUnicode(val.length(), val.get());
}

void VarUnicodeValue::toMem(void * target)
{
    memcpy(target, val.get(), (val.length()+1)*2);
}

unsigned VarUnicodeValue::getHash(unsigned initval)
{
    return hashc((unsigned char *) val.get(), val.length()*2, initval);
}

int VarUnicodeValue::rangeCompare(ITypeInfo * targetType)
{
    return 0;
}

void VarUnicodeValue::serialize(MemoryBuffer & tgt)
{
    tgt.append(val.length());
    tgt.append(val.length()*2, val.get());
}

void VarUnicodeValue::deserialize(MemoryBuffer & src)
{
    size32_t len;
    src.read(len);
    UChar * buff = (UChar *) checked_malloc(len*2, DEFVALUE_MALLOC_FAILED);
    src.read(len*2, buff);
    val.setown(buff);
}

IValue *createVarUnicodeValue(char const * value, unsigned size, char const * locale, bool utf8, bool unescape)
{
    UChar * buff = 0;
    unsigned bufflen = 0;
    if(unescape)
        rtlCodepageToUnicodeXUnescape(bufflen, buff, size, value, utf8 ? "UTF-8" : "US-ASCII");
    else
        rtlCodepageToUnicodeX(bufflen, buff, size, value, utf8 ? "UTF-8" : "US-ASCII");
    IValue * ret = new VarUnicodeValue(bufflen, buff, makeUnicodeType(bufflen, createLowerCaseAtom(locale)));
    rtlFree(buff);
    return ret;
}

IValue * createVarUnicodeValue(UChar const * text, size32_t len, ITypeInfo * type)
{
    size32_t nlen = type->getStringLen();
    if(nlen == UNKNOWN_LENGTH)
    {
        ITypeInfo * newType = getStretchedType(len, type);
        type->Release();
        type = newType;
        nlen = len;
    }
    if(nlen > len)
        nlen = len;

    UChar * buff = 0;
    unsigned bufflen = 0;
    rtlUnicodeSubStrFTX(bufflen, buff, len, text, 1, nlen);
    assertex(bufflen == nlen);
    IValue * ret = new VarUnicodeValue(bufflen, buff, type);
    rtlFree(buff);
    return ret;
}

IValue * createVarUnicodeValue(size32_t len, const void * text, ITypeInfo * type)
{
    return createVarUnicodeValue((UChar const *)text, len, type);
}

//==========================================================================

Utf8Value::Utf8Value(const char * _value, ITypeInfo * _type) : MemoryValue(_type)
{
    unsigned len = _type->getStringLen();
    assertex(len != UNKNOWN_LENGTH);
    unsigned size = rtlUtf8Size(len, _value);
    val.set(size, _value);
}

size32_t Utf8Value::getSize()
{
#ifdef _DEBUG
    unsigned size = rtlUtf8Size(type->getStringLen(), val.get());
    assertex(size == val.length());
#endif
    return val.length();
}

const void *Utf8Value::queryValue() const
{
    return (const void *) val.get();
}

const char *Utf8Value::generateCPP(StringBuffer &out, CompilerType compiler)
{
    out.append("((char *)");
    MemoryValue::generateCPP(out, compiler);
    return out.append(")").str();
}

const char *Utf8Value::generateECL(StringBuffer &out)
{
    unsigned size = rtlUtf8Size(type->getStringLen(), val.get());
    out.append("U");
    appendStringAsQuotedECL(out, size, (const char *)val.get());
    return out.str();
}

IValue *Utf8Value::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    size32_t olen = type->getStringLen();
    const char * data = (const char *)val.get();

    switch(tc)
    {
    case type_unicode:
    case type_varunicode:
        {
            Owned<IValue> temp = createUnicodeValue(data, rtlUtf8Size(olen, data), str(t->queryLocale()), true, false);
            return temp->castTo(t);
        }
    case type_utf8:
        {
            unsigned targetLength = t->getStringLen();
            if (targetLength == UNKNOWN_LENGTH)
            {
                if (type->queryLocale() == t->queryLocale())
                    return LINK(this);
            }
            return createUtf8Value(olen, data, LINK(t));
        }
    case type_string:
        {
            rtlDataAttr buff;
            unsigned bufflen;
            rtlUtf8ToCodepageX(bufflen, buff.refstr(), olen, data, t->queryCharset()->queryCodepageName());
            Owned<IValue> temp = createStringValue(buff.getstr(), makeStringType(bufflen, LINK(t->queryCharset()), 0));
            return temp->castTo(t);
        }
    case type_data:
        {
            unsigned size = rtlUtf8Size(olen, data);
            if (size >= t->getSize())
               return createDataValue(data, LINK(t));
            Owned<IValue> temp = createDataValue(data, size);
            return temp->castTo(t);
        }
    }

    Owned<ITypeInfo> stringType = makeStringType(UNKNOWN_LENGTH, LINK(t->queryCharset()), LINK(t->queryCollation()));
    Owned<IValue> stringValue = castTo(stringType);
    return stringValue->castTo(t);
}

int Utf8Value::compare(IValue * to)
{
    ITypeInfo * toType = to->queryType();
    assertex(toType->getTypeCode()==type->getTypeCode());
    assertex(haveCommonLocale(type, toType));
    char const * locale = str(getCommonLocale(type, toType));
    return rtlCompareUtf8Utf8(type->getStringLen(), (const char *)val.get(), toType->getStringLen(), (const char *)to->queryValue(), locale);
}

int Utf8Value::compare(const void *mem)
{
    size32_t len = type->getStringLen();
    return rtlCompareUtf8Utf8(len, (const char *)val.get(), len, (const char *)mem, str(type->queryLocale()));
}

bool Utf8Value::getBoolValue()
{
    return rtlUtf8ToBool(type->getStringLen(), (const char *)val.get());
}

__int64 Utf8Value::getIntValue()
{
    return rtlUtf8ToInt(type->getStringLen(), (const char *)val.get());
}

void Utf8Value::pushDecimalValue()
{
    rtlDecPushUtf8(type->getStringLen(), (const char *)val.get());
}

const char * Utf8Value::getStringValue(StringBuffer &out)
{
    return getCodepageValue(out, "US-ASCII");
}

const char * Utf8Value::getCodepageValue(StringBuffer &out, char const * codepage)
{
    unsigned bufflen;
    rtlDataAttr buff;
    rtlUtf8ToCodepageX(bufflen, buff.refstr(), type->getStringLen(), (const char *)val.get(), codepage);
    out.append(bufflen, buff.getstr());
    return out.str();
}

void * Utf8Value::getUCharStringValue(unsigned len, void * _out)
{
    unsigned thisLen = type->getStringLen();
    UChar * out = (UChar *)_out;
    rtlUtf8ToUnicode(len, out, thisLen, (const char *)val.get());
    //wierd semantics.
    if (len > thisLen)
        out[thisLen] = 0x0000;
    return out;
}

const char *Utf8Value::getUTF8Value(StringBuffer &out)
{
    unsigned size = rtlUtf8Size(type->getStringLen(), val.get());
    return out.append(size, (const char *)val.get()).str();
}

IValue * createUtf8Value(const char * text, ITypeInfo * type)
{
    return new Utf8Value(text, type);
}

IValue * createUtf8Value(size32_t len, const char * text, ITypeInfo * type)
{
    size32_t nlen = type->getStringLen();
    if(nlen == UNKNOWN_LENGTH)
    {
        ITypeInfo * newType = getStretchedType(len, type);
        type->Release();
        type = newType;
        nlen = len;
    }
    if (nlen <= len)
        return createUtf8Value(text, type);

    rtlDataAttr buff(nlen * 4);
    rtlUtf8SubStrFT(nlen, buff.getstr(), len, text, 1, nlen);
    return new Utf8Value(buff.getstr(), type);
}

//===========================================================================

DataValue::DataValue(const void *v, ITypeInfo *_type) : MemoryValue(v, _type)
{
}

const char *DataValue::generateECL(StringBuffer &out)
{
    out.append("X'");
    appendDataAsHex(out, val.length(), val.get());
    out.append('\'');
    return out.str();
}


IValue *DataValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    size32_t osize = type->getSize();
    size32_t nsize = t->getSize();
    switch (tc)
    {
    case type_data:
        if (nsize == UNKNOWN_LENGTH)
            return LINK(this);
        if (nsize <= osize)
            return new DataValue(val.get(), LINK(t));
        else
        {
            char *newstr = (char *) checked_malloc(nsize, DEFVALUE_MALLOC_FAILED);
            memcpy(newstr, val.get(), osize);
            memset(newstr+osize, 0, nsize-osize);
            IValue * ret = new DataValue(newstr, LINK(t));
            free(newstr);
            return ret;
        }
        break;
    case type_string:
        if (nsize == UNKNOWN_LENGTH)
        {
            nsize = osize;
            t = getStretchedType(osize, t);
        }
        else
            t->Link();
        if (nsize <= osize)
            return new StringValue((char *)val.get(), t);
        else
        {
            char *newstr = (char *) checked_malloc(nsize, DEFVALUE_MALLOC_FAILED);
            memcpy(newstr, val.get(), osize);
            memset(newstr+osize, t->queryCharset()->queryFillChar(), nsize-osize);
            IValue * ret = new StringValue(newstr, t);
            free(newstr);
            return ret;
        }
        break;
    case type_decimal:
        return t->castFrom(osize, (const char *)val.get());
    case type_boolean:
        return createBoolValue(getBoolValue());
    }
    ITypeInfo * tempType = makeStringType(getSize(), NULL, NULL);
    IValue * temp = castTo(tempType);
    IValue * ret = temp->castTo(t);
    temp->Release();
    tempType->Release();
    return ret;
}

const void *DataValue::queryValue() const
{
    return val.get();
}

bool DataValue::getBoolValue()
{
    //return true if any character is non space
    //return true if any non zero character
    const char * cur = (const char *)val.get();
    unsigned len = type->getSize();
    char fill = 0;
    while (len--)
    {
        if (*cur++ != fill)
            return true;
    }
    return false;
}

const char *DataValue::getStringValue(StringBuffer &out)
{
    appendDataAsHex(out, val.length(), val.get());
    return out.str();
}

IValue *createDataValue(const char *val, unsigned size)
{
    return createDataValue(val, makeDataType(size));
}

IValue *createDataValue(const char *val, ITypeInfo *type)
{
    assertex(type->getSize() != UNKNOWN_LENGTH);
    return new DataValue(val, type);
}


//===========================================================================

QStringValue::QStringValue(unsigned len, const void *v, ITypeInfo *_type) : MemoryValue(_type)
{
    unsigned newSize = _type->getSize();
    char * temp = (char *)val.allocate(newSize);
    rtlStrToQStr(_type->getStringLen(), temp, len, v);
}

QStringValue::QStringValue(const char *v, ITypeInfo *_type) : MemoryValue(_type)
{
    unsigned newSize = _type->getSize();
    char * temp = (char *)val.allocate(newSize);
    memcpy(temp, v, newSize);
}

const char *QStringValue::generateECL(StringBuffer &out)
{
    unsigned strLen = type->getStringLen();
    char * strData = (char *)checked_malloc(strLen, DEFVALUE_MALLOC_FAILED);
    rtlQStrToStr(strLen, strData, strLen, (const char *)val.get());
    out.append('Q');
    appendStringAsQuotedECL(out, strLen, strData);
    free(strData);
    return out.str();
}

int QStringValue::compare(IValue * to)
{
    return rtlCompareQStrQStr(type->getStringLen(), val.get(), to->queryType()->getStringLen(), to->queryValue());
}


int QStringValue::compare(const void *mem)
{
    size32_t len = type->getStringLen();
    return rtlCompareQStrQStr(len, val.get(), len, mem);
}

IValue *QStringValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    switch (tc)
    {
    case type_boolean:
        return createBoolValue(getBoolValue());
    }

    unsigned strLen = type->getStringLen();
    char * strData = (char *)checked_malloc(strLen, DEFVALUE_MALLOC_FAILED);
    rtlQStrToStr(strLen, strData, strLen, (const char *)val.get());
    IValue * ret = t->castFrom(strLen, strData);
    free(strData);
    return ret;
}

const void *QStringValue::queryValue() const
{
    return (const void *) val.get();
}

bool QStringValue::getBoolValue()
{
    //return true if any character is non space
    //return true if any non zero character
    const char * cur = (const char *)val.get();
    unsigned len = type->getSize();
    while (len--)
    {
        if (*cur++)
            return true;
    }
    return false;
}

__int64 QStringValue::getIntValue()
{
    unsigned strLen = type->getStringLen();
    char * strData = (char *)checked_malloc(strLen, DEFVALUE_MALLOC_FAILED);
    rtlQStrToStr(strLen, strData, strLen, (const char *)val.get());
    __int64 ret = rtlStrToInt8(strLen, strData);
    free(strData);
    return ret;
}

const char *QStringValue::getStringValue(StringBuffer &out)
{
    unsigned strLen = type->getStringLen();
    char * strData = (char *)checked_malloc(strLen, DEFVALUE_MALLOC_FAILED);
    rtlQStrToStr(strLen, strData, strLen, (const char *)val.get());
    out.append(strLen, strData);
    free(strData);
    return out.str();
}

void QStringValue::pushDecimalValue()
{
    unsigned strLen = type->getStringLen();
    char * strData = (char *)checked_malloc(strLen, DEFVALUE_MALLOC_FAILED);
    rtlQStrToStr(strLen, strData, strLen, (const char *)val.get());
    DecPushString(strLen, strData);
    free(strData);
}

IValue *createQStringValue(unsigned len, const char *val, ITypeInfo *type)
{
    if (type->getSize() == UNKNOWN_LENGTH)
    {
        ITypeInfo * newType = getStretchedType(len, type);
        type->Release();
        return new QStringValue(len, val, newType);
    }
    return new QStringValue(len, val, type);
}

//===========================================================================

CharValue::CharValue(char _val, ITypeInfo * _type) : CValue(_type)
{
    val = _val;
}

const char *CharValue::generateECL(StringBuffer &out)
{
    appendStringAsQuotedECL(out, 1, &val);
    return out.str();
}

const char *CharValue::generateCPP(StringBuffer &out, CompilerType compiler)
{
    switch (val)
    {
        case '\n': out.append("'\\n'"); break;
        case '\r': out.append("'\\r'"); break;
        case '\t': out.append("'\\t'"); break;
        case '\'': out.append("'\\''"); break;
        default:
            if ((val >= ' ') && (val <= 126))
                out.append('\'').append(val).append('\'');
            else
                out.append((int)val); 
            break;
    }
    return out.str();
}

void CharValue::toMem(void *target)
{
    *(char *)target = val;
}

unsigned CharValue::getHash(unsigned initval)
{
    char buf = val;;
    return hashc((unsigned char *)&buf, 1, initval);
}

int CharValue::compare(IValue *_to)
{
    assertThrow(_to->queryType()==type);
    CharValue *to = (CharValue *) _to;
    return memcmp(&val, &to->val, 1);
}

int CharValue::compare(const void *mem)
{
    return memcmp(&val, (const char *) mem, 1);
}

IValue *CharValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    switch (tc)
    {
    case type_boolean:
        return createBoolValue(val != 0);
    }

    return t->castFrom(1, &val);
}

bool CharValue::getBoolValue()
{
    return (val != ' ');
}

const char *CharValue::getStringValue(StringBuffer &out)
{
    return out.append(val).str();
}

void CharValue::pushDecimalValue()
{
    DecPushString(1,&val);
}

void CharValue::serialize(MemoryBuffer &tgt)
{
    tgt.append(val);
}

void CharValue::deserialize(MemoryBuffer &src)
{
    src.read(val);
}

int CharValue::rangeCompare(ITypeInfo * targetType)
{
    //MORE: to integegral types?
    return 0;
}

IValue *createCharValue(char val, bool caseSensitive)
{
    return createCharValue(val, makeCharType(caseSensitive));
}

IValue *createCharValue(char val, ITypeInfo *type)
{
    return new CharValue(val, type);
}

//===========================================================================

int IntValue::compare(IValue *_to)
{
    assertThrow(_to->queryType()==type);
    IntValue *to = (IntValue *) _to;
    if (val == to->val)
        return 0;
    else if (type->isSigned())
        return (__int64) val > (__int64) to->val ? 1 : -1;
    else
        return val > to->val ? 1 : -1;
}

IValue * IntValue::castViaString(ITypeInfo * t)
{
    size32_t len;
    char * text;
    if (type->isSigned())
        rtlInt8ToStrX(len, text, val);
    else
        rtlUInt8ToStrX(len, text, (unsigned __int64)val);
    IValue * ret = t->castFrom(len, text);          // Include EBCDIC conversion, and creating correct type
    rtlFree(text);
    return ret;
}

IValue *IntValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    unsigned nLen = t->getStringLen();
    switch (tc)
    {
    case type_qstring:
    case type_utf8:
    case type_unicode:
        return castViaString(t);
    case type_string:
    case type_varstring:
    {
        if (nLen == UNKNOWN_LENGTH)
            return castViaString(t);

        char *newstr = (char *) checked_malloc(nLen, DEFVALUE_MALLOC_FAILED);
        if (type->isSigned())
            rtlInt8ToStr(nLen, newstr, val);
        else
            rtlUInt8ToStr(nLen, newstr, (unsigned __int64)val);
        IValue * ret = t->castFrom(nLen, newstr);
        free(newstr);
        return ret;
    }
    case type_int:
    case type_swapint:
        return createTruncIntValue(val, LINK(t));
    case type_boolean:
        return createBoolValue(val!=0);
    }
    return t->castFrom(type->isSigned(), (__int64)val);
}

byte * IntValue::getAddressValue()
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return (byte *)&val;
#else
    return (byte *)&val + (8 - type->getSize());
#endif
}


const void * IntValue::queryValue() const
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return reinterpret_cast<const byte *>(&val);
#else
    return reinterpret_cast<const byte *>(&val) + (8 - type->getSize());
#endif
}


void IntValue::toMem(void *target)
{
    size32_t size = type->getSize();
    const byte * data = getAddressValue();
    
    if (type->isSwappedEndian())
        _cpyrevn(target, data, size);
    else
        memcpy(target, data, size);
}

unsigned IntValue::getHash(unsigned initval)
{
    return hashc(getAddressValue(), type->getSize(), initval);
}

const char *IntValue::generateECL(StringBuffer &s)
{
    return getStringValue(s);
}

const char *IntValue::generateCPP(StringBuffer &s, CompilerType compiler)
{
    if (type->isSwappedEndian())
    {
        if (type->isSigned())
            s.append(rtlReadSwapInt(getAddressValue(), type->getSize()));
        else
            s.append(rtlReadSwapUInt(getAddressValue(), type->getSize()));
    }
    else
        getStringValue(s);

    switch (compiler)
    {
    case GccCppCompiler:
        if (val && (type->getSize() > sizeof(unsigned)))
        {
            s.append("LL");
            if (!type->isSigned())
                s.append("U");
        }
        else if (!type->isSigned())
            s.append("U");
        break;
    case Vs6CppCompiler:
        if (val && (type->getSize() > sizeof(unsigned)))
        {
            if (!type->isSigned())
                s.append("U");
            s.append("i64");
        }
        else if (!type->isSigned())
            s.append("U");
        break;
    default:
        throwUnexpected();
    }

    return s.str();
}

const char *IntValue::getStringValue(StringBuffer &s)
{
    if (type->isSigned())
        s.append((__int64)val);
    else
        s.append(val);
    return s.str();
}

bool IntValue::getBoolValue()
{
    return val != 0;
}

__int64 IntValue::getIntValue()
{
    return val;
}

void IntValue::pushDecimalValue(void)
{
    if (type->isSigned())
        DecPushInt64(val);
    else
        DecPushUInt64(val);
}

int IntValue::rangeCompare(ITypeInfo * targetType)
{
    if (type->isSigned())
        return ::rangeCompare((double)(__int64)val, targetType);
    return ::rangeCompare((double)(unsigned __int64)val, targetType);
}

void IntValue::serialize(MemoryBuffer &tgt)
{
    tgt.append(val);
}

void IntValue::deserialize(MemoryBuffer &src)
{
    src.read(val);
}


//===========================================================================

int PackedIntValue::compare(IValue *_to)
{
    assertThrow(_to->queryType()==type);
    unsigned __int64 val = value->getIntValue();
    unsigned __int64 toVal = _to->getIntValue();
    if (val == toVal)
        return 0;
    else if (type->isSigned())
        return (__int64) val > (__int64) toVal ? 1 : -1;
    else
        return val > toVal ? 1 : -1;
}

void PackedIntValue::toMem(void *target)
{
    unsigned __int64 val = value->getIntValue();
    if (type->isSigned())
        rtlSetPackedSigned(target, val);
    else
        rtlSetPackedUnsigned(target, val);
}

//---------------------------------------------------------------------------

bool isInRange(__int64 value, bool isSigned, unsigned size)
{
    if (isSigned)
        value += (maxIntValue[size]+1);
    if (value & ~maxUIntValue[size])
        return false;
    return true;
}

inline unsigned getMinSize(unsigned __int64 value, bool isSigned)
{
    if (isSigned)
    {
        if ((value + 0x80) <= 0xff) return 1;
        if ((value + 0x8000) <= 0xffff) return 2;
        if ((value + 0x80000000) <= 0xffffffff) return 4;
    }
    else
    {
        if (value <= 0xff) return 1;
        if (value <= 0xffff) return 2;
        if (value <= 0xffffffff) return 4;
    }
    return 8;
}

unsigned getMinimumIntegerSize(unsigned __int64 value, bool isSigned)
{
    return getMinSize(value, isSigned);
}


static unsigned __int64 truncateInt(unsigned __int64 value, unsigned size, bool isSigned)
{
    if (isSigned)
    {
        unsigned shift = (8-size)*8;
        value <<= shift;
        value = ((__int64)value) >> shift;
    }
    else
    {
        value &= maxUIntValue[size];
    }
    return value;
}

IValue *createIntValue(__int64 val, ITypeInfo * type)
{
#ifdef _DEBUG
    if (!isInRange(val, type->isSigned(), type->getSize()))
        throw MakeStringException(1, "Out of range value");
#endif
    return new IntValue(val, type);
}

IValue *createIntValue(__int64 val, unsigned size, bool isSigned)
{
#ifdef _DEBUG
    if (!isInRange(val, isSigned, size))
        throw MakeStringException(0, "Out of range value");
#endif
    ITypeInfo * type = makeIntType(size, isSigned);
    return new IntValue(val, type);
}

IValue *createTruncIntValue(__int64 val, ITypeInfo * type)
{
    if (type->getTypeCode() == type_packedint)
        return createPackedIntValue(val, type);
    return new IntValue(truncateInt(val, type->getSize(), type->isSigned()), type);
}

IValue *createPackedIntValue(__int64 val, ITypeInfo * type)
{
    assertex(type->getTypeCode() == type_packedint);
    ITypeInfo * promotedType = type->queryPromotedType();
    IValue * value = createTruncIntValue(val, LINK(promotedType));
    return new PackedIntValue(value, type);
}

IValue *createPackedIntValue(IValue * val, ITypeInfo * type)
{
    assertex(type->getTypeCode() == type_packedint);
    return new PackedIntValue(val, type);
}

IValue *createTruncIntValue(__int64 val, unsigned size, bool isSigned)
{
    val = truncateInt(val, size, isSigned);
    ITypeInfo * type = makeIntType(size, isSigned);
    return new IntValue(val, type);
}

IValue * createMinIntValue(__int64 value)
{
    return createIntValue(value, getMinSize(value, true), true);
}

IValue * createMinUIntValue(unsigned __int64 value)
{
    return createIntValue(value, getMinSize(value, false), false);
}


IValue *createBitfieldValue(__int64 val, ITypeInfo * type)
{
#ifdef _DEBUG
    size32_t bitsize = type->getBitSize();
    if (bitsize != sizeof(__int64) * 8)
        val = val & (((__int64)1 << bitsize)-1);
#endif
    return new BitfieldValue(val, type);
}


//===========================================================================

IValue *EnumValue::castTo(ITypeInfo * t)
{
    t = queryUnqualifiedType(t);
    if (t == type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    if (t->isInteger())
        return IntValue::castTo(t);

    CEnumeratedTypeInfo * et = (CEnumeratedTypeInfo *) type;
    IValue *ret = et->queryValue((int) val);
    if (ret) 
        return ret->castTo(t);

    // cope better with unknown index
    Owned<IValue> temp = createStringValue("", 0U);
    return temp->castTo(t);
}

IValue * createEnumValue(int value, ITypeInfo * type)
{
  return new EnumValue(value, type);
}

//===========================================================================

int RealValue::compare(IValue *_to)
{
    assertThrow(_to->queryType()==type);
    RealValue *to = (RealValue *) _to;
    if (val == to->val)
        return 0;
    else
        return val > to->val ? 1 : -1;
}

IValue *RealValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    switch (tc)
    {
    case type_real:
    {
        double value = val;
        switch (t->getSize())
        {
        case 4: value = (float)value; break;
        case 8: value = (double)value; break;
        }
        return createRealValue(value, LINK(t));
    }
    case type_int:
    case type_swapint:
    case type_packedint:
        return createTruncIntValue((__int64)val, LINK(t));
    case type_boolean:
        return createBoolValue(val!=0);
    }
    return t->castFrom(val);
}

void RealValue::toMem(void *target)
{
    RealUnion u;

    size32_t size = type->getSize();
    switch (size)
    {
    case 4:
        u.r4 = (float)val;
        break;
    case 8:
        u.r8 = val;
        break;
    };
    memcpy(target, &u, size);
}

unsigned RealValue::getHash(unsigned initval)
{
    RealUnion u;

    size32_t size = type->getSize();
    switch (size)
    {
    case 4:
        u.r4 = (float)val;
        break;
    case 8:
        u.r8 = val;
        break;
    };

    return hashc((unsigned char *) &u, size, initval);
}

const char *RealValue::generateECL(StringBuffer &s)
{
    return getStringValue(s);
}

const char *RealValue::generateCPP(StringBuffer &s, CompilerType compiler)
{
    return getStringValue(s);
}

const char *RealValue::getStringValue(StringBuffer &s)
{
    size32_t size = type->getSize();
    if (size==4)
        return s.append((float) val);
    else
        return s.append(val);
}

bool RealValue::getBoolValue()
{
    return val != 0;
}

__int64 RealValue::getIntValue()
{
    return (__int64)(unsigned __int64)val;
}

double RealValue::getRealValue()
{
    return val;
}

void RealValue::pushDecimalValue()
{
    DecPushReal(val);
}

void RealValue::serialize(MemoryBuffer &tgt)
{
    tgt.append(val);
}

void RealValue::deserialize(MemoryBuffer &src)
{
    src.read((double &)val);
}

int RealValue::rangeCompare(ITypeInfo * targetType)
{
    return ::rangeCompare(val, targetType);
}

IValue *createRealValue(double val, unsigned size)
{
    return new RealValue(val, size);
}

IValue *createRealValue(double val, ITypeInfo * type)
{
    return new RealValue(val, type);
}

//===========================================================================

DecimalValue::DecimalValue(const void * v, ITypeInfo * _type) : CValue(_type)
{
    unsigned len = _type->getSize();
    val = (char *)checked_malloc(len, DEFVALUE_MALLOC_FAILED);
    memcpy(val, v, len);
}

DecimalValue::~DecimalValue()
{
    free(val);
}

int DecimalValue::compare(IValue *_to)
{
    assertThrow(_to->getTypeCode()==type_decimal);
    BcdCriticalBlock bcdBlock;
    pushDecimalValue();
    _to->pushDecimalValue();
    return DecDistinct();
}

IValue *DecimalValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    BcdCriticalBlock bcdBlock;
    char newstr[400];
    pushDecimalValue();
    switch (tc)
    {
    case type_real:
    {
        double value = DecPopReal();
        switch (t->getSize())
        {
        case 4: value = (float)value; break;
        case 8: value = (double)value; break;
        }
        return createRealValue(value, LINK(t));
    }
    case type_int:
    case type_swapint:
    case type_packedint:
        return createTruncIntValue(DecPopInt64(), LINK(t));
    case type_boolean:
        return createBoolValue(DecCompareNull() != 0);
    case type_decimal:
        return createDecimalValueFromStack(t);
    }
    DecPopCString(sizeof(newstr), newstr);
    return t->castFrom((size32_t)strlen(newstr), newstr);
}

void DecimalValue::toMem(void *target)
{
    size32_t size = type->getSize();
    memcpy(target, val, size);
}

unsigned DecimalValue::getHash(unsigned initval)
{
    size32_t size = type->getSize();
    return hashc((unsigned char *) val, size, initval);
}

const char *DecimalValue::generateECL(StringBuffer &s)
{
    getStringValue(s);
    s.append('D');
    return s;
}

const char *DecimalValue::generateCPP(StringBuffer &s, CompilerType compiler)
{
    size32_t size = type->getSize();
    s.append("\"");
    for (unsigned i=0;i<size;i++)
    {
        unsigned char c = ((unsigned char *)val)[i];
        s.append("\\x");
        s.appendhex(c, false);
    }
    s.append('"');
    return s.str();
}

const char *DecimalValue::getStringValue(StringBuffer &s)
{
    char strval[64];
    BcdCriticalBlock bcdBlock;
    pushDecimalValue();
    DecPopCString(sizeof(strval), strval);
    s.append(strval);
    return s.str();
}

bool DecimalValue::getBoolValue()
{
    BcdCriticalBlock bcdBlock;
    pushDecimalValue();
    return DecCompareNull() != 0;
}

__int64 DecimalValue::getIntValue()
{
    BcdCriticalBlock bcdBlock;
    pushDecimalValue();
    return DecPopInt64();
}

double DecimalValue::getRealValue()
{
    BcdCriticalBlock bcdBlock;
    pushDecimalValue();
    return DecPopReal();
}

void DecimalValue::pushDecimalValue()
{
    if (type->isSigned())
        DecPushDecimal(val, type->getSize(), type->getPrecision());
    else
        DecPushUDecimal(val, type->getSize(), type->getPrecision());
}

const void * DecimalValue::queryValue() const
{
    return (const void *)val;
}

void DecimalValue::serialize(MemoryBuffer &tgt)
{
    tgt.append(type->getSize());
    tgt.append(type->getSize(), val);
}

void DecimalValue::deserialize(MemoryBuffer &src)
{
    size32_t size;
    src.read(size);
    val = checked_malloc(size, DEFVALUE_MALLOC_FAILED);
    assertex(val);
}

int DecimalValue::rangeCompare(ITypeInfo * targetType)
{
    return ::rangeCompare(getRealValue(), targetType);
}

IValue *createDecimalValue(void * val, ITypeInfo * type)
{
    return new DecimalValue(val, type);
}

IValue *createDecimalValueFromStack(ITypeInfo * type)
{
    return static_cast<CDecimalTypeInfo*>(type)->createValueFromStack();
}

//===========================================================================

const char *BoolValue::generateECL(StringBuffer &s)
{
    s.append(val ? "true" : "false");
    return s.str();
}

const char *BoolValue::generateCPP(StringBuffer &s, CompilerType compiler)
{
    s.append(val ? "true" : "false");
    return s.str();
}

const char *BoolValue::getStringValue(StringBuffer &s)
{
    s.append(val ? "true" : "false");
    return s.str();
}

bool BoolValue::getBoolValue()
{
    return val;
}

__int64 BoolValue::getIntValue()
{
    return val;
}

void BoolValue::pushDecimalValue()
{
    DecPushUlong(val);
}

void BoolValue::toMem(void *target)
{
    memcpy(target, &val, type->getSize());
}

unsigned BoolValue::getHash(unsigned initval)
{
    size32_t size = type->getSize();
    return hashc((unsigned char *) &val, size, initval);
}

int BoolValue::compare(IValue *_to)
{
    assertThrow(_to->queryType()==type);
    BoolValue *to = (BoolValue *) _to;
    return (int) val - (int) to->val;
}

int BoolValue::compare(const void *mem)
{
    type->Link();
    IValue *to = createValueFromMem(type, mem);
    int ret = compare(to);
    to->Release();
    return ret;
}

IValue *BoolValue::castTo(ITypeInfo *t)
{
    t = queryUnqualifiedType(t);
    if (t==type)
        return LINK(this);

    type_t tc = t->getTypeCode();
    if (tc == type_any)
        return LINK(this);

    switch (tc)
    {
    case type_string:
    case type_qstring:
    case type_varstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        if (!val)
            return t->castFrom(0, "");
        break;
    case type_int:
    case type_swapint:
        return createTruncIntValue(val, LINK(t));
    case type_packedint:
        return createPackedIntValue(val, LINK(t));
    }
    return t->castFrom(false, (__int64)val);
}

BoolValue *BoolValue::getTrue()
{
    trueconst->Link();
    return trueconst;
}

BoolValue *BoolValue::getFalse()
{
    falseconst->Link();
    return falseconst;
}

void BoolValue::serialize(MemoryBuffer &tgt)
{
    tgt.append(val);
}

void BoolValue::deserialize(MemoryBuffer &src)
{
    src.read(val);
}

IValue *createBoolValue(bool v)
{
    return v ? BoolValue::getTrue() : BoolValue::getFalse();
}

int BoolValue::rangeCompare(ITypeInfo * targetType)
{
    return 0; // can fit in anything
}

/* In type: linked */
IValue *createValueFromMem(ITypeInfo *type, const void *mem)
{
    int size = type->getSize();
    type_t code = type->getTypeCode();

    switch (code)
    {
    case type_string:
        return createStringValue((const char *) mem, type);
    case type_unicode:
        return new UnicodeValue((UChar const *) mem, type);
    case type_utf8:
        return new Utf8Value((char const *) mem, type);
    case type_data:
        type->Release();
        return createDataValue((const char *) mem, size);
    case type_varstring:
        return createVarStringValue((size32_t)strlen((const char *) mem), (const char *) mem, type);
    case type_varunicode:
        return new VarUnicodeValue(rtlUnicodeStrlen((UChar const *) mem), (UChar const *) mem, type);
    case type_qstring:
        return new QStringValue((const char *)mem, type);
    case type_decimal:
        return new DecimalValue(mem, type);
    case type_bitfield:
        UNIMPLEMENTED;
        //needs more thought;
    case type_int:
    {
        unsigned __int64 val;
        if (type->isSigned())
            val = rtlReadInt(mem, size);
        else
            val = rtlReadUInt(mem, size);
        return createIntValue(val, type);
    }
    case type_swapint:
    {
        unsigned __int64 val;
        if (type->isSigned())
            val = rtlReadSwapInt(mem, size);
        else
            val = rtlReadSwapUInt(mem, size);
        return createIntValue(val, type);
    }
    case type_packedint:
    {
        unsigned __int64 val = type->isSigned() ? rtlGetPackedSigned(mem) : rtlGetPackedUnsigned(mem);
        return createPackedIntValue(val, type);
    }
    case type_real:
    {
        RealUnion u;
        memcpy(&u, mem, size);
        double val = 0;
        switch (size)
        {
        case 4:
            val = u.r4;
            break;
        case 8:
            val = u.r8;
            break;
        }
        type->Release();
        return createRealValue(val, size);
    }
    case type_boolean:
        type->Release();
        return createBoolValue(*(bool *)mem);
    }
    type->Release();
    return NULL;
}

//----------------------------------------------------------------------------

void appendValueToBuffer(MemoryBuffer & mem, IValue * value)
{
    ITypeInfo * type = value->queryType();
    unsigned len = type->getSize();
    void * temp = checked_malloc(len, DEFVALUE_MALLOC_FAILED);
    value->toMem(temp);

    if (type->isSwappedEndian() != mem.needSwapEndian())
        mem.appendSwap(len, temp);
    else
        mem.append(len, temp);
    free(temp);
}

//============================================================================

IValue * addValues(IValue * left, IValue * right)
{
    IValue * ret;
    ITypeInfo * pnt = getPromotedAddSubType(left->queryType(), right->queryType());
    
    switch(pnt->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        ret = createTruncIntValue(left->getIntValue() + right->getIntValue(), pnt);
        break;
    case type_real:
        ret = createRealValue(left->getRealValue() + right->getRealValue(), pnt);
        break;
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            left->pushDecimalValue();
            right->pushDecimalValue();
            DecAdd();
            ret = ((CDecimalTypeInfo*)pnt)->createValueFromStack();
            pnt->Release();
            break;
        }
    default:
        throwUnexpected();
    }
    return ret;
}

#define CALCULATE_AND_RETURN(op)    IValue * res = LINK(*values);       \
                                    while (--num)                       \
                                    {                                   \
                                        values++;                       \
                                        IValue * tmp = op(res, *values);\
                                        res->Release();                 \
                                        res = tmp;                      \
                                    }                                   \
                                    return res;                         


IValue * subtractValues(IValue * left, IValue * right)
{
    IValue * ret;
    ITypeInfo * pnt = getPromotedAddSubType(left->queryType(), right->queryType());
    
    switch(pnt->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        ret = createTruncIntValue(left->getIntValue() - right->getIntValue(), pnt);
        break;
    case type_real:
        ret = createRealValue(left->getRealValue() - right->getRealValue(), pnt);
        break;
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            left->pushDecimalValue();
            right->pushDecimalValue();
            DecSub();
            ret = ((CDecimalTypeInfo*)pnt)->createValueFromStack();
            pnt->Release();
            break;
        }
    default:
        throwUnexpected();
    }
    return ret;
}

IValue * multiplyValues(IValue * left, IValue * right)
{
    IValue * ret;
    ITypeInfo * pnt = getPromotedMulDivType(left->queryType(), right->queryType());
    
    switch(pnt->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        ret = createTruncIntValue(left->getIntValue() * right->getIntValue(), pnt);
        break;
    case type_real:
        ret = createRealValue(left->getRealValue() * right->getRealValue(), pnt);
        break;
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            left->pushDecimalValue();
            right->pushDecimalValue();
            DecMul();
            ret = ((CDecimalTypeInfo*)pnt)->createValueFromStack();
            pnt->Release();
            break;
        }
    default:
        throwUnexpected();
    }
    return ret;
}

IValue * divideValues(IValue * left, IValue * right, byte dbz)
{
    Owned<ITypeInfo> pnt = getPromotedMulDivType(left->queryType(), right->queryType());

    //Use a cast to a boolean as a shortcut for testing against zero
    if (!right->getBoolValue())
    {
        //If no action is selected, return NULL so the expression doesn't get constant folded.
        if (dbz == DBZnone)
            return NULL;

        if (dbz == DBZfail)
            rtlFailDivideByZero();
    }

    switch(pnt->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
    {
        __int64 lv = left->getIntValue();
        __int64 rv = right->getIntValue();
        __int64 res = 0;
        if (rv)
        {
            if (pnt->isSigned())
                res = lv / rv;
            else
                res = (__int64)((unsigned __int64)lv / (unsigned __int64)rv);
        }
        return createTruncIntValue(res, pnt.getClear());
    }
    case type_real:
    {
        double lv = left->getRealValue();
        double rv = right->getRealValue();
        double res;
        if (rv)
            res = lv / rv;
        else if (dbz == DBZnan)
            res =  rtlCreateRealNull();
        else
            res = 0.0;

        return createRealValue(res, pnt.getClear());
    }
    case type_decimal:
    {
        BcdCriticalBlock bcdBlock;
        left->pushDecimalValue();
        right->pushDecimalValue();
        DecDivide(dbz);
        return createDecimalValueFromStack(pnt);
    }
    default:
        throwUnexpected();
        return NULL;
    }
}

IValue * modulusValues(IValue * left, IValue * right, byte dbz)
{
    Owned<ITypeInfo> pnt = getPromotedMulDivType(left->queryType(), right->queryType());
    
    //Use a cast to a boolean as a shortcut for testing against zero
    if (!right->getBoolValue())
    {
        //If no action is selected, return NULL so the expression doesn't get constant folded.
        if (dbz == DBZnone)
            return NULL;

        if (dbz == DBZfail)
            rtlFailDivideByZero();
    }

    switch(pnt->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
    {   
        __int64 lv = left->getIntValue();
        __int64 rv = right->getIntValue();
        __int64 res = 0;
        if (rv)
        {
            if (pnt->isSigned())
                res = lv % rv;
            else
                res = (__int64)((unsigned __int64)lv % (unsigned __int64)rv);
        }
        return createTruncIntValue(res, pnt.getClear());
    }
    case type_real:
    {
        double rv = right->getRealValue();
        double res;
        if (rv)
            res = fmod(left->getRealValue(), rv);
        else if (dbz == DBZnan)
            res =  rtlCreateRealNull();
        else
            res = 0.0;

        return createRealValue(res, pnt.getClear());
    }
    case type_decimal:
    {
        BcdCriticalBlock bcdBlock;
        left->pushDecimalValue();
        right->pushDecimalValue();
        DecModulus(dbz);
        return createDecimalValueFromStack(pnt);
    }
    default:
        throwUnexpected();
        return NULL;
    }
}

IValue * powerValues(IValue * left, IValue * right)
{
    IValue * ret;
    ITypeInfo * pnt = makeRealType(8);

    switch(pnt->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_real:
        ret = createRealValue(pow(left->getRealValue(), right->getRealValue()), pnt);
        break;
/*
// TBD
    case type_decimal:
        BcdCriticalBlock bcdBlock;
        left->pushDecimalValue();
        DecLongPower(right->getIntValue());
        ret = ((CDecimalTypeInfo*)pnt)->createValueFromStack();
        pnt->Release();
        break
*/
    default:
        pnt->Release();
        throwUnexpected();
    }
    return ret;
}

IValue * negateValue(IValue * v)
{
    switch(v->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        return createTruncIntValue(-(v->getIntValue()), v->getType());
    case type_real:     
        return createRealValue(-(v->getRealValue()), v->getSize());
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            v->pushDecimalValue();
            DecNegate();
            return ((CDecimalTypeInfo*)v->queryType())->createValueFromStack();
        }
    }
    throwUnexpected();
    return NULL;
}

IValue * expValue(IValue * v)
{
    return createRealValue(exp(v->getRealValue()), 8);
}

IValue * roundUpValue(IValue * v)
{
    switch(v->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        return LINK(v);
    case type_real:
        return createTruncIntValue(rtlRoundUp(v->getRealValue()), 8, true);
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            v->pushDecimalValue();
            DecRoundUp();
            OwnedITypeInfo resultType = getRoundType(v->queryType());
            return createDecimalValueFromStack(resultType);
        }
    }
    throwUnexpected();
    return NULL;
}

IValue * roundValue(IValue * v)
{
    switch(v->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        return LINK(v);
    case type_real:
        return createTruncIntValue(rtlRound(v->getRealValue()), 8, true);
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            v->pushDecimalValue();
            DecRound();
            Owned<ITypeInfo> resultType = getRoundType(v->queryType());
            return createDecimalValueFromStack(resultType);
        }
    }
    throwUnexpected();
    return NULL;
}

IValue * roundToValue(IValue * v, int places)
{
    switch(v->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        return createRealValue(rtlRoundTo(v->getRealValue(), places), 8);
    case type_real:
        return createRealValue(rtlRoundTo(v->getRealValue(), places), 8);
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            v->pushDecimalValue();
            DecRoundTo(places);
            OwnedITypeInfo resultType = getRoundToType(v->queryType());
            return createDecimalValueFromStack(resultType);
        }
    }
    throwUnexpected();
    return NULL;
}

IValue * truncateValue(IValue * v)
{
    switch(v->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        return LINK(v);
    case type_real:
        return createTruncIntValue(v->getIntValue(), 8, true);
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            v->pushDecimalValue();
            DecTruncate();
            OwnedITypeInfo resultType = getTruncType(v->queryType());
            return createDecimalValueFromStack(resultType);
        }
    }
    throwUnexpected();
    return NULL;
}

IValue * lnValue(IValue * v)
{
    return createRealValue(rtlLog(v->getRealValue()), 8);
}

IValue * sinValue(IValue * v)
{
    return createRealValue(sin(v->getRealValue()), 8);
}

IValue * cosValue(IValue * v)
{
    return createRealValue(cos(v->getRealValue()), 8);
}

IValue * tanValue(IValue * v)
{
    return createRealValue(tan(v->getRealValue()), 8);
}

IValue * sinhValue(IValue * v)
{
    return createRealValue(sinh(v->getRealValue()), 8);
}

IValue * coshValue(IValue * v)
{
    return createRealValue(cosh(v->getRealValue()), 8);
}

IValue * tanhValue(IValue * v)
{
    return createRealValue(tanh(v->getRealValue()), 8);
}

IValue * asinValue(IValue * v)
{
    return createRealValue(rtlASin(v->getRealValue()), 8);
}

IValue * acosValue(IValue * v)
{
    return createRealValue(rtlACos(v->getRealValue()), 8);
}

IValue * atanValue(IValue * v)
{
    return createRealValue(atan(v->getRealValue()), 8);
}

IValue * atan2Value(IValue * y, IValue* x)
{
    return createRealValue(atan2(y->getRealValue(), x->getRealValue()), 8);
}

IValue * log10Value(IValue * v)
{
    return createRealValue(rtlLog10(v->getRealValue()), 8);
}

IValue * sqrtValue(IValue * v)
{
    switch(v->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_real:
        return createRealValue(rtlSqrt(v->getRealValue()), 8);
    case type_decimal:
        //MORE: This should probably do this more accurately.
        return createRealValue(rtlSqrt(v->getRealValue()), 8);
    }
    throwUnexpected();
    return NULL;
}

IValue * absValue(IValue * v)
{
    switch(v->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        {
            ITypeInfo * type = v->queryType();
            if (type->isSigned())
            {
                __int64 val = v->getIntValue();
                if (val < 0)
                    return createIntValue(-val, LINK(type));
            }
            return LINK(v);
        }
    case type_real:
        return createRealValue(fabs(v->getRealValue()), v->getSize());
    case type_decimal:
        {
            BcdCriticalBlock bcdBlock;
            v->pushDecimalValue();
            DecAbs();
            return ((CDecimalTypeInfo*)v->queryType())->createValueFromStack();
        }
    }
    throwUnexpected();
    return NULL;
}

IValue * substringValue(IValue * v, IValue * lower, IValue * higher)
{
    ITypeInfo * type = v->queryType();
    unsigned srcLen = type->getStringLen();
    const void * raw = v->queryValue();
    unsigned low = lower ? (unsigned)lower->getIntValue() : 0;
    unsigned high = higher ? (unsigned)higher->getIntValue() : srcLen;

    unsigned retLen = 0;
    void * retPtr;
    ITypeInfo * retType = NULL;
    switch (type->getTypeCode())
    {
    case type_string:
        rtlSubStrFTX(retLen, *(char * *)&retPtr, srcLen, (const char *)raw, low, high);
        break;
    case type_varstring:
        rtlSubStrFTX(retLen, *(char * *)&retPtr, srcLen, (const char *)raw, low, high);
        retType = makeStringType(retLen, LINK(type->queryCharset()), LINK(type->queryCollation()));
        break;
    case type_data:
        rtlSubDataFTX(retLen, retPtr, srcLen, raw, low, high);
        break;
    case type_qstring:
        rtlSubQStrFTX(retLen, *(char * *)&retPtr, srcLen, (const char *)raw, low, high);
        break;
    case type_unicode:
        rtlUnicodeSubStrFTX(retLen, *(UChar * *)&retPtr, srcLen, (const UChar *)raw, low, high);
        break;
    case type_varunicode:
        rtlUnicodeSubStrFTX(retLen, *(UChar * *)&retPtr, srcLen, (const UChar *)raw, low, high);
        retType = makeUnicodeType(retLen, type->queryLocale());
        break;
    case type_utf8:
        rtlUtf8SubStrFTX(retLen, *(char * *)&retPtr, srcLen, (const char *)raw, low, high);
        break;
    default:
        UNIMPLEMENTED;
    }

    if (retType == NULL)
        retType = getStretchedType(retLen, type);
    IValue * ret = createValueFromMem(retType, retPtr);
    rtlFree(retPtr);
    return ret;
}

IValue * trimStringValue(IValue * v, char typecode)
{
    ITypeInfo * type = v->queryType();
    type_t tc = type->getTypeCode();
    if(isUnicodeType(type))
    {
        unsigned tlen = 0;
        rtlDataAttr resultstr;
        unsigned len = type->getStringLen();
        if (tc == type_utf8)
        {
            char const * str = (char const *)v->queryValue();
            switch(typecode) 
            {
            case 'A':
                rtlTrimUtf8All(tlen, resultstr.refstr(), len, str);
                break;
            case 'B':
                rtlTrimUtf8Both(tlen, resultstr.refstr(), len, str);
                break;
            case 'L':
                rtlTrimUtf8Left(tlen, resultstr.refstr(), len, str);
                break;
            default:
                rtlTrimUtf8Right(tlen, resultstr.refstr(), len, str);
                break;
            }

            ITypeInfo * newtype = makeUtf8Type(tlen, type->queryLocale());
            return createUtf8Value(tlen, resultstr.getstr(), newtype);
        }
        else
        {
            UChar const * str = (UChar const *)v->queryValue();
            switch(typecode) 
            {
            case 'A':
                rtlTrimUnicodeAll(tlen, resultstr.refustr(), len, str);
                break;
            case 'B':
                rtlTrimUnicodeBoth(tlen, resultstr.refustr(), len, str);
                break;
            case 'L':
                rtlTrimUnicodeLeft(tlen, resultstr.refustr(), len, str);
                break;
            default:
                rtlTrimUnicodeRight(tlen, resultstr.refustr(), len, str);
                break;
            }

            ITypeInfo * newtype = makeUnicodeType(tlen, v->queryType()->queryLocale());
            return createUnicodeValue(resultstr.getustr(), tlen, newtype);
        }
    }
    else
    {
        Owned<ITypeInfo> st = getStringType(type);
        Owned<ITypeInfo> asciiType = getAsciiType(st);
        Owned<IValue> sv = v->castTo(asciiType);
        StringBuffer s;
        sv->getStringValue(s);
        unsigned tlen = 0;
        rtlDataAttr resultstr;
        unsigned len = s.length();
        char const * str = s.str();
        switch(typecode)
        {
        case 'A':
            rtlTrimAll(tlen, resultstr.refstr(), len, str);
            break;
        case 'B':
            rtlTrimBoth(tlen, resultstr.refstr(), len, str);
            break;
        case 'L':
            rtlTrimLeft(tlen, resultstr.refstr(), len, str);
            break;
        default:
            rtlTrimRight(tlen, resultstr.refstr(), len, str);
            break;
        }

        ITypeInfo * newtype = makeStringType(tlen, LINK(asciiType->queryCharset()), LINK(asciiType->queryCollation()));
        return createStringValue(resultstr.getstr(), newtype);
    }
}

//---------------------------------------------------------------------
void getStringFromIValue(StringBuffer & s, IValue* val) 
{
    Owned<ITypeInfo> tp  = getStringType(val->queryType());
    Owned<IValue> sv = val->castTo(tp);
    sv->getStringValue(s);
}

void getStringFromIValue(unsigned & len, char* & str, IValue* val) 
{
    StringBuffer s;
    getStringFromIValue(s, val);
    len = s.length();
    str = s.detach();
}
//---------------------------------------------------------------------

IValue * concatValues(IValue * left, IValue * right)
{
    ITypeInfo * leftType = left->queryType();
    ITypeInfo * rightType = right->queryType();
    type_t ltc = leftType->getTypeCode();
    type_t rtc = rightType->getTypeCode();
    if(isUnicodeType(leftType))
    {
        assertex(isUnicodeType(rightType));
        assertex(leftType->queryLocale() == rightType->queryLocale());

        rtlDataAttr out;
        unsigned outlen;
        if (ltc == type_utf8 && rtc == type_utf8)
        {
            rtlConcatUtf8(outlen, out.addrstr(), leftType->getStringLen(), (char const *)left->queryValue(), rightType->getStringLen(), (char const *)right->queryValue(), -1);
            ITypeInfo * newtype = makeUtf8Type(outlen, leftType->queryLocale());
            return createUtf8Value(outlen, out.getstr(), newtype);
        }
        else
        {
            rtlConcatUnicode(outlen, out.addrustr(), leftType->getStringLen(), (UChar const *)left->queryValue(), rightType->getStringLen(), (UChar const *)right->queryValue(), -1);
            ITypeInfo * newtype = makeUnicodeType(outlen, leftType->queryLocale());
            return createUnicodeValue(out.getustr(), outlen, newtype);
        }
    }
    else
    {
        Owned<ITypeInfo> lt = getStringType(leftType);
        Owned<ITypeInfo> rt = getStringType(rightType);
        Owned<IValue> lv = left->castTo(lt);
        Owned<IValue> rv = right->castTo(rt);
        assertex(!isUnicodeType(rt));
        assertex(lt->queryCharset() == rt->queryCharset());
        assertex(lt->queryCollation() == rt->queryCollation());
        size32_t len = lt->getStringLen() + rt->getStringLen();
        StringBuffer s;
        lv->getStringValue(s);
        rv->getStringValue(s);
        if (ltc == type_varstring || rtc == type_varstring)
        {
            ITypeInfo * newtype = makeVarStringType(len);
            return createVarStringValue(len, s.str(), newtype);
        }
        else if (ltc == type_string || rtc == type_string)
        {
            ITypeInfo * newtype = makeStringType(len, LINK(lt->queryCharset()), LINK(lt->queryCollation()));
            return createStringValue(s.str(), newtype);
        }
        else
        {
            return createDataValue(s.str(), len);
        }
    }
}

IValue * binaryAndValues(IValue * left, IValue * right)
{
    IValue * ret;
    Owned<ITypeInfo> pnt = getBandType(left->queryType(), right->queryType());

    switch(pnt->getTypeCode())
    {
    case type_boolean:
        return createBoolValue(left->getBoolValue() && right->getBoolValue());
    case type_int:
    case type_swapint:
    case type_packedint:
        ret = createTruncIntValue(left->getIntValue() & right->getIntValue(), pnt.getClear());
        break;
    default:
        throwUnexpected();
    }
    return ret;
}

IValue * binaryOrValues(IValue * left, IValue * right)
{
    IValue * ret;
    Owned<ITypeInfo> pnt = getBorType(left->queryType(), right->queryType());

    switch(pnt->getTypeCode())
    {
    case type_boolean:
        return createBoolValue(left->getBoolValue() || right->getBoolValue());
    case type_int:
    case type_swapint:
    case type_packedint:
        ret = createTruncIntValue(left->getIntValue() | right->getIntValue(), pnt.getClear());
        break;
    default:
        throwUnexpected();
    }
    return ret;
}

IValue * binaryXorValues(IValue * left, IValue * right)
{
    IValue * ret;
    ITypeInfo * pnt = getPromotedNumericType(left->queryType(), right->queryType());

    switch(pnt->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        ret = createTruncIntValue(left->getIntValue() ^ right->getIntValue(), pnt);
        break;
    default:
        throwUnexpected();
    }
    return ret;
}

IValue * binaryNotValues(IValue * v)
{
    switch(v->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        return createTruncIntValue(~v->getIntValue(), v->getType());
    }
    throwUnexpected();
    return NULL;
}

IValue * logicalNotValues(IValue * v)
{
    return createBoolValue(!v->getBoolValue());
}

IValue * logicalAndValues(IValue * left, IValue * right)
{
    return createBoolValue(left->getBoolValue() && right->getBoolValue());
}

IValue * logicalOrValues(IValue * left, IValue * right)
{
    return createBoolValue(left->getBoolValue() || right->getBoolValue());
}

int orderValues(IValue * left, IValue * right)
{
    //The following line can be uncommented to check that the types are consistent everywhere
    //but remains commented out to improve resilience when the types are wrong.
//    return left->compare(right);

    Owned<ITypeInfo> pt = getPromotedCompareType(left->queryType(), right->queryType());
    Owned<IValue> lv = left->castTo(pt);
    Owned<IValue> rv = right->castTo(pt);
    return lv->compare(rv);
}

#define COMPARE_AND_RETURN(op)  \
        return createBoolValue(orderValues(left, right) op 0);

IValue * equalValues(IValue * left, IValue * right)
{
    COMPARE_AND_RETURN(==)
}

IValue * notEqualValues(IValue * left, IValue * right)
{
    COMPARE_AND_RETURN(!=)
}

IValue * lessValues(IValue * left, IValue * right)
{
    COMPARE_AND_RETURN(<)
}

IValue * lessEqualValues(IValue * left, IValue * right)
{
    COMPARE_AND_RETURN(<=)
}

IValue * greaterValues(IValue * left, IValue * right)
{
    COMPARE_AND_RETURN(>)
}

IValue * greaterEqualValues(IValue * left, IValue * right)
{
    COMPARE_AND_RETURN(>=)
}


IValue * shiftLeftValues(IValue * left, IValue * right)
{
    ITypeInfo * retType = left->getType();
    switch(retType->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        return createTruncIntValue(left->getIntValue() << right->getIntValue(), retType);
    default:
        UNIMPLEMENTED;
    }
}

IValue * shiftRightValues(IValue * left, IValue * right)
{
    ITypeInfo * retType = left->getType();
    switch(retType->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_packedint:
        if (retType->isSigned())
            return createTruncIntValue(((__int64)left->getIntValue()) >> right->getIntValue(), retType);
        else
            return createTruncIntValue(((unsigned __int64)left->getIntValue()) >> right->getIntValue(), retType);
    default:
        UNIMPLEMENTED;
    }
}

extern DEFTYPE_API void serializeValue(MemoryBuffer & target, IValue * value)
{
    ITypeInfo * type = value->queryType();
    type->serialize(target);
    void * buffer = target.reserve(type->getSize());
    value->toMem(buffer);
}

extern DEFTYPE_API IValue * deserializeValue(MemoryBuffer & source)
{
    Owned<ITypeInfo> type = deserializeType(source);
    const void * buffer = source.readDirect(type->getSize());
    return createValueFromMem(LINK(type), buffer);
}

