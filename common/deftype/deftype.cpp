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

#include "platform.h"

#include "jlib.hpp"
#include "jstream.hpp"
#include "jmisc.hpp"
#include "defvalue.hpp"
#include "jexcept.hpp"

#include "deftype.ipp"
#include <stdio.h>
#include <math.h>
#include <algorithm>

#include "rtlbcd.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"

//#define DATA_STRING_COMPATIBLE
#define HASHFIELD(p) hashcode = hashc((unsigned char *) &p, sizeof(p), hashcode)

static IAtom * asciiAtom;
static IAtom * dataAtom;
static IAtom * ebcdicAtom;
static IAtom * utf8Atom;
static IAtom * asciiCodepageAtom;
static IAtom * ebcdicCodepageAtom;
static IAtom * ascii2ebcdicAtom;
static IAtom * ebcdic2asciiAtom;
static IAtom * emptyAtom;
static CriticalSection * typeCS;
static TypeCache * globalTypeCache;

static CBoolTypeInfo *btt = NULL;
static CBlobTypeInfo *bltt = NULL;
static CVoidTypeInfo *vtt = NULL;
static CNullTypeInfo *ntt = NULL;
static CRecordTypeInfo *rtt = NULL;
static CAnyTypeInfo *anytt = NULL;
static CPatternTypeInfo *patt = NULL;
static CTokenTypeInfo *tokentt = NULL;
static CFeatureTypeInfo *featurett = NULL;
static CStringTypeToTypeMap * stt;
static CStringTypeToTypeMap * datatt;
static CStringTypeToTypeMap * vstt;
static CStringTypeToTypeMap * qstt;
static CUnicodeTypeToTypeMap * utt;
static CUnicodeTypeToTypeMap * vutt;
static CUnicodeTypeToTypeMap * u8tt;
static CIntTypeInfo *itt[2][8];
static CSwapIntTypeInfo *sitt[2][8];
static TypeToTypeMap * pitt;
static CRealTypeInfo *realtt[10];
static CBitfieldTypeInfo *bftt[64][8];
static CCharTypeInfo * ctt[2];
static TypeToTypeMap * setTT;

static CEventTypeInfo *ett = NULL;

//charssets and collation...
static ICharsetInfo * dataCharset;
static ICharsetInfo * asciiCharset;
static ICharsetInfo * ebcdicCharset;
static ICharsetInfo * utf8Charset;
static ICollationInfo * asciiCollation;
static ICollationInfo * ebcdicCollation;
static ITranslationInfo * ascii2ebcdic;
static ITranslationInfo * ebcdic2ascii;

//---------------------------------------------------------------------------

MODULE_INIT(INIT_PRIORITY_DEFTYPE)
{
    typeCS = new CriticalSection;
    asciiAtom = createLowerCaseAtom("ascii");
    dataAtom = createLowerCaseAtom("data");
    ebcdicAtom = createLowerCaseAtom("ebcdic");
    utf8Atom = createLowerCaseAtom("utf-8");
    asciiCodepageAtom = createLowerCaseAtom(ASCII_LIKE_CODEPAGE);
    ebcdicCodepageAtom = createLowerCaseAtom("IBM037");
    ascii2ebcdicAtom = createAtom("ascii2ebcdic");
    ebcdic2asciiAtom = createAtom("ebcdic2ascii");
    emptyAtom = createAtom("");
    globalTypeCache = new TypeCache;
    stt = new CStringTypeToTypeMap;
    datatt = new CStringTypeToTypeMap;
    vstt = new CStringTypeToTypeMap;
    qstt = new CStringTypeToTypeMap;
    utt = new CUnicodeTypeToTypeMap;
    vutt = new CUnicodeTypeToTypeMap;
    u8tt = new CUnicodeTypeToTypeMap;
    pitt = new TypeToTypeMap;
    setTT = new TypeToTypeMap;
    return 1;
}
MODULE_EXIT()
{
    ClearTypeCache();

    delete globalTypeCache;
    delete stt;
    delete datatt;
    delete vstt;
    delete qstt;
    delete utt;
    delete vutt;
    delete u8tt;
    delete pitt;
    delete setTT;
    delete typeCS;
}

//---------------------------------------------------------------------------

int cards[] = { 10, 100, 1000, 10000, 100000, 1000000, 10000000 };

unsigned promotedIntSize[9] = { 0, 1, 2, 4, 4, 8, 8, 8, 8 };

//===========================================================================

static IValue * castViaString(ITypeInfo * type, size32_t len, const char * text)
{
    Owned<IValue> temp = createStringValue(text, len);
    return temp->castTo(type);
}

static IValue * castViaString(ITypeInfo * type, bool isSignedValue, __int64 value, int len)
{
    Owned<ITypeInfo> stype = makeStringType(len, NULL, NULL);
    Owned<IValue> temp = createIntValue(value, 8, isSignedValue);
    Owned<IValue> temp2 = temp->castTo(stype);
    return temp2->castTo(type);
}

bool isAscii(ITypeInfo * type)
{
    return type->queryCharset()->queryName() == asciiAtom;
}

//===========================================================================

CTypeInfo::~CTypeInfo()
{
}

IValue * CTypeInfo::castFrom(double value)       
{ 
    //NB: Force VMT use...
    if (value < 0)
        return ((ITypeInfo *)this)->castFrom(true, (__int64)value); 
    return ((ITypeInfo *)this)->castFrom(false, (__int64)(unsigned __int64)value); 
}

IValue * CTypeInfo::castFrom(size32_t len, const UChar * text)
{
    unsigned bufflen;
    rtlDataAttr buff;
    rtlUnicodeToStrX(bufflen, buff.refstr(), len, text);
    return ((ITypeInfo *)this)->castFrom(bufflen, buff.getstr());
}

unsigned CTypeInfo::getCardinality()
{
    if (length < 4)
        return 1U << (length * 8);
    return (unsigned)-1;
}

unsigned CTypeInfo::getCrc()
{
    unsigned crc = getTypeCode();
    crc = hashc((const byte *)&length, sizeof(length), crc);
    return crc;
}

unsigned CTypeInfo::getHash() const
{
    unsigned hashcode = getHashKind();
    HASHFIELD(length);
    return hashcode;
}


bool CTypeInfo::equals(const CTypeInfo & other) const
{
    return (getHashKind() == other.getHashKind()) && (length == other.length);
}



void CHashedTypeInfo::addObserver(IObserver & observer)
{
    assertex(!observed);
    assert(&observer == globalTypeCache);
    observed = true;
}

void CHashedTypeInfo::removeObserver(IObserver & observer)
{
    assertex(observed);
    assert(&observer == globalTypeCache);
    observed = false;
}


void CHashedTypeInfo::beforeDispose()
{
    CriticalBlock block(*typeCS);
    if (observed)
        globalTypeCache->removeExact(this);
    assertex(!observed);
}




//===========================================================================

inline unsigned getPromotedBitfieldSize(unsigned len)
{
    if (len <= 8)
        return 1;
    else if (len <= 16)
        return 2;
    else if (len <= 32)
        return 4;
    else
        return 8;
}

inline ITypeInfo * getPromotedBitfieldType(unsigned len)
{
    return makeIntType(getPromotedBitfieldSize(len), false);
}

CBitfieldTypeInfo::CBitfieldTypeInfo(int _length, ITypeInfo * _baseType) : CTypeInfo((_length+7)/8), bitLength(_length) 
{
    storeType  = _baseType;
    promoted = getPromotedBitfieldType(_length);
}

bool CBitfieldTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_real: case type_int: case type_decimal: case type_swapint: case type_bitfield: case type_any: case type_packedint:
        return true;
    }
    return false;
}


IValue * CBitfieldTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    return createBitfieldValue(value, LINK(this));
}

IValue * CBitfieldTypeInfo::castFrom(size32_t len, const char * text)
{
    unsigned __int64 value;
    if (false)//typeIsSigned)
        value = rtlStrToInt8(len, text);
    else
        value = rtlStrToUInt8(len, text);
    return createBitfieldValue(value, LINK(this));
}

unsigned CBitfieldTypeInfo::getCrc()
{
    unsigned crc = CTypeInfo::getCrc();
    crc = hashc((const byte *)&bitLength, sizeof(bitLength), crc);
    unsigned childCrc = storeType->getCrc();
    crc = hashc((const byte *)&childCrc, sizeof(childCrc), crc);
    return crc;
}

StringBuffer &CBitfieldTypeInfo::getECLType(StringBuffer &out)
{
    out.append("bitfield").append(bitLength);
    if (storeType->getSize() != getPromotedBitfieldSize(bitLength))
        out.append("_").append(storeType->getSize());
    return out;
}

//---------------------------------------------------------------------------

bool CIntTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_real: case type_int: case type_decimal: case type_swapint: case type_bitfield: case type_any: case type_packedint:
        return true;
    }
    return false;
}

unsigned CIntTypeInfo::getStringLen(void)
{
    unsigned sign = isSigned() ? 1 : 0;
    return getDigits() + sign;
}

unsigned CIntTypeInfo::getDigits(void)
{
    switch (length)
    {
    case 1: return 3;
    case 2: return 5;
    case 3: return 8;
    case 4: return 10;
    case 5: return 13;
    case 6: return 15;
    case 7: return 17;
    case 8: return 20;
    }
    assertex(false);
    return 0;
}

unsigned CIntTypeInfo::getCrc()
{
    unsigned crc = CTypeInfo::getCrc();
    crc = hashc((const byte *)&typeIsSigned, sizeof(typeIsSigned), crc);
    return crc;
}

StringBuffer &CIntTypeInfo::getECLType(StringBuffer &out)
{
    if (!typeIsSigned)
        out.append("unsigned");
    else
        out.append("integer");
    if(length >0)
        out.append(length);
    return out;
}

//---------------------------------------------------------------------------

StringBuffer &CSwapIntTypeInfo::getECLType(StringBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.append("big_endian ");
#else
    out.append("little_endian ");
#endif
    if (!typeIsSigned)
        out.append("unsigned ");
    out.append("integer");
    if(length >0)
        out.append(length);
    return out;
}

//---------------------------------------------------------------------------

bool CPackedIntTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_real: case type_int: case type_decimal: case type_swapint: case type_bitfield: case type_any: case type_packedint:
        return true;
    }
    return false;
}

unsigned CPackedIntTypeInfo::getCrc()
{
    return CTypeInfo::getCrc() ^ basetype->getCrc();
}

StringBuffer &CPackedIntTypeInfo::getECLType(StringBuffer &out)
{
    out.append("packed ");
    if (!isSigned())
        out.append("unsigned ");
    out.append("integer");
    out.append(basetype->getSize());
    return out;
}

IValue * CPackedIntTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    return createTruncIntValue(value, LINK(this));
}

IValue * CPackedIntTypeInfo::castFrom(size32_t len, const char * text)
{
    unsigned __int64 value;
    if (basetype->isSigned())
        value = rtlStrToInt8(len, text);
    else
        value = rtlStrToUInt8(len, text);
    return createTruncIntValue(value, LINK(this));
}

//---------------------------------------------------------------------------

unsigned CRealTypeInfo::getStringLen(void)
{
    switch (length)
    {               // sign + digits + dot + E+-<digits>
    case 4: return 1 + FLOAT_SIG_DIGITS + 1 + 4;
    case 8: return 1 + DOUBLE_SIG_DIGITS + 1 + 5;
    }
    assertex(false);
    return 0;
}

unsigned CRealTypeInfo::getDigits(void)
{
    switch (length)
    {
    case 4: return FLOAT_SIG_DIGITS;
    case 8: return DOUBLE_SIG_DIGITS;
    }
    assertex(false);
    return 0;
}

IValue * CRealTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    if (isSignedValue)
        return createRealValue((double)value, LINK(this));
    return createRealValue((double)(unsigned __int64)value, LINK(this));
}

IValue * CRealTypeInfo::castFrom(double value)
{ 
    return createRealValue(value, LINK(this));
}

IValue * CRealTypeInfo::castFrom(size32_t len, const char * text)
{
    return createRealValue(rtlStrToReal(len, text), LINK(this));
}

StringBuffer &CRealTypeInfo::getECLType(StringBuffer &out)
{
    out.append("real");
    if(length >0)
        out.append(length);
    return out;
}

bool CRealTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_real: case type_int: case type_decimal: case type_swapint: case type_bitfield: case type_any: case type_packedint:
        return true;
    }
    return false;
}

//---------------------------------------------------------------------------

inline unsigned cvtDigitsToLength(bool isSigned, unsigned digits)
{
    if (digits == UNKNOWN_LENGTH)
        return UNKNOWN_LENGTH;
    return isSigned ? digits/2+1 : (digits+1)/2;
}

CDecimalTypeInfo::CDecimalTypeInfo(unsigned _digits, unsigned _prec, bool _isSigned)
: CHashedTypeInfo(cvtDigitsToLength(_isSigned, _digits))
{
    digits = (_digits == UNKNOWN_LENGTH) ? UNKNOWN_DIGITS : (byte)_digits;
    prec = (_prec == UNKNOWN_LENGTH) ? UNKNOWN_DIGITS : (byte)_prec;
    typeIsSigned = _isSigned;
};

IValue * CDecimalTypeInfo::createValueFromStack()
{
    Linked<ITypeInfo> retType;
    if ((length == UNKNOWN_LENGTH) || (length == MAX_DECIMAL_LEADING + MAX_DECIMAL_PRECISION))
    {
        unsigned tosDigits, tosPrecision;
        DecClipInfo(tosDigits, tosPrecision);
        unsigned tosLeading = tosDigits - tosPrecision;
        if (tosLeading > MAX_DECIMAL_LEADING)
            tosLeading = MAX_DECIMAL_LEADING;
        if (tosPrecision > MAX_DECIMAL_PRECISION)
            tosPrecision = MAX_DECIMAL_PRECISION;
        Owned<ITypeInfo> newType = makeDecimalType(tosLeading+tosPrecision, tosPrecision, typeIsSigned);
        return createDecimalValueFromStack(newType);
    }

    void * val = alloca(length);
    DecSetPrecision(getDigits(), prec);
    if (typeIsSigned)
        DecPopDecimal(val, length, prec);
    else
        DecPopUDecimal(val, length, prec);
    return createDecimalValue(val, LINK(this));
}

IValue * CDecimalTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    BcdCriticalBlock bcdBlock;
    
    if (isSignedValue)
        DecPushInt64(value);
    else
        DecPushUInt64(value);
    return createValueFromStack();
}

IValue * CDecimalTypeInfo::castFrom(double value)
{ 
    DecPushReal(value);
    return createValueFromStack();
}

IValue * CDecimalTypeInfo::castFrom(size32_t len, const char * text)
{
    DecPushString(len, text);
    return createValueFromStack();
}

unsigned CDecimalTypeInfo::getHash() const
{
    unsigned hashcode = CHashedTypeInfo::getHash();
    HASHFIELD(prec);
    HASHFIELD(digits);
    HASHFIELD(typeIsSigned);
    return hashcode;
}


bool CDecimalTypeInfo::equals(const CTypeInfo & _other) const
{
    if (!CHashedTypeInfo::equals(_other))
        return false;
    const CDecimalTypeInfo & other = static_cast<const CDecimalTypeInfo &>(_other);
    return (prec == other.prec) && (digits == other.digits) && (typeIsSigned == other.typeIsSigned);
}



unsigned CDecimalTypeInfo::getStringLen(void)
{
    if (length == UNKNOWN_LENGTH)
        return UNKNOWN_LENGTH;
    return (typeIsSigned ? 1 : 0) + getDigits() + (prec ? 1 : 0); // sign + digits + dot
}

StringBuffer &CDecimalTypeInfo::getECLType(StringBuffer &out)
{
    if (!typeIsSigned)
        out.append('u');
    out.append("decimal");
    if (digits != UNKNOWN_DIGITS)
    {
        out.append((int)digits);
        if (prec)
            out.append("_").append((int)prec);
    }
    return out;
}

bool CDecimalTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_real: case type_int: case type_decimal: case type_swapint: case type_any: case type_packedint:
        return true;
    }
    return false;
}

unsigned CDecimalTypeInfo::getBitSize()
{
    if (digits == UNKNOWN_DIGITS)
        return UNKNOWN_LENGTH;

    if (typeIsSigned)
        return (digits+1)*4;
    else
        return digits*4;
};

unsigned CDecimalTypeInfo::getCrc()
{
    unsigned crc = CTypeInfo::getCrc();
    crc = hashc((const byte *)&typeIsSigned, sizeof(typeIsSigned), crc);
    crc = hashc((const byte *)&prec, sizeof(prec), crc);
    crc = hashc((const byte *)&digits, sizeof(digits), crc);
    return crc;
}


void CDecimalTypeInfo::serialize(MemoryBuffer &tgt)
{
    CTypeInfo::serialize(tgt);
    tgt.append(prec);
    tgt.append(digits);
    tgt.append(typeIsSigned);
}

//---------------------------------------------------------------------------
bool CBoolTypeInfo::assignableFrom(ITypeInfo *t2)
{
    // FIX: only bool or int type can be assigned to a bool type
    //return (t2->isScalar());
    switch(t2->getTypeCode())
    {
    case type_boolean:
    case type_any:
        return true;
    }

    return false;
}

IValue * CBoolTypeInfo::castFrom(bool /*isSignedValue*/, __int64 value)
{
    return createBoolValue(value != 0);
}

IValue * CBoolTypeInfo::castFrom(size32_t len, const char * text)
{
    return castViaString(this, len, text);
}

StringBuffer &CBoolTypeInfo::getECLType(StringBuffer &out)
{
    return out.append("boolean");
}

//---------------------------------------------------------------------------

IValue * CVoidTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    return NULL;
}

IValue * CVoidTypeInfo::castFrom(size32_t len, const char * text)
{
    return NULL;
}

StringBuffer &CVoidTypeInfo::getECLType(StringBuffer &out)
{
    return out;
}


//---------------------------------------------------------------------------

//Make crc match void for backward compatibility
unsigned CNullTypeInfo::getCrc()
{
    unsigned crc = type_void;
    crc = hashc((const byte *)&length, sizeof(length), crc);
    return crc;
}

//---------------------------------------------------------------------------

IValue * CAnyTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    return createIntValue(value, sizeof(__int64), isSignedValue);
}

IValue * CAnyTypeInfo::castFrom(size32_t len, const char * text)
{
    return createStringValue(text, len);
}

IValue * CAnyTypeInfo::castFrom(double value)
{
    return createRealValue(value, sizeof(double));
}

StringBuffer &CAnyTypeInfo::getECLType(StringBuffer &out)
{
    return out.append("any");
}


//---------------------------------------------------------------------------

CStringTypeInfo::CStringTypeInfo(unsigned _length, ICharsetInfo * _charset, ICollationInfo * _collation) : CTypeInfo(_length), charset(_charset), collation(_collation)
{
    if (!charset)
        charset.setown(getCharset(NULL));
    if (!collation)
        collation.set(charset->queryDefaultCollation());
}

unsigned cardGuesses[] = { 0, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

unsigned CStringTypeInfo::getCardinality()
{
    // Wild guess at cardinalities. Probably more accurate than the default integer variety...
    unsigned numChars = getStringLen();
    if (numChars < 10)
        return cardGuesses[numChars];
    return (unsigned)-1;
}

bool CStringTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_string: case type_varstring:
    case type_qstring: case type_any:
#ifdef DATA_STRING_COMPATIBLE
    case type_data:
#endif
        return true;
    }
    return false;
}

IValue * CStringTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    return castViaString(this, isSignedValue, value, getStringLen());
}

IValue * CStringTypeInfo::castFrom(double value)
{
    char * text = NULL;
    unsigned length = getStringLen();
    if (length == UNKNOWN_LENGTH)
    {
        rtlRealToStrX(length, text, value);
    }
    else
    {
        text = (char *)malloc(length);
        rtlRealToStr(length, text, value);
    }
    IValue * ret = castFrom(length, text);
    free(text);
    return ret;
}

IValue * CStringTypeInfo::castFrom(size32_t len, const char * text)
{
    Owned<ICharsetInfo> ascii = getCharset(asciiAtom);
    return createStringValue(text, LINK(this), len, ascii);
}

unsigned CStringTypeInfo::getCrc()
{
    unsigned crc = CTypeInfo::getCrc();
    if (charset)
    {
        const char * name = str(charset->queryName());
        //MORE: This and following should really be case insensitive, but we get away with it at the moment because the atoms are created very early
        crc = hashc((const byte *)name, (size32_t)strlen(name), crc);
    }
    if (collation)
    {
        const char * name = str(collation->queryName());
        crc = hashc((const byte *)name, (size32_t)strlen(name), crc);
    }
    return crc;
}

StringBuffer &CStringTypeInfo::getECLType(StringBuffer &out)
{
    if (charset->queryName() == ebcdicAtom)
        out.append("EBCDIC ");
    out.append("string");
    if (length != UNKNOWN_LENGTH)
        out.append(length);
    return out;
}

StringBuffer &CStringTypeInfo::getDescriptiveType(StringBuffer &out)
{
    out.append("string");
    if (length != UNKNOWN_LENGTH)
        out.append(length);
    if(charset || collation)
    {
        out.append(" (");
        if(charset) out.append("charset:").append(str(charset->queryName()));
        if(charset && collation) out.append(", ");
        if(collation) out.append("collation:").append(str(collation->queryName()));
        out.append(")");
    }
    return out;
}

//---------------------------------------------------------------------------

unsigned CUnicodeTypeInfo::getCardinality()
{
    // Wild guess at cardinalities. Probably more accurate than the default integer variety...
    if (length < 19)
        return cardGuesses[(length+1)/2];
    return (unsigned)-1;
}

IValue * CUnicodeTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    Owned<ITypeInfo> asciiType = makeStringType(getStringLen(), 0, 0);
    Owned<IValue> asciiValue = asciiType->castFrom(isSignedValue, value);
    return asciiValue->castTo(this);
}

IValue * CUnicodeTypeInfo::castFrom(double value)
{
    Owned<ITypeInfo> asciiType = makeStringType(getStringLen(), 0, 0);
    Owned<IValue> asciiValue = asciiType->castFrom(value);
    return asciiValue->castTo(this);
}

IValue * CUnicodeTypeInfo::castFrom(size32_t len, const char * text)
{
    Owned<IValue> unicodeValue = createUnicodeValue(text, len, str(locale), false);
    return unicodeValue->castTo(this);
}

IValue * CUnicodeTypeInfo::castFrom(size32_t len, const UChar * uchars)
{
    return createUnicodeValue(len, uchars, LINK(this));
}

StringBuffer &CUnicodeTypeInfo::getECLType(StringBuffer &out)
{
    out.append("unicode");
    if(locale && *str(locale))
        out.append('_').append(str(locale));
    if(length != UNKNOWN_LENGTH)
        out.append(length/2);
    return out;
}

bool CUnicodeTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_any:
        return true;
    }
    //All string types can be converted to unicode with no loss of information, so allow an assign without a cast
    if (isStringType(t2))
        return true;

    return isUnicodeType(t2) && haveCommonLocale(this, t2);
}

//---------------------------------------------------------------------------

CVarUnicodeTypeInfo::CVarUnicodeTypeInfo(unsigned len, IAtom * _locale) : CUnicodeTypeInfo(len, _locale)
{
#if UNKNOWN_LENGTH != 0
    assertex(len != 0);
#endif
};

IValue * CVarUnicodeTypeInfo::castFrom(size32_t len, const char * text)
{
    Owned<IValue> unicodeValue = createVarUnicodeValue(text, len, str(locale), false);
    return unicodeValue->castTo(this);
}

IValue * CVarUnicodeTypeInfo::castFrom(size32_t len, const UChar * uchars)
{
    return createVarUnicodeValue(len, uchars, LINK(this));
}

StringBuffer & CVarUnicodeTypeInfo::getECLType(StringBuffer & out)
{
    out.append("varunicode");
    if(locale && *str(locale))
        out.append('_').append(str(locale));
    if(length != UNKNOWN_LENGTH)
        out.append(length/2-1);
    return out;
}

//---------------------------------------------------------------------------

IValue * CUtf8TypeInfo::castFrom(size32_t len, const UChar * uchars)
{
    unsigned tlen = getStringLen();
    if (tlen == UNKNOWN_LENGTH)
        tlen = len;

    rtlDataAttr buff(tlen * 4);
    rtlUnicodeToUtf8(tlen, buff.getstr(), len, uchars);
    return createUtf8Value(tlen, buff.getstr(), LINK(this));
}

StringBuffer &CUtf8TypeInfo::getECLType(StringBuffer &out)
{
    out.append("utf8");
    if(locale && *str(locale))
        out.append('_').append(str(locale));
    if(length != UNKNOWN_LENGTH)
        out.append('_').append(length/4);
    return out;
}

//---------------------------------------------------------------------------

CDataTypeInfo::CDataTypeInfo(int _length) : CStringTypeInfo(_length, getCharset(dataAtom), NULL) 
{
}

StringBuffer &CDataTypeInfo::getECLType(StringBuffer &out)
{
    if(length != UNKNOWN_LENGTH && length != INFINITE_LENGTH)
        return out.append("data").append(length);
    else
        return out.append("data");
}

bool CDataTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_data:
    case type_any:
#ifdef DATA_STRING_COMPATIBLE
    case type_string: case type_varstring: 
#endif
        return true;
    }
    return false;
}

IValue * CDataTypeInfo::castFrom(size32_t len, const char * text)
{
    if (length == UNKNOWN_LENGTH)
        return createDataValue(text, len);
    return createDataValue(text, LINK(this), len);
}


//---------------------------------------------------------------------------

CVarStringTypeInfo::CVarStringTypeInfo(unsigned len, ICharsetInfo * _charset, ICollationInfo * _collation) : 
CStringTypeInfo(len, _charset, _collation) 
{
#if UNKNOWN_LENGTH != 0
    assertex(len != 0);
#endif
};

IValue * CVarStringTypeInfo::castFrom(size32_t len, const char * text)
{
    Owned<ICharsetInfo> ascii = getCharset(asciiAtom);
    return createVarStringValue(text, LINK(this), len, ascii);
}

StringBuffer &CVarStringTypeInfo::getECLType(StringBuffer &out)
{
    out.append("varstring");
    if (length != UNKNOWN_LENGTH)
        out.append(length-1);
    return out;
}

StringBuffer &CVarStringTypeInfo::getDescriptiveType(StringBuffer &out)
{
    out.append("varstring");
    if (length != UNKNOWN_LENGTH)
        out.append(length-1);
    if(charset || collation)
    {
        out.append(" (");
        if(charset) out.append("charset:").append(str(charset->queryName()));
        if(charset && collation) out.append(", ");
        if(collation) out.append("collation:").append(str(collation->queryName()));
        out.append(")");
    }
    return out;
}

//---------------------------------------------------------------------------

CQStringTypeInfo::CQStringTypeInfo(unsigned _strLength) : CStringTypeInfo(_strLength == UNKNOWN_LENGTH ? UNKNOWN_LENGTH : rtlQStrSize(_strLength), NULL, NULL)
{
    strLength = _strLength;
}

#if 0
bool CQStringTypeInfo::assignableFrom(ITypeInfo *t2)  
{ 
    switch (t2->getTypeCode())
    {
    case type_qstring:
    case type_any:
        return true;
    }
    return false;
}
#endif

IValue * CQStringTypeInfo::castFrom(size32_t len, const char * text)
{
    if (length != UNKNOWN_LENGTH)
    {
        if (len >= strLength)
            len = strLength;
    }
    return createQStringValue(len, text, LINK(this));
}

StringBuffer &CQStringTypeInfo::getECLType(StringBuffer &out)
{
    out.append("qstring");
    if (length != UNKNOWN_LENGTH)
        out.append(strLength);
    return out;
}

//---------------------------------------------------------------------------

IValue * CCharTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    return castViaString(this, isSignedValue, value, 1);
}

StringBuffer &CCharTypeInfo::getECLType(StringBuffer &out)
{
    assertex(false);
    return out;
}

IValue * CCharTypeInfo::castFrom(size32_t len, const char * text)
{
    return createCharValue(len ? *text : ' ', caseSensitive);
}

//---------------------------------------------------------------------------

IValue * CIntTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    return createTruncIntValue(value, LINK(this));
}

IValue * CIntTypeInfo::castFrom(size32_t len, const char * text)
{
    unsigned __int64 value;
    if (typeIsSigned)
        value = rtlStrToInt8(len, text);
    else
        value = rtlStrToUInt8(len, text);
    return createTruncIntValue(value, LINK(this));
}

//===========================================================================

class StringValueMapper : public Mapping
{
    size32_t index;
public:
    StringValueMapper(const void *k, int ksize, size32_t index);
    size32_t getIndex() { return index; }
};

StringValueMapper::StringValueMapper(const void *_key, int _ksize, size32_t _index) : Mapping(_key, _ksize)
{
    index = _index;
}

static int getIntSize(size32_t range)
{
    if (range <= 0xff)
        return 1;
    else if (range <= 0xffff)
        return 2;
    else
        return 4;
}

//===========================================================================

CEnumeratedTypeInfo::CEnumeratedTypeInfo(ITypeInfo *_base, size32_t _numValues)
    : CTypeInfo(getIntSize(_numValues)), valueMap(_base->getSize(), false), base(_base)
{
    numValues = _numValues;
//  valueMap.setCapacity(_numValues);
}

StringBuffer &CEnumeratedTypeInfo::getECLType(StringBuffer &out)
{
    assertex(false);
    return out;
}

ITypeInfo *CEnumeratedTypeInfo::getTypeInfo()
{
    Link();
    return this;
}

int CEnumeratedTypeInfo::addValue(IValue *val, size32_t frequency)
{
    assertex(!IsShared());
    size32_t baseSize = base->getSize();
    assertex(val->queryType()==base);
    void *buf = malloc(baseSize);
    val->toMem(buf);
    size32_t index = valueList.length();
    valueMap.addOwn(*new StringValueMapper(buf, baseSize, index));
    valueList.append(*val);
    free(buf);
    return index;
}

IValue *CEnumeratedTypeInfo::castFrom(bool isSignedValue, __int64 value)
{
    return createEnumValue((int) value, LINK(this));
}

IValue *CEnumeratedTypeInfo::castFrom(size32_t len, const char * text)
{
    MemoryAttr temp;
    size32_t baselen = base->getSize();
    if (len<baselen)
    {
        char *pad = (char *)temp.allocate(baselen);
        memcpy_iflen(pad, text, len);
        memset(pad+len, ' ', baselen-len);
        text = pad;
    }
    StringValueMapper *ret = (StringValueMapper *) valueMap.find(text);
    if (ret)
        return createEnumValue(ret->getIndex(), LINK(this));
    else
    {
//      assertex(false);
        return NULL;    // MORE - is this right?
    }
}

IValue *CEnumeratedTypeInfo::queryValue(size32_t index)
{
    if (valueList.isItem(index))
        return (IValue *) &valueList.item(index);
    else
        return NULL;
}

ITypeInfo *CEnumeratedTypeInfo::queryBase()
{
    return base;
}

extern DEFTYPE_API IEnumeratedTypeBuilder *makeEnumeratedTypeBuilder(ITypeInfo *base, aindex_t numValues)
{
    return new CEnumeratedTypeInfo(base, numValues);
}

//===========================================================================

ITypeInfo *CBasedTypeInfo::queryChildType()
{
    return basetype;
}

bool CBasedTypeInfo::assignableFrom(ITypeInfo *t2)
{
    return getTypeCode()==t2->getTypeCode() && queryChildType()==t2->queryChildType();
}

unsigned CBasedTypeInfo::getHash() const
{
    unsigned hashcode = CHashedTypeInfo::getHash();
    HASHFIELD(basetype);
    return hashcode;
}


bool CBasedTypeInfo::equals(const CTypeInfo & _other) const
{
    if (!CHashedTypeInfo::equals(_other))
        return false;
    const CBasedTypeInfo & other = static_cast<const CBasedTypeInfo &>(_other);
    return basetype == other.basetype;
}


void CBasedTypeInfo::serializeSkipChild(MemoryBuffer &tgt)
{
    CTypeInfo::serialize(tgt); 
    ITypeInfo * child = basetype ? basetype->queryChildType() : NULL;
    serializeType(tgt, child); 

}


//===========================================================================

IValue * CKeyedIntTypeInfo::castFrom(bool /*isSignedValue*/, __int64 value)
{
    return createIntValue(value, LINK(this));
}

//===========================================================================

bool CTransformTypeInfo::assignableFrom(ITypeInfo *t2)
{
    if (getTypeCode()==t2->getTypeCode())
    {
        ITypeInfo *c1 = queryChildType();
        ITypeInfo *c2 = t2->queryChildType();
        if (c1==NULL || c2==NULL || c1->assignableFrom(c2))
            return true;
    }
    return false;
}

bool CSortListTypeInfo::assignableFrom(ITypeInfo *t2)
{
    return (getTypeCode()==t2->getTypeCode());
/*
    {
        //Not convinced any of this should be here...
        ITypeInfo *c1 = queryChildType();
        ITypeInfo *c2 = t2->queryChildType();
        if (c1 == c2)
            return true;
        if (!c1 || !c2)
            return false;
        if (c1->assignableFrom(c2))
            return true;
        return false;
    }
    return queryChildType()->assignableFrom(t2);
*/
}

bool CRowTypeInfo::assignableFrom(ITypeInfo *t2)
{
    if (getTypeCode()==t2->getTypeCode())
    {
        ITypeInfo *c1 = queryChildType();
        ITypeInfo *c2 = t2->queryChildType();
        if (c1==NULL || c2==NULL || c1->assignableFrom(c2))
            return true;
    }
    return false;
}

//===========================================================================

bool CDictionaryTypeInfo::assignableFrom(ITypeInfo *t2)
{
    if (getTypeCode()==t2->getTypeCode())
    {
        ITypeInfo *c1 = queryChildType();
        ITypeInfo *c2 = t2->queryChildType();
        if (c1==NULL || c2==NULL || c1->assignableFrom(c2))
            return true;
    }
    return false;
}

StringBuffer & CDictionaryTypeInfo::getECLType(StringBuffer & out)
{
    ITypeInfo * recordType = ::queryRecordType(this);
    out.append(queryTypeName());
    if (recordType)
    {
        out.append(" of ");
        recordType->getECLType(out);
    }
    return out;
}

void CDictionaryTypeInfo::serialize(MemoryBuffer &tgt)
{
    CBasedTypeInfo::serializeSkipChild(tgt);
}

//===========================================================================

bool CTableTypeInfo::assignableFrom(ITypeInfo *t2)
{
    if (getTypeCode()==t2->getTypeCode())
    {
        ITypeInfo *c1 = queryChildType();
        ITypeInfo *c2 = t2->queryChildType();
        if (c1==NULL || c2==NULL || c1->assignableFrom(c2))
            return true;
    }
    return false;
}

StringBuffer & CTableTypeInfo::getECLType(StringBuffer & out)   
{ 
    ITypeInfo * recordType = ::queryRecordType(this);
    out.append(queryTypeName());
    if (recordType)
    {
        out.append(" of ");
        recordType->getECLType(out);
    }
    return out; 
}


void CTableTypeInfo::serialize(MemoryBuffer &tgt)
{
    CBasedTypeInfo::serializeSkipChild(tgt);
}

bool CGroupedTableTypeInfo::assignableFrom(ITypeInfo *t2)
{
    if (getTypeCode()==t2->getTypeCode())
    {
        ITypeInfo *c1 = queryChildType();
        ITypeInfo *c2 = t2->queryChildType();
        if (!c1 || !c2)
            return c1==c2;
        if (c1->assignableFrom(c2))
            return true;
    }
    return false;
}

bool CSetTypeInfo::assignableFrom(ITypeInfo *t2)
{
    return getTypeCode()==t2->getTypeCode() && 
        (!queryChildType() || !t2->queryChildType() || queryChildType()->assignableFrom(t2->queryChildType()));
}

StringBuffer & CSetTypeInfo::getECLType(StringBuffer & out) 
{ 
    out.append(queryTypeName());
    if (basetype)
    {
        out.append(" of "); 
        queryChildType()->getECLType(out); 
    }
    else
        out.append(" of any");

    return out; 
}

//===========================================================================

bool CFunctionTypeInfo::assignableFrom(ITypeInfo *t2)
{
    return this == t2;
}

IInterface *CFunctionTypeInfo::queryModifierExtra()
{
    return static_cast<IFunctionTypeExtra *>(this);
}

unsigned CFunctionTypeInfo::getHash() const
{
    unsigned hashcode = CBasedTypeInfo::getHash();
    HASHFIELD(parameters);
    HASHFIELD(defaults);
    return hashcode;
}


bool CFunctionTypeInfo::equals(const CTypeInfo & _other) const
{
    if (!CBasedTypeInfo::equals(_other))
        return false;
    const CFunctionTypeInfo & other = static_cast<const CFunctionTypeInfo &>(_other);
    return (parameters == other.parameters) && (defaults == other.defaults);
}



void CFunctionTypeInfo::serialize(MemoryBuffer &tgt)
{ 
    throwUnexpected();
    CBasedTypeInfo::serialize(tgt);
}

//===========================================================================


size32_t CArrayTypeInfo::getSize()                  
{ 
    if (length == UNKNOWN_LENGTH)
        return UNKNOWN_LENGTH;
    if (basetype->isReference())
        return length * sizeof(void *);
    size32_t baseSize = basetype->getSize();
    if (baseSize == UNKNOWN_LENGTH)
        return UNKNOWN_LENGTH;
    return baseSize * length;
}

//===========================================================================

unsigned CModifierTypeInfo::getHash() const
{
    unsigned hashcode = CHashedTypeInfo::getHash();
    HASHFIELD(baseType);
    HASHFIELD(kind);
    HASHFIELD(extra);
    return hashcode;
}


bool CModifierTypeInfo::equals(const CTypeInfo & _other) const
{
    if (!CHashedTypeInfo::equals(_other))
        return false;
    const CModifierTypeInfo & other = static_cast<const CModifierTypeInfo &>(_other);
    return (baseType == other.baseType) && (kind == other.kind) && (extra == other.extra);
}


//===========================================================================

extern DEFTYPE_API ITypeInfo *makeStringType(unsigned len, ICharsetInfo * charset, ICollationInfo * collation)
{
    if (!charset)
        charset = getCharset(NULL);
    if (!collation)
    {
        collation = charset->queryDefaultCollation();
        collation->Link();
    }

    CStringTypeKey key;
    key.length = len;
    key.charset.set(charset);
    key.collation.set(collation);

    CriticalBlock procedure(*typeCS);
    ITypeInfo *ret;
    IInterface * * match = stt->getValue(key);
    if (match)
    {
        ::Release(charset);
        ::Release(collation);
        ret = (ITypeInfo *)LINK(*match);
    }
    else
    {
        CStringTypeInfo* t = new CStringTypeInfo(len, charset, collation);
        ret = t;
        stt->setValue(key, ret);
    }
    return ret;
}

extern DEFTYPE_API ITypeInfo *makeVarStringType(unsigned len, ICharsetInfo * charset, ICollationInfo * collation)
{
    //NB: Length passed is the number of characters....
    unsigned size = (len != UNKNOWN_LENGTH) ? len + 1 : UNKNOWN_LENGTH;

    //NB: Length passed is the number of characters....
    if (!charset)
        charset = getCharset(NULL);
    if (!collation)
    {
        collation = charset->queryDefaultCollation();
        collation->Link();
    }

    CStringTypeKey key;
    key.length = size;
    key.charset.set(charset);
    key.collation.set(collation);

    CriticalBlock procedure(*typeCS);
    ITypeInfo *ret;
    IInterface * * match = vstt->getValue(key);
    if (match)
    {
        ::Release(charset);
        ::Release(collation);
        ret = (ITypeInfo *)LINK(*match);
    }
    else
    {
        ret = new CVarStringTypeInfo(size, charset, collation);
        vstt->setValue(key, ret);
    }
    return ret;
}


extern DEFTYPE_API ITypeInfo *makeQStringType(int len)
{
    CStringTypeKey key;
    key.length = len;
    key.charset.set(NULL);
    key.collation.set(NULL);

    CriticalBlock procedure(*typeCS);
    ITypeInfo *ret;
    IInterface * * match = qstt->getValue(key);
    if (match)
    {
        ret = (ITypeInfo *)LINK(*match);
    }
    else
    {
        ret = new CQStringTypeInfo(len);
        qstt->setValue(key, ret);
    }
    return ret;
}

extern DEFTYPE_API ITypeInfo *makeUnicodeType(unsigned len, IAtom * locale)
{
    if(!locale)
        locale = emptyAtom;
    CUnicodeTypeKey key;
    key.length = len;
    key.locale.set(locale);

    CriticalBlock procedure(*typeCS);
    ITypeInfo *ret;
    IInterface * * match = utt->getValue(key);
    if(match)
        ret = (ITypeInfo *)LINK(*match);
    else
    {
        if(len == UNKNOWN_LENGTH)
            ret = new CUnicodeTypeInfo(UNKNOWN_LENGTH, locale);
        else
            ret = new CUnicodeTypeInfo(len*2, locale);
        utt->setValue(key, ret);
    }
    return ret;
}

extern DEFTYPE_API ITypeInfo *makeVarUnicodeType(unsigned len, IAtom * locale)
{
    if(!locale)
        locale = emptyAtom;

    CUnicodeTypeKey key;
    key.length = len;
    key.locale.set(locale);

    CriticalBlock procedure(*typeCS);
    ITypeInfo *ret;
    IInterface * * match = vutt->getValue(key);
    if(match)
        ret = (ITypeInfo *)LINK(*match);
    else
    {
        if(len == UNKNOWN_LENGTH)
            ret = new CVarUnicodeTypeInfo(UNKNOWN_LENGTH, locale);
        else
            ret = new CVarUnicodeTypeInfo((len+1)*2, locale);
        vutt->setValue(key, ret);
    }
    return ret;
}

extern DEFTYPE_API ITypeInfo *makeUtf8Type(unsigned len, IAtom * locale)
{
    if(!locale)
        locale = emptyAtom;
    CUnicodeTypeKey key;
    key.length = len;
    key.locale.set(locale);

    CriticalBlock procedure(*typeCS);
    ITypeInfo *ret;
    IInterface * * match = u8tt->getValue(key);
    if(match)
        ret = (ITypeInfo *)LINK(*match);
    else
    {
        if (len == UNKNOWN_LENGTH)
            ret = new CUtf8TypeInfo(UNKNOWN_LENGTH, locale);
        else
            ret = new CUtf8TypeInfo(len*4, locale);
        u8tt->setValue(key, ret);
    }
    return ret;
}

extern DEFTYPE_API ITypeInfo *makeDataType(int len)
{
    CStringTypeKey key;
    key.length = len;
    key.charset.set(NULL);
    key.collation.set(NULL);

    CriticalBlock procedure(*typeCS);
    ITypeInfo *ret;
    IInterface * * match = datatt->getValue(key);
    if (match)
    {
        ret = (ITypeInfo *)LINK(*match);
    }
    else
    {
        ret = new CDataTypeInfo(len);
        datatt->setValue(key, ret);
    }
    return ret;
}

extern DEFTYPE_API ITypeInfo *makeIntType(int len, bool isSigned)
{
    assertex(len>0 && len <= 8);
    if (len <= 0 || len > 8)
        return NULL;

    CriticalBlock procedure(*typeCS);
    CIntTypeInfo *ret = itt[isSigned][len-1];
    if (ret==NULL)
        ret = itt[isSigned][len-1] = new CIntTypeInfo(len, isSigned);
    ::Link(ret);
    return ret;
}

extern DEFTYPE_API ITypeInfo *makeSwapIntType(int len, bool isSigned)
{
    assertex(len>0 && len <= 8);
    if (len <= 0 || len > 8)
        return NULL;

    CriticalBlock procedure(*typeCS);
    CIntTypeInfo *ret = sitt[isSigned][len-1];
    if (ret==NULL)
        ret = sitt[isSigned][len-1] = new CSwapIntTypeInfo(len, isSigned);
    ::Link(ret);
    return ret;
}

extern DEFTYPE_API ITypeInfo *makePackedIntType(ITypeInfo * basetype)
{
    CriticalBlock procedure(*typeCS);

    IInterface * * match = pitt->getValue(basetype);
    if (match)
    {
        ::Release(basetype);
        return (ITypeInfo *)LINK(*match);
    }
    ITypeInfo * next = new CPackedIntTypeInfo(basetype);
    pitt->setValue(basetype, next);
    return next;
}


extern DEFTYPE_API ITypeInfo *makePackedIntType(int len, bool isSigned)
{
    return makePackedIntType(makeIntType(len, isSigned));
}

extern DEFTYPE_API ITypeInfo *makeRealType(int len)
{
    assertex(len == 4 || len == 8);
    if (len != 4 && len != 8)
        return NULL;

    CriticalBlock procedure(*typeCS);
    CRealTypeInfo *ret = realtt[len-1];
    if (ret==NULL)
        ret = realtt[len-1] = new CRealTypeInfo(len);
    ::Link(ret);
    return ret;
}

/* Precondition: len>0 && len>64 */
extern DEFTYPE_API ITypeInfo *makeBitfieldType(int len, ITypeInfo * basetype)
{
    assertex(len>0 && len <= 64);
    if (len <= 0 || len > 64)
        return NULL;

    CriticalBlock procedure(*typeCS);
    if (basetype)
    {
        assertex(basetype->getTypeCode() == type_int);
    }
    else
        basetype = getPromotedBitfieldType(len);

    unsigned baseSize=basetype->getSize();
    CBitfieldTypeInfo *ret = bftt[len-1][baseSize-1];
    if (ret==NULL)
        ret = bftt[len-1][baseSize-1] = new CBitfieldTypeInfo(len, basetype);
    else
        ::Release(basetype);
    ::Link(ret);
    return ret;
}

extern DEFTYPE_API ITypeInfo *makeBoolType()
{
    CriticalBlock procedure(*typeCS);
    if (!btt)
        btt = new CBoolTypeInfo();
    ::Link(btt);
    return btt;
}

extern DEFTYPE_API ITypeInfo *makeBlobType()
{
    CriticalBlock procedure(*typeCS);
    if (!bltt)
        bltt = new CBlobTypeInfo();
    ::Link(bltt);
    return bltt;
}

extern DEFTYPE_API ITypeInfo *makeVoidType()
{
    CriticalBlock procedure(*typeCS);
    if (!vtt)
        vtt = new CVoidTypeInfo();
    ::Link(vtt);
    return vtt;
}

extern DEFTYPE_API ITypeInfo *makeNullType()
{
    CriticalBlock procedure(*typeCS);
    if (!ntt)
        ntt = new CNullTypeInfo();
    ::Link(ntt);
    return ntt;
}

extern DEFTYPE_API ITypeInfo *makeRecordType()
{
    CriticalBlock procedure(*typeCS);
    if (!rtt)
        rtt = new CRecordTypeInfo();
    ::Link(rtt);
    return rtt;
}

extern DEFTYPE_API ITypeInfo *makePatternType()
{
    CriticalBlock procedure(*typeCS);
    if (!patt)
        patt = new CPatternTypeInfo();
    ::Link(patt);
    return patt;
}

extern DEFTYPE_API ITypeInfo *makeTokenType()
{
    CriticalBlock procedure(*typeCS);
    if (!tokentt)
        tokentt = new CTokenTypeInfo();
    ::Link(tokentt);
    return tokentt;
}

extern DEFTYPE_API ITypeInfo *makeFeatureType()
{
    CriticalBlock procedure(*typeCS);
    if (!featurett)
        featurett = new CFeatureTypeInfo();
    ::Link(featurett);
    return featurett;
}

extern DEFTYPE_API ITypeInfo *makeEventType()
{
    CriticalBlock procedure(*typeCS);
    if (!ett)
        ett = new CEventTypeInfo();
    ::Link(ett);
    return ett;
}

extern DEFTYPE_API ITypeInfo *makeAnyType()
{
    CriticalBlock procedure(*typeCS);
    if (!anytt)
        anytt = new CAnyTypeInfo();
    ::Link(anytt);
    return anytt;
}

extern DEFTYPE_API ITypeInfo *makeCharType(bool caseSensitive)
{
    CriticalBlock procedure(*typeCS);
    if (!ctt[caseSensitive])
        ctt[caseSensitive] = new CCharTypeInfo(caseSensitive);
    ::Link(ctt[caseSensitive]);
    return ctt[caseSensitive];
}

extern DEFTYPE_API ITypeInfo *makeSetType(ITypeInfo *basetype)
{
    CriticalBlock procedure(*typeCS);
    IInterface * * match = setTT->getValue(basetype);
    if (match)
    {
        ::Release(basetype);
        return (ITypeInfo *)LINK(*match);
    }
    ITypeInfo * next = new CSetTypeInfo(basetype);
    setTT->setValue(basetype, next);
    return next;
}

//-----------------------------

static ITypeInfo * commonUpType(CHashedTypeInfo * candidate)
{
    ITypeInfo * match;
    {
        CriticalBlock block(*typeCS);
        match = globalTypeCache->addOrFind(*candidate);
        if (match == candidate)
            return match;
        if (!static_cast<CHashedTypeInfo *>(match)->isAliveAndLink())
        {
            globalTypeCache->replace(*candidate);
            return candidate;
        }
    }
    candidate->Release();
    return match;
}

extern DEFTYPE_API ITypeInfo *makeKeyedBlobType(ITypeInfo * basetype)
{
    return commonUpType(new CKeyedBlobTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeFilePosType(ITypeInfo *basetype)
{
    assertex(basetype);
    return commonUpType(new CFilePosTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeKeyedIntType(ITypeInfo *basetype)
{
    assertex(basetype);
    return commonUpType(new CKeyedIntTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeDecimalType(unsigned digits, unsigned prec, bool isSigned)
{
    assertex((digits == UNKNOWN_LENGTH) || (digits - prec <= MAX_DECIMAL_LEADING));
    assertex((prec == UNKNOWN_LENGTH) || ((prec <= digits) && (prec <= MAX_DECIMAL_PRECISION)));
    assertex((prec != UNKNOWN_LENGTH) || (digits == UNKNOWN_LENGTH));
    return commonUpType(new CDecimalTypeInfo(digits, prec, isSigned));
}


extern DEFTYPE_API ITypeInfo *makeRuleType(ITypeInfo *basetype)
{
    return commonUpType(new CRuleTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeTableType(ITypeInfo *basetype)
{
    assertex(!basetype || basetype->getTypeCode() == type_row);
    return commonUpType(new CTableTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeDictionaryType(ITypeInfo *basetype)
{
    assertex(!basetype || basetype->getTypeCode() == type_row);
    return commonUpType(new CDictionaryTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeGroupedTableType(ITypeInfo *basetype)
{
    return commonUpType(new CGroupedTableTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeFunctionType(ITypeInfo *basetype, IInterface * parameters, IInterface * defaults, IInterface * attrs)
{
    if (!basetype || !parameters)
    {
        ::Release(basetype);
        ::Release(parameters);
        ::Release(defaults);
        ::Release(attrs);
        throwUnexpected();
    }
    assertex(basetype->getTypeCode() != type_function); // not just yet anyway
    return commonUpType(new CFunctionTypeInfo(basetype, parameters, defaults, attrs));
}

/* In basetype: linked. Return: linked */
extern DEFTYPE_API ITypeInfo *makeRowType(ITypeInfo *basetype)
{
    assertex(!basetype || basetype->getTypeCode() == type_record);
    return commonUpType(new CRowTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeTransformType(ITypeInfo *basetype)
{
    assertex(!basetype || basetype->getTypeCode() == type_record);
    return commonUpType(new CTransformTypeInfo(basetype));
}

extern DEFTYPE_API ITypeInfo *makeSortListType(ITypeInfo *basetype)
{
    return commonUpType(new CSortListTypeInfo(basetype));
}

/* In basetype: linked. Return: linked */
ITypeInfo * makeArrayType(ITypeInfo * basetype, unsigned size)
{
    return commonUpType(new CArrayTypeInfo(basetype, size));
}

/* In basetype: linked. Return: linked */
ITypeInfo * makePointerType(ITypeInfo * basetype)
{
    return commonUpType(new CPointerTypeInfo(basetype));
}

/* In basetype: linked. Return: linked */
ITypeInfo * makeConstantModifier(ITypeInfo * basetype)
{
    return makeModifier(basetype, typemod_const, NULL);
}

/* In basetype: linked. Return: linked */
ITypeInfo * makeNonConstantModifier(ITypeInfo * basetype)
{
    return makeModifier(basetype, typemod_nonconst, NULL);
}

/* In basetype: linked. Return: linked */
ITypeInfo * makeReferenceModifier(ITypeInfo * basetype)
{
    return makeModifier(basetype, typemod_ref, NULL);
}

ITypeInfo * makeWrapperModifier(ITypeInfo * basetype)
{
    return makeModifier(basetype, typemod_wrapper, NULL);
}

ITypeInfo * makeModifier(ITypeInfo * basetype, typemod_t kind, IInterface * extra)
{
    if (kind == typemod_none)
    {
        ::Release(extra);
        return basetype;
    }

#ifdef _DEBUG
    ITypeInfo * cur = basetype;
    for (;;)
    {
        if (cur->queryModifier() == typemod_none)
            break;
        if (cur->queryModifier() == kind)
        {
            if (cur->queryModifierExtra() == extra)
                throwUnexpected();
        }
        cur = cur->queryTypeBase();
    }
#endif

    return commonUpType(new CModifierTypeInfo(basetype, kind, extra));
}

/* In basetype: linked. Return: linked */
ITypeInfo * makeClassType(const char * name)
{
    //MORE!!
    return new CClassTypeInfo(name);
}

//===========================================================================

template <class T>
inline void ReleaseAndClear(T * & ptr)
{
    if (ptr)
    {
        ptr->Release();
        ptr = NULL;
    }
}

void ClearTypeCache()
{
#ifdef TRACE_HASH
    globalTypeCache->dumpStats();
#endif
    size32_t i;
    stt->kill();
    vstt->kill();
    qstt->kill();
    datatt->kill();
    utt->kill();
    vutt->kill();
    u8tt->kill();
    for (i = 0; i < 8; i++)
    {
        ReleaseAndClear(itt[false][i]);
        ReleaseAndClear(itt[true][i]);
        ReleaseAndClear(sitt[false][i]);
        ReleaseAndClear(sitt[true][i]);
    }
    for (i = 0; i < _elements_in(bftt); i++)
    {
        unsigned j;
        for (j = 0; j < _elements_in(bftt[0]); j++)
            ReleaseAndClear(bftt[i][j]);
    }
    for (i = 0; i < _elements_in(realtt); i++)
        ReleaseAndClear(realtt[i]);

    for (i = 0; i < _elements_in(ctt); i++)
        ReleaseAndClear(ctt[i]);

    ReleaseAndClear(btt);
    ReleaseAndClear(bltt);
    ReleaseAndClear(vtt);
    ReleaseAndClear(ntt);
    ReleaseAndClear(rtt);
    ReleaseAndClear(patt);
    ReleaseAndClear(tokentt);
    ReleaseAndClear(featurett);
    ReleaseAndClear(ett);
    ReleaseAndClear(anytt);
    pitt->kill();
    setTT->kill();
    globalTypeCache->kill();

    ReleaseAndClear(dataCharset);
    ReleaseAndClear(asciiCharset);
    ReleaseAndClear(ebcdicCharset);
    ReleaseAndClear(utf8Charset);
    ReleaseAndClear(asciiCollation);
    ReleaseAndClear(ebcdicCollation);
    ReleaseAndClear(ascii2ebcdic);
    ReleaseAndClear(ebcdic2ascii);
}

//===========================================================================

//This function is here for efficiency - if a particular case is not implemented,
//the default case handles it...

MemoryBuffer & appendBufferFromMem(MemoryBuffer & mem, ITypeInfo * type, const void * data)
{
    unsigned len = type->getSize();

    switch (type->getTypeCode())
    {
    case type_string:
    case type_data:
    case type_varstring:
    case type_qstring:
    case type_decimal:
        mem.append(len, data);
        break;
    default:
        mem.appendEndian(len, data);
        break;
    }
    return mem;
}

//============================================================================

bool isNumericType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_bitfield:
    case type_real:
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_decimal:
        return true;
    }
    return false;
}

bool isStringType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_data:
    case type_string:
    case type_varstring:
    case type_qstring:
        return true;
    }
    return false;
}

bool isSimpleStringType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_data:
    case type_string:
    case type_varstring:
        return true;
    }
    return false;
}

bool isIntegralType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_bitfield:
    case type_int:
    case type_swapint:
    case type_packedint:
        return true;
    }
    return false;
}

bool isSimpleIntegralType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_int:
    case type_swapint:
        return true;
    }
    return false;
}

bool isPatternType(ITypeInfo * type)
{
    switch(type->getTypeCode())
    {
    case type_pattern:
    case type_token:
    case type_rule:
        return true;
    default:
        return false;
    }
}

bool isUnicodeType(ITypeInfo * type)
{
    switch(type->getTypeCode())
    {
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        return true;
    default:
        return false;
    }
}

bool isDatasetType(ITypeInfo * type)
{
    switch(type->getTypeCode())
    {
    case type_table:
    case type_groupedtable:
        return true;
    default:
        return false;
    }
}

bool isSingleValuedType(ITypeInfo * type)
{
    if (!type)
        return false;

    switch (type->getTypeCode())
    {
    case type_boolean:
    case type_int:
    case type_real:
    case type_decimal:
    case type_string:
    case type_date:
    case type_bitfield:
    case type_char:
    case type_enumerated:
    case type_varstring:
    case type_data:
    case type_alien:
    case type_swapint:
    case type_packedint:
    case type_qstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        return true;
    }
    return false;
}

bool isStandardSizeInt(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_int:
    case type_swapint:
        switch (type->getSize())
        {
        case 1:
        case 2:
        case 4:
        case 8:
            return true;
        }
        break;
    }
    return false;
}


//============================================================================

ITypeInfo * getNumericType(ITypeInfo * type)
{
    unsigned digits = 9;
    switch (type->getTypeCode())
    {
    case type_real:
    case type_int:
    case type_swapint:
    case type_decimal:
    case type_packedint:
        return LINK(type);
    case type_bitfield:
        {
            ITypeInfo * promoted = type->queryPromotedType();
            return LINK(promoted);
        }
    case type_varstring:
    case type_qstring:
    case type_string:
    case type_unicode:
    case type_utf8:
    case type_varunicode:
    case type_data:
        digits = type->getDigits();
        if (digits == UNKNOWN_LENGTH)
            digits = 20;
        break;
    case type_boolean:
        digits = 1;
        break;
    }
    if (digits > 9)
        return makeIntType(8, true);
    if (digits > 4)
        return makeIntType(4, true);
    if (digits > 2)
        return makeIntType(2, true);
    return makeIntType(1, true);
}

ITypeInfo * getStringType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_string: case type_varstring:
        return LINK(type);
    }
    return makeStringType(type->getStringLen(), NULL, NULL);
}

ITypeInfo * getVarStringType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_string: case type_varstring:
        return LINK(type);
    }
    return makeVarStringType(type->getStringLen());
}

//============================================================================

static ITypeInfo * getPromotedSet(ITypeInfo * left, ITypeInfo * right, bool isCompare)
{
    ITypeInfo * leftChild = left;
    ITypeInfo * rightChild = right;

    if (left->getTypeCode() == type_set)
        leftChild = left->queryChildType();
    if (right->getTypeCode() == type_set)
        rightChild = right->queryChildType();

    if (leftChild && rightChild)
        return makeSetType(getPromotedType(leftChild, rightChild));
    if (!leftChild)
        return LINK(right);
    return LINK(left);
}

static ITypeInfo * getPromotedUnicode(ITypeInfo * left, ITypeInfo * right)
{
    unsigned lLen = left->getStringLen();
    unsigned rLen = right->getStringLen();
    if(lLen < rLen)
        lLen = rLen;
    return makeUnicodeType(lLen, getCommonLocale(left, right));
}

static ITypeInfo * getPromotedVarUnicode(ITypeInfo * left, ITypeInfo * right)
{
    unsigned lLen = left->getStringLen();
    unsigned rLen = right->getStringLen();
    if(lLen < rLen)
        lLen = rLen;
    return makeVarUnicodeType(lLen, getCommonLocale(left, right));
}
        
static ITypeInfo * getPromotedUtf8(ITypeInfo * left, ITypeInfo * right)
{
    unsigned lLen = left->getStringLen();
    unsigned rLen = right->getStringLen();
    if(lLen < rLen)
        lLen = rLen;
    return makeUtf8Type(lLen, getCommonLocale(left, right));
}

static ITypeInfo * getPromotedVarString(ITypeInfo * left, ITypeInfo * right)
{
    unsigned lLen = left->getStringLen();
    unsigned rLen = right->getStringLen();
    if (lLen < rLen) lLen = rLen;
    //MORE: Didn't this ought to have the charset logic of getPromotedString?
    return makeVarStringType(lLen);
}
        
static ITypeInfo * getPromotedString(ITypeInfo * left, ITypeInfo * right)
{
    unsigned lLen = left->getStringLen();
    unsigned rLen = right->getStringLen();
    if (lLen < rLen) lLen = rLen;

    ICollationInfo * collation = left->queryCollation();                //MORE!!

    ICharsetInfo * lCharset = left->queryCharset();
    ICharsetInfo * rCharset = right->queryCharset();
    if (!lCharset || (rCharset && (lCharset != rCharset) && (rCharset->queryName() == asciiAtom)))
    {
        lCharset = rCharset;
        collation = right->queryCollation();
    }

    return makeStringType(lLen, LINK(lCharset), LINK(collation));
}
        
static ITypeInfo * getPromotedQString(ITypeInfo * left, ITypeInfo * right)
{
    unsigned lLen = left->getStringLen();
    unsigned rLen = right->getStringLen();
    if (lLen < rLen) lLen = rLen;
    return makeQStringType(lLen);
}
        
static ITypeInfo * getPromotedData(ITypeInfo * left, ITypeInfo * right)
{
    unsigned lLen = left->getStringLen();
    unsigned rLen = right->getStringLen();
    assertex(lLen != rLen);
    return makeDataType(UNKNOWN_LENGTH);
}

static ITypeInfo * makeUnknownLengthDecimal(bool isCompare)
{
    if (isCompare)
        return makeDecimalType(UNKNOWN_LENGTH, UNKNOWN_LENGTH, true);
    return makeDecimalType(MAX_DECIMAL_DIGITS, MAX_DECIMAL_PRECISION, true);
}


static ITypeInfo * getPromotedDecimalReal(ITypeInfo * type, bool isCompare)
{
    return makeUnknownLengthDecimal(isCompare);
}

static ITypeInfo * getPromotedDecimal(ITypeInfo * left, ITypeInfo * right, bool isCompare)
{
    if (left->getTypeCode() == type_real)
        return getPromotedDecimalReal(right, isCompare);
    if (right->getTypeCode() == type_real)
        return getPromotedDecimalReal(left, isCompare);

    unsigned lDigits = left->getDigits();
    unsigned rDigits  = right->getDigits();
    if (lDigits == UNKNOWN_LENGTH || rDigits == UNKNOWN_LENGTH)
        return makeDecimalType(UNKNOWN_LENGTH, UNKNOWN_LENGTH, left->isSigned() || right->isSigned());

    if (isCompare)
        return makeUnknownLengthDecimal(isCompare);

    unsigned lPrec = left->getPrecision();
    unsigned rPrec = right->getPrecision();
    unsigned lLead  = lDigits - lPrec;
    unsigned rLead  = rDigits - rPrec;
    if (lLead < rLead) lLead = rLead;
    if (lPrec < rPrec) lPrec = rPrec;
    return makeDecimalType(lLead + lPrec, lPrec, left->isSigned() || right->isSigned());
}
        
static ITypeInfo * getPromotedReal(ITypeInfo * left, ITypeInfo * right)
{
    unsigned lDigits = left->getDigits();
    unsigned rDigits = right->getDigits();
    if (lDigits < rDigits) lDigits = rDigits;
    if (lDigits >= DOUBLE_SIG_DIGITS) 
        return makeRealType(8);
    return makeRealType(4);
}
        

static void getPromotedIntegerSize(ITypeInfo * left, ITypeInfo * right, unsigned & pSize, bool & pSigned)
{
    unsigned lSize = left->getSize();
    unsigned rSize = right->getSize();
    bool lSigned = left->isSigned();
    bool rSigned = right->isSigned();

    //This assumes it's an addition! For other operands rules are not necessarily the same!
    //Need to get the correct size and sign combination.  
    //Try and preserve value whenever possible.
    //u1+s1=>s2 s1+u4=>s8 u2+u4=>u4 u1+s2=s2 b+s1->s1 b+u1=>u1
    //Also needs to cope with integer6 etc...
    if (left->getTypeCode() == type_boolean)
    {
        lSigned = rSigned;
        lSize = rSize;
    }
    else if (right->getTypeCode() == type_boolean)
    {
        //don't need to do anything....
    }
    else if (lSigned != rSigned)
    {
        if (lSigned)
        {
            if (lSize <= rSize)
            {
                lSize = promotedIntSize[rSize];
                if ((lSize == rSize) && (lSize != 8))
                    lSize += lSize;
            }
        }
        else
        {
            lSigned = true;
            if (lSize < rSize)
                lSize = rSize;
            else
            {
                if (lSize != promotedIntSize[lSize])
                    lSize = promotedIntSize[lSize];
                else if (lSize != 8)
                    lSize += lSize; 
            }
        }
    }
    else
    {
        if (lSize < rSize)
            lSize = rSize;
    }
    pSize = lSize;
    pSigned = lSigned;
}

static ITypeInfo * getPromotedInteger(ITypeInfo * left, ITypeInfo * right)
{
    unsigned size;
    bool isSigned;
    getPromotedIntegerSize(left, right, size, isSigned);
    return makeIntType(size, isSigned);
}

static ITypeInfo * getPromotedSwapInteger(ITypeInfo * left, ITypeInfo * right)
{
    unsigned size;
    bool isSigned;
    getPromotedIntegerSize(left, right, size, isSigned);
    return makeSwapIntType(size, isSigned);
}
        
static ITypeInfo * getPromotedPackedInteger(ITypeInfo * left, ITypeInfo * right)
{
    unsigned size;
    bool isSigned;
    getPromotedIntegerSize(left, right, size, isSigned);
    return makePackedIntType(size, isSigned);
}
        
//============================================================================

static ITypeInfo * getPromotedType(ITypeInfo * lType, ITypeInfo * rType, bool isCompare)
{
    ITypeInfo * l = lType->queryPromotedType();
    ITypeInfo * r = rType->queryPromotedType();
    if (l == r)
        return LINK(l);

    type_t lcode = l->getTypeCode();
    type_t rcode = r->getTypeCode();
    if (lcode == type_any) return LINK(r);
    if (rcode == type_any) return LINK(l);
    if ((lcode == type_set) || (rcode == type_set))
        return getPromotedSet(l, r, isCompare);
    if ((lcode == type_unicode) || (rcode == type_unicode))
        return getPromotedUnicode(l, r);
    if ((lcode == type_utf8) || (rcode == type_utf8))
        return getPromotedUtf8(l, r);
    if ((lcode == type_varunicode) || (rcode == type_varunicode))
        return getPromotedVarUnicode(l, r);
    if ((lcode == type_string) || (rcode == type_string))
        return getPromotedString(l, r);
    if ((lcode == type_varstring) || (rcode == type_varstring))
        return getPromotedVarString(l, r);
    if ((lcode == type_data) || (rcode == type_data))
        return getPromotedData(l, r);
    if ((lcode == type_qstring) || (rcode == type_qstring))
        return getPromotedQString(l, r);
    if ((lcode == type_decimal) || (rcode == type_decimal))
        return getPromotedDecimal(l, r, isCompare);
    if ((lcode == type_real) || (rcode == type_real)) 
        return getPromotedReal(l, r);
    if ((lcode == type_int) || (rcode == type_int)) 
        return getPromotedInteger(l, r);
    if (lcode == type_boolean) return LINK(l);
    if (rcode == type_boolean) return LINK(r);

    if ((lcode == type_swapint) || (rcode == type_swapint)) 
        return getPromotedSwapInteger(l, r);
    if ((lcode == type_packedint) || (rcode == type_packedint)) 
        return getPromotedPackedInteger(l, r);

    //NB: Enumerations should come last...
    if (l->getSize() >= r->getSize())
        return LINK(l);
    return LINK(r);
    //MORE(!)
    //return makeIntType(4);
}

ITypeInfo * getPromotedType(ITypeInfo * lType, ITypeInfo * rType)
{
    return getPromotedType(lType, rType, false);
}

ITypeInfo * getPromotedNumericType(ITypeInfo * l_type, ITypeInfo * r_type)
{
    Owned<ITypeInfo> l = getNumericType(l_type->queryPromotedType());
    Owned<ITypeInfo> r = getNumericType(r_type->queryPromotedType());
    return getPromotedType(l,r,false);
}

ITypeInfo * getPromotedAddSubType(ITypeInfo * lType, ITypeInfo * rType)
{
    Owned<ITypeInfo> ret = getPromotedNumericType(lType, rType);
    if (isDecimalType(ret) && !isUnknownSize(ret) && (ret->getDigits() - ret->getPrecision() < MAX_DECIMAL_LEADING))
        return makeDecimalType(ret->getDigits()+1, ret->getPrecision(), ret->isSigned());
    return ret.getClear();
}

ITypeInfo * getPromotedMulDivType(ITypeInfo * lType, ITypeInfo * rType)
{
    Owned<ITypeInfo> ret = getPromotedNumericType(lType, rType);
    if (isDecimalType(ret) && !isUnknownSize(ret))
        return makeUnknownLengthDecimal(false);
    return ret.getClear();
}

ITypeInfo * getPromotedCompareType(ITypeInfo * left, ITypeInfo * right)
{
    Owned<ITypeInfo> promoted = getPromotedType(left, right, true);
    if (left != right)
    {
        type_t ptc = promoted->getTypeCode();
        switch (ptc)
        {
        case type_string:
            {
                if ((left->getTypeCode() == ptc) && (right->getTypeCode() == ptc))
                {
                    if ((left->queryCollation() == right->queryCollation()) && 
                        (left->queryCharset() == right->queryCharset()))
                    {
                        promoted.setown(getStretchedType(UNKNOWN_LENGTH, left));
                    }
                }
            }
            break;
        case type_unicode:
        case type_utf8:
            {
            }
            break;
        }
    }
    return promoted.getClear();
}

static bool preservesValue(ITypeInfo * after, ITypeInfo * before, bool preserveInformation)
{
    type_t beforeType = before->getTypeCode();
    type_t afterType = after->getTypeCode();

    switch (beforeType)
    {
    case type_boolean: 
        return true;
    case type_keyedint:
        return preservesValue(after, before->queryChildType(), preserveInformation);
    case type_packedint:
        before = before->queryPromotedType();
        //fall through
    case type_int:
    case type_swapint:
    case type_enumerated:
        switch (afterType)
        {
        case type_keyedint:
            return preservesValue(after->queryChildType(), before, preserveInformation);
        case type_packedint:
            after = after->queryPromotedType();
            //fall through.
        case type_int: case type_swapint: case type_enumerated:
            {
                bool beforeSigned = before->isSigned();
                bool afterSigned  = after->isSigned();
                size32_t beforeSize = before->getSize();
                size32_t afterSize = after->getSize();
                if (preserveInformation && (beforeSize <= afterSize)) 
                    return true; // sign doesn't matter...
                return !((beforeSigned && !afterSigned) ||
                         (beforeSize > afterSize) ||
                         (!beforeSigned && afterSigned && (beforeSize == afterSize)));
            }
        case type_decimal:
            {
                bool beforeSigned = before->isSigned();
                bool afterSigned  = after->isSigned();
                size32_t beforeSize, afterSize;
                beforeSize = before->getDigits();
                afterSize  = after->getDigits() - after->getPrecision();
                return ((!beforeSigned || afterSigned) && (beforeSize <= afterSize));
            }
        case type_real:
            return (before->getSize() < after->getSize());       // I think this is correct for all instances
        case type_string: case type_data: case type_varstring: case type_qstring: case type_unicode: case type_varunicode: case type_utf8:
            return (after->getDigits() >= before->getDigits());
        }
        return false;
    case type_decimal:
        switch (afterType)
        {
        case type_decimal:
            {
                unsigned beforePrec = before->getPrecision();
                unsigned afterPrec = after->getPrecision();

                return (!before->isSigned() || after->isSigned()) &&
                       (beforePrec <= afterPrec) &&
                       (before->getDigits() - beforePrec <= after->getDigits() - afterPrec);
            }
        case type_real:
            return before->getDigits() < after->getDigits();    //NB: Not <= since real errs on over estimation
        case type_int: case type_swapint: case type_packedint:
            return ((before->getPrecision() == 0) &&
                    (after->getDigits() > before->getDigits()) &&
                    (!before->isSigned() || after->isSigned()));
        case type_string: case type_varstring: case type_data: case type_qstring: case type_unicode: case type_varunicode: case type_utf8:
            return (after->getStringLen() >= before->getStringLen());
        }
        return false;
    case type_varstring:
        //casting from a var string may lose the length information, and so be irreversible.
        switch (afterType)
        {
        case type_varstring: case type_varunicode:
            return (before->getStringLen() <= after->getStringLen());
        }
        return false;
    case type_string:
    case type_data:
    case type_qstring:
        switch (afterType)
        {
        case type_string: case type_data: case type_varstring: case type_unicode: case type_varunicode: case type_utf8:
            return (before->getStringLen() <= after->getStringLen());
        case type_qstring:
            return (beforeType == type_qstring) && (before->getStringLen() <= after->getStringLen());
        }
        return false;
    case type_set:
        if (afterType != type_set)
            return false;
        if (!before->queryChildType())
            return true;
        if (!after->queryChildType())
            return false;
        return preservesValue(after->queryChildType(), before->queryChildType());
    case type_varunicode:
        switch (afterType)
        {
        case type_varunicode:
            return (before->getStringLen() <= after->getStringLen());
        }
        return false;
    case type_unicode:
    case type_utf8:
        switch (afterType)
        {
        case type_unicode: case type_varunicode: case type_utf8:
            return (before->getStringLen() <= after->getStringLen());
        }
        return false;
    default:
        return (beforeType == afterType) && (after->getSize() >= before->getSize());
    }
}

bool preservesValue(ITypeInfo * after, ITypeInfo * before)
{
    return preservesValue(after, before, false);
}
bool castLosesInformation(ITypeInfo * after, ITypeInfo * before)
{
    return !preservesValue(after, before, true);
}

// should always call preservesValue first to determine if conversion is possible 
bool preservesOrder(ITypeInfo * after, ITypeInfo * before)
{
    type_t beforeType = before->getTypeCode();
    type_t afterType = after->getTypeCode();
    if (beforeType == type_keyedint)
        return preservesOrder(after, before->queryChildType());
    if (afterType == type_keyedint)
        return preservesOrder(after->queryChildType(), before);

    switch (beforeType)
    {
    case type_boolean:
        return true;
    case type_decimal:
        switch (afterType)
        {
        case type_real: case type_decimal:
            return true;
        case type_int: case type_swapint: case type_packedint:
            return (before->getPrecision() == 0);
        }
        return false;
    case type_int: 
    case type_swapint:
    case type_packedint:
        switch (afterType)
        {
        case type_int: case type_swapint: case type_real: case type_enumerated: case type_decimal: case type_packedint:
            return true;
        }
        return false;
    case type_string:
    case type_varstring:
    case type_data:
    case type_qstring:
        switch (afterType)
        {
        case type_string: case type_varstring: case type_data: case type_enumerated:
            return true;
        case type_qstring:
            return (beforeType == type_qstring);
        }
        return false;
    case type_enumerated:
        switch (afterType)
        {
        case type_string: case type_varstring: case type_data: case type_enumerated: case type_int: case type_swapint: case type_packedint:
            return true;
        }
        return false;
    default:
        return (beforeType == afterType);
    }
}


bool isSameBasicType(ITypeInfo * left, ITypeInfo * right)
{
    if (!left || !right)
        return left==right;
    while (left->isReference())
        left = left->queryTypeBase();
    while (right->isReference())
        right = right->queryTypeBase();
    return queryUnqualifiedType(left)==queryUnqualifiedType(right);
}


extern DEFTYPE_API ITypeInfo * getRoundType(ITypeInfo * type)
{
    if (type->getTypeCode() == type_decimal)
    {
        unsigned olddigits = type->getDigits();
        if (olddigits == UNKNOWN_LENGTH)
            return LINK(type);

        //rounding could increase the number of digits by 1.
        unsigned newdigits = (olddigits - type->getPrecision())+1;
        if (newdigits > MAX_DECIMAL_LEADING)
            newdigits = MAX_DECIMAL_LEADING;
        return makeDecimalType(newdigits, 0, type->isSigned());
    }
    return makeIntType(8, true);
}

extern DEFTYPE_API ITypeInfo * getRoundToType(ITypeInfo * type)
{
    if (type->getTypeCode() == type_decimal)
    {
        unsigned olddigits = type->getDigits();
        unsigned oldPrecision = type->getPrecision();
        if ((olddigits == UNKNOWN_LENGTH) || (olddigits-oldPrecision == MAX_DECIMAL_LEADING))
            return LINK(type);
        //rounding could increase the number of digits by 1.
        return makeDecimalType(olddigits+1, oldPrecision, type->isSigned());
    }
    return makeRealType(8);
}

extern DEFTYPE_API ITypeInfo * getTruncType(ITypeInfo * type)
{
    if (type->getTypeCode() == type_decimal)
    {
        unsigned olddigits = type->getDigits();
        if (olddigits == UNKNOWN_LENGTH)
            return LINK(type);

        unsigned newdigits = (olddigits - type->getPrecision());
        return makeDecimalType(newdigits, 0, type->isSigned());
    }
    return makeIntType(8, true);
}

//---------------------------------------------------------------------------

CCharsetInfo::~CCharsetInfo()
{
    ::Release(defaultCollation);
}

ICollationInfo * CCharsetInfo::queryDefaultCollation()
{
    if (!defaultCollation)
        defaultCollation = getCollation(name);
    return defaultCollation;
}

CCollationInfo::~CCollationInfo()
{
}

ICharsetInfo * CCollationInfo::getCharset()
{
    return ::getCharset(name);
}


//---------------------------------------------------------------------------

CTranslationInfo::CTranslationInfo(IAtom * _name, ICharsetInfo * _src, ICharsetInfo * _tgt) : src(_src), tgt(_tgt)
{
    name = _name;
}

IAtom * CTranslationInfo::queryName()
{
    return name;
}

ICharsetInfo * CTranslationInfo::querySourceCharset()
{
    return src;
}

ICharsetInfo * CTranslationInfo::queryTargetCharset()
{
    return tgt;
}


//---------------------------------------------------------------------------

CAscii2EbcdicTranslationInfo::CAscii2EbcdicTranslationInfo() : CTranslationInfo(ascii2ebcdicAtom, getCharset(asciiAtom), getCharset(ebcdicAtom))
{
}


const char * CAscii2EbcdicTranslationInfo::queryRtlFunction()
{
    return "ascii2ebcdic";
}

const char * CAscii2EbcdicTranslationInfo::queryVarRtlFunction()
{
    return "ascii2ebcdicX";
}

StringBuffer & CAscii2EbcdicTranslationInfo::translate(StringBuffer & tgt, unsigned len, const char * src)
{
    char * buf = (char*)malloc(len);
    rtlStrToEStr(len, buf, len, src);
    tgt.append(len, buf);
    free(buf);
    return tgt;
}


CEbcdic2AsciiTranslationInfo::CEbcdic2AsciiTranslationInfo() : CTranslationInfo(ebcdic2asciiAtom, getCharset(asciiAtom), getCharset(ebcdicAtom))
{
}


const char * CEbcdic2AsciiTranslationInfo::queryRtlFunction()
{
    return "ebcdic2ascii";
}

const char * CEbcdic2AsciiTranslationInfo::queryVarRtlFunction()
{
    return "ebcdic2asciiX";
}

StringBuffer & CEbcdic2AsciiTranslationInfo::translate(StringBuffer & tgt, unsigned len, const char * src)
{
    char * buf = (char*)malloc(len);
    rtlEStrToStr(len, buf, len, src);
    tgt.append(len, buf);
    free(buf);
    return tgt;
}

//---------------------------------------------------------------------------

ICharsetInfo * getCharset(IAtom * atom)
{
    if ((atom == NULL) || (atom == asciiAtom))
    {
        if (!asciiCharset)
            asciiCharset = new CCharsetInfo(asciiAtom, 0x20, asciiCodepageAtom);
        return LINK(asciiCharset);
    }
    else if (atom == dataAtom)
    {
        if (!dataCharset)
            dataCharset = new CCharsetInfo(dataAtom, 0, asciiCodepageAtom);
        return LINK(dataCharset);
    }
    else if (atom == ebcdicAtom)
    {
        if (!ebcdicCharset)
            ebcdicCharset = new CCharsetInfo(ebcdicAtom, 0x40, ebcdicCodepageAtom);
        return LINK(ebcdicCharset);
    }
    else if (atom == utf8Atom)
    {
        if (!utf8Charset)
            utf8Charset = new CCharsetInfo(utf8Atom, 0x20, utf8Atom);
        return LINK(utf8Charset);
    }
    return NULL;
}

ICollationInfo * getCollation(IAtom * atom)
{
    if ((atom == NULL) || (atom == asciiAtom) || (atom == dataAtom) || (atom == utf8Atom))
    {
        if (!asciiCollation)
            asciiCollation = new CSimpleCollationInfo(asciiAtom);
        return LINK(asciiCollation);
    }
    else if (atom == ebcdicAtom)
    {
        if (!ebcdicCollation)
            ebcdicCollation = new CSimpleCollationInfo(ebcdicAtom);
        return LINK(ebcdicCollation);
    }
    return NULL;
}


ITranslationInfo * queryDefaultTranslation(ICharsetInfo * tgt, ICharsetInfo * src)
{
    if ((src == asciiCharset) && (tgt == ebcdicCharset))
    {
        if (!ascii2ebcdic)
            ascii2ebcdic = new CAscii2EbcdicTranslationInfo;
        return ascii2ebcdic;
    }
    if ((tgt == asciiCharset) && (src == ebcdicCharset))
    {
        if (!ebcdic2ascii)
            ebcdic2ascii = new CEbcdic2AsciiTranslationInfo;
        return ebcdic2ascii;
    }
    return NULL;
}

ITranslationInfo * getDefaultTranslation(ICharsetInfo * tgt, ICharsetInfo * src)
{
    ITranslationInfo *translator = queryDefaultTranslation(tgt, src);
    ::Link(translator);
    return translator;
}

ICharsetInfo * getAsciiCharset()
{
    return getCharset(asciiAtom);
}

//---------------------------------------------------------------------------

ITypeInfo * getStretchedType(unsigned newLen, ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_string:
        return makeStringType(newLen, LINK(type->queryCharset()), LINK(type->queryCollation()));
    case type_varstring:
        return makeVarStringType(newLen, LINK(type->queryCharset()), LINK(type->queryCollation()));
    case type_unicode:
        return makeUnicodeType(newLen, type->queryLocale());
    case type_varunicode:
        return makeVarUnicodeType(newLen, type->queryLocale());
    case type_utf8:
        return makeUtf8Type(newLen, type->queryLocale());
    case type_qstring:
        return makeQStringType(newLen);
    case type_data:
        return makeDataType(newLen);
    case type_int:
        return makeIntType(newLen, type->isSigned());
    default:
        throw MakeStringException(99, "Internal error: getStretchedType");
    }
    return NULL;
}

ITypeInfo * getMaxLengthType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_boolean:
        return LINK(type);
    case type_int:
        return makeIntType(8, type->isSigned());
    case type_string:
    case type_varstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
    case type_qstring:
    case type_data:
        return getStretchedType(UNKNOWN_LENGTH, type);
    default:
        return LINK(type);
    }
    return NULL;
}

ITypeInfo * getAsciiType(ITypeInfo * type)
{
    ICharsetInfo * charset = type->queryCharset();
    if (charset && (charset->queryName() != asciiAtom))
    {
        switch (type->getTypeCode())
        {
        case type_string:
            return makeStringType(type->getSize(), NULL, NULL);
        case type_varstring:
            return makeVarStringType(type->getStringLen(), NULL, NULL);
        }
    }
    return LINK(type);
}


ITypeInfo * getBandType(ITypeInfo * lType, ITypeInfo * rType)
{
    if (lType->isBoolean() && rType->isBoolean())
        return LINK(lType);
    unsigned lSize = lType->getSize();
    unsigned rSize = rType->getSize();
    return makeIntType(std::min(lSize,rSize),lType->isSigned()&&rType->isSigned());
}

ITypeInfo * getBorType(ITypeInfo * lType, ITypeInfo * rType)
{
    if (lType->isBoolean() && rType->isBoolean())
        return LINK(lType);
    unsigned lSize = lType->getSize();
    unsigned rSize = rType->getSize();
    return makeIntType(std::max(lSize,rSize),lType->isSigned()&&rType->isSigned());
}

bool hasDefaultLocale(ITypeInfo * type)
{
    return ((type->queryLocale() == 0) || (*str(type->queryLocale()) == 0));
}

bool haveCommonLocale(ITypeInfo * type1, ITypeInfo * type2)
{
    //for the moment, disallow binary ops unless locales identical or one is default --- may later change, e.g. to use common parent where present
    return ((type1->queryLocale() == type2->queryLocale()) || hasDefaultLocale(type1) || hasDefaultLocale(type2));
}

IAtom * getCommonLocale(ITypeInfo * type1, ITypeInfo * type2)
{
    //for the moment, disallow binary ops unless locales identical or one is default --- may later change, e.g. to use common parent where present
    if(!hasDefaultLocale(type1))
        return type1->queryLocale();
    return type2->queryLocale();
}

bool isLittleEndian(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_packedint:
        return true;
    case type_int:
#if __BYTE_ORDER == __LITTLE_ENDIAN
        return true;
#else
        return false;
#endif
    case type_swapint:
#if __BYTE_ORDER == __LITTLE_ENDIAN
        return false;
#else
        return true;
#endif
    default:
        return true;
    }
}



inline ITypeInfo * queryChildType(ITypeInfo * t, type_t search)
{
    while (t)
    {
        type_t code = t->getTypeCode();
        if (code == search)
            return t;

        switch (code)
        {
        case type_set:
        case type_dictionary:
        case type_groupedtable:
        case type_row:
        case type_table:
        case type_rule:
        case type_transform:
        case type_function:
        case type_pointer:
        case type_array:
            t = t->queryChildType();
            break;
        default:
            return NULL;
        }
    }
    return NULL;
}


ITypeInfo * queryRowType(ITypeInfo * t)
{
    return queryChildType(t, type_row);
}

ITypeInfo * queryRecordType(ITypeInfo * t)
{
    return queryChildType(t, type_record);
}

ITypeInfo * queryUnqualifiedType(ITypeInfo * t)
{
    if (!t)
        return t;
    for (;;)
    {
        ITypeInfo * base = t->queryTypeBase();
        if (base == t)
            return t;
        t = base;
    }
}

ITypeInfo * getFullyUnqualifiedType(ITypeInfo * t)
{
    if (!t)
        return t;
    for (;;)
    {
        ITypeInfo * base = t->queryTypeBase();
        if (base == t)
        {
            ITypeInfo * child = t->queryChildType();
            if (!child)
                return LINK(t);
            Owned<ITypeInfo> newChild = getFullyUnqualifiedType(child);
            return replaceChildType(t, newChild);
        }
        t = base;
    }
}

ITypeInfo * removeModifier(ITypeInfo * t, typemod_t modifier)
{
    typemod_t curModifier = t->queryModifier();
    if (curModifier == typemod_none)
        return LINK(t);

    ITypeInfo * base = t->queryTypeBase();
    if (curModifier == modifier)
        return LINK(base);

    OwnedITypeInfo newBase = removeModifier(base, modifier);
    if (newBase == base)
        return LINK(t);
    return makeModifier(newBase.getClear(), curModifier, LINK(t->queryModifierExtra()));
}

bool hasModifier(ITypeInfo * t, typemod_t modifier)
{
    for (;;)
    {
        typemod_t curModifier = t->queryModifier();
        if (curModifier == modifier)
            return true;
        if (curModifier == typemod_none)
            return false;
        t = t->queryTypeBase();
    }
}

ITypeInfo * queryModifier(ITypeInfo * t, typemod_t modifier)
{
    for (;;)
    {
        typemod_t curModifier = t->queryModifier();
        if (curModifier == modifier)
            return t;
        if (curModifier == typemod_none)
            return NULL;
        t = t->queryTypeBase();
    }
}

ITypeInfo * cloneModifier(ITypeInfo * donorModifier, ITypeInfo * srcType)
{
    typemod_t curModifier = donorModifier->queryModifier();
    assertex(curModifier != typemod_none);
    return makeModifier(LINK(srcType), curModifier, LINK(donorModifier->queryModifierExtra()));
}


ITypeInfo * cloneModifiers(ITypeInfo * donorType, ITypeInfo * srcType)
{
    typemod_t curModifier = donorType->queryModifier();
    if (curModifier == typemod_none)
        return LINK(srcType);
    ITypeInfo * base = donorType->queryTypeBase();
    return makeModifier(cloneModifiers(base, srcType), curModifier, LINK(donorType->queryModifierExtra()));
}


ITypeInfo * replaceChildType(ITypeInfo * type, ITypeInfo * newChild)
{
    if (type->queryChildType() == newChild)
        return LINK(type);

    OwnedITypeInfo newType;
    switch (type->getTypeCode())
    {
    case type_dictionary:
        newType.setown(makeDictionaryType(LINK(newChild)));
        break;
    case type_table:
        newType.setown(makeTableType(LINK(newChild)));
        break;
    case type_groupedtable:
        newType.setown(makeGroupedTableType(LINK(newChild)));
        break;
    case type_row:
        newType.setown(makeRowType(LINK(newChild)));
        break;
    case type_set:
        newType.setown(makeSetType(LINK(newChild)));
        break;
    case type_transform:
        newType.setown(makeTransformType(LINK(newChild)));
        break;
    case type_sortlist:
        newType.setown(makeSortListType(LINK(newChild)));
        break;
    case type_rule:
        newType.setown(makeRuleType(LINK(newChild)));
        break;
    case type_function:
    {
        IFunctionTypeExtra * extra = dynamic_cast<IFunctionTypeExtra *>(type);
        assertex(extra);
        newType.setown(makeFunctionType(LINK(newChild), LINK(extra->queryParameters()), LINK(extra->queryDefaults()), LINK(extra->queryAttributes())));
        break;
    }
    default:
        throwUnexpected();
    }
    return cloneModifiers(type, newType);
}

//---------------------------------------------------------------------------

extern unsigned getClarionResultType(ITypeInfo *type)
{
    if (type)
    {
        type_t tc = type->getTypeCode();
        size32_t size = ((tc == type_row) || (tc == type_record)) ? 0 : type->getSize();
        return tc | (size << 16) |
                (type->isInteger() && !type->isSigned() ? type_unsigned : 0) |
                (type->queryCharset() && type->queryCharset()->queryName()==ebcdicAtom ? type_ebcdic : 0);
    }
    else
        return 0;
}

//---------------------------------------------------------------------------
extern DEFTYPE_API ICharsetInfo * deserializeCharsetInfo(MemoryBuffer &src)
{
    StringAttr name;
    src.read(name);
    return getCharset(createLowerCaseAtom(name));
}

extern DEFTYPE_API ICollationInfo * deserializeCollationInfo(MemoryBuffer &src)
{
    StringAttr name;
    src.read(name);
    return getCollation(createLowerCaseAtom(name));
}

extern DEFTYPE_API ITypeInfo * deserializeType(MemoryBuffer &src)
{
    unsigned char tc;
    src.read(tc);
    switch(tc)
    {
    case type_none:
        return NULL;
    case type_int:
        {
            unsigned char size;
            bool isSigned;
            src.read(size);
            src.read(isSigned);
            return makeIntType(size, isSigned);
        }
    case type_swapint:
        {
            unsigned char size;
            bool isSigned;
            src.read(size);
            src.read(isSigned);
            return makeSwapIntType(size, isSigned);
        }
    case type_packedint:
        {
            unsigned char size;
            bool isSigned;
            src.read(size);
            src.read(isSigned);
            return makePackedIntType(size, isSigned);
        }
    case type_char:
        {
            bool isCaseSensitive;
            src.read(isCaseSensitive);
            return makeCharType(isCaseSensitive);
        }
    case type_real:
        {
            unsigned char size;
            src.read(size);
            return makeRealType(size);
        }
    case type_boolean:
        return makeBoolType();
    case type_blob:
        return makeBlobType();
    case type_void:
        return makeVoidType();
    case type_null:
        return makeNullType();
    case type_pattern:
        return makePatternType();
    case type_rule:
        {
            ITypeInfo *base = deserializeType(src);
            return makeRuleType(base);
        }
    case type_token:
        return makeTokenType();
    case type_feature:
        return makeFeatureType();
    case type_event:
        return makeEventType();
    case type_string:
    case type_varstring:
        {
            size32_t size;
            bool b;
            src.read(size);
            src.read(b);
            ICollationInfo *collation = b ? deserializeCollationInfo(src) : NULL;
            src.read(b);
            ICharsetInfo *charset = b ? deserializeCharsetInfo(src) : NULL;
            if (tc==type_string)
                return makeStringType(size, charset, collation);
            else
            {
                if (size != UNKNOWN_LENGTH) size--;
                return makeVarStringType(size, charset, collation);
            }
        }
    case type_unicode:
        {
            size32_t size;
            StringAttr locale;
            src.read(size);
            src.read(locale);
            return makeUnicodeType(size, createLowerCaseAtom(locale.get()));
        }
    case type_varunicode:
        {
            size32_t size;
            StringAttr locale;
            src.read(size);
            src.read(locale);
            return makeVarUnicodeType(size, createLowerCaseAtom(locale.get()));
        }
    case type_utf8:
        {
            size32_t size;
            StringAttr locale;
            src.read(size);
            src.read(locale);
            return makeUtf8Type(size, createLowerCaseAtom(locale.get()));
        }
    case type_qstring:
        {
            size32_t size;
            src.read(size);
            return makeQStringType(size);
        }
    case type_data:
        {
            size32_t size;
            src.read(size);
            return makeDataType(size);
        }
    case type_decimal:
        {
            unsigned char prec, digits;
            bool isSigned;
            src.read(prec);
            src.read(digits);
            src.read(isSigned);

            unsigned fulldigits = (digits == CDecimalTypeInfo::UNKNOWN_DIGITS) ? UNKNOWN_LENGTH : digits;
            unsigned fullprec = (prec == CDecimalTypeInfo::UNKNOWN_DIGITS) ? UNKNOWN_LENGTH : prec;
            return makeDecimalType(fulldigits, fullprec, isSigned);
        }
    case type_bitfield:
        {
            int bitLength;
            src.read(bitLength); 
            ITypeInfo *base = deserializeType(src);
            return makeBitfieldType(bitLength, base);
        }
    case type_set:
        {
            ITypeInfo *base = deserializeType(src);
            return makeSetType(base);
        }
    case type_pointer:
        {
            ITypeInfo *base = deserializeType(src);
            return makePointerType(base);
        }
    case type_array:
        {
            size32_t size;
            src.read(size);
            ITypeInfo *base = deserializeType(src);
            return makeArrayType(base, size);
        }
    case type_class:
        {
            StringAttr name;
            src.read(name);
            return makeClassType(name);
        }
    case type_record:
        return makeRecordType();

    case type_table:
        {
            ITypeInfo *base = deserializeType(src);
            return makeTableType(makeRowType(base));
        }
    case type_groupedtable:
        {
            ITypeInfo *base = deserializeType(src);
            return makeGroupedTableType(base);
        }

        
    }
    assertex(false);
    return NULL;
}

void serializeType(MemoryBuffer &tgt, ITypeInfo * type)
{
    if (type)
        type->serialize(tgt);
    else
        tgt.append((byte)type_none);
}

bool getNormalizedLocaleName(unsigned len, char const * str, StringBuffer & buff)
{
    return rtlGetNormalizedUnicodeLocaleName(len, str, buff.reserve(len));
}

//---------------------------------------------------------------------------
    

static bool alreadyHadSize(int size, IntArray &sizes)
{
    ForEachItemIn(idx, sizes)
    {
        if (sizes.item(idx)==size)
            return true;
    }
    sizes.append(size);
    return false;
}

StringBuffer &appendStartComplexType(StringBuffer &xml, bool hasMixedContent, unsigned *updatePos)
{
    xml.append("<xs:complexType");
    if (hasMixedContent || updatePos)
        xml.append(" mixed=\"").append(hasMixedContent ? '1' : '0').append('\"');
    if (updatePos)
        *updatePos = xml.length()-2;
    return xml.append('>');
}

void XmlSchemaBuilder::addSchemaPrefix(bool hasMixedContent)
{
    if (addHeader)
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.append(
    "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:hpcc=\"urn:hpccsystems:xsd:appinfo\" elementFormDefault=\"qualified\" attributeFormDefault=\"unqualified\">\n"
        "<xs:element name=\"Dataset\">"
            "<xs:complexType>"
                "<xs:sequence minOccurs=\"0\" maxOccurs=\"unbounded\">\n"
                    "<xs:element name=\"Row\">");
    appendStartComplexType(xml, hasMixedContent, NULL).append("<xs:sequence>\n");
    attributes.append(*new StringBufferItem);
}


void XmlSchemaBuilder::addSchemaSuffix()
{
    xml.append(             "</xs:sequence>");
    xml.append(attributes.tos());
    attributes.pop();
    xml.append(         "</xs:complexType>"
                    "</xs:element>\n"
                "</xs:sequence>"
            "</xs:complexType>"
        "</xs:element>\n");
    xml.append(typesXml);
    xml.append("</xs:schema>");
}


void XmlSchemaBuilder::getXmlTypeName(StringBuffer & xmlType, ITypeInfo & type)
{
    size32_t len = type.getStringLen();
    switch (type.getTypeCode())
    {
    case type_boolean:
        xmlType.append("xs:boolean"); break;
    case type_real:
        xmlType.append("xs:double"); break;
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_bitfield:
        //MORE: Could generate different types depending on the size of the fields (e.g., long/unsignedLong)
        if (type.isSigned())
            xmlType.append("xs:integer");
        else
            xmlType.append("xs:nonNegativeInteger"); 
        break;
    case type_data:
        if (len == UNKNOWN_LENGTH)
            xmlType.append("xs:hexBinary");
        else
        {
            xmlType.append("data").append(len);
            if (!alreadyHadSize(len, dataSizes))
            {
                typesXml.appendf(
                            "<xs:simpleType name=\"data%d\">"
                                "<xs:restriction base=\"xs:hexBinary\">"
                                    "<xs:length value=\"%d\"/>"
                                "</xs:restriction>"
                            "</xs:simpleType>", len, len).newline();
            }
        }
        break;
    case type_decimal:
        type.getECLType(xmlType);
        len = type.getDigits()*255 + type.getPrecision();
        if (!alreadyHadSize(len, decimalSizes))
        {
            typesXml.append("<xs:simpleType name=\"");
            type.getECLType(typesXml);
            typesXml.append("\"><xs:restriction base=\"xs:decimal\">");
            typesXml.appendf("<xs:totalDigits value=\"%d\"/>", type.getDigits());
            typesXml.appendf("<xs:fractionDigits value=\"%d\" fixed=\"true\"/>", type.getPrecision());
            typesXml.append("</xs:restriction></xs:simpleType>").newline();
        }
        break;
    case type_string:
    case type_qstring:
    case type_unicode:
    case type_varstring:
    case type_varunicode:
    case type_utf8:
        //NB: xs::maxLength is in unicode characters...
        if (len==UNKNOWN_LENGTH)
            xmlType.append("xs:string");
        else
        {
            xmlType.append("string").append(len);
            if (!alreadyHadSize(len, stringSizes))
            {
                typesXml.appendf("<xs:simpleType name=\"string%d\">"
                                "<xs:restriction base=\"xs:string\">"
                                    "<xs:maxLength value=\"%d\"/>"
                                "</xs:restriction>"
                              "</xs:simpleType>", len, len).newline();
            }
        }
        break;
    case type_set:
        {
            StringBuffer elementName;
            getXmlTypeName(elementName, *type.queryChildType());

            unsigned typeIndex = setTypes.find(type);
            if (typeIndex == NotFound)
            {
                typeIndex = setTypes.ordinality();
                setTypes.append(type);

                typesXml.appendf("<xs:complexType name=\"setof_%s_%d\"><xs:sequence>"
                                 "<xs:element name=\"All\" minOccurs=\"0\"><xs:complexType/></xs:element>"
                                 "<xs:element name=\"Item\" minOccurs=\"0\" maxOccurs=\"unbounded\" type=\"%s\"/>"
                                 "</xs:sequence></xs:complexType>", elementName.str(), typeIndex, elementName.str()).newline();
            }
            //%d is to ensure it is unique e.g., integers come back as the same xml type.
            xmlType.appendf("setof_%s_%d", elementName.str(), typeIndex);
            break;
        }
    default:
        UNIMPLEMENTED;
    }
}

void XmlSchemaBuilder::appendField(StringBuffer &s, const char * name, ITypeInfo & type, bool keyed)
{
    const char * tag = name;
    if (*tag == '@')
    {
        s.append("<xs:attribute");
        tag++;
    }
    else
        s.append("<xs:element");

    s.append(" name=\"").append(tag).append("\" type=\"");
    getXmlTypeName(s, type);
    s.append("\"");
    if (optionalNesting)
    {
        if (*name == '@')
            s.append(" use=\"optional\"");
        else
            s.append(" minOccurs=\"0\"");
    }
    else
    {
        if (*name == '@')
            s.append(" use=\"required\"");
    }
    if (keyed)
    {
        s.append("><xs:annotation><xs:appinfo hpcc:keyed=\"true\"/></xs:annotation>");
        if (*name == '@')
            s.append("</xs:attribute>\n");
        else
            s.append("</xs:element>\n");
    }
    else
        s.append("/>\n");
}

void XmlSchemaBuilder::addField(const char * name, ITypeInfo & type, bool keyed)
{
    if (xml.length() == 0)
        addSchemaPrefix();

    if (!*name)
        return;

    if (*name == '@')
    {
        if (attributes.length())
            appendField(attributes.tos(), name, type, keyed);
    }
    else
        appendField(xml, name, type, keyed);
}

void XmlSchemaBuilder::addSetField(const char * name, const char * itemname, ITypeInfo & type)
{
    if (xml.length() == 0)
        addSchemaPrefix();

    if (!name || !*name) //xpath('') content inherited by parent
        return;

    StringBuffer elementType;
    getXmlTypeName(elementType, *type.queryChildType());

    if (!itemname || !*itemname) // xpaths 'Name', '/Name', and 'Name/' seem to be equivalent
    {
        itemname = name;
        name = NULL;
    }

    if (name && *name)
    {
        xml.append("<xs:element name=\"").append(name).append("\"");
        if (optionalNesting)
            xml.append(" minOccurs=\"0\"");
        xml.append(">").newline();
        xml.append("<xs:complexType><xs:sequence>");                // could use xs::choice instead
        xml.append("<xs:element name=\"All\" minOccurs=\"0\"/>").newline();
    }

    xml.append("<xs:element name=\"").append(itemname).append("\" minOccurs=\"0\" maxOccurs=\"unbounded\" type=\"").append(elementType).append("\"/>").newline();

    if (name && *name)
        xml.append("</xs:sequence></xs:complexType></xs:element>").newline();
}

void XmlSchemaBuilder::beginRecord(const char * name, bool hasMixedContent, unsigned *updatePos)
{
    if (!name || !*name)
        return;
    if (xml.length() == 0)
        addSchemaPrefix(hasMixedContent);

    attributes.append(*new StringBufferItem);

    xml.append("<xs:element name=\"").append(name).append("\"");
    if (optionalNesting)
        xml.append(" minOccurs=\"0\"");
    xml.append(">").newline();
    appendStartComplexType(xml, hasMixedContent, updatePos);
    xml.append("<xs:sequence>").newline();
    nesting.append(optionalNesting);
    optionalNesting = 0;
}

void XmlSchemaBuilder::updateMixedRecord(unsigned updatePos, bool hasMixedContent)
{
    if (updatePos)
        xml.setCharAt(updatePos, hasMixedContent ? '1' : '0');
}

void XmlSchemaBuilder::endRecord(const char * name)
{
    if (!name || !*name)
        return;
    xml.append("</xs:sequence>").newline();
    xml.append(attributes.tos());
    attributes.pop();
    xml.append("</xs:complexType>").newline();
    xml.append("</xs:element>").newline();
    optionalNesting = nesting.popGet();
}

bool XmlSchemaBuilder::beginDataset(const char * name, const char * row, bool hasMixedContent, unsigned *updatePos)
{
    if (xml.length() == 0)
        addSchemaPrefix();

    if ((!name || !*name) && row) // xpath("Name") and xpath("/Name") seem to be equivalent
    {
        name = row;
        row = NULL;
    }

    if (name && *name)
    {
        xml.append("<xs:element name=\"").append(name).append("\"");
        if (!row || !*row)
            xml.append(" minOccurs=\"0\" maxOccurs=\"unbounded\"");
        else if (optionalNesting)
            xml.append(" minOccurs=\"0\"");
        xml.append(">").newline();
        if (row && *row)
            appendStartComplexType(xml, false, NULL);
        else
            appendStartComplexType(xml, hasMixedContent, updatePos);
        xml.newline();
    }

    xml.append("<xs:sequence");
    if (!name || !*name || (row && *row))
        xml.append(" minOccurs=\"0\" maxOccurs=\"unbounded\"");
    xml.append('>').newline();

    if (row && *row)
    {
        attributes.append(*new StringBufferItem);
        xml.append("<xs:element name=\"").append(row).append("\">").newline();
        appendStartComplexType(xml, hasMixedContent, updatePos);
        xml.append("<xs:sequence>").newline();
    }
    nesting.append(optionalNesting);
    optionalNesting = 0;
    return true;
}

void XmlSchemaBuilder::endDataset(const char * name, const char * row)
{
    if ((!name || !*name) && row) // xpath("Name") and xpath("/Name") seem to be equivalent
    {
        name = row;
        row = NULL;
    }

    if (row && *row)
    {
        xml.append("</xs:sequence>").newline();
        xml.append(attributes.tos());
        attributes.pop();
        xml.append("</xs:complexType></xs:element>").newline();
    }
    xml.append("</xs:sequence>").newline();
    if (name && *name)
    {
        xml.append("</xs:complexType>").newline();
        xml.append("</xs:element>").newline();
    }
    optionalNesting = nesting.popGet();
}

bool XmlSchemaBuilder::addSingleFieldDataset(const char * name, const char * childname, ITypeInfo & type)
{
    if (xml.length() == 0)
        addSchemaPrefix();

    if (name && *name)
    {
        xml.append("<xs:element name=\"").append(name).append("\"");
        if (optionalNesting)
            xml.append(" minOccurs=\"0\"");
        xml.append(">").newline();
        xml.append("<xs:complexType>").newline();
    }

    xml.append("<xs:sequence minOccurs=\"0\" maxOccurs=\"unbounded\">").newline();
    addField(childname, type, false);
    xml.append("</xs:sequence>").newline();

    if (name && *name)
    {
        xml.append("</xs:complexType>").newline();
        xml.append("</xs:element>").newline();
    }
    return true;
}



void XmlSchemaBuilder::clear()
{
    xml.clear();
    dataSizes.kill();
    stringSizes.kill();
    decimalSizes.kill();
    nesting.kill();
    optionalNesting = 0;
}


void XmlSchemaBuilder::getXml(StringBuffer & results)
{
    if (xml.length() != 0)
    {
        addSchemaSuffix();
        results.append(xml);
        clear();
    }
}


void XmlSchemaBuilder::getXml(IStringVal & results)
{
    if (xml.length() != 0)
    {
        addSchemaSuffix();
        results.set(xml);
        clear();
    }
    else
        results.clear();
}
