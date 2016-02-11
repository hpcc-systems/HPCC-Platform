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

#include "limits.h"
#ifdef _USE_BOOST_REGEX
#include "boost/regex.hpp" // must precede platform.h ; n.b. this uses a #pragma comment(lib, ...) to link the appropriate .lib in MSVC
#endif
#include "platform.h"
#include <math.h>
#include <stdio.h>
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jlib.hpp"
#include "jptree.hpp"
#include "junicode.hpp"
#include "eclrtl.hpp"
#include "rtlbcd.hpp"
#include "eclrtl_imp.hpp"
#include "unicode/uchar.h"
#include "unicode/ucol.h"
#include "unicode/ustring.h"
#include "unicode/ucnv.h"
#include "unicode/schriter.h"
#include "unicode/regex.h"
#include "unicode/normlzr.h"
#include "unicode/locid.h"
#include "jlog.hpp"
#include "jmd5.hpp"
#include "rtlqstr.ipp"

#include "roxiemem.hpp"

#define UTF8_CODEPAGE "UTF-8"
#define UTF8_MAXSIZE     4

IRandomNumberGenerator * random_;
static CriticalSection random_Sect;

MODULE_INIT(INIT_PRIORITY_ECLRTL_ECLRTL)
{
    random_ = createRandomNumberGenerator();
    random_->seed((unsigned)get_cycles_now());
    return true;
}
MODULE_EXIT()
{
    random_->Release();
}

//=============================================================================
// Miscellaneous string functions...
ECLRTL_API void * rtlMalloc(size32_t size)
{
    if (!size)
        return NULL;

    void * retVal = malloc(size);
    if (!retVal)
    {
        PrintStackReport();
        rtlThrowOutOfMemory(0, "Memory allocation error!");
    }
    return retVal;
}

void rtlFree(void *ptr)
{
    free(ptr);
}

ECLRTL_API void * rtlRealloc(void * _ptr, size32_t size)
{
    void * retVal = realloc(_ptr, size);

    if( (0 < size) && (NULL == retVal))
    {
        PrintStackReport();
        rtlThrowOutOfMemory(0, "Memory reallocation error!");
    }
    return retVal;
}

//=============================================================================

ECLRTL_API void rtlReleaseRow(const void * row)
{
    ReleaseRoxieRow(row);
}

ECLRTL_API void rtlReleaseRowset(unsigned count, byte * * rowset)
{
    ReleaseRoxieRowset(count, rowset);
}

ECLRTL_API void * rtlLinkRow(const void * row)
{
    LinkRoxieRow(row);
    return const_cast<void *>(row);
}

ECLRTL_API byte * * rtlLinkRowset(byte * * rowset)
{
    LinkRoxieRowset(rowset);
    return rowset;
}

//=============================================================================
// Unicode helper classes and functions

// escape

static bool stripIgnorableCharacters(size32_t & lenResult, UChar * & result, size32_t length, const UChar * in)
{
    unsigned numStripped = 0;
    unsigned lastGood = 0;
    for (unsigned i=0; i < length; i++)
    {
        UChar32 c = in[i];
        unsigned stripSize = 0;
        if (U16_IS_SURROGATE(c))
        {
            U16_GET(in, 0, i, length, c);
            if (u_hasBinaryProperty(c, UCHAR_DEFAULT_IGNORABLE_CODE_POINT))
                stripSize = 2;
            else
                i++; // skip the surrogate
        }
        else
        {
            if (u_hasBinaryProperty(c, UCHAR_DEFAULT_IGNORABLE_CODE_POINT))
                stripSize = 1;
        }

        if (stripSize != 0)
        {
            if (numStripped == 0)
                result = (UChar *)rtlMalloc((length-stripSize)*sizeof(UChar));

            //Copy and non ignorable characters skipped up to this point.  (Note result+x is scaled by UChar)
            memcpy(result + lastGood - numStripped, in+lastGood, (i-lastGood) * sizeof(UChar));
            lastGood = i+stripSize;
            numStripped += stripSize;
            i += (stripSize-1);
        }
    }
    if (numStripped == 0)
        return false;
    lenResult = length-numStripped;
    memcpy(result + lastGood - numStripped, in+lastGood, (length-lastGood) * sizeof(UChar));
    return true;
}

void escapeUnicode(unsigned inlen, UChar const * in, StringBuffer & out)
{
    UCharCharacterIterator iter(in, inlen);
    for(iter.first32(); iter.hasNext(); iter.next32())
    {
        UChar32 c = iter.current32();
        if(c < 0x80)
            out.append((char) c);
        else if (c < 0x10000)
            out.appendf("\\u%04X", c);
        else
            out.appendf("\\U%08X", c);
    }
}

// locales and collators

static unsigned const unicodeStrengthLimit = 5;

static UCollationStrength unicodeStrength[unicodeStrengthLimit] =
{
    UCOL_PRIMARY,
    UCOL_SECONDARY,
    UCOL_TERTIARY,
    UCOL_QUATERNARY,
    UCOL_IDENTICAL
};

class RTLLocale : public CInterface
{
public:
    RTLLocale(char const * _locale) : locale(_locale)
    {
        for(unsigned i=0; i<unicodeStrengthLimit; i++)
            colls[i] = NULL;
        UErrorCode err = U_ZERO_ERROR;
        colls[2] = ucol_open(locale.get(), &err);
        assertex(U_SUCCESS(err));
    }
    ~RTLLocale()
    {
        for(unsigned i=0; i<unicodeStrengthLimit; i++)
            if(colls[i]) ucol_close(colls[i]);
    }
    UCollator * queryCollator() const { return colls[2]; }
    UCollator * queryCollator(unsigned strength) const
    {
        if(strength == 0) strength = 1;
        if(strength > unicodeStrengthLimit) strength = unicodeStrengthLimit;
        if(!colls[strength-1])
        {
            UErrorCode err = U_ZERO_ERROR;
            const_cast<UCollator * *>(colls)[strength-1] = ucol_open(locale.get(), &err);
            assertex(U_SUCCESS(err));
            ucol_setStrength(colls[strength-1], unicodeStrength[strength-1]);
        }
        return colls[strength-1];
    }

private:
    StringAttr locale;
    UCollator * colls[unicodeStrengthLimit];
};

typedef MapStringTo<RTLLocale, char const *> MapStrToLocale;
MapStrToLocale *localeMap;
CriticalSection localeCrit;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    localeMap = new MapStrToLocale;
    return true;
}
MODULE_EXIT()
{
    delete localeMap;
}

bool rtlGetNormalizedUnicodeLocaleName(unsigned len, char const * in, char * out)
{
    bool isPrimary = true;
    bool ok = true;
    unsigned i;
    for(i=0; i<len; i++)
        if(in[i] == '_')
        {
            out[i] = '_';
            isPrimary = false;
        }
        else if(isalpha(in[i]))
        {
            out[i] = (isPrimary ? tolower(in[i]) : toupper(in[i]));
        }
        else
        {
            out[i] = 0;
            ok = false;
        }
    return ok;
}

RTLLocale * queryRTLLocale(char const * locale)
{
    if (!locale) locale = "";
    CriticalBlock b(localeCrit);
    RTLLocale * loc = localeMap->getValue(locale);
    if(!loc)
    {
        unsigned ll = strlen(locale);
        StringBuffer lnorm;
        rtlGetNormalizedUnicodeLocaleName(ll, locale, lnorm.reserve(ll));
        localeMap->setValue(locale, lnorm.str());
        loc = localeMap->getValue(locale);
    }
    return loc;
}

// converters

class RTLUnicodeConverter : public CInterface
{
public:
    RTLUnicodeConverter(char const * codepage)
    {
        UErrorCode err = U_ZERO_ERROR;
        conv = ucnv_open(codepage, &err);
        if (!U_SUCCESS(err))
        {
            StringBuffer msg;
            msg.append("Unrecognised codepage '").append(codepage).append("'");
            rtlFail(0, msg.str());
        }
    }
    ~RTLUnicodeConverter()
    {
        ucnv_close(conv);
    }
    UConverter * query() const { return conv; }
private:
    UConverter * conv;
};

typedef MapStringTo<RTLUnicodeConverter, char const *> MapStrToUnicodeConverter;
static __thread MapStrToUnicodeConverter *unicodeConverterMap = NULL;
static __thread ThreadTermFunc prevThreadTerminator = NULL;

static void clearUnicodeConverterMap()
{
    delete unicodeConverterMap;
    unicodeConverterMap = NULL;  // Important to clear, as this is called when threadpool threads end...
    if (prevThreadTerminator)
    {
        (*prevThreadTerminator)();
        prevThreadTerminator = NULL;
    }
}

RTLUnicodeConverter * queryRTLUnicodeConverter(char const * codepage)
{
    if (!unicodeConverterMap) // NB: one per thread, so no contention
    {
        unicodeConverterMap = new MapStrToUnicodeConverter;
        // Use thread terminator hook to clear them up on thread exit.
        // NB: May need to revisit if not on a jlib Thread.
        prevThreadTerminator = addThreadTermFunc(clearUnicodeConverterMap);
    }
    RTLUnicodeConverter * conv = unicodeConverterMap->getValue(codepage);
    if(!conv)
    {
        unicodeConverterMap->setValue(codepage, codepage);
        conv = unicodeConverterMap->getValue(codepage);
    }
    return conv;
}

// normalization

bool unicodeNeedsNormalize(unsigned inlen, UChar * in, UErrorCode * err)
{
    return !unorm_isNormalized(in, inlen, UNORM_NFC, err);
}

bool vunicodeNeedsNormalize(UChar * in, UErrorCode * err)
{
    return !unorm_isNormalized(in, -1, UNORM_NFC, err);
}

void unicodeReplaceNormalized(unsigned inlen, UChar * in, UErrorCode * err)
{
    UChar * buff = (UChar *)rtlMalloc(inlen*2);
    unsigned len = unorm_normalize(in, inlen, UNORM_NFC, 0, buff, inlen, err);
    while(len<inlen) buff[len++] = 0x0020;
    memcpy(in, buff, inlen);
    free(buff);
}

void vunicodeReplaceNormalized(unsigned inlen, UChar * in, UErrorCode * err)
{
    UChar * buff = (UChar *)rtlMalloc(inlen*2);
    unsigned len = unorm_normalize(in, -1, UNORM_NFC, 0, buff, inlen-1, err);
    buff[len] = 0x0000;
    memcpy(in, buff, inlen);
    free(buff);
}

void unicodeGetNormalized(unsigned & outlen, UChar * & out, unsigned inlen, UChar * in, UErrorCode * err)
{
    outlen = unorm_normalize(in, inlen, UNORM_NFC, 0, 0, 0, err);
    out = (UChar *)rtlMalloc(outlen*2);
    unorm_normalize(in, inlen, UNORM_NFC, 0, out, outlen, err);
}

void vunicodeGetNormalized(UChar * & out, unsigned inlen, UChar * in, UErrorCode * err)
{
    unsigned outlen = unorm_normalize(in, inlen, UNORM_NFC, 0, 0, 0, err);
    out = (UChar *)rtlMalloc((outlen+1)*2);
    unorm_normalize(in, inlen, UNORM_NFC, 0, out, outlen, err);
    out[outlen] = 0x0000;
}

void unicodeEnsureIsNormalized(unsigned len, UChar * str)
{
    UErrorCode err = U_ZERO_ERROR;
    if(unicodeNeedsNormalize(len, str, &err))
        unicodeReplaceNormalized(len, str, &err);
}

void vunicodeEnsureIsNormalized(unsigned len, UChar * str)
{
    UErrorCode err = U_ZERO_ERROR;
    if(vunicodeNeedsNormalize(str, &err))
        vunicodeReplaceNormalized(len, str, &err);
}

void unicodeEnsureIsNormalizedX(unsigned & len, UChar * & str)
{
    UErrorCode err = U_ZERO_ERROR;
    if(unicodeNeedsNormalize(len, str, &err))
    {
        unsigned inlen = len;
        UChar * in = str;
        unicodeGetNormalized(len, str, inlen, in, &err);
        free(in);
    }
}

void vunicodeEnsureIsNormalizedX(unsigned inlen, UChar * & str)
{
    UErrorCode err = U_ZERO_ERROR;
    if(unicodeNeedsNormalize(inlen, str, &err))
    {
        UChar * in = str;
        vunicodeGetNormalized(str, inlen, in, &err);
        free(in);
    }
}

void unicodeNormalizedCopy(UChar * out, UChar * in, unsigned len)
{
    UErrorCode err = U_ZERO_ERROR;
    if(unicodeNeedsNormalize(len, in, &err))
        unorm_normalize(in, len, UNORM_NFC, 0, out, len, &err);
    else
        memcpy(out, in, len);
}

void normalizeUnicodeString(UnicodeString const & in, UnicodeString & out)
{
    UErrorCode err = U_ZERO_ERROR;
    Normalizer::compose(in, false, 0, out, err);
    assertex(U_SUCCESS(err));
}

// padding

static void multimemset(char * out, size_t outlen, char const * in, size_t inlen)
{
    size_t outpos = 0;
    size_t inpos = 0;
    while(outpos < outlen)
    {
        out[outpos++] = in[inpos++];
        if(inpos == inlen)
            inpos = 0;
    }
}

typedef MapStringTo<MemoryAttr, size32_t> MemoryAttrMapping;
MemoryAttrMapping *unicodeBlankCache;
CriticalSection ubcCrit;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    unicodeBlankCache = new MemoryAttrMapping;
    return true;
}
MODULE_EXIT()
{
    delete unicodeBlankCache;
}

UChar unicodeSpace = 0x0020;

void codepageBlankFill(char const * codepage, char * out, size_t len)
{
    CriticalBlock b(ubcCrit);
    MemoryAttr * cached = unicodeBlankCache->getValue(codepage);
    if(cached)
    {
        char const * blank = (char const *)cached->get();
        size_t blanklen = cached->length();
        if(blanklen==1)
            memset(out, *blank, len);
        else
            multimemset(out, len, blank, blanklen);
    }
    else
    {
        unsigned blanklen;
        char * blank;
        rtlUnicodeToCodepageX(blanklen, blank, 1, &unicodeSpace, codepage);
        unicodeBlankCache->setValue(codepage, blanklen);
        unicodeBlankCache->getValue(codepage)->set(blanklen, blank);
        if(blanklen==1)
            memset(out, *blank, len);
        else
            multimemset(out, len, blank, blanklen);
        free(blank);
    }
}

//---------------------------------------------------------------------------
// floating point functions

static const double smallPowers[16] = { 
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7,
    1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15 };

static double powerOfTen(int x)
{
    if (x < 0)
        return 1 / powerOfTen(-x);
    double value = smallPowers[x&15];
    double scale = 1e16;
    x >>= 4;
    while (x)
    {
        if (x & 1)
            value *= scale;
        scale *= scale;
        x >>= 1;
    }
    return value;
};


static double kk = (1.0 / ((unsigned __int64)1<<53));
__int64 rtlRound(double x)
{
    //a fudge to make numbers that are inexact after a division round up "correctly".
    //coded rather oddly as microsoft's optimizer has a habit of throwing it away otherwise...
    volatile double tt = x * kk;
    x += tt;
    if (x >= 0.0)
        return (__int64)(x + 0.5);
    return -(__int64)(-x + 0.5);
}

double rtlRoundTo(const double x, int places)
{
    if (x < 0)
        return -rtlRoundTo(-x, places);
    volatile double tt = x * kk;
    double x0 = x + tt;
    if (places >= 0)
    {
        double scale = powerOfTen(places);
        return floor(x * scale + 0.5) / scale;
    }
    else
    {
        double scale = powerOfTen(-places);
        return floor(x / scale + 0.5) * scale;
    }
}

__int64 rtlRoundDown(double x)
{
    if (x >= 0.0)
        return (__int64)floor(x);
    return (__int64)ceil(x);
}

__int64 rtlRoundUp(double x)
{
    if (x >= 0.0)
        return (__int64)ceil(x);
    return (__int64)floor(x);
}


//=============================================================================
// Numeric conversion functions... - fixed length target

#define intToStringNBody() \
    unsigned len = numtostr(temp, val); \
    if (len > l) \
        memset(t,'*',l); \
    else \
    { \
        memcpy(t,temp,len); \
        memset(t+len, ' ', l-len); \
    }


void rtlUInt4ToStr(size32_t l, char * t, unsigned val)
{
    char temp[20];
    intToStringNBody();
}

void rtlUInt8ToStr(size32_t l, char * t, unsigned __int64 val)
{
    char temp[40];
    intToStringNBody();
}

void rtlInt4ToStr(size32_t l, char * t, int val)
{
    char temp[20];
    intToStringNBody();
}

void rtlInt8ToStr(size32_t l, char * t, __int64 val)
{
    char temp[40];
    intToStringNBody();
}

//=============================================================================
// Numeric conversion functions... - unknown length target

#define intToUnknownStringBody() \
    unsigned len = numtostr(temp, val); \
    char * result = (char *)rtlMalloc(len); \
    memcpy(result, temp, len); \
    l = len; \
    t = result;

void rtlUInt4ToStrX(size32_t & l, char * & t, unsigned val)
{
    char temp[20];
    intToUnknownStringBody();
}

void rtlUInt8ToStrX(size32_t & l, char * & t, unsigned __int64 val)
{
    char temp[40];
    intToUnknownStringBody();
}

void rtlInt4ToStrX(size32_t & l, char * & t, int val)
{
    char temp[20];
    intToUnknownStringBody();
}

void rtlInt8ToStrX(size32_t & l, char * & t, __int64 val)
{
    char temp[40];
    intToUnknownStringBody();
}

//=============================================================================
// Numeric conversion functions... - fixed length ebcdic target

// ILKA - converting ebcdic to numeric still uses string in between, for more efficiency
//        a function numtoebcdicstr should be implemented

#define intToEbcdicStringNBody() \
    unsigned len = numtostr(astr, val); \
    rtlStrToEStr(sizeof(estr),estr,len,astr); \
    if (len > l) \
        memset(t,0x2A,l); \
    else \
    { \
        memcpy(t,estr,len); \
        memset(t+len, '@', l-len); \
    }

void rtl_l42en(size32_t l, char * t, unsigned val)
{
    char astr[20];
    char estr[20];
    intToEbcdicStringNBody();
}

void rtl_l82en(size32_t l, char * t, unsigned __int64 val)
{
    char astr[40];
    char estr[40];
    intToEbcdicStringNBody();
}

void rtl_ls42en(size32_t l, char * t, int val)
{
    char astr[20];
    char estr[20];
    intToEbcdicStringNBody();
}

void rtl_ls82en(size32_t l, char * t, __int64 val)
{
    char astr[40];
    char estr[40];
    intToEbcdicStringNBody();
}

//=============================================================================
// Numeric conversion functions... - unknown length ebcdic target

#if defined _MSC_VER
#pragma warning(push)
#pragma warning(disable:4700)
#endif
void rtl_l42ex(size32_t & l, char * & t, unsigned val)
{
    char astr[20];
    unsigned alen = numtostr(astr, val);
    rtlStrToEStrX(l,t,alen,astr);
}

void rtl_l82ex(size32_t & l, char * & t, unsigned __int64 val)
{
    char astr[40];
    unsigned alen = numtostr(astr, val);
    rtlStrToEStrX(l,t,alen,astr);
}

void rtl_ls42ex(size32_t & l, char * & t, int val)
{
    char astr[20];
    unsigned alen = numtostr(astr, val);
    rtlStrToEStrX(l,t,alen,astr);
}

void rtl_ls82ex(size32_t & l, char * & t, __int64 val)
{
    char astr[40];
    unsigned alen = numtostr(astr, val);
    rtlStrToEStrX(l,t,alen,astr);
}
#ifdef _MSC_VER
#pragma warning(pop)
#endif

//=============================================================================
// Numeric conversion functions... - fixed length variable target

#define intToVarStringNBody() \
    unsigned len = numtostr(temp, val) + 1; \
    if (len > l) \
    { \
        memset(t,'*',l); \
        t[l-1]=0; \
    } \
    else \
        memcpy(t,temp,len);

void rtlUInt4ToVStr(size32_t l, char * t, unsigned val)
{
    char temp[20];
    intToVarStringNBody();
}

void rtlUInt8ToVStr(size32_t l, char * t, unsigned __int64 val)
{
    char temp[40];
    intToVarStringNBody();
}

void rtlInt4ToVStr(size32_t l, char * t, int val)
{
    char temp[20];
    intToVarStringNBody();
}

void rtlInt8ToVStr(size32_t l, char * t, __int64 val)
{
    char temp[40];
    intToVarStringNBody();
}

//=============================================================================
// Numeric conversion functions... - unknown length variable target

#define intToVarStringXBody() \
    unsigned len = numtostr(temp, val); \
    temp[len] = 0;                      \
    return strdup(temp);

char * rtlUInt4ToVStrX(unsigned val)
{
    char temp[20];
    intToVarStringXBody();
}

char * rtlUInt8ToVStrX(unsigned __int64 val)
{
    char temp[40];
    intToVarStringXBody();
}

char * rtlInt4ToVStrX(int val)
{
    char temp[20];
    intToVarStringXBody();
}

char * rtlInt8ToVStrX(__int64 val)
{
    char temp[40];
    intToVarStringXBody();
}

//---------------------------------------------------------------------------

double rtlStrToReal(size32_t l, const char * t)
{
    char * temp = (char *)alloca(l+1);
    memcpy(temp, t, l);
    temp[l] = 0;
    return rtlVStrToReal(temp);
}

double rtlEStrToReal(size32_t l, const char * t)
{
    char * astr = (char*)alloca(l);
    rtlEStrToStr(l,astr,l,t);
    char * temp = (char *)alloca(l+1);
    memcpy(temp, astr, l);
    temp[l] = 0;
    return rtlVStrToReal(temp);
}

double rtlVStrToReal(const char * t)
{
    char * end;
    return strtod(t, &end);
}

double rtl_ex2f(const char * t)
{
    unsigned len = strlen(t);
    char * astr = (char*)alloca(len+1);
    rtlEStrToStr(len,astr,len,t);
    astr[len] = 0;
    return rtlVStrToReal(astr);
}

double rtlUnicodeToReal(size32_t l, UChar const * t)
{
    unsigned bufflen;
    char * buff;
    rtlUnicodeToCodepageX(bufflen, buff, l, t, "ascii");
    double ret = rtlStrToReal(bufflen, buff);
    rtlFree(buff);
    return ret;
}

//---------------------------------------------------------------------------

static void truncFixedReal(size32_t l, char * t, StringBuffer & temp)
{
    const char * str = temp.str();
    unsigned len = temp.length();
    if (len > l)
    {
        //If we don't lose significant digits left of the decimal point then truncate the string.
        const char * dot = strchr(str, '.');
        if (dot && ((size_t)(dot - str) <= l))
            len = l;
    }

    if (len > l)
        memset(t,'*',l);
    else
    {
        memcpy(t,temp.str(),len);
        memset(t+len, ' ', l-len);
    }
}

static void roundFixedReal(size32_t l, char * t, StringBuffer & temp)
{
    const char * str = temp.str();
    unsigned len = temp.length();
    if (len > l)
    {
        //If we don't lose significant digits left of the decimal point then truncate the string.
        const char * dot = strchr(str, '.');
        if (dot && ((size_t)(dot - str) <= l))
        {
            len = l;
            //Unfortunately we now need to potentially round the number which could even lead to
            //an extra digit, and failure to fit.  Is there a simpler way of handling this?
            bool decimalIsNext = ((dot - str) == l);
            char next = decimalIsNext ? dot[1] : str[len];
            bool rounding = (next >= '5');
            unsigned cur = len;
            while ((cur > 0) && rounding)
            {
                next = str[cur-1];
                if (next == '-')
                    break;
                if (next != '.')
                {
                    if (next != '9')
                    {
                        temp.setCharAt(cur-1, next+1);
                        rounding = false;
                        break;
                    }
                    else
                        temp.setCharAt(cur-1, '0');
                }
                cur--;
            }
            if (rounding)
            {
                //Ugly, but it is an exceptional case.
                if (!decimalIsNext)
                    temp.insert(cur, '1');
                else
                    len++; // overflow
            }
        }
    }

    if (len > l)
        memset(t,'*',l);
    else
    {
        memcpy(t,temp.str(),len);
        memset(t+len, ' ', l-len);
    }
}

void rtlRealToStr(size32_t l, char * t, double val)
{
    StringBuffer temp;
    temp.append(val);

    //This could either truncate or round when converting a real to a string
    //Rounding is more user friendly, but then (string3)(string)1.99 != (string3)1.99 which is
    //rather count intuitive.  (That is still true if the value is out of range.)
    truncFixedReal(l, t, temp);
}

void rtlRealToStr(size32_t l, char * t, float val)
{
    StringBuffer temp;
    temp.append(val);

    //See comment above
    truncFixedReal(l, t, temp);
}

void rtlRealToStrX(size32_t & l, char * & t, double val)
{
    StringBuffer temp;
    temp.append(val);
    unsigned len = temp.length();
    char * result = (char *)rtlMalloc(len);
    memcpy(result,temp.str(),len);
    l = len;
    t = result;
}

void rtlRealToStrX(size32_t & l, char * & t, float val)
{
    StringBuffer temp;
    temp.append(val);
    unsigned len = temp.length();
    char * result = (char *)rtlMalloc(len);
    memcpy(result,temp.str(),len);
    l = len;
    t = result;
}

void rtlRealToVStr(size32_t l, char * t, double val)
{
    StringBuffer temp;
    temp.append(val);
    unsigned len = temp.length()+1;
    if (len > l)
    {
        memset(t,'*',l);
        t[l-1]=0;
    }
    else
    {
        memcpy(t,temp.str(),len);
    }
}

void rtlRealToVStr(size32_t l, char * t, float val)
{
    StringBuffer temp;
    temp.append(val);
    unsigned len = temp.length()+1;
    if (len > l)
    {
        memset(t,'*',l);
        t[l-1]=0;
    }
    else
    {
        memcpy(t,temp.str(),len);
    }
}

char * rtlRealToVStrX(double val)
{
    StringBuffer temp;
    temp.append(val);
    return strdup(temp);
}

char * rtlRealToVStrX(float val)
{
    StringBuffer temp;
    temp.append(val);
    return strdup(temp);
}

//---------------------------------------------------------------------------

#define SkipSpaces(l, t) \
    while (l)            \
    {                    \
        char c = *t;     \
        switch (c)       \
        {                \
        case ' ':        \
        case '\t':       \
        case '-':        \
        case '+':        \
            break;       \
        default:         \
            goto done;   \
        }                \
        l--;             \
        t++;             \
    }                    \
done:


#define SkipSignSpaces(l, t, negate) \
    while (l)            \
    {                    \
        char c = *t;     \
        switch (c)       \
        {                \
        case '-':        \
            negate = true; \
            break;       \
        case ' ':        \
        case '\t':       \
        case '+':        \
            break;       \
        default:         \
            goto done;   \
        }                \
        l--;             \
        t++;             \
    }                    \
done:


unsigned rtlStrToUInt4(size32_t l, const char * t)
{
    SkipSpaces(l, t);
    unsigned v = 0;
    while (l--)
    {
        char c = *t++;
        if ((c >= '0') && (c <= '9')) 
            v = v * 10 + (c-'0');
        else
            break;
    }
    return v;
}

unsigned __int64 rtlStrToUInt8(size32_t l, const char * t)
{
    SkipSpaces(l, t);
    unsigned __int64 v = 0;
    while (l--)
    {
        char c = *t++;
        if ((c >= '0') && (c <= '9')) 
            v = v * 10 + (c-'0');
        else
            break;
    }
    return v;
}

int rtlStrToInt4(size32_t l, const char * t)
{
    bool negate = false;
    SkipSignSpaces(l, t, negate);
    int v = 0;
    while (l--)
    {
        char c = *t++;
        if ((c >= '0') && (c <= '9')) 
            v = v * 10 + (c-'0');
        else 
            break;
    }
    return negate ? -v : v;
}

__int64 rtlStrToInt8(size32_t l, const char * t)
{
    bool negate = false;
    SkipSignSpaces(l, t, negate);
    __int64 v = 0;
    while (l--)
    {
        char c = *t++;
        if ((c >= '0') && (c <= '9')) 
            v = v * 10 + (c-'0');
        else 
            break;
    }
    return negate ? -v : v;
}

__int64 rtlUnicodeToInt8(size32_t l, UChar const * t)
{
    unsigned bufflen;
    char * buff;
    rtlUnicodeToCodepageX(bufflen, buff, l, t, "ascii");
    __int64 ret = rtlStrToInt8(bufflen, buff);
    rtlFree(buff);
    return ret;
}

bool rtlStrToBool(size32_t l, const char * t)
{
    while (l--)
    {
        char c = *t++;
        if (c != ' ')
            return true;
    }
    return false;
}

bool rtlUnicodeToBool(size32_t l, UChar const * t)
{
    while(l--)
        if(*t++ != 0x20) return true;
    return false;
}

// return true for "on", "true" or any non-zero constant, else false;
bool rtlCsvStrToBool(size32_t l, const char * t)
{
    return clipStrToBool(l, t);
}


//---------------------------------------------------------------------------

unsigned rtlEStrToUInt4(size32_t l, const char * t)
{
    char * astr = (char*)alloca(l);
    rtlEStrToStr(l,astr,l,t);
    return rtlStrToUInt4(l,astr);
}

unsigned __int64 rtlEStrToUInt8(size32_t l, const char * t)
{
    char * astr = (char*)alloca(l);
    rtlEStrToStr(l,astr,l,t);
    return rtlStrToUInt8(l,astr);
}

int rtlEStrToInt4(size32_t l, const char * t)
{
    char * astr = (char*)alloca(l);
    rtlEStrToStr(l,astr,l,t);
    return rtlStrToInt4(l,astr);
}

__int64 rtlEStrToInt8(size32_t l, const char * t)
{
    char * astr = (char*)alloca(l);
    rtlEStrToStr(l,astr,l,t);
    return rtlStrToInt8(l,astr);
}

bool rtl_en2b(size32_t l, const char * t)
{
    char * astr = (char*)alloca(l);
    rtlEStrToStr(l,astr,l,t);
    return rtlStrToBool(l,astr);
}

//---------------------------------------------------------------------------

unsigned rtlVStrToUInt4(const char * t)
{
    return rtlStrToUInt4(strlen(t), t);
}

unsigned __int64 rtlVStrToUInt8(const char * t)
{
    return rtlStrToUInt8(strlen(t), t);
}

int rtlVStrToInt4(const char * t)
{
    return rtlStrToInt4(strlen(t), t);
}

__int64 rtlVStrToInt8(const char * t)
{
    return rtlStrToInt8(strlen(t), t);
}

bool rtlVStrToBool(const char * t)
{
    char c;
    while ((c = *t++) != 0)
    {
        //MORE: Allow spaces if we change the semantics.
        return true;
    }
    return false;
}

//---------------------------------------------------------------------------

void holeIntFormat(size32_t maxlen, char * target, __int64 value, unsigned width, unsigned flags)
{
    StringBuffer result;
    if (flags & 1)
        result.appendf("%0*" I64F "d", width, value);
    else
        result.appendf("%*" I64F "d", width, value);
    size32_t written = result.length();
    if (written > maxlen)
        memset(target, '*', maxlen);
    else
    {
        memset(target+written, ' ', maxlen-written);
        memcpy(target, result.str(), written);
    }
}

void holeRealFormat(size32_t maxlen, char * target, double value, unsigned width, unsigned places)
{
    if ((int) width <= 0)
        return;

    const unsigned tempSize = 500;
    char temp[tempSize*2+2];  // Space for leading digits/0, '-' and \0 terminator

    //Ensure that we output at most 2*tempSize characters.
    unsigned formatWidth = width < tempSize ? width : tempSize;
    if (places >= formatWidth)
        places = formatWidth-1;
    unsigned written = sprintf(temp, "%*.*f", formatWidth, places, value);

    const char * src = temp;
    if (written > width)
    {
        //Strip a leading 0 for very small numbers.
        if (*src == '0')
        {
            written--;
            src++;
        }
    }
    if (written > width)
    {
        memset(target, '*', width);
        if (places)
            target[width-places-1] = '.';
    }
    else
    {
        unsigned delta = width - written;
        if (delta)
            memset(target, ' ', delta);
        memcpy(target+delta, src, written);
    }
}

//=============================================================================
// Conversion functions...

void rtlIntFormat(unsigned & len, char * & target, __int64 value, unsigned width, unsigned flags)
{
    if ((int) width <= 0)
    {
        len = 0;
        target = NULL;
        return;
    }
    len = width;
    target = (char *)rtlMalloc(width);
    holeIntFormat(width, target, value, width, flags);
}

void rtlRealFormat(unsigned & len, char * & target, double value, unsigned width, unsigned places)
{
    if ((int) width < 0)
    {
        len = 0;
        target = NULL;
        return;
    }
    len = width;
    target = (char *)rtlMalloc(width);
    holeRealFormat(width, target, value, width, places);
}

//=============================================================================
// String functions...

bool rtlDataToBool(unsigned len, const void * _src)
{
    const char * src = (const char *)_src;
    while (len--)
        if (*src++)
            return true;
    return false;
}

void rtlBoolToData(unsigned tlen, void * tgt, bool src)
{
    memset(tgt, 0, tlen);
    if (src)
        ((char *)tgt)[tlen-1] = 1;
}

void rtlBoolToStr(unsigned tlen, void * tgt, bool src)
{
    memset(tgt, ' ', tlen);
    if (src)
        ((char *)tgt)[tlen-1] = '1';
}

void rtlBoolToVStr(char * tgt, bool src)
{
    if (src)
        *tgt++ = '1';
    *tgt = 0;
}

void rtlBoolToStrX(unsigned & tlen, char * & tgt, bool src)
{
    if (src)
    {
        char * ret = (char *)rtlMalloc(1);
        ret[0] = '1';
        tlen = 1;
        tgt = ret;
    }
    else
    {
        tlen = 0;
        tgt = NULL;
    }
}

char * rtlBoolToVStrX(bool src)
{
    if (src)
        return strdup("1");
    else
        return strdup("");
}

//-----------------------------------------------------------------------------
// String copying functions....

void rtlDataToData(unsigned tlen, void * tgt, unsigned slen, const void * src)
{
    if (slen > tlen)
        slen = tlen;
    memcpy(tgt, src, slen);
    if (tlen > slen)
        memset((char *)tgt+slen, 0, tlen-slen);
}

void rtlStrToData(unsigned tlen, void * tgt, unsigned slen, const void * src)
{
    if (slen > tlen)
        slen = tlen;
    memcpy(tgt, src, slen);
    if (tlen > slen)
        memset((char *)tgt+slen, 0, tlen-slen);
}

void rtlStrToStr(unsigned tlen, void * tgt, unsigned slen, const void * src)
{
    if (slen > tlen)
        slen = tlen;
    memcpy(tgt, src, slen);
    if (tlen > slen)
        memset((char *)tgt+slen, ' ', tlen-slen);
}

void rtlStrToVStr(unsigned tlen, void * tgt, unsigned slen, const void * src)
{
    if ((slen >= tlen) && (tlen != 0))
        slen = tlen-1;
    memcpy(tgt, src, slen);
    *((char *)tgt+slen)=0;
}

void rtlStr2EStr(unsigned tlen, char * tgt, unsigned slen, const char * src)
{
    rtlStrToEStr(tlen,tgt,slen,src);
}

void rtlEStr2Data(unsigned tlen, void * tgt, unsigned slen, const char * src)
{
    if (slen > tlen)
        slen = tlen;
    rtlEStrToStr(slen,(char *)tgt,slen,src);
    if (tlen > slen)
        memset((char *)tgt+slen, 0, tlen-slen);
}

void rtlEStr2Str(unsigned tlen, void * tgt, unsigned slen, const char * src)
{
    rtlEStrToStr(tlen,(char *)tgt,slen,src);
}

void rtlEStrToVStr(unsigned tlen, void * tgt, unsigned slen, const char * src)
{
    if (slen >= tlen)
        slen = tlen-1;
    rtlEStrToStr(slen,(char *)tgt,slen,src);
    *((char *)tgt+slen)=0;
}

void rtlEStrToEStr(unsigned tlen, void * tgt, unsigned slen, const void * src)
{
    if (slen > tlen)
        slen = tlen;
    memcpy(tgt, src, slen);
    if (tlen > slen)
        memset((char *)tgt+slen, '@', tlen-slen);
}

void rtlVStrToData(unsigned tlen, void * tgt, const char * src)
{
    rtlStrToData(tlen, tgt, strlen(src), src);
}

void rtlVStrToStr(unsigned tlen, void * tgt, const char * src)
{
    rtlStrToStr(tlen, tgt, strlen(src), src);
}

void rtlVStr2EStr(unsigned tlen, char * tgt, const char * src)
{
    rtlStr2EStr(tlen, tgt, strlen(src), src);
}

void rtlVStrToVStr(unsigned tlen, void * tgt, const char * src)
{
    rtlStrToVStr(tlen, tgt, strlen(src), src);
}

char *rtlCreateQuotedString(unsigned _len_tgt,char * tgt)
{
    // Add ' at start and end. MORE! also needs to handle embedded quotes
    char * result = (char *)rtlMalloc(_len_tgt + 3);
    result[0] = '\'';
    memcpy(result+1, tgt, _len_tgt);
    result[_len_tgt+1] = '\'';
    result[_len_tgt+2] = 0;
    return result;
}

//-----------------------------------------------------------------------------

//List of strings with length of -1 to mark the end...
void rtlConcat(unsigned & tlen, char * * tgt, ...)
{
    va_list args;

    unsigned totalLength = 0;
    va_start(args, tgt);
    for (;;)
    {
        unsigned len = va_arg(args, unsigned);
        if (len+1==0)
            break;
        char * str = va_arg(args, char *);
        totalLength += len;
    }
    va_end(args);

    char * buffer = (char *)rtlMalloc(totalLength);
    char * cur = buffer;
    va_start(args, tgt);
    for (;;)
    {
        unsigned len = va_arg(args, unsigned);
        if (len+1==0)
            break;
        char * str = va_arg(args, char *);
        memcpy(cur, str, len);
        cur += len;
    }
    va_end(args);

    tlen = totalLength;
    *tgt = buffer;
}

void rtlConcatVStr(char * * tgt, ...)
{
    va_list args;

    unsigned totalLength = 0;
    va_start(args, tgt);
    for (;;)
    {
        unsigned len = va_arg(args, unsigned);
        if (len+1==0)
            break;
        char * str = va_arg(args, char *);
        totalLength += len;
    }
    va_end(args);

    char * buffer = (char *)rtlMalloc(totalLength+1);
    char * cur = buffer;
    va_start(args, tgt);
    for (;;)
    {
        unsigned len = va_arg(args, unsigned);
        if (len+1==0)
            break;
        char * str = va_arg(args, char *);
        memcpy(cur, str, len);
        cur += len;
    }
    va_end(args);

    cur[0] = 0;
    *tgt = buffer;
}

void rtlConcatUnicode(unsigned & tlen, UChar * * tgt, ...)
{
    va_list args;

    unsigned totalLength = 0;
    va_start(args, tgt);
    for(;;)
    {
        unsigned len = va_arg(args, unsigned);
        if(len+1==0)
            break;
        UChar * str = va_arg(args, UChar *);
        totalLength += len;
    }
    va_end(args);

    UChar * buffer = (UChar *)rtlMalloc(totalLength*2); //I *believe* this is a valid upper limit, as an NFC concatenation can only be shorter than the sum of its parts
    unsigned idx = 0;
    UErrorCode err = U_ZERO_ERROR;
    va_start(args, tgt);
    for(;;)
    {
        unsigned len = va_arg(args, unsigned);
        if(len+1==0)
            break;
        UChar * str = va_arg(args, UChar *);
        if (len)
            idx = unorm_concatenate(buffer, idx, str, len, buffer, totalLength, UNORM_NFC, 0, &err);
    }
    va_end(args);

    *tgt = buffer;
    tlen = idx;
}

void rtlConcatVUnicode(UChar * * tgt, ...)
{
    va_list args;

    unsigned totalLength = 0;
    va_start(args, tgt);
    for(;;)
    {
        unsigned len = va_arg(args, unsigned);
        if(len+1==0)
            break;
        UChar * str = va_arg(args, UChar *);
        totalLength += len;
    }
    va_end(args);

    UChar * buffer = (UChar *)rtlMalloc((totalLength+1)*2); //I *believe* this is a valid upper limit, as an NFC concatenation can only be shorter than the sum of its parts
    unsigned idx = 0;
    UErrorCode err = U_ZERO_ERROR;
    va_start(args, tgt);
    for(;;)
    {
        unsigned len = va_arg(args, unsigned);
        if(len+1==0)
            break;
        UChar * str = va_arg(args, UChar *);
        if (len)
            idx = unorm_concatenate(buffer, idx, str, len, buffer, totalLength, UNORM_NFC, 0, &err);
    }
    va_end(args);

    buffer[idx++] = 0x0000;
    *tgt = buffer;
}

//List of strings with length of -1 to mark the end...
void rtlConcatStrF(unsigned tlen, void * _tgt, int fill, ...)
{
    va_list args;

    char * tgt = (char *)_tgt;
    unsigned offset = 0;
    va_start(args, fill);
    while (offset != tlen)
    {
        unsigned len = va_arg(args, unsigned);
        if (len+1==0)
            break;
        const char * str = va_arg(args, const char *);
        unsigned copyLen = len + offset > tlen ? tlen - offset : len;
        memcpy(tgt+offset, str, copyLen);
        offset += copyLen;
    }
    va_end(args);

    if (offset < tlen)
        memset(tgt+offset, fill, tlen-offset);
}



void rtlConcatVStrF(unsigned tlen, char * tgt, ...)
{
    va_list args;

    unsigned offset = 0;
    va_start(args, tgt);
    while (offset != tlen)
    {
        unsigned len = va_arg(args, unsigned);
        if (len+1==0)
            break;
        const char * str = va_arg(args, const char *);
        unsigned copyLen = len + offset > tlen ? tlen - offset : len;
        memcpy(tgt+offset, str, copyLen);
        offset += copyLen;
    }
    va_end(args);

    memset(tgt+offset, 0, (tlen+1)-offset);
}


void rtlConcatUnicodeF(unsigned tlen, UChar * tgt, ...)
{
    va_list args;
    unsigned idx = 0;
    UErrorCode err = U_ZERO_ERROR;
    va_start(args, tgt);
    for(;;)
    {
        unsigned len = va_arg(args, unsigned);
        if(len+1==0)
            break;
        UChar * str = va_arg(args, UChar *);
        if (len)
            idx = unorm_concatenate(tgt, idx, str, len, tgt, tlen, UNORM_NFC, 0, &err);
    }
    va_end(args);

    while (idx < tlen)
        tgt[idx++] = ' ';
}


void rtlConcatVUnicodeF(unsigned tlen, UChar * tgt, ...)
{
    va_list args;
    unsigned idx = 0;
    UErrorCode err = U_ZERO_ERROR;
    va_start(args, tgt);
    for(;;)
    {
        unsigned len = va_arg(args, unsigned);
        if(len+1==0)
            break;
        UChar * str = va_arg(args, UChar *);
        if (len)
            idx = unorm_concatenate(tgt, idx, str, len, tgt, tlen, UNORM_NFC, 0, &err);
    }
    va_end(args);

    while (idx < tlen)
        tgt[idx++] = 0;
    tgt[tlen] = 0;
}


//------------------------------------------------------------------------------------------------
// The followinf concat functions are all deprecated in favour of the variable number of argument
// versions
unsigned rtlConcatStrToStr(unsigned tlen, char * tgt, unsigned idx, unsigned slen, const char * src)
{
    unsigned len = tlen-idx;
    if (len > slen)
        len = slen;
    memcpy(tgt+idx, src, len);
    return idx+len;
}

unsigned rtlConcatVStrToStr(unsigned tlen, char * tgt, unsigned idx, const char * src)
{
    while (idx != tlen)
    {
        char next = *src++;
        if (!next)
            break;
        tgt[idx++] = next;
    }
    return idx;
}

void rtlConcatStrToVStr(unsigned tlen, void * _tgt, unsigned slen, const void * src)
{
    char * tgt = (char *)_tgt;
    unsigned tend = strlen(tgt);
    rtlStrToVStr(tlen-tend, tgt+tend, slen, src);
}

void rtlConcatVStrToVStr(unsigned tlen, void * _tgt, const char * src)
{
    char * tgt = (char *)_tgt;
    unsigned tend = strlen(tgt);
    rtlVStrToVStr(tlen-tend, tgt+tend, src);
}

unsigned rtlConcatUnicodeToUnicode(unsigned tlen, UChar * tgt, unsigned idx, unsigned slen, UChar const * src)
{
    UErrorCode err = U_ZERO_ERROR;
    return unorm_concatenate(tgt, idx, src, slen, tgt, tlen, UNORM_NFC, 0, &err);
}

unsigned rtlConcatVUnicodeToUnicode(unsigned tlen, UChar * tgt, unsigned idx, UChar const * src)
{
    return rtlConcatUnicodeToUnicode(tlen, tgt, idx, rtlUnicodeStrlen(src), src);
}

void rtlESpaceFill(unsigned tlen, char * tgt, unsigned idx)
{
    if (idx < tlen)
        memset(tgt+idx, '@', tlen-idx);
}

void rtlSpaceFill(unsigned tlen, char * tgt, unsigned idx)
{
    if (idx < tlen)
        memset(tgt+idx, ' ', tlen-idx);
}

void rtlZeroFill(unsigned tlen, char * tgt, unsigned idx)
{
    if (idx < tlen)
        memset(tgt+idx, 0, tlen-idx);
}

void rtlNullTerminate(unsigned tlen, char * tgt, unsigned idx)
{
    if (idx >= tlen)
        idx = tlen-1;
    tgt[idx] = 0;
}

void rtlUnicodeSpaceFill(unsigned tlen, UChar * tgt, unsigned idx)
{
    while(idx<tlen) tgt[idx++] = 0x0020;
}

void rtlUnicodeNullTerminate(unsigned tlen, UChar * tgt, unsigned idx)
{
    if (idx >= tlen)
        idx = tlen-1;
    tgt[idx] = 0x0000;
}

void rtlUnicodeStrcpy(UChar * tgt, UChar const * src)
{
    memcpy(tgt, src, rtlUnicodeStrlen(src)*2+2);
}


void rtlConcatExtend(unsigned & tlen, char * & tgt, unsigned slen, const char * src)
{
    unsigned len = tlen + slen;
    tgt = (char *)rtlRealloc(tgt, len);
    memcpy(tgt+tlen, src, slen);
    tlen = len;
}

void rtlConcatUnicodeExtend(size32_t & tlen, UChar * & tgt, size32_t slen, const UChar * src)
{
    unsigned len = tlen + slen;
    tgt = (UChar *)rtlRealloc(tgt, len * sizeof(UChar));
    memcpy(tgt+tlen, src, slen * sizeof(UChar));
    tlen = len;
}

//-----------------------------------------------------------------------------

inline void normalizeFrom(unsigned & from, unsigned slen)
{
    from--;
    if ((int)from < 0) 
        from = 0;
    else if (from > slen)
        from = slen;
}

inline void normalizeFromTo(unsigned & from, unsigned & to)
{
    from--;
    if ((int)from < 0) from = 0;
    if ((int)to < (int)from) to = from;
}

inline void clipFromTo(unsigned & from, unsigned & to, unsigned slen)
{
    if (to > slen)
    {
        to = slen;
        if (from > slen) 
            from = slen;
    }
}

//NB: From and to are 1 based:  Now fills to ensure the correct length.
void * doSubStrFT(unsigned & tlen, unsigned slen, const void * src, unsigned from, unsigned to, byte fillChar)
{
    normalizeFromTo(from, to);
    unsigned len = to - from;
    clipFromTo(from, to, slen);

    unsigned copylen = to - from;
    char * buffer = (char *)rtlMalloc(len);
    memcpy(buffer, (byte *)src+from, copylen);
    if (copylen < len)
        memset(buffer+copylen, fillChar, len-copylen);
    tlen = len;
    return buffer;
}

void rtlSubStrFX(unsigned & tlen, char * & tgt, unsigned slen, const char * src, unsigned from)
{
    normalizeFrom(from, slen);

    tlen = slen-from;
    tgt = (char *) rtlMalloc(tlen);
    memcpy(tgt, src+from, tlen);
}

void rtlSubStrFTX(unsigned & tlen, char * & tgt, unsigned slen, const char * src, unsigned from, unsigned to)
{
    tgt = (char *)doSubStrFT(tlen, slen, src, from, to, ' ');
}

void rtlSubStrFT(unsigned tlen, char * tgt, unsigned slen, const char * src, unsigned from, unsigned to)
{
    unsigned char fillChar = ' '; // More, should be passed as a parameter

    normalizeFromTo(from, to);
    clipFromTo(from, to, slen);

    unsigned copylen = to - from;
    if (copylen > tlen)
        copylen = tlen;
    memcpy(tgt, (const char *)src+from, copylen);
    if (copylen < tlen)
        memset(tgt+copylen, fillChar, tlen-copylen);
}

void rtlSubDataFT(unsigned tlen, void * tgt, unsigned slen, const void * src, unsigned from, unsigned to)
{
    normalizeFromTo(from, to);
    clipFromTo(from, to, slen);

    unsigned copylen = to - from;
    if (copylen > tlen)
        copylen = tlen;
    memcpy(tgt, (char *)src+from, copylen);
    if (copylen < tlen)
        memset((byte*)tgt+copylen, 0, tlen-copylen);
}

void rtlSubDataFTX(unsigned & tlen, void * & tgt, unsigned slen, const void * src, unsigned from, unsigned to)
{
    tgt = doSubStrFT(tlen, slen, src, from, to, 0);
}

void rtlSubDataFX(unsigned & tlen, void * & tgt, unsigned slen, const void * src, unsigned from)
{
    normalizeFrom(from, slen);

    tlen = slen-from;
    tgt = (char *) rtlMalloc(tlen);
    memcpy(tgt, (const byte *)src+from, tlen);
}

void rtlUnicodeSubStrFTX(unsigned & tlen, UChar * & tgt, unsigned slen, UChar const * src, unsigned from, unsigned to)
{
    normalizeFromTo(from, to);
    tlen = to - from;
    clipFromTo(from, to, slen);

    tgt = (UChar *)rtlMalloc(tlen*2);
    unsigned copylen = to - from;
    memcpy(tgt, src+from, copylen*2);
    while(copylen<tlen)
        tgt[copylen++] = 0x0020;
}

void rtlUnicodeSubStrFX(unsigned & tlen, UChar * & tgt, unsigned slen, UChar const * src, unsigned from)
{
    normalizeFrom(from, slen);

    tlen = slen - from;
    tgt = (UChar *)rtlMalloc(tlen*2);
    memcpy(tgt, src+from, tlen*2);
}


void rtlSubQStrFTX(unsigned & tlen, char * & tgt, unsigned slen, char const * src, unsigned from, unsigned to)
{
    normalizeFromTo(from, to);
    tlen = to - from;
    clipFromTo(from, to, slen);

    tgt = (char *)rtlMalloc(rtlQStrSize(tlen));
    copyQStrRange(tlen, tgt, src, from, to);
}

void rtlSubQStrFX(unsigned & tlen, char * & tgt, unsigned slen, char const * src, unsigned from)
{
    normalizeFrom(from, slen);

    tlen = slen - from;
    tgt = (char *)rtlMalloc(rtlQStrSize(tlen));

    copyQStrRange(tlen, tgt, src, from, slen);
}

void rtlSubQStrFT(unsigned tlen, char * tgt, unsigned slen, const char * src, unsigned from, unsigned to)
{
    normalizeFromTo(from, to);
    clipFromTo(from, to, slen);

    copyQStrRange(tlen, tgt, src, from ,to);
}

//-----------------------------------------------------------------------------

unsigned rtlTrimStrLen(size32_t l, const char * t)
{
    while (l)
    {
        if (t[l-1] != ' ')
            break;
        l--;
    }
    return l;
}

unsigned rtlTrimDataLen(size32_t l, const void * _t)
{
    const char * t = (const char *)_t;
    while (l)
    {
        if (t[l-1] != 0)
            break;
        l--;
    }
    return l;
}

unsigned rtlTrimUnicodeStrLen(size32_t l, UChar const * t)
{
    if (!l)
        return 0;
    UCharCharacterIterator iter(t, l);
    for(iter.last32(); iter.hasPrevious(); iter.previous32())
        if(!u_isspace(iter.current32()))
            break;
    if(u_isspace(iter.current32())) return iter.getIndex(); // required as the reverse iteration above doesn't hit the first character
    return iter.getIndex() + 1;
}

inline size32_t rtlQuickTrimUnicode(size32_t len, UChar const * str)
{
    while (len && u_isspace(str[len-1]))
        len--;
    return len;
}

unsigned rtlTrimVStrLen(const char * t)
{
    const char * first = t;
    const char * last = first;
    unsigned char c;
    while ((c = *t++) != 0)
    {
        if (c != ' ')
            last = t;       //nb after increment of t
    }
    return (last - first);
}

unsigned rtlTrimVUnicodeStrLen(UChar const * t)
{
    return rtlTrimUnicodeStrLen(rtlUnicodeStrlen(t), t);
}

inline unsigned rtlLeftTrimStrStart(size32_t slen, const char * src)
{
    unsigned i = 0;
    while(i < slen && src[i] == ' ')
        i++;
    return i;
}

inline unsigned rtlLeftTrimUnicodeStrStart(size32_t slen, UChar const * src)
{
    UCharCharacterIterator iter(src, slen);
    for(iter.first32(); iter.hasNext(); iter.next32())
        if(!u_isspace(iter.current32()))
            break;
    return iter.getIndex();
}

inline unsigned rtlLeftTrimVStrStart(const char * src)
{
    unsigned i = 0;
    while(src[i] == ' ')
        i++;
    return i;
}   

inline void rtlTrimUtf8Len(unsigned & trimLen, size32_t & trimSize, size32_t len, const char * t)
{
    const byte * start = (const byte *)t;
    const byte * cur = start;
    unsigned trimLength = 0;
    const byte * trimEnd = cur;
    for (unsigned i=0; i < len; i++)
    {
        unsigned next = readUtf8Character(UTF8_MAXSIZE, cur);
        if (!u_isspace(next))
        {
            trimLength = i+1;
            trimEnd = cur;
        }
    }
    trimLen = trimLength;
    trimSize = trimEnd-start;
}

inline void rtlTrimUtf8Start(unsigned & trimLen, size32_t & trimSize, size32_t len, const char * t)
{
    const byte * start = (const byte *)t;
    const byte * cur = start;
    for (unsigned i=0; i < len; i++)
    {
        const byte * prev = cur;
        unsigned next = readUtf8Character(UTF8_MAXSIZE, cur);
        if (!u_isspace(next))
        {
            trimLen = i;
            trimSize = prev-start;
            return;
        }
    }
    trimLen = len;
    trimSize = cur-start;
}

inline char * rtlDupSubString(const char * src, unsigned len)
{
    char * buffer = (char *)rtlMalloc(len + 1);
    memcpy(buffer, src, len);
    buffer[len] = 0;
    return buffer;
}

inline UChar * rtlDupSubUnicode(UChar const * src, unsigned len)
{
    UChar * buffer = (UChar *)rtlMalloc((len + 1) * 2);
    memcpy(buffer, src, len*2);
    buffer[len] = 0x00;
    return buffer;
}

inline void rtlCopySubStringV(size32_t tlen, char * tgt, unsigned slen, const char * src)
{
    if (slen >= tlen)
        slen = tlen-1;
    memcpy(tgt, src, slen);
    tgt[slen] = 0;
}

//not yet used, but would be needed for assignment to string rather than vstring
inline void rtlCopySubString(size32_t tlen, char * tgt, unsigned slen, const char * src, char fill)
{
    if (slen > tlen)
        slen = tlen;
    memcpy(tgt, src, slen);
    memset(tgt + slen, fill, tlen-slen);
}

unsigned rtlTrimUtf8StrLen(size32_t len, const char * t)
{
    const byte * cur = (const byte *)t;
    unsigned trimLength = 0;
    for (unsigned i=0; i < len; i++)
    {
        unsigned next = readUtf8Character(UTF8_MAXSIZE, cur);
        if (!u_isspace(next))
            trimLength = i+1;
    }
    return trimLength;
}

//-----------------------------------------------------------------------------
// Functions to trim off left side blank spaces

void rtlTrimRight(size32_t & tlen, char * & tgt, unsigned slen, const char * src)
{
    tlen = rtlTrimStrLen(slen, src);
    tgt = rtlDupSubString(src, tlen);
}

void rtlTrimUnicodeRight(size32_t & tlen, UChar * & tgt, unsigned slen, UChar const * src)
{
    tlen = rtlTrimUnicodeStrLen(slen, src);
    tgt = rtlDupSubUnicode(src, tlen);
}

void rtlTrimVRight(size32_t & tlen, char * & tgt, const char * src)
{
    tlen = rtlTrimVStrLen(src);
    tgt = rtlDupSubString(src, tlen);
}

void rtlTrimVUnicodeRight(size32_t & tlen, UChar * & tgt, UChar const * src)
{
    rtlTrimUnicodeRight(tlen, tgt, rtlUnicodeStrlen(src), src);
}

void rtlTrimUtf8Right(unsigned &tlen, char * &tgt, unsigned slen, char const * src)
{
    unsigned trimLength;
    size32_t trimSize;
    rtlTrimUtf8Len(trimLength, trimSize, slen, src);
    tlen = trimLength;
    tgt = rtlDupSubString(src, trimSize);
}

void rtlAssignTrimRightV(size32_t tlen, char * tgt, unsigned slen, const char * src)
{
    unsigned len = rtlTrimStrLen(slen, src);
    rtlCopySubStringV(tlen, tgt, len, src);
}

void rtlAssignTrimVRightV(size32_t tlen, char * tgt, const char * src)
{
    unsigned len = rtlTrimVStrLen(src);
    rtlCopySubStringV(tlen, tgt, len, src);
}


//-------------------------------------------------------------------------------
// Functions to trim off left side blank spaces
void rtlTrimLeft(unsigned & tlen, char * & tgt, unsigned slen, const char * src)
{
    unsigned start = rtlLeftTrimStrStart(slen, src);
    unsigned len = slen - start;

    tlen = len;
    tgt = rtlDupSubString(src + start, len);
}

void rtlTrimUnicodeLeft(unsigned & tlen, UChar * & tgt, unsigned slen, UChar const * src)
{
    unsigned start = rtlLeftTrimUnicodeStrStart(slen, src);
    unsigned len = slen - start;

    tlen = len;
    tgt = rtlDupSubUnicode(src + start, len);
}

void rtlTrimVLeft(unsigned & tlen, char * & tgt, const char * src)
{
    unsigned start = rtlLeftTrimVStrStart(src);
    unsigned len = strlen(src+start);
    
    tlen = len;
    tgt = rtlDupSubString(src + start, len);
}

void rtlTrimVUnicodeLeft(unsigned & tlen, UChar * & tgt, UChar const * src)
{
    rtlTrimUnicodeLeft(tlen, tgt, rtlUnicodeStrlen(src), src);
}

ECLRTL_API void rtlTrimUtf8Left(unsigned &tlen, char * &tgt, unsigned slen, const char * src)
{
    unsigned trimLength;
    size32_t trimSize;
    rtlTrimUtf8Start(trimLength, trimSize, slen, src);

    unsigned len = slen-trimLength;
    const char * start = src+trimSize;
    tlen = len;
    tgt = rtlDupSubString(start, rtlUtf8Size(len, start));
}


void rtlAssignTrimLeftV(size32_t tlen, char * tgt, unsigned slen, const char * src)
{
    unsigned start = rtlLeftTrimStrStart(slen, src);
    unsigned len = slen - start;
    rtlCopySubStringV(tlen, tgt, len, src+start);
}

void rtlAssignTrimVLeftV(size32_t tlen, char * tgt, const char * src)
{
    unsigned start = rtlLeftTrimVStrStart(src);
    unsigned len = strlen(src+start);
    rtlCopySubStringV(tlen, tgt, len, src+start);
}


//--------------------------------------------------------------------------------
// Functions to trim off blank spaces of both sides
void rtlTrimBoth(unsigned & tlen, char * & tgt, unsigned slen, const char * src)
{
    unsigned len = rtlTrimStrLen(slen, src);
    unsigned start = len ? rtlLeftTrimStrStart(slen, src) : 0;
    len -= start;
    
    tlen = len;
    tgt = rtlDupSubString(src + start, len);
}

void rtlTrimUnicodeBoth(unsigned & tlen, UChar * & tgt, unsigned slen, UChar const * src)
{
    unsigned len = rtlTrimUnicodeStrLen(slen, src);
    unsigned start = len ? rtlLeftTrimUnicodeStrStart(slen, src) : 0;
    len -= start;
    
    tlen = len;
    tgt = rtlDupSubUnicode(src + start, len);
}

void rtlTrimVBoth(unsigned & tlen, char * & tgt, const char * src)
{
    unsigned len = rtlTrimVStrLen(src);
    unsigned start = len ? rtlLeftTrimVStrStart(src) : 0;
    len -= start;

    tlen = len;
    tgt = rtlDupSubString(src + start, len);
}

void rtlTrimVUnicodeBoth(unsigned & tlen, UChar * & tgt, UChar const * src)
{
    rtlTrimUnicodeBoth(tlen, tgt, rtlUnicodeStrlen(src), src);
}

ECLRTL_API void rtlTrimUtf8Both(unsigned &tlen, char * &tgt, unsigned slen, const char * src)
{
    unsigned lTrimLength;
    size32_t lTrimSize;
    rtlTrimUtf8Start(lTrimLength, lTrimSize, slen, src);
    rtlTrimUtf8Right(tlen, tgt, slen-lTrimLength, src+lTrimSize);
}

void rtlAssignTrimBothV(size32_t tlen, char * tgt, unsigned slen, const char * src)
{
    unsigned len = rtlTrimStrLen(slen, src);
    unsigned start = len ? rtlLeftTrimStrStart(slen, src) : 0;
    len -= start;
    
    rtlCopySubStringV(tlen, tgt, len, src+start);
}

void rtlAssignTrimVBothV(size32_t tlen, char * tgt, const char * src)
{
    unsigned len = rtlTrimVStrLen(src);
    unsigned start = len ? rtlLeftTrimVStrStart(src) : 0;
    len -= start;

    rtlCopySubStringV(tlen, tgt, len, src+start);
}


//-----------------------------------------------------------------------------
// Functions used to trim off all blank spaces in a string.
unsigned rtlTrimStrLenNonBlank(size32_t l, const char * t)
{
    unsigned len = 0;

    while (l)
    {
        l--;
        if (t[l] != ' ')
            len++;
    }
    return len;
}

unsigned rtlTrimVStrLenNonBlank(const char * t)
{
    unsigned len = 0;

    unsigned char c;
    while ((c = *t++) != 0)
    {
        if (c != ' ')
            len++;
    }
    return len;
}

void rtlTrimAll(unsigned & tlen, char * & tgt, unsigned slen, const char * src)
{
    tlen = rtlTrimStrLenNonBlank(slen, src);
    char * buffer = (char *)rtlMalloc(tlen + 1);
    int ind = 0;
    for(unsigned i = 0; i < slen; i++) {
        if(src[i] != ' ') {
            buffer[ind] = src[i];
            ind++;
        }
    }
    buffer[tlen] = 0;
    tgt = buffer;
}

void rtlTrimUnicodeAll(unsigned & tlen, UChar * & tgt, unsigned slen, const UChar * src)
{
    UnicodeString rawStr;
    UCharCharacterIterator iter(src, slen);
    for(iter.first32(); iter.hasNext(); iter.next32())
        if(!u_isspace(iter.current32()))
            rawStr.append(iter.current32());
    UnicodeString tgtStr;
    normalizeUnicodeString(rawStr, tgtStr); // normalized in case crazy string like [combining accent] [space] [vowel]
    tlen = tgtStr.length();
    tgt = (UChar *)rtlMalloc((tlen+1)*2);
    tgtStr.extract(0, tlen, tgt);
    tgt[tlen] = 0x0000;
}

void rtlTrimVAll(unsigned & tlen, char * & tgt, const char * src)
{
    tlen = rtlTrimVStrLenNonBlank(src);
    char * buffer = (char *)rtlMalloc(tlen + 1);
    int ind = 0;
    int i = 0;
    while(src[i] != 0) {
        if(src[i] != ' ') {
            buffer[ind] = src[i];
            ind++;
        }
        i++;
    }
    buffer[tlen] = 0;
    tgt = buffer;
}

void rtlTrimVUnicodeAll(unsigned & tlen, UChar * & tgt, const UChar * src)
{
    rtlTrimUnicodeAll(tlen, tgt, rtlUnicodeStrlen(src), src);
}

ECLRTL_API void rtlTrimUtf8All(unsigned &tlen, char * &tgt, unsigned slen, const char * src)
{
    //Go via unicode because of possibility of combining accents etc.
    rtlDataAttr temp1(slen*sizeof(UChar));
    rtlUtf8ToUnicode(slen, temp1.getustr(), slen, src);

    unsigned trimLen;
    rtlDataAttr trimText;
    rtlTrimUnicodeAll(trimLen, trimText.refustr(), slen, temp1.getustr());
    rtlUnicodeToUtf8X(tlen, tgt, trimLen, trimText.getustr());
}

void rtlAssignTrimAllV(unsigned tlen, char * tgt, unsigned slen, const char * src)
{
    unsigned to = 0;
    for (unsigned from = 0; (from < slen)&&(to+1 < tlen); from++) 
    {
        if (src[from] != ' ') 
            tgt[to++] = src[from];
    }
    tgt[to] = 0;
}

void rtlAssignTrimVAllV(unsigned tlen, char * tgt, const char * src)
{
    unsigned to = 0;
    for (;(*src && (to+1 < tlen));src++) 
    {
        if (*src != ' ') 
            tgt[to++] = *src;
    }
    tgt[to] = 0;
}


//-----------------------------------------------------------------------------

ECLRTL_API void rtlUnicodeToVAscii(unsigned outlen, char * out, unsigned inlen, UChar const * in)
{
    rtlUnicodeToVCodepage(outlen, out, inlen, in, ASCII_LIKE_CODEPAGE);
}

ECLRTL_API void rtlData2VUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in)
{
    rtlCodepageToVUnicode(outlen, out, inlen, in, ASCII_LIKE_CODEPAGE);
}

ECLRTL_API void rtlStrToVUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in)
{
    rtlCodepageToVUnicode(outlen, out, inlen, in, ASCII_LIKE_CODEPAGE);
}

ECLRTL_API void rtlData2Unicode(unsigned outlen, UChar * out, unsigned inlen, void const * in)
{
    rtlCodepageToUnicode(outlen, out, inlen, (const char *)in, ASCII_LIKE_CODEPAGE);
}

ECLRTL_API void rtlAssignTrimUnicodeLeftV(size32_t tlen, UChar * tgt, unsigned slen, const UChar * src)
{
    unsigned len;
    UChar * str;
    rtlTrimUnicodeLeft(len, str, slen, src);
    if (len >= tlen)
        len = tlen-1;
    memcpy(tgt, str, len*2);
    tgt[len] = 0;
    rtlFree(str);
}


ECLRTL_API void rtlAssignTrimVUnicodeLeftV(size32_t tlen, UChar * tgt, const UChar * src)
{
    unsigned len;
    UChar * str;
    rtlTrimVUnicodeLeft(len, str, src);
    if (len >= tlen)
        len = tlen-1;
    memcpy(tgt, str, len*2);
    tgt[len] = 0;
    rtlFree(str);
}


ECLRTL_API void rtlAssignTrimUnicodeRightV(size32_t tlen, UChar * tgt, unsigned slen, const UChar * src)
{
    unsigned len;
    UChar * str;
    rtlTrimUnicodeRight(len, str, slen, src);
    if (len >= tlen)
        len = tlen-1;
    memcpy(tgt, str, len*2);
    tgt[len] = 0;
    rtlFree(str);
}


ECLRTL_API void rtlAssignTrimVUnicodeRightV(size32_t tlen, UChar * tgt, const UChar * src)
{
    unsigned len;
    UChar * str;
    rtlTrimVUnicodeRight(len, str, src);
    if (len >= tlen)
        len = tlen-1;
    memcpy(tgt, str, len*2);
    tgt[len] = 0;
    rtlFree(str);
}


ECLRTL_API void rtlAssignTrimUnicodeBothV(size32_t tlen, UChar * tgt, unsigned slen, const UChar * src)
{
    unsigned len;
    UChar * str;
    rtlTrimUnicodeBoth(len, str, slen, src);
    if (len >= tlen)
        len = tlen-1;
    memcpy(tgt, str, len*2);
    tgt[len] = 0;
    rtlFree(str);
}


ECLRTL_API void rtlAssignTrimVUnicodeBothV(size32_t tlen, UChar * tgt, const UChar * src)
{
    unsigned len;
    UChar * str;
    rtlTrimVUnicodeBoth(len, str, src);
    if (len >= tlen)
        len = tlen-1;
    memcpy(tgt, str, len*2);
    tgt[len] = 0;
    rtlFree(str);
}


ECLRTL_API void rtlAssignTrimUnicodeAllV(size32_t tlen, UChar * tgt, unsigned slen, const UChar * src)
{
    unsigned len;
    UChar * str;
    rtlTrimUnicodeAll(len, str, slen, src);
    if (len >= tlen)
        len = tlen-1;
    memcpy(tgt, str, len*2);
    tgt[len] = 0;
    rtlFree(str);
}


ECLRTL_API void rtlAssignTrimVUnicodeAllV(size32_t tlen, UChar * tgt, const UChar * src)
{
    unsigned len;
    UChar * str;
    rtlTrimVUnicodeAll(len, str, src);
    if (len >= tlen)
        len = tlen-1;
    memcpy(tgt, str, len*2);
    tgt[len] = 0;
    rtlFree(str);
}


//-----------------------------------------------------------------------------

int rtlCompareStrStr(unsigned l1, const char * p1, unsigned l2, const char * p2)
{
    unsigned len = l1;
    if (len > l2)
        len = l2;
    int diff = memcmp(p1, p2, len);
    if (diff == 0)
    {
        if (len != l1)
        {
            for (;(diff == 0) && (len != l1);len++)
                diff = ((unsigned char *)p1)[len] - ' ';
        }
        else if (len != l2)
        {
            for (;(diff == 0) && (len != l2);len++)
                diff = ' ' - ((unsigned char *)p2)[len];
        }
    }
    return diff;
}

int rtlCompareVStrVStr(const char * p1, const char * p2)
{
    return rtlCompareStrStr(strlen(p1), p1, strlen(p2), p2);
}

int rtlCompareStrBlank(unsigned l1, const char * p1)
{
    while (l1--)
    {
        int diff = (*(unsigned char *)(p1++)) - ' ';
        if (diff)
            return diff;
    }
    return 0;
}

int rtlCompareDataData(unsigned l1, const void * p1, unsigned l2, const void * p2)
{
    unsigned len = l1;
    if (len > l2)
        len = l2;
    int diff = memcmp(p1, p2, len);
    if (diff == 0)
    {
        if (l1 > l2)
            diff = +1;
        else if (l1 < l2)
            diff = -1;
    }
    return diff;
}

int rtlCompareEStrEStr(unsigned l1, const char * p1, unsigned l2, const char * p2)
{
    unsigned len = l1;
    if (len > l2)
        len = l2;
    int diff = memcmp(p1, p2, len);
    if (diff == 0)
    {
        if (len != l1)
        {
            for (;(diff == 0) && (len != l1);len++)
                diff = ((unsigned char *)p1)[len] - '@';
        }
        else if (len != l2)
        {
            for (;(diff == 0) && (len != l2);len++)
                diff = '@' - ((unsigned char *)p2)[len];
        }
    }
    return diff;
}

const static UChar nullUStr = 0;
int rtlCompareUnicodeUnicode(unsigned l1, UChar const * p1, unsigned l2, UChar const * p2, char const * locale)
{
    while(l1 && u_isUWhiteSpace(p1[l1-1])) l1--;
    while(l2 && u_isUWhiteSpace(p2[l2-1])) l2--;
    if (!p1) p1 = &nullUStr;
    if (!p2) p2 = &nullUStr;
    return ucol_strcoll(queryRTLLocale(locale)->queryCollator(), p1, l1, p2, l2);
}

int rtlCompareUnicodeUnicodeStrength(unsigned l1, UChar const * p1, unsigned l2, UChar const * p2, char const * locale, unsigned strength)
{
    while(l1 && u_isUWhiteSpace(p1[l1-1])) l1--;
    while(l2 && u_isUWhiteSpace(p2[l2-1])) l2--;
    if (!p1) p1 = &nullUStr;
    if (!p2) p2 = &nullUStr;
    return ucol_strcoll(queryRTLLocale(locale)->queryCollator(strength), p1, l1, p2, l2);
}

int rtlCompareVUnicodeVUnicode(UChar const * p1, UChar const * p2, char const * locale)
{
    return rtlCompareUnicodeUnicode(rtlUnicodeStrlen(p1), p1, rtlUnicodeStrlen(p2), p2, locale);
}

int rtlCompareVUnicodeVUnicodeStrength(UChar const * p1, UChar const * p2, char const * locale, unsigned strength)
{
    return rtlCompareUnicodeUnicodeStrength(rtlUnicodeStrlen(p1), p1, rtlUnicodeStrlen(p2), p2, locale, strength);
}

void rtlKeyUnicodeX(unsigned & tlen, void * & tgt, unsigned slen, const UChar * src, const char * locale)
{
    while(slen && u_isUWhiteSpace(src[slen-1])) slen--;
    UCollator * coll = queryRTLLocale(locale)->queryCollator();
    tlen = ucol_getSortKey(coll, src, slen, 0, 0);
    tgt = rtlMalloc(tlen);
    ucol_getSortKey(coll, src, slen, (unsigned char *)tgt, tlen);
}

void rtlKeyUnicodeStrengthX(unsigned & tlen, void * & tgt, unsigned slen, const UChar * src, const char * locale, unsigned strength)
{
    while(slen && u_isUWhiteSpace(src[slen-1])) slen--;
    UCollator * coll = queryRTLLocale(locale)->queryCollator(strength);
    tlen = ucol_getSortKey(coll, src, slen, 0, 0);
    tgt = rtlMalloc(tlen);
    ucol_getSortKey(coll, src, slen, (unsigned char *)tgt, tlen);
}

ECLRTL_API int rtlPrefixDiffStrEx(unsigned l1, const char * p1, unsigned l2, const char * p2, unsigned origin)
{
    unsigned len = l1 < l2 ? l1 : l2;
    const byte * str1 = (const byte *)p1;
    const byte * str2 = (const byte *)p2;
    for (unsigned i=0; i<len; i++)
    {
        byte c1 = str1[i];
        byte c2 = str2[i];
        if (c1 != c2)
        {
            if (c1 < c2)
                return -(int)(i+origin+1);
            else
                return (int)(i+origin+1);
        }
    }
    if (l1 != l2)
        return (l1 < l2) ? -(int)(len+origin+1) : (int)(len+origin+1);
    return 0;
}

ECLRTL_API int rtlPrefixDiffStr(unsigned l1, const char * p1, unsigned l2, const char * p2)
{
    return rtlPrefixDiffStrEx(l1, p1, l2, p2, 0);
}

//MORE: I'm not sure this can really be implemented....
ECLRTL_API int rtlPrefixDiffUnicodeEx(unsigned l1, const UChar * p1, unsigned l2, const UChar * p2, char const * locale, unsigned origin)
{
    while(l1 && u_isUWhiteSpace(p1[l1-1])) l1--;
    while(l2 && u_isUWhiteSpace(p2[l2-1])) l2--;
    unsigned len = l1 < l2 ? l1 : l2;
    for (unsigned i=0; i<len; i++)
    {
        if (p1[i] != p2[i])
        {
            int c = ucol_strcoll(queryRTLLocale(locale)->queryCollator(), p1+i, l1-i, p2+i, l2-i);
            if (c < 0)
                return -(int)(i+origin+1);
            else if (c > 0)
                return (int)(i+origin+1);
        }
    }
    if (l1 != l2)
        return (l1 < l2) ? -(int)(len+origin+1) : (int)(len+origin+1);
    return 0;
}

ECLRTL_API int rtlPrefixDiffUnicode(unsigned l1, const UChar * p1, unsigned l2, const UChar * p2, char const * locale)
{
    return rtlPrefixDiffUnicodeEx(l1, p1, l2, p2, locale, 0);
}

//-----------------------------------------------------------------------------

void rtlStringToLower(size32_t l, char * t)
{
    for (;l--;t++)
        *t = tolower(*t);
}

void rtlStringToUpper(size32_t l, char * t)
{
    for (;l--;t++)
        *t = toupper(*t);
}

void rtlUnicodeToLower(size32_t l, UChar * t, char const * locale)
{
    UChar * buff = (UChar *)rtlMalloc(l*2);
    UErrorCode err = U_ZERO_ERROR;
    u_strToLower(buff, l, t, l, locale, &err);
    unicodeNormalizedCopy(buff, t, l);
}

void rtlUnicodeToLowerX(size32_t & lenout, UChar * & out, size32_t l, const UChar * t, char const * locale)
{
    out = (UChar *)rtlMalloc(l*2);
    lenout = l;
    UErrorCode err = U_ZERO_ERROR;
    u_strToLower(out, l, t, l, locale, &err);
}

void rtlUnicodeToUpper(size32_t l, UChar * t, char const * locale)
{
    UChar * buff = (UChar *)rtlMalloc(l*2);
    UErrorCode err = U_ZERO_ERROR;
    u_strToUpper(buff, l, t, l, locale, &err);
    unicodeNormalizedCopy(buff, t, l);
}

//=============================================================================
// Miscellaneous helper functions...

//-----------------------------------------------------------------------------

int searchTableStringN(unsigned count, const char * * table, unsigned width, const char * search)
{
    int left = 0;
    int right = count;
    do
    {
        int mid = (left + right) >> 1;
        int cmp = memcmp(search, table[mid], width);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else
            return mid;
    } while (left < right);
    return -1;
}

int rtlSearchTableStringN(unsigned count, char * * table, unsigned width, const char * search)
{
    int left = 0;
    int right = count;
    do
    {
        int mid = (left + right) >> 1;
        //we could use rtlCompareStrStr, but both source and target strings should
        //be the correct length, so no point.... (unless new weird collation sequences)
        //we would also need to call a different function for data
        int cmp = memcmp(search, table[mid], width);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else
            return mid;
    } while (left < right);
    return -1;
}


int rtlSearchTableVStringN(unsigned count, char * * table, const char * search)
{
    int left = 0;
    int right = count;
    do
    {
        int mid = (left + right) >> 1;
        int cmp = strcmp(search, table[mid]);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else 
            return mid;
    } while (left < right);
    return -1;
}

int rtlNewSearchDataTable(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search)
{
    int left = 0;
    int right = count;

    do
    {
        int mid = (left + right) >> 1;
        int cmp = rtlCompareDataData( width, search, elemlen, table[mid]);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else {
            return mid;
        }
                
    } while (left < right);

    return -1;
}

int rtlNewSearchEStringTable(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search)
{
    int left = 0;
    int right = count;

    do
    {
        int mid = (left + right) >> 1;
        int cmp = rtlCompareEStrEStr( width, search, elemlen, table[mid]);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else {
            return mid;
        }
                
    } while (left < right);

    return -1;
}

int rtlNewSearchQStringTable(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search)
{
    int left = 0;
    int right = count;

    do
    {
        int mid = (left + right) >> 1;
        int cmp = rtlCompareQStrQStr( width, search, elemlen, table[mid]);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else {
            return mid;
        }
                
    } while (left < right);

    return -1;
}

int rtlNewSearchStringTable(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search)
{
    int left = 0;
    int right = count;

    do
    {
        int mid = (left + right) >> 1;
        int cmp = rtlCompareStrStr( width, search, elemlen, table[mid]);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else {
            return mid;
        }
                
    } while (left < right);

    return -1;
}

int rtlNewSearchUnicodeTable(unsigned count, unsigned elemlen, UChar * * table, unsigned width, const UChar * search, const char * locale)
{
    UCollator * coll = queryRTLLocale(locale)->queryCollator();
    int left = 0;
    int right = count;
    
    if (!search) search = &nullUStr;
    size32_t trimWidth = rtlQuickTrimUnicode(width, search);

    do
    {
        int mid = (left + right) >> 1;
        size32_t elemTrimWidth = rtlQuickTrimUnicode(elemlen, table[mid]);
        UCollationResult cmp = ucol_strcoll(coll, search, trimWidth, table[mid], elemTrimWidth);
        if (cmp == UCOL_LESS)
            right = mid;
        else if (cmp == UCOL_GREATER)
            left = mid+1;
        else
            return mid;
    } while (left < right);

    return -1;
}

int rtlNewSearchVUnicodeTable(unsigned count, UChar * * table, const UChar * search, const char * locale)
{
    UCollator * coll = queryRTLLocale(locale)->queryCollator();
    int left = 0;
    int right = count;

    do
    {
        int mid = (left + right) >> 1;
        UCollationResult cmp = ucol_strcoll(coll, search, rtlUnicodeStrlen(search), table[mid], rtlUnicodeStrlen(table[mid]));
        if (cmp == UCOL_LESS)
            right = mid;
        else if (cmp == UCOL_GREATER)
            left = mid+1;
        else
            return mid;
    } while (left < right);

    return -1;
}

//-----------------------------------------------------------------------------

template <class T>
int rtlSearchIntegerTable(unsigned count, T * table, T search)
{
    int left = 0;
    int right = count;

    do
    {
        int mid = (left + right) >> 1;
        T midValue = table[mid];
        if (search < midValue)
            right = mid;
        else if (search > midValue)
            left = mid+1;
        else
            return mid;
    } while (left < right);

    return -1;
}


int rtlSearchTableInteger8(unsigned count, __int64 * table, __int64 search)
{
    return rtlSearchIntegerTable(count, table, search);
}

int rtlSearchTableUInteger8(unsigned count, unsigned __int64 * table, unsigned __int64 search)
{
    return rtlSearchIntegerTable(count, table, search);
}

int rtlSearchTableInteger4(unsigned count, int * table, int search)
{
    return rtlSearchIntegerTable(count, table, search);
}

int rtlSearchTableUInteger4(unsigned count, unsigned  * table, unsigned search)
{
    return rtlSearchIntegerTable(count, table, search);
}

//-----------------------------------------------------------------------------

unsigned rtlCrc32(unsigned len, const void * buffer, unsigned crc)
{
        return crc32((const char *)buffer, len, crc);
}

//=============================================================================
// EBCDIC helper functions...

static char ccsid819[] = "\
\000\001\002\003\234\011\206\177\227\215\216\013\014\015\016\017\
\020\021\022\023\235\205\010\207\030\031\222\217\034\035\036\037\
\200\201\202\203\204\012\027\033\210\211\212\213\214\005\006\007\
\220\221\026\223\224\225\226\004\230\231\232\233\024\025\236\032\
\040\240\342\344\340\341\343\345\347\361\242\056\074\050\053\174\
\046\351\352\353\350\355\356\357\354\337\041\044\052\051\073\254\
\055\057\302\304\300\301\303\305\307\321\246\054\045\137\076\077\
\370\311\312\313\310\315\316\317\314\140\072\043\100\047\075\042\
\330\141\142\143\144\145\146\147\150\151\253\273\360\375\376\261\
\260\152\153\154\155\156\157\160\161\162\252\272\346\270\306\244\
\265\176\163\164\165\166\167\170\171\172\241\277\320\335\336\256\
\136\243\245\267\251\247\266\274\275\276\133\135\257\250\264\327\
\173\101\102\103\104\105\106\107\110\111\255\364\366\362\363\365\
\175\112\113\114\115\116\117\120\121\122\271\373\374\371\372\377\
\134\367\123\124\125\126\127\130\131\132\262\324\326\322\323\325\
\060\061\062\063\064\065\066\067\070\071\263\333\334\331\332\237";

static unsigned char ccsid1047[] = "\
\000\001\002\003\234\011\206\177\227\215\216\013\014\015\016\017\
\020\021\022\023\235\012\010\207\030\031\222\217\034\035\036\037\
\200\201\202\203\204\205\027\033\210\211\212\213\214\005\006\007\
\220\221\026\223\224\225\226\004\230\231\232\233\024\025\236\032\
\040\240\342\344\340\341\343\345\347\361\242\056\074\050\053\174\
\046\351\352\353\350\355\356\357\354\337\041\044\052\051\073\136\
\055\057\302\304\300\301\303\305\307\321\246\054\045\137\076\077\
\370\311\312\313\310\315\316\317\314\140\072\043\100\047\075\042\
\330\141\142\143\144\145\146\147\150\151\253\273\360\375\376\261\
\260\152\153\154\155\156\157\160\161\162\252\272\346\270\306\244\
\265\176\163\164\165\166\167\170\171\172\241\277\320\133\336\256\
\254\243\245\267\251\247\266\274\275\276\335\250\257\135\264\327\
\173\101\102\103\104\105\106\107\110\111\255\364\366\362\363\365\
\175\112\113\114\115\116\117\120\121\122\271\373\374\371\372\377\
\134\367\123\124\125\126\127\130\131\132\262\324\326\322\323\325\
\060\061\062\063\064\065\066\067\070\071\263\333\334\331\332\237";

static unsigned char ccsid1047_rev[] = "\
\000\001\002\003\067\055\056\057\026\005\025\013\014\015\016\017\
\020\021\022\023\074\075\062\046\030\031\077\047\034\035\036\037\
\100\132\177\173\133\154\120\175\115\135\134\116\153\140\113\141\
\360\361\362\363\364\365\366\367\370\371\172\136\114\176\156\157\
\174\301\302\303\304\305\306\307\310\311\321\322\323\324\325\326\
\327\330\331\342\343\344\345\346\347\350\351\255\340\275\137\155\
\171\201\202\203\204\205\206\207\210\211\221\222\223\224\225\226\
\227\230\231\242\243\244\245\246\247\250\251\300\117\320\241\007\
\040\041\042\043\044\045\006\027\050\051\052\053\054\011\012\033\
\060\061\032\063\064\065\066\010\070\071\072\073\004\024\076\377\
\101\252\112\261\237\262\152\265\273\264\232\212\260\312\257\274\
\220\217\352\372\276\240\266\263\235\332\233\213\267\270\271\253\
\144\145\142\146\143\147\236\150\164\161\162\163\170\165\166\167\
\254\151\355\356\353\357\354\277\200\375\376\373\374\272\256\131\
\104\105\102\106\103\107\234\110\124\121\122\123\130\125\126\127\
\214\111\315\316\313\317\314\341\160\335\336\333\334\215\216\337";

void rtlEStrToStr(unsigned outlen, char *out, unsigned inlen, const char *in)
{
    unsigned char *codepage = ccsid1047;
    unsigned i,j;
    unsigned lim = inlen;
    if (lim>outlen) lim = outlen;
    for (i=0;i<lim;i++)
    {
        j = in[i] & 0x00ff;
        out[i] = codepage[j];
    }
    for (;i<outlen; i++)
        out[i] = ' ';
}

void rtlStrToEStr(unsigned outlen, char *out, unsigned inlen, const char *in)
{
    unsigned char *codepage = ccsid1047_rev;
    unsigned i,j;
    unsigned lim = inlen;
    if (lim>outlen) lim = outlen;
    for (i=0;i<lim;i++)
    {
        j = in[i] & 0x00ff;
        out[i] = codepage[j];
    }
    for (;i<outlen; i++)
        out[i] = codepage[' '];
}

//---------------------------------------------------------------------------

void rtlCodepageToUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in, char const * codepage)
{
    //If the input contains a character which doesn't exist in its claimed codepage, this will
    //generate U+FFFD (substitution character). This most likely won't be displayed.
    UConverter * conv = queryRTLUnicodeConverter(codepage)->query();
    UErrorCode err = U_ZERO_ERROR;
    unsigned len = ucnv_toUChars(conv, out, outlen, in, inlen, &err);
    while(len<outlen) out[len++] = 0x0020;
    unicodeEnsureIsNormalized(outlen, out);
}

void rtlCodepageToVUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in, char const * codepage)
{
    //If the input contains a character which doesn't exist in its claimed codepage, this will
    //generate U+FFFD (substitution character). This most likely won't be displayed.
    UConverter * conv = queryRTLUnicodeConverter(codepage)->query();
    UErrorCode err = U_ZERO_ERROR;
    unsigned len = ucnv_toUChars(conv, out, outlen-1, in, inlen, &err);
    if (len >= outlen) len = outlen-1;
    out[len] = 0;
    vunicodeEnsureIsNormalized(outlen, out);
}

void rtlVCodepageToUnicode(unsigned outlen, UChar * out, char const * in, char const * codepage)
{
    rtlCodepageToUnicode(outlen, out, strlen(in), in, codepage);
}

void rtlVCodepageToVUnicode(unsigned outlen, UChar * out, char const * in, char const * codepage)
{
    rtlCodepageToVUnicode(outlen, out, strlen(in), in, codepage);
}

void rtlCodepageToUnicodeUnescape(unsigned outlen, UChar * out, unsigned inlen, char const * in, char const * codepage)
{
    //If the input contains a character which doesn't exist in its claimed codepage, this will
    //generate U+FFFD (substitution character). This most likely won't be displayed.
    UnicodeString raw(in, inlen, codepage);
    UnicodeString unescaped = raw.unescape();
    UnicodeString normalized;
    normalizeUnicodeString(unescaped, normalized);
    if((unsigned)normalized.length()>outlen)
        normalized.truncate(outlen);
    else if((unsigned)normalized.length()<outlen)
        normalized.padTrailing(outlen);
    normalized.extract(0, outlen, out);
}

void rtlUnicodeToCodepage(unsigned outlen, char * out, unsigned inlen, UChar const * in, char const * codepage)
{
    //If the unicode contains a character which doesn't exist in the destination codepage,
    //this will generate the SUBstitute control code (ASCII: 0x1A, EBCDIC-US: 0x3F). There's
    //no telling how your terminal may display this (I've seen a divide sign and a right
    //arrow, amongst others). Perhaps we should ensure our display tools handle it neatly.
    UConverter * conv = queryRTLUnicodeConverter(codepage)->query();
    UErrorCode err = U_ZERO_ERROR;
    unsigned len = ucnv_fromUChars(conv, (char *)out, outlen, in, inlen, &err);
    if(len<outlen)
        codepageBlankFill(codepage, out+len, outlen-len);
}

void rtlUnicodeToData(unsigned outlen, void * out, unsigned inlen, UChar const * in)
{
    //If the unicode contains a character which doesn't exist in the destination codepage,
    //this will generate the SUBstitute control code (ASCII: 0x1A, EBCDIC-US: 0x3F). There's
    //no telling how your terminal may display this (I've seen a divide sign and a right
    //arrow, amongst others). Perhaps we should ensure our display tools handle it neatly.
    UConverter * conv = queryRTLUnicodeConverter(ASCII_LIKE_CODEPAGE)->query();
    UErrorCode err = U_ZERO_ERROR;
    unsigned len = ucnv_fromUChars(conv, (char *)out, outlen, in, inlen, &err);
    if(len<outlen)
        memset((char *)out+len, 0, outlen-len);
}

void rtlUnicodeToVCodepage(unsigned outlen, char * out, unsigned inlen, UChar const * in, char const * codepage)
{
    //If the unicode contains a character which doesn't exist in the destination codepage,
    //this will generate the SUBstitute control code (ASCII: 0x1A, EBCDIC-US: 0x3F). There's
    //no telling how your terminal may display this (I've seen a divide sign and a right
    //arrow, amongst others). Perhaps we should ensure our display tools handle it neatly.
    UConverter * conv = queryRTLUnicodeConverter(ASCII_LIKE_CODEPAGE)->query();
    UErrorCode err = U_ZERO_ERROR;
    unsigned len = ucnv_fromUChars(conv, (char *)out, outlen-1, in, inlen, &err);
    if (len >= outlen) len = outlen-1;
    out[len] = 0;
}

void rtlVUnicodeToCodepage(unsigned outlen, char * out, UChar const * in, char const * codepage)
{
    rtlUnicodeToCodepage(outlen, out, rtlUnicodeStrlen(in), in, codepage);
}

void rtlVUnicodeToData(unsigned outlen, void * out, UChar const * in)
{
    rtlUnicodeToData(outlen, out, rtlUnicodeStrlen(in), in);
}

void rtlVUnicodeToVCodepage(unsigned outlen, char * out, UChar const * in, char const * codepage)
{
    rtlUnicodeToVCodepage(outlen, out, rtlUnicodeStrlen(in), in, codepage);
}

void rtlCodepageToUnicodeX(unsigned & outlen, UChar * & out, unsigned inlen, char const * in, char const * codepage)
{
    //If the input contains a character which doesn't exist in its claimed codepage, this will
    //generate U+FFFD (substitution character). This most likely won't be displayed.
    UConverter * conv = queryRTLUnicodeConverter(codepage)->query();
    UErrorCode err = U_ZERO_ERROR;
    outlen = ucnv_toUChars(conv, 0, 0, in, inlen, &err);
    if(err==U_BUFFER_OVERFLOW_ERROR) err = U_ZERO_ERROR;
    out = (UChar *)rtlMalloc(outlen*2);
    ucnv_toUChars(conv, out, outlen, in, inlen, &err);
}

UChar * rtlCodepageToVUnicodeX(unsigned inlen, char const * in, char const * codepage)
{
    //If the input contains a character which doesn't exist in its claimed codepage, this will
    //generate U+FFFD (substitution character). This most likely won't be displayed.
    UConverter * conv = queryRTLUnicodeConverter(codepage)->query();
    UErrorCode err = U_ZERO_ERROR;
    unsigned outlen = ucnv_toUChars(conv, 0, 0, in, inlen, &err);
    if(err == U_BUFFER_OVERFLOW_ERROR) err = U_ZERO_ERROR;
    UChar * out = (UChar *)rtlMalloc((outlen+1)*2);
    ucnv_toUChars(conv,  out, outlen, in, inlen, &err);
    out[outlen] = 0x0000;
    vunicodeEnsureIsNormalizedX(outlen, out);
    return out;
}

void rtlVCodepageToUnicodeX(unsigned & outlen, UChar * & out, char const * in, char const * codepage)
{
    rtlCodepageToUnicodeX(outlen, out, strlen(in), in, codepage);
}

UChar * rtlVCodepageToVUnicodeX(char const * in, char const * codepage)
{
    return rtlCodepageToVUnicodeX(strlen(in), in, codepage);
}

void rtlCodepageToUnicodeXUnescape(unsigned & outlen, UChar * & out, unsigned inlen, char const * in, char const * codepage)
{
    //If the input contains a character which doesn't exist in its claimed codepage, this will
    //generate U+FFFD (substitution character). This most likely won't be displayed.
    UnicodeString raw(in, inlen, codepage);
    UnicodeString unescaped = raw.unescape();
    UnicodeString normalized;
    normalizeUnicodeString(unescaped, normalized);
    outlen = normalized.length();
    out = (UChar *)rtlMalloc(outlen*2);
    normalized.extract(0, outlen, out);
}

void rtlCodepageToUtf8XUnescape(unsigned & outlen, char * & out, unsigned inlen, char const * in, char const * codepage)
{
    //If the input contains a character which doesn't exist in its claimed codepage, this will
    //generate U+FFFD (substitution character). This most likely won't be displayed.
    UnicodeString raw(in, inlen, codepage);
    UnicodeString unescaped = raw.unescape();
    UnicodeString normalized;
    normalizeUnicodeString(unescaped, normalized);

    UConverter * utf8Conv = queryRTLUnicodeConverter(UTF8_CODEPAGE)->query();
    UErrorCode err = U_ZERO_ERROR;
    size32_t outsize = normalized.extract(NULL, 0, utf8Conv, err);
    err = U_ZERO_ERROR;
    out = (char *)rtlMalloc(outsize);
    outsize = normalized.extract(out, outsize, utf8Conv, err);
    outlen = rtlUtf8Length(outsize, out);
}

void rtlUnicodeToCodepageX(unsigned & outlen, char * & out, unsigned inlen, UChar const * in, char const * codepage)
{
    //If the unicode contains a character which doesn't exist in the destination codepage,
    //this will generate the SUBstitute control code (ASCII: 0x1A, EBCDIC-US: 0x3F). There's
    //no telling how your terminal may display this (I've seen a divide sign and a right
    //arrow, amongst others). Perhaps we should ensure our display tools handle it neatly.
    UConverter * conv = queryRTLUnicodeConverter(codepage)->query();
    UErrorCode err = U_ZERO_ERROR;
    outlen = ucnv_fromUChars(conv, 0, 0, in, inlen, &err);
    if(err == U_BUFFER_OVERFLOW_ERROR) err = U_ZERO_ERROR;
    out = (char *)rtlMalloc(outlen);
    ucnv_fromUChars(conv, out, outlen, in, inlen, &err);
}

void rtlUnicodeToDataX(unsigned & outlen, void * & out, unsigned inlen, UChar const * in)
{
    rtlUnicodeToCodepageX(outlen, (char * &)out, inlen, in, ASCII_LIKE_CODEPAGE);
}

char * rtlUnicodeToVCodepageX(unsigned inlen, UChar const * in, char const * codepage)
{
    //If the unicode contains a character which doesn't exist in the destination codepage,
    //this will generate the SUBstitute control code (ASCII: 0x1A, EBCDIC-US: 0x3F). There's
    //no telling how your terminal may display this (I've seen a divide sign and a right
    //arrow, amongst others). Perhaps we should ensure our display tools handle it neatly.
    UConverter * conv = queryRTLUnicodeConverter(codepage)->query();
    UErrorCode err = U_ZERO_ERROR;
    unsigned outlen = ucnv_fromUChars(conv, 0, 0, in, inlen, &err);
    if(err == U_BUFFER_OVERFLOW_ERROR) err = U_ZERO_ERROR;
    char * out = (char *)rtlMalloc(outlen+1);
    ucnv_fromUChars(conv, out, outlen, in, inlen, &err);
    out[outlen] = 0x00;
    return out;
}

void rtlVUnicodeToCodepageX(unsigned & outlen, char * & out, UChar const * in, char const * codepage)
{
    rtlUnicodeToCodepageX(outlen, out, rtlUnicodeStrlen(in), in, codepage);
}

char * rtlVUnicodeToVCodepageX(UChar const * in, char const * codepage)
{
    return rtlUnicodeToVCodepageX(rtlUnicodeStrlen(in), in, codepage);
}

void rtlStrToUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in)
{
    rtlCodepageToUnicode(outlen, out, inlen, in, ASCII_LIKE_CODEPAGE);
}

void rtlUnicodeToStr(unsigned outlen, char * out, unsigned inlen, UChar const * in)
{
    rtlUnicodeToCodepage(outlen, out, inlen, in, ASCII_LIKE_CODEPAGE);
}

void rtlStrToUnicodeX(unsigned & outlen, UChar * & out, unsigned inlen, char const * in)
{
    rtlCodepageToUnicodeX(outlen, out, inlen, in, ASCII_LIKE_CODEPAGE);
}

void rtlUnicodeToStrX(unsigned & outlen, char * & out, unsigned inlen, UChar const * in)
{
    rtlUnicodeToCodepageX(outlen, out, inlen, in, ASCII_LIKE_CODEPAGE);
}

void rtlUnicodeToEscapedStrX(unsigned & outlen, char * & out, unsigned inlen, UChar const * in)
{
    StringBuffer outbuff;
    escapeUnicode(inlen, in, outbuff);
    outlen = outbuff.length();
    out = (char *)rtlMalloc(outlen);
    memcpy(out, outbuff.str(), outlen);
}

bool rtlCodepageToCodepage(unsigned outlen, char * out, unsigned inlen, char const * in, char const * outcodepage, char const * incodepage)
{
    UConverter * inconv = queryRTLUnicodeConverter(incodepage)->query();
    UConverter * outconv = queryRTLUnicodeConverter(outcodepage)->query();
    UErrorCode err = U_ZERO_ERROR;
    char * target = out;
    ucnv_convertEx(outconv, inconv, &target, out+outlen, &in, in+inlen, NULL, NULL, NULL, NULL, TRUE, TRUE, &err);
    unsigned len = target - out;
    if(len < outlen)
        codepageBlankFill(outcodepage, target, outlen-len);
    return U_SUCCESS(err);
}

bool rtlCodepageToCodepageX(unsigned & outlen, char * & out, unsigned maxoutlen, unsigned inlen, char const * in, char const * outcodepage, char const * incodepage)
{
    UConverter * inconv = queryRTLUnicodeConverter(incodepage)->query();
    UConverter * outconv = queryRTLUnicodeConverter(outcodepage)->query();
    UErrorCode err = U_ZERO_ERROR;

    //GH->PG is there a better way of coding this with out temporary buffer?
    char * tempBuffer = (char *)rtlMalloc(maxoutlen);
    char * target = tempBuffer;
    ucnv_convertEx(outconv, inconv, &target, tempBuffer+maxoutlen, &in, in+inlen, NULL, NULL, NULL, NULL, TRUE, TRUE, &err);
    unsigned len = target - tempBuffer;
    outlen = len;
    if (len == maxoutlen)
        out = tempBuffer;
    else
    {
        out = (char *)rtlRealloc(tempBuffer, len);
        if (!out)
            out = tempBuffer;
    }
    return U_SUCCESS(err);
}

int rtlSingleUtf8ToCodepage(char * out, unsigned inlen, char const * in, char const * outcodepage)
{
    const byte head = *in; // Macros require unsigned argument on some versions of ICU
    if(!U8_IS_LEAD(head))
        return -1;
    uint8_t trailbytes = U8_COUNT_TRAIL_BYTES(head);
    if(inlen < (unsigned)(trailbytes+1))
        return -1;
    if(!rtlCodepageToCodepage(1, out, trailbytes+1, in, outcodepage, UTF8_CODEPAGE))
        return -1;
    return static_cast<int>(trailbytes); //cast okay as is certainly 0--3
}

//---------------------------------------------------------------------------

void rtlStrToDataX(unsigned & tlen, void * & tgt, unsigned slen, const void * src)
{
    void * data  = rtlMalloc(slen);
    memcpy(data, src, slen);

    tgt = data;
    tlen = slen;
}

void rtlStrToStrX(unsigned & tlen, char * & tgt, unsigned slen, const void * src)
{
    char * data  = (char *)rtlMalloc(slen);
    memcpy(data, src, slen);

    tgt = data;
    tlen = slen;
}

char * rtlStrToVStrX(unsigned slen, const void * src)
{
    char * data  = (char *)rtlMalloc(slen+1);
    memcpy(data, src, slen);
    data[slen] = 0;
    return data;
}

char * rtlEStrToVStrX(unsigned slen, const char * src)
{
    char * astr = (char*)alloca(slen);
    rtlEStrToStr(slen,astr,slen,src);
    return rtlStrToVStrX(slen, astr);
}

void rtlEStrToStrX(unsigned & tlen, char * & tgt, unsigned slen, const char * src)
{
    char * data  = (char *)rtlMalloc(slen);
    rtlEStrToStr(slen, data, slen, src);

    tgt = data;
    tlen = slen;
}

void rtlStrToEStrX(unsigned & tlen, char * & tgt, unsigned slen, const char * src)
{
    char * data  = (char *)rtlMalloc(slen);
    rtlStrToEStr(slen, data, slen, src);

    tgt = data;
    tlen = slen;
}


//---------------------------------------------------------------------------
// See http://www.isthe.com/chongo/tech/comp/fnv/index.html

#define FNV1_64_INIT HASH64_INIT
#define FNV_64_PRIME I64C(0x100000001b3U)
#define APPLY_FNV64(hval, next) { hval *= FNV_64_PRIME; hval ^= next; }


hash64_t rtlHash64Data(size32_t len, const void *buf, hash64_t hval)
{
    const unsigned char *bp = (const unsigned char *)buf;   /* start of buffer */

#if __BYTE_ORDER == __LITTLE_ENDIAN
    //This possibly breaks the aliasing rules for c++, but I can't see it causing any problems
    while (len >= sizeof(unsigned))
    {
        unsigned next = *(const unsigned *)bp;
        bp += sizeof(unsigned);

        for (unsigned i=0; i < sizeof(unsigned); i++)
        {
            APPLY_FNV64(hval, (byte)next);
            next >>= 8;
        }

        len -= sizeof(unsigned);
    }
#endif

    const unsigned char *be = bp + len;     /* beyond end of buffer */
    while (bp < be) 
    {
        APPLY_FNV64(hval, *bp++);
    }

    return hval;
}


hash64_t rtlHash64VStr(const char *str, hash64_t hval)
{
    const unsigned char *s = (const unsigned char *)str;
    unsigned char c;

    while ((c = *s++) != 0) 
    {
        APPLY_FNV64(hval, c);
    }

    return hval;
}

hash64_t rtlHash64Unicode(unsigned length, UChar const * k, hash64_t hval)
{
    unsigned trimLength = rtlTrimUnicodeStrLen(length, k);
    for (unsigned i=0; i < trimLength; i++)
    {
        //Handle surrogate pairs correctly, but still hash the utf16 representation
        const byte * cur = reinterpret_cast<const byte *>(&k[i]);
        UChar32 c = k[i];
        if (U16_IS_SURROGATE(c))
        {
            U16_GET(k, 0, i, length, c);
            if (!u_hasBinaryProperty(c, UCHAR_DEFAULT_IGNORABLE_CODE_POINT))
            {
                APPLY_FNV64(hval, cur[0]);
                APPLY_FNV64(hval, cur[1]);
                APPLY_FNV64(hval, cur[2]);
                APPLY_FNV64(hval, cur[3]);
            }
            //Skip the surrogate pair
            i++;
        }
        else
        {
            if (!u_hasBinaryProperty(c, UCHAR_DEFAULT_IGNORABLE_CODE_POINT))
            {
                APPLY_FNV64(hval, cur[0]);
                APPLY_FNV64(hval, cur[1]);
            }
        }
    }
    return hval;
}

hash64_t rtlHash64VUnicode(UChar const * k, hash64_t initval)
{
    return rtlHash64Unicode(rtlUnicodeStrlen(k), k, initval);
}



//---------------------------------------------------------------------------
// See http://www.isthe.com/chongo/tech/comp/fnv/index.html

#define FNV1_32_INIT HASH32_INIT
#define FNV_32_PRIME 0x1000193
#define APPLY_FNV32(hval, next) { hval *= FNV_32_PRIME; hval ^= next; }


unsigned rtlHash32Data(size32_t len, const void *buf, unsigned hval)
{
    const unsigned char *bp = (const unsigned char *)buf;   /* start of buffer */

#if __BYTE_ORDER == __LITTLE_ENDIAN
    //This possibly breaks the aliasing rules for c++, but I can't see it causing any problems
    while (len >= sizeof(unsigned))
    {
        unsigned next = *(const unsigned *)bp;
        bp += sizeof(unsigned);

        for (unsigned i=0; i < sizeof(unsigned); i++)
        {
            APPLY_FNV32(hval, (byte)next);
            next >>= 8;
        }

        len -= sizeof(unsigned);
    }
#endif

    const unsigned char *be = bp + len;     /* beyond end of buffer */
    while (bp < be) 
    {
        APPLY_FNV32(hval, *bp++);
    }

    return hval;
}


unsigned rtlHash32VStr(const char *str, unsigned hval)
{
    const unsigned char *s = (const unsigned char *)str;
    unsigned char c;

    while ((c = *s++) != 0) 
    {
        APPLY_FNV32(hval, c);
    }

    return hval;
}

unsigned rtlHash32Unicode(unsigned length, UChar const * k, unsigned hval)
{
    unsigned trimLength = rtlTrimUnicodeStrLen(length, k);
    for (unsigned i=0; i < trimLength; i++)
    {
        //Handle surrogate pairs correctly, but still hash the utf16 representation
        const byte * cur = reinterpret_cast<const byte *>(&k[i]);
        UChar32 c = k[i];
        if (U16_IS_SURROGATE(c))
        {
            U16_GET(k, 0, i, length, c);
            if (!u_hasBinaryProperty(c, UCHAR_DEFAULT_IGNORABLE_CODE_POINT))
            {
                APPLY_FNV32(hval, cur[0]);
                APPLY_FNV32(hval, cur[1]);
                APPLY_FNV32(hval, cur[2]);
                APPLY_FNV32(hval, cur[3]);
            }
            //Skip the surrogate pair
            i++;
        }
        else
        {
            if (!u_hasBinaryProperty(c, UCHAR_DEFAULT_IGNORABLE_CODE_POINT))
            {
                APPLY_FNV32(hval, cur[0]);
                APPLY_FNV32(hval, cur[1]);
            }
        }
    }
    return hval;
}

unsigned rtlHash32VUnicode(UChar const * k, unsigned initval)
{
    return rtlHash32Unicode(rtlUnicodeStrlen(k), k, initval);
}


//---------------------------------------------------------------------------
// Hash Helper functions

#define mix(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<<8); \
  c -= a; c -= b; c ^= (b>>13); \
  a -= b; a -= c; a ^= (c>>12);  \
  b -= c; b -= a; b ^= (a<<16); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>3);  \
  b -= c; b -= a; b ^= (a<<10); \
  c -= a; c -= b; c ^= (b>>15); \
}


#define GETBYTE0(n) ((unsigned)k[n])
#define GETBYTE1(n) ((unsigned)k[n+1]<<8)
#define GETBYTE2(n) ((unsigned)k[n+2]<<16)
#define GETBYTE3(n) ((unsigned)k[n+3]<<24)
#define GETWORD(k,n) (GETBYTE0(n)+GETBYTE1(n)+GETBYTE2(n)+GETBYTE3(n))

// the above looks inefficient but the compiler optimizes well

// this hash looks slow but is about twice as quick as using our CRC table
// and gives gives better results
// (see paper at http://burtleburtle.net/bob/hash/evahash.html for more info)

unsigned rtlHashData( unsigned length, const void *_k, unsigned initval)
{
   const unsigned char * k = (const unsigned char *)_k;
   register unsigned a,b,c,len;

   /* Set up the internal state */
   len = length;
   a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
   c = initval;         /* the previous hash value */

   /*---------------------------------------- handle most of the key */
   while (len >= 12)
   {
      a += GETWORD(k,0);
      b += GETWORD(k,4);
      c += GETWORD(k,8);
      mix(a,b,c);
      k += 12; len -= 12;
   }

   /*------------------------------------- handle the last 11 bytes */
   c += length;
   switch(len)              /* all the case statements fall through */
   {
   case 11: c+=GETBYTE3(7);
   case 10: c+=GETBYTE2(7);
   case 9 : c+=GETBYTE1(7);
      /* the first byte of c is reserved for the length */
   case 8 : b+=GETBYTE3(4); 
   case 7 : b+=GETBYTE2(4);
   case 6 : b+=GETBYTE1(4);
   case 5 : b+=GETBYTE0(4);
   case 4 : a+=GETBYTE3(0);
   case 3 : a+=GETBYTE2(0);
   case 2 : a+=GETBYTE1(0);
   case 1 : a+=GETBYTE0(0);
     /* case 0: nothing left to add */
   }
   mix(a,b,c);
   /*-------------------------------------------- report the result */
   return c;
}

unsigned rtlHashString( unsigned length, const char *_k, unsigned initval)
{
    return rtlHashData(rtlTrimStrLen(length, _k), _k, initval);
}

unsigned rtlHashUnicode(unsigned length, UChar const * k, unsigned initval)
{
    unsigned trimLength = rtlTrimUnicodeStrLen(length, k);
    //Because of the implementation of HASH we need to strip ignoreable code points instead of skipping them
    size32_t tempLength;
    rtlDataAttr temp;
    if (stripIgnorableCharacters(tempLength, temp.refustr(), trimLength, k))
        return rtlHashData(tempLength*2, temp.getustr(), initval);

    return rtlHashData(trimLength*sizeof(UChar), k, initval);
}

unsigned rtlHashVStr(const char * k, unsigned initval)
{
    return rtlHashData(rtlTrimVStrLen(k), k, initval);
}

unsigned rtlHashVUnicode(UChar const * k, unsigned initval)
{
    return rtlHashUnicode(rtlTrimVUnicodeStrLen(k), k, initval);
}

#define GETWORDNC(k,n) ((GETBYTE0(n)+GETBYTE1(n)+GETBYTE2(n)+GETBYTE3(n))&0xdfdfdfdf)

unsigned rtlHashDataNC( unsigned length, const void * _k, unsigned initval)
{
   const unsigned char * k = (const unsigned char *)_k;
   register unsigned a,b,c,len;

   /* Set up the internal state */
   len = length;
   a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
   c = initval;         /* the previous hash value */

   /*---------------------------------------- handle most of the key */
   while (len >= 12)
   {
      a += GETWORDNC(k,0);
      b += GETWORDNC(k,4);
      c += GETWORDNC(k,8);
      mix(a,b,c);
      k += 12; len -= 12;
   }

   /*------------------------------------- handle the last 11 bytes */
   c += length;
   switch(len)              /* all the case statements fall through */
   {
   case 11: c+=GETBYTE3(7)&0xdf;
   case 10: c+=GETBYTE2(7)&0xdf;
   case 9 : c+=GETBYTE1(7)&0xdf;
      /* the first byte of c is reserved for the length */
   case 8 : b+=GETBYTE3(4)&0xdf;
   case 7 : b+=GETBYTE2(4)&0xdf;
   case 6 : b+=GETBYTE1(4)&0xdf;
   case 5 : b+=GETBYTE0(4)&0xdf;
   case 4 : a+=GETBYTE3(0)&0xdf;
   case 3 : a+=GETBYTE2(0)&0xdf;
   case 2 : a+=GETBYTE1(0)&0xdf;
   case 1 : a+=GETBYTE0(0)&0xdf;
     /* case 0: nothing left to add */
   }
   mix(a,b,c);
   /*-------------------------------------------- report the result */
   return c;
}


unsigned rtlHashVStrNC(const char * k, unsigned initval)
{
    return rtlHashDataNC(strlen(k), k, initval);
}



//---------------------------------------------------------------------------

unsigned rtlCrcData( unsigned length, const void *_k, unsigned initval)
{
    return crc32((const char *)_k, length, initval);
}

unsigned rtlCrcUnicode(unsigned length, UChar const * k, unsigned initval)
{
    return crc32((char const *)k, length*2, initval);
}

unsigned rtlCrcVStr( const char * k, unsigned initval)
{
    return crc32(k, strlen(k), initval);
}

unsigned rtlCrcVUnicode(UChar const * k, unsigned initval)
{
    return rtlCrcUnicode(rtlUnicodeStrlen(k), k, initval);
}

//---------------------------------------------------------------------------
// MD5 processing:


void rtlHashMd5Init(size32_t sizestate, void * _state)
{
    assertex(sizestate >= sizeof(md5_state_s));
    md5_state_s * state = (md5_state_s *)_state;
    md5_init(state);
}

void rtlHashMd5Data(size32_t len, const void *buf, size32_t sizestate, void * _state)
{
    md5_state_s * state = (md5_state_s * )_state;
    md5_append(state, (const md5_byte_t *)buf, len);
}


void rtlHashMd5Finish(void * out, size32_t sizestate, void * _state)
{
    typedef md5_byte_t digest_t[16];
    md5_state_s * state = (md5_state_s *)_state;
    md5_finish(state, *(digest_t*)out);
}



//---------------------------------------------------------------------------

unsigned rtlRandom()
{
    CriticalBlock block(random_Sect);   
    return random_->next();
}

void rtlSeedRandom(unsigned value)
{
    CriticalBlock block(random_Sect);   
    random_->seed(value);
}


// These are all useful functions for testing - not really designed for other people to use them...

ECLRTL_API unsigned rtlTick()
{
    return msTick(); 
}

ECLRTL_API bool rtlGPF()
{
    char * x = 0;
    *x = 0;
    return false;
}

ECLRTL_API unsigned rtlSleep(unsigned delay)
{
    MilliSleep(delay);
    return 0;
}


ECLRTL_API unsigned rtlDisplay(unsigned len, const char * src)
{
    LOG(MCprogress, unknownJob, "%.*s", len, src);
    return 0;
}


void rtlEcho(unsigned len, const char * src)
{
    printf("%.*s\n", len, src);
}

ECLRTL_API unsigned __int64 rtlNano()
{
    return cycle_to_nanosec(get_cycles_now());
}

ECLRTL_API void rtlTestGetPrimes(unsigned & num, void * & data)
{
    unsigned numPrimes = 6;
    unsigned size = sizeof(unsigned) * numPrimes;

    unsigned * primes = (unsigned *)rtlMalloc(size);
    primes[0] = 1;
    primes[1] = 2;
    primes[2] = 3;
    primes[3] = 5;
    primes[4] = 7;
    primes[5] = 11;

    num = numPrimes;
    data = primes;
}

ECLRTL_API void rtlTestFibList(bool & outAll, size32_t & outSize, void * & outData, bool inAll, size32_t inSize, const void * inData)
{
    const unsigned * inList = (const unsigned *)inData;
    unsigned * outList = (unsigned *)rtlMalloc(inSize);
    unsigned * curOut = outList;
    unsigned count = inSize / sizeof(*inList);
    unsigned prev = 0;
    for (unsigned i=0; i < count; i++)
    {
        unsigned next = *inList++;
        *curOut++ = next + prev;
        prev = next;
    }

    outAll = inAll;
    outSize = inSize;
    outData = outList;
}


unsigned rtlDelayReturn(unsigned value, unsigned sleepTime)
{
    MilliSleep(sleepTime);
    return value;
}

//---------------------------------------------------------------------------

class CRtlFailException : public CInterface, public IUserException
{
public:
    CRtlFailException(int _code, char const * _msg) : code(_code) { msg = strdup(_msg); }
    ~CRtlFailException() { free(msg); }
    IMPLEMENT_IINTERFACE;
    virtual int             errorCode() const { return code; }
    virtual StringBuffer &  errorMessage(StringBuffer & buff) const { return buff.append(msg); }
    virtual MessageAudience errorAudience() const { return MSGAUD_user; }
private:
    int code;
    char * msg;
};

void rtlFail(int code, const char *msg)
{
    throw dynamic_cast<IUserException *>(new CRtlFailException(code, msg));
}

void rtlSysFail(int code, const char *msg)
{
    throw MakeStringException(MSGAUD_user, code, "%s", msg);
}

void rtlThrowOutOfMemory(int code, const char *msg)
{
    throw static_cast<IUserException *>(new CRtlFailException(code, msg));
}

void rtlReportRowOverflow(unsigned size, unsigned max)
{
    throw MakeStringException(MSGAUD_user, 1000, "Row size %u exceeds the maximum size specified(%u)", size, max);
}

void rtlReportFieldOverflow(unsigned size, unsigned max, const char * name)
{
    if (!name)
        rtlReportRowOverflow(size, max);
    else
        throw MakeStringException(MSGAUD_user, 1000, "Assignment to field '%s' causes row overflow.  Size %u exceeds the maximum size specified(%u)", name, size, max);
}

void rtlCheckRowOverflow(unsigned size, unsigned max)
{
    if (size > max)
        rtlReportRowOverflow(size, max);
}

void rtlCheckFieldOverflow(unsigned size, unsigned max, const char * field)
{
    if (size > max)
        rtlReportFieldOverflow(size, max, field);
}

void rtlFailUnexpected()
{
    throw MakeStringException(MSGAUD_user, -1, "Unexpected code execution");
}

void rtlFailOnAssert()
{
    throw MakeStringException(MSGAUD_user, -1, "Abort execution");
}

void rtlFailDivideByZero()
{
    throw MakeStringException(MSGAUD_user, -1, "Division by zero");
}

//---------------------------------------------------------------------------

void deserializeRaw(unsigned recordSize, void *record, MemoryBuffer &in)
{
    in.read(recordSize, record);
}

void deserializeDataX(size32_t & len, void * & data, MemoryBuffer &in)
{
    free(data);
    in.read(sizeof(len), &len);
    data = rtlMalloc(len);
    in.read(len, data);
}

void deserializeStringX(size32_t & len, char * & data, MemoryBuffer &in)
{
    free(data);
    in.read(sizeof(len), &len);
    data = (char *)rtlMalloc(len);
    in.read(len, data);
}

char * deserializeCStringX(MemoryBuffer &in)
{
    unsigned len;
    in.read(sizeof(len), &len);
    char * data = (char *)rtlMalloc(len+1);
    in.read(len, data);
    data[len] = 0;
    return data;
}

void deserializeUnicodeX(size32_t & len, UChar * & data, MemoryBuffer &in)
{
    free(data);
    in.read(sizeof(len), &len);
    data = (UChar *)rtlMalloc(len*sizeof(UChar));
    in.read(len*sizeof(UChar), data);
}

void deserializeUtf8X(size32_t & len, char * & data, MemoryBuffer &in)
{
    free(data);
    in.read(sizeof(len), &len);
    unsigned size = rtlUtf8Size(len, in.readDirect(0));
    data = (char *)rtlMalloc(size);
    in.read(size, data);
}

UChar * deserializeVUnicodeX(MemoryBuffer &in)
{
    unsigned len;
    in.read(sizeof(len), &len);
    UChar * data = (UChar *)rtlMalloc((len+1)*sizeof(UChar));
    in.read(len*sizeof(UChar), data);
    data[len] = 0;
    return data;
}

void deserializeSet(bool & isAll, size32_t & len, void * & data, MemoryBuffer &in)
{
    free(data);
    in.read(isAll);
    in.read(sizeof(len), &len);
    data = rtlMalloc(len);
    in.read(len, data);
}

void serializeRaw(unsigned recordSize, const void *record, MemoryBuffer &out)
{
    out.append(recordSize, record);
}


void serializeDataX(size32_t len, const void * data, MemoryBuffer &out)
{
    out.append(len).append(len, data);
}

void serializeStringX(size32_t len, const char * data, MemoryBuffer &out)
{
    out.append(len).append(len, data);
}

void serializeCStringX(const char * data, MemoryBuffer &out)
{
    unsigned len = strlen(data);
    out.append(len).append(len, data);
}

void serializeUnicodeX(size32_t len, const UChar * data, MemoryBuffer &out)
{
    out.append(len).append(len*sizeof(UChar), data);
}

void serializeUtf8X(size32_t len, const char * data, MemoryBuffer &out)
{
    out.append(len).append(rtlUtf8Size(len, data), data);
}

void serializeSet(bool isAll, size32_t len, const void * data, MemoryBuffer &out)
{
    out.append(isAll).append(len).append(len, data);
}


//---------------------------------------------------------------------------

ECLRTL_API void serializeFixedString(unsigned len, const char *field, MemoryBuffer &out)
{
    out.append(len, field);
}

ECLRTL_API void serializeLPString(unsigned len, const char *field, MemoryBuffer &out)
{
    out.append(len);
    out.append(len, field);
}

ECLRTL_API void serializeVarString(const char *field, MemoryBuffer &out)
{
    out.append(field); 
}

ECLRTL_API void serializeBool(bool field, MemoryBuffer &out)
{
    out.append(field); 
}

ECLRTL_API void serializeFixedData(unsigned len, const void *field, MemoryBuffer &out)
{
    out.append(len, field); 
}

ECLRTL_API void serializeLPData(unsigned len, const void *field, MemoryBuffer &out)
{
    out.append(len);
    out.append(len, field); 
}

ECLRTL_API void serializeInt1(signed char field, MemoryBuffer &out)
{
    // MORE - why did overloading pick the int method for this???
    // out.append(field); 
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeInt2(signed short field, MemoryBuffer &out)
{
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeInt3(signed int field, MemoryBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.appendEndian(3, &field); 
#else
    out.appendEndian(3, ((char *) &field) + 1); 
#endif
}


ECLRTL_API void serializeInt4(signed int field, MemoryBuffer &out)
{
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeInt5(signed __int64 field, MemoryBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.appendEndian(5, &field); 
#else
    out.appendEndian(5, ((char *) &field) + 3); 
#endif
}

ECLRTL_API void serializeInt6(signed __int64 field, MemoryBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.appendEndian(6, &field); 
#else
    out.appendEndian(6, ((char *) &field) + 2); 
#endif
}

ECLRTL_API void serializeInt7(signed __int64 field, MemoryBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.appendEndian(7, &field); 
#else
    out.appendEndian(7, ((char *) &field) + 1); 
#endif
}

ECLRTL_API void serializeInt8(signed __int64 field, MemoryBuffer &out)
{
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeUInt1(unsigned char field, MemoryBuffer &out)
{
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeUInt2(unsigned short field, MemoryBuffer &out)
{
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeUInt3(unsigned int field, MemoryBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.appendEndian(3, &field); 
#else
    out.appendEndian(3, ((char *) &field) + 1); 
#endif
}

ECLRTL_API void serializeUInt4(unsigned int field, MemoryBuffer &out)
{
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeUInt5(unsigned __int64 field, MemoryBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.appendEndian(5, &field); 
#else
    out.appendEndian(5, ((char *) &field) + 3); 
#endif
}

ECLRTL_API void serializeUInt6(unsigned __int64 field, MemoryBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.appendEndian(6, &field); 
#else
    out.appendEndian(6, ((char *) &field) + 2); 
#endif
}

ECLRTL_API void serializeUInt7(unsigned __int64 field, MemoryBuffer &out)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    out.appendEndian(7, &field); 
#else
    out.appendEndian(7, ((char *) &field) + 1); 
#endif
}

ECLRTL_API void serializeUInt8(unsigned __int64 field, MemoryBuffer &out)
{
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeReal4(float field, MemoryBuffer &out)
{
    out.appendEndian(sizeof(field), &field); 
}

ECLRTL_API void serializeReal8(double field, MemoryBuffer &out)
{
    out.append(sizeof(field), &field); 
}

//These maths functions can all have out of range arguments....
//---------------------------------------------------------------------------
ECLRTL_API double rtlLog10(double x)
{
    if (x <= 0) return 0;
    return log10(x);
}

ECLRTL_API double rtlLog(double x)
{
    if (x <= 0) return 0;
    return log(x);
}

ECLRTL_API double rtlSqrt(double x)
{
    if (x < 0) return 0;
    return sqrt(x);
}

ECLRTL_API double rtlACos(double x)
{
    if (fabs(x) > 1) return 0;
    return acos(x);
}

ECLRTL_API double rtlASin(double x)
{
    if (fabs(x) > 1) return 0;
    return asin(x);
}

//---------------------------------------------------------------------------

ECLRTL_API bool rtlIsValidReal(unsigned size, const void * data)
{
    byte * bytes = (byte *)data;

    //Valid unless it is a Nan, represented by exponent all 1's and non-zero mantissa (ignore the sign).
    if (size == 4)
    {
        //sign(1) exponent(8) mantissa(23)
        if (((bytes[3] & 0x7f) == 0x7f) && ((bytes[2] & 0x80) == 0x80))
        {
            if ((bytes[2] & 0x7f) != 0 || bytes[1] || bytes[0])
                return false;
        }
    }
    else if (size == 8)
    {
        //sign(1) exponent(11) mantissa(52)
        if (((bytes[7] & 0x7f) == 0x7f) && ((bytes[6] & 0xF0) == 0xF0))
        {
            if ((bytes[6] & 0xF) || bytes[5] || bytes[4] || bytes[3] || bytes[2] || bytes[1] || bytes[0])
                return false;
        }
    }
    else
    {
        //sign(1) exponent(15) mantissa(64)
        assertex(size==10);
        if (((bytes[9] & 0x7f) == 0x7f) && (bytes[8] == 0xFF))
        {
            if (bytes[7] || bytes[6] || bytes[5] || bytes[4] || bytes[3] || bytes[2] || bytes[1] || bytes[0])
                return false;
        }
    }

    return true;
}

double rtlCreateRealNull()
{
    union
    {
        byte data[8];
        double r;
    } u;
    //Use a non-signaling NaN
    memcpy(u.data, "\x01\x00\x00\x00\x00\x00\xF0\x7f", 8);
    return u.r;
}


void rtlUnicodeToUnicode(size32_t outlen, UChar * out, size32_t inlen, UChar const *in)
{
    if(inlen>outlen) inlen = outlen;
    memcpy(out, in, inlen*2);
    while(inlen<outlen)
        out[inlen++] = 0x0020;
}

void rtlUnicodeToVUnicode(size32_t outlen, UChar * out, size32_t inlen, UChar const *in)
{
    if((inlen>=outlen) && (outlen != 0)) inlen = outlen-1;
    memcpy(out, in, inlen*2);
    out[inlen] = 0x0000;
}

void rtlVUnicodeToUnicode(size32_t outlen, UChar * out, UChar const *in)
{
    rtlUnicodeToUnicode(outlen, out, rtlUnicodeStrlen(in), in);
}

void rtlVUnicodeToVUnicode(size32_t outlen, UChar * out, UChar const *in)
{
    rtlUnicodeToVUnicode(outlen, out, rtlUnicodeStrlen(in), in);
}

void rtlUnicodeToUnicodeX(unsigned & tlen, UChar * & tgt, unsigned slen, UChar const * src)
{
    tgt  = (UChar *)rtlMalloc(slen*2);
    memcpy(tgt, src, slen*2);
    tlen = slen;
}

UChar * rtlUnicodeToVUnicodeX(unsigned slen, UChar const * src)
{
    UChar * data  = (UChar *)rtlMalloc((slen+1)*2);
    memcpy(data, src, slen*2);
    data[slen] = 0x0000;
    return data;
}

void rtlVUnicodeToUnicodeX(unsigned & tlen, UChar * & tgt, UChar const * src)
{
    rtlUnicodeToUnicodeX(tlen, tgt, rtlUnicodeStrlen(src), src);
}

UChar * rtlVUnicodeToVUnicodeX(UChar const * src)
{
    return rtlUnicodeToVUnicodeX(rtlUnicodeStrlen(src), src);
}

void rtlDecPushUnicode(size32_t len, UChar const * data)
{
    char * buff = 0;
    unsigned bufflen = 0;
    rtlUnicodeToStrX(bufflen, buff, len, data);
    DecPushString(bufflen, buff);
    rtlFree(buff);
}

unsigned rtlUnicodeStrlen(UChar const * str)
{
    return u_strlen(str);
}

//---------------------------------------------------------------------------

unsigned rtlUtf8Size(const void * data)
{
    return readUtf8Size(data);
}

unsigned rtlUtf8Size(unsigned len, const void * _data)
{
    const byte * data = (const byte *)_data;
    size32_t offset = 0;
    for (unsigned i=0; i< len; i++)
        offset += readUtf8Size(data+offset);
    return offset;
}

unsigned rtlUtf8Length(unsigned size, const void * _data)
{
    const byte * data = (const byte *)_data;
    size32_t length = 0;
    for (unsigned offset=0; offset < size; offset += readUtf8Size(data+offset))
        length++;
    return length;
}

unsigned rtlUtf8Char(const void * data)
{
    return readUtf8Char(data);
}

void rtlUtf8ToData(size32_t outlen, void * out, size32_t inlen, const char *in)
{
    unsigned insize = rtlUtf8Size(inlen, in);
    rtlCodepageToCodepage(outlen, (char *)out, insize, in, ASCII_LIKE_CODEPAGE, UTF8_CODEPAGE);
}

void rtlUtf8ToDataX(size32_t & outlen, void * & out, size32_t inlen, const char *in)
{
    unsigned insize = rtlUtf8Size(inlen, in);
    char * cout;
    rtlCodepageToCodepageX(outlen, cout, inlen, insize, in, ASCII_LIKE_CODEPAGE, UTF8_CODEPAGE);
    out = cout;
}

void rtlUtf8ToStr(size32_t outlen, char * out, size32_t inlen, const char *in)
{
    unsigned insize = rtlUtf8Size(inlen, in);
    rtlCodepageToCodepage(outlen, (char *)out, insize, in, ASCII_LIKE_CODEPAGE, UTF8_CODEPAGE);
}

void rtlUtf8ToStrX(size32_t & outlen, char * & out, size32_t inlen, const char *in)
{
    unsigned insize = rtlUtf8Size(inlen, in);
    rtlCodepageToCodepageX(outlen, out, inlen, insize, in, ASCII_LIKE_CODEPAGE, UTF8_CODEPAGE);
}

char * rtlUtf8ToVStr(size32_t inlen, const char *in)
{
    unsigned utfSize = rtlUtf8Size(inlen, in);
    char *ret = (char *) rtlMalloc(inlen+1);
    rtlCodepageToCodepage(inlen, ret, utfSize, in, ASCII_LIKE_CODEPAGE, UTF8_CODEPAGE);
    ret[inlen] = 0;
    return ret;
}

void rtlDataToUtf8(size32_t outlen, char * out, size32_t inlen, const void *in)
{
    rtlCodepageToCodepage(outlen*UTF8_MAXSIZE, (char *)out, inlen, (const char *)in, UTF8_CODEPAGE, ASCII_LIKE_CODEPAGE);
}

void rtlDataToUtf8X(size32_t & outlen, char * & out, size32_t inlen, const void *in)
{
    unsigned outsize;
    rtlCodepageToCodepageX(outsize, out, inlen*UTF8_MAXSIZE, inlen, (const char *)in, UTF8_CODEPAGE, ASCII_LIKE_CODEPAGE);
    outlen = rtlUtf8Length(outsize, out);
}

void rtlStrToUtf8(size32_t outlen, char * out, size32_t inlen, const char *in)
{
    rtlCodepageToCodepage(outlen*UTF8_MAXSIZE, (char *)out, inlen, in, UTF8_CODEPAGE, ASCII_LIKE_CODEPAGE);
}

void rtlStrToUtf8X(size32_t & outlen, char * & out, size32_t inlen, const char *in)
{
    unsigned outsize;
    rtlCodepageToCodepageX(outsize, out, inlen*UTF8_MAXSIZE, inlen, in, UTF8_CODEPAGE, ASCII_LIKE_CODEPAGE);
    outlen = rtlUtf8Length(outsize, out);
}

void rtlUtf8ToUtf8(size32_t outlen, char * out, size32_t inlen, const char *in)
{
    //Packs as many characaters as it can into the target, but don't include any half characters
    size32_t offset = 0;
    size32_t outsize = outlen*UTF8_MAXSIZE;
    for (unsigned i=0; i< inlen; i++)
    {
        unsigned nextSize = readUtf8Size(in+offset);
        if (offset + nextSize > outsize)
            break;
        offset += nextSize;
    }
    memcpy(out, in, offset);
    if (offset != outsize)
        memset(out+offset, ' ', outsize-offset);
}

void rtlUtf8ToUtf8X(size32_t & outlen, char * & out, size32_t inlen, const char *in)
{
    unsigned insize = rtlUtf8Size(inlen, in);
    char * buffer = (char *)rtlMalloc(insize);
    memcpy(buffer, in, insize);
    outlen = inlen;
    out = buffer;
}

static int rtlCompareUtf8Utf8ViaUnicode(size32_t llen, const char * left, size32_t rlen, const char * right, const char * locale)
{
    rtlDataAttr uleft(llen*sizeof(UChar));
    rtlDataAttr uright(rlen*sizeof(UChar));
    rtlUtf8ToUnicode(llen, uleft.getustr(), llen, left);
    rtlUtf8ToUnicode(rlen, uright.getustr(), rlen, right);
    return rtlCompareUnicodeUnicode(llen, uleft.getustr(), rlen, uright.getustr(), locale);
}

int rtlCompareUtf8Utf8(size32_t llen, const char * left, size32_t rlen, const char * right, const char * locale)
{
    //MORE: Do a simple comparison as long as there are no non->0x80 characters around
    //      fall back to a full unicode comparison if we hit one - or in the next character to allow for accents etc.
    const byte * bleft = (const byte *)left;
    const byte * bright = (const byte *)right;
    unsigned len = llen > rlen ? rlen : llen;
    for (unsigned i = 0; i < len; i++)
    {
        byte nextLeft = bleft[i];
        byte nextRight = bright[i];
        if (nextLeft >= 0x80 || nextRight >= 0x80)
            return rtlCompareUtf8Utf8ViaUnicode(llen-i, left+i, rlen-i, right+i, locale);
        if ((i+1 != len) && ((bleft[i+1] >= 0x80) || bright[i+1] >= 0x80))
            return rtlCompareUtf8Utf8ViaUnicode(llen-i, left+i, rlen-i, right+i, locale);
        if (nextLeft != nextRight)
            return nextLeft - nextRight;
    }
    int diff = 0;
    if (len != llen)
    {
        for (;(diff == 0) && (len != llen);len++)
            diff = bleft[len] - ' ';
    }
    else if (len != rlen)
    {
        for (;(diff == 0) && (len != rlen);len++)
            diff = ' ' - bright[len];
    }
    return diff;
}

int rtlCompareUtf8Utf8Strength(size32_t llen, const char * left, size32_t rlen, const char * right, const char * locale, unsigned strength)
{
    //GH->PG Any better way of doing this?  We could possible decide it was a binary comparison instead I guess.
    rtlDataAttr uleft(llen*sizeof(UChar));
    rtlDataAttr uright(rlen*sizeof(UChar));
    rtlUtf8ToUnicode(llen, uleft.getustr(), llen, left);
    rtlUtf8ToUnicode(rlen, uright.getustr(), rlen, right);
    return rtlCompareUnicodeUnicodeStrength(llen, uleft.getustr(), rlen, uright.getustr(), locale, strength);
}

void rtlDecPushUtf8(size32_t len, const void * data)
{
    DecPushString(len, (const char *)data); // good enough for the moment
}

bool rtlUtf8ToBool(size32_t inlen, const char * in)
{
    return rtlStrToBool(inlen, in);
}

__int64 rtlUtf8ToInt(size32_t inlen, const char * in)
{
    return rtlStrToInt8(inlen, in);         // good enough for the moment
}

double rtlUtf8ToReal(size32_t inlen, const char * in)
{
    return rtlStrToReal(inlen, in);         // good enough for the moment
}

void rtlCodepageToUtf8(unsigned outlen, char * out, unsigned inlen, char const * in, char const * codepage)
{
    rtlCodepageToCodepage(outlen*UTF8_MAXSIZE, (char *)out, inlen, in, UTF8_CODEPAGE, codepage);
}

void rtlCodepageToUtf8X(unsigned & outlen, char * & out, unsigned inlen, char const * in, char const * codepage)
{
    unsigned outsize;
    rtlCodepageToCodepageX(outsize, out, inlen*UTF8_MAXSIZE, inlen, in, UTF8_CODEPAGE, codepage);
    outlen = rtlUtf8Length(outsize, out);
}

void rtlUtf8ToCodepage(unsigned outlen, char * out, unsigned inlen, char const * in, char const * codepage)
{
    unsigned insize = rtlUtf8Size(inlen, in);
    rtlCodepageToCodepage(outlen, (char *)out, insize, in, codepage, UTF8_CODEPAGE);
}

void rtlUtf8ToCodepageX(unsigned & outlen, char * & out, unsigned inlen, char const * in, char const * codepage)
{
    unsigned insize = rtlUtf8Size(inlen, in);
    rtlCodepageToCodepageX(outlen, out, inlen, insize, in, codepage, UTF8_CODEPAGE);
}

void rtlUnicodeToUtf8X(unsigned & outlen, char * & out, unsigned inlen, const UChar * in)
{
    unsigned outsize;
    rtlUnicodeToCodepageX(outsize, out, inlen, in, UTF8_CODEPAGE);
    outlen = rtlUtf8Length(outsize, out);
}

void rtlUnicodeToUtf8(unsigned outlen, char * out, unsigned inlen, const UChar * in)
{
    rtlUnicodeToCodepage(outlen*UTF8_MAXSIZE, out, inlen, in, UTF8_CODEPAGE);
}

void rtlUtf8ToUnicodeX(unsigned & outlen, UChar * & out, unsigned inlen, char const * in)
{
    rtlCodepageToUnicodeX(outlen, out, rtlUtf8Size(inlen, in), in, UTF8_CODEPAGE);
}

void rtlUtf8ToUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in)
{
    rtlCodepageToUnicode(outlen, out, rtlUtf8Size(inlen, in), in, UTF8_CODEPAGE);
}

ECLRTL_API void rtlUtf8SubStrFT(unsigned tlen, char * tgt, unsigned slen, char const * src, unsigned from, unsigned to)
{
    normalizeFromTo(from, to);
    clipFromTo(from, to, slen);

    unsigned copylen = to - from;
    unsigned startOffset = rtlUtf8Size(from, src);
    rtlUtf8ToUtf8(tlen, tgt, copylen, src+startOffset);
}

ECLRTL_API void rtlUtf8SubStrFTX(unsigned & tlen, char * & tgt, unsigned slen, char const * src, unsigned from, unsigned to)
{
    normalizeFromTo(from, to);
    unsigned len = to - from;
    clipFromTo(from, to, slen);

    unsigned copylen = to - from;
    unsigned fillSize = len - copylen;
    unsigned startOffset = rtlUtf8Size(from, src);
    unsigned copySize = rtlUtf8Size(copylen, src+startOffset);

    char * buffer = (char *)rtlMalloc(copySize + fillSize);
    memcpy(buffer, (byte *)src+startOffset, copySize);
    if (fillSize)
        memset(buffer+copySize, ' ', fillSize);
    tlen = len;
    tgt = buffer;
}

ECLRTL_API void rtlUtf8SubStrFX(unsigned & tlen, char * & tgt, unsigned slen, char const * src, unsigned from)
{
    normalizeFromTo(from, slen);
    unsigned len = slen - from;
    unsigned startOffset = rtlUtf8Size(from, src);
    unsigned copySize = rtlUtf8Size(len, src+startOffset);

    char * buffer = (char *)rtlMalloc(copySize);
    memcpy(buffer, (byte *)src+startOffset, copySize);
    tlen = len;
    tgt = buffer;
}

ECLRTL_API void rtlUtf8ToLower(size32_t l, char * t, char const * locale)
{
    //Convert to lower case, but only go via unicode routines if we have to...
    for (unsigned i=0; i< l; i++)
    {
        byte next = *t;
        if (next >= 0x80)
        {
            //yuk, go via unicode to do the convertion.
            unsigned len = l-i;
            unsigned size = rtlUtf8Size(len, t+i);
            rtlDataAttr unicode(len*sizeof(UChar));
            rtlCodepageToUnicode(len, unicode.getustr(), size, t+i, UTF8_CODEPAGE);
            rtlUnicodeToLower(len, unicode.getustr(), locale);
            rtlUnicodeToCodepage(size, t+i, len, unicode.getustr(), UTF8_CODEPAGE);
            return;
        }

        *t++ = tolower(next);
    }
}

ECLRTL_API void rtlConcatUtf8(unsigned & tlen, char * * tgt, ...)
{
    //Going to have to go via unicode because of normalization.  However, it might be worth optimizing the case where no special characters are present
    va_list args;

    unsigned totalLength = 0;
    unsigned maxLength = 0;
    va_start(args, tgt);
    for(;;)
    {
        unsigned len = va_arg(args, unsigned);
        if(len+1==0)
            break;
        const char * str = va_arg(args, const char *);
        totalLength += len;
        if (len > maxLength)
            maxLength = len;
    }
    va_end(args);

    rtlDataAttr next(maxLength*sizeof(UChar));
    rtlDataAttr result(totalLength*sizeof(UChar));
    unsigned idx = 0;
    UErrorCode err = U_ZERO_ERROR;
    va_start(args, tgt);
    for(;;)
    {
        unsigned len = va_arg(args, unsigned);
        if(len+1==0)
            break;
        if (len)
        {
            const char * str = va_arg(args, const char *);
            rtlUtf8ToUnicode(len, next.getustr(), len, str);
            idx = unorm_concatenate(result.getustr(), idx, next.getustr(), len, result.getustr(), totalLength, UNORM_NFC, 0, &err);
        }
    }
    va_end(args);

    rtlUnicodeToUtf8X(tlen, *tgt, idx, result.getustr());
}

ECLRTL_API unsigned rtlConcatUtf8ToUtf8(unsigned tlen, char * tgt, unsigned offset, unsigned slen, const char * src)
{
    //NB: Inconsistently with the other varieties, idx is a byte offset, not a character position to make the code more efficient.....
    //normalization is done in the space filling routine at the end
    unsigned ssize = rtlUtf8Size(slen, src);
    assertex(tlen * UTF8_MAXSIZE >= offset+ssize);
    memcpy(tgt+offset, src, ssize);
    return offset + ssize;

}

ECLRTL_API void rtlUtf8SpaceFill(unsigned tlen, char * tgt, unsigned offset)
{
    const byte * src = (const byte *)tgt;
    for (unsigned i=0; i<offset; i++)
    {
        if (src[i] >= 0x80)
        {
            unsigned idx = rtlUtf8Length(offset, tgt);
            rtlDataAttr unicode(idx*sizeof(UChar));
            rtlUtf8ToUnicode(idx, unicode.getustr(), idx, tgt);
            unicodeEnsureIsNormalized(idx, unicode.getustr());
            rtlUnicodeToUtf8(tlen, tgt, idx, unicode.getustr());
            return;
        }
    }

    //no special characters=>easy route.
    memset(tgt+offset, ' ', tlen*UTF8_MAXSIZE-offset);
}

ECLRTL_API unsigned rtlHash32Utf8(unsigned length, const char * k, unsigned initval)
{
    //These need to hash the same way as a UNICODE string would => convert to UNICODE
    //It would be hard to optimize to hash the string without performing the conversion.
    size32_t tempLength;
    rtlDataAttr temp;
    rtlUtf8ToUnicodeX(tempLength, temp.refustr(), length, k);
    return rtlHash32Unicode(tempLength, temp.getustr(), initval);
}

ECLRTL_API unsigned rtlHashUtf8(unsigned length, const char * k, unsigned initval)
{
    //These need to hash the same way as a UNICODE string would => convert to UNICODE
    size32_t tempLength;
    rtlDataAttr temp;
    rtlUtf8ToUnicodeX(tempLength, temp.refustr(), length, k);
    return rtlHashUnicode(tempLength, temp.getustr(), initval);
}

ECLRTL_API hash64_t rtlHash64Utf8(unsigned length, const char * k, hash64_t initval)
{
    //These need to hash the same way as a UNICODE string would => convert to UNICODE
    size32_t tempLength;
    rtlDataAttr temp;
    rtlUtf8ToUnicodeX(tempLength, temp.refustr(), length, k);
    return rtlHash64Unicode(tempLength, temp.getustr(), initval);
}

unsigned rtlCrcUtf8(unsigned length, const char * k, unsigned initval)
{
    return rtlCrcData(rtlUtf8Size(length, k), k, initval);
}

int rtlNewSearchUtf8Table(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search, const char * locale)
{
    //MORE: Hopelessly inefficient....  Should rethink - possibly introducing a class for doing string searching, and the Utf8 variety pre-converting the
    //search strings into unicode.
    int left = 0;
    int right = count;

    do
    {
        int mid = (left + right) >> 1;
        int cmp = rtlCompareUtf8Utf8(width, search, elemlen, table[mid], locale);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else
            return mid;
    } while (left < right);

    return -1;
}

//---------------------------------------------------------------------------
#ifdef _USE_BOOST_REGEX

class CStrRegExprFindInstance : implements IStrRegExprFindInstance
{
private:
    bool            matched;
    const boost::regex * regEx;
    boost::cmatch   subs;
    char *          sample; //only required if findstr/findvstr will be called

public:
    CStrRegExprFindInstance(const boost::regex * _regEx, const char * _str, size32_t _from, size32_t _len, bool _keep)
        : regEx(_regEx)
    {
        matched = false;
        sample = NULL;
        try
        {
            if (_keep)
            {
                sample = (char *)rtlMalloc(_len + 1);  //required for findstr
                memcpy(sample, _str + _from, _len);
                sample[_len] = (char)NULL;
                matched = boost::regex_search(sample, subs, *regEx);
            }
            else 
            {
                matched = boost::regex_search(_str + _from, _str + _len, subs, *regEx);
            }
        }
        catch (const std::runtime_error & e)
        {
            throw MakeStringException(0, "Error in regex search: %s (regex: %s)", e.what(), regEx->str().c_str());
        }

    }

    ~CStrRegExprFindInstance() //CAVEAT non-virtual destructor !
    {
        free(sample);
    }

    //IStrRegExprFindInstance

    bool found() const { return matched; }

    void getMatchX(unsigned & outlen, char * & out, unsigned n = 0) const
    {
        if (matched && (n < subs.size()))
        {
            outlen = subs[n].second - subs[n].first;
            out = (char *)rtlMalloc(outlen);
            memcpy(out, subs[n].first, outlen);
        }
        else
        {
            outlen = 0;
            out = NULL;
        }
    }

    char const * findvstr(unsigned outlen, char * out, unsigned n = 0)
    {
        if (matched && (n < subs.size()))
        {
            unsigned sublen = subs[n].second - subs[n].first;
            if (sublen >= outlen)
                sublen = outlen - 1;
            memcpy(out, subs[n].first, sublen);
            out[sublen] = 0;
        }
        else
        {
            out[0] = 0;
        }
        return out;
    }
};

//---------------------------------------------------------------------------

class CCompiledStrRegExpr : implements ICompiledStrRegExpr
{
private:
    boost::regex    regEx;

public:
    CCompiledStrRegExpr(const char * _regExp, bool _isCaseSensitive = false) 
    {
        try
        {
            if (_isCaseSensitive)
                regEx.assign(_regExp, boost::regbase::perl);
            else
                regEx.assign(_regExp, boost::regbase::perl | boost::regbase::icase);                
        }
        catch(const boost::bad_expression & e)
        {
            StringBuffer msg;
            msg.append("Bad regular expression: ").append(e.what()).append(": ").append(_regExp);
            rtlFail(0, msg.str());  //throws
        }
    }
    
    //ICompiledStrRegExpr

    void replace(size32_t & outlen, char * & out, size32_t slen, char const * str, size32_t rlen, char const * replace) const
    {
        std::string src(str, str + slen);
        std::string fmt(replace, replace + rlen);
        std::string tgt;
        try
        {
//          tgt = boost::regex_merge(src, cre->regEx, fmt, boost::format_perl); //Algorithm regex_merge has been renamed regex_replace, existing code will continue to compile, but new code should use regex_replace instead.
            tgt = boost::regex_replace(src, regEx, fmt, boost::format_perl);
        }
        catch(const std::runtime_error & e)
        {
            throw MakeStringException(0, "Error in regex replace: %s (regex: %s)", e.what(), regEx.str().c_str());
        }
        outlen = tgt.length();
        out = (char *)rtlMalloc(outlen);
        memcpy(out, tgt.data(), outlen);
    }

    IStrRegExprFindInstance * find(const char * str, size32_t from, size32_t len, bool needToKeepSearchString) const
    {
        CStrRegExprFindInstance * findInst = new CStrRegExprFindInstance(&regEx, str, from, len, needToKeepSearchString);
        return findInst;
    }
};

//---------------------------------------------------------------------------

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(const char * regExpr, bool isCaseSensitive)
{
    CCompiledStrRegExpr * expr = new CCompiledStrRegExpr(regExpr, isCaseSensitive);
    return expr;
}

ECLRTL_API void rtlDestroyCompiledStrRegExpr(ICompiledStrRegExpr * compiledExpr)
{
    if (compiledExpr)
        delete (CCompiledStrRegExpr*)compiledExpr;
}

ECLRTL_API void rtlDestroyStrRegExprFindInstance(IStrRegExprFindInstance * findInst)
{
    if (findInst)
        delete (CStrRegExprFindInstance*)findInst;
}

//---------------------------------------------------------------------------

// RegEx Compiler for unicode strings 

class CUStrRegExprFindInstance : implements IUStrRegExprFindInstance
{
private:
    bool            matched;
    RegexMatcher *  matcher;
    UnicodeString   sample;
    unsigned        matchedSize;

public:
    CUStrRegExprFindInstance(RegexMatcher * _matcher, const UChar * _str, size32_t _from, size32_t _len)
        : matcher(_matcher)
    {
        matched = false;

        sample.setTo(_str + _from, _len);
        matcher->reset(sample);
        matched = matcher->find();
        if (matched)
            matchedSize = (unsigned)matcher->groupCount() + 1;
    }

    //IUStrRegExprFindInstance

    bool found() const { return matched; }

    void getMatchX(unsigned & outlen, UChar * & out, unsigned n = 0) const
    {
        if(matched && (n < matchedSize))
        {
            assertex(matcher);
            UErrorCode uerr = U_ZERO_ERROR;
            int32_t start = n ? matcher->start(n, uerr) : matcher->start(uerr);
            int32_t end = n ? matcher->end(n, uerr) : matcher->end(uerr);
            outlen = end - start;
            out = (UChar *)rtlMalloc(outlen*2);
            sample.extract(start, outlen, out);
        }
        else
        {
            outlen = 0;
            out = NULL;
        }
    }

    UChar const * findvstr(unsigned outlen, UChar * out, unsigned n = 0)
    {
        if(matched && (n < matchedSize))
        {
            assertex(matcher);
            UErrorCode uerr = U_ZERO_ERROR;
            int32_t start = n ? matcher->start(n, uerr) : matcher->start(uerr);
            int32_t end = n ? matcher->end(n, uerr) : matcher->end(uerr);
            unsigned sublen = end - start;
            if(sublen >= outlen)
                sublen = outlen - 1;
            sample.extract(start, sublen, out);
            out[sublen] = 0;
        }
        else
        {
            out[0] = 0;
        }
        return out;
    }

};

//---------------------------------------------------------------------------

class CCompiledUStrRegExpr : implements ICompiledUStrRegExpr
{
private:
    RegexPattern *  pattern;
    RegexMatcher *  matcher;

public:
    CCompiledUStrRegExpr(const UChar * _UregExp, bool _isCaseSensitive = false) 
    {
        UErrorCode uerr = U_ZERO_ERROR;
        UParseError uperr;
        if (_isCaseSensitive)
            pattern = RegexPattern::compile(_UregExp, uperr, uerr);
        else
            pattern = RegexPattern::compile(_UregExp, UREGEX_CASE_INSENSITIVE, uperr, uerr);

        matcher = pattern->matcher(uerr);
        if (U_FAILURE(uerr))
        {
            char * expAscii;
            unsigned expAsciiLen;
            rtlUnicodeToEscapedStrX(expAsciiLen, expAscii, rtlUnicodeStrlen(_UregExp), _UregExp);
            StringBuffer msg;
            msg.append("Bad regular expression: ").append(u_errorName(uerr)).append(": ").append(expAsciiLen, expAscii);
            rtlFree(expAscii);
            delete matcher;
            delete pattern;
            matcher = 0;
            pattern = 0;
            rtlFail(0, msg.str());  //throws
        }
    }
    
    ~CCompiledUStrRegExpr()
    {
        if (matcher)
            delete matcher;
        if (pattern)
            delete pattern;
    }

    void replace(size32_t & outlen, UChar * & out, size32_t slen, const UChar * str, size32_t rlen, UChar const * replace) const
    {
        UnicodeString const src(str, slen);
        UErrorCode err = U_ZERO_ERROR;
        RegexMatcher * replacer = pattern->matcher(src, err);
        UnicodeString const fmt(replace, rlen);
        UnicodeString const tgt = replacer->replaceAll(fmt, err);
        outlen = tgt.length();
        out = (UChar *)rtlMalloc(outlen*2);
        tgt.extract(0, outlen, out);
        delete replacer;
    }

    IUStrRegExprFindInstance * find(const UChar * str, size32_t from, size32_t len) const
    {
        CUStrRegExprFindInstance * findInst = new CUStrRegExprFindInstance(matcher, str, from, len);
        return findInst;
    }
};

//---------------------------------------------------------------------------

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive)
{
    CCompiledUStrRegExpr * expr = new CCompiledUStrRegExpr(regExpr, isCaseSensitive);
    return expr;
}

ECLRTL_API void rtlDestroyCompiledUStrRegExpr(ICompiledUStrRegExpr * compiledExpr)
{
    if (compiledExpr)
        delete (CCompiledUStrRegExpr*)compiledExpr;
}

ECLRTL_API void rtlDestroyUStrRegExprFindInstance(IUStrRegExprFindInstance * findInst)
{
    if (findInst)
        delete (CUStrRegExprFindInstance*)findInst;
}

#else // _USE_BOOST_REGEX not set
ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(const char * regExpr, bool isCaseSensitive)
{
    UNIMPLEMENTED_X("Boost regex disabled");
}

ECLRTL_API void rtlDestroyCompiledStrRegExpr(ICompiledStrRegExpr * compiledExpr)
{
}

ECLRTL_API void rtlDestroyStrRegExprFindInstance(IStrRegExprFindInstance * findInst)
{
}

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive)
{
    UNIMPLEMENTED_X("Boost regex disabled");
}

ECLRTL_API void rtlDestroyCompiledUStrRegExpr(ICompiledUStrRegExpr * compiledExpr)
{
}

ECLRTL_API void rtlDestroyUStrRegExprFindInstance(IUStrRegExprFindInstance * findInst)
{
}
#endif
//---------------------------------------------------------------------------

ECLRTL_API int rtlQueryLocalFailCode(IException * e)
{
    return e->errorCode();
}

ECLRTL_API void rtlGetLocalFailMessage(size32_t & len, char * & text, IException * e, const char * tag)
{
    rtlExceptionExtract(len, text, e, tag);
}

ECLRTL_API void rtlFreeException(IException * e)
{
    e->Release();
}

//---------------------------------------------------------------------------

//Generally any calls to this function have also checked that the length(trim(str)) <= fieldLen, so exceptions should only occur if compareLen > fieldLen
//However, function can now also handle the exception case.
ECLRTL_API void rtlCreateRange(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str, byte fill, byte pad)
{
    //
    if (compareLen > fieldLen)
    {
        if ((int)compareLen >= 0) 
        {
            //x[1..m] = y, m is larger than fieldLen, so truncate to fieldLen
            compareLen = fieldLen;
        }
        else
            compareLen = 0;             // probably m[1..-1] or something silly
    }

    if (len > compareLen)
    {
        while ((len > compareLen) && (str[len-1] == pad))
            len--;

        //so change the search range to FF,FF,FF ..  00.00.00 which will then never match.
        if (len > compareLen)
        {
            compareLen = 0;
            fill = (fill == 0) ? 255 : 0;
        }
    }

    outlen = fieldLen;
    out = (char *)rtlMalloc(fieldLen);
    if (len >= compareLen)
        memcpy(out, str, compareLen);
    else
    {
        memcpy(out, str, len);
        memset(out+len, pad, compareLen-len);
    }
    memset(out + compareLen, fill, fieldLen-compareLen);
}


ECLRTL_API void rtlCreateStrRangeLow(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str)
{
    rtlCreateRange(outlen, out, fieldLen, compareLen, len, str, 0, ' ');
}

ECLRTL_API void rtlCreateStrRangeHigh(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str)
{
    rtlCreateRange(outlen, out, fieldLen, compareLen, len, str, 255, ' ');
}


ECLRTL_API void rtlCreateDataRangeLow(size32_t & outlen, void * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const void * str)
{
    rtlCreateRange(outlen, *(char * *)&out, fieldLen, compareLen, len, (const char *)str, 0, 0);
}

ECLRTL_API void rtlCreateDataRangeHigh(size32_t & outlen, void * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const void * str)
{
    rtlCreateRange(outlen, *(char * *)&out, fieldLen, compareLen, len, (const char *)str, 255, 0);
}


ECLRTL_API void rtlCreateRangeLow(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str)
{
    rtlCreateRange(outlen, out, fieldLen, compareLen, len, str, 0, ' ');
}

ECLRTL_API void rtlCreateRangeHigh(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str)
{
    rtlCreateRange(outlen, out, fieldLen, compareLen, len, str, 255, ' ');
}


ECLRTL_API void rtlCreateUnicodeRange(size32_t & outlen, UChar * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const UChar * str, byte fill)
{
    //Same as function above!
    if (compareLen > fieldLen)
    {
        if ((int)compareLen >= 0) 
        {
            //x[1..m] = y, m is larger than fieldLen, so truncate to fieldLen
            compareLen = fieldLen;
        }
        else
            compareLen = 0;             // probably m[1..-1] or something silly
    }

    if (len > compareLen)
    {
        while ((len > compareLen) && (str[len-1] == ' '))
            len--;

        //so change the search range to FF,FF,FF ..  00.00.00 which will then never match.
        if (len > compareLen)
        {
            compareLen = 0;
            fill = (fill == 0) ? 255 : 0;
        }
    }

    outlen = fieldLen;
    out = (UChar *)rtlMalloc(fieldLen*sizeof(UChar));
    if (len >= compareLen)
        memcpy(out, str, compareLen*sizeof(UChar));
    else
    {
        memcpy(out, str, len * sizeof(UChar));
        while (len != compareLen)
            out[len++] = ' ';
    }
    memset(out + compareLen, fill, (fieldLen-compareLen) * sizeof(UChar));
}

ECLRTL_API void rtlCreateUnicodeRangeLow(size32_t & outlen, UChar * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const UChar * str)
{
    rtlCreateUnicodeRange(outlen, out, fieldLen, compareLen, len, str, 0x00);
}


ECLRTL_API void rtlCreateUnicodeRangeHigh(size32_t & outlen, UChar * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const UChar * str)
{
    rtlCreateUnicodeRange(outlen, out, fieldLen, compareLen, len, str, 0xFF);
}

//---------------------------------------------------------------------------

ECLRTL_API unsigned rtlCountRows(size32_t len, const void * data, IRecordSize * rs)
{
    if (rs->isFixedSize())
        return len / rs->getFixedSize();
    unsigned count = 0;
    while (len)
    {
        size32_t thisLen = rs->getRecordSize(data);
        data = (byte *)data + thisLen;
        if (thisLen > len)
            throw MakeStringException(0, "Invalid raw data");
        len -= thisLen;
        count++;
    }
    return count;
}

//---------------------------------------------------------------------------

ECLRTL_API size32_t rtlCountToSize(unsigned count, const void * data, IRecordSize * rs)
{
    if (rs->isFixedSize())
        return count * rs->getFixedSize();
    unsigned size = 0;
    for (unsigned i=0;i<count;i++)
    {
        size32_t thisLen = rs->getRecordSize(data);
        data = (byte *)data + thisLen;
        size += thisLen;
    }
    return size;
}

//---------------------------------------------------------------------------

class rtlCodepageConverter
{
public:
    rtlCodepageConverter(char const * sourceName, char const * targetName, bool & failed) : uerr(U_ZERO_ERROR)
    {
        srccnv = ucnv_open(sourceName, &uerr);
        tgtcnv = ucnv_open(targetName, &uerr);
        tgtMaxRatio = ucnv_getMaxCharSize(tgtcnv);
        failed = U_FAILURE(uerr);
    }
    ~rtlCodepageConverter()
    {
        ucnv_close(srccnv);
        ucnv_close(tgtcnv);
    }
    void convertX(unsigned & targetLength, char * & target, unsigned sourceLength, char const * source, bool & failed, bool preflight)
    {
        //convert from source to utf-16: try to avoid preflighting by guessing upper bound
        //unicode length in UChars equal source length in chars if single byte encoding, and be less for multibyte
        UChar * ubuff = (UChar *)rtlMalloc(sourceLength*2);
        int32_t ulen = ucnv_toUChars(srccnv, ubuff, sourceLength, source, sourceLength, &uerr);
        if(ulen > (int32_t)sourceLength)
        {
            //okay, so our guess was wrong, and we have to reallocate
            free(ubuff);
            ubuff = (UChar *)rtlMalloc(ulen*2);
            ucnv_toUChars(srccnv, ubuff, ulen, source, sourceLength, &uerr);
        }
        if(preflight)
        {
            //convert from utf-16 to target: preflight to get buffer of exactly the right size
            UErrorCode uerr2 = uerr; //preflight has to use copy of error code, as it is considered an 'error'
            int32_t tlen = ucnv_fromUChars(tgtcnv, 0, 0, ubuff, ulen, &uerr2);
            target = (char *)rtlMalloc(tlen);
            targetLength = ucnv_fromUChars(tgtcnv, target, tlen, ubuff, ulen, &uerr);
        }
        else
        {
            //convert from utf-16 to target: avoid preflighting by allocating buffer of maximum size
            target = (char *)rtlMalloc(ulen*tgtMaxRatio);
            targetLength = ucnv_fromUChars(tgtcnv, target, ulen*tgtMaxRatio, ubuff, ulen, &uerr);
        }
        free(ubuff);
        failed = U_FAILURE(uerr);
    }
    unsigned convert(unsigned targetLength, char * target, unsigned sourceLength, char const * source, bool & failed)
    {
        char * tgtStart = target;
        ucnv_convertEx(tgtcnv, srccnv, &target, target+targetLength, &source, source+sourceLength, 0, 0, 0, 0, true, true, &uerr);
        int32_t ret = target-tgtStart;
        failed = U_FAILURE(uerr);
        return ret;
    }

private:
    UErrorCode uerr;
    UConverter * srccnv;
    UConverter * tgtcnv;
    int8_t tgtMaxRatio;
};

void * rtlOpenCodepageConverter(char const * sourceName, char const * targetName, bool & failed)
{
    return new rtlCodepageConverter(sourceName, targetName, failed);
}

void rtlCloseCodepageConverter(void * converter)
{
    delete ((rtlCodepageConverter *)converter);
}

void rtlCodepageConvertX(void * converter, unsigned & targetLength, char * & target, unsigned sourceLength, char const * source, bool & failed, bool preflight)
{
    ((rtlCodepageConverter *)converter)->convertX(targetLength, target, sourceLength, source, failed, preflight);
}

unsigned rtlCodepageConvert(void * converter, unsigned targetLength, char * target, unsigned sourceLength, char const * source, bool & failed)
{
    return ((rtlCodepageConverter *)converter)->convert(targetLength, target, sourceLength, source, failed);
}

//---------------------------------------------------------------------------

void appendUChar(MemoryBuffer & buff, char x)
{
    UChar c = x;
    buff.append(sizeof(c), &c);
}

void appendUChar(MemoryBuffer & buff, UChar c)
{
    buff.append(sizeof(c), &c);
}

void appendUStr(MemoryBuffer & x, const char * text)
{
    while (*text)
    {
        UChar c = *text++;
        x.append(sizeof(c), &c);
    }
}

ECLRTL_API void xmlDecodeStrX(size32_t & outLen, char * & out, size32_t inLen, const char * in)
{
    StringBuffer input(inLen, in);
    StringBuffer temp;
    decodeXML(input, temp, NULL, NULL, false);
    outLen = temp.length();
    out = temp.detach();
}

bool hasPrefix(const UChar * ustr, const UChar * end, const char * str, unsigned len)
{
    if ((unsigned)(end - ustr) < len)
        return false;
    while (len--)
    {
        if (*ustr++ != *str++)
            return false;
    }
    return true;
}

ECLRTL_API void xmlDecodeUStrX(size32_t & outLen, UChar * & out, size32_t inLen, const UChar * in)
{
    const UChar * cur = in;
    const UChar * end = in+inLen;
    MemoryBuffer ret;
    while (cur<end)
    {
        switch(*cur)
        {
        case '&':
            if(hasPrefix(cur+1, end, "amp;", 4))
            {
                cur += 4;
                appendUChar(ret, '&');
            }
            else if(hasPrefix(cur+1, end, "lt;", 3))
            {
                cur += 3;
                appendUChar(ret, '<');
            }
            else if(hasPrefix(cur+1, end, "gt;", 3))
            {
                cur += 3;
                appendUChar(ret, '>');
            }
            else if(hasPrefix(cur+1, end, "quot;", 5))
            {
                cur += 5;
                appendUChar(ret, '"');
            }
            else if(hasPrefix(cur+1, end, "apos;", 5))
            {
                cur += 5;
                appendUChar(ret, '\'');
            }
            else if(hasPrefix(cur+1, end, "nbsp;", 5))
            {
                cur += 5;
                appendUChar(ret, (UChar) 0xa0);
            }
            else if(hasPrefix(cur+1, end, "#", 1))
            {
                const UChar * saveCur = cur;
                bool error = true;  // until we have seen a digit...
                cur += 2;
                unsigned base = 10;
                if (*cur == 'x')
                {
                    base = 16;
                    cur++;
                }
                UChar value = 0;
                while (cur < end)
                {
                    unsigned digit;
                    UChar next = *cur;
                    if ((next >= '0') && (next <= '9'))
                        digit = next-'0';
                    else if ((next >= 'A') && (next <= 'F'))
                        digit = next-'A'+10;
                    else if ((next >= 'a') && (next <= 'f'))
                        digit = next-'a'+10;
                    else if (next==';')
                        break;
                    else
                        digit = base;
                    if (digit >= base)
                    {
                        error = true;
                        break;
                    }
                    error = false;
                    value = value * base + digit;
                    cur++;
                }
                if (error)
                {
                    appendUChar(ret, '&');
                    cur = saveCur;
                }
                else
                    appendUChar(ret, value);
            }
            else
                appendUChar(ret, *cur);
            break;
        default:
            appendUChar(ret, *cur);
            break;
        }
        cur++;
    }
    outLen = ret.length()/2;
    out = (UChar *)ret.detach();
}

ECLRTL_API void xmlEncodeStrX(size32_t & outLen, char * & out, size32_t inLen, const char * in, unsigned flags)
{
    StringBuffer temp;
    encodeXML(in, temp, flags, inLen, false);
    outLen = temp.length();
    out = temp.detach();
}


ECLRTL_API void xmlEncodeUStrX(size32_t & outLen, UChar * & out, size32_t inLen, const UChar * in, unsigned flags)
{
    const UChar * cur = in;
    MemoryBuffer ret;
    ret.ensureCapacity(inLen*2);
    while (inLen)
    {
        UChar next = *cur;
        switch(*cur)
        {
        case '&':
            appendUStr(ret, "&amp;");
            break;
        case '<':
            appendUStr(ret, "&lt;");
            break;
        case '>':
            appendUStr(ret, "&gt;");
            break;
        case '\"':
            appendUStr(ret, "&quot;");
            break;
        case '\'':
            appendUStr(ret, "&apos;");
            break;
        case ' ':
            appendUStr(ret, flags & ENCODE_SPACES?"&#32;":" ");
            break;
        case '\n':
            appendUStr(ret, flags & ENCODE_NEWLINES?"&#10;":"\n");
            break;
        case '\r':
            appendUStr(ret, flags & ENCODE_NEWLINES?"&#13;":"\r");
            break;
        case '\t':
            appendUStr(ret, flags & ENCODE_SPACES?"&#9;":"\t");
            break;
        default:
            appendUChar(ret, next);
            break;
        }
        inLen--;
        cur++;
    }
    outLen = ret.length()/2;
    out = (UChar *)ret.detach();
}

//---------------------------------------------------------------------------

#define STRUCTURED_EXCEPTION_TAG    "Error"
inline bool isStructuredMessage(const char * text, const char * tag)
{
    if (!text || text[0] != '<')
        return false;
    if (!tag)
        return true;
    size32_t lenTag = strlen(tag);
    if (memcmp(text+1,tag,lenTag) != 0)
        return false;
    if (text[lenTag+1] != '>')
        return false;
    return true;
}

inline bool isStructuredError(const char * text) { return isStructuredMessage(text, STRUCTURED_EXCEPTION_TAG); }

void rtlExtractTag(size32_t & outLen, char * & out, const char * text, const char * tag, const char * rootTag)
{
    if (!tag || !isStructuredMessage(text, rootTag))
    {
        if (text && (!tag || strcmp(tag, "text")==0))
           rtlStrToStrX(outLen, out, strlen(text), text);
        else
        {
            outLen = 0;
            out = NULL;
        }
    }
    else
    {
        StringBuffer startTag, endTag;
        startTag.append("<").append(tag).append(">");
        endTag.append("</").append(tag).append(">");

        const char * start = strstr(text, startTag.str());
        const char * end = strstr(text, endTag.str());
        if (start && end)
        {
            start += startTag.length();
            xmlDecodeStrX(outLen, out, end-start, start);
        }
        else
        {
            outLen = 0;
            out = NULL;
        }
    }
}

void rtlExceptionExtract(size32_t & outLen, char * & out, const char * text, const char * tag)
{
    if (!tag) tag = "text";
    rtlExtractTag(outLen, out, text, tag, STRUCTURED_EXCEPTION_TAG);
}


void rtlExceptionExtract(size32_t & outLen, char * & out, IException * e, const char * tag)
{
    StringBuffer text;
    e->errorMessage(text);
    rtlExceptionExtract(outLen, out, text.str(), tag);
}


void rtlAddExceptionTag(StringBuffer & errorText, const char * tag, const char * value)
{
    if (!isStructuredError(errorText.str()))
    {
        StringBuffer temp;
        temp.append("<" STRUCTURED_EXCEPTION_TAG "><text>");
        encodeXML(errorText.str(), temp, ENCODE_WHITESPACE, errorText.length(), false);
        temp.append("</text></" STRUCTURED_EXCEPTION_TAG ">");
        errorText.swapWith(temp);
    }

    StringBuffer temp;
    temp.append("<").append(tag).append(">");
    encodeXML(value, temp, ENCODE_WHITESPACE, (unsigned)-1, false);
    temp.append("</").append(tag).append(">");

    unsigned len = errorText.length();
    unsigned pos = len - strlen(STRUCTURED_EXCEPTION_TAG) - 3;
    errorText.insert(pos, temp);
}

//---------------------------------------------------------------------------

void rtlRowBuilder::forceAvailable(size32_t size)
{
    const size32_t chunkSize = 64;
    maxsize = (size + chunkSize-1) & ~(chunkSize-1);
    ptr = rtlRealloc(ptr, maxsize);
}

//---------------------------------------------------------------------------

inline unsigned numExtraBytesFromValue(unsigned __int64 first)
{
    if (first >= I64C(0x10000000))
        if (first >= I64C(0x40000000000))
            if (first >= I64C(0x2000000000000))
                if (first >= I64C(0x100000000000000))
                    return 8;
                else
                    return 7;
            else
                return 6;
        else
            if (first >= I64C(0x800000000))
                return 5;
            else
                return 4;
    else
        if (first >=     0x4000)
            if (first >= 0x200000)
                return 3;
            else
                return 2;
        else
            if (first >= 0x80)
                return 1;
            else
                return 0;
}

//An packed byte format, based on the unicode packing of utf-8.
//The number of top bits set in the leading byte indicates how many extra
//bytes follow (0..8).  It gives the same compression as using a top bit to
//indicate continuation, but seems to be quicker (and requires less look ahead).

/*
byte numExtraBytesFromFirstTable[256] = 
{ 
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 7, 8 
};
inline unsigned numExtraBytesFromFirst(byte first)
{
    return numExtraBytesFromFirstTable(first);
}
*/

//NB: This seems to be faster than using the table lookup above.  Probably affects the data cache less
inline unsigned numExtraBytesFromFirst(byte first)
{
    if (first >= 0xF0)
        if (first >= 0xFC)
            if (first >= 0xFE)
                if (first >= 0xFF)
                    return 8;
                else
                    return 7;
            else
                return 6;
        else
            if (first >= 0xF8)
                return 5;
            else
                return 4;
    else
        if (first >= 0xC0)
            if (first >= 0xE0)
                return 3;
            else
                return 2;
        else
            if (first >= 0x80)
                return 1;
            else
                return 0;
}

const static byte leadingValueMask[9] = { 0x7f, 0x3f, 0x1f, 0x0f, 0x07, 0x03, 0x01, 0x00, 0x00 };
const static byte leadingLengthMask[9] = { 0x00, 0x80, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC, 0xFE, 0xFF };

//maximum number of bytes for a packed value is size+1 bytes for size <=8 and last byte being fully used.
unsigned __int64 rtlGetPackedUnsigned(const void * _ptr)
{
    const byte * ptr = (const byte *)_ptr;
    byte first = *ptr++;
    unsigned numExtra = numExtraBytesFromFirst(first);
    unsigned __int64 value = first & leadingValueMask[numExtra];

    //Loop unrolling has a negligable effect
    while (numExtra--)
        value = (value << 8) | *ptr++;
    return value;
}


void rtlSetPackedUnsigned(void * _ptr, unsigned __int64 value)
{
    byte * ptr = (byte *)_ptr;
    unsigned numExtra = numExtraBytesFromValue(value);
    byte firstMask = leadingLengthMask[numExtra];
    while (numExtra)
    {
        ptr[numExtra--] = (byte)value;
        value >>= 8;
    }
    ptr[0] = (byte)value | firstMask;
}


size32_t rtlGetPackedSize(const void * ptr)
{
    return numExtraBytesFromFirst(*(byte*)ptr)+1;
}

size32_t rtlGetPackedSizeFromFirst(byte first)
{
    return numExtraBytesFromFirst(first)+1;
}




//Store signed by moving the sign to the bottom bit, and inverting if negative.
//so small positive and negative numbers are stored compactly.
__int64 rtlGetPackedSigned(const void * ptr)
{
    unsigned __int64 value = rtlGetPackedUnsigned(ptr);
    unsigned __int64 shifted = (value >> 1);
    return (__int64)((value & 1) ? ~shifted : shifted);
}

void rtlSetPackedSigned(void * ptr, __int64 value)
{
    unsigned __int64 storeValue;
    if (value < 0)
        storeValue = (~value << 1) | 1;
    else
        storeValue = value << 1;
    rtlSetPackedUnsigned(ptr, storeValue);
}

IAtom * rtlCreateFieldNameAtom(const char * name)
{
    return createAtom(name);
}

void rtlBase64Encode(size32_t & tlen, char * & tgt, size32_t slen, const void * src)
{
    tlen = 0;
    tgt = NULL;
    if (slen)
    {
        StringBuffer out;
        JBASE64_Encode(src, slen, out);
        tlen = out.length();
        if (tlen)
        {
            char * data  = (char *) rtlMalloc(tlen);
            out.getChars(0, tlen, data);
            tgt = data;
        }
    }
}

void rtlBase64Decode(size32_t & tlen, void * & tgt, size32_t slen, const char * src)
{
    tlen = 0;
    if (slen)
    {
        StringBuffer out;
        if (JBASE64_Decode(slen, src, out))
            tlen = out.length();
        if (tlen)
        {
            char * data = (char *) rtlMalloc(tlen);
            out.getChars(0, tlen, data);
            tgt = (void *) data;
        }
    }
}


//---------------------------------------------------------------------------

void RtlCInterface::Link() const            { atomic_inc(&xxcount); }
bool RtlCInterface::Release(void) const
{
    if (atomic_dec_and_test(&xxcount))
    {
        delete this;
        return true;
    }
    return false;
}

//---------------------------------------------------------------------------

class RtlRowStream : implements IRowStream, public RtlCInterface
{
public:
    RtlRowStream(size32_t _count, byte * * _rowset) : count(_count), rowset(_rowset)
    {
        rtlLinkRowset(rowset);
        cur = 0;
    }
    ~RtlRowStream()
    {
        rtlReleaseRowset(count, rowset);
    }
    RTLIMPLEMENT_IINTERFACE

    virtual const void *nextRow()
    {
        if (cur >= count)
            return NULL;
        byte * ret = rowset[cur];
        cur++;
        rtlLinkRow(ret);
        return ret;
    }
    virtual void stop()
    {
        cur = count;
    }

protected:
    size32_t cur;
    size32_t count;
    byte * * rowset;

};

ECLRTL_API IRowStream * createRowStream(size32_t count, byte * * rowset)
{
    return new RtlRowStream(count, rowset);
}



#if 0
void PrintExtract(StringBuffer & s, const char * tag)
{
    size32_t outLen;
    char * out = NULL;
    rtlExceptionExtract(outLen, out, s.str(), tag);
    PrintLog("%s = %.*s", tag, outLen, out);
    rtlFree(out);
}

void testStructuredExceptions()
{
    StringBuffer s;
    s.append("This<is>some text");
    PrintExtract(s, NULL);
    PrintExtract(s, "text");
    PrintExtract(s, "is");
    rtlAddExceptionTag(s, "location", "192.168.12.1");
    PrintExtract(s, NULL);
    PrintExtract(s, "text");
    PrintExtract(s, "is");
    PrintExtract(s, "location");
    rtlAddExceptionTag(s, "author", "gavin");
    PrintExtract(s, NULL);
    PrintExtract(s, "text");
    PrintExtract(s, "is");
    PrintExtract(s, "location");
    PrintExtract(s, "author");
    PrintLog("%s", s.str());
}

static void testPackedUnsigned()
{
    unsigned __int64 values[] = { 0, 1, 2, 10, 127, 128, 16383, 16384, 32767, 32768, 0xffffff, 0x7fffffff, 0xffffffff,
        I64C(0xffffffffffffff), I64C(0x100000000000000), I64C(0x7fffffffffffffff), I64C(0xffffffffffffffff) };
    unsigned numBytes[] = { 1, 1, 1, 1, 1, 2, 2, 3, 3, 3, 4, 5, 5, 8, 9, 9, 9 };
    unsigned numValues = _elements_in(values);
    byte temp[9];
    for (unsigned i = 0; i < numValues; i++)
    {
        rtlSetPackedUnsigned(temp, values[i]);
        assertex(rtlGetPackedSize(temp) == numBytes[i]);
        assertex(rtlGetPackedUnsigned(temp) == values[i]);
    }
    for (unsigned j= 0; j < 2000000; j++)
    {
        unsigned __int64 value = I64C(1) << (rtlRandom() & 63);
//      unsigned value = rtlRandom();
        rtlSetPackedUnsigned(temp, value);
        assertex(rtlGetPackedSize(temp) == numExtraBytesFromValue(value)+1);
        assertex(rtlGetPackedUnsigned(temp) == value);
    }
    for (unsigned k= 0; k < 63; k++)
    {
        unsigned __int64 value1 = I64C(1) << k;
        rtlSetPackedUnsigned(temp, value1);
        assertex(rtlGetPackedSize(temp) == numExtraBytesFromValue(value1)+1);
        assertex(rtlGetPackedUnsigned(temp) == value1);
        unsigned __int64 value2 = value1-1;
        rtlSetPackedUnsigned(temp, value2);
        assertex(rtlGetPackedSize(temp) == numExtraBytesFromValue(value2)+1);
        assertex(rtlGetPackedUnsigned(temp) == value2);
    }
}


static void testPackedSigned()
{
    __int64 values[] =    { 0, 1, -2, 10, 63, 64, -64, -65, 8191, 8192, 0x3fffffff,
        I64C(0x7fffffffffffff), I64C(0x80000000000000), I64C(0x7fffffffffffffff), I64C(0x8000000000000000) };
    unsigned numBytes[] = { 1, 1,  1,  1,  1,  2,   1,   2,    2,    3,          5,
        8,                      9,                      9,                        9 };

    unsigned numValues = _elements_in(values);
    byte temp[9];
    for (unsigned i = 0; i < numValues; i++)
    {
        rtlSetPackedSigned(temp, values[i]);
        assertex(rtlGetPackedSize(temp) == numBytes[i]);
        assertex(rtlGetPackedSigned(temp) == values[i]);
    }
}

#endif

void ensureRtlLoaded()
{
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class EclRtlTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( EclRtlTests );
        CPPUNIT_TEST(RegexTest);
        CPPUNIT_TEST(MultiRegexTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    void RegexTest()
    {
        rtlCompiledStrRegex r;
        size32_t outlen;
        char * out = NULL;
        r.setPattern("([A-Z]+)[ ]?'(S) ", true);
        r->replace(outlen, out, 7, "ABC'S  ", 5, "$1$2 ");
        ASSERT(outlen==6);
        ASSERT(out != NULL);
        ASSERT(memcmp(out, "ABCS  ", outlen)==0);
        rtlFree(out);
    }

    void MultiRegexTest()
    {
        class RegexTestThread : public Thread
        {
            virtual int run()
            {
                for (int i = 0; i < 100000; i++)
                {
                    rtlCompiledStrRegex r;
                    size32_t outlen;
                    char * out = NULL;
                    r.setPattern("([A-Z]+)[ ]?'(S) ", true);
                    r->replace(outlen, out, 7, "ABC'S  ", 5, "$1$2 ");
                    ASSERT(outlen==6);
                    ASSERT(out != NULL);
                    ASSERT(memcmp(out, "ABCS  ", outlen)==0);
                    rtlFree(out);
                }
                return 0;
            }
        };
        RegexTestThread t1;
        RegexTestThread t2;
        RegexTestThread t3;
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( EclRtlTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( EclRtlTests, "EclRtlTests" );

#endif
