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
#include <math.h>
#include <stdio.h>
#include <atomic>
#include <algorithm>

#include "jmisc.hpp"
#include "jlib.hpp"
#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
#include "rtlrecord.hpp"
#include "rtlkey.hpp"
#include "nbcd.hpp"
#include "rtlqstr.ipp"

static const char * queryXPath(const RtlFieldInfo * field)
{
    const char * xpath = field->xpath;
    if (xpath)
    {
        const char * sep = strchr(xpath, xpathCompoundSeparatorChar);
        if (!sep)
            return xpath;
        return sep+1;
    }
    return field->name;
}

static const char * queryScalarXPath(const RtlFieldInfo * field)
{
    if (field->hasNonScalarXpath())
        return field->name;
    return queryXPath(field);
}

static bool hasOuterXPath(const RtlFieldInfo * field)
{
    const char * xpath = field->xpath;
    assertex(xpath);
    return (*xpath != xpathCompoundSeparatorChar);
}

static void queryNestedOuterXPath(StringAttr & ret, const RtlFieldInfo * field)
{
    const char * xpath = field->xpath;
    assertex(xpath);
    const char * sep = strchr(xpath, xpathCompoundSeparatorChar);
    assertex(sep);
    ret.set(xpath, (size32_t)(sep-xpath));
}

inline const char * queryName(const RtlFieldInfo * field) { return field ? field->name : nullptr; }

//-------------------------------------------------------------------------------------------------------------------

static bool incrementBuffer(byte *buf, size32_t size)
{
    int i = size;
    while (i--)
    {
        buf[i]++;
        if (buf[i]!=0)
            return true;
    }
    return false;
}

static bool decrementBuffer(byte *buf, size32_t size)
{
    int i = size;
    while (i--)
    {
        buf[i]--;
        if (buf[i]!=0xff)
            return true;
    }
    return false;
}

//-------------------------------------------------------------------------------------------------------------------

class DummyFieldProcessor : public CInterfaceOf<IFieldProcessor>
{
public:
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field) {}
    virtual void processBool(bool value, const RtlFieldInfo * field) {}
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field) {}
    virtual void processInt(__int64 value, const RtlFieldInfo * field) {}
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field) {}
    virtual void processReal(double value, const RtlFieldInfo * field) {}
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field) {}
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field) {}
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field) {}
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field) {}
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field) {}

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data) { return false; }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows) { return true; }
    virtual bool processBeginRow(const RtlFieldInfo * field) { return true; }
    virtual void processEndSet(const RtlFieldInfo * field) {}
    virtual void processEndDataset(const RtlFieldInfo * field) {}
    virtual void processEndRow(const RtlFieldInfo * field) {}
};

//-------------------------------------------------------------------------------------------------------------------

size32_t ECLRTL_API getMinSize(const RtlFieldInfo * const * fields)
{
    size32_t minSize = 0;
    for(;;)
    {
        const RtlFieldInfo * cur = *fields;
        if (!cur)
            return minSize;
        minSize += cur->type->getMinSize();
        fields++;
    }
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlTypeInfoBase::getMinSize() const
{
    return length;
}

size32_t RtlTypeInfoBase::size(const byte * self, const byte * selfrow) const 
{
    return length; 
}

size32_t RtlTypeInfoBase::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const 
{
    rtlFailUnexpected();
    return 0;
}

size32_t RtlTypeInfoBase::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & out) const 
{
    rtlFailUnexpected();
    return 0;
}

size32_t RtlTypeInfoBase::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    rtlFailUnexpected();
    return 0;
}

size32_t RtlTypeInfoBase::buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const
{
    if (field->initializer && !isVirtualInitializer(field->initializer))
    {
        size32_t initSize = size((const byte *) field->initializer, nullptr);
        builder.ensureCapacity(offset+initSize, queryName(field));
        memcpy(builder.getSelf()+offset, field->initializer, initSize);
        return offset+initSize;
    }
    else
    {
        // This code covers a lot (though not all) of the derived cases
        size32_t initSize = getMinSize();
        builder.ensureCapacity(offset+initSize, queryName(field));
        memset(builder.getSelf() + offset, 0, initSize);
        return offset + initSize;
    }
}

size32_t RtlTypeInfoBase::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    size32_t newLen;
    rtlDataAttr temp;
    rtlInt8ToStrX(newLen, temp.refstr(), val);
    return buildString(builder, offset, field, newLen, temp.getstr());
}

size32_t RtlTypeInfoBase::buildReal(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, double val) const
{
    size32_t newLen;
    rtlDataAttr temp;
    rtlRealToStrX(newLen, temp.refstr(), val);
    return buildString(builder, offset, field, newLen, temp.getstr());
}

double RtlTypeInfoBase::getReal(const void * ptr) const
{
    size32_t len;
    rtlDataAttr value;
    getString(len, value.refstr(), ptr);
    return rtlStrToReal(len, value.getstr());
}

bool RtlTypeInfoBase::equivalent(const RtlTypeInfo *to) const
{
    if (to==this)
        return true;
    if (!to)
        return false;
    if (to->length != length || to->fieldType != fieldType)
        return false;
    if (queryLocale())
    {
        // do we permit a locale difference?
    }
    auto child = queryChildType();
    if (child && !child->equivalent(to->queryChildType()))
        return false;
    auto fields = queryFields();
    if (fields)
    {
        auto tofields = to->queryFields();
        if (!tofields)
            return false; // Should never happen
        for (unsigned idx = 0; fields[idx]; idx++)
        {
            if (!fields[idx]->equivalent(tofields[idx]))
                return false;
        }
    }
    return true;
}

static bool strequivalent(const char *name1, const char *name2)
{
    if (name1)
        return name2 && streq(name1, name2);
    else
        return name2==nullptr;
}

bool RtlFieldInfo::equivalent(const RtlFieldInfo *to) const
{
    if (to==this)
        return true;
    if (!to)
        return false;
    if (!strequivalent(name, to->name))
        return false;
    if (!strequivalent(xpath, to->xpath))
        return false;
    if (!type->equivalent(to->type))
        return false;
    // Initializer differences can be ignored
    if (flags != to->flags)
        return false;
    return true;
}

const char * RtlTypeInfoBase::queryLocale() const 
{
    return NULL; 
}

bool RtlTypeInfoBase::isScalar() const
{
    return true;
}

const RtlFieldInfo * const * RtlTypeInfoBase::queryFields() const 
{
    return NULL; 
}

const RtlTypeInfo * RtlTypeInfoBase::queryChildType() const 
{
    return NULL; 
}

const IFieldFilter * RtlTypeInfoBase::queryFilter() const
{
    return nullptr;
}

size32_t RtlTypeInfoBase::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    size32_t thisSize = size(nullptr, nullptr);
    byte * dest = builder.ensureCapacity(offset + thisSize, nullptr) + offset;
    in.read(thisSize, dest);
    return offset + thisSize;
}

void RtlTypeInfoBase::readAhead(IRowPrefetcherSource & in) const
{
    size32_t thisSize = size(nullptr, nullptr);
    in.skip(thisSize);
}

size32_t RtlTypeInfoBase::buildUtf8ViaString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t newLen;
    rtlDataAttr temp;
    rtlUtf8ToStrX(newLen, temp.refstr(), len, value);
    return buildString(builder, offset, field, newLen, temp.getstr());
}

void RtlTypeInfoBase::getUtf8ViaString(size32_t & resultLen, char * & result, const void * ptr) const
{
    size32_t newLen;
    rtlDataAttr temp;
    getString(newLen, temp.refstr(), ptr);
    rtlStrToUtf8X(resultLen, result, newLen, temp.getstr());
}

int RtlTypeInfoBase::compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const
{
    throwUnexpected();
}

void RtlTypeInfoBase::setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const
{
    byte *dst = (byte *) buffer;
    size32_t minSize = getMinSize();
    if (value)
    {
        assertex(isFixedSize() || (inclusive && (subLength == MatchFullString)));
        size32_t copySize;
        if (subLength != MatchFullString)
        {
            if (isFixedSize())
            {
                //For a substring comparison, clear the bytes after subLength - eventually they should never be compared
                //but currently the index code uses memcmp() so they need to be initialised
                unsigned copyLen = std::min(length, subLength);
                copySize = copyLen;
                memcpy(dst, value, copySize);
                memset(dst+copySize, fill, minSize-copySize);
            }
            else
            {
                assertex(inclusive);
                size32_t copyLen = std::min(rtlReadSize32t(value), subLength);
                copySize = copyLen;
                *(size32_t *)buffer = copyLen;
                memcpy(dst + sizeof(size32_t), value + sizeof(size32_t), copySize);
            }
        }
        else
        {
            copySize = size(value, nullptr);
            memcpy(dst, value, copySize);
        }
        if (!inclusive)
        {
            if (fill == 0)
                incrementBuffer(dst, copySize);
            else
                decrementBuffer(dst, copySize);
        }
    }
    else
    {
        //Generally cannot work for variable length fields - since always possible to create a larger value
        //Default does not work for real types, or variable length signed integers
        assertex((fill == 0) || isFixedSize());
        memset(dst, fill, minSize);
    }
}

void RtlTypeInfoBase::setLowBound(void * buffer, const byte * value, size32_t subLength, bool inclusive) const
{
    setBound(buffer, value, subLength, 0, inclusive);
}

void RtlTypeInfoBase::setHighBound(void * buffer, const byte * value, size32_t subLength, bool inclusive) const
{
    setBound(buffer, value, subLength, 0xff, inclusive);
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlBoolTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    builder.ensureCapacity(sizeof(bool)+offset, queryName(field));
    bool val = source.getBooleanResult(field);
    * (bool *) (builder.getSelf() + offset) = val;
    offset += sizeof(bool);
    return offset;
}

size32_t RtlBoolTypeInfo::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    builder.ensureCapacity(sizeof(bool)+offset, queryName(field));
    * (bool *) (builder.getSelf() + offset) = val != 0;
    offset += sizeof(bool);
    return offset;

}

size32_t RtlBoolTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    return buildInt(builder, offset, field, rtlStrToBool(len, value));
}

size32_t RtlBoolTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t size = rtlUtf8Length(len, value);
    return buildInt(builder, offset, field, rtlStrToBool(size, value));
}

size32_t RtlBoolTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    target.processBool(*(const bool *)self, field);
    return sizeof(bool);
}

size32_t RtlBoolTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    target.outputBool(*(const bool *)self, queryScalarXPath(field));
    return sizeof(bool);
}

void RtlBoolTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    const bool * cast = static_cast<const bool *>(ptr);
    rtlBoolToStrX(resultLen, result, *cast);
}

void RtlBoolTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    getString(resultLen, result, ptr);
}

__int64 RtlBoolTypeInfo::getInt(const void * ptr) const
{
    const bool * cast = static_cast<const bool *>(ptr);
    return (__int64)*cast;
}

bool RtlBoolTypeInfo::getBool(const void * ptr) const
{
    const bool * cast = static_cast<const bool *>(ptr);
    return *cast;
}

int RtlBoolTypeInfo::compare(const byte * left, const byte * right) const
{
    bool leftValue = getBool(left);
    bool rightValue = getBool (right);
    return (!leftValue && rightValue) ? -1 : (leftValue && !rightValue) ? +1 : 0;
}

unsigned RtlBoolTypeInfo::hash(const byte *self, unsigned inhash) const
{
    __int64 val = getInt(self);
    return rtlHash32Data8(&val, inhash);
}

//-------------------------------------------------------------------------------------------------------------------

double RtlRealTypeInfo::value(const void * self) const
{
    if (length == 4)
        return *(const float *)self;
    return *(const double *)self;
}

size32_t RtlRealTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    double val = source.getRealResult(field);
    return buildReal(builder, offset, field, val);
}

size32_t RtlRealTypeInfo::buildReal(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, double val) const
{
    byte *dest = builder.ensureCapacity(length+offset, queryName(field)) + offset;
    if (length == 4)
        *(float *) dest = (float) val;
    else
        *(double *) dest = val;
    offset += length;
    return offset;
}

size32_t RtlRealTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    return buildReal(builder, offset, field, rtlStrToReal(len, value));
}

size32_t RtlRealTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t size = rtlUtf8Length(len, value);
    return buildReal(builder, offset, field, rtlStrToReal(size, value));
}

size32_t RtlRealTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    target.processReal(value(self), field);
    return length;
}

size32_t RtlRealTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    target.outputReal(value(self), queryScalarXPath(field));
    return length;
}

void RtlRealTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    double num = value(ptr);
    rtlRealToStrX(resultLen, result,  num);
}

void RtlRealTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    getString(resultLen, result, ptr);
}

__int64 RtlRealTypeInfo::getInt(const void * ptr) const
{
    double num = value(ptr);
    return (__int64)num;
}

double RtlRealTypeInfo::getReal(const void * ptr) const
{
    return value(ptr);
}

int RtlRealTypeInfo::compare(const byte * left, const byte * right) const
{
    double leftValue = getReal(left);
    double rightValue = getReal(right);
    return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
}

unsigned RtlRealTypeInfo::hash(const byte *self, unsigned inhash) const
{
    double val = getReal(self);
    return rtlHash32Data8(&val, inhash);
}

void RtlRealTypeInfo::setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const
{
    assertex(subLength == MatchFullString);
    double dvalue;
    float fvalue;
    if (value)
    {
        assertex(inclusive);
    }
    else
    {
        dvalue = rtlCreateRealInf();
        if (fill == 0)
            dvalue = -dvalue;
        if (length == 4)
        {
            fvalue = dvalue;
            value = reinterpret_cast<const byte *>(&fvalue);
        }
        else
            value = reinterpret_cast<const byte *>(&dvalue);
    }
    memcpy(buffer, value, length);
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlIntTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    builder.ensureCapacity(length+offset, queryName(field));
    __int64 val = isUnsigned() ? (__int64) source.getUnsignedResult(field) : source.getSignedResult(field);
    rtlWriteInt(builder.getSelf() + offset, val, length);
    offset += length;
    return offset;
}

size32_t RtlIntTypeInfo::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    builder.ensureCapacity(length+offset, queryName(field));
    rtlWriteInt(builder.getSelf() + offset, val, length);
    offset += length;
    return offset;
}

size32_t RtlIntTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlIntTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t size = rtlUtf8Length(len, value);
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlIntTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isUnsigned())
        target.processUInt(rtlReadUInt(self, length), field);
    else
        target.processInt(rtlReadInt(self, length), field);
    return length;
}

size32_t RtlIntTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    if (isUnsigned())
        target.outputUInt(rtlReadUInt(self, length), length, queryScalarXPath(field));
    else
        target.outputInt(rtlReadInt(self, length), length, queryScalarXPath(field));
    return length;
}

void RtlIntTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isUnsigned())
        rtlUInt8ToStrX(resultLen, result,  rtlReadUInt(ptr, length));
    else
        rtlInt8ToStrX(resultLen, result, rtlReadInt(ptr, length));
}

void RtlIntTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    getString(resultLen, result, ptr);
}

__int64 RtlIntTypeInfo::getInt(const void * ptr) const
{
    if (isUnsigned())
        return rtlReadUInt(ptr, length);
    else
        return rtlReadInt(ptr, length);
}

double RtlIntTypeInfo::getReal(const void * ptr) const
{
    if (isUnsigned())
        return (double) rtlReadUInt(ptr, length);
    else
        return (double) rtlReadInt(ptr, length);
}

int RtlIntTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isUnsigned())
    {
        unsigned __int64 leftValue = rtlReadUInt(left, length);
        unsigned __int64 rightValue = rtlReadUInt(right, length);
        return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
    }
    else
    {
        __int64 leftValue = rtlReadInt(left, length);
        __int64 rightValue = rtlReadInt(right, length);
        return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
    }
}

bool RtlIntTypeInfo::canMemCmp() const
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return false;
#else
    return isUnsigned();
#endif
}

unsigned RtlIntTypeInfo::hash(const byte *self, unsigned inhash) const
{
    __int64 val = getInt(self);
    return rtlHash32Data8(&val, inhash);
}


bool RtlIntTypeInfo::canTruncate() const
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return true;
#else
    return false;
#endif
}

bool RtlIntTypeInfo::canExtend(char &fillChar) const
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    fillChar = 0;
    return true;
#else
    return false;
#endif
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlBlobTypeInfo::getMinSize() const
{
    return sizeof(offset_t);
}

size32_t RtlBlobTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    throwUnexpected();  // This is only expected to be used for reading at present
}

size32_t RtlBlobTypeInfo::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    throwUnexpected();  // This is only expected to be used for reading at present
}

size32_t RtlBlobTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    throwUnexpected();  // This is only expected to be used for reading at present
}

size32_t RtlBlobTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    throwUnexpected();  // This is only expected to be used for reading at present
}

size32_t RtlBlobTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    UNIMPLEMENTED;
}

size32_t RtlBlobTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    UNIMPLEMENTED;
}

void RtlBlobTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    UNIMPLEMENTED;
}

void RtlBlobTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    UNIMPLEMENTED;
}

__int64 RtlBlobTypeInfo::getInt(const void * ptr) const
{
    return *(offset_t *) ptr;
}

double RtlBlobTypeInfo::getReal(const void * ptr) const
{
    UNIMPLEMENTED;
}

bool RtlBlobTypeInfo::canTruncate() const
{
    return false;
}

bool RtlBlobTypeInfo::canExtend(char &fillChar) const
{
    return false;
}

int RtlBlobTypeInfo::compare(const byte * left, const byte * right) const
{
    UNIMPLEMENTED;
}
unsigned RtlBlobTypeInfo::hash(const byte *self, unsigned inhash) const
{
    UNIMPLEMENTED;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlSwapIntTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    builder.ensureCapacity(length+offset, queryName(field));
    __int64 val = isUnsigned() ? (__int64) source.getUnsignedResult(field) : source.getSignedResult(field);
    // NOTE - we assume that the value returned from the source is NOT already a swapped int - source doesn't know that we are going to store it swapped
    rtlWriteSwapInt(builder.getSelf() + offset, val, length);
    offset += length;
    return offset;
}

size32_t RtlSwapIntTypeInfo::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    builder.ensureCapacity(length+offset, queryName(field));
    rtlWriteSwapInt(builder.getSelf() + offset, val, length);
    offset += length;
    return offset;
}

size32_t RtlSwapIntTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlSwapIntTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t size = rtlUtf8Length(len, value);
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlSwapIntTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isUnsigned())
        target.processUInt(rtlReadSwapUInt(self, length), field);
    else
        target.processInt(rtlReadSwapInt(self, length), field);
    return length;
}

size32_t RtlSwapIntTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    if (isUnsigned())
        target.outputUInt(rtlReadSwapUInt(self, length), length, queryScalarXPath(field));
    else
        target.outputInt(rtlReadSwapInt(self, length), length, queryScalarXPath(field));
    return length;
}

void RtlSwapIntTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isUnsigned())
        rtlUInt8ToStrX(resultLen, result,  rtlReadSwapUInt(ptr, length));
    else
        rtlInt8ToStrX(resultLen, result,  rtlReadSwapInt(ptr, length));
}

void RtlSwapIntTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    getString(resultLen, result, ptr);
}

__int64 RtlSwapIntTypeInfo::getInt(const void * ptr) const
{
    if (isUnsigned())
        return rtlReadSwapUInt(ptr, length);
    else
        return rtlReadSwapInt(ptr, length);
}

double RtlSwapIntTypeInfo::getReal(const void * ptr) const
{
    if (isUnsigned())
        return (double) rtlReadSwapUInt(ptr, length);
    else
        return (double) (rtlReadSwapInt(ptr, length));
}

int RtlSwapIntTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isUnsigned())
    {
        unsigned __int64 leftValue = rtlReadSwapUInt(left, length);
        unsigned __int64 rightValue = rtlReadSwapUInt(right, length);
        return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
    }
    else
    {
        __int64 leftValue = rtlReadSwapInt(left, length);
        __int64 rightValue = rtlReadSwapInt(right, length);
        return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
    }
}

bool RtlSwapIntTypeInfo::canMemCmp() const
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return isUnsigned();
#else
    return false;
#endif
}

unsigned RtlSwapIntTypeInfo::hash(const byte *self, unsigned inhash) const
{
    __int64 val = getInt(self);
    return rtlHash32Data8(&val, inhash);
}


bool RtlSwapIntTypeInfo::canTruncate() const
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return false;
#else
    return true;
#endif
}

bool RtlSwapIntTypeInfo::canExtend(char &fillChar) const
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return false;
#else
    fillChar = 0;
    return true;
#endif
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlKeyedIntTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    __int64 val = isUnsigned() ? (__int64) source.getUnsignedResult(field) : source.getSignedResult(field);
    return buildInt(builder, offset, field, val);
}

size32_t RtlKeyedIntTypeInfo::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    builder.ensureCapacity(length+offset, queryName(field));
    if (isUnsigned())
#if __BYTE_ORDER == __LITTLE_ENDIAN
        rtlWriteSwapInt(builder.getSelf() + offset, val, length);
#else
        rtlWriteInt(builder.getSelf() + offset, val, length);
#endif
    else
#if __BYTE_ORDER == __LITTLE_ENDIAN
        rtlWriteSwapInt(builder.getSelf() + offset, addBias(val, length), length);
#else
        rtlWriteInt(builder.getSelf() + offset, addBias(val, length), length);
#endif
    offset += length;
    return offset;
}

size32_t RtlKeyedIntTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlKeyedIntTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t size = rtlUtf8Length(len, value);
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlKeyedIntTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isUnsigned())
        target.processUInt(getUInt(self), field);
    else
        target.processInt(getInt(self), field);
    return length;
}

size32_t RtlKeyedIntTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    if (isUnsigned())
        target.outputUInt(getUInt(self), length, queryScalarXPath(field));
    else
        target.outputInt(getInt(self), length, queryScalarXPath(field));
    return length;
}

void RtlKeyedIntTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isUnsigned())
        rtlUInt8ToStrX(resultLen, result, getUInt(ptr));
    else
        rtlInt8ToStrX(resultLen, result, getInt(ptr));
}

void RtlKeyedIntTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    getString(resultLen, result, ptr);
}

__int64 RtlKeyedIntTypeInfo::getInt(const void * ptr) const
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    if (isUnsigned())
        return rtlReadSwapUInt(ptr, length);
    else
        return removeBias(rtlReadSwapUInt(ptr, length), length);
#else
    if (isUnsigned())
        return rtlReadUInt(ptr, length);
    else
        return removeBias(rtlReadInt(ptr, length), length);
#endif
}

double RtlKeyedIntTypeInfo::getReal(const void * ptr) const
{
    if (isUnsigned())
        return (double) getUInt(ptr);
    else
        return (double) getInt(ptr);
}

int RtlKeyedIntTypeInfo::compare(const byte * left, const byte * right) const
{
    // The whole point of biased ints is that we can do this:
    return memcmp(left, right, length);
}

unsigned RtlKeyedIntTypeInfo::hash(const byte *self, unsigned inhash) const
{
    __int64 val = getInt(self);
    return rtlHash32Data8(&val, inhash);
}

unsigned __int64 RtlKeyedIntTypeInfo::addBias(__int64 value, unsigned length)
{
    return value + ((unsigned __int64)1 << (length*8-1));
}

__int64 RtlKeyedIntTypeInfo::removeBias(unsigned __int64 value, unsigned length)
{
    return value - ((unsigned __int64)1 << (length*8-1));
}

const RtlTypeInfo * RtlKeyedIntTypeInfo::queryChildType() const
{
    return child;
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlPackedIntTypeInfo::getMinSize() const
{
    return 1;
}

size32_t RtlPackedIntTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    return rtlGetPackedSize(self); 
}

size32_t RtlPackedIntTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    __int64 value = isUnsigned() ? (__int64) source.getUnsignedResult(field) : source.getSignedResult(field);
    return buildInt(builder, offset, field, value);
}

size32_t RtlPackedIntTypeInfo::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    size32_t sizeInBytes = rtlGetPackedSize(&val);
    builder.ensureCapacity(sizeInBytes+offset, queryName(field));
    rtlSetPackedUnsigned(builder.getSelf() + offset, val);
    offset += sizeInBytes;
    return offset;
}


size32_t RtlPackedIntTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlPackedIntTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t size = rtlUtf8Length(len, value);
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlPackedIntTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isUnsigned())
        target.processUInt(rtlGetPackedUnsigned(self), field);
    else
        target.processInt(rtlGetPackedSigned(self), field);
    return rtlGetPackedSize(self); 
}

size32_t RtlPackedIntTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    size32_t fieldsize = rtlGetPackedSize(self);
    if (isUnsigned())
        target.outputUInt(rtlGetPackedUnsigned(self), fieldsize, queryScalarXPath(field));
    else
        target.outputInt(rtlGetPackedSigned(self), fieldsize, queryScalarXPath(field));
    return fieldsize;
}

size32_t RtlPackedIntTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    char temp[9];
    size32_t size = in.readPackedInt(temp);
    byte * dest = builder.ensureCapacity(offset + size, nullptr) + offset;
    memcpy(dest, temp, size);
    return offset + size;
}

void RtlPackedIntTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    in.skipPackedInt();
}


void RtlPackedIntTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isUnsigned())
        rtlUInt8ToStrX(resultLen, result,  rtlGetPackedUnsigned(ptr));
    else
        rtlInt8ToStrX(resultLen, result,  rtlGetPackedSigned(ptr));
}

void RtlPackedIntTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    getString(resultLen, result, ptr);
}

__int64 RtlPackedIntTypeInfo::getInt(const void * ptr) const
{
    if (isUnsigned())
        return rtlGetPackedUnsigned(ptr);
    else
        return rtlGetPackedSigned(ptr);
}

double RtlPackedIntTypeInfo::getReal(const void * ptr) const
{
    if (isUnsigned())
        return (double) rtlGetPackedUnsigned(ptr);
    else
        return (double) rtlGetPackedSigned(ptr);
}

int RtlPackedIntTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isUnsigned())
    {
        unsigned __int64 leftValue = rtlGetPackedUnsigned(left);
        unsigned __int64 rightValue = rtlGetPackedUnsigned(right);
        return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
    }
    else
    {
        __int64 leftValue = rtlGetPackedSigned(left);
        __int64 rightValue = rtlGetPackedSigned(right);
        return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
    }
}

unsigned RtlPackedIntTypeInfo::hash(const byte *self, unsigned inhash) const
{
    __int64 val = getInt(self);
    return rtlHash32Data8(&val, inhash);
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlStringTypeInfo::getMinSize() const
{
    if (isFixedSize())
        return length;
    return sizeof(size32_t);
}

size32_t RtlStringTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return length;
    return sizeof(size32_t) + rtlReadSize32t(self);
}

size32_t RtlStringTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t size;
    rtlDataAttr value;
    source.getStringResult(field, size, value.refstr());
    return buildString(builder, offset, field, size, value.getstr());
}

size32_t RtlStringTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    if (!isFixedSize())
    {
        builder.ensureCapacity(offset+size+sizeof(size32_t), queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, size);
        // NOTE - it has been the subject of debate whether we should convert the incoming data to EBCDIC, or expect the IFieldSource to have already returned ebcdic
        // In order to be symmetrical with the passing of ecl data to a IFieldProcessor the former interpretation is preferred.
        // Expecting source.getStringResult to somehow "know" that EBCDIC was expected seems odd.
        if (isEbcdic())
            rtlStrToEStr(size, (char *) dest+sizeof(size32_t), size, (char *)value);
        else
            memcpy_iflen(dest+sizeof(size32_t), value, size);
        offset += size+sizeof(size32_t);
    }
    else
    {
        builder.ensureCapacity(offset+length, queryName(field));
        byte *dest = builder.getSelf()+offset;
        if (isEbcdic())
            rtlStrToEStr(length, (char *) dest, size, (char *) value);
        else
            rtlStrToStr(length, dest, size, value);
        offset += length;
    }
    return offset;
}

size32_t RtlStringTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t codepoints, const char *value) const
{
    if (isEbcdic())
        return buildUtf8ViaString(builder, offset, field, codepoints, value);

    if (!isFixedSize())
    {
        builder.ensureCapacity(offset+codepoints+sizeof(size32_t), queryName(field));
        char *dest = (char *) builder.getSelf()+offset;
        rtlWriteInt4(dest, codepoints);
        rtlUtf8ToStr(codepoints, dest+sizeof(size32_t), codepoints, value);
        offset += codepoints+sizeof(size32_t);
    }
    else
    {
        builder.ensureCapacity(offset+length, queryName(field));
        char *dest = (char *) builder.getSelf()+offset;
        rtlUtf8ToStr(length, dest, codepoints, value);
        offset += length;
    }
    return offset;
}

size32_t RtlStringTypeInfo::buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const
{
    if (field->initializer || !isFixedSize())
        return RtlTypeInfoBase::buildNull(builder, offset, field);
    else
    {
        builder.ensureCapacity(offset+length, queryName(field));
        memset(builder.getSelf()+offset, isEbcdic() ? 0x40 : ' ', length);
        return offset + length;
    }
}

size32_t RtlStringTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    unsigned thisLength;
    unsigned thisSize;
    if (isFixedSize())
    {
        thisLength = length;
        thisSize = thisLength;
    }
    else
    {
        str = reinterpret_cast<const char *>(self + sizeof(size32_t));
        thisLength = rtlReadSize32t(self);
        thisSize = sizeof(size32_t) + thisLength;
    }

    if (isEbcdic())
    {
        unsigned lenAscii;
        rtlDataAttr ascii;
        rtlEStrToStrX(lenAscii, ascii.refstr(), thisLength, str);
        target.processString(lenAscii, ascii.getstr(), field);
    }
    else
    {
        target.processString(thisLength, str, field);
    }
    return thisSize;
}

size32_t RtlStringTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    unsigned thisLength;
    unsigned thisSize;
    if (isFixedSize())
    {
        thisLength = length;
        thisSize = thisLength;
    }
    else
    {
        str = reinterpret_cast<const char *>(self + sizeof(size32_t));
        thisLength = rtlReadSize32t(self);
        thisSize = sizeof(size32_t) + thisLength;
    }

    if (isEbcdic())
    {
        unsigned lenAscii;
        rtlDataAttr ascii;
        rtlEStrToStrX(lenAscii, ascii.refstr(), thisLength, str);
        target.outputString(lenAscii, ascii.getstr(), queryScalarXPath(field));
    }
    else
    {
        target.outputString(thisLength, str, queryScalarXPath(field));
    }
    return thisSize;
}

size32_t RtlStringTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (isFixedSize())
    {
        size32_t size = length;
        byte * dest = builder.ensureCapacity(offset+size, nullptr) + offset;
        in.read(size, dest);
        offset += size;
    }
    else
    {
        size32_t size = in.readSize();
        byte * dest = builder.ensureCapacity(offset+sizeof(size32_t)+size, nullptr) + offset;
        rtlWriteSize32t(dest, size);
        in.read(size, dest + sizeof(size32_t));
        offset += sizeof(size32_t)+size;
    }
    return offset;
}

void RtlStringTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    if (isFixedSize())
    {
        in.skip(length);
    }
    else
    {
        size32_t thisLength = in.readSize();
        in.skip(thisLength);
    }
}


void RtlStringTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isFixedSize())
    {
        if (isEbcdic())
            return rtlEStrToStrX(resultLen, result, length, (const char *)ptr);
        else
            return rtlStrToStrX(resultLen, result, length, (const char *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        if (isEbcdic())
            return rtlEStrToStrX(resultLen, result, len, (const char *)ptr + sizeof(size32_t));
        else
            return rtlStrToStrX(resultLen, result, len, (const char *)ptr + sizeof(size32_t));
    }
}

void RtlStringTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isEbcdic())
    {
        getUtf8ViaString(resultLen, result, ptr);
        return;
    }

    if (isFixedSize())
    {
        return rtlStrToUtf8X(resultLen, result, length, (const char *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        return rtlStrToUtf8X(resultLen, result, len, (const char *)ptr + sizeof(size32_t));
    }
}

__int64 RtlStringTypeInfo::getInt(const void * ptr) const
{
    //Utf8 output is the same as string output, so avoid the intermediate translation
    if (isFixedSize())
        return rtlStrToInt8(length, (const char *)ptr);
    size32_t len = rtlReadSize32t(ptr);
    return rtlStrToInt8(len, (const char *)ptr + sizeof(size32_t));
}

int RtlStringTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isFixedSize())
        return memcmp(left, right, length);
    else if (isEbcdic())
    {
        // Logically this should be
        // if (isFixedSize())
        //    return rtlCompareEStrEStr(length, (const char *)left, length, (const char *)right);
        // but that's the same as a memcmp if lengths match

        size32_t lenLeft = rtlReadSize32t(left);
        size32_t lenRight = rtlReadSize32t(right);
        return rtlCompareEStrEStr(lenLeft, (const char *)left + sizeof(size32_t), lenRight, (const char *)right + sizeof(size32_t));
    }
    else
    {
        // Logically this should be
        // if (isFixedSize())
        //    return rtlCompareStrStr(length, (const char *)left, length, (const char *)right);  // Actually this is a memcmp
        // but that's the same as a memcmp if lengths match

        size32_t lenLeft = rtlReadSize32t(left);
        size32_t lenRight = rtlReadSize32t(right);
        return rtlCompareStrStr(lenLeft, (const char *)left + sizeof(size32_t), lenRight, (const char *)right + sizeof(size32_t));
    }
}

int RtlStringTypeInfo::compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const
{
    if (isFixedSize())
    {
        lenLeft = std::min(lenLeft, length);
        lenRight = std::min(lenRight, length);
        if (isEbcdic())
            return rtlCompareEStrEStr(lenLeft, (const char *)left, lenRight, (const char *)right);
        else
            return rtlCompareStrStr(lenLeft, (const char *)left, lenRight, (const char *)right);
    }
    else
    {
        size32_t maxLeft = rtlReadSize32t(left);
        size32_t maxRight = rtlReadSize32t(right);

        if (isEbcdic())
            return rtlCompareEStrEStr(std::min(lenLeft, maxLeft), (const char *)left + sizeof(size32_t), std::min(lenRight, maxRight), (const char *)right + sizeof(size32_t));
        else
            return rtlCompareStrStr(std::min(lenLeft, maxLeft), (const char *)left + sizeof(size32_t), std::min(lenRight, maxRight), (const char *)right + sizeof(size32_t));
    }
}

bool RtlStringTypeInfo::canMemCmp() const
{
    return isFixedSize();
}

unsigned RtlStringTypeInfo::hash(const byte * self, unsigned inhash) const
{
    size32_t len;
    if (isFixedSize())
        len = length;
    else
    {
        len = rtlReadSize32t(self);
        self += sizeof(size32_t);
    }
    return rtlHash32Data(rtlTrimStrLen(len, (const char *) self), self, inhash);
}

bool RtlStringTypeInfo::canExtend(char &fillChar) const
{
    if (isFixedSize())
    {
        fillChar = isEbcdic() ? 0x40 : ' ';
        return true;
    }
    return false;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlDataTypeInfo::getMinSize() const
{
    if (isFixedSize())
        return length;
    return sizeof(size32_t);
}

size32_t RtlDataTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return length;
    return sizeof(size32_t) + rtlReadSize32t(self);
}

size32_t RtlDataTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t size;
    rtlDataAttr value;
    source.getDataResult(field, size, value.refdata());
    return buildString(builder, offset, field, size, value.getstr());
}

size32_t RtlDataTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    if (!isFixedSize())
    {
        builder.ensureCapacity(offset+size+sizeof(size32_t), queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, size);
        memcpy_iflen(dest+sizeof(size32_t), value, size);
        offset += size+sizeof(size32_t);
    }
    else
    {
        builder.ensureCapacity(offset+length, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlDataToData(length, dest, size, value);
        offset += length;
    }
    return offset;
}

size32_t RtlDataTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t codepoints, const char *value) const
{
    return buildUtf8ViaString(builder, offset, field, codepoints, value);
}

size32_t RtlDataTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    unsigned thisLength;
    unsigned thisSize;
    if (isFixedSize())
    {
        thisLength = length;
        thisSize = thisLength;
    }
    else
    {
        str = reinterpret_cast<const char *>(self + sizeof(size32_t));
        thisLength = rtlReadSize32t(self);
        thisSize = sizeof(size32_t) + thisLength;
    }

    target.processData(thisLength, str, field);
    return thisSize;
}

size32_t RtlDataTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    unsigned thisLength;
    unsigned thisSize;
    if (isFixedSize())
    {
        thisLength = length;
        thisSize = thisLength;
    }
    else
    {
        str = reinterpret_cast<const char *>(self + sizeof(size32_t));
        thisLength = rtlReadSize32t(self);
        thisSize = sizeof(size32_t) + thisLength;
    }

    target.outputData(thisLength, str, queryScalarXPath(field));
    return thisSize;
}

size32_t RtlDataTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (isFixedSize())
    {
        size32_t size = length;
        byte * dest = builder.ensureCapacity(offset+size, nullptr) + offset;
        in.read(size, dest);
        offset += size;
    }
    else
    {
        size32_t size = in.readSize();
        byte * dest = builder.ensureCapacity(offset+sizeof(size32_t)+size, nullptr) + offset;
        rtlWriteSize32t(dest, size);
        in.read(size, dest + sizeof(size32_t));
        offset += sizeof(size32_t)+size;
    }
    return offset;
}

void RtlDataTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    if (isFixedSize())
    {
        in.skip(length);
    }
    else
    {
        size32_t thisLength = in.readSize();
        in.skip(thisLength);
    }
}


void RtlDataTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isFixedSize())
    {
        return rtlStrToStrX(resultLen, result, length, (const char *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        return rtlStrToStrX(resultLen, result, len, (const char *)ptr + sizeof(size32_t));
    }
}

void RtlDataTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isFixedSize())
    {
        return rtlStrToUtf8X(resultLen, result, length, (const char *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        return rtlStrToUtf8X(resultLen, result, len, (const char *)ptr + sizeof(size32_t));
    }
}

__int64 RtlDataTypeInfo::getInt(const void * ptr) const
{
    //Utf8 output is the same as string output, so avoid the intermediate translation
    if (isFixedSize())
        return rtlStrToInt8(length, (const char *)ptr);
    size32_t len = rtlReadSize32t(ptr);
    return rtlStrToInt8(len, (const char *)ptr + sizeof(size32_t));
}

int RtlDataTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isFixedSize())
        // Logically this should be return rtlCompareDataData(length, (const char *)left, length, (const char *)right);
        // but that acts as a memcmp if lengths match
        return memcmp(left, right, length);

    size32_t lenLeft = rtlReadSize32t(left);
    size32_t lenRight = rtlReadSize32t(right);
    return rtlCompareDataData(lenLeft, (const char *)left + sizeof(size32_t), lenRight, (const char *)right + sizeof(size32_t));
}

int RtlDataTypeInfo::compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const
{
    if (isFixedSize())
    {
        lenLeft = std::min(lenLeft, length);
        lenRight = std::min(lenRight, length);
        return rtlCompareDataData(lenLeft, (const char *)left, lenRight, (const char *)right);
    }
    else
    {
        size32_t maxLeft = rtlReadSize32t(left);
        size32_t maxRight = rtlReadSize32t(right);
        lenLeft = std::min(lenLeft, maxLeft);
        lenRight = std::min(lenRight, maxRight);

        return rtlCompareDataData(lenLeft, (const char *)left + sizeof(size32_t), lenRight, (const char *)right + sizeof(size32_t));
    }
}

bool RtlDataTypeInfo::canMemCmp() const
{
    return isFixedSize();
}

unsigned RtlDataTypeInfo::hash(const byte *self, unsigned inhash) const
{
    size32_t len;
    if (isFixedSize())
        len = length;
    else
    {
        len = rtlReadSize32t(self);
        self += sizeof(size32_t);
    }
    return rtlHash32Data(len, self, inhash);
}

bool RtlDataTypeInfo::canExtend(char &fillChar) const
{
    if (isFixedSize())
    {
        fillChar = 0;
        return true;
    }
    return false;
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlVarStringTypeInfo::getMinSize() const
{
    if (isFixedSize())
        return length+1;
    return 1;
}

size32_t RtlVarStringTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return length + 1;
    const char * str = reinterpret_cast<const char *>(self);
    return (size32_t)strlen(str)+1;
}

size32_t RtlVarStringTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t size;
    rtlDataAttr value;
    source.getStringResult(field, size, value.refstr());
    return buildString(builder, offset, field, size, value.getstr());
}

size32_t RtlVarStringTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    if (!isFixedSize())
    {
        builder.ensureCapacity(offset+size+1, queryName(field));
        // See notes re EBCDIC conversion in RtlStringTypeInfo code
        byte *dest = builder.getSelf()+offset;
        if (isEbcdic())
            rtlStrToEStr(size, (char *) dest, size, (char *)value);
        else
            memcpy_iflen(dest, value, size);
        dest[size] = '\0';
        offset += size+1;
    }
    else
    {
        builder.ensureCapacity(offset+length+1, queryName(field));
        byte *dest = builder.getSelf()+offset;
        if (isEbcdic())
            rtlEStrToVStr(length+1, dest, size, value);
        else
            rtlStrToVStr(length+1, dest, size, value);
        offset += length+1;
    }
    return offset;
}

size32_t RtlVarStringTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t codepoints, const char *value) const
{
    return buildUtf8ViaString(builder, offset, field, codepoints, value);
}

size32_t RtlVarStringTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    unsigned thisLength = (size32_t)strlen(str);
    unsigned thisSize;
    if (isFixedSize())
        thisSize = length+1;
    else
        thisSize = thisLength+1;

    if (isEbcdic())
    {
        unsigned lenAscii;
        rtlDataAttr ascii;
        rtlEStrToStrX(lenAscii, ascii.refstr(), thisLength, str);
        target.processString(lenAscii, ascii.getstr(), field);
    }
    else
        target.processString(thisLength, str, field);

    return thisSize;
}

size32_t RtlVarStringTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    unsigned thisLength = (size32_t)strlen(str);
    unsigned thisSize;
    if (isFixedSize())
        thisSize = length+1;
    else
        thisSize = thisLength+1;

    if (isEbcdic())
    {
        unsigned lenAscii;
        rtlDataAttr ascii;
        rtlEStrToStrX(lenAscii, ascii.refstr(), thisLength, str);
        target.outputString(lenAscii, ascii.getstr(), queryScalarXPath(field));
    }
    else
        target.outputString(thisLength, str, queryScalarXPath(field));

    return thisSize;
}

size32_t RtlVarStringTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (isFixedSize())
    {
        size32_t size = length+1;
        byte * dest = builder.ensureCapacity(offset+size, nullptr) + offset;
        in.read(size, dest);
        return offset + size;
    }
    else
        return offset + in.readVStr(builder, offset, offset);
}

void RtlVarStringTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    if (isFixedSize())
    {
        in.skip(length+1);
    }
    else
    {
        in.skipVStr();
    }
}

void RtlVarStringTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    const char * str = (const char *)ptr;
    return rtlStrToStrX(resultLen, result, strlen(str), str);
}

void RtlVarStringTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    const char * str = (const char *)ptr;
    return rtlStrToUtf8X(resultLen, result, strlen(str), str);
}

__int64 RtlVarStringTypeInfo::getInt(const void * ptr) const
{
    const char * str = (const char *)ptr;
    return rtlVStrToInt8(str);
}

int RtlVarStringTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isEbcdic())
    {
        const char * leftValue = (const char *)left;
        const char * rightValue = (const char *)right;
        return rtlCompareEStrEStr(strlen(leftValue), leftValue, strlen(rightValue), rightValue);
    }
    return rtlCompareVStrVStr((const char *)left, (const char *)right);
}

int RtlVarStringTypeInfo::compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const
{
    size32_t maxLeft = strlen((const char *)left);
    size32_t maxRight = strlen((const char *)right);
    lenLeft = std::min(lenLeft, maxLeft);
    lenRight = std::min(lenRight, maxRight);

    if (isEbcdic())
        return rtlCompareEStrEStr(lenLeft, (const char *)left, lenRight, (const char *)right);
    else
        return rtlCompareStrStr(lenLeft, (const char *)left, lenRight, (const char *)right);
}

void RtlVarStringTypeInfo::setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const
{
    //Do not support substring matching or non-exclusive bounds.
    //They could possibly be implemented by copying a varstring at maxlength padded with spaces.
    if ((subLength != MatchFullString) || !inclusive)
        UNIMPLEMENTED_X("RtlVarStringTypeInfo::setBound");
    RtlTypeInfoBase::setBound(buffer, value, subLength, fill, inclusive);
}

unsigned RtlVarStringTypeInfo::hash(const byte * self, unsigned inhash) const
{
    return rtlHash32VStr((const char *) self, inhash);
}


bool RtlVarStringTypeInfo::canExtend(char &fillChar) const
{
    if (isFixedSize())
    {
        fillChar = 0;
        return true;
    }
    return false;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlQStringTypeInfo::getMinSize() const
{
    if (isFixedSize())
        return rtlQStrSize(length);
    return sizeof(size32_t);
}

size32_t RtlQStringTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return rtlQStrSize(length);
    return sizeof(size32_t) + rtlQStrSize(rtlReadSize32t(self));
}

size32_t RtlQStringTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t size;
    rtlDataAttr value;
    source.getStringResult(field, size, value.refstr());
    return buildString(builder, offset, field, size, value.getstr());
}

size32_t RtlQStringTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    if (!isFixedSize())
    {
        size32_t sizeInBytes = rtlQStrSize(size) + sizeof(size32_t);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, size);
        rtlStrToQStr(size, (char *) dest+sizeof(size32_t), size, value);
        offset += sizeInBytes;
    }
    else
    {
        size32_t sizeInBytes = rtlQStrSize(length);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlStrToQStr(length, (char *) dest, size, value);
        offset += sizeInBytes;
    }
    return offset;
}

size32_t RtlQStringTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t codepoints, const char *value) const
{
    return buildUtf8ViaString(builder, offset, field, codepoints, value);
}

size32_t RtlQStringTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    unsigned thisLength;
    unsigned thisSize;
    if (isFixedSize())
    {
        thisLength = length;
        thisSize = rtlQStrSize(thisLength);
    }
    else
    {
        str = reinterpret_cast<const char *>(self + sizeof(size32_t));
        thisLength = rtlReadSize32t(self);
        thisSize = sizeof(size32_t) + rtlQStrSize(thisLength);
    }

    target.processQString(thisLength, str, field);
    return thisSize;
}

size32_t RtlQStringTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    unsigned thisLength;
    unsigned thisSize;
    if (isFixedSize())
    {
        thisLength = length;
        thisSize = rtlQStrSize(thisLength);
    }
    else
    {
        str = reinterpret_cast<const char *>(self + sizeof(size32_t));
        thisLength = rtlReadSize32t(self);
        thisSize = sizeof(size32_t) + rtlQStrSize(thisLength);
    }

    target.outputQString(thisLength, str, queryScalarXPath(field));
    return thisSize;
}

size32_t RtlQStringTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (isFixedSize())
    {
        size32_t size = rtlQStrSize(length);
        byte * dest = builder.ensureCapacity(offset+size, nullptr) + offset;
        in.read(size, dest);
        offset += size;
    }
    else
    {
        size32_t thisLength = in.readSize();
        size32_t size = rtlQStrSize(thisLength);
        byte * dest = builder.ensureCapacity(offset+sizeof(size32_t)+size, nullptr) + offset;
        rtlWriteSize32t(dest, thisLength);
        in.read(size, dest + sizeof(size32_t));
        offset += sizeof(size32_t)+size;
    }
    return offset;
}

void RtlQStringTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    if (isFixedSize())
    {
        size32_t size = rtlQStrSize(length);
        in.skip(size);
    }
    else
    {
        size32_t thisLength = in.readSize();
        size32_t size = rtlQStrSize(thisLength);
        in.skip(size);
    }
}


void RtlQStringTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isFixedSize())
    {
        return rtlQStrToStrX(resultLen, result, length, (const char *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        return rtlQStrToStrX(resultLen, result, len, (const char *)ptr + sizeof(size32_t));
    }
}

void RtlQStringTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    //NOTE: QStrings cannot contain non-string characters, so converting to str is the same as utf8
    getString(resultLen, result, ptr);
}

__int64 RtlQStringTypeInfo::getInt(const void * ptr) const
{
    size32_t lenTemp;
    rtlDataAttr temp;
    getUtf8(lenTemp, temp.refstr(), ptr);
    return rtlStrToInt8(lenTemp, temp.getstr());
}

int RtlQStringTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isFixedSize())
        // Logically this should be return rtlCompareQStrQStr(length, left, length, right);
        // but that acts as a memcmp if lengths match
        return memcmp(left, right, rtlQStrSize(length));

    size32_t lenLeft = rtlReadSize32t(left);
    size32_t lenRight = rtlReadSize32t(right);
    return rtlCompareQStrQStr(lenLeft, left + sizeof(size32_t), lenRight, right + sizeof(size32_t));
}

int RtlQStringTypeInfo::compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const
{
    if (isFixedSize())
    {
        lenLeft = std::min(lenLeft, length);
        lenRight = std::min(lenRight, length);
        return rtlSafeCompareQStrQStr(lenLeft, (const char *)left, lenRight, (const char *)right);
    }
    else
    {
        size32_t maxLeft = rtlReadSize32t(left);
        size32_t maxRight = rtlReadSize32t(right);
        lenLeft = std::min(lenLeft, maxLeft);
        lenRight = std::min(lenRight, maxRight);

        return rtlSafeCompareQStrQStr(lenLeft, (const char *)left + sizeof(size32_t), lenRight, (const char *)right + sizeof(size32_t));
    }
}

void RtlQStringTypeInfo::setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const
{
    byte *dst = (byte *) buffer;
    size32_t minSize = getMinSize();
    if (value)
    {
        //Eventually the comparison code should not ever use non inclusive, or compare characters after subLength
        //Until then this code needs to fill the rest of the string with the correct fill character and
        //increment the appropriate qcharacter
        size32_t copyLen;
        if (isFixedSize())
        {
            //For a substring comparison, the bytes after subLength will eventually never be compared - but index currently does
            copyLen = std::min(length, subLength);
            rtlQStrToQStr(length, (char *)buffer, copyLen, (const char *)value);
            if (fill != 0)
            {
                for (unsigned i=copyLen; i < length; i++)
                    setQChar(dst, i, 0x3F);
            }
            else if (copyLen != length)
                setQChar(dst, copyLen, 0);    // work around a bug copying qstrings of the same size, but different lengths
        }
        else
        {
            copyLen = std::min(rtlReadSize32t(value), subLength);
            size32_t copySize = size(value, nullptr);
            memcpy(dst, value, copySize);
        }

        if (!inclusive)
        {
            if (fill == 0)
                incrementQString(dst, copyLen);
            else
                decrementQString(dst, copyLen);
        }
    }
    else
    {
        assertex((fill == 0) || isFixedSize());
        memset(dst, fill, minSize);
    }
}

bool RtlQStringTypeInfo::canMemCmp() const
{
    return isFixedSize();
}

unsigned RtlQStringTypeInfo::hash(const byte * self, unsigned inhash) const
{
    rtlDataAttr val;
    unsigned len;
    getString(len, val.refstr(), self);
    return rtlHash32Data(rtlTrimStrLen(len, val.getstr()), val.getstr(), inhash);
}

bool RtlQStringTypeInfo::canExtend(char &fillChar) const
{
    if (isFixedSize())
    {
        fillChar = 0;
        return true;
    }
    return false;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlDecimalTypeInfo::calcSize() const
{
    if (isUnsigned())
        return (getDecimalDigits()+1)/2;
    return (getDecimalDigits()+2)/2;
}

size32_t RtlDecimalTypeInfo::getMinSize() const
{
    return calcSize();
}

size32_t RtlDecimalTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    return calcSize();
}

size32_t RtlDecimalTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    Decimal value;
    source.getDecimalResult(field, value);
    size32_t sizeInBytes = calcSize();
    builder.ensureCapacity(sizeInBytes+offset, queryName(field));
    if (isUnsigned())
        value.getUDecimal(sizeInBytes, getDecimalPrecision(), builder.getSelf()+offset);
    else
        value.getDecimal(sizeInBytes, getDecimalPrecision(), builder.getSelf()+offset);
    offset += sizeInBytes;
    return offset;
}

size32_t RtlDecimalTypeInfo::buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const
{
    if (field->initializer)
        return RtlTypeInfoBase::buildNull(builder, offset, field);
    Decimal value;
    size32_t sizeInBytes = calcSize();
    builder.ensureCapacity(sizeInBytes+offset, queryName(field));
    if (isUnsigned())
        value.getUDecimal(sizeInBytes, getDecimalPrecision(), builder.getSelf()+offset);
    else
        value.getDecimal(sizeInBytes, getDecimalPrecision(), builder.getSelf()+offset);
    offset += sizeInBytes;
    return offset;
}

size32_t RtlDecimalTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    Decimal dvalue;
    dvalue.setString(len, value);
    size32_t sizeInBytes = calcSize();
    builder.ensureCapacity(sizeInBytes+offset, queryName(field));
    if (isUnsigned())
        dvalue.getUDecimal(sizeInBytes, getDecimalPrecision(), builder.getSelf()+offset);
    else
        dvalue.getDecimal(sizeInBytes, getDecimalPrecision(), builder.getSelf()+offset);
    offset += sizeInBytes;
    return offset;
}


size32_t RtlDecimalTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t size = rtlUtf8Length(len, value);
    return buildString(builder, offset, field, size, value);
}


size32_t RtlDecimalTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    size32_t thisSize = calcSize();
    if (isUnsigned())
        target.processUDecimal(self, thisSize, getDecimalPrecision(), field);
    else
        target.processDecimal(self, thisSize, getDecimalPrecision(), field);
    return thisSize;
}

size32_t RtlDecimalTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    size32_t thisSize = calcSize();
    if (isUnsigned())
        target.outputUDecimal(self, thisSize, getDecimalPrecision(), queryScalarXPath(field));
    else
        target.outputDecimal(self, thisSize, getDecimalPrecision(), queryScalarXPath(field));
    return thisSize;
}

void RtlDecimalTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    Decimal temp;
    size32_t sizeInBytes = calcSize();
    if (isUnsigned())
        temp.setUDecimal(sizeInBytes, getDecimalPrecision(), ptr);
    else
        temp.setDecimal(sizeInBytes, getDecimalPrecision(), ptr);
    temp.getStringX(resultLen, result);
}

void RtlDecimalTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    getString(resultLen, result, ptr);
}

__int64 RtlDecimalTypeInfo::getInt(const void * ptr) const
{
    Decimal temp;
    size32_t sizeInBytes = calcSize();
    if (isUnsigned())
        temp.setUDecimal(sizeInBytes, getDecimalPrecision(), ptr);
    else
        temp.setDecimal(sizeInBytes, getDecimalPrecision(), ptr);
    return temp.getInt64();
}

double RtlDecimalTypeInfo::getReal(const void * ptr) const
{
    Decimal temp;
    size32_t sizeInBytes = calcSize();
    if (isUnsigned())
        temp.setUDecimal(sizeInBytes, getDecimalPrecision(), ptr);
    else
        temp.setDecimal(sizeInBytes, getDecimalPrecision(), ptr);
    return temp.getReal();
}

int RtlDecimalTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isUnsigned())
        return decCompareUDecimal(calcSize(), left, right);
    else
        return decCompareDecimal(calcSize(), left, right);
}

unsigned RtlDecimalTypeInfo::hash(const byte * self, unsigned inhash) const
{
    return rtlHash32Data(calcSize(), self, inhash);
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlCharTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    throwUnexpected();  // Can't have a field of type char
}

size32_t RtlCharTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    rtlFailUnexpected();
}

size32_t RtlCharTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    return buildUtf8ViaString(builder, offset, field, len, value);
}

size32_t RtlCharTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    char c;
    if (isEbcdic())
        rtlEStrToStr(1, &c, 1, str);
    else
        c = *str;
    target.processString(1, &c, field);
    return 1;
}

size32_t RtlCharTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    const char * str = reinterpret_cast<const char *>(self);
    char c;
    if (isEbcdic())
        rtlEStrToStr(1, &c, 1, str);
    else
        c = *str;
    target.outputString(1, &c, queryScalarXPath(field));
    return 1;
}

void RtlCharTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    const char * str = (const char *)ptr;
    return rtlStrToStrX(resultLen, result, 1, str);
}

void RtlCharTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    const char * str = (const char *)ptr;
    return rtlStrToUtf8X(resultLen, result, 1, str);
}

__int64 RtlCharTypeInfo::getInt(const void * ptr) const
{
    const char * str = (const char *)ptr;
    return rtlStrToInt8(1, str);
}

int RtlCharTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isEbcdic())
        return rtlCompareEStrEStr(1, (const char *)left, 1, (const char *)right);
    else
        return rtlCompareStrStr(1, (const char *)left, 1, (const char *)right);
}

unsigned RtlCharTypeInfo::hash(const byte * self, unsigned inhash) const
{
    return rtlHash32Data(1, self, inhash);  // MORE - should we trim?
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlUnicodeTypeInfo::getMinSize() const
{
    if (isFixedSize())
        return length * sizeof(UChar);
    return sizeof(size32_t);
}

size32_t RtlUnicodeTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return length * sizeof(UChar);
    return sizeof(size32_t) + rtlReadSize32t(self) * sizeof(UChar);
}

size32_t RtlUnicodeTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t sizeInChars;
    UChar *value;
    source.getUnicodeResult(field, sizeInChars, value);
    if (!isFixedSize())
    {
        size32_t sizeInBytes = sizeInChars * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes+sizeof(size32_t), queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, sizeInChars);  // NOTE - in chars!
        memcpy_iflen(dest+sizeof(size32_t), value, sizeInBytes);
        offset += sizeInBytes+sizeof(size32_t);
    }
    else
    {
        size32_t sizeInBytes = length * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlUnicodeToUnicode(length, (UChar *) dest, sizeInChars, value);
        offset += sizeInBytes;
    }
    rtlFree(value);
    return offset;
}

size32_t RtlUnicodeTypeInfo::buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const
{
    if (field->initializer || !isFixedSize())
        return RtlTypeInfoBase::buildNull(builder, offset, field);
    else
    {
        size32_t sizeInBytes = length * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlUnicodeToUnicode(length, (UChar *) dest, 0, nullptr);
        return offset + sizeInBytes;
    }
}

size32_t RtlUnicodeTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t sizeInChars, const char *value) const
{
    if (!isFixedSize())
    {
        size32_t sizeInBytes = sizeInChars * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes+sizeof(size32_t), queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, sizeInChars);  // NOTE - in chars!
        rtlUtf8ToUnicode(sizeInChars, (UChar *) (dest+sizeof(size32_t)), sizeInChars, value);
        offset += sizeInBytes+sizeof(size32_t);
    }
    else
    {
        size32_t sizeInBytes = length * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlUtf8ToUnicode(length, (UChar *) dest, sizeInChars, value);
        offset += sizeInBytes;
    }
    return offset;
}

size32_t RtlUnicodeTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t sizeInChars, const char *value) const
{
    if (!isFixedSize())
    {
        size32_t sizeInBytes = sizeInChars * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes+sizeof(size32_t), queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlWriteSize32t(dest, sizeInChars);  // NOTE - in chars!
        rtlStrToUnicode(sizeInChars, (UChar *) (dest+sizeof(size32_t)), sizeInChars, value);
        offset += sizeInBytes+sizeof(size32_t);
    }
    else
    {
        size32_t sizeInBytes = length * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlStrToUnicode(length, (UChar *) dest, sizeInChars, value);
        offset += sizeInBytes;
    }
    return offset;
}


size32_t RtlUnicodeTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    const UChar * ustr = reinterpret_cast<const UChar *>(self);
    unsigned thisLength;
    unsigned thisSize;
    if (isFixedSize())
    {
        thisLength = length;
        thisSize = thisLength * sizeof(UChar);
    }
    else
    {
        ustr = reinterpret_cast<const UChar *>(self + sizeof(size32_t));
        thisLength = rtlReadSize32t(self);
        thisSize = sizeof(size32_t) + thisLength * sizeof(UChar);
    }

    target.processUnicode(thisLength, ustr, field);
    return thisSize;
}

size32_t RtlUnicodeTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    const UChar * ustr = reinterpret_cast<const UChar *>(self);
    unsigned thisLength;
    unsigned thisSize;
    if (isFixedSize())
    {
        thisLength = length;
        thisSize = thisLength * sizeof(UChar);
    }
    else
    {
        ustr = reinterpret_cast<const UChar *>(self + sizeof(size32_t));
        thisLength = rtlReadSize32t(self);
        thisSize = sizeof(size32_t) + thisLength * sizeof(UChar);
    }

    target.outputUnicode(thisLength, ustr, queryScalarXPath(field));
    return thisSize;
}

size32_t RtlUnicodeTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (isFixedSize())
    {
        size32_t size = length * sizeof(UChar);
        byte * dest = builder.ensureCapacity(offset+size, nullptr) + offset;
        in.read(size, dest);
        offset += size;
    }
    else
    {
        size32_t thisLength = in.readSize();
        size32_t size = thisLength * sizeof(UChar);
        byte * dest = builder.ensureCapacity(offset+sizeof(size32_t)+size, nullptr) + offset;
        rtlWriteSize32t(dest, thisLength);
        in.read(size, dest + sizeof(size32_t));
        offset += sizeof(size32_t)+size;
    }
    return offset;
}

void RtlUnicodeTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    if (isFixedSize())
    {
        size32_t size = length * sizeof(UChar);
        in.skip(size);
    }
    else
    {
        size32_t thisLength = in.readSize();
        size32_t size = thisLength * sizeof(UChar);
        in.skip(size);
    }
}


void RtlUnicodeTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isFixedSize())
    {
        return rtlUnicodeToStrX(resultLen, result, length, (const UChar *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        const char * str = (const char *)ptr + sizeof(size32_t);
        return rtlUnicodeToStrX(resultLen, result, len, (const UChar *)str);
    }
}

void RtlUnicodeTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isFixedSize())
    {
        return rtlUnicodeToUtf8X(resultLen, result, length, (const UChar *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        const char * str = (const char *)ptr + sizeof(size32_t);
        return rtlUnicodeToUtf8X(resultLen, result, len, (const UChar *)str);
    }
}

__int64 RtlUnicodeTypeInfo::getInt(const void * ptr) const
{
    //Utf8 output is the same as string output, so avoid the intermediate translation
    if (isFixedSize())
        return rtlUnicodeToInt8(length, (const UChar *)ptr);
    size32_t len = rtlReadSize32t(ptr);
    const char * str = (const char *)ptr + sizeof(size32_t);
    return rtlUnicodeToInt8(len, (const UChar *)str);
}

int RtlUnicodeTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isFixedSize())
        return rtlCompareUnicodeUnicode(length, (const UChar *)left, length, (const UChar *)right, locale);

    size32_t lenLeft = rtlReadSize32t(left);
    size32_t lenRight = rtlReadSize32t(right);
    const UChar * valueLeft = reinterpret_cast<const UChar *>(left + sizeof(size32_t));
    const UChar * valueRight = reinterpret_cast<const UChar *>(right + sizeof(size32_t));
    return rtlCompareUnicodeUnicode(lenLeft, valueLeft, lenRight, valueRight, locale);
}

int RtlUnicodeTypeInfo::compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const
{
    if (isFixedSize())
    {
        lenLeft = std::min(lenLeft, length);
        lenRight = std::min(lenRight, length);
        return rtlCompareUnicodeUnicode(lenLeft, (const UChar *)left, lenRight, (const UChar *)right, locale);
    }
    else
    {
        size32_t maxLeft = rtlReadSize32t(left);
        size32_t maxRight = rtlReadSize32t(right);
        lenLeft = std::min(lenLeft, maxLeft);
        lenRight = std::min(lenRight, maxRight);

        return rtlCompareUnicodeUnicode(lenLeft, (const UChar *)(left + sizeof(size32_t)), lenRight, (const UChar *)(right + sizeof(size32_t)), locale);
    }
}

void RtlUnicodeTypeInfo::setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const
{
    byte *dst = (byte *) buffer;
    size32_t minSize = getMinSize();
    if (value)
    {
        assertex(inclusive);
        if (subLength != MatchFullString)
        {
            if (isFixedSize())
            {
                //Only support a lower bound for unicode substrings - filled with spaces
                assertex(fill == 0);

                //For a substring comparison, the bytes after subLength will eventually never be compared - but index currently does
                size32_t copyLen = std::min(length, subLength);
                rtlUnicodeToUnicode(length, (UChar *)buffer, copyLen, (const UChar *)value);
            }
            else
            {
                size32_t copyLen = std::min(rtlReadSize32t(value), subLength);
                size32_t copySize = copyLen * sizeof(UChar);
                *(size32_t *)buffer = copyLen;
                memcpy(dst + sizeof(size32_t), value + sizeof(size32_t), copySize);
            }
        }
        else
        {
            size32_t copySize = size(value, nullptr);
            memcpy(dst, value, copySize);
        }
    }
    else
    {
        //Generally cannot work for variable length fields - since always possible to create a larger value
        //Default does not work for real types, or variable length signed integers
        assertex(fill == 0);
        memset(dst, fill, minSize);
    }
}

unsigned RtlUnicodeTypeInfo::hash(const byte * self, unsigned inhash) const
{
    size32_t len;
    if (isFixedSize())
        len = length;
    else
    {
        len = rtlReadSize32t(self);
        self += sizeof(size32_t);
    }
    const UChar * uself = reinterpret_cast<const UChar *>(self);
    return rtlHash32Unicode(rtlTrimUnicodeStrLen(len, uself), uself, inhash);
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlVarUnicodeTypeInfo::getMinSize() const
{
    if (isFixedSize())
        return (length+1) * sizeof(UChar);
    return sizeof(UChar);
}

size32_t RtlVarUnicodeTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return (length+1) * sizeof(UChar);
    const UChar * ustr = reinterpret_cast<const UChar *>(self);
    return (rtlUnicodeStrlen(ustr)+1) * sizeof(UChar);
}

size32_t RtlVarUnicodeTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t sizeInChars;
    UChar *value;
    source.getUnicodeResult(field, sizeInChars, value);
    if (!isFixedSize())
    {
        size32_t sizeInBytes = (sizeInChars+1) * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        UChar *dest = (UChar *) (builder.getSelf()+offset);
        memcpy_iflen(dest, value, sizeInBytes - sizeof(UChar));
        dest[sizeInChars] = 0;
        offset += sizeInBytes;
    }
    else
    {
        size32_t sizeInBytes = (length+1) * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlUnicodeToVUnicode(length+1, (UChar *) dest, sizeInChars, value);
        offset += sizeInBytes;
    }
    rtlFree(value);
    return offset;
}

size32_t RtlVarUnicodeTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t sizeInChars, const char *value) const
{
    size32_t usize;
    rtlDataAttr uvalue;
    rtlUtf8ToUnicodeX(usize, uvalue.refustr(), sizeInChars, value);
    if (!isFixedSize())
    {
        size32_t sizeInBytes = (usize+1) * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        UChar *dest = (UChar *) (builder.getSelf()+offset);
        memcpy_iflen(dest, uvalue.getustr(), sizeInBytes - sizeof(UChar));
        dest[usize] = 0;
        offset += sizeInBytes;
    }
    else
    {
        size32_t sizeInBytes = (length+1) * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        byte *dest = builder.getSelf()+offset;
        rtlUnicodeToVUnicode(length+1, (UChar *) dest, usize, uvalue.getustr());
        offset += sizeInBytes;
    }
    return offset;
}

size32_t RtlVarUnicodeTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t sizeInChars, const char *value) const
{
    size32_t lengthTarget;
    if (!isFixedSize())
    {
        lengthTarget = (sizeInChars + 1);
    }
    else
    {
        lengthTarget = (length + 1);
    }
    size32_t sizeTarget = lengthTarget * sizeof(UChar);

    builder.ensureCapacity(offset+sizeTarget, queryName(field));
    byte *dest = builder.getSelf()+offset;
    rtlStrToVUnicode(lengthTarget, (UChar *) dest, sizeInChars, value);
    offset += sizeTarget;
    return offset;
}


size32_t RtlVarUnicodeTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    const UChar * ustr = reinterpret_cast<const UChar *>(self);
    unsigned thisLength = rtlUnicodeStrlen(ustr);
    unsigned thisSize;
    if (isFixedSize())
        thisSize = (length + 1) * sizeof(UChar);
    else
        thisSize = (thisLength + 1) * sizeof(UChar);

    target.processUnicode(thisLength, ustr, field);
    return thisSize;
}

size32_t RtlVarUnicodeTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    const UChar * ustr = reinterpret_cast<const UChar *>(self);
    unsigned thisLength = rtlUnicodeStrlen(ustr);
    unsigned thisSize;
    if (isFixedSize())
        thisSize = (length + 1) * sizeof(UChar);
    else
        thisSize = (thisLength + 1) * sizeof(UChar);

    target.outputUnicode(thisLength, ustr, queryScalarXPath(field));
    return thisSize;
}

size32_t RtlVarUnicodeTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (isFixedSize())
    {
        size32_t size = (length+1)*sizeof(UChar);
        byte * dest = builder.ensureCapacity(offset+size, nullptr) + offset;
        in.read(size, dest);
        return offset + size;
    }
    else
        return offset + in.readVUni(builder, offset, offset);
}

void RtlVarUnicodeTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    if (isFixedSize())
    {
        size32_t size = (length+1)*sizeof(UChar);
        in.skip(size);
    }
    else
    {
        in.skipVUni();
    }
}


void RtlVarUnicodeTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    const UChar * str = (const UChar *)ptr;
    rtlUnicodeToStrX(resultLen, result, rtlUnicodeStrlen(str), str);
}

void RtlVarUnicodeTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    const UChar * str = (const UChar *)ptr;
    rtlUnicodeToUtf8X(resultLen, result, rtlUnicodeStrlen(str), str);
}

__int64 RtlVarUnicodeTypeInfo::getInt(const void * ptr) const
{
    const UChar * str = (const UChar *)ptr;
    return rtlUnicodeToInt8(rtlUnicodeStrlen(str), str);
}

int RtlVarUnicodeTypeInfo::compare(const byte * left, const byte * right) const
{
    const UChar * valueLeft = reinterpret_cast<const UChar *>(left);
    const UChar * valueRight = reinterpret_cast<const UChar *>(right);
    size32_t lenLeft = rtlUnicodeStrlen(valueLeft);
    size32_t lenRight = rtlUnicodeStrlen(valueRight);
    return rtlCompareUnicodeUnicode(lenLeft, valueLeft, lenRight, valueRight, locale);
}

int RtlVarUnicodeTypeInfo::compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const
{
    size32_t maxLeft = rtlUnicodeStrlen((const UChar *)left);
    size32_t maxRight = rtlUnicodeStrlen((const UChar *)right);
    lenLeft = std::min(lenLeft, maxLeft);
    lenRight = std::min(lenRight, maxRight);

    return rtlCompareUnicodeUnicode(lenLeft, (const UChar *)left, lenRight, (const UChar *)right, locale);
}

void RtlVarUnicodeTypeInfo::setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const
{
    byte *dst = (byte *) buffer;
    size32_t minSize = getMinSize();
    if (value)
    {
        assertex(inclusive);
        if (subLength != MatchFullString)
        {
            UNIMPLEMENTED_X("VarUnicode::setBound(substring)");
        }
        else
        {
            size32_t copySize = size(value, nullptr);
            memcpy(dst, value, copySize);
        }
    }
    else
    {
        //Generally cannot work for variable length fields - since always possible to create a larger value
        //Default does not work for real types, or variable length signed integers
        assertex(fill == 0);
        memset(dst, fill, minSize);
    }
}

unsigned RtlVarUnicodeTypeInfo::hash(const byte * _self, unsigned inhash) const
{
    const UChar * self = reinterpret_cast<const UChar *>(_self);
    return rtlHash32VUnicode(self, inhash);
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlUtf8TypeInfo::getMinSize() const
{
    return sizeof(size32_t);
}

size32_t RtlUtf8TypeInfo::size(const byte * self, const byte * selfrow) const 
{
    assertex(!isFixedSize());
    return sizeof(size32_t) + rtlUtf8Size(rtlReadSize32t(self), self+sizeof(unsigned));
}

size32_t RtlUtf8TypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t sizeInChars;
    rtlDataAttr value;
    source.getUTF8Result(field, sizeInChars, value.refstr());
    return buildUtf8(builder, offset, field, sizeInChars, value.getstr());
}

size32_t RtlUtf8TypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t sizeInChars, const char *value) const
{
    size32_t sizeInBytes = rtlUtf8Size(sizeInChars, value);
    assertex(!isFixedSize());
    builder.ensureCapacity(offset+sizeInBytes+sizeof(size32_t), queryName(field));
    byte *dest = builder.getSelf()+offset;
    rtlWriteSize32t(dest, sizeInChars);  // NOTE - in chars!
    memcpy_iflen(dest+sizeof(size32_t), value, sizeInBytes);
    return offset + sizeInBytes+sizeof(size32_t);
}

size32_t RtlUtf8TypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t sizeInChars, const char *value) const
{
    assertex(!isFixedSize());

    //MORE: The target size could be precalculated - which would avoid creating a temporary
    size32_t tempLen;
    rtlDataAttr temp;
    rtlStrToUtf8X(tempLen, temp.refstr(), sizeInChars, value);
    size32_t sizeInBytes = rtlUtf8Size(tempLen, temp.getstr());
    builder.ensureCapacity(offset+sizeInBytes+sizeof(size32_t), queryName(field));
    byte *dest = builder.getSelf()+offset;
    rtlWriteSize32t(dest, sizeInChars);  // NOTE - in chars!
    memcpy_iflen((dest+sizeof(size32_t)), temp.getstr(), sizeInBytes);
    return offset + sizeInBytes+sizeof(size32_t);
}


size32_t RtlUtf8TypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    assertex(!isFixedSize());
    const char * str = reinterpret_cast<const char *>(self + sizeof(size32_t));
    unsigned thisLength = rtlReadSize32t(self);
    unsigned thisSize = sizeof(size32_t) + rtlUtf8Size(thisLength, str);

    target.processUtf8(thisLength, str, field);
    return thisSize;
}

size32_t RtlUtf8TypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    assertex(!isFixedSize());
    const char * str = reinterpret_cast<const char *>(self + sizeof(size32_t));
    unsigned thisLength = rtlReadSize32t(self);
    unsigned thisSize = sizeof(size32_t) + rtlUtf8Size(thisLength, str);

    target.outputUtf8(thisLength, str, queryScalarXPath(field));
    return thisSize;
}

size32_t RtlUtf8TypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    assertex(!isFixedSize());
    size32_t thisLength = in.readSize();
    size32_t size = in.readUtf8(builder, offset + sizeof(size32_t), offset + sizeof(size32_t), thisLength);
    rtlWriteSize32t(builder.getSelf() + offset, thisLength);
    return offset + sizeof(size32_t) + size;
}

void RtlUtf8TypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    assertex(!isFixedSize());
    size32_t thisLength = in.readSize();
    in.skipUtf8(thisLength);
}


void RtlUtf8TypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isFixedSize())
    {
        rtlUtf8ToStrX(resultLen, result, length, (const char *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        rtlUtf8ToStrX(resultLen, result, len, (const char *)ptr + sizeof(size32_t));
    }
}

void RtlUtf8TypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isFixedSize())
    {
        rtlUtf8ToUtf8X(resultLen, result, length, (const char *)ptr);
    }
    else
    {
        size32_t len = rtlReadSize32t(ptr);
        rtlUtf8ToUtf8X(resultLen, result, len, (const char *)ptr + sizeof(size32_t));
    }
}

__int64 RtlUtf8TypeInfo::getInt(const void * ptr) const
{
    //Utf8 output is the same as string output, so avoid the intermediate translation
    if (isFixedSize())
        return rtlUtf8ToInt(length, (const char *)ptr);
    size32_t len = rtlReadSize32t(ptr);
    return rtlUtf8ToInt(len, (const char *)ptr + sizeof(size32_t));
}

int RtlUtf8TypeInfo::compare(const byte * left, const byte * right) const
{
    assertex(!isFixedSize());
    size32_t lenLeft = rtlReadSize32t(left);
    size32_t lenRight = rtlReadSize32t(right);
    const char * valueLeft = reinterpret_cast<const char *>(left) + sizeof(size32_t);
    const char * valueRight = reinterpret_cast<const char *>(right) + sizeof(size32_t);
    return rtlCompareUtf8Utf8(lenLeft, valueLeft, lenRight, valueRight, locale);
}

int RtlUtf8TypeInfo::compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const
{
    size32_t maxLeft = rtlReadSize32t(left);
    size32_t maxRight = rtlReadSize32t(right);
    lenLeft = std::min(lenLeft, maxLeft);
    lenRight = std::min(lenRight, maxRight);
    return rtlCompareUtf8Utf8(lenLeft, (const char *)(left + sizeof(size32_t)), lenRight, (const char *)(right + sizeof(size32_t)), locale);
}

void RtlUtf8TypeInfo::setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const
{
    assertex(!isFixedSize());
    byte *dst = (byte *) buffer;
    size32_t minSize = getMinSize();
    if (value)
    {
        assertex(inclusive);
        if (subLength != MatchFullString)
        {
            //Note: Only variable length utf8 is supported by the system
            size32_t copyLen = std::min(rtlReadSize32t(value), subLength);
            size32_t copySize = rtlUtf8Size(copyLen, value + sizeof(size32_t));
            *(size32_t *)buffer = copyLen;
            memcpy(dst + sizeof(size32_t), value + sizeof(size32_t), copySize);
        }
        else
        {
            size32_t copySize = size(value, nullptr);
            memcpy(dst, value, copySize);
        }
    }
    else
    {
        //Generally cannot work for variable length fields - since always possible to create a larger value
        //Default does not work for real types, or variable length signed integers
        assertex(fill == 0);
        memset_iflen(dst, fill, minSize);
    }
}

unsigned RtlUtf8TypeInfo::hash(const byte * self, unsigned inhash) const
{
    assertex(!isFixedSize());
    size32_t len = rtlReadSize32t(self);
    const char * uself = reinterpret_cast<const char *>(self + sizeof(size32_t));
    return rtlHash32Utf8(rtlTrimUtf8StrLen(len, uself), uself,inhash);
}


//-------------------------------------------------------------------------------------------------------------------

inline size32_t sizeFields(const RtlFieldInfo * const * cur, const byte * self, const byte * selfrow)
{
    unsigned offset = 0;
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        offset += child->size(self+offset, selfrow);
        cur++;
    }
    return offset;
}

static size32_t processFields(const RtlFieldInfo * const * cur, const byte * self, const byte * selfrow, IFieldProcessor & target)
{
    unsigned offset = 0;
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        child->process(self+offset, selfrow, target);
        offset += child->size(self+offset, selfrow);
        cur++;
    }
    return offset;
}

static size32_t buildFields(const RtlFieldInfo * const * cur, ARowBuilder &builder, size32_t offset, IFieldSource &source)
{
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        offset = child->build(builder, offset, source);
        cur++;
    }
    return offset;
}


static size32_t toXMLFields(const RtlFieldInfo * const * cur, const byte * self, const byte * selfrow, IXmlWriter & target)
{
    size32_t offset = 0;
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        size32_t size = child->toXML(self+offset, selfrow, target);
        offset += size;
        cur++;
    }

    return offset;
}

static size32_t deserializeFields(const RtlFieldInfo * const * cur, ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset)
{
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        offset = child->type->deserialize(builder, in, offset);
        cur++;
    }

    return offset;
}

static void readAheadFields(const RtlFieldInfo * const * cur, IRowPrefetcherSource & in)
{
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        child->type->readAhead(in);
        cur++;
    }
}

int ECLRTL_API compareFields(const RtlFieldInfo * const * cur, const byte * left, const byte * right, bool excludePayload)
{
    size32_t leftOffset = 0;
    size32_t rightOffset = 0;
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child || (excludePayload && (child->flags & RFTMispayloadfield)))
            return 0;
        auto type = child->type;
        int rc = type->compare(left + leftOffset, right + rightOffset);
        if (rc != 0)
            return rc;
        leftOffset += type->size(left + leftOffset, left);
        rightOffset += type->size(right + rightOffset, right);
        cur++;
    }
}

unsigned ECLRTL_API hashFields(const RtlFieldInfo * const * cur, const byte *self, unsigned inhash, bool excludePayload)
{
    size32_t offset = 0;
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child || (excludePayload && (child->flags & RFTMispayloadfield)))
            break;
        auto type = child->type;
        inhash = type->hash(self + offset, inhash);
        offset += type->size(self + offset, self);
        cur++;
    }
    return inhash;
}

extern bool ECLRTL_API hasTrailingFileposition(const RtlFieldInfo * const * fields)
{
    if (!*fields)
        return false;
    while (*fields)
        fields++;
    return (fields[-1]->type->getType() == type_filepos);  // note - filepos is created as an RtlSwapIntTypeInfo with type set to type_filepos
}

extern bool ECLRTL_API hasTrailingFileposition(const RtlTypeInfo * type)
{
    switch (type->getType())
    {
    case type_record:
    {
        const RtlRecordTypeInfo * record = static_cast<const RtlRecordTypeInfo *>(type);
        return hasTrailingFileposition(record->fields);
    }
    }
    return false;
}

//Does the field list contain a keyed int?  We are not interested in fields inside child datasets.
//And keyed fields cannot exist within ifblocks.
static bool containsKeyedSignedInt(const RtlFieldInfo * const * fields)
{
    if (!*fields)
        return false;
    while (*fields)
    {
        const RtlTypeInfo * type = (*fields)->type;
        if (type->getType() == type_keyedint)
        {
            const RtlTypeInfo * baseType = type->queryChildType();
            if (baseType->isSigned() && (baseType->getType() == type_int))
                return true;
        }
        else if (type->getType() == type_record)
        {
            const RtlRecordTypeInfo * record = static_cast<const RtlRecordTypeInfo *>(type);
            if (containsKeyedSignedInt(record->fields))
                return true;
        }
        fields++;
    }
    return false;
}

extern bool ECLRTL_API containsKeyedSignedInt(const RtlTypeInfo * type)
{
    switch (type->getType())
    {
    case type_record:
    {
        const RtlRecordTypeInfo * record = static_cast<const RtlRecordTypeInfo *>(type);
        return containsKeyedSignedInt(record->fields);
    }
    }
    return false;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlRecordTypeInfo::getMinSize() const
{
    return ::getMinSize(fields);
}

size32_t RtlRecordTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    return sizeFields(fields, self, self);
}

size32_t RtlRecordTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (target.processBeginRow(field))
    {
        unsigned offset = processFields(fields, self, self, target);
        target.processEndRow(field);
        return offset;
    }
    return size(self, selfrow);
}

size32_t RtlRecordTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    const char * xpath = queryXPath(field);
    if (*xpath)
        target.outputBeginNested(xpath, false);

    unsigned thisSize = toXMLFields(fields, self, self, target);

    if (*xpath)
        target.outputEndNested(xpath);

    return thisSize;
}

size32_t RtlRecordTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    //Will not generally be called because it will have been expanded
    return deserializeFields(fields, builder, in, offset);
}

void RtlRecordTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    //This could be called if the record contains ifblocks
    in.noteStartChild();
    readAheadFields(fields, in);
    in.noteFinishChild();
}


size32_t RtlRecordTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    source.processBeginRow(field);
    offset = buildFields(fields, builder, offset, source);
    source.processEndRow(field);
    return offset;
}

size32_t RtlRecordTypeInfo::buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const
{
    const RtlFieldInfo * const * cur = fields;
    for (;;)
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        offset = child->type->buildNull(builder, offset, child);
        cur++;
    }
    return offset;
}

size32_t RtlRecordTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    throwUnexpected();
}

size32_t RtlRecordTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    throwUnexpected();
}

void RtlRecordTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    resultLen = 0;
    result = nullptr;
}

void RtlRecordTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    resultLen = 0;
    result = nullptr;
}

__int64 RtlRecordTypeInfo::getInt(const void * ptr) const
{
    return 0;
}

int RtlRecordTypeInfo::compare(const byte * left, const byte * right) const
{
    return compareFields(fields, left, right);
}

unsigned RtlRecordTypeInfo::hash(const byte * self, unsigned inhash) const
{
    return hashFields(fields, self, inhash);
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlCompoundTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    UNIMPLEMENTED;
}

size32_t RtlCompoundTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    UNIMPLEMENTED;
}

void RtlCompoundTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    //MORE: Should this fail instead?
    resultLen = 0;
    result = nullptr;
}

void RtlCompoundTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    //MORE: Should this fail instead?
    resultLen = 0;
    result = nullptr;
}

__int64 RtlCompoundTypeInfo::getInt(const void * ptr) const
{
    //MORE: Should this fail instead?
    return 0;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlSetTypeInfo::getMinSize() const
{
    return sizeof(bool) + sizeof(size32_t);
}

size32_t RtlSetTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    return sizeof(bool) + sizeof(size32_t) + rtlReadSize32t(self + sizeof(bool));
}

size32_t RtlSetTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    bool isAll;
    source.processBeginSet(field, isAll);
    size32_t sizeInBytes = sizeof(bool) + sizeof(size32_t);
    builder.ensureCapacity(offset+sizeInBytes, queryName(field));
    byte *dest = builder.getSelf()+offset;
    if (isAll)
    {
        * (bool *) dest = true;
        rtlWriteInt4(dest+1, 0);
        offset += sizeInBytes;
    }
    else
    {
        * (bool *) dest = false;
        size32_t newOffset = offset + sizeInBytes;
        RtlFieldStrInfo dummyField("<set element>", NULL, child);
        while (source.processNextSet(field))
        {
            newOffset = child->build(builder, newOffset, &dummyField, source);
        }
        // Go back in and patch the size, remembering it may have moved
        rtlWriteInt4(builder.getSelf()+offset+1, newOffset - (offset+sizeInBytes));
        offset = newOffset;
    }
    source.processEndSet(field);
    return offset;
}

size32_t RtlSetTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    unsigned offset = sizeof(bool) + sizeof(size32_t);
    unsigned max = offset + rtlReadSize32t(self + sizeof(bool));
    unsigned elements = 0;
    if (!*(bool *)self)
    {
        unsigned tempOffset = sizeof(bool) + sizeof(size32_t);
        if (child->isFixedSize())
        {
            unsigned elemSize = child->size(NULL, NULL);
            elements = (max-offset) / elemSize;
            assert(elements*elemSize == max-offset);
        }
        else
        {
            DummyFieldProcessor dummy;
            while (tempOffset < max)
            {
                tempOffset += child->process(self+tempOffset, selfrow, field, dummy);  // NOTE - good thing we can't have a set of sets, or this would recurse
                elements++;
            }
        }
    }
    if (target.processBeginSet(field, elements, *(bool *)self, self+offset))
    {
        while (offset < max)
        {
            offset += child->process(self+offset, selfrow, field, target);
        }
    }
    target.processEndSet(field);
    return max;
}

size32_t RtlSetTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    unsigned offset = sizeof(bool) + sizeof(size32_t);
    unsigned max = offset + rtlReadSize32t(self + sizeof(bool));

    StringAttr outerTag;
    if (hasOuterXPath(field))
    {
        queryNestedOuterXPath(outerTag, field);
        target.outputBeginNested(outerTag, false);
    }

    if (*(bool *)self)
        target.outputSetAll();
    else
    {
        const char *innerPath = queryXPath(field);
        target.outputBeginArray(innerPath);
        while (offset < max)
        {
            child->toXML(self+offset, selfrow, field, target);
            offset += child->size(self+offset, selfrow);
        }
        target.outputEndArray(innerPath);
    }

    if (outerTag)
        target.outputEndNested(outerTag);
    return max;
}

size32_t RtlSetTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    bool isAll;
    in.read(1, &isAll);
    size32_t size = in.readSize();
    byte * dest = builder.ensureCapacity(offset+sizeof(bool)+sizeof(size32_t)+size, nullptr) + offset;
    *dest = isAll;
    rtlWriteSize32t(dest + sizeof(bool), size);
    in.read(size, dest + sizeof(bool) + sizeof(size32_t));
    return offset+sizeof(bool)+sizeof(size32_t)+size;
}


void RtlSetTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    in.skip(1);
    size32_t thisLength = in.readSize();
    in.skip(thisLength);
}

int RtlSetTypeInfo::compare(const byte * left, const byte * right) const
{
    const bool allLeft = *(const bool *)left;
    const bool allRight = *(const bool *)right;
    if (allLeft || allRight)
    {
        if (allLeft && allRight)
            return 0;
        if (allLeft)
            return +1;
        else
            return -1;
    }

    size32_t sizeLeft = rtlReadSize32t(left + sizeof(bool));
    const byte * ptrLeft = left + sizeof(bool) + sizeof(size32_t);
    size32_t sizeRight = rtlReadSize32t(right + sizeof(bool));
    const byte * ptrRight = right + sizeof(bool) + sizeof(size32_t);
    size32_t offLeft = 0;
    size32_t offRight = 0;
    for (;;)
    {
        if ((offLeft == sizeLeft) || (offRight == sizeRight))
        {
            if ((offLeft == sizeLeft) && (offRight == sizeRight))
                return 0;
            if (offLeft == sizeLeft)
                return -1;
            else
                return +1;
        }

        int rc = child->compare(ptrLeft + offLeft, ptrRight + offRight);
        if (rc != 0)
            return rc;

        offLeft += child->size(ptrLeft + offLeft, ptrLeft + offLeft);
        offRight += child->size(ptrRight + offRight, ptrRight + offRight);
    }
}

unsigned RtlSetTypeInfo::hash(const byte * self, unsigned inhash) const
{
    const bool allLeft = *(const bool *) self;
    self += sizeof(bool);
    if (allLeft)
    {
        // Nothing - this is unfortunate as it means hash(all) and hash([]) are the same...
        // But it matches generated code
    }
    else
    {
        size32_t size = rtlReadSize32t(self);
        self += sizeof(size32_t);
#if 0
        // This might be the smart way to hash - since it means that the hash depends on the value, not the type,
        // and that things that compare equal will hash equal. But that is not what the code generator does
        for (size32_t offset = 0; offset < size; offset += child->size(self + offset, self + offset))
        {
            inhash = child->hash(self + offset, inhash);
        }
#else
        inhash = rtlHash32Data(size, self, inhash);
#endif
    }
    return inhash;
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlRowTypeInfo::getMinSize() const
{
    if (isLinkCounted())
        return sizeof(void *);
    return child->getMinSize();
}

size32_t RtlRowTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isLinkCounted())
        return sizeof(void *);
    return child->size(self, selfrow);
}

size32_t RtlRowTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isLinkCounted())
    {
        const byte * row = *(const byte * *)self;
        if (row)
            child->process(row, row, field, target);
        return sizeof(row);
    }
    return child->process(self, self, field, target);
}

size32_t RtlRowTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    if (isLinkCounted())
    {
        const byte * row = *(const byte * *)self;
        child->toXML(row, row, field, target);
        return sizeof(row);
    }

    return child->toXML(self, self, field, target);
}

size32_t RtlRowTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    //Link counted isn't yet implemented in the code generator
    if (isLinkCounted())
        UNIMPLEMENTED;

    return child->deserialize(builder, in, offset);
}


void RtlRowTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    return child->readAhead(in);
}


int RtlRowTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isLinkCounted())
    {
        const byte * leftRow = *(const byte * *)left;
        const byte * rightRow = *(const byte * *)right;
        if (leftRow && rightRow)
            return child->compare(leftRow, rightRow);
        if (leftRow)
            return +1;
        if (rightRow)
            return -1;
        return 0;
    }
    return child->compare(left, right);
}

unsigned RtlRowTypeInfo::hash(const byte * self, unsigned inhash) const
{
    if (isLinkCounted())
    {
        const byte * selfRow = *(const byte * *)self;
        if (selfRow)
            inhash = child->hash(selfRow, inhash);
        return inhash;
    }
    return child->hash(self, inhash);
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlDatasetTypeInfo::getMinSize() const
{
    if (isLinkCounted())
        return sizeof(size32_t) + sizeof(void * *);
    return sizeof(size32_t);
}

size32_t RtlDatasetTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isLinkCounted())
        return sizeof(size32_t) + sizeof(void * *);
    return sizeof(size32_t) + rtlReadSize32t(self);
}

size32_t RtlDatasetTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    source.processBeginDataset(field);
    if (isLinkCounted())
    {
        // a 32-bit record count, and a pointer to an array of record pointers
        size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        size32_t numRows = 0;
        Owned<IEngineRowAllocator> childAllocator = builder.queryAllocator()->createChildRowAllocator(child);
        const byte **childRows = NULL;
        RtlFieldStrInfo dummyField("<nested row>", NULL, child);
        while (source.processNextRow(field))
        {
            RtlDynamicRowBuilder childBuilder(*childAllocator);
            size32_t childLen = child->build(childBuilder, 0, &dummyField, source);
            childRows = childAllocator->appendRowOwn(childRows, ++numRows, (void *) childBuilder.finalizeRowClear(childLen));
        }
        // Go back in and patch the count, remembering it may have moved
        rtlWriteInt4(builder.getSelf()+offset, numRows);
        * ( const void * * ) (builder.getSelf()+offset+sizeof(size32_t)) = childRows;
        offset += sizeInBytes;
    }
    else
    {
        // a 32-bit size, then rows inline
        size32_t sizeInBytes = sizeof(size32_t);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        size32_t newOffset = offset + sizeInBytes;
        RtlFieldStrInfo dummyField("<nested row>", NULL, child);
        while (source.processNextRow(field))
            newOffset = child->build(builder, newOffset, &dummyField, source);
        // Go back in and patch the size, remembering it may have moved
        rtlWriteInt4(builder.getSelf()+offset, newOffset - (offset+sizeInBytes));
        offset = newOffset;
    }
    source.processEndDataset(field);
    return offset;

}

size32_t RtlDatasetTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isLinkCounted())
    {
        size32_t thisCount = rtlReadSize32t(self);
        if (target.processBeginDataset(field, thisCount))
        {
            const byte * * rows = *reinterpret_cast<const byte * * const *>(self + sizeof(size32_t));
            for (unsigned i= 0; i < thisCount; i++)
            {
                const byte * row = rows[i];
                child->process(row, row, field, target);
            }
            target.processEndDataset(field);
        }
        return sizeof(size32_t) + sizeof(void * *);
    }
    else
    {
        unsigned offset = sizeof(size32_t);
        unsigned max = offset + rtlReadSize32t(self);
        unsigned thisCount = 0;
        DummyFieldProcessor dummy;
        while (offset < max)
        {
            offset += child->process(self+offset, self+offset, field, dummy);
            thisCount++;
        }
        offset = sizeof(size32_t);
        if (target.processBeginDataset(field, thisCount))
        {
            while (offset < max)
            {
                offset += child->process(self+offset, self+offset, field, target);
            }
            target.processEndDataset(field);
        }
        return max;
    }
}

size32_t RtlDatasetTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    StringAttr outerTag;
    if (hasOuterXPath(field))
    {
        queryNestedOuterXPath(outerTag, field);
        target.outputBeginNested(outerTag, false);
    }

    const char *innerPath = queryXPath(field);
    target.outputBeginArray(innerPath);

    unsigned thisSize;
    if (isLinkCounted())
    {
        size32_t thisCount = rtlReadSize32t(self);
        const byte * * rows = *reinterpret_cast<const byte * * const *>(self + sizeof(size32_t));
        for (unsigned i= 0; i < thisCount; i++)
        {
            const byte * row = rows[i];
            if (row)
                child->toXML(row, row, field, target);
        }
        thisSize = sizeof(size32_t) + sizeof(void * *);
    }
    else
    {
        unsigned offset = sizeof(size32_t);
        unsigned max = offset + rtlReadSize32t(self);
        while (offset < max)
        {
            child->toXML(self+offset, self+offset, field, target);
            offset += child->size(self+offset, self+offset);
        }
        thisSize = max;
    }

    target.outputEndArray(innerPath);
    if (outerTag)
        target.outputEndNested(outerTag);

    return thisSize;
}

size32_t RtlDatasetTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    const bool canUseMemcpy = true;

    if (canUseMemcpy && !isLinkCounted())
    {
        size32_t size = in.readSize();
        byte * dest = builder.ensureCapacity(offset+sizeof(size32_t)+size, nullptr) + offset;
        rtlWriteSize32t(dest, size);
        in.read(size, dest + sizeof(size32_t));
        return offset + sizeof(size32_t) + size;
    }
    else
    {
        if (isLinkCounted())
        {
            //Currently inefficient because it is recreating deserializers and resolving child allocators each time it is called.
            ICodeContext * ctx = nullptr; // Slightly dodgy, but not needed if the child deserializers are also calculated
            unsigned activityId = 0;

            // a 32-bit record count, and a pointer to an hash table with record pointers
            size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
            byte * dest = builder.ensureCapacity(offset+sizeInBytes, nullptr) + offset;
            Owned<IEngineRowAllocator> childAllocator = builder.queryAllocator()->createChildRowAllocator(child);
            Owned<IOutputRowDeserializer> deserializer = childAllocator->queryOutputMeta()->createDiskDeserializer(ctx, activityId);
            rtlDeserializeChildRowset(*(size32_t *)dest, *(const byte * * *)(dest + sizeof(size32_t)), childAllocator, deserializer, in);
            return offset + sizeInBytes;
        }
        else
        {
            size32_t startOffset = offset + sizeof(size32_t);
            size32_t nextOffset = startOffset;
            offset_t endOffset = in.beginNested();
            while (in.finishedNested(endOffset))
                nextOffset = child->deserialize(builder, in, nextOffset);
            rtlWriteSize32t(builder.getSelf() + offset, nextOffset - startOffset);
            return nextOffset;
        }
    }
}


void RtlDatasetTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    size32_t size = in.readSize();
    in.skip(size);
}

int RtlDatasetTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isLinkCounted())
    {
        const size32_t countLeft = rtlReadSize32t(left);
        const size32_t countRight = rtlReadSize32t(right);
        const byte * * rowsLeft = (const byte * *)((const byte *)left + sizeof(size32_t));
        const byte * * rowsRight = (const byte * *)((const byte *)right + sizeof(size32_t));
        size32_t row = 0;
        for (;;)
        {
            if ((row == countLeft) || (row == countRight))
            {
                if (countLeft == countRight)
                    return 0;
                if (row == countLeft)
                    return -1;
                else
                    return +1;
            }

            int rc = child->compare(rowsLeft[row], rowsRight[row]);
            if (rc != 0)
                return rc;
            row++;
        }
    }

    size32_t lenLeft = rtlReadSize32t(left);
    size32_t lenRight = rtlReadSize32t(right);
    const byte * ptrLeft = left + sizeof(size32_t);
    const byte * ptrRight = right + sizeof(size32_t);
    size32_t offLeft = 0;
    size32_t offRight = 0;
    for (;;)
    {
        if ((offLeft == lenLeft) || (offRight == lenRight))
        {
            if ((offLeft == lenLeft) && (offRight == lenRight))
                return 0;
            if (offLeft == lenLeft)
                return -1;
            else
                return +1;
        }

        int rc = child->compare(ptrLeft + offLeft, ptrRight + offRight);
        if (rc != 0)
            return rc;

        offLeft += child->size(ptrLeft + offLeft, ptrLeft + offLeft);
        offRight += child->size(ptrRight + offRight, ptrRight + offRight);
    }
}

unsigned RtlDatasetTypeInfo::hash(const byte * self, unsigned inhash) const
{
    if (isLinkCounted())
    {
        const size32_t count = rtlReadSize32t(self);
        self += sizeof(size32_t);
        const byte * * rows = (const byte * *) self;
        for (size32_t row = 0; row < count; row++)
        {
            inhash = child->hash(rows[row], inhash);
        }
    }
    else
    {
        size32_t len = rtlReadSize32t(self);
        self += sizeof(size32_t);
        for (size32_t offset = 0; offset < len; offset += child->size(self + offset, self + offset))
        {
            inhash = child->hash(self + offset, inhash);
        }
    }
    return inhash;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlDictionaryTypeInfo::getMinSize() const
{
    if (isLinkCounted())
        return sizeof(size32_t) + sizeof(void * *);
    return sizeof(size32_t);
}

size32_t RtlDictionaryTypeInfo::size(const byte * self, const byte * selfrow) const
{
    if (isLinkCounted())
        return sizeof(size32_t) + sizeof(void * *);
    return sizeof(size32_t) + rtlReadSize32t(self);
}

size32_t RtlDictionaryTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    source.processBeginDataset(field);
    if (isLinkCounted())
    {
        // a 32-bit record count, and a pointer to an hash table with record pointers
        size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
        builder.ensureCapacity(offset+sizeInBytes, queryName(field));
        Owned<IEngineRowAllocator> childAllocator = builder.queryAllocator()->createChildRowAllocator(child);
        CHThorHashLookupInfo hashInfo(*static_cast<const RtlRecordTypeInfo *>(child));
        RtlLinkedDictionaryBuilder dictBuilder(childAllocator, &hashInfo);
        RtlFieldStrInfo dummyField("<nested row>", NULL, child);
        while (source.processNextRow(field))
        {
            RtlDynamicRowBuilder childBuilder(childAllocator);
            size32_t childLen = child->build(childBuilder, 0, &dummyField, source);
            dictBuilder.appendOwn((void *) childBuilder.finalizeRowClear(childLen));
        }
        // Go back in and patch the count
        rtlWriteInt4(builder.getSelf()+offset, dictBuilder.getcount());
        * ( const void * * ) (builder.getSelf()+offset+sizeof(size32_t)) = dictBuilder.linkrows();
        offset += sizeInBytes;
    }
    else
        throwUnexpected();  // And may never be...
    source.processEndDataset(field);
    return offset;
}

size32_t RtlDictionaryTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isLinkCounted())
    {
        size32_t thisCount = rtlReadSize32t(self);
        if (target.processBeginDataset(field, thisCount))
        {
            const byte * * rows = *reinterpret_cast<const byte * * const *>(self + sizeof(size32_t));
            for (unsigned i= 0; i < thisCount; i++)
            {
                const byte * row = rows[i];
                if (row)
                    child->process(row, row, field, target);
            }
            target.processEndDataset(field);
        }
        return sizeof(size32_t) + sizeof(void * *);
    }
    else
    {
        //MORE: We could interpret serialized dictionaries if there was ever a need
        throwUnexpected();
    }
}

size32_t RtlDictionaryTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    StringAttr outerTag;
    if (hasOuterXPath(field))
    {
        queryNestedOuterXPath(outerTag, field);
        target.outputBeginNested(outerTag, false);
    }

    const char *innerPath = queryXPath(field);
    target.outputBeginArray(innerPath);

    unsigned thisSize;
    if (isLinkCounted())
    {
        size32_t thisCount = rtlReadSize32t(self);
        const byte * * rows = *reinterpret_cast<const byte * * const *>(self + sizeof(size32_t));
        for (unsigned i= 0; i < thisCount; i++)
        {
            const byte * row = rows[i];
            if (row)
                child->toXML(row, row, field, target);
        }
        thisSize = sizeof(size32_t) + sizeof(void * *);
    }
    else
    {
        //MORE: We could interpret serialized dictionaries if there was ever a need
        throwUnexpected();
    }

    target.outputEndArray(innerPath);
    if (outerTag)
        target.outputEndNested(outerTag);

    return thisSize;
}

size32_t RtlDictionaryTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (isLinkCounted())
    {
        //Currently inefficient because it is recreating deserializers and resolving child allocators each time it is called.
        ICodeContext * ctx = nullptr; // Slightly dodgy, but not needed if the child deserializers are also calculated
        unsigned activityId = 0;

        // a 32-bit record count, and a pointer to an hash table with record pointers
        size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
        byte * dest = builder.ensureCapacity(offset + sizeInBytes, nullptr) + offset;
        Owned<IEngineRowAllocator> childAllocator = builder.queryAllocator()->createChildRowAllocator(child);
        Owned<IOutputRowDeserializer> deserializer = childAllocator->queryOutputMeta()->createDiskDeserializer(ctx, activityId);
        rtlDeserializeChildDictionary(*(size32_t *)dest, *(const byte * * *)(dest + sizeof(size32_t)), childAllocator, deserializer, in);
        return offset + sizeInBytes;
    }
    else
    {
        UNIMPLEMENTED;
    }
}

void RtlDictionaryTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    size32_t size = in.readSize();
    in.skip(size);
}


int RtlDictionaryTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isLinkCounted())
    {
        const size32_t countLeft = rtlReadSize32t(left);
        const size32_t countRight = rtlReadSize32t(right);
        const byte * * rowsLeft = (const byte * *)((const byte *)left + sizeof(size32_t));
        const byte * * rowsRight = (const byte * *)((const byte *)right + sizeof(size32_t));
        size32_t leftRow = 0;
        size32_t rightRow = 0;
        for (;;)
        {
            //Dictionaries are compared as datasets => skip until the first non-null entry
            while ((leftRow != countLeft) && !rowsLeft[leftRow])
                leftRow++;

            while ((rightRow != countRight) && !rowsRight[rightRow])
                rightRow++;

            if ((leftRow == countLeft) || (rightRow == countRight))
            {
                if ((leftRow == countLeft) && (rightRow == countRight))
                    return 0;
                if (leftRow == countLeft)
                    return -1;
                else
                    return +1;
            }

            int rc = child->compare(rowsLeft[leftRow], rowsRight[rightRow]);
            if (rc != 0)
                return rc;
            leftRow++;
            rightRow++;
        }
    }
    else
    {
        //Non LCR dictionaries are not supported.
        throwUnexpected();
    }
}

unsigned RtlDictionaryTypeInfo::hash(const byte * self, unsigned inhash) const
{
    if (isLinkCounted())
    {
        const size32_t count = rtlReadSize32t(self);
        self += sizeof(size32_t);
        const byte * * rows = (const byte * *) self;
        size32_t row = 0;
        for (;;)
        {
            //Dictionaries are compared as datasets => skip until the first non-null entry
            while ((row != count) && !rows[row])
                row++;
            if (row == count)
                break;
            inhash = child->hash(rows[row], inhash);
            row++;
        }
    }
    else
    {
        //Non LCR dictionaries are not supported.
        throwUnexpected();
    }
    return inhash;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlIfBlockTypeInfo::getMinSize() const
{
    return 0;
}

size32_t RtlIfBlockTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (getCondition(selfrow))
        return sizeFields(fields, self, selfrow);
    return 0;
}

size32_t RtlIfBlockTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    throwUnexpected();
}

size32_t RtlIfBlockTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    throwUnexpected();
}

size32_t RtlIfBlockTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (getCondition(selfrow))
        return processFields(fields, self, selfrow, target);
    return 0;
}

size32_t RtlIfBlockTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    if (getCondition(selfrow))
        return toXMLFields(fields, self, selfrow, target);
    return 0;
}

size32_t RtlIfBlockTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (getCondition(builder.getSelf()))
        return deserializeFields(fields, builder, in, offset);
    return offset;
}

void RtlIfBlockTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    if (getCondition(in.querySelf()))
        readAheadFields(fields, in);
}


void RtlIfBlockTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    //MORE: Should this fail instead?
    resultLen = 0;
    result = nullptr;
}

void RtlIfBlockTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    //MORE: Should this fail instead?
    resultLen = 0;
    result = nullptr;
}

__int64 RtlIfBlockTypeInfo::getInt(const void * ptr) const
{
    //MORE: Should this fail instead?
    return 0;
}

int RtlIfBlockTypeInfo::compare(const byte * left, const byte * right) const
{
    bool includeLeft = getCondition(left);
    bool includeRight = getCondition(right);
    if (includeLeft && includeRight)
        return compareFields(fields, left, right);

    //If the fields are all blank then this isn't actually correct, but that is unlikely to be a problem
    if (includeLeft)
        return +1;
    else if (includeRight)
        return -1;
    else
        return 0;
}

unsigned RtlIfBlockTypeInfo::hash(const byte * self, unsigned inhash) const
{
    bool included = getCondition(self);
    if (included)
        inhash = hashFields(fields, self, inhash);
    return inhash;
}

bool RtlComplexIfBlockTypeInfo::getCondition(const RtlRow & selfrow) const
{
    //Call the function in the derived class that evaluates the test condition
    return getCondition(selfrow.queryRow());
}


static CriticalSection ifcs;
RtlSerialIfBlockTypeInfo::~RtlSerialIfBlockTypeInfo()
{
    ::Release(filter);
    delete parentRecord;
}

void RtlSerialIfBlockTypeInfo::doCleanup() const
{
    delete parentRecord;
    parentRecord = nullptr;
    ::Release(filter);
    filter = nullptr;
    RtlIfBlockTypeInfo::doCleanup();
}


bool RtlSerialIfBlockTypeInfo::getCondition(const byte * selfrow) const
{
    const IFieldFilter * filter = resolveCondition();
    RtlDynRow row(*parentRecord, nullptr);
    //Can only expand offset for fields up to and including this if block - otherwise it will call getCondition() again...
    row.setRow(selfrow, numPrevFields);
    return filter->matches(row);
}

bool RtlSerialIfBlockTypeInfo::getCondition(const RtlRow & selfrow) const
{
    return resolveCondition()->matches(selfrow);
}

const IFieldFilter * RtlSerialIfBlockTypeInfo::queryFilter() const
{
    return resolveCondition();
}

const IFieldFilter * RtlSerialIfBlockTypeInfo::resolveCondition() const
{
    //The following cast is a hack to avoid <atomic> being included from the header
    //if parentRecord is not initialised then may need to create the filter condition
    typedef std::atomic<RtlRecord *> AtomicRtlRecord;
    AtomicRtlRecord & atomicParentRecord = *reinterpret_cast<AtomicRtlRecord *>(&parentRecord);
    RtlRecord * parent = atomicParentRecord.load(std::memory_order_relaxed);
    if (!parent)
    {
        CriticalBlock block(ifcs);
        parent = atomicParentRecord.load(std::memory_order_relaxed);
        if (!parent)
        {
            if (!filter)
                filter = createCondition();
            parent = new RtlRecord(*rowType, true);
            atomicParentRecord.store(parent, std::memory_order_relaxed);
            numPrevFields = parentRecord->queryIfBlockLimit(this);
        }
    }
    return filter;
}

IFieldFilter * RtlDynamicIfBlockTypeInfo::createCondition() const
{
    //The filter should be initialised on deserialization
    UNIMPLEMENTED;
}



//-------------------------------------------------------------------------------------------------------------------

__int64 RtlBitfieldTypeInfo::signedValue(const void * self) const
{
    __int64 value = rtlReadInt(self, getBitfieldIntSize());
    unsigned shift = getBitfieldShift();
    unsigned numBits = getBitfieldNumBits();
    unsigned bitsInValue = sizeof(value) * 8;
    value <<= (bitsInValue - shift - numBits);
    return value >> (bitsInValue - numBits);
}


unsigned __int64 RtlBitfieldTypeInfo::unsignedValue(const void * self) const
{
    unsigned __int64 value = rtlReadUInt(self, getBitfieldIntSize());
    unsigned shift = getBitfieldShift();
    unsigned numBits = getBitfieldNumBits();
    unsigned bitsInValue = sizeof(value) * 8;
    value <<= (bitsInValue - shift - numBits);
    return value >> (bitsInValue - numBits);
}


size32_t RtlBitfieldTypeInfo::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    builder.ensureCapacity(length+offset, queryName(field));
    byte * cur = builder.getSelf() + offset;
    unsigned __int64 value = rtlReadUInt(builder.getSelf(), getBitfieldIntSize());
    unsigned shift = getBitfieldShift();
    unsigned numBits = getBitfieldNumBits();
    unsigned __int64 mask = (U64C(1) << numBits) - 1;

    value &= ~(mask << shift);
    value |= ((val << shift) & mask);

    rtlWriteInt(cur, val, getBitfieldIntSize());
    return offset + getSize();
}

size32_t RtlBitfieldTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlBitfieldTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    size32_t size = rtlUtf8Length(len, value);
    return buildInt(builder, offset, field, rtlStrToInt8(size, value));
}

size32_t RtlBitfieldTypeInfo::getMinSize() const
{
    if (fieldType & RFTMislastbitfield)
        return getBitfieldIntSize();
    return 0;
}

size32_t RtlBitfieldTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (fieldType & RFTMislastbitfield)
        return getBitfieldIntSize();
    return 0;
}

size32_t RtlBitfieldTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isUnsigned())
        target.processUInt(unsignedValue(self), field);
    else
        target.processInt(signedValue(self), field);
    return getSize();
}

size32_t RtlBitfieldTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    size32_t fieldsize = getBitfieldIntSize();
    if (isUnsigned())
        target.outputUInt(unsignedValue(self), fieldsize, queryScalarXPath(field));
    else
        target.outputInt(signedValue(self), fieldsize, queryScalarXPath(field));

    if (fieldType & RFTMislastbitfield)
        return fieldsize;
    return 0;
}

void RtlBitfieldTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    if (isUnsigned())
        rtlUInt8ToStrX(resultLen, result,  unsignedValue(ptr));
    else
        rtlInt8ToStrX(resultLen, result,  signedValue(ptr));
}

void RtlBitfieldTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    getString(resultLen, result, ptr);
}

__int64 RtlBitfieldTypeInfo::getInt(const void * ptr) const
{
    if (isUnsigned())
        return (__int64)unsignedValue(ptr);
    else
        return signedValue(ptr);
}

int RtlBitfieldTypeInfo::compare(const byte * left, const byte * right) const
{
    if (isUnsigned())
    {
        unsigned __int64 leftValue = unsignedValue(left);
        unsigned __int64 rightValue = unsignedValue(right);
        return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
    }
    else
    {
        __int64 leftValue = signedValue(left);
        __int64 rightValue = signedValue(right);
        return (leftValue < rightValue) ? -1 : (leftValue > rightValue) ? +1 : 0;
    }
}

unsigned RtlBitfieldTypeInfo::hash(const byte * self, unsigned inhash) const
{
    __int64 val = getInt(self);
    return rtlHash32Data8(&val, inhash);
}

size32_t RtlBitfieldTypeInfo::getSize() const
{
    if (fieldType & RFTMislastbitfield)
        return getBitfieldIntSize();
    return 0;
}


//-------------------------------------------------------------------------------------------------------------------

size32_t RtlUnimplementedTypeInfo::getMinSize() const
{
    rtlFailUnexpected();
}

size32_t RtlUnimplementedTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    rtlFailUnexpected();
}

size32_t RtlUnimplementedTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    rtlFailUnexpected();
}

size32_t RtlUnimplementedTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    rtlFailUnexpected();
}

size32_t RtlUnimplementedTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    rtlFailUnexpected();
}

size32_t RtlUnimplementedTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    rtlFailUnexpected();
}

size32_t RtlUnimplementedTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    rtlFailUnexpected();
    return offset;
}

void RtlUnimplementedTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    rtlFailUnexpected();
}


void RtlUnimplementedTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    resultLen = 0;
    result = nullptr;
    rtlFailUnexpected();
}

void RtlUnimplementedTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    resultLen = 0;
    result = nullptr;
    rtlFailUnexpected();
}

__int64 RtlUnimplementedTypeInfo::getInt(const void * ptr) const
{
    rtlFailUnexpected();
    return 0;
}

int RtlUnimplementedTypeInfo::compare(const byte * left, const byte * right) const
{
    rtlFailUnexpected();
}

unsigned RtlUnimplementedTypeInfo::hash(const byte * self, unsigned inhash) const
{
    rtlFailUnexpected();
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlAlienTypeInfo::getMinSize() const
{
    rtlFailUnexpected();
}

size32_t RtlAlienTypeInfo::size(const byte * self, const byte * selfrow) const
{
    if (isFixedSize())
        return length;
    rtlFailUnexpected();
}

size32_t RtlAlienTypeInfo::buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const
{
    rtlFailUnexpected();
}

size32_t RtlAlienTypeInfo::buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const
{
    rtlFailUnexpected();
}

size32_t RtlAlienTypeInfo::buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const
{
    rtlFailUnexpected();
}

size32_t RtlAlienTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    rtlFailUnexpected();
}

size32_t RtlAlienTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    rtlFailUnexpected();
}

size32_t RtlAlienTypeInfo::deserialize(ARowBuilder & builder, IRowDeserializerSource & in, size32_t offset) const
{
    if (isFixedSize())
        return RtlCompoundTypeInfo::deserialize(builder, in, offset);
    rtlFailUnexpected();
}

void RtlAlienTypeInfo::readAhead(IRowPrefetcherSource & in) const
{
    if (isFixedSize())
        in.skip(length);
    else
        rtlFailUnexpected();
}


void RtlAlienTypeInfo::getString(size32_t & resultLen, char * & result, const void * ptr) const
{
    resultLen = 0;
    result = nullptr;
    rtlFailUnexpected();
}

void RtlAlienTypeInfo::getUtf8(size32_t & resultLen, char * & result, const void * ptr) const
{
    resultLen = 0;
    result = nullptr;
    rtlFailUnexpected();
}

__int64 RtlAlienTypeInfo::getInt(const void * ptr) const
{
    rtlFailUnexpected();
    return 0;
}

int RtlAlienTypeInfo::compare(const byte * left, const byte * right) const
{
    rtlFailUnexpected();
}

unsigned RtlAlienTypeInfo::hash(const byte * self, unsigned inhash) const
{
    rtlFailUnexpected();
}

//-------------------------------------------------------------------------------------------------------------------

RtlFieldStrInfo::RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, unsigned _flags, const char *_initializer)
: RtlFieldInfo(_name, _xpath, _type, _flags, _initializer)
{
}

RtlFieldStrInfo::RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, unsigned _flags)
: RtlFieldInfo(_name, _xpath, _type, _flags, NULL)
{
}

RtlFieldStrInfo::RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type)
: RtlFieldInfo(_name, _xpath, _type, 0, NULL)
{
}

RtlFieldStrInfo::RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, const char *_initializer)
: RtlFieldInfo(_name, _xpath, _type, 0, _initializer)
{
}

unsigned ECLRTL_API countFields(const RtlFieldInfo * const * fields)
{
    unsigned cnt = 0;
    for (;*fields;fields++)
        cnt++;
    return cnt;
}

//-------------------------------------------------------------------------------------------------------------------

static const unsigned sameTypeMask = RFTMkind|RFTMebcdic;
size32_t translateScalar(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, const RtlTypeInfo &destType, const RtlTypeInfo &sourceType, const byte *source)
{
    switch(destType.getType())
    {
    case type_boolean:
    case type_int:
    case type_swapint:
    case type_packedint:
    case type_filepos:
    case type_keyedint:
        offset = destType.buildInt(builder, offset, field, sourceType.getInt(source));
        break;
    case type_real:
        offset = destType.buildReal(builder, offset, field, sourceType.getReal(source));
        break;
    case type_data:
    case type_string:
        //Special case if source and destination types are identical to avoid cloning strings
        if ((destType.fieldType & sameTypeMask) == (sourceType.fieldType & sameTypeMask))
        {
            size32_t length = sourceType.length;
            if (!sourceType.isFixedSize())
            {
                length = rtlReadSize32t(source);
                source += sizeof(size32_t);
            }
            offset = destType.buildString(builder, offset, field, length, (const char *)source);
            break;
        }
        //fallthrough
    case type_decimal:  // Go via string - not common enough to special-case
    case type_varstring:
    case type_qstring:
    {
        size32_t size;
        rtlDataAttr text;
        sourceType.getString(size, text.refstr(), source);
        offset = destType.buildString(builder, offset, field, size, text.getstr());
        break;
    }
    case type_utf8:
        //MORE: Could special case casting from utf8 to utf8 similar to strings above
    case type_unicode:
    case type_varunicode:
    {
        size32_t utf8chars;
        rtlDataAttr utf8Text;
        sourceType.getUtf8(utf8chars, utf8Text.refstr(), source);
        offset = destType.buildUtf8(builder, offset, field, utf8chars, utf8Text.getstr());
        break;
    }
    case type_set:
    {
        bool isAll = *(bool *) source;
        source+= sizeof(bool);
        byte *dest = builder.ensureCapacity(offset+sizeof(bool)+sizeof(size32_t), field->name)+offset;
        *(size32_t *) (dest + sizeof(bool)) = 0; // Patch later when size known
        offset += sizeof(bool) + sizeof(size32_t);
        if (isAll)
        {
            *(bool*) dest = true;
        }
        else
        {
            *(bool*) dest = false;
            size32_t sizeOffset = offset - sizeof(size32_t);  // Where we need to patch
            size32_t childSize = *(size32_t *)source;
            source += sizeof(size32_t);
            const byte *initialSource = source;
            size32_t initialOffset = offset;
            const RtlTypeInfo *destChildType = destType.queryChildType();
            const RtlTypeInfo *sourceChildType = sourceType.queryChildType();
            while ((size_t)(source - initialSource) < childSize)
            {
                offset = translateScalar(builder, offset, field, *destChildType, *sourceChildType, source);
                source += sourceChildType->size(source, nullptr); // MORE - shame to repeat a calculation that the translate above almost certainly just did
            }
            dest = builder.getSelf() + sizeOffset;  // Note - man have been moved by reallocs since last calculated
            *(size32_t *)dest = offset - initialOffset;
        }
        break;
    }
    default:
        throwUnexpected();
    }
    return offset;
}

//-------------------------------------------------------------------------------------------------------------------


/* 

Stack:
* Change hqlhtcpp so that the correct derived classes are generated.
* Test so that toXML calls the default implementaions and check that the same values are generated.  (Don't if contains ifblocks/alien)
* Release
* Think about bitfields - how do I know it is the last bitfield, how am I going to keep track of the offsets.
* What code would need to be generated for alien datatypes.
* Could have alien int and alien string varieties????
* What would an ecl interpreter look like (a special workunit?)  helpers are interpreted?  What about the graph?

* Could I add associations to register user attributes - so a callback could know when they were assigned to?
* Could I add ctx->noteLocation() into the generated code - so could put breakpoints on variables.
* Add annotation when a member of the target dataset is updated.

ctx->noteFieldAssigned(self, <field>);              - does this include temporary datasets?
ctx->noteAttributeX(<name>, Int|String|Unicode|
ctx->noteLocation(location);                        - reduce locations, so only one returned per line for a non-dataset?  Should it just be the first item on the line that is tagged??
* Need static information about the breakpoints so debugger knows where to put valid brakpoints....
* Debugger will want to know about the type of the breakpoints.
* Should try and compress the location format - possibly have a table of <module.attributes>-># with breakpoint as 12:23

Also need some information about which datasets, and stored variables etc. are used so they can be displayed.
- Most datasets can be deduced from the parameters passed into the transform
- Some are trickier e.g., the extract, could possibly define some mappings
- options to disable projects/other more complex operations inline (so easier to walk through)

Bitfields:
- Two separate questions:
i) How is the meta information generated.
ii) How is it stored internally in an IHqlExpression * ?

* Could store the offset in the type - either in the base type of as a qualifier.
  + much easier code generation.
  - Doesn't provie an easy indication of the last field in a bitfield (because can't really modify after the fact)
  - Problematic when fields are removed to merge them.

* Could add a bitfield container to the record.
  + Makes it easier to handle the last bitfield
  + Matches the structure used for the cursor.
  - Everything needs to walk the bitfield containers similar to ifblocks.
  - Makes it just as tricky to merge
  - Harder to create the record, unless the code is implicitly handled by appendOperand().

* The type of no_select could contain a modifier to indicate the offset/islast
  + the type of a no_select would have a 1:1 mapping with type info.
  - A bit complicated to calculate, especially when it isn't used much of the time

=> On Reflection is is probably easiest to keep the structure as it is (some comments should go in hqlexpr to avoid revisiting).

* interperet bitfield offsets and "is last bitfield" dynamically
  + Greatly simplifies generating the meta - you can always use the field.
  - Requires another parameter and significant extra complexity for the uncommon case.  (especially incrementing self)

* Could generate from the expanded record instead of walking the record structure directly
  + That already knows how the bitfields are allocated, and could easily know which is the last field.
  - A field is no longer sufficient as key fr searching for the information.  
  - Best would be a createFieldTypeKey(select-expr) which returns field when approriate, or modified if a bitfield.  Then the pain is localised.
  
* Output a bitfield container item into the type information
  + Solves the size problem
  - Individual bitfields still need to know their offsets, so doesn't solve the full problem.

=>
    Change so that either use meta to generate the information, or use no_select when appropriate to fidn out the nesc. information.
    Probably the latter for the moment.

    a) Create a key function and make sure it is always used.
    b) Need to work out how to generate no_ifblock.
      - ifblock is context dependent, so need to generate as part of the parent record, and in the parent record context.
*/

