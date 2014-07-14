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

#include "platform.h"
#include <math.h>
#include <stdio.h>
#include "jmisc.hpp"
#include "jlib.hpp"
#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlds_imp.hpp"
#include "nbcd.hpp"

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
    return field->name->str();
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

const char * RtlTypeInfoBase::queryLocale() const 
{
    return NULL; 
}

const RtlFieldInfo * const * RtlTypeInfoBase::queryFields() const 
{
    return NULL; 
}

const RtlTypeInfo * RtlTypeInfoBase::queryChildType() const 
{
    return NULL; 
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlBoolTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    builder.ensureCapacity(sizeof(bool)+offset, field->name->str());
    bool val = source.getBooleanResult(field);
    * (bool *) (builder.getSelf() + offset) = val;
    offset += sizeof(bool);
    return offset;
}

size32_t RtlBoolTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    target.processBool(*(const bool *)self, field);
    return sizeof(bool);
}

size32_t RtlBoolTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    target.outputBool(*(const bool *)self, queryXPath(field));
    return sizeof(bool);
}

//-------------------------------------------------------------------------------------------------------------------

double RtlRealTypeInfo::value(const byte * self) const
{
    if (length == 4)
        return *(const float *)self;
    return *(const double *)self;
}

size32_t RtlRealTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    builder.ensureCapacity(length+offset, field->name->str());
    double val = source.getRealResult(field);
    byte *dest = builder.getSelf() + offset;
    if (length == 4)
        *(float *) dest = (float) val;
    else
        *(double *) dest = val;
    offset += length;
    return offset;
}

size32_t RtlRealTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    target.processReal(value(self), field);
    return length;
}

size32_t RtlRealTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    target.outputReal(value(self), queryXPath(field));
    return length;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlIntTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    builder.ensureCapacity(length+offset, field->name->str());
    __int64 val = isUnsigned() ? (__int64) source.getUnsignedResult(field) : source.getSignedResult(field);
    rtlWriteInt(builder.getSelf() + offset, val, length);
    offset += length;
    return offset;
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
        target.outputUInt(rtlReadUInt(self, length), queryXPath(field));
    else
        target.outputInt(rtlReadInt(self, length), queryXPath(field));
    return length;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlSwapIntTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    builder.ensureCapacity(length+offset, field->name->str());
    __int64 val = isUnsigned() ? (__int64) source.getUnsignedResult(field) : source.getSignedResult(field);
    // NOTE - we assume that the value returned from the source is already a swapped int
    rtlWriteInt(builder.getSelf() + offset, val, length);
    offset += length;
    return offset;
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
        target.outputUInt(rtlReadSwapUInt(self, length), queryXPath(field));
    else
        target.outputInt(rtlReadSwapInt(self, length), queryXPath(field));
    return length;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlPackedIntTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    return rtlGetPackedSize(self); 
}

size32_t RtlPackedIntTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    unsigned __int64 value = isUnsigned() ? (__int64) source.getUnsignedResult(field) : source.getSignedResult(field);
    size32_t sizeInBytes = rtlGetPackedSize(&value);
    builder.ensureCapacity(sizeInBytes+offset, field->name->str());
    rtlSetPackedUnsigned(builder.getSelf() + offset, value);
    offset += sizeInBytes;
    return offset;
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
    if (isUnsigned())
        target.outputUInt(rtlGetPackedUnsigned(self), queryXPath(field));
    else
        target.outputInt(rtlGetPackedSigned(self), queryXPath(field));
    return rtlGetPackedSize(self); 
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlStringTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return length;
    return sizeof(size32_t) + rtlReadUInt4(self);
}

size32_t RtlStringTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t size;
    char *value;
    source.getStringResult(field, size, value);
    if (!isFixedSize())
    {
        builder.ensureCapacity(offset+size+sizeof(size32_t), field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, size);
#if 0
        // NOTE - you might argue that we should convert the incoming data to EBCDIC. But it seems more useful to
        // define the semantics as being that the IFieldSource should return EBCDIC if you have declared the matching field as EBCDIC
        // (otherwise, why did you bother?)
        if (isEbcdic())
            rtlStrToEStr(size, (char *) dest+sizeof(size32_t), size, (char *)value);
        else
#endif
            memcpy(dest+sizeof(size32_t), value, size);
        offset += size+sizeof(size32_t);
    }
    else
    {
        builder.ensureCapacity(offset+length, field->name->str());
        byte *dest = builder.getSelf()+offset;
#if 0
        // See above...
        if (isEbcdic())
            rtlStrToEStr(length, (char *) dest, size, (char *) value);
        else
#endif
            rtlStrToStr(length, dest, size, value);
        offset += length;
    }
    rtlFree(value);
    return offset;
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
        thisLength = rtlReadUInt4(self);
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
        thisLength = rtlReadUInt4(self);
        thisSize = sizeof(size32_t) + thisLength;
    }

    if (isEbcdic())
    {
        unsigned lenAscii;
        rtlDataAttr ascii;
        rtlEStrToStrX(lenAscii, ascii.refstr(), thisLength, str);
        target.outputString(lenAscii, ascii.getstr(), queryXPath(field));
    }
    else
    {
        target.outputString(thisLength, str, queryXPath(field));
    }
    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlDataTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return length;
    return sizeof(size32_t) + rtlReadUInt4(self);
}

size32_t RtlDataTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t size;
    void *value;
    source.getDataResult(field, size, value);
    if (!isFixedSize())
    {
        builder.ensureCapacity(offset+size+sizeof(size32_t), field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, size);
        memcpy(dest+sizeof(size32_t), value, size);
        offset += size+sizeof(size32_t);
    }
    else
    {
        builder.ensureCapacity(offset+length, field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlDataToData(length, dest, size, value);
        offset += length;
    }
    rtlFree(value);
    return offset;
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
        thisLength = rtlReadUInt4(self);
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
        thisLength = rtlReadUInt4(self);
        thisSize = sizeof(size32_t) + thisLength;
    }

    target.outputData(thisLength, str, queryXPath(field));
    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

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
    char *value;
    source.getStringResult(field, size, value);
    if (!isFixedSize())
    {
        builder.ensureCapacity(offset+size+1, field->name->str());
        // See notes re EBCDIC conversion in RtlStringTypeInfo code
        byte *dest = builder.getSelf()+offset;
        memcpy(dest, value, size);
        dest[size] = '\0';
        offset += size+1;
    }
    else
    {
        builder.ensureCapacity(offset+length, field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlStrToVStr(length, dest, size, value);
        offset += length;
    }
    rtlFree(value);
    return offset;
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
        target.outputString(lenAscii, ascii.getstr(), queryXPath(field));
    }
    else
        target.outputString(thisLength, str, queryXPath(field));

    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlQStringTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return rtlQStrSize(length);
    return sizeof(size32_t) + rtlQStrSize(rtlReadUInt4(self));
}

size32_t RtlQStringTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t size;
    char *value;
    source.getStringResult(field, size, value);
    if (!isFixedSize())
    {
        size32_t sizeInBytes = rtlQStrSize(size) + sizeof(size32_t);
        builder.ensureCapacity(offset+sizeInBytes, field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, size);
        rtlStrToQStr(size, (char *) dest+sizeof(size32_t), size, value);
        offset += sizeInBytes;
    }
    else
    {
        size32_t sizeInBytes = rtlQStrSize(length);
        builder.ensureCapacity(offset+sizeInBytes, field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlStrToQStr(length, (char *) dest, size, value);
        offset += sizeInBytes;
    }
    rtlFree(value);
    return offset;
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
        thisLength = rtlReadUInt4(self);
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
        thisLength = rtlReadUInt4(self);
        thisSize = sizeof(size32_t) + rtlQStrSize(thisLength);
    }

    target.outputQString(thisLength, str, queryXPath(field));
    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlDecimalTypeInfo::calcSize() const
{
    if (isUnsigned())
        return (getDecimalDigits()+1)/2;
    return (getDecimalDigits()+2)/2;
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
    builder.ensureCapacity(sizeInBytes+offset, field->name->str());
    if (isUnsigned())
        value.getUDecimal(sizeInBytes, getDecimalPrecision(), builder.getSelf()+offset);
    else
        value.getDecimal(sizeInBytes, getDecimalPrecision(), builder.getSelf()+offset);
    offset += sizeInBytes;
    return offset;
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
        target.outputUDecimal(self, thisSize, getDecimalPrecision(), queryXPath(field));
    else
        target.outputDecimal(self, thisSize, getDecimalPrecision(), queryXPath(field));
    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlCharTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    throwUnexpected();  // Can't have a field of type char
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
    target.outputString(1, &c, queryXPath(field));
    return 1;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlUnicodeTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isFixedSize())
        return length * sizeof(UChar);
    return sizeof(size32_t) + rtlReadUInt4(self) * sizeof(UChar);
}

size32_t RtlUnicodeTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t sizeInChars;
    UChar *value;
    source.getUnicodeResult(field, sizeInChars, value);
    if (!isFixedSize())
    {
        size32_t sizeInBytes = sizeInChars * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes+sizeof(size32_t), field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlWriteInt4(dest, sizeInChars);  // NOTE - in chars!
        memcpy(dest+sizeof(size32_t), value, sizeInBytes);
        offset += sizeInBytes+sizeof(size32_t);
    }
    else
    {
        size32_t sizeInBytes = length * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlUnicodeToUnicode(length, (UChar *) dest, sizeInChars, value);
        offset += sizeInBytes;
    }
    rtlFree(value);
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
        thisLength = rtlReadUInt4(self);
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
        thisLength = rtlReadUInt4(self);
        thisSize = sizeof(size32_t) + thisLength * sizeof(UChar);
    }

    target.outputUnicode(thisLength, ustr, queryXPath(field));
    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

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
        builder.ensureCapacity(offset+sizeInBytes, field->name->str());
        UChar *dest = (UChar *) builder.getSelf()+offset;
        memcpy(dest, value, sizeInBytes - sizeof(UChar));
        dest[sizeInChars] = 0;
        offset += sizeInBytes;
    }
    else
    {
        size32_t sizeInBytes = length * sizeof(UChar);
        builder.ensureCapacity(offset+sizeInBytes, field->name->str());
        byte *dest = builder.getSelf()+offset;
        rtlUnicodeToVUnicode(length, (UChar *) dest, sizeInChars, value);
        offset += sizeInBytes;
    }
    rtlFree(value);
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

    target.outputUnicode(thisLength, ustr, queryXPath(field));
    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlUtf8TypeInfo::size(const byte * self, const byte * selfrow) const 
{
    assertex(!isFixedSize());
    return sizeof(size32_t) + rtlUtf8Size(rtlReadUInt4(self), self+sizeof(unsigned));
}

size32_t RtlUtf8TypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    size32_t sizeInChars;
    char *value;
    source.getUTF8Result(field, sizeInChars, value);
    size32_t sizeInBytes = rtlUtf8Size(sizeInChars, value);
    assertex(!isFixedSize());
    builder.ensureCapacity(offset+sizeInBytes+sizeof(size32_t), field->name->str());
    byte *dest = builder.getSelf()+offset;
    rtlWriteInt4(dest, sizeInChars);  // NOTE - in chars!
    memcpy(dest+sizeof(size32_t), value, sizeInBytes);
    offset += sizeInBytes+sizeof(size32_t);
    rtlFree(value);
    return offset;
}

size32_t RtlUtf8TypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    assertex(!isFixedSize());
    const char * str = reinterpret_cast<const char *>(self + sizeof(size32_t));
    unsigned thisLength = rtlReadUInt4(self);
    unsigned thisSize = sizeof(size32_t) + rtlUtf8Size(thisLength, str);

    target.processUtf8(thisLength, str, field);
    return thisSize;
}

size32_t RtlUtf8TypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    assertex(!isFixedSize());
    const char * str = reinterpret_cast<const char *>(self + sizeof(size32_t));
    unsigned thisLength = rtlReadUInt4(self);
    unsigned thisSize = sizeof(size32_t) + rtlUtf8Size(thisLength, str);

    target.outputUtf8(thisLength, str, queryXPath(field));
    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

inline size32_t sizeFields(const RtlFieldInfo * const * cur, const byte * self, const byte * selfrow)
{
    unsigned offset = 0;
    loop
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        offset += child->size(self+offset, selfrow);
        cur++;
    }
    return offset;
}

inline size32_t processFields(const RtlFieldInfo * const * cur, const byte * self, const byte * selfrow, IFieldProcessor & target)
{
    unsigned offset = 0;
    loop
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

inline size32_t buildFields(const RtlFieldInfo * const * cur, ARowBuilder &builder, size32_t offset, IFieldSource &source)
{
    loop
    {
        const RtlFieldInfo * child = *cur;
        if (!child)
            break;
        offset = child->build(builder, offset, source);
        cur++;
    }
    return offset;
}


inline size32_t toXMLFields(const RtlFieldInfo * const * cur, const byte * self, const byte * selfrow, IXmlWriter & target)
{
    size32_t offset = 0;
    loop
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

//-------------------------------------------------------------------------------------------------------------------

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

size32_t RtlRecordTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    source.processBeginRow(field);
    offset = buildFields(fields, builder, offset, source);
    source.processEndRow(field);
    return offset;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlSetTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    return sizeof(bool) + sizeof(size32_t) + rtlReadUInt4(self + sizeof(bool));
}

size32_t RtlSetTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    bool isAll;
    source.processBeginSet(field, isAll);
    size32_t sizeInBytes = sizeof(bool) + sizeof(size32_t);
    builder.ensureCapacity(offset+sizeInBytes, field->name->str());
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
    unsigned max = offset + rtlReadUInt4(self + sizeof(bool));
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
    unsigned max = offset + rtlReadUInt4(self + sizeof(bool));

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

//-------------------------------------------------------------------------------------------------------------------

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

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlDatasetTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (isLinkCounted())
        return sizeof(size32_t) + sizeof(void * *);
    return sizeof(size32_t) + rtlReadUInt4(self);
}

size32_t RtlDatasetTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    source.processBeginDataset(field);
    if (isLinkCounted())
    {
        // a 32-bit record count, and a pointer to an array of record pointers
        size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
        builder.ensureCapacity(offset+sizeInBytes, field->name->str());
        size32_t numRows = 0;
        Owned<IEngineRowAllocator> childAllocator = builder.queryAllocator()->createChildRowAllocator(child);
        byte **childRows = NULL;
        RtlFieldStrInfo dummyField("<nested row>", NULL, child);
        while (source.processNextRow(field))
        {
            RtlDynamicRowBuilder childBuilder(childAllocator);
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
        builder.ensureCapacity(offset+sizeInBytes, field->name->str());
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
        size32_t thisCount = rtlReadUInt4(self);
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
        unsigned max = offset + rtlReadUInt4(self);
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
        size32_t thisCount = rtlReadUInt4(self);
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
        unsigned max = offset + rtlReadUInt4(self);
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

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlDictionaryTypeInfo::size(const byte * self, const byte * selfrow) const
{
    if (isLinkCounted())
        return sizeof(size32_t) + sizeof(void * *);
    return sizeof(size32_t) + rtlReadUInt4(self);
}

size32_t RtlDictionaryTypeInfo::build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const
{
    source.processBeginDataset(field);
    if (isLinkCounted())
    {
        // a 32-bit record count, and a pointer to an hash table with record pointers
        size32_t sizeInBytes = sizeof(size32_t) + sizeof(void *);
        builder.ensureCapacity(offset+sizeInBytes, field->name->str());
        Owned<IEngineRowAllocator> childAllocator = builder.queryAllocator()->createChildRowAllocator(child);
        RtlLinkedDictionaryBuilder dictBuilder(childAllocator, hashInfo);
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
        UNIMPLEMENTED;  // And may never be...
    source.processEndDataset(field);
    return offset;
}

size32_t RtlDictionaryTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    if (isLinkCounted())
    {
        size32_t thisCount = rtlReadUInt4(self);
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
        UNIMPLEMENTED;
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
        size32_t thisCount = rtlReadUInt4(self);
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
        UNIMPLEMENTED;
    }

    target.outputEndArray(innerPath);
    if (outerTag)
        target.outputEndNested(outerTag);

    return thisSize;
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlIfBlockTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    if (getCondition(selfrow))
        return sizeFields(fields, self, selfrow);
    return 0;
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

//-------------------------------------------------------------------------------------------------------------------

__int64 RtlBitfieldTypeInfo::signedValue(const byte * self) const
{
    __int64 value = rtlReadInt(self, getBitfieldIntSize());
    unsigned shift = getBitfieldShift();
    unsigned numBits = getBitfieldNumBits();
    value <<= (sizeof(value) - shift - numBits);
    return value >> numBits;

}


unsigned __int64 RtlBitfieldTypeInfo::unsignedValue(const byte * self) const
{
    unsigned __int64 value = rtlReadInt(self, getBitfieldIntSize());
    unsigned shift = getBitfieldShift();
    unsigned numBits = getBitfieldNumBits();
    value <<= (sizeof(value) - shift - numBits);
    return value >> numBits;

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
    return size(self, selfrow);
}

size32_t RtlBitfieldTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    if (isUnsigned())
        target.outputUInt(unsignedValue(self), queryXPath(field));
    else
        target.outputInt(signedValue(self), queryXPath(field));
    return size(self, selfrow);
}

//-------------------------------------------------------------------------------------------------------------------

size32_t RtlUnimplementedTypeInfo::size(const byte * self, const byte * selfrow) const 
{
    rtlFailUnexpected();
    return 0;
}

size32_t RtlUnimplementedTypeInfo::process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const
{
    rtlFailUnexpected();
    return 0;
}

size32_t RtlUnimplementedTypeInfo::toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const
{
    rtlFailUnexpected();
    return 0;
}

//-------------------------------------------------------------------------------------------------------------------

RtlFieldStrInfo::RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type) 
: RtlFieldInfo(rtlCreateFieldNameAtom(_name), _xpath, _type) 
{
}




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

