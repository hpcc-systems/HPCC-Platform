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

#ifndef rtlfield_imp_hpp
#define rtlfield_imp_hpp

#include "eclrtl.hpp"

// A base implementation of RtlTypeInfo
struct ECLRTL_API RtlTypeInfoBase : public RtlTypeInfo
{
    inline RtlTypeInfoBase(unsigned _fieldType, unsigned _length) : RtlTypeInfo(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
    // virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;


    virtual const char * queryLocale() const;
    virtual const RtlFieldInfo * const * queryFields() const;
    virtual const RtlTypeInfo * queryChildType() const;
};

//-------------------------------------------------------------------------------------------------------------------

struct ECLRTL_API RtlBoolTypeInfo : public RtlTypeInfoBase
{
    inline RtlBoolTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlRealTypeInfo : public RtlTypeInfoBase
{
    inline RtlRealTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
private:
    inline double value(const byte * self) const;
};

//MORE: Create specialist versions
struct ECLRTL_API RtlIntTypeInfo : public RtlTypeInfoBase
{
    inline RtlIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlSwapIntTypeInfo : public RtlTypeInfoBase
{
    inline RtlSwapIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlPackedIntTypeInfo : public RtlTypeInfoBase
{
    inline RtlPackedIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlStringTypeInfo : public RtlTypeInfoBase
{
    inline RtlStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlDataTypeInfo : public RtlTypeInfoBase
{
    inline RtlDataTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlVarStringTypeInfo : public RtlTypeInfoBase
{
    inline RtlVarStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlQStringTypeInfo : public RtlTypeInfoBase
{
    inline RtlQStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlDecimalTypeInfo : public RtlTypeInfoBase
{
    inline RtlDecimalTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;

    size32_t calcSize() const;
};

struct ECLRTL_API RtlCharTypeInfo : public RtlTypeInfoBase
{
    inline RtlCharTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlUnicodeTypeInfo : public RtlTypeInfoBase
{
public:
    inline RtlUnicodeTypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;

    virtual const char * queryLocale() const { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlVarUnicodeTypeInfo : public RtlTypeInfoBase
{
public:
    inline RtlVarUnicodeTypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;

    virtual const char * queryLocale() const { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlUtf8TypeInfo : public RtlTypeInfoBase
{
public:
    inline RtlUtf8TypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;

    virtual const char * queryLocale() const { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlRecordTypeInfo : public RtlTypeInfoBase
{
    inline RtlRecordTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields) : RtlTypeInfoBase(_fieldType, _length), fields(_fields) {}
    const RtlFieldInfo * const * fields;                // null terminated

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual const RtlFieldInfo * const * queryFields() const { return fields; }
};

struct ECLRTL_API RtlCompoundTypeInfo : public RtlTypeInfoBase
{
    inline RtlCompoundTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlTypeInfoBase(_fieldType, _length), child(_child) {}
    const RtlTypeInfo * child;

    virtual const RtlTypeInfo * queryChildType() const { return child; }
};


struct ECLRTL_API RtlSetTypeInfo : public RtlCompoundTypeInfo
{
    inline RtlSetTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

struct ECLRTL_API RtlRowTypeInfo : public RtlCompoundTypeInfo
{
    inline RtlRowTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};


struct ECLRTL_API RtlDatasetTypeInfo : public RtlCompoundTypeInfo
{
    inline RtlDatasetTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};


struct ECLRTL_API RtlDictionaryTypeInfo : public RtlCompoundTypeInfo
{
    inline RtlDictionaryTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};


struct ECLRTL_API RtlIfBlockTypeInfo : public RtlTypeInfoBase
{
    inline RtlIfBlockTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields) : RtlTypeInfoBase(_fieldType, _length), fields(_fields) {}
    const RtlFieldInfo * const * fields;                // null terminated

    virtual bool getCondition(const byte * selfrow) const = 0;
    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;

    virtual const RtlFieldInfo * const * queryFields() const { return fields; }
};


struct ECLRTL_API RtlBitfieldTypeInfo : public RtlTypeInfoBase
{
    inline RtlBitfieldTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;

protected:
    __int64 signedValue(const byte * self) const;
    unsigned __int64 unsignedValue(const byte * self) const;
};

struct ECLRTL_API RtlUnimplementedTypeInfo : public RtlTypeInfoBase
{
    inline RtlUnimplementedTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};

/*

struct ECLRTL_API RtlAlienTypeInfo : public RtlTypeInfoBase
{
public:
    inline RtlAlienTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}

    virtual size32_t size(const byte * self, const byte * selfrow) const = 0;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const;
};
*/


//-------------------------------------------------------------------------------------------------------------------

struct ECLRTL_API RtlFieldStrInfo : public RtlFieldInfo
{
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type);
};


#endif
