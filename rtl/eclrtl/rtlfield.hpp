/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®

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

#ifndef rtlfield_hpp
#define rtlfield_hpp

#include "eclhelper.hpp"
#include "eclrtl.hpp"

/*
The classes in this file are used to represent the type of various fields, and information about the fields.
They are primarily designed to be used in generated code, so should have as little overhead as possible when used
in that context.  For that reason the classes have no destructors.

The file rtldynfield contains classes which manage instances of these classes which are dynamically created.
*/

interface IFieldFilter;
class RtlRow;
size32_t ECLRTL_API getMinSize(const RtlFieldInfo * const * fields);

// A base implementation of RtlTypeInfo
// base classes should always implement buildString and buildUtf8 - default implementations of buildInt/buildReal
// are implemented in terms of them.
// The helper function buildUtf8ViaString can be used to provide a simple implementation of buildUtf8
struct ECLRTL_API RtlTypeInfoBase : public RtlTypeInfo
{
    constexpr inline RtlTypeInfoBase(unsigned _fieldType, unsigned _length) : RtlTypeInfo(_fieldType, _length) {}

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const override;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildReal(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, double val) const override;

    virtual double getReal(const void * ptr) const override;
    virtual bool isScalar() const override;
    virtual bool isNumeric() const override { return false; }
    virtual bool canTruncate() const override { return false; }
    virtual bool canExtend(char &) const override { return false; }
    virtual bool canMemCmp() const override { return false; }
    virtual bool equivalent(const RtlTypeInfo *) const override;

    virtual const char * queryLocale() const override;
    virtual const RtlFieldInfo * const * queryFields() const override;
    virtual const RtlTypeInfo * queryChildType() const override;
    virtual const IFieldFilter * queryFilter() const override;

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void doCleanup() const override {}
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const override;
    virtual void setLowBound(void * buffer, const byte * value, size32_t subLength, bool inclusive) const override;
    virtual void setHighBound(void * buffer, const byte * value, size32_t subLength, bool inclusive) const override;
protected:
    virtual void setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const;
    size32_t buildUtf8ViaString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const;
    void getUtf8ViaString(size32_t & resultLen, char * & result, const void * ptr) const;
};

//-------------------------------------------------------------------------------------------------------------------

struct ECLRTL_API RtlBoolTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlBoolTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual bool canMemCmp() const override { return true; }
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
protected:
    bool getBool(const void * ptr) const;
};

struct ECLRTL_API RtlRealTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlRealTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildReal(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, double val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual double getReal(const void * ptr) const override;
    virtual bool isNumeric() const override { return true; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
    virtual void setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const override;

private:
    inline double value(const void * self) const;
};

struct ECLRTL_API RtlIntTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;

    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual double getReal(const void * ptr) const override;
    virtual bool canTruncate() const override;
    virtual bool canExtend(char &fillChar) const override;
    virtual bool isNumeric() const override { return true; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual bool canMemCmp() const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};

struct ECLRTL_API RtlBlobTypeInfo final : public RtlTypeInfoBase
{
    // Used for values stored in blobs. The child type represents the original type
    constexpr inline RtlBlobTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo *_child)
    : RtlTypeInfoBase(_fieldType, _length), child(_child)
    {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;

    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual double getReal(const void * ptr) const override;
    virtual bool canTruncate() const override;
    virtual bool canExtend(char &fillChar) const override;
    virtual bool isNumeric() const override { return false; }
    virtual bool isScalar() const override { return false; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
    virtual const RtlTypeInfo * queryChildType() const override { return child; }
private:
    const RtlTypeInfo *child = nullptr;
};

struct ECLRTL_API RtlSwapIntTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlSwapIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;

    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual double getReal(const void * ptr) const override;
    virtual bool canTruncate() const override;
    virtual bool canExtend(char &fillChar) const override;
    virtual bool isNumeric() const override { return true; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual bool canMemCmp() const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};

struct ECLRTL_API RtlKeyedIntTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlKeyedIntTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo *_child) : RtlTypeInfoBase(_fieldType, _length), child(_child) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;

    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual double getReal(const void * ptr) const override;
    virtual bool isNumeric() const override { return true; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual bool canMemCmp() const override { return true; }
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
    virtual const RtlTypeInfo * queryChildType() const override;

private:
    inline __uint64 getUInt(const void * ptr) const { return (__uint64) getInt(ptr); }
    static unsigned __int64 addBias(__int64 value, unsigned length);
    static __int64 removeBias(unsigned __int64 value, unsigned length);
    const RtlTypeInfo *child = nullptr;
};

struct ECLRTL_API RtlPackedIntTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlPackedIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;

    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual double getReal(const void * ptr) const override;
    virtual bool isNumeric() const override { return true; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};

struct ECLRTL_API RtlStringTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t codepoints, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool canTruncate() const override { return isFixedSize(); }
    virtual bool canExtend(char &fillChar) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual bool canMemCmp() const override;
    virtual unsigned hash(const byte * self, unsigned inhash) const override;
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const override;
};

struct ECLRTL_API RtlDataTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlDataTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool canTruncate() const override { return isFixedSize(); }
    virtual bool canExtend(char &fillChar) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual bool canMemCmp() const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const override;
};

struct ECLRTL_API RtlVarStringTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlVarStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool canExtend(char &fillChar) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte * self, unsigned inhash) const override;
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const override;
    virtual void setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const override;
};

struct ECLRTL_API RtlQStringTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlQStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool canExtend(char &fillChar) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual bool canMemCmp() const override;
    virtual unsigned hash(const byte * self, unsigned inhash) const override;
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const override;
    virtual void setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const override;
};

struct ECLRTL_API RtlDecimalTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlDecimalTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual double getReal(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;

    size32_t calcSize() const;
};

struct ECLRTL_API RtlCharTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlCharTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual bool canMemCmp() const override { return true; }
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};

struct ECLRTL_API RtlUnicodeTypeInfo final : public RtlTypeInfoBase
{
public:
    constexpr inline RtlUnicodeTypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte * self, unsigned inhash) const override;
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const override;
    virtual void setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const override;

    virtual const char * queryLocale() const override { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlVarUnicodeTypeInfo final : public RtlTypeInfoBase
{
public:
    constexpr inline RtlVarUnicodeTypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;

    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte * _self, unsigned inhash) const override;
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const override;
    virtual void setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const override;

    virtual const char * queryLocale() const override { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlUtf8TypeInfo final : public RtlTypeInfoBase
{
public:
    constexpr inline RtlUtf8TypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte * self, unsigned inhash) const override;
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const override;
    virtual void setBound(void * buffer, const byte * value, size32_t subLength, byte fill, bool inclusive) const override;

    virtual const char * queryLocale() const override { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlRecordTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlRecordTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields) : RtlTypeInfoBase(_fieldType, _length), fields(_fields) {}
    virtual void doDelete() const override { delete this; }
    const RtlFieldInfo * const * fields;                // null terminated

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
    virtual const RtlFieldInfo * const * queryFields() const override { return fields; }
    virtual bool isScalar() const override { return false; }
};

struct ECLRTL_API RtlCompoundTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlCompoundTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlTypeInfoBase(_fieldType, _length), child(_child) {}

    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual const RtlTypeInfo * queryChildType() const override { return child; }
    virtual bool isScalar() const override { return false; }

    const RtlTypeInfo * child;
};


struct ECLRTL_API RtlSetTypeInfo final : public RtlCompoundTypeInfo
{
    constexpr inline RtlSetTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};

struct ECLRTL_API RtlRowTypeInfo final : public RtlCompoundTypeInfo
{
    constexpr inline RtlRowTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};


struct ECLRTL_API RtlDatasetTypeInfo final : public RtlCompoundTypeInfo
{
    constexpr inline RtlDatasetTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};


struct ECLRTL_API RtlDictionaryTypeInfo final : public RtlCompoundTypeInfo
{
    constexpr inline RtlDictionaryTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child)
    : RtlCompoundTypeInfo(_fieldType, _length, _child) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};


struct ECLRTL_API RtlIfBlockTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlIfBlockTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields, const RtlRecordTypeInfo * _rowType)
    : RtlTypeInfoBase(_fieldType, _length), fields(_fields), rowType(_rowType) {}

    const RtlFieldInfo * const * fields;                // null terminated
    const RtlRecordTypeInfo * rowType;                  // may be updated when parent deserialized

    virtual bool getCondition(const byte * selfrow) const = 0;
    virtual bool getCondition(const RtlRow & selfrow) const = 0;

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool isScalar() const override { return false; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;

    virtual const RtlFieldInfo * const * queryFields() const override { return fields; }
};

//Ifblock for complex conditions that cannot be serialized
struct ECLRTL_API RtlComplexIfBlockTypeInfo : public RtlIfBlockTypeInfo
{
    constexpr inline RtlComplexIfBlockTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields, const RtlRecordTypeInfo * _rowType)
    : RtlIfBlockTypeInfo(_fieldType, _length, _fields, _rowType) {}

    using RtlIfBlockTypeInfo::getCondition;
    virtual bool getCondition(const RtlRow & selfrow) const override;
};

struct ECLRTL_API RtlSerialIfBlockTypeInfo : public RtlIfBlockTypeInfo
{
    inline RtlSerialIfBlockTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields, const RtlRecordTypeInfo * _rowType)
    : RtlIfBlockTypeInfo(_fieldType, _length, _fields, _rowType) {}
    ~RtlSerialIfBlockTypeInfo();

    virtual bool getCondition(const byte * selfrow) const override;
    virtual bool getCondition(const RtlRow & selfrow) const override;
    virtual IFieldFilter * createCondition() const = 0;
    virtual const IFieldFilter * queryFilter() const override;

    virtual void doCleanup() const override;
protected:
    const IFieldFilter * resolveCondition() const;

    //The following are initialised on demand in resolveCondition()
    mutable const IFieldFilter * filter = nullptr;
    mutable RtlRecord * parentRecord = nullptr;
    mutable unsigned numPrevFields = 0;
};

struct ECLRTL_API RtlSimpleIfBlockTypeInfo : public RtlSerialIfBlockTypeInfo
{
    inline RtlSimpleIfBlockTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields, const RtlRecordTypeInfo * _rowType)
    : RtlSerialIfBlockTypeInfo(_fieldType, _length, _fields, _rowType) {}
};

struct ECLRTL_API RtlDynamicIfBlockTypeInfo final : public RtlSerialIfBlockTypeInfo
{
    inline RtlDynamicIfBlockTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields, const RtlRecordTypeInfo * _rowType, const IFieldFilter * ownedFilter)
    : RtlSerialIfBlockTypeInfo(_fieldType, _length, _fields, _rowType) { filter = ownedFilter; }
    virtual IFieldFilter * createCondition() const override;
    virtual void doDelete() const final override { delete this; }

    void setParent(const RtlRecordTypeInfo * _rowType) { rowType = _rowType; }
};

struct ECLRTL_API RtlBitfieldTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlBitfieldTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;

protected:
    size32_t getSize() const;
    __int64 signedValue(const void * self) const;
    unsigned __int64 unsignedValue(const void * self) const;
};

struct ECLRTL_API RtlUnimplementedTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlUnimplementedTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool isScalar() const override { return false; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
};

struct ECLRTL_API RtlAlienTypeInfo final : public RtlCompoundTypeInfo
{
public:
    constexpr inline RtlAlienTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}
    virtual void doDelete() const override { delete this; }

    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
    virtual unsigned hash(const byte *self, unsigned inhash) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowPrefetcherSource & in) const override;
};


//-------------------------------------------------------------------------------------------------------------------

struct ECLRTL_API RtlFieldStrInfo : public RtlFieldInfo
{
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type);
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, unsigned _flags);
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, unsigned _flags, const char * _initializer);
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, const char * _initializer);  // So old WU dlls can load (and then fail) rather than failing to load.
};

extern unsigned ECLRTL_API countFields(const RtlFieldInfo * const * fields);
extern int ECLRTL_API compareFields(const RtlFieldInfo * const * cur, const byte * left, const byte * right, bool excludePayload = false);
extern unsigned ECLRTL_API hashFields(const RtlFieldInfo * const * cur, const byte *self, unsigned inhash, bool excludePayload = false);
extern bool ECLRTL_API hasTrailingFileposition(const RtlFieldInfo * const * fields);
extern bool ECLRTL_API hasTrailingFileposition(const RtlTypeInfo * type);
extern size32_t translateScalar(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, const RtlTypeInfo &destType, const RtlTypeInfo &sourceType, const byte *source);

#endif
