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

    virtual const char * queryLocale() const override;
    virtual const RtlFieldInfo * const * queryFields() const override;
    virtual const RtlTypeInfo * queryChildType() const override;

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;

protected:
    size32_t buildUtf8ViaString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const;
    void getUtf8ViaString(size32_t & resultLen, char * & result, const void * ptr) const;
};

//-------------------------------------------------------------------------------------------------------------------

struct ECLRTL_API RtlBoolTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlBoolTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

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
protected:
    bool getBool(const void * ptr) const;
};

struct ECLRTL_API RtlRealTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlRealTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

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

private:
    inline double value(const void * self) const;
};

//MORE: Create specialist versions
struct ECLRTL_API RtlIntTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

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
};

struct ECLRTL_API RtlFileposTypeInfo : public RtlTypeInfoBase
{
    // Used for values stored in the fieldpos field of indexes
    constexpr inline RtlFileposTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo *_child, IThorIndexCallback *_callback)
    : RtlTypeInfoBase(_fieldType, _length), child(_child), callback(_callback)
    {}
    constexpr inline RtlFileposTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo *_child)
    : RtlTypeInfoBase(_fieldType, _length), child(_child), callback(nullptr)
    {}
    virtual void doDelete() const final override { delete this; }

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
    virtual bool isNumeric() const override { return true; }
    virtual int compare(const byte * left, const byte * right) const override;
    virtual const RtlTypeInfo * queryChildType() const override { return child; }

    void setCallback(IThorIndexCallback *_callback);
private:
    const RtlTypeInfo *child = nullptr;
    IThorIndexCallback *callback = nullptr;
};

struct ECLRTL_API RtlSwapIntTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlSwapIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

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
};

struct ECLRTL_API RtlKeyedIntTypeInfo final : public RtlTypeInfoBase
{
    constexpr inline RtlKeyedIntTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo *_child) : RtlTypeInfoBase(_fieldType, _length), child(_child) {}
    virtual void doDelete() const final override { delete this; }

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
private:
    inline __uint64 getUInt(const void * ptr) const { return (__uint64) getInt(ptr); }
    static unsigned __int64 addBias(__int64 value, unsigned length);
    static __int64 removeBias(unsigned __int64 value, unsigned length);
    const RtlTypeInfo *child = nullptr;
};

struct ECLRTL_API RtlPackedIntTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlPackedIntTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;

    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual double getReal(const void * ptr) const override;
    virtual bool isNumeric() const override { return true; }
    virtual int compare(const byte * left, const byte * right) const override;
};

struct ECLRTL_API RtlStringTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t codepoints, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool canTruncate() const override { return isFixedSize(); }
    virtual bool canExtend(char &fillChar) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};

struct ECLRTL_API RtlDataTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlDataTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool canTruncate() const override { return isFixedSize(); }
    virtual bool canExtend(char &fillChar) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};

struct ECLRTL_API RtlVarStringTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlVarStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool canExtend(char &fillChar) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};

struct ECLRTL_API RtlQStringTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlQStringTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool canExtend(char &fillChar) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};

struct ECLRTL_API RtlDecimalTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlDecimalTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

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

    size32_t calcSize() const;
};

struct ECLRTL_API RtlCharTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlCharTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};

struct ECLRTL_API RtlUnicodeTypeInfo : public RtlTypeInfoBase
{
public:
    constexpr inline RtlUnicodeTypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;

    virtual const char * queryLocale() const override { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlVarUnicodeTypeInfo : public RtlTypeInfoBase
{
public:
    constexpr inline RtlVarUnicodeTypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;

    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;

    virtual const char * queryLocale() const override { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlUtf8TypeInfo : public RtlTypeInfoBase
{
public:
    constexpr inline RtlUtf8TypeInfo(unsigned _fieldType, unsigned _length, const char * _locale) : RtlTypeInfoBase(_fieldType, _length), locale(_locale) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;

    virtual const char * queryLocale() const override { return locale; }

protected:
    const char * locale;
};

struct ECLRTL_API RtlRecordTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlRecordTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields) : RtlTypeInfoBase(_fieldType, _length), fields(_fields) {}
    virtual void doDelete() const final override { delete this; }
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
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual int compare(const byte * left, const byte * right) const override;
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


struct ECLRTL_API RtlSetTypeInfo : public RtlCompoundTypeInfo
{
    constexpr inline RtlSetTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};

struct ECLRTL_API RtlRowTypeInfo : public RtlCompoundTypeInfo
{
    constexpr inline RtlRowTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};


struct ECLRTL_API RtlDatasetTypeInfo : public RtlCompoundTypeInfo
{
    constexpr inline RtlDatasetTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child) : RtlCompoundTypeInfo(_fieldType, _length, _child) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};


struct ECLRTL_API RtlDictionaryTypeInfo : public RtlCompoundTypeInfo
{
    constexpr inline RtlDictionaryTypeInfo(unsigned _fieldType, unsigned _length, const RtlTypeInfo * _child, IHThorHashLookupInfo *_hashInfo)
    : RtlCompoundTypeInfo(_fieldType|RFTMnoserialize, _length, _child), hashInfo(_hashInfo) {}
    virtual void doDelete() const final override { delete this; }

    IHThorHashLookupInfo * hashInfo;

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual int compare(const byte * left, const byte * right) const override;
};


struct ECLRTL_API RtlIfBlockTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlIfBlockTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields) : RtlTypeInfoBase(_fieldType, _length), fields(_fields) {}
    const RtlFieldInfo * const * fields;                // null terminated

    virtual bool getCondition(const byte * selfrow) const = 0;
    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool isScalar() const override { return false; }
    virtual int compare(const byte * left, const byte * right) const override;

    virtual const RtlFieldInfo * const * queryFields() const override { return fields; }
};

struct ECLRTL_API RtlDynamicIfBlockTypeInfo final : public RtlIfBlockTypeInfo
{
    constexpr inline RtlDynamicIfBlockTypeInfo(unsigned _fieldType, unsigned _length, const RtlFieldInfo * const * _fields) : RtlIfBlockTypeInfo(_fieldType, _length, _fields) {}
    virtual bool getCondition(const byte * selfrow) const override;
    virtual void doDelete() const final override { delete this; }
};

struct ECLRTL_API RtlBitfieldTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlBitfieldTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

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

protected:
    size32_t getSize() const;
    __int64 signedValue(const void * self) const;
    unsigned __int64 unsignedValue(const void * self) const;
};

struct ECLRTL_API RtlUnimplementedTypeInfo : public RtlTypeInfoBase
{
    constexpr inline RtlUnimplementedTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t getMinSize() const override;
    virtual size32_t size(const byte * self, const byte * selfrow) const override;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t size, const char *value) const override;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *value) const override;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const override;
    virtual __int64 getInt(const void * ptr) const override;
    virtual bool isScalar() const override { return false; }
    virtual int compare(const byte * left, const byte * right) const override;
};

/*

struct ECLRTL_API RtlAlienTypeInfo : public RtlTypeInfoBase
{
public:
    constexpr inline RtlAlienTypeInfo(unsigned _fieldType, unsigned _length) : RtlTypeInfoBase(_fieldType, _length) {}
    virtual void doDelete() const final override { delete this; }

    virtual size32_t size(const byte * self, const byte * selfrow) const override = 0;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const override;
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & target) const override;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const override;
    virtual void readAhead(IRowDeserializerSource & in) const override;
};
*/


//-------------------------------------------------------------------------------------------------------------------

struct ECLRTL_API RtlFieldStrInfo : public RtlFieldInfo
{
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type);
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, unsigned _flags);
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, unsigned _flags, const char * _initializer);
    RtlFieldStrInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, const char * _initializer);  // So old WU dlls can load (and then fail) rather than failing to load.
};

extern unsigned ECLRTL_API countFields(const RtlFieldInfo * const * fields);

#endif
