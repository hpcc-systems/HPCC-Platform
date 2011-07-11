/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _DEFVALUEI_INCL
#define _DEFVALUEI_INCL

#include "eclrtl.hpp"
#include "defvalue.hpp"

class DEFTYPE_API CValue : public CInterface, public IValue
{
protected:
    ITypeInfo *type;

public:
    IMPLEMENT_IINTERFACE;

    CValue(ITypeInfo *);
    ~CValue();

//interface  IValue (mostly implemented in derived classes)

    virtual const void *queryValue() const { UNIMPLEMENTED; }
    virtual int compare(const void * mem);
    virtual ITypeInfo *getType();
    virtual ITypeInfo *queryType() const;
    virtual double getRealValue();  
    virtual type_t getTypeCode();
    virtual size32_t getSize();
    virtual void * getUCharStringValue(unsigned len, void * out) { UNIMPLEMENTED; return out; }
    virtual const char *getUTF8Value(StringBuffer & out) { return getStringValue(out); }
    virtual const char *getCodepageValue(StringBuffer & out, char const * codepage) { UNIMPLEMENTED; }

    // serializable
    virtual void serialize(MemoryBuffer &tgt) { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &src) { UNIMPLEMENTED; }

protected:
    IValue * doCastTo(unsigned osize, const char * text, ITypeInfo *t);     // common code for string casting
};

class VarStringValue : public CValue
{
protected:
    StringAttr val;

public:
    VarStringValue(unsigned len, const char *v, ITypeInfo *t);

//interface  IValue
    virtual const void *queryValue() const;
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();
    virtual const char *getStringValue(StringBuffer &); 
    virtual void pushDecimalValue();
    virtual void toMem(void * ptr);
    virtual unsigned getHash(unsigned initval);
    virtual int rangeCompare(ITypeInfo * targetType);
// serializable
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);
}; 

class MemoryValue : public CValue
{
public:
//interface  IValue
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual int compare(IValue * other);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue() { return 0; };
    virtual void pushDecimalValue();    
    virtual void toMem(void * ptr);
    virtual unsigned getHash(unsigned initval);
    virtual int rangeCompare(ITypeInfo * targetType);
// serializable
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);

protected:
    MemoryValue(ITypeInfo *t);
    MemoryValue(const void *v, ITypeInfo *t);

protected:
    MemoryAttr val;
}; 

class StringValue : public MemoryValue
{
public:
    StringValue(const char *v, ITypeInfo *t);

//interface  IValue
    virtual const void *queryValue() const;
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();
    virtual void pushDecimalValue();
    virtual const char *getStringValue(StringBuffer &); 

}; 

class UnicodeValue : public MemoryValue
{
public:
    UnicodeValue(UChar const * _value, ITypeInfo * _type);

//interface  IValue
    virtual const void *queryValue() const;
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();
    virtual void pushDecimalValue();
    virtual const char *getStringValue(StringBuffer &); 
    virtual void * getUCharStringValue(unsigned len, void * out);
    virtual const char *getUTF8Value(StringBuffer & out);
    virtual const char *getCodepageValue(StringBuffer & out, char const * codepage);
}; 

IValue * createUnicodeValue(UChar const * text, size32_t len, ITypeInfo * t);

class UnicodeAttr
{
public:
    UnicodeAttr() : text(0) {}
    ~UnicodeAttr() { free(text); }
    
    operator UChar const * () const { return text; }
    UChar const * get(void) const { return text; }
    size32_t length() const { return text ? rtlUnicodeStrlen(text) : 0; }
    void set(UChar const * _text, unsigned _len);

private:
    UChar * text;

private:
    UnicodeAttr &operator = (const StringAttr & from);
};

class VarUnicodeValue : public CValue
{
protected:
    UnicodeAttr val;

public:
    VarUnicodeValue(unsigned len, const UChar * v, ITypeInfo * _type);

//interface  IValue
    virtual const void *queryValue() const;
    virtual const char *generateCPP(StringBuffer & out, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * to);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();
    virtual __int64 getIntValue();
    virtual const char *getStringValue(StringBuffer & out);
    virtual void * getUCharStringValue(unsigned len, void * out);
    virtual const char *getUTF8Value(StringBuffer & out);
    virtual const char *getCodepageValue(StringBuffer & out, char const * codepage);
    virtual void pushDecimalValue();
    virtual void toMem(void * target);
    virtual unsigned getHash(unsigned initval);
    virtual int rangeCompare(ITypeInfo * targetType);
// serializable
    virtual void serialize(MemoryBuffer & tgt);
    virtual void deserialize(MemoryBuffer & src);
}; 

IValue * createVarUnicodeValue(UChar const * text, size32_t len, ITypeInfo * t);

class Utf8Value : public MemoryValue
{
public:
    Utf8Value(char const * _value, ITypeInfo * _type);

//interface  IValue
    virtual size32_t getSize();
    virtual const void *queryValue() const;
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();
    virtual void pushDecimalValue();
    virtual const char *getStringValue(StringBuffer &); 
    virtual void * getUCharStringValue(unsigned len, void * out);
    virtual const char *getUTF8Value(StringBuffer & out);
    virtual const char *getCodepageValue(StringBuffer & out, char const * codepage);
}; 

class DataValue : public MemoryValue
{
public:
    DataValue(const void *v, ITypeInfo *t);

//interface  IValue
    virtual const void *queryValue() const;
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);

    virtual bool getBoolValue();    
    virtual const char *getStringValue(StringBuffer &); 
protected:
    void generateHex(StringBuffer &out);

}; 

class QStringValue : public MemoryValue
{
public:
    QStringValue(unsigned len, const void *v, ITypeInfo *t);
    QStringValue(const char *v, ITypeInfo *_type);

//interface  IValue
    virtual const void *queryValue() const;
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();
    virtual void pushDecimalValue();    
    virtual const char *getStringValue(StringBuffer &); 
protected:
    void generateHex(StringBuffer &out);

}; 

class CharValue : public CValue
{
private:
    char val;

public:
    CharValue(char v, ITypeInfo *t);

//interface  IValue
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue() { return 0; };    
    virtual const char *getStringValue(StringBuffer &); 
    virtual void pushDecimalValue();
    virtual void toMem(void * ptr);
    virtual unsigned getHash(unsigned initval);
    virtual int rangeCompare(ITypeInfo * targetType);
// serializable
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);

}; 

class IntValue : public CValue
{
protected:
    unsigned __int64 val;

public:
    IntValue(unsigned __int64 v, ITypeInfo *_type) : CValue(_type), val(v) {}

// implementing IValue
    virtual const void *queryValue() const;
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();  
    virtual const char *getStringValue(StringBuffer &); 
    virtual void pushDecimalValue();
    virtual void toMem(void * ptr);
    virtual unsigned getHash(unsigned initval);
    virtual int rangeCompare(ITypeInfo * targetType);
// serializable
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);

protected:
    IValue * castViaString(ITypeInfo * t);
    byte * getAddressValue();
}; 

class PackedIntValue : public CValue
{
protected:
    Owned<IValue> value;

public:
    PackedIntValue(IValue * v, ITypeInfo *_type) : CValue(_type), value(v) {}

// implementing IValue
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler)     { return value->generateCPP(s, compiler); }
    virtual const char *generateECL(StringBuffer &s)        { return value->generateECL(s); }
    virtual IValue * castTo(ITypeInfo * type)               { return value->castTo(type); }
    virtual int compare(IValue * other);

    virtual bool getBoolValue()                             { return value->getBoolValue(); }
    virtual __int64 getIntValue()                           { return value->getIntValue(); }
    virtual const char *getStringValue(StringBuffer & s)    { return value->getStringValue(s); }
    virtual void pushDecimalValue()                         { value->pushDecimalValue(); }
    virtual void toMem(void * ptr);
    virtual unsigned getHash(unsigned initval)              { return value->getHash(initval); }
    virtual int rangeCompare(ITypeInfo * targetType)        { return value->rangeCompare(targetType); }
// serializable
    virtual void serialize(MemoryBuffer &tgt)               { value->serialize(tgt); }
    virtual void deserialize(MemoryBuffer &src)             { value->deserialize(src); }
}; 

class BitfieldValue : public IntValue
{
public:
    BitfieldValue(unsigned __int64 v, ITypeInfo *_type) : IntValue(v, _type) {}

};


class RealValue : public CValue
{
protected:
    double val;

public:
    RealValue(double v, ITypeInfo *_type) : CValue(_type), val(v) {}
    RealValue(double v, int size = sizeof(double)) : CValue(makeRealType(size)), val(v) {}

// implementing IValue

    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();  
    virtual double getRealValue();  
    virtual const char *getStringValue(StringBuffer &); 
    virtual void pushDecimalValue();
    virtual void toMem(void * ptr);
    virtual unsigned getHash(unsigned initval);
    virtual int rangeCompare(ITypeInfo * targetType);
// serializable
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);

}; 

class DecimalValue : public CValue
{
protected:
    void * val;

public:
    DecimalValue(const void * v, ITypeInfo * _type);
    ~DecimalValue();

// implementing IValue

    virtual const void *queryValue() const;
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();  
    virtual double getRealValue();  
    virtual const char *getStringValue(StringBuffer &); 
    virtual void pushDecimalValue();
    virtual void toMem(void * ptr);
    virtual unsigned getHash(unsigned initval);
    virtual int rangeCompare(ITypeInfo * targetType);
// serializable
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);

};

class EnumValue : public IntValue
{
public:
    EnumValue(unsigned int v, ITypeInfo *_type) : IntValue(v, _type) {}

// implementing IValue
    virtual IValue * castTo(ITypeInfo * type);
}; 

class BoolValue : public CValue
{
private:
    bool val;

public:
    BoolValue(bool v) : CValue(makeBoolType()), val(v)
    {   
    }

    static BoolValue *trueconst;
    static BoolValue *falseconst;

//  implementing IValue

    virtual const void *queryValue() const { return &val; }
    virtual const char *generateCPP(StringBuffer &s, CompilerType compiler);
    virtual const char *generateECL(StringBuffer &s);
    virtual IValue * castTo(ITypeInfo * type);
    virtual int compare(IValue * other);
    virtual int compare(const void * mem);

    virtual bool getBoolValue();    
    virtual __int64 getIntValue();  
    virtual const char *getStringValue(StringBuffer &); 
    virtual void pushDecimalValue();
    virtual void toMem(void * ptr);
    virtual unsigned getHash(unsigned initval);
    virtual int rangeCompare(ITypeInfo * targetType);
// serializable
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);

//  others

    static BoolValue *getTrue();
    static BoolValue *getFalse();
}; 

#endif
