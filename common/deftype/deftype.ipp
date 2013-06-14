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

#ifndef _DEFTYPEI_INCL
#define _DEFTYPEI_INCL


#include "deftype.hpp"
#include "javahash.hpp"
#include "javahash.tpp"

class CTypeInfo;
class CIntTypeInfo;
class CStringTypeInfo;
class CBoolTypeInfo;
class CVoidTypeInfo;

class CTypeInfo : public CInterfaceOf<ITypeInfo>
{
protected:
    size32_t length;
public:
    CTypeInfo(size32_t _length) : length(_length) {};
    ~CTypeInfo();

    virtual size32_t getAlignment()               { return getSize(); };
    virtual size32_t getSize()                    { return length; };
    virtual const char *queryTypeName()         { assert(false); return "???"; }
    virtual unsigned getBitSize()               { return length*8; };
    virtual unsigned getDigits()                { return getSize(); };
    virtual unsigned getStringLen()             { return getSize(); };
    virtual unsigned getPrecision()             { return 0; }
    virtual unsigned getCardinality();
    virtual bool assignableFrom(ITypeInfo *t2)  { return getTypeCode()==t2->getTypeCode(); };
    virtual StringBuffer &getDescriptiveType(StringBuffer & out) { return getECLType(out); }

    virtual const char *getProxyName()          { return "CTypeInfo"; }

    virtual IValue * castFrom(double value);
    virtual IValue * castFrom(size32_t len, const UChar * text);
    virtual bool isReference()                  { return false; }
    virtual bool isSigned()                     { return false; }
    virtual bool isSwappedEndian()              { return false; }
    virtual ITypeInfo * queryChildType()        { return NULL; }
    virtual IInterface * queryDistributeInfo()  { return NULL; }
    virtual IInterface * queryLocalUngroupedSortInfo()   { return NULL; }
    virtual IInterface * queryGlobalSortInfo()  { return NULL; }
    virtual IInterface * queryGroupInfo()       { return NULL; }
    virtual IInterface * queryGroupSortInfo()   { return NULL; }
    virtual ITypeInfo * queryPromotedType()     { return this; }    // very common implementation
    virtual ICharsetInfo * queryCharset()       { return NULL; }
    virtual ICollationInfo * queryCollation()   { return NULL; }
    virtual IAtom * queryLocale()                 { return NULL; }
    virtual ITypeInfo * queryTypeBase()         { return this; }
    virtual unsigned getCrc();
    virtual typemod_t queryModifier()           { return typemod_none; }
    virtual IInterface * queryModifierExtra()   { return NULL; }

    virtual void serialize(MemoryBuffer &tgt)   { tgt.append((unsigned char) getTypeCode()); }
    virtual void deserialize(MemoryBuffer &tgt) { UNIMPLEMENTED; }

    virtual type_t getHashKind() const          { return getTypeCode(); };
    virtual unsigned getHash() const;
    virtual bool equals(const CTypeInfo & other) const;
};

class CHashedTypeInfo : public CTypeInfo
{
public:
    CHashedTypeInfo(size32_t _length) : CTypeInfo(_length) { observed = false; }

    void addObserver(IObserver & observer);
    void removeObserver(IObserver & observer);

    virtual void beforeDispose();

private:
    bool observed;
};

class CStringTypeInfo : public CTypeInfo
{
public:
    CStringTypeInfo(unsigned _length, ICharsetInfo * _charset, ICollationInfo * _collation);

    virtual type_t getTypeCode() const          { return type_string; };

    virtual unsigned getCardinality();
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(double value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual size32_t getAlignment()               { return 1; };
    virtual bool assignableFrom(ITypeInfo *t2);

    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual StringBuffer &getDescriptiveType(StringBuffer & out);
    virtual ICharsetInfo * queryCharset()       { return charset; }
    virtual ICollationInfo * queryCollation()   { return collation; }
    virtual const char *queryTypeName()         { return "char"; }
    virtual bool isInteger()                    { return false; };
    virtual bool isScalar()                     { return true; }
    virtual unsigned getCrc();

    virtual void serialize(MemoryBuffer &tgt)   
    { 
        CTypeInfo::serialize(tgt); 
        tgt.append(length);
        if (collation)
        {
            tgt.append(true);
            collation->serialize(tgt);
        }
        else
            tgt.append(false);
        if (charset)
        {
            tgt.append(true);
            charset->serialize(tgt);
        }
        else
            tgt.append(false);
    }

protected:
    Owned<ICollationInfo> collation;
    Owned<ICharsetInfo> charset;
};

class CUnicodeTypeInfo : public CTypeInfo
{
public:
    CUnicodeTypeInfo(unsigned _length, IAtom * _locale) : CTypeInfo(_length), locale(_locale) {}

    virtual type_t getTypeCode() const { return type_unicode; };
    virtual unsigned getDigits() { return getStringLen(); }
    //N.B. getSize() in bytes, i.e. UChars*2; getStringLen in UChars, i.e. getSize()/2 (where length known)
    virtual unsigned getStringLen() { if(getSize()==UNKNOWN_LENGTH) return UNKNOWN_LENGTH; else return getSize()/2; }
    virtual unsigned getCardinality();
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(double value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual IValue * castFrom(size32_t len, const UChar * text);

    virtual size32_t getAlignment() { return 2; };
    virtual bool assignableFrom(ITypeInfo *t2);

    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual IAtom * queryLocale()         { return locale; }
    virtual const char *queryTypeName()         { return "UChar"; }
    virtual bool isInteger()                    { return false; };
    virtual bool isScalar()                     { return true; }

    virtual void serialize(MemoryBuffer &tgt)   { CTypeInfo::serialize(tgt); tgt.append(getStringLen()).append(locale->str()); }
protected:
    IAtom * locale;
};

class CVarUnicodeTypeInfo : public CUnicodeTypeInfo
{
public:
    CVarUnicodeTypeInfo(unsigned len, IAtom * _locale);
    virtual type_t getTypeCode() const                { return type_varunicode; };
    virtual unsigned getStringLen()             { return length != UNKNOWN_LENGTH ? length/2-1 : UNKNOWN_LENGTH; };

    virtual IValue * castFrom(size32_t len, const char * text);
    virtual IValue * castFrom(size32_t len, const UChar * text);
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char *queryTypeName()         { return "var UChar"; } // ???
};

class CUtf8TypeInfo : public CUnicodeTypeInfo
{
public:
    CUtf8TypeInfo(unsigned len, IAtom * _locale) : CUnicodeTypeInfo(len, _locale) {}

    virtual type_t getTypeCode() const          { return type_utf8; };
    virtual unsigned getSize()                  { return UNKNOWN_LENGTH; };
    virtual unsigned getStringLen()             { return length != UNKNOWN_LENGTH ? length/4 : UNKNOWN_LENGTH; };

    virtual IValue * castFrom(size32_t len, const UChar * text);
    virtual size32_t getAlignment() { return 1; };
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char *queryTypeName()         { return "utf8"; } // ???
};

class CDataTypeInfo : public CStringTypeInfo
{
public:
    CDataTypeInfo(int _length);
    virtual type_t getTypeCode() const                { return type_data; };
    virtual bool assignableFrom(ITypeInfo *t2);
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char *queryTypeName()         { return "data"; } // ???

    virtual void serialize(MemoryBuffer &tgt)   
    { 
        CTypeInfo::serialize(tgt); 
        tgt.append(length);
    }
};

class CVarStringTypeInfo : public CStringTypeInfo
{
public:
    CVarStringTypeInfo(unsigned len, ICharsetInfo * _charset, ICollationInfo * _collation);
    virtual type_t getTypeCode() const                { return type_varstring; };
    virtual unsigned getStringLen()             { return length != UNKNOWN_LENGTH ? length-1 : UNKNOWN_LENGTH; };
    virtual unsigned getDigits()                { return length != UNKNOWN_LENGTH ? length-1 : UNKNOWN_LENGTH; };

    virtual IValue * castFrom(size32_t len, const char * text);
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual StringBuffer &getDescriptiveType(StringBuffer & out);
    virtual const char *queryTypeName()         { return "var char"; } // ???
};

class CQStringTypeInfo : public CStringTypeInfo
{
public:
    CQStringTypeInfo(unsigned _strLength);
    virtual type_t getTypeCode() const                { return type_qstring; };
    virtual unsigned getStringLen()             { return strLength; }
    virtual unsigned getDigits()                { return strLength; }

//  virtual bool assignableFrom(ITypeInfo *t2);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char *queryTypeName()         { return "var char"; } // ???
    virtual void serialize(MemoryBuffer &tgt)   
    { 
        CTypeInfo::serialize(tgt); 
        tgt.append(getStringLen());
    }
protected:
    unsigned strLength;
};

class CCharTypeInfo : public CTypeInfo
{
protected:
    bool caseSensitive;
public:
    CCharTypeInfo(bool _case) : CTypeInfo(1), caseSensitive(_case) {};
    virtual type_t getTypeCode() const             { return type_char; };

    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char *queryTypeName()         { return "char"; } 
    virtual bool isInteger()                 { return false; };
    virtual bool isScalar()                     { return true; }

    virtual void serialize(MemoryBuffer &tgt) { CTypeInfo::serialize(tgt); tgt.append(caseSensitive); }
};

class CIntTypeInfo : public CTypeInfo
{
public:
    CIntTypeInfo(int _length, bool _isSigned) : CTypeInfo(_length) { typeIsSigned = _isSigned; };
    virtual type_t getTypeCode() const { return type_int; };
    virtual bool isInteger() { return true; };

    virtual bool assignableFrom(ITypeInfo *t2);
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual bool isScalar()                     { return true; }
    virtual bool isSigned()                     { return typeIsSigned; }
    virtual unsigned getStringLen();
    virtual unsigned getDigits();
    virtual const char *queryTypeName() { return "byte"; }
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual unsigned getCrc();

    virtual void serialize(MemoryBuffer &tgt) { CTypeInfo::serialize(tgt); tgt.append((unsigned char) length).append(typeIsSigned); }
protected:
    bool  typeIsSigned;
};

//Store internally in correct order, but swap when writing to memory
class CSwapIntTypeInfo : public CIntTypeInfo
{
public:
    CSwapIntTypeInfo(int _length, bool _isSigned) : CIntTypeInfo(_length, _isSigned) { };
    virtual type_t getTypeCode() const { return type_swapint; };

    virtual bool isSwappedEndian()                     { return true; }
    virtual StringBuffer &getECLType(StringBuffer & out);
};

class CRealTypeInfo : public CTypeInfo
{
public:
    CRealTypeInfo(int _length) : CTypeInfo(_length) {};
    virtual type_t getTypeCode() const              { return type_real; };
    virtual bool isInteger()                    { return false; };
    virtual bool isSigned()                     { return true; }

    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(double value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual unsigned getStringLen();
    virtual unsigned getDigits();
    virtual bool isScalar()                     { return true; }
    virtual const char *queryTypeName() { return "real"; }
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual bool assignableFrom(ITypeInfo *t2);

    virtual void serialize(MemoryBuffer &tgt) { CTypeInfo::serialize(tgt); tgt.append((unsigned char) length); }
};

class CDecimalTypeInfo : public CHashedTypeInfo
{
protected:
    unsigned char prec;
    unsigned char digits;
    bool  typeIsSigned;

public:
    enum { UNKNOWN_DIGITS = 0xff };
    IValue * createValueFromStack(void);

    CDecimalTypeInfo(unsigned _digits, unsigned _prec, bool _isSigned);
    virtual type_t getTypeCode() const { return type_decimal; };
    virtual bool isInteger() { return false; };

    virtual bool assignableFrom(ITypeInfo *t2);
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(double value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual size32_t getAlignment()               { return 1; };
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual unsigned getPrecision() { return (prec == UNKNOWN_DIGITS) ? UNKNOWN_LENGTH : prec; };
    virtual unsigned getDigits()    { return (digits == UNKNOWN_DIGITS) ? UNKNOWN_LENGTH : digits; };
    virtual unsigned getStringLen(); 
    virtual unsigned getBitSize();
    virtual bool isSigned()         { return typeIsSigned; }
    virtual bool isScalar()                     { return true; }
    virtual const char *queryTypeName() { return "decimal"; }
    virtual unsigned getCrc();
    virtual unsigned getHash() const;
    virtual bool equals(const CTypeInfo & other) const;

    virtual void serialize(MemoryBuffer &tgt);
};

class CBitfieldTypeInfo : public CTypeInfo
{
private:
    int bitLength;
    ITypeInfo * promoted;
    ITypeInfo * storeType;
public:
    CBitfieldTypeInfo(int _length, ITypeInfo * _storeType);
    
    /* fix memory leak */
    ~CBitfieldTypeInfo() { ::Release(promoted); ::Release(storeType); }
    
    virtual bool assignableFrom(ITypeInfo *t2);
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual unsigned getBitSize()      { return bitLength; };
    virtual type_t getTypeCode() const  { return type_bitfield; };
    virtual bool isInteger()      { return true; };
    virtual bool isScalar()                     { return true; }

    virtual unsigned getCardinality()           { return (1 << bitLength); };
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual ITypeInfo * queryPromotedType()     { return promoted; }
    virtual const char *queryTypeName()         { return "bitfield"; }
    virtual ITypeInfo * queryChildType()        { return storeType; }
    virtual unsigned getCrc();

    virtual void serialize(MemoryBuffer &tgt)   { CTypeInfo::serialize(tgt); tgt.append(bitLength); promoted->serialize(tgt); }
};

class CBlobTypeInfo : public CIntTypeInfo
{
public:
    CBlobTypeInfo() : CIntTypeInfo(8, false) {}
    virtual type_t getTypeCode() const { return type_blob; };

    virtual void serialize(MemoryBuffer &tgt) { CTypeInfo::serialize(tgt); }
};

class CBoolTypeInfo : public CTypeInfo
{
public:
    CBoolTypeInfo() : CTypeInfo(1) {};
    virtual type_t getTypeCode() const { return type_boolean; };
    virtual bool isInteger() { return false; };
    virtual bool isScalar()                     { return true; }

    virtual bool assignableFrom(ITypeInfo *t2);
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual unsigned getCardinality() { return 2; };
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char *queryTypeName() { return "boolean"; }

};

class CVoidTypeInfo : public CTypeInfo
{
public:
    CVoidTypeInfo() : CTypeInfo(0) {};
    virtual type_t getTypeCode() const { return type_void; };
    virtual bool isInteger() { return false; };
    virtual bool isScalar()                     { return false; }

    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual unsigned getCardinality() { return 0; };
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char *queryTypeName()  { return "void"; }

};

class CNullTypeInfo : public CVoidTypeInfo
{
public:
    virtual type_t getTypeCode() const { return type_null; };
    virtual const char *queryTypeName()  { return "null"; }
    virtual unsigned getCrc();
};

class CRecordTypeInfo : public CVoidTypeInfo
{
public:
    virtual type_t getTypeCode() const { return type_record; };
    virtual const char *queryTypeName()  { return "record"; }
};

class CSimpleBaseTypeInfo : public CTypeInfo
{
public:
    CSimpleBaseTypeInfo() : CTypeInfo(0) {};
    virtual bool isInteger()                { return false; };
    virtual bool isScalar()                 { return false; }

    virtual IValue * castFrom(bool isSignedValue, __int64 value)                    { return NULL; }
    virtual IValue * castFrom(size32_t len, const char * text)  { return NULL; }
    virtual unsigned getCardinality()                           { return 0; };
    virtual StringBuffer &getECLType(StringBuffer & out)        { return out.append(queryTypeName()); }

};

class CPatternTypeInfo : public CSimpleBaseTypeInfo
{
public:
    virtual type_t getTypeCode() const          { return type_pattern; };
    virtual const char *queryTypeName()     { return "pattern"; }

};

class CTokenTypeInfo : public CSimpleBaseTypeInfo
{
public:
    virtual type_t getTypeCode() const          { return type_token; };
    virtual const char *queryTypeName()     { return "token"; }

};

class CFeatureTypeInfo : public CSimpleBaseTypeInfo
{
public:
    virtual type_t getTypeCode() const          { return type_feature; };
    virtual const char *queryTypeName()     { return "feature"; }

};

class CEventTypeInfo : public CSimpleBaseTypeInfo
{
public:
    virtual type_t getTypeCode() const          { return type_event; };
    virtual const char *queryTypeName()     { return "event"; }

};
class CAnyTypeInfo : public CTypeInfo
{
public:
    CAnyTypeInfo() : CTypeInfo(UNKNOWN_LENGTH) {};

    virtual type_t getTypeCode() const { return type_any; };
    virtual bool isInteger()                    { return true; };
    virtual bool isScalar()                     { return true; }

    virtual bool assignableFrom(ITypeInfo *t2)                      { return true; }
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(double value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual unsigned getCardinality() { return 0; };
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char *queryTypeName()  { return "void"; }

};

class CEnumeratedTypeInfo : public CTypeInfo, public IEnumeratedTypeBuilder
{
private:
    size32_t numValues;
    KeptHashTable valueMap;
    Array valueList;
    Owned<ITypeInfo> base;

public:
    IMPLEMENT_IINTERFACE_USING(CTypeInfo)
    CEnumeratedTypeInfo(ITypeInfo *base, size32_t numValues);

//interface ITypeInfo
    virtual type_t getTypeCode() const { return type_enumerated; };
    virtual bool isInteger() { return false; };
    virtual bool isScalar()                     { return true; }

    virtual unsigned getCardinality() { return numValues; };
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual const char* queryTypeName() { return "enumerated"; } // better: add base->queryTypeName().

//interface IEnumeratedTypeBuilder
    virtual ITypeInfo *getTypeInfo();
    virtual int addValue(IValue *val, size32_t frequency);

// Others
    IValue *queryValue(size32_t index);
    ITypeInfo *queryBase();

    virtual void serialize(MemoryBuffer &tgt) { UNIMPLEMENTED; }
};

class CBasedTypeInfo : public CHashedTypeInfo
{
public:
    CBasedTypeInfo(ITypeInfo *_basetype, int _size) : CHashedTypeInfo(_size), basetype(_basetype) {}

    virtual IValue * castFrom(bool isSignedValue, __int64 value)    { assertThrow(false); return NULL; } 
    virtual IValue * castFrom(size32_t len, const char * text) { assertThrow(false); return NULL; }
    virtual bool isInteger() { return false; }
    virtual ITypeInfo * queryChildType();
    virtual bool assignableFrom(ITypeInfo *t2);

    virtual void serialize(MemoryBuffer &tgt) { CTypeInfo::serialize(tgt); serializeType(tgt, basetype); }
            void serializeSkipChild(MemoryBuffer &tgt);

    virtual unsigned getHash() const;
    virtual bool equals(const CTypeInfo & other) const;

protected:
    Owned<ITypeInfo>    basetype;
};

class CPackedIntTypeInfo : public CBasedTypeInfo
{
public:
    CPackedIntTypeInfo(ITypeInfo * _basetype) : CBasedTypeInfo(_basetype, UNKNOWN_LENGTH) { }
    virtual type_t getTypeCode() const { return type_packedint; };

    virtual bool assignableFrom(ITypeInfo *t2);
    virtual IValue * castFrom(bool isSignedValue, __int64 value);
    virtual IValue * castFrom(size32_t len, const char * text);
    virtual bool isInteger() { return true; };
    virtual bool isScalar()                     { return true; }
    virtual bool isSigned()                     { return basetype->isSigned(); }
    virtual unsigned getStringLen()             { return basetype->getStringLen(); }
    virtual unsigned getDigits()                { return basetype->getDigits(); }
    virtual const char *queryTypeName()         { return "byte"; }
    virtual ITypeInfo * queryPromotedType()     { return basetype->queryPromotedType(); }
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual unsigned getCrc();
};

class CTransformTypeInfo : public CBasedTypeInfo
{
public:
    CTransformTypeInfo(ITypeInfo * _basetype) : CBasedTypeInfo(_basetype, sizeof(char *)) {}

    virtual type_t getTypeCode() const                      { return type_transform; }
    virtual bool isScalar()                                 { return false; }
    virtual const char *queryTypeName()                     { return "transform"; }
    virtual StringBuffer &getECLType(StringBuffer & out)    { out.append(queryTypeName()); 
                                                              if (basetype) 
                                                                  { out.append(" of "); queryChildType()->getECLType(out); } 
                                                              return out; 
                                                            }
    virtual bool assignableFrom(ITypeInfo *t2);
};

class CSortListTypeInfo : public CBasedTypeInfo
{
public:
    CSortListTypeInfo(ITypeInfo * _basetype) : CBasedTypeInfo(_basetype, sizeof(char *)) {}

    virtual type_t getTypeCode() const                      { return type_sortlist; }
    virtual bool isScalar()                                 { return false; }
    virtual const char *queryTypeName()                     { return "sortlist"; }
    virtual StringBuffer &getECLType(StringBuffer & out)    { out.append(queryTypeName()); 
                                                              if (basetype) 
                                                                  { out.append(" of "); queryChildType()->getECLType(out); } 
                                                              return out; 
                                                            }
    virtual bool assignableFrom(ITypeInfo *t2);
};

class CRowTypeInfo : public CBasedTypeInfo
{
public:
    CRowTypeInfo(ITypeInfo * _basetype) : CBasedTypeInfo(_basetype, sizeof(char *)) {}

    virtual type_t getTypeCode() const                          { return type_row; }
    virtual bool isScalar()                                 { return false; }
    virtual const char *queryTypeName()                     { return "row"; }
    virtual StringBuffer &getECLType(StringBuffer & out)    { out.append(queryTypeName()); 
                                                              if (basetype) 
                                                                  { out.append(" of "); queryChildType()->getECLType(out); } 
                                                              return out; 
                                                            }
    virtual bool assignableFrom(ITypeInfo *t2);
};

class CDictionaryTypeInfo : public CBasedTypeInfo
{
public:
    CDictionaryTypeInfo(ITypeInfo * _basetype)
        : CBasedTypeInfo(_basetype, UNKNOWN_LENGTH)
    {}

    virtual type_t getTypeCode() const                      { return type_dictionary; }
    virtual bool isScalar()                                 { return false; }
    virtual const char *queryTypeName()                     { return "dictionary"; }
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual bool assignableFrom(ITypeInfo *t2);

    virtual void serialize(MemoryBuffer &tgt);
};

class CTableTypeInfo : public CBasedTypeInfo
{
private:
    Owned<IInterface> distributeinfo;
    Owned<IInterface> globalsortinfo;
    Owned<IInterface> localsortinfo;
public:
    CTableTypeInfo(ITypeInfo * _basetype, IInterface * _distributeinfo, IInterface *_globalsortinfo, IInterface *_localsortinfo) 
        : CBasedTypeInfo(_basetype, UNKNOWN_LENGTH), distributeinfo(_distributeinfo), globalsortinfo(_globalsortinfo), localsortinfo(_localsortinfo) {}

    virtual type_t getTypeCode() const                          { return type_table; }
    virtual bool isScalar()                                 { return false; }
    virtual const char *queryTypeName()                     { return "table"; }
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual bool assignableFrom(ITypeInfo *t2);
    virtual IInterface * queryDistributeInfo();
    virtual IInterface * queryGlobalSortInfo();
    virtual IInterface * queryLocalUngroupedSortInfo();

    virtual void serialize(MemoryBuffer &tgt);
    //MORE: Delete this when persist attributes change again
    virtual unsigned getCrc()                               {
                                                                unsigned crc = getTypeCode();
                                                                size32_t xlength = 0;
                                                                crc = hashc((const byte *)&xlength, sizeof(xlength), crc);
                                                                return crc;
                                                            }

    virtual unsigned getHash() const;
    virtual bool equals(const CTypeInfo & other) const;
};

class CGroupedTableTypeInfo : public CBasedTypeInfo
{
private:
    Owned<IInterface> groupinfo;
    Owned<IInterface> groupsortinfo;
public:
    CGroupedTableTypeInfo(ITypeInfo * _basetype, IInterface *_groupinfo, IInterface *_groupsortinfo) 
    : CBasedTypeInfo(_basetype, UNKNOWN_LENGTH), groupinfo(_groupinfo), groupsortinfo(_groupsortinfo) {}

    virtual type_t getTypeCode() const                          { return type_groupedtable; }
    virtual StringBuffer &getECLType(StringBuffer & out)    { out.append(queryTypeName()).append(" of "); queryChildType()->getECLType(out); return out; };
    virtual const char *queryTypeName()                     { return "groupedtable"; }
    virtual bool isScalar()                                 { return false; }
    virtual bool assignableFrom(ITypeInfo *t2);
    virtual IInterface * queryDistributeInfo();
    virtual IInterface * queryGroupInfo();
    virtual IInterface * queryGlobalSortInfo();
    virtual IInterface * queryLocalUngroupedSortInfo();
    virtual IInterface * queryGroupSortInfo();

    virtual void serialize(MemoryBuffer &tgt);
    //MORE: Delete this when persist attributes change again
    virtual unsigned getCrc()                               {
                                                                unsigned crc = getTypeCode();
                                                                size32_t xlength = 0;
                                                                crc = hashc((const byte *)&xlength, sizeof(xlength), crc);
                                                                return crc;
                                                            }

    virtual unsigned getHash() const;
    virtual bool equals(const CTypeInfo & other) const;
};

class CSetTypeInfo : public CBasedTypeInfo
{
public:
    CSetTypeInfo(ITypeInfo * _basetype) : CBasedTypeInfo(_basetype, UNKNOWN_LENGTH) {}
    virtual type_t getTypeCode() const { return type_set; }
    virtual StringBuffer &getECLType(StringBuffer & out);
    virtual bool isScalar()                     { return false; }
    virtual const char *queryTypeName() { return "set"; }
    virtual bool assignableFrom(ITypeInfo *t2);

};

class CRuleTypeInfo : public CBasedTypeInfo
{
public:
    CRuleTypeInfo(ITypeInfo * _basetype) : CBasedTypeInfo(_basetype, 0) {}
    virtual type_t getTypeCode() const                                  { return type_rule; }
    virtual StringBuffer &getECLType(StringBuffer & out)            { return out.append("rule"); };
    virtual bool isScalar()                                         { return false; }
    virtual const char *queryTypeName()                             { return "rule"; }
    virtual bool assignableFrom(ITypeInfo *t2)                      { return getTypeCode()==t2->getTypeCode(); };
};


class CPointerTypeInfo : public CBasedTypeInfo
{
public:
    CPointerTypeInfo(ITypeInfo * _basetype) : CBasedTypeInfo(_basetype, sizeof(void *)) 
    { // loose format for debug
    }

    virtual type_t getTypeCode() const              { return type_pointer; }
    virtual StringBuffer &getECLType(StringBuffer & out) { return out.append("<pointer>"); }
    virtual bool isScalar()                     { return true; }
    virtual const char *queryTypeName()         { return "pointer"; }
};


class CArrayTypeInfo : public CBasedTypeInfo
{
public:
    CArrayTypeInfo(ITypeInfo * _basetype, unsigned _size) : CBasedTypeInfo(_basetype, _size) { }
    virtual size32_t getAlignment()               { return basetype->getAlignment(); };
    virtual type_t getTypeCode() const              { return type_array; }
    virtual size32_t getSize();
    virtual unsigned getCardinality() { return length; }
    virtual StringBuffer &getECLType(StringBuffer & out) { return out.append("<array>"); }
    virtual bool isScalar()                     { return false; }
    virtual const char *queryTypeName()         { return "array"; }

    virtual void serialize(MemoryBuffer &tgt)   
    { 
        CTypeInfo::serialize(tgt); 
        tgt.append(length);
        serializeType(tgt, basetype);
    }
};

class CClassTypeInfo : public CTypeInfo
{
public:
    CClassTypeInfo(const char * _name) : CTypeInfo(1) { name.set(_name); };
    virtual type_t getTypeCode() const { return type_class; };
    virtual bool isInteger() { return false; };
    virtual bool isScalar()                     { return false; }

    virtual IValue * castFrom(bool isSignedValue, __int64 value)    { assertex(false); return NULL; }
    virtual IValue * castFrom(size32_t len, const char * text)       { assertex(false); return NULL; }
    virtual unsigned getCardinality() { return 0; };
    virtual StringBuffer &getECLType(StringBuffer & out) { return out.append("<class ").append(name).append(">"); };
    virtual const char *queryTypeName()      { return name; }

    virtual void serialize(MemoryBuffer &tgt) { CTypeInfo::serialize(tgt); tgt.append(name); }
private:
    StringAttr name;
};


class CFunctionTypeInfo : public CBasedTypeInfo, implements IFunctionTypeExtra
{
private:
    Owned<IInterface> parameters;
    Owned<IInterface> defaults;
public:
    CFunctionTypeInfo(ITypeInfo * _basetype, IInterface * _parameters, IInterface * _defaults) 
        : CBasedTypeInfo(_basetype, UNKNOWN_LENGTH), parameters(_parameters), defaults(_defaults) 
    {}
    IMPLEMENT_IINTERFACE_USING(CBasedTypeInfo)

    virtual type_t getTypeCode() const                      { return type_function; }
    virtual bool isScalar()                                 { return false; }
    virtual const char *queryTypeName()                     { return "function"; }
    virtual StringBuffer &getECLType(StringBuffer & out)    { out.append(queryTypeName()).append(" returning "); queryChildType()->getECLType(out); return out; }
    virtual bool assignableFrom(ITypeInfo *t2);
    virtual IInterface * queryModifierExtra();

    virtual void serialize(MemoryBuffer &tgt);
    virtual unsigned getHash() const;
    virtual bool equals(const CTypeInfo & other) const;

//IFunctionTypeExtra
    virtual IInterface * queryParameters() { return parameters; }
    virtual IInterface * queryDefaults() { return defaults; }
};



class CIndirectTypeInfo : public CHashedTypeInfo
{
public:
    CIndirectTypeInfo(ITypeInfo * _baseType) : CHashedTypeInfo(0), baseType(_baseType)    
    { // loose format for debug: so that we can set conditional breakpoint
    }
    
    ~CIndirectTypeInfo()         { baseType->Release(); }

    virtual type_t getTypeCode() const                                  { return baseType->getTypeCode(); }
    virtual size32_t getSize()                                      { return baseType->getSize(); }
    virtual size32_t getAlignment()                                 { return baseType->getAlignment(); };
    virtual unsigned getStringLen()                                 { return baseType->getStringLen(); }
    virtual unsigned getDigits()                                    { return baseType->getDigits(); }
    virtual unsigned getBitSize()                                   { return baseType->getBitSize(); }
    virtual unsigned getPrecision()                                 { return baseType->getPrecision(); }
    virtual bool assignableFrom(ITypeInfo * source)                 { return baseType->assignableFrom(source); }
    virtual IValue * castFrom(bool isSignedValue, __int64 value)    { return baseType->castFrom(isSignedValue, value); }
    virtual IValue * castFrom(double value)                         { return baseType->castFrom(value); }
    virtual IValue * castFrom(size32_t len, const char * text)      { return baseType->castFrom(len, text); }
    virtual StringBuffer &getECLType(StringBuffer & out)            { return baseType->getECLType(out); };
    virtual const char *queryTypeName()                             { return baseType->queryTypeName(); }
    virtual unsigned getCardinality()                               { return baseType->getCardinality(); }
    virtual bool isInteger()                                        { return baseType->isInteger(); }
    virtual bool isReference()                                      { return baseType->isReference(); }
    virtual bool isScalar()                                         { return baseType->isScalar(); }
    virtual bool isSigned()                                         { return baseType->isSigned(); }
    virtual bool isSwappedEndian()                                  { return baseType->isSwappedEndian(); }
    virtual ITypeInfo * queryChildType()                            { return baseType->queryChildType(); }
    virtual ICharsetInfo * queryCharset()                           { return baseType->queryCharset(); }
    virtual ICollationInfo * queryCollation()                       { return baseType->queryCollation(); }
    virtual IAtom * queryLocale()                                     { return baseType->queryLocale(); }
    virtual IInterface * queryLocalUngroupedSortInfo()                      { return baseType->queryLocalUngroupedSortInfo(); }
    virtual IInterface * queryGlobalSortInfo()                      { return baseType->queryGlobalSortInfo(); }
    virtual IInterface * queryGroupInfo()                           { return baseType->queryGroupInfo(); }
    virtual IInterface * queryGroupSortInfo()                       { return baseType->queryGroupSortInfo(); }
    virtual IInterface * queryDistributeInfo()                      { return baseType->queryDistributeInfo(); }
    virtual ITypeInfo * queryPromotedType()                         { return baseType->queryPromotedType(); }
    virtual ITypeInfo * queryTypeBase()                             { return baseType; }
    virtual unsigned getCrc()                                       { return baseType->getCrc(); }

    virtual void serialize(MemoryBuffer &tgt) { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &src) { UNIMPLEMENTED; }

protected:
    ITypeInfo * baseType;
};


class CModifierTypeInfo : public CIndirectTypeInfo
{
public:
    CModifierTypeInfo(ITypeInfo * _baseType, typemod_t _kind, IInterface * _extra) : CIndirectTypeInfo(_baseType) { kind = _kind; extra.setown(_extra); }
    
    virtual bool isReference()                                      { if (kind == typemod_ref) return true; return baseType->isReference(); }
    virtual typemod_t queryModifier()                               { return kind; }
    virtual IInterface * queryModifierExtra()                       { return extra; }

    virtual void serialize(MemoryBuffer &tgt)                       { baseType->serialize(tgt); }       // modifiers are lost..


    virtual type_t getHashKind() const          { return type_modifier; };
    virtual unsigned getHash() const;
    virtual bool equals(const CTypeInfo & other) const;

protected:
    typemod_t kind;
    OwnedIInterface extra;
};


//---------------------------------------------------------------------------

class CCharsetInfo : public CInterface, implements ICharsetInfo
{
public:
    CCharsetInfo(IAtom * _name, unsigned char _fillChar, IAtom * _codepage)
    {
        name = _name;
        defaultCollation = NULL;
        fillChar = _fillChar;
        codepage = _codepage;
    }
    ~CCharsetInfo();
    IMPLEMENT_IINTERFACE

    virtual IAtom * queryName()                           { return name; }
    virtual ICollationInfo * queryDefaultCollation();
    virtual unsigned char queryFillChar()               { return fillChar; }
    virtual char const * queryCodepageName()            { return codepage->str(); }

    virtual void serialize(MemoryBuffer &tgt) 
    { 
        tgt.append(name->getAtomNamePtr());
    }
    virtual void deserialize(MemoryBuffer &) { UNIMPLEMENTED; }
protected:
    IAtom *   name;
    IAtom *   codepage;
    ICollationInfo * defaultCollation;
    unsigned char fillChar;
};

class CCollationInfo : public CInterface, implements ICollationInfo
{
public:
    CCollationInfo(IAtom * _name) { name = _name; }
    ~CCollationInfo();
    IMPLEMENT_IINTERFACE

    virtual IAtom * queryName()                           { return name; }
    virtual ICharsetInfo * getCharset();

    virtual void serialize(MemoryBuffer &tgt) 
    { 
        tgt.append(name->getAtomNamePtr());
    }
    virtual void deserialize(MemoryBuffer &) { UNIMPLEMENTED; }
protected:
    IAtom *   name;
};


class CSimpleCollationInfo : public CCollationInfo
{
public:
    CSimpleCollationInfo(IAtom * _name) : CCollationInfo(_name)
    { // loose format for debug
    }

    virtual int compare(const char * left, const char * right, unsigned len)        { return memcmp(left, right, len); }
    virtual const char * getCompareName(bool varLength)                             { return varLength ? "strcmp" : "memcmp"; }
};


class CNoCaseCollationInfo : public CCollationInfo
{
public:
    CNoCaseCollationInfo(IAtom * _name) : CCollationInfo(_name) {}

    virtual int compare(const char * left, const char * right, unsigned len)        { return memicmp(left, right, len); }
    virtual const char * getCompareName(bool varLength)                             { return varLength ? "stricmp" : "memicmp"; }
};

class CTranslationInfo : public CInterface, implements ITranslationInfo
{
public:
    CTranslationInfo(IAtom * _name, ICharsetInfo * src, ICharsetInfo * tgt);
    IMPLEMENT_IINTERFACE

    virtual IAtom * queryName();
    virtual ICharsetInfo * querySourceCharset();
    virtual ICharsetInfo * queryTargetCharset();

protected:
    IAtom * name;
    Owned<ICharsetInfo> src;
    Owned<ICharsetInfo> tgt;
};

class CAscii2EbcdicTranslationInfo : public CTranslationInfo
{
public:
    CAscii2EbcdicTranslationInfo();

    virtual const char * queryRtlFunction();
    virtual const char * queryVarRtlFunction();
    virtual StringBuffer & translate(StringBuffer & tgt, unsigned len, const char * src);
};

class CEbcdic2AsciiTranslationInfo : public CTranslationInfo
{
public:
    CEbcdic2AsciiTranslationInfo();

    virtual const char * queryRtlFunction();
    virtual const char * queryVarRtlFunction();
    virtual StringBuffer & translate(StringBuffer & tgt, unsigned len, const char * src);
};

//---------------------------------------------------------------------------

class TypeCache : public JavaHashTableOf<CHashedTypeInfo>
{
public:
    TypeCache() : JavaHashTableOf<CHashedTypeInfo>(false) {}

    //hash compare is much quicker, so did it first.
    virtual bool matchesFindParam(const void * et1, const void * et2, unsigned fphash) const
    { 
        const CHashedTypeInfo * element = static_cast<const CHashedTypeInfo *>(et1);
        const CHashedTypeInfo * key = static_cast<const CHashedTypeInfo *>(et2);
        return (element->getHash() == fphash) && element->equals(*key);
    }
};

typedef MapXToIInterface<TypeInfoAttr, ITypeInfo *> TypeToTypeMap;

typedef Linked<IInterface> ExtraInfoAttr;

struct CStringTypeKey
{
    ExtraInfoAttr charset;
    ExtraInfoAttr collation;
    memsize_t length;   // to make sure that there is no random packing in the structure
};
typedef MapXToIInterface<CStringTypeKey, CStringTypeKey &> CStringTypeToTypeMap;

struct CUnicodeTypeKey
{
    ExtraInfoAttr locale;
    memsize_t length;   // to make sure that there is no random packing in the structure
};
typedef MapXToIInterface<CUnicodeTypeKey, CUnicodeTypeKey &> CUnicodeTypeToTypeMap;

#endif
