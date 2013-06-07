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

#ifndef _DEFTYPE_INCL
#define _DEFTYPE_INCL

#include "jcomp.hpp"

#ifdef _WIN32
#ifdef DEFTYPE_EXPORTS
#define DEFTYPE_API __declspec(dllexport)
#else
#define DEFTYPE_API __declspec(dllimport)
#endif
#else
#define DEFTYPE_API
#endif

#define CHEAP_UCHAR_DEF
#ifdef _WIN32
typedef wchar_t UChar;
#else //__WIN32
typedef unsigned short UChar;
#endif //__WIN32

interface ITypeInfo;
interface IValue;

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define type_bigendianint       type_swapint
#define type_littleendianint    type_int
#else
#define type_bigendianint       type_int
#define type_littleendianint    type_swapint
#endif

// NOTE - do not change the values here - they are also used in Clarion and stored in a database!!
// Add new types to the end

enum type_vals 
{
    type_boolean        = 0, 
    type_int            = 1,    
    type_real           = 2, 
    type_decimal        = 3, 
    type_string         = 4, 
    type_alias          = 5, // This is only used when serializing expression graphs
    type_date           = 6, 
type_unused2            = 7, 
type_unused3            = 8, 
    type_bitfield       = 9, 
type_unused4            = 10, 
    type_char           = 11,
    type_enumerated     = 12, 
    type_record         = 13, 
    type_varstring      = 14,
    type_blob           = 15, 
    type_data           = 16, 
    type_pointer        = 17, 
    type_class          = 18, 
    type_array          = 19,
    type_table          = 20, 
    type_set            = 21, 
    type_row            = 22, 
    type_groupedtable   = 23,
    type_void           = 24,
    type_alien          = 25,
    type_swapint        = 26,
    type_none           = 27,
    type_packedint      = 28,
type_unused5            = 29,
    type_qstring        = 30,
    type_unicode        = 31,
    type_any            = 32,
    type_varunicode     = 33,
    type_pattern        = 34,
    type_rule           = 35,
    type_token          = 36,
    type_feature        = 37,
    type_event          = 38,
    type_null           = 39,       // not the same as type_void, which should be reserved for actions.
    type_scope          = 40,
    type_utf8           = 41,
    type_transform      = 42,
    type_ifblock        = 43,       // not a real type -but used for the rtlfield serialization
    type_function       = 44,
    type_sortlist       = 45,
    type_dictionary     = 46,

    type_max,

    type_modifier       = 0xff,     // used by getKind()
    type_unsigned       = 0x100,  // combined with some of the above, when returning summary type information. Not returned by getTypeCode()
    type_ebcdic         = 0x200,   // combined with some of the above, when returning summary type information. Not returned by getTypeCode()

//Some pseudo types - never actually created
    type_stringorunicode= 0xfc, // any string/unicode variant
    type_numeric        = 0xfd,
    type_scalar         = 0xfe,
};

enum typemod_t
{
    typemod_none        = 0,
    typemod_const       = 1,
    typemod_ref         = 2,
    typemod_wrapper     = 3,
    typemod_builder     = 4,
    typemod_original    = 5,
    typemod_member      = 6,
    typemod_serialized  = 7,
    typemod_outofline   = 8,
    typemod_attr        = 9,
    typemod_indirect    = 10,       // type definition needs to go via an ecl definition
    typemod_max
};

#define INFINITE_LENGTH         0xFFFFFFF0
#define UNKNOWN_LENGTH          0xFFFFFFF1

typedef enum type_vals type_t;

//MORE: Something like this should replace caseSensitive for strings...
interface ICollationInfo;
interface ICharsetInfo : public serializable
{
public:
    virtual IAtom * queryName() = 0;
    virtual ICollationInfo * queryDefaultCollation() = 0;
    virtual unsigned char queryFillChar() = 0;
    virtual char const * queryCodepageName() = 0;
};

interface ICollationInfo : public serializable
{
public:
    virtual IAtom * queryName() = 0;
    virtual ICharsetInfo * getCharset() = 0;
    virtual int compare(const char * left, const char * right, unsigned len) = 0;
    virtual const char * getCompareName(bool varLength) = 0;
};

interface ITranslationInfo : public IInterface
{
public:
    virtual IAtom * queryName() = 0;
    virtual const char * queryRtlFunction() = 0;
    virtual const char * queryVarRtlFunction() = 0;
    virtual ICharsetInfo * querySourceCharset() = 0;
    virtual ICharsetInfo * queryTargetCharset() = 0;
    virtual StringBuffer & translate(StringBuffer & tgt, unsigned len, const char * src) = 0;
};

interface IValue;
interface ITypeInfo : public serializable
{
public:
    virtual type_t getTypeCode() const = 0;
    virtual size32_t getSize() = 0;
    virtual unsigned getAlignment() = 0;
    virtual unsigned getBitSize() = 0;
    virtual unsigned getPrecision() = 0;
    virtual unsigned getStringLen() = 0;
    virtual unsigned getDigits() = 0;
    virtual bool assignableFrom(ITypeInfo * source) = 0;
    virtual IValue * castFrom(bool isSignedValue, __int64 value) = 0;
    virtual IValue * castFrom(double value) = 0;
    virtual IValue * castFrom(size32_t len, const char * text) = 0;
    virtual IValue * castFrom(size32_t len, const UChar * text) = 0;
    virtual StringBuffer &getECLType(StringBuffer & out) = 0;
    virtual StringBuffer &getDescriptiveType(StringBuffer & out) = 0;
    virtual const char *queryTypeName() = 0;
    virtual unsigned getCardinality() = 0;
    virtual bool isInteger() = 0;
    virtual bool isReference() = 0;
    virtual bool isScalar() = 0;
    virtual bool isSigned() = 0;
    virtual bool isSwappedEndian() = 0;
    virtual ITypeInfo * queryChildType() = 0;
    virtual ICharsetInfo * queryCharset() = 0;
    virtual ICollationInfo * queryCollation() = 0;
    virtual IAtom * queryLocale() = 0;
    virtual IInterface * queryDistributeInfo() = 0;
    virtual IInterface * queryGroupInfo() = 0;
    virtual IInterface * queryGlobalSortInfo() = 0;
    virtual IInterface * queryLocalUngroupedSortInfo() = 0;
    virtual IInterface * queryGroupSortInfo() = 0;
    virtual ITypeInfo * queryPromotedType() = 0;
    virtual ITypeInfo * queryTypeBase() = 0;
    virtual unsigned getCrc() = 0;      // must be run independant.
    virtual typemod_t queryModifier() = 0;
    virtual IInterface * queryModifierExtra() = 0;

    inline bool isBoolean() const { return getTypeCode() == type_boolean; }

private:
    inline IValue * castFrom(__int64 value) { return NULL; }
};

interface IFunctionTypeExtra : public IInterface
{
    virtual IInterface * queryParameters() = 0;
    virtual IInterface * queryDefaults() = 0;
};

interface IEnumeratedTypeBuilder : public IInterface
{
public:
    virtual ITypeInfo *getTypeInfo() = 0;
    virtual int addValue(IValue *val, size32_t frequency) = 0;
};

extern DEFTYPE_API ITypeInfo *makeStringType(unsigned size, ICharsetInfo * _charset = NULL, ICollationInfo * _collation = NULL);
extern DEFTYPE_API ITypeInfo *makeVarStringType(unsigned size, ICharsetInfo * _charset = NULL, ICollationInfo * _collation = NULL);    //NB: size is numchars+1
extern DEFTYPE_API ITypeInfo *makeQStringType(int len);
extern DEFTYPE_API ITypeInfo *makeUnicodeType(unsigned len, IAtom * locale); // takes length in UChars, i.e. bytes/2 if known; locale is like fr_BE_EURO, or 0 for default
extern DEFTYPE_API ITypeInfo *makeVarUnicodeType(unsigned len, IAtom * locale); // takes length in UChars + 1, i.e. bytes/2 + 1 if known; locale is like fr_BE_EURO, or 0 for default
extern DEFTYPE_API ITypeInfo *makeUtf8Type(unsigned len, IAtom * locale);       // takes length in UChars, i.e. bytes/4 if known; locale is like fr_BE_EURO, or 0 for default
extern DEFTYPE_API ITypeInfo *makeCharType(bool caseSensitive = false);
extern DEFTYPE_API ITypeInfo *makeIntType(int size, bool isSigned);
extern DEFTYPE_API ITypeInfo *makeSwapIntType(int size, bool isSigned);
extern DEFTYPE_API ITypeInfo *makePackedIntType(ITypeInfo * basetype);
extern DEFTYPE_API ITypeInfo *makePackedIntType(int size, bool isSigned);
extern DEFTYPE_API ITypeInfo *makeRealType(int size);
extern DEFTYPE_API ITypeInfo *makeDataType(int size);
extern DEFTYPE_API ITypeInfo *makeBitfieldType(int sizeInBits, ITypeInfo * basetype = NULL);
extern DEFTYPE_API ITypeInfo *makeBoolType();
extern DEFTYPE_API ITypeInfo *makeBlobType();
extern DEFTYPE_API ITypeInfo *makeRecordType();     // not used by any IHqlExpressions
extern DEFTYPE_API ITypeInfo *makeVoidType();
extern DEFTYPE_API ITypeInfo *makeNullType();
extern DEFTYPE_API ITypeInfo *makeEventType();
extern DEFTYPE_API ITypeInfo *makeAnyType();
extern DEFTYPE_API ITypeInfo *makeType(type_t type, int size);
extern DEFTYPE_API IEnumeratedTypeBuilder *makeEnumeratedTypeBuilder(ITypeInfo *base, aindex_t numvalues);
extern DEFTYPE_API ITypeInfo *makeDecimalType(unsigned digits, unsigned prec, bool isSigned);
extern DEFTYPE_API ITypeInfo *makeDictionaryType(ITypeInfo *basetype);
extern DEFTYPE_API ITypeInfo *makeTableType(ITypeInfo *basetype, IInterface * distributeinfo, IInterface *gloalSortinfo, IInterface * localSortInfo);
extern DEFTYPE_API ITypeInfo *makeGroupedTableType(ITypeInfo *basetype, IInterface *groupinfo, IInterface *sortinfo);
extern DEFTYPE_API ITypeInfo *makeRowType(ITypeInfo *basetype);
extern DEFTYPE_API ITypeInfo *makeSetType(ITypeInfo *basetype);
extern DEFTYPE_API ITypeInfo *makeTransformType(ITypeInfo *basetype);
extern DEFTYPE_API ITypeInfo *makeSortListType(ITypeInfo *basetype);
extern DEFTYPE_API ITypeInfo *makeFunctionType(ITypeInfo *basetype, IInterface * args, IInterface * defaults);

extern DEFTYPE_API ITypeInfo * makePointerType(ITypeInfo * basetype);
extern DEFTYPE_API ITypeInfo * makeArrayType(ITypeInfo * basetype, unsigned size=0);
extern DEFTYPE_API ITypeInfo * makeClassType(const char * className);
extern DEFTYPE_API ITypeInfo * makeConstantModifier(ITypeInfo * basetype);
extern DEFTYPE_API ITypeInfo * makeReferenceModifier(ITypeInfo * basetype);
extern DEFTYPE_API ITypeInfo * makeWrapperModifier(ITypeInfo * basetype);
extern DEFTYPE_API ITypeInfo * makeModifier(ITypeInfo * basetype, typemod_t modifier, IInterface * extra=NULL);
extern DEFTYPE_API ITypeInfo * makePatternType();
extern DEFTYPE_API ITypeInfo * makeRuleType(ITypeInfo * attrType);
extern DEFTYPE_API ITypeInfo * makeTokenType();
extern DEFTYPE_API ITypeInfo * makeFeatureType();

inline ITypeInfo * makeOutOfLineModifier(ITypeInfo * basetype) { return makeModifier(basetype, typemod_outofline); }
inline ITypeInfo * makeOriginalModifier(ITypeInfo * basetype, IInterface * extra) { return makeModifier(basetype, typemod_original, extra); }
inline ITypeInfo * makeAttributeModifier(ITypeInfo * basetype, IInterface * extra) { return makeModifier(basetype, typemod_attr, extra); }

extern DEFTYPE_API MemoryBuffer & appendBufferFromMem(MemoryBuffer & mem, ITypeInfo * type, const void * data);

extern DEFTYPE_API void ClearTypeCache();

extern DEFTYPE_API ICharsetInfo * getCharset(IAtom * charset);
extern DEFTYPE_API ICollationInfo * getCollation(IAtom * collation);
extern DEFTYPE_API ITranslationInfo * getDefaultTranslation(ICharsetInfo * tgt, ICharsetInfo * src);
extern DEFTYPE_API ITranslationInfo * queryDefaultTranslation(ICharsetInfo * tgt, ICharsetInfo * src);
extern DEFTYPE_API bool isAscii(ITypeInfo * type);

//---------------------------------------------------------------------------

extern DEFTYPE_API ITypeInfo * getStretchedType(unsigned newLen, ITypeInfo * type);
extern DEFTYPE_API ITypeInfo * getMaxLengthType(ITypeInfo * type);
extern DEFTYPE_API ITypeInfo * getNumericType(ITypeInfo * type);
extern DEFTYPE_API ITypeInfo * getStringType(ITypeInfo * type);
extern DEFTYPE_API ITypeInfo * getVarStringType(ITypeInfo * type);
extern DEFTYPE_API ITypeInfo * getPromotedType(ITypeInfo * l_type, ITypeInfo * r_type);
extern DEFTYPE_API ITypeInfo * getPromotedAddSubType(ITypeInfo * l_type, ITypeInfo * r_type);
extern DEFTYPE_API ITypeInfo * getPromotedMulDivType(ITypeInfo * l_type, ITypeInfo * r_type);
extern DEFTYPE_API ITypeInfo * getPromotedDivType(ITypeInfo * l_type, ITypeInfo * r_type);
extern DEFTYPE_API ITypeInfo * getPromotedNumericType(ITypeInfo * l_type, ITypeInfo * r_type);
extern DEFTYPE_API unsigned getClarionResultType(ITypeInfo *type);
extern DEFTYPE_API ITypeInfo * getAsciiType(ITypeInfo * type);
extern DEFTYPE_API ITypeInfo * getBandType(ITypeInfo * type1, ITypeInfo * type2);
extern DEFTYPE_API ITypeInfo * getBorType(ITypeInfo * type1, ITypeInfo * type2);
extern DEFTYPE_API bool haveCommonLocale(ITypeInfo * type1, ITypeInfo * type2);
extern DEFTYPE_API IAtom * getCommonLocale(ITypeInfo * type1, ITypeInfo * type2);
extern DEFTYPE_API ITypeInfo * getPromotedCompareType(ITypeInfo * left, ITypeInfo * right);

extern DEFTYPE_API bool isNumericType(ITypeInfo * type);
extern DEFTYPE_API bool isStringType(ITypeInfo * type);
extern DEFTYPE_API bool isSimpleStringType(ITypeInfo * type);
extern DEFTYPE_API bool isIntegralType(ITypeInfo * type);
extern DEFTYPE_API bool isPatternType(ITypeInfo * type);
extern DEFTYPE_API bool isUnicodeType(ITypeInfo * type);
extern DEFTYPE_API bool isLittleEndian(ITypeInfo * type);
extern DEFTYPE_API bool isDatasetType(ITypeInfo * type);
extern DEFTYPE_API bool isSingleValuedType(ITypeInfo * type);
inline bool isFixedSize(ITypeInfo * type) { return type && (type->getSize() != UNKNOWN_LENGTH); }
inline bool isUnknownSize(ITypeInfo * type) { return type && (type->getSize() == UNKNOWN_LENGTH); }
inline bool isAnyType(ITypeInfo * type) { return type && (type->getTypeCode() == type_any); }
inline bool isDecimalType(ITypeInfo * type) { return type && (type->getTypeCode() == type_decimal); }
inline bool isDictionaryType(ITypeInfo * type) { return type && (type->getTypeCode() == type_dictionary); }


//If casting a value from type before to type after is the value preserved.
//If the value is not preserved then it means more than one source value can match a target value.
extern DEFTYPE_API bool preservesValue(ITypeInfo * after, ITypeInfo * before);
extern DEFTYPE_API bool preservesOrder(ITypeInfo * after, ITypeInfo * before);
// not quite the same as !preservesValue() since cast between integers of the same size are ok
// if it loses information then multiple before can may to the same after
extern DEFTYPE_API bool castLosesInformation(ITypeInfo * after, ITypeInfo * before);

extern DEFTYPE_API ICharsetInfo * deserializeCharsetInfo(MemoryBuffer &src);
extern DEFTYPE_API ICollationInfo * deserializeCollationInfo(MemoryBuffer &src);
extern DEFTYPE_API ITypeInfo * deserializeType(MemoryBuffer &src);
extern DEFTYPE_API void serializeType(MemoryBuffer &src, ITypeInfo * type);

extern DEFTYPE_API bool getNormalizedLocaleName(unsigned len, char const * str, StringBuffer & buff);
extern DEFTYPE_API bool isSameBasicType(ITypeInfo * left, ITypeInfo * right);
extern DEFTYPE_API ITypeInfo * queryRecordType(ITypeInfo * t);
extern DEFTYPE_API ITypeInfo * queryRowType(ITypeInfo * t);
extern DEFTYPE_API ITypeInfo * queryUnqualifiedType(ITypeInfo * t);
extern DEFTYPE_API ITypeInfo * getFullyUnqualifiedType(ITypeInfo * t);
extern DEFTYPE_API ITypeInfo * removeModifier(ITypeInfo * t, typemod_t modifier);               // don't link inputs
extern DEFTYPE_API ITypeInfo * cloneModifier(ITypeInfo * donorType, ITypeInfo * srcType);       // don't link inputs
extern DEFTYPE_API ITypeInfo * cloneModifiers(ITypeInfo * donorType, ITypeInfo * srcType);      // don't link inputs
extern DEFTYPE_API bool hasModifier(ITypeInfo * t, typemod_t modifier);
extern DEFTYPE_API ITypeInfo * queryModifier(ITypeInfo * t, typemod_t modifier);
extern DEFTYPE_API ITypeInfo * replaceChildType(ITypeInfo * type, ITypeInfo * newChild);
extern DEFTYPE_API ITypeInfo * getRoundType(ITypeInfo * type);
extern DEFTYPE_API ITypeInfo * getRoundToType(ITypeInfo * type);
extern DEFTYPE_API ITypeInfo * getTruncType(ITypeInfo * type);

inline bool hasConstModifier(ITypeInfo * t)      { return hasModifier(t, typemod_const); }
inline bool hasReferenceModifier(ITypeInfo * t)  { return hasModifier(t, typemod_ref); }
inline bool hasWrapperModifier(ITypeInfo * t)    { return hasModifier(t, typemod_wrapper); }
inline bool hasOutOfLineModifier(ITypeInfo * t)  { return hasModifier(t, typemod_outofline); }
inline bool sameUnqualifiedType(ITypeInfo * t1, ITypeInfo * t2)  { return queryUnqualifiedType(t1) == queryUnqualifiedType(t2); }
inline ITypeInfo * stripFunctionType(ITypeInfo * type)
{
    if (type->getTypeCode() == type_function)
        return type->queryChildType();
    return type;
}
 
typedef Linked<ITypeInfo> TypeInfoAttr;
typedef Owned<ITypeInfo> OwnedITypeInfo;
typedef IArrayOf<ITypeInfo> TypeInfoArray;

interface ISchemaBuilder
{
public:
    virtual void addField(const char * name, ITypeInfo & type) = 0;
    virtual void addSetField(const char * name, const char * itemname, ITypeInfo & type) = 0;
    virtual void beginIfBlock() = 0;
    virtual bool beginDataset(const char * name, const char * childname) = 0;
    virtual void beginRecord(const char * name) = 0;
    virtual void endIfBlock() = 0;
    virtual void endDataset(const char * name, const char * childname) = 0;
    virtual void endRecord(const char * name) = 0;
    virtual bool addSingleFieldDataset(const char * name, const char * childname, ITypeInfo & type) = 0;        // return true if supported
};

class DEFTYPE_API XmlSchemaBuilder : public ISchemaBuilder
{
public:
    XmlSchemaBuilder(bool _addHeader) { optionalNesting = 0; addHeader = _addHeader; }

    virtual void addField(const char * name, ITypeInfo & type);
    virtual void addSetField(const char * name, const char * itemname, ITypeInfo & type);
    virtual void beginIfBlock()                                     { optionalNesting++; }
    virtual bool beginDataset(const char * name, const char * childname);
    virtual void beginRecord(const char * name);
    virtual void endIfBlock()                                       { optionalNesting--; }
    virtual void endDataset(const char * name, const char * childname);
    virtual void endRecord(const char * name);
    virtual bool addSingleFieldDataset(const char * name, const char * childname, ITypeInfo & type);

    void getXml(StringBuffer & results);
    void getXml(IStringVal & results);

private:
    void addSchemaPrefix();
    void addSchemaSuffix();
    void clear();
    void getXmlTypeName(StringBuffer & xmlType, ITypeInfo & type);
    void appendField(StringBuffer &xml, const char * name, ITypeInfo & type);

protected:
    StringBuffer xml;
    StringBufferArray attributes;
    StringBuffer typesXml;
    IntArray dataSizes;
    IntArray stringSizes;
    IntArray decimalSizes;
    UnsignedArray nesting;
    CopyArray setTypes;
    unsigned optionalNesting;
    bool addHeader;
};

#endif
