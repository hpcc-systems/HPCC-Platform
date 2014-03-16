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

#pragma warning(disable:4786)

#include <limits.h>
#include <map>
#include <string>
#include <algorithm>
#include <vector>

#include "xsdparser.hpp"
#include "jstring.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jlog.hpp"

//==========================================================
//  Definitions

// TODO: only support string type for now
class CXmlAttribute : extends CInterface, implements IXmlAttribute
{
    StringAttr m_name, m_defValue;
    
public:
    IMPLEMENT_IINTERFACE;

    CXmlAttribute(const char* name) : m_name(name) { }

    const char* queryName() { return m_name.get(); }
    const char* queryTypeName() { return "string"; }

    bool isUseRequired() {  return false; }
    const char* getFixedValue() { return NULL; }

    void setDefaultValue(const char* v) {  m_defValue.set(v); }
    bool hasDefaultValue() {  return m_defValue.get()!=NULL; }
    const char* getDefaultValue() { return m_defValue.get(); }

    virtual const char* getSampleValue(StringBuffer& out) 
    {
        const char* s = getFixedValue();
        if (s)
            return out.append(s).str();
        out.appendf("[@%s]", queryName());

        return out.str();
    }
};

class CSimpleType : extends CInterface, implements IXmlType
{
    StringAttr m_name, m_defValue;
    CXmlAttribute** m_attrs;
    size_t m_nAttrs;

public:
    IMPLEMENT_IINTERFACE;

    CSimpleType(const char* name, size_t nAttrs=0, CXmlAttribute** attrs=NULL) 
        : m_name(name), m_nAttrs(nAttrs), m_attrs(attrs) { }

    virtual ~CSimpleType() 
    { 
        if (m_attrs) 
        { 
            for (int i=0; i<m_nAttrs; i++)
                m_attrs[i]->Release();
            delete[] m_attrs; 
        } 
    }
    
    XmlSubType getSubType() { return SubType_Default; }
    bool isArray() {  return false; }

    bool isComplexType() { return false; }
    size_t getFieldCount() { return 0; }
    IXmlType* queryFieldType(int idx) { return NULL; }
    const char* queryFieldName(int idx) { return NULL; }

    const char* queryName() { return m_name.get(); }

    size_t getAttrCount() { return m_nAttrs; }
    IXmlAttribute* queryAttr(int idx) { assert(idx>=0 && idx<m_nAttrs); return m_attrs[idx]; }
    const char* queryAttrName(int idx) { assert(idx>=0 && idx<m_nAttrs); return m_attrs[idx]->queryName(); }

    void setDefaultValue(const char* v) {  m_defValue.set(v); }
    bool hasDefaultValue() {  return m_defValue.get()!=NULL; }
    const char* getDefaultValue() { return m_defValue.get(); }

    virtual void getSampleValue(StringBuffer& out, const char* fieldName);

    void toString(StringBuffer& s, int indent, StringStack& parent) 
    {  s.appendf("Simple Type: %s", m_name.get()); }
};

void CSimpleType::getSampleValue(StringBuffer& out, const char* fieldName) 
{
    const char* name = queryName();

    // use default value if exists
    if (hasDefaultValue())
        out.append(getDefaultValue());

    // string
    else if (streq(name,"string"))
    {
        if (fieldName)
            out.appendf("[ %s ]", fieldName);
        else
            out.append("String");
    }

    // numerical
    else if (streq(name, "short"))
        out.append("4096");
    else if (streq(name,"int") || streq(name, "integer"))
        out.append("32716");
    else if (streq(name, "long"))
        out.append("2147483647");
    else if (streq(name, "float"))
        out.append("3.14159");
    else if (streq(name, "double"))
        out.append("3.14159265358979");
    else if (streq(name, "boolean"))
        out.append("1");
    else if (streq(name, "nonPositiveInteger"))
        out.append("-1");
    else if (streq(name, "negativeInteger"))
        out.append("-2");
    else if (streq(name, "byte"))
        out.append("127");
    else if (streq(name, "nonNegativeInteger"))
        out.append("1");
    else if (streq(name, "positiveInteger"))
        out.append("2");
    else if (streq(name, "unsignedLong"))
        out.append("4294967295");
    else if (streq(name, "unsignedInt"))
        out.append("0");
    else if (streq(name, "unsignedShort"))
        out.append("65535");
    else if (streq(name, "unsignedByte"))
        out.append("255");
    else if (streq(name, "decimal"))
        out.append("3.1415926535897932384626433832795");

    // time
    else if (streq(name, "duration"))
        out.append("P1Y2M3DT10H30M");
    else if (streq(name, "dateTime"))
        out.append("2007-10-23 11:34:30");
    else if (streq(name, "time"))
        out.append("11:34:30");
    else if (streq(name, "date"))
        out.append("2007-10-23");
    else if (streq(name, "gYearMonth"))
        out.append("2007-10");
    else if (streq(name, "gYear"))
        out.append("2007");
    else if (streq(name, "gMonthDay"))
        out.append("--10-23");
    else if (streq(name, "gDay"))
        out.append("---23");
    else if (streq(name, "gMonth"))
        out.append("--10--");

    // other
    else if (streq(name, "hexBinary"))
        out.append("A9D4C56EFB");
    else if (streq(name, "base64Binary"))
        out.append("YmFzZTY0QmluYXJ5");
    else if (streq(name, "anyURI"))
        out.append("http://anyURI/");
    else if (streq(name, "QName"))
        out.append("q:name");
    else if (streq(name, "NOTATION"))
        out.append("NOTATION");
    else if (streq(name, "normalizedString"))
        out.append("normalizedString");
    else if (streq(name, "token"))
        out.append("token");
    else if (streq(name, "language"))
        out.append("en-us");

    // unhandled
    else
        out.appendf("%s value", name);
}

//==========================================================

// RestrictionFacetType
typedef int RestrictionFacetType;

const RestrictionFacetType RF_MinLength    = 0x0001;
const RestrictionFacetType RF_MaxLength    = 0x0002;
const RestrictionFacetType RF_MinExclusive = 0x0004;
const RestrictionFacetType RF_MaxExclusive = 0x0008;
const RestrictionFacetType RF_MinInclusive = 0x0010; 
const RestrictionFacetType RF_MaxInclusive = 0x0020;
const RestrictionFacetType RF_Enumeration  = 0x0040;
const RestrictionFacetType RF_Pattern      = 0x0080;
const RestrictionFacetType RF_WhiteSpace   = 0x0100;
const RestrictionFacetType RF_TotalDigits  = 0x0200;
const RestrictionFacetType RF_FractionDigits=0x0400;
const RestrictionFacetType RF_Length       = 0x0800;

static const char* getFacetName(RestrictionFacetType type)
{
    switch(type)
    {
    case RF_MinLength:   return "RF_MinLength";
    case RF_MaxLength:   return "RF_MaxLength";
    case RF_MinExclusive:return "RF_MinExclusive";
    case RF_MaxExclusive:return "RF_MaxExclusive";
    case RF_MinInclusive:return "RF_MinInclusive";
    case RF_MaxInclusive:return "RF_MaxInclusive";
    case RF_Enumeration :return "RF_Enumeration";
    case RF_Pattern     :return "RF_Pattern";
    case RF_WhiteSpace  :return "RF_WhiteSpace";
    case RF_TotalDigits :return "RF_TotalDigits";
    case RF_FractionDigits:return "RF_FractionDigits";
    case RF_Length      :return "RF_Length";
    default: return "Unknown type";
    }
}

enum RestrictionWhiteSpace 
{
    RF_WS_Preserve,
    RF_WS_Replace,
    RF_WS_Collapse
};

union RestrictionFacetValue
{
    int intValue;
    double doubleValue;
    StringAttr* pattern;
    StringArray* enums;
    RestrictionWhiteSpace whiteSpace;
};

struct RestrictionFacet
{   
    RestrictionFacetType type;
    RestrictionFacetValue value;
    RestrictionFacet(RestrictionFacetType t, RestrictionFacetValue v) : type(t), value(v) { }
};

class CRestrictionType : extends CSimpleType 
{
    IXmlType* m_baseType;
    int m_types; 
    typedef std::vector<RestrictionFacet> FacetArray;
    FacetArray m_facets;

public:
    CRestrictionType(const char* name, IXmlType* base) 
        : CSimpleType(name), m_baseType(base), m_types(0)
    { }

    void addFacet(RestrictionFacetType type, RestrictionFacetValue value);
    RestrictionFacetValue queryFacetValue(RestrictionFacetType type);
    IXmlType* queryBaseType() {  return m_baseType; }

    virtual void getSampleValue(StringBuffer& out, const char* fieldName);
    virtual void toString(StringBuffer& s, int indent, StringStack& parent);
};

void CRestrictionType::addFacet(RestrictionFacetType type, RestrictionFacetValue value)
{
    if (m_types & type)
        ERRLOG(-1,"Error in CRestrictionType::addFacet: one facet type can only have one value");
    else 
    {
        m_types |= type;
        m_facets.push_back(RestrictionFacet(type,value));
    }
}

RestrictionFacetValue CRestrictionType::queryFacetValue(RestrictionFacetType type)
{
    if (m_types & type)
    {
        for (FacetArray::const_iterator it = m_facets.begin(); it != m_facets.end(); it++)
        {
            if (it->type == type)
                return it->value;
        }
    }
    
    throw MakeStringException(-1,"Error in CRestrictionType::queryFacetValue: unknown facet: %s", getFacetName(type));  
}

void CRestrictionType::getSampleValue(StringBuffer& out, const char* fieldName)
{
    if (m_types & RF_Enumeration)
    {
        assert(m_facets.size()==1 && m_facets[0].type==RF_Enumeration);
        int count = m_facets[0].value.enums->length();
        out.append(m_facets[0].value.enums->item(count>1 ? 1 : 0));
    }
    else if (m_types & RF_MaxLength)
    {
        if (streq(queryBaseType()->queryName(), "string"))
        {
            int maxLength = queryFacetValue(RF_MaxLength).intValue;
            if (maxLength==1)
                out.append('[');
            else if (maxLength>=2)
            {
                const char* name = fieldName ? fieldName : queryName();
                out.append('[');
                int gap = maxLength - 2 - strlen(name);
                if (gap <= 0)
                    out.append(maxLength-2, name);
                else if (gap<=2)
                    out.append(' ').append(name);
                else {
                    out.appendN((gap-2)/2, 'X');
                    out.append(' ').append(name).append(' ');
                    out.appendN((gap-2+1)/2,'X');
                }
                out.append(']');
            }
        }
        else
        {
            m_baseType->getSampleValue(out,fieldName);
        }
    }
    else
    {
        ERRLOG("CRestrictionType::getSampleValue() unimplemeted yet");
        m_baseType->getSampleValue(out,fieldName);
    }
}

void CRestrictionType::toString(StringBuffer& s, int indent, StringStack& parent) 
{  
    s.appendf("CRestrictionType: %s", m_baseType->queryName());
    for (int i=0; i<m_facets.size(); i++)
    {
        RestrictionFacet& f = m_facets[i];
        switch (f.type)
        {
        case RF_Length: s.appendf(", length='%d'", f.value.intValue); 
            break;
        case RF_MinLength: s.appendf(", minLength='%d'", f.value.intValue); 
            break;
        case RF_MaxLength: s.appendf(", maxLength='%d'", f.value.intValue); 
            break;

        case RF_TotalDigits: s.appendf(", totalDigits='%d'", f.value.intValue); 
            break;
        case RF_FractionDigits: s.appendf(", fractionDigits='%d'", f.value.intValue); 
            break;

        case RF_MinExclusive: 
            if (streq(m_baseType->queryName(),"integer"))
                s.appendf(", minExclusive='%d'", f.value.intValue); 
            else
                s.appendf(", minExclusive='%g'", f.value.doubleValue); 
            break;
        case RF_MaxExclusive:               
            if (streq(m_baseType->queryName(),"integer"))
                s.appendf(", maxExclusive='%d'", f.value.intValue); 
            else
                s.appendf(", maxExclusive='%g'", f.value.doubleValue); 
            break;
        
        case RF_MinInclusive: 
            if (streq(m_baseType->queryName(),"integer"))
                s.appendf(", minInclusive='%d'", f.value.intValue); 
            else
                s.appendf(", minInclusive='%g'", f.value.doubleValue); 
            break;
        case RF_MaxInclusive:               
            if (streq(m_baseType->queryName(),"integer"))
                s.appendf(", maxInclusive='%d'", f.value.intValue); 
            else
                s.appendf(", maxInclusive='%g'", f.value.doubleValue); 
            break;

        case RF_Pattern: s.appendf(", pattern='%s'", f.value.pattern->get());
            break;

        case RF_WhiteSpace: 
            s.appendf(", whiteSpace='%s'", (f.value.whiteSpace==RF_WS_Preserve)?"preserve" : ((f.value.whiteSpace==RF_WS_Replace)?"replace" : "collapse"));
            break;

        case RF_Enumeration:
            {
                s.append(", enumeration={");
                for (int i=0; i<f.value.enums->length(); i++)
                {
                    if (i>0)
                        s.append(",");
                    s.append(f.value.enums->item(i));
                }
                s.append("}");
            }               
            break;

        default:
            throw MakeStringException(-1,"Unknown/unhandled restriction facet: %d", (int)f.type);
        }
    }
}

/*
class CSimpleEnumType : extends CSimpleType
{
    IXmlType* m_baseType;
    StringArray m_enums;
public: 
    CSimpleEnumType(const char* name, IXmlType* base)
        : CSimpleType(name), m_baseType(base)
    { }

    void setEnumValues(int n, const char* values[]) { 
        for (int i=0;i<n;i++)
            m_enums.append(values[i]);
    }
    void addEnumValue(const char* v) { m_enums.append(v); }

    size32_t getEnumCounts() { return m_enums.length(); }
    const char* getEnumAt(int idx) {  (idx>=0 && idx<m_enums.length()) ? m_enums.item(idx) : NULL; }

    void toString(StringBuffer& s, int indent, StringStack& parent) { s.appendf("CSimpleEnumType: %s", m_baseType->queryName()); }
};
*/

class CComplexType : extends CInterface, implements IXmlType
{
protected:
    StringAttr m_name;
    size_t     m_fldCount;
    char**     m_fldNames;
    IXmlType** m_fldTypes;
    size_t     m_nAttrs;
    IXmlAttribute** m_attrs;
    XmlSubType m_subType;

public: 
    IMPLEMENT_IINTERFACE;

    CComplexType(const char* name, XmlSubType subType, size_t count, IXmlType** els, char** names, size_t nAttrs, IXmlAttribute** attrs=NULL) 
        : m_name(name), m_subType(subType), m_fldCount(count), m_fldNames(names), 
        m_fldTypes(els), m_nAttrs(nAttrs), m_attrs(attrs) { }
    
    virtual ~CComplexType() 
    { 
        // types are cached, but not linked
        if (m_fldTypes)
            delete[] m_fldTypes;
        
        if (m_fldNames)
        {
            for (int i=0; i<m_fldCount; i++)
                free(m_fldNames[i]); 
            delete[] m_fldNames;
        }

        if (m_attrs) 
        {
            for (int i=0; i<m_nAttrs; i++)
                m_attrs[i]->Release();
            delete[] m_attrs;
        }
    }

    const char* queryName() {  return m_name.get(); }

    bool isComplexType() { return true; }
    bool isArray()       { return false;}
    XmlSubType getSubType() { return m_subType; }
    size_t getFieldCount() { return m_fldCount; }
    IXmlType* queryFieldType(int idx) { return m_fldTypes[idx]; }
    const char* queryFieldName(int idx) {  return m_fldNames[idx]; }

    size_t getAttrCount() { return m_nAttrs; }
    IXmlAttribute* queryAttr(int idx) { return m_attrs[idx]; }
    const char* queryAttrName(int idx) { assert(idx>=0 && idx<m_nAttrs); return m_attrs[idx]->queryName(); }

    bool hasDefaultValue() {  assertex(!"N/A"); return false; }
    const char* getDefaultValue() { assertex(!"N/A"); return NULL; }

    void getSampleValue(StringBuffer& out, const char* fieldName) { assert(false); }

    void toString(StringBuffer& s, int indent, StringStack& parent);
};

// treat as a ComplexType with 1 field
class CArrayType : extends CInterface, implements IXmlType
{
protected:
    StringAttr m_name;
    StringAttr m_itemName;
    IXmlType* m_itemType;

public: 
    IMPLEMENT_IINTERFACE;

    CArrayType(const char* name, const char* itemName, IXmlType* itemType) 
        : m_name(name), m_itemName(itemName), m_itemType(itemType) { }

    const char* queryName() {  return m_name.get(); }

    bool isComplexType() { return false; }
    bool isArray()       { return true;}
    XmlSubType getSubType() { return SubType_Array; }
    const char* queryItemName() { return m_itemName.get(); }
    size_t getFieldCount() { return 1; }
    IXmlType* queryFieldType(int idx) { return m_itemType; }
    const char* queryFieldName(int idx) { return m_itemName.get(); }

    size_t getAttrCount() { return 0; }  // removed assert false to account for arrays
    const char* queryAttrName(int idx) { assert(false); return NULL; }
    IXmlAttribute* queryAttr(int idx) { assert(false); return NULL; }

    bool hasDefaultValue() {  assertex(!"N/A"); return false; }
    const char* getDefaultValue() { assertex(!"N/A"); return NULL; }
    
    void getSampleValue(StringBuffer& out, const char* fieldName) { assert(false); }

    void toString(StringBuffer& s, int indent, StringStack& parent); 
};

class CXmlSchema : extends CInterface, implements IXmlSchema
{
protected:
    Owned<IPTree> m_schema;
    StringAttr    m_xsdNs;
    int           m_unnamedIdx;
    
    void setSchemaNamespace();
    const char* xsdNs() { return m_xsdNs.get(); }
    IXmlType* parseComplexType(IPTree* complexDef);
    IXmlType* parseSimpleType(IPTree* simpleDef);
    IXmlType* getNativeSchemaType(const char* type, const char* defValue);
    IXmlType* parseTypeDef(IPTree* el, const char* name=NULL);
    size_t parseAttributes(IPTree* typeDef, IXmlAttribute** &attrs);

    typedef std::map<std::string, IXmlType*> TypeMap;
    TypeMap m_types;
    void addCache(const char* name, IXmlType* type) 
    {  
        if (name)
        {
            assert(m_types.find(name) == m_types.end());
            m_types[name] = type;
        }
        else
        {
            VStringBuffer n("__unnnamed__%d", m_unnamedIdx++);
            m_types[n.str()] = type;
        }
    }

    IXmlType* queryTypeByName(const char* typeName,const char* defValue);

public:
    IMPLEMENT_IINTERFACE;

    CXmlSchema(const char* src);
    CXmlSchema(IPTree* schema);

    virtual ~CXmlSchema();

    IXmlType* queryElementType(const char* name);
    IXmlType* queryTypeByName(const char* typeName) { return queryTypeByName(typeName); }
};

XMLLIB_API IXmlSchema* createXmlSchemaFromFile(const char* file)
{
    StringBuffer src;
    try {
        src.loadFile(file);
    } catch (IException* e) {
        StringBuffer msg;
        fprintf(stderr,"Exception caught: %s", e->errorMessage(msg).str());
        return NULL;
    }

    return new CXmlSchema(src);
}

XMLLIB_API IXmlSchema* createXmlSchemaFromString(const char* schemaSrc)
{
    return new CXmlSchema(schemaSrc);
}

XMLLIB_API IXmlSchema* createXmlSchemaFromPTree(IPTree* schema)
{
    return new CXmlSchema(schema);
}

//==========================================================
//  Implementation

void CComplexType::toString(StringBuffer& s, int indent, StringStack& parent)
{  
    s.appendf("%s: ComplexType", queryName()?queryName():"<unnamed>");
    if (queryName())
        parent.push_back(queryName());
    for (int i=0; i<m_fldCount; i++)
    {
        s.append('\n').pad(indent+1).appendf("%s: ", queryFieldName(i));
        if (!queryFieldName(i) || std::find(parent.begin(),parent.end(),queryFieldName(i)) == parent.end())
            queryFieldType(i)->toString(s,indent+1,parent);
        else
            s.appendf(" --> see type: %s", m_fldTypes[i]->queryName());
    }
    if (m_nAttrs>0)
    {
        s.append('\n').pad(indent+1).append("Attributes:");
        for (int i=0; i<m_nAttrs; i++)
            s.appendf("%s", queryAttrName(i));
    }
    if (queryName())
        parent.pop_back();
}

void CArrayType::toString(StringBuffer& s, int indent, StringStack& parent)
{ 
    s.appendf("%s: array of %s: item=%s", queryName()?queryName():"<unnamed>", 
        m_itemType->queryName()?m_itemType->queryName():"<unnamed>", queryItemName()); 
    if (!m_itemType->queryName() || std::find(parent.begin(),parent.end(),m_itemType->queryName()) == parent.end())
    {
        if (queryName())
            parent.push_back(queryName());
        s.append('\n').pad(indent+1);
        m_itemType->toString(s, indent+2,parent);
        if (queryName())
            parent.pop_back();
    }
    else
        s.append('\n').pad(indent+1).appendf("--> see type: %s", m_itemType->queryName());
}

CXmlSchema::CXmlSchema(const char* schemaSrc) 
{
    m_unnamedIdx = 0;
    try {
        m_schema.setown(createPTreeFromXMLString(schemaSrc));
        setSchemaNamespace();
    } catch (IException* e) {
        StringBuffer msg;
        fprintf(stderr,"Exception caught: %s", e->errorMessage(msg).str());
    }   

    if (!m_schema.get())
        m_schema.setown(createPTree("xsd:schema"));
}

CXmlSchema::CXmlSchema(IPTree* schema) 
{
    m_unnamedIdx = 0;
    m_schema.setown(schema);
    setSchemaNamespace();
}

CXmlSchema::~CXmlSchema()
{
    for (TypeMap::const_iterator it = m_types.begin(); it != m_types.end(); it++)
        it->second->Release();
}

void CXmlSchema::setSchemaNamespace()
{
    Owned<IAttributeIterator> attrs = m_schema->getAttributes();
    
    for (attrs->first(); attrs->isValid(); attrs->next())
    {
        if (strcmp(attrs->queryValue(), "http://www.w3.org/2001/XMLSchema") == 0)
        {
            const char* name = attrs->queryName();
            if (strncmp(name, "@xmlns",6)==0)
            {
                if (*(name+6)==0)
                    m_xsdNs.set("");
                else if (*(name+6)==':')
                {
                    char* x = (char*)malloc(strlen(name)-7+2);
                    sprintf(x, "%s:", name+7);
                    m_xsdNs.setown(x);
                }
                break;
            }
        }
    }

    if (!m_xsdNs.get())
        m_xsdNs.set("xsd:");
}

IXmlType* CXmlSchema::getNativeSchemaType(const char* typeName, const char* defValue)
{
    StringBuffer key(typeName);
    if (defValue) 
        key.append(':').append(defValue);
    TypeMap::const_iterator it = m_types.find(key.str());
    if (it != m_types.end())
        return it->second;
    //TODO: Need validataion?
    CSimpleType* typ = new CSimpleType(typeName);
    if (defValue)
        typ->setDefaultValue(defValue);
    addCache(key,typ);
    return typ;
}

size_t CXmlSchema::parseAttributes(IPTree* typeDef, IXmlAttribute** &attrs)
{
    attrs=NULL;
    Owned<IPTreeIterator> ats = typeDef->getElements(VStringBuffer("%sattribute",xsdNs()));
    size_t nAttrs = 0;
    for (ats->first(); ats->isValid(); ats->next())
        nAttrs++;
    if (nAttrs>0)
    {
        attrs = new IXmlAttribute*[nAttrs];
        int idx = 0;
        for (ats->first(); ats->isValid(); ats->next())
            attrs[idx++] = new CXmlAttribute(ats->query().queryProp("@name"));
    }
    return nAttrs;
}   

IXmlType* CXmlSchema::parseComplexType(IPTree* complexDef)
{
    const char* name = complexDef->queryProp("@name"); // can be NULL: unnamed type (in-place type definition)

    XmlSubType subType = SubType_Default;

    // all
    IPTree* sub = complexDef->queryBranch(VStringBuffer("%sall",xsdNs()));
    if (sub) 
        subType = SubType_Complex_All;
    
    // sequence
    if (!sub)
    {
        sub = complexDef->queryBranch(VStringBuffer("%ssequence",xsdNs()));
        if (sub)
            subType = SubType_Complex_Sequence;
    }

    // choice
    if (!sub)
    {
        sub = complexDef->queryBranch(VStringBuffer("%schoice",xsdNs()));
        if (sub)
            subType = SubType_Complex_Choice;
    }

    // simpleContent
    if (!sub)
    {
        sub = complexDef->queryBranch(VStringBuffer("%ssimpleContent",xsdNs()));
        if (sub)
            subType = SubType_Complex_SimpleContent;
    }

    if (!sub)
    {
        // attributes only?
        if (complexDef->queryBranch(VStringBuffer("%sattribute[1]",xsdNs())))
        {
            sub = complexDef; // a workaround since xsd:attribute is directly below xsd:complexType
            subType = SubType_Complex_All; // empty all
        }
    }

    if (sub)
    {
        Owned<IPTreeIterator> els = sub->getElements(VStringBuffer("%selement",xsdNs()));
        
        size_t fldCount = 0;
        size_t typelessCount = 0;
        for (els->first(); els->isValid(); els->next())
        {
            fldCount++;
            if (!els->query().hasProp("@type") && !els->query().hasChildren())
                typelessCount++;
        }

        // TODO: verify with struct with one field
        if ((fldCount-typelessCount)==1) // hack: assume 1 to be array
        {
            for (els->first(); els->isValid(); els->next())
                if (els->query().hasProp("@type") || els->query().hasChildren())
                    break;

            IPTree& el = els->query();
            const char* maxOccurs = sub->queryProp("@maxOccurs");
            if (!maxOccurs)
                maxOccurs = els->query().queryProp("@maxOccurs");
            if (maxOccurs && strcmp(maxOccurs, "unbounded")==0)
            {
                const char* itemName = el.queryProp("@name");
                const char* typeName = el.queryProp("@type");
                IXmlType* type = typeName ? queryTypeByName(typeName,el.queryProp("@default")) : parseTypeDef(&el);

                CArrayType* typ = new CArrayType(name, itemName, type);
                addCache(name,typ);
                return typ;
            }
        }

        if (subType == SubType_Complex_SimpleContent)
        {
            assert(fldCount==0);

            IPTree* ext = sub->queryBranch(VStringBuffer("%sextension", xsdNs()));
            if (ext)
            {
                // attrs
                IXmlAttribute** attrs=NULL;
                size_t nAttrs = parseAttributes(ext,attrs);

                // let the first fldType be to the base type
                IXmlType** types = new IXmlType*[1];
                const char* base = sub->queryProp(VStringBuffer("%sextension/@base",xsdNs()));              
                assert(base);
                if (startsWith(base,xsdNs()))
                    types[0] = getNativeSchemaType(base+strlen(xsdNs()),NULL);
                else
                {
                    StringBuffer schema;
                    toXML(complexDef,schema);
                    DBGLOG(-1,"Invalid schema: %s", schema.str());
                    throw MakeStringException(-1, "Invalid schema encoutered");
                }
                
                CComplexType* typ = new CComplexType(name,subType,fldCount,types,NULL,nAttrs,attrs);
                addCache(name,typ);
                return typ;
            }
            else if (sub->queryBranch(VStringBuffer("%srestriction", xsdNs())))
            {
                assert(false);
            } 
            else
            {
                StringBuffer schema;
                toXML(complexDef,schema);
                DBGLOG(-1,"Invalid schema: %s", schema.str());
                throw MakeStringException(-1, "Invalid schema encoutered");
            }
        }
        else
        {
            // attrs
            IXmlAttribute** attrs=NULL;
            size_t nAttrs = parseAttributes(complexDef,attrs);

            IXmlType** types = fldCount ? new IXmlType*[fldCount] : NULL;
            char** names = fldCount ? new char*[fldCount] : NULL;
            CComplexType* typ = new CComplexType(name,subType,fldCount,types,names,nAttrs,attrs);
            addCache(name,typ);

            int fldIdx = 0;
            for (els->first(); els->isValid(); els->next())
            {
                IPTree& el = els->query();
                
                const char* itemName = el.queryProp("@name");
                const char* typeName = el.queryProp("@type");
                IXmlType* type = typeName ? queryTypeByName(typeName,el.queryProp("@default")) : parseTypeDef(&el);
                if (!type)
                    type = getNativeSchemaType("none", el.queryProp("@default")); //really should be tag only, no content?
                
                types[fldIdx] = type;
                names[fldIdx] = strdup(itemName);
                fldIdx++;
            }
    
            return typ;
        }
    }

    // unhandled
    {
        StringBuffer schema;
        toXML(complexDef,schema);
        DBGLOG(-1,"Parse schema failed: name=%s, schema: %s", name?name:"<no-name>",schema.str());
        throw MakeStringException(-1, "Internal error: parse schema failed");
    }

    return NULL;
}

IXmlType* CXmlSchema::parseSimpleType(IPTree* simpleDef)
{
    IPTree* sub = simpleDef->queryBranch(VStringBuffer("%srestriction",xsdNs()));
    if (sub)
    {
        const char* base = sub->queryProp("@base");
        if (startsWith(base, xsdNs()))
            base += m_xsdNs.length();
        const char* name = simpleDef->queryProp("@name");
        if (!name || !*name)
            throw MakeStringException(-1, "Invalid schema: missing name for simple restriction type");
        if (!base || !*base)
            throw MakeStringException(-1, "Invalid schema: missing base type for simple restriction type: %s", name);

        IXmlType* baseType = getNativeSchemaType(base, sub->queryProp("@default"));
        CRestrictionType* type = new CRestrictionType(name,baseType);
        addCache(name,type);
        RestrictionFacetValue fv;
        
        if (sub->queryProp(VStringBuffer("%senumeration[1]/@value",xsdNs())))
        {
            Owned<IPTreeIterator> it = sub->getElements(VStringBuffer("%senumeration", xsdNs()));
            StringArray* enums = new StringArray();
            for (it->first(); it->isValid(); it->next())
                enums->append( it->query().queryProp("@value") );
            fv.enums = enums;
            type->addFacet(RF_Enumeration,fv);
        }

        const char* v = sub->queryProp(VStringBuffer("%smaxLength/@value",xsdNs()));
        if (v)
        {
            fv.intValue = atoi(v);
            type->addFacet(RF_MaxLength, fv);
        }

        v = sub->queryProp(VStringBuffer("%sminLength/@value",xsdNs()));
        if (v)
        {
            fv.intValue = atoi(v);
            type->addFacet(RF_MinLength, fv);
        }

        //TODO: more facets here

        return type;
    }

    assert(!"Unhandled simple type");
    return NULL;
}

IXmlType* CXmlSchema::queryElementType(const char* name)
{
    VStringBuffer xpath("%selement[@name='%s']", xsdNs(), name);
    IPTree* el = m_schema->queryBranch(xpath);

    // <xsd:element name="xxx" 
    if (el)
    {
        const char* type = el->queryProp("@type");
        if (type)
            return queryTypeByName(type,el->queryProp("@default"));
        else
            return parseTypeDef(el);
    }

    //TODO: roxie type: 
    //xpath.setf("%selement[@name=\"Dataset\"]/%scomplexType/%s:sequence/%s:element[@name=\"Row\"]",xsdNs(),xsdNs(),xsdNs(),xsdNs());

    return NULL;
}

IXmlType* CXmlSchema::parseTypeDef(IPTree* el, const char* name)
{
    // complex?
    VStringBuffer xpath("%scomplexType",xsdNs());
    if (name)
        xpath.appendf("[@name='%s']", name);
    IPTree* complex = el->queryBranch(xpath);
    if (complex)
        return parseComplexType(complex);

    // simple?
    xpath.setf("%ssimpleType",xsdNs());
    if (name)
        xpath.appendf("[@name='%s']", name);
    IPTree* simple = el->queryBranch(xpath);
    if (simple)
        return parseSimpleType(simple);

    // unknown
    return NULL;
}

IXmlType* CXmlSchema::queryTypeByName(const char* name, const char* defValue)
{
    if (startsWith(name, xsdNs()))
        return getNativeSchemaType(name+m_xsdNs.length(),defValue);

    const char* colon = strchr(name, ':');
    if (colon) 
        name = colon+1; // TODO: verify tns: 

    TypeMap::const_iterator it = m_types.find(name);
    if (it != m_types.end())
        return it->second;

    return parseTypeDef(m_schema.get(), name);
}
