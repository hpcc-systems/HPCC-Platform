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

#ifndef __SOAPTYPE_HPP__
#define __SOAPTYPE_HPP__

#pragma warning( disable : 4786)

#include "jliball.hpp"

#include "SOAP/Platform/soapmessage.hpp"
#include "SOAP/Platform/soapservice.hpp"
#include "SOAP/Platform/soapparam.hpp"

#define PURE_VIRTUAL_FUNC_CALLED throw MakeStringException(-1, "Pure virtual function called at %s(%s)", __FILE__, __LINE__); 

// ==============================================================================
// interfaces

interface ISoapField;

interface ISoapType 
{
    // basic
    virtual bool hasCustomHttpContent() = 0;
    virtual bool isPrimitiveType() = 0;
    virtual bool isArrayType() = 0;
    virtual const char* queryTypeName() = 0; // for array, the name of the elmenent type
    
    // wsdl
    virtual StringBuffer& getXsdDefinition(IEspContext& ctx, StringBuffer &schema, wsdlIncludedTable &added) = 0;
    virtual StringBuffer& getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns="xsd", const char *wsns="tns", unsigned flags=1) = 0;
    
    // form
    virtual StringBuffer& getHtmlForm(IEspContext &ctx, CHttpRequest* req, const char *serv, 
        const char *method, StringBuffer &form, bool includeFormTag, const char *prefix) = 0;

    // field factory
    virtual ISoapField* createField(const char* name) = 0;
    virtual ISoapField* createField(const char *serviceName, IRpcMessage* rpcmsg) = 0;
    virtual ISoapField* createField(const char *serviceName, IProperties *params, MapStrToBuf *attachments) = 0;
};

union FieldValue
{
    int intV;
    unsigned int uintV;
    __int64 int64V;
    short shortV;
    const char* stringV;
    double doubleV;
    bool   boolV;
    ISoapField* structV;
    StringArray* stringarrayV;
    Array*       arrayV;

    FieldValue() { }
    FieldValue(int v) { intV = v; }
    FieldValue(unsigned int v) { uintV = v; }
    FieldValue(__int64 v) { int64V; }
    FieldValue(short v) { shortV = v; }
    FieldValue(const char* v) { stringV = v; }
    FieldValue(double v) { doubleV = v; }
    FieldValue(bool v) { boolV = v; }
    FieldValue(ISoapField* v) {  structV = v; }
    FieldValue(StringArray* v) {  stringarrayV = v; }
    FieldValue(Array* v) { arrayV = v; }
};

interface ISoapField : public IInterface
{
    virtual const char* queryFieldName() = 0;
    virtual ISoapType*  queryFieldType() = 0;
    virtual FieldValue  gtValue() = 0;
    virtual void        stValue(FieldValue)  = 0;
    virtual void        setNull() = 0;
    virtual bool        isNull() = 0;
    
    virtual void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                            const char* itemname, const char *xsdtype="", const char *xmlns="") = 0;
    virtual void marshall(StringBuffer &str, const char *tagname, const char *basepath, 
                            const char* itemname, const char *xsdtype="", const char *xmlns="") = 0;

    virtual void unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                            const char* itemname, const char *xsdtype="", const char *xmlns="") = 0;
    virtual void unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath,
                            const char* itemname, const char *xsdtype="", const char *xmlns="") = 0;
    virtual void unmarshall(CSoapValue &soapval, const char *tagname) = 0;
    
    virtual void serializeContent(StringBuffer& buffer) = 0;
    virtual void unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath, const char* itemname=NULL) = 0;
    virtual void unserialize(CSoapValue& soapval) = 0;
    virtual void unserialize(IProperties& params, MapStrToBuf *attachments, const char *basepath, const char* itemname=NULL) = 0;
};

// ==============================================================================
// types

class CSoapTypeBase : public ISoapType
{
public:
    // basic
    virtual bool hasCustomHttpContent() {  return false; }
    virtual bool isPrimitiveType()  { return false; }
    virtual bool isArrayType()  {  return false; }
    
    // wsdl
    virtual StringBuffer& getXsdDefinition(IEspContext& ctx, StringBuffer &schema, wsdlIncludedTable &added)
    {
        return getXsdDefinition(ctx, queryTypeName(), schema, added);
    }
    virtual StringBuffer& getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns="xsd", const char *wsns="tns", unsigned flags=1) = 0;

    
    // form
    virtual StringBuffer& getHtmlForm(IEspContext &ctx, CHttpRequest* req, const char *serv, 
        const char *method, StringBuffer &form, bool includeFormTag, const char *prefix) 
    {
        assertex(false);
        return form;
    }

    // field factory
    virtual ISoapField* createField(const char* name) 
    {
        assertex(false);
        return NULL;
    }
    virtual ISoapField* createField(const char *serviceName, IRpcMessage* rpcmsg)
    {   
        assert(false);
        return NULL;
    }
    virtual ISoapField* createField(const char *serviceName, IProperties *params, MapStrToBuf *attachments)
    {
        assertex(false);
        return NULL;
    }
};

class CSoapPrimitiveType : public CSoapTypeBase
{
public:
    bool isPrimitiveType()            { return true;  }

    StringBuffer& getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns="xsd", const char *wsns="tns", unsigned flags=1) 
    {
        //TODO: do we need this?
        assertex(false);
        return schema;
    }
};

class CSoapStringType : public CSoapPrimitiveType
{
protected:
    CSoapStringType() { }
    
public:
    const char* queryTypeName()      {  return "string"; }
    static CSoapStringType* instance() {  static CSoapStringType type; return &type; }
    StringBuffer& getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns="xsd", const char *wsns="tns", unsigned flags=1)
    {
        assertex(false);
        return schema;
    }
    ISoapField* createField(const char* name);
};

class ESPHTTP_API CSoapIntType : public CSoapPrimitiveType 
{
public:
    const char* queryTypeName()   {  return "int"; }
    static CSoapIntType* instance() {  static CSoapIntType type; return &type; }
    ISoapField* createField(const char* name); 
};

class ESPHTTP_API CSoapUIntType : public CSoapPrimitiveType 
{
public:
    const char* queryTypeName()   {  return "unsignedInt"; }
    static CSoapUIntType* instance() {  static CSoapUIntType type; return &type; }
    ISoapField* createField(const char* name); 
};

class ESPHTTP_API CSoapShortType : public CSoapPrimitiveType 
{
public:
    const char* queryTypeName()   {  return "short"; }
    static CSoapShortType* instance() {  static CSoapShortType type; return &type; }
    ISoapField* createField(const char* name); 
};

class ESPHTTP_API CSoapUCharType : public CSoapPrimitiveType 
{
public:
    const char* queryTypeName()   {  return "unsignedByte"; }
    static CSoapUCharType* instance() {  static CSoapUCharType type; return &type; }
    ISoapField* createField(const char* name); 
};

class ESPHTTP_API CSoapInt64Type : public CSoapPrimitiveType 
{
public:
    const char* queryTypeName()   {  return "__int64"; }
    static CSoapInt64Type* instance() {  static CSoapInt64Type type; return &type; }
    ISoapField* createField(const char* name); 
};

class ESPHTTP_API CSoapBoolType : public CSoapPrimitiveType 
{
public:
    const char* queryTypeName()    {  return "boolean"; }
    static CSoapBoolType* instance() {  static CSoapBoolType type; return &type; }
    ISoapField* createField(const char* name);
};

class ESPHTTP_API CSoapDoubleType : public CSoapPrimitiveType 
{
public:
    const char* queryTypeName()      {  return "double"; }
    static CSoapDoubleType* instance() {  static CSoapDoubleType type; return &type; }
    ISoapField* createField(const char* name);
};

class ESPHTTP_API CSoapFloatType : public CSoapPrimitiveType 
{
public:
    const char* queryTypeName()      {  return "float"; }
    static CSoapFloatType* instance() {  static CSoapFloatType type; return &type; }
    ISoapField* createField(const char* name);
};

enum EspmType {  EspStruct, EspRequest, EspResponse };

class ESPHTTP_API CSoapStructType : public CSoapTypeBase
{
protected:
    const size_t m_fldCount;
    ISoapType  **m_types;
    const char **m_names;
    IProperties**m_props;

    EspmType     m_espmType;    

    void getFieldsDefinition(IEspContext& ctx, StringBuffer& schema);

public:
    // constructor
    CSoapStructType(const size_t count) : m_fldCount(count), m_types(NULL), m_names(NULL), 
        m_espmType(EspStruct),m_props(NULL)
    { 
    }
    virtual ~CSoapStructType()
    {
        for (int i=0;i<m_fldCount;i++)
            ::Release(m_props[i]);
        delete m_props;
    }
    
    void init(ISoapType* types[], const char* names[])
    {
        m_types = types;
        m_names = names;
        m_props = new IProperties*[m_fldCount];
        memset(m_props,0,sizeof(IProperties*)*m_fldCount);
    }

    virtual void addAttribute(int fldIdx, const char* name, const char* value) 
    { 
        if (m_props[fldIdx]==NULL)
            m_props[fldIdx] = createProperties();
        m_props[fldIdx]->setProp(name,value); 
    }   
    
    virtual const char* queryAttribute(int fldIdx, const char* name) 
    { 
        return m_props[fldIdx]?m_props[fldIdx]->queryProp(name):NULL; 
    }
    
    EspmType getEspmType()    { return m_espmType; }

    // accessors
    virtual const size_t getFieldCount()        { return m_fldCount; }
    virtual ISoapType* queryFieldType(int idx)  { assert(idx>=0 && idx<=m_fldCount); return m_types[idx]; }
    virtual const char* queryFieldName(int idx) { assert(idx>=0 && idx<=m_fldCount); return m_names[idx]; }

    // xsd & form
    virtual StringBuffer& getXsdDefinition(IEspContext &context, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns="xsd", const char *wsns="tns", unsigned flags=1);
    virtual StringBuffer& getHtmlForm(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &form, bool includeFormTag=true, const char *prefix=NULL);
};

class ESPHTTP_API CSoapStructTypeEx : public CSoapStructType
{
protected:
    CSoapStructType *m_parent;
    
public:
    // constructor
    CSoapStructTypeEx(const size_t count,CSoapStructType* parent) 
        : CSoapStructType(count), m_parent(parent)
    { 
    }
    
    void addAttribute(int fldIdx, const char* name, const char* value) 
    { 
        if (fldIdx < m_parent->getFieldCount())
            m_parent->addAttribute(fldIdx,name,value);
        else
            CSoapStructType::addAttribute(fldIdx-m_parent->getFieldCount(),name,value);
    }   
    
    const char* queryAttribute(int fldIdx, const char* name) 
    { 
        if (fldIdx < m_parent->getFieldCount())
            return m_parent->queryAttribute(fldIdx,name);
        else
            return CSoapStructType::queryAttribute(fldIdx-m_parent->getFieldCount(),name);
    }
    
    EspmType getEspmType()    { return m_espmType; }

    // accessors
    const size_t getFieldCount()        { return m_fldCount+m_parent->getFieldCount(); }
    ISoapType* queryFieldType(int idx)  
    { 
        assert(idx>=0 && idx<=getFieldCount()); 
        if (idx < m_parent->getFieldCount())
            return m_parent->queryFieldType(idx); 
        else
            return m_types[idx-m_parent->getFieldCount()];
    }
    const char* queryFieldName(int idx) 
    { 
        assert(idx>=0 && idx<=getFieldCount()); 
        if (idx < m_parent->getFieldCount())
            return m_parent->queryFieldName(idx);
        else
            return m_names[idx-m_parent->getFieldCount()]; 
    }
    //virtual StringBuffer& getHtmlForm(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &form, bool includeFormTag=true, const char *prefix=NULL);
};

//=============================================================================

class CSoapFieldBase : public ISoapField, public CInterface
{
protected:
    StringAttr m_name;
    ISoapType* m_type;
    
public:
    IMPLEMENT_IINTERFACE;
    
    CSoapFieldBase(const char* name, ISoapType* type) : m_name(name), m_type(type) { }
    
    const char* queryFieldName() { return m_name.get(); }
    ISoapType*  queryFieldType() { return m_type; }
    virtual void        setNull() { assertex(false); }
    virtual bool        isNull()             { assertex(false); return false; }
};

class ESPHTTP_API CSoapStringField : public CSoapFieldBase
{
protected:
    StringBuffer m_value;
    bool m_isNil;
    nilBehavior m_nilBH;
    
public:
    CSoapStringField(const char* name) : CSoapFieldBase(name,CSoapStringType::instance()),
        m_nilBH(nilIgnore), m_isNil(true)
    { }
    
    FieldValue gtValue() {  FieldValue fv(m_value.str()); return fv; }
    void stValue(FieldValue value)   { m_value.set(value.stringV); }
    
    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                const char* itemname, const char *xsdtype="", const char *xmlns="", bool *encodex=NULL);
    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                const char* itemname, const char *xsdtype="", const char *xmlns="")
    { marshall(rpc_call, tagname, basepath, itemname, xsdtype, xmlns, NULL); }
    
    void marshall(StringBuffer &str, const char *tagname, const char *basepath, bool encodeXml, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    void marshall(StringBuffer &str, const char *tagname, const char *basepath, 
                const char* itemname, const char *xsdtype="", const char *xmlns="")
    { marshall(str,tagname,basepath,false,itemname,xsdtype,xmlns);  }
    
    
    void unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                const char* itemname, const char *xsdtype="", const char *xmlns="");
    void unmarshall(CSoapValue &soapval, const char *tagname);
    void unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath,
                const char* itemname, const char *xsdtype="", const char *xmlns="");
    void unmarshall(MapStrToBuf *attachments, const char *tagname, const char *basepath, 
                const char* itemname, const char *xsdtype="", const char *xmlns="");
    
    void serializeContent(StringBuffer& buffer)
    { buffer.append(m_value.str()); }
    
    void unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath, const char* itemname=NULL)
    { unmarshall(rpc_request, queryFieldName(), basepath, itemname); }
    
    void unserialize(CSoapValue& soapval)
    { unmarshall(soapval, queryFieldName()); }
    
    void unserialize(IProperties& params, MapStrToBuf *attachments, const char *basepath, const char* itemname=NULL)
    { unmarshall(params, attachments, queryFieldName(), basepath, itemname); }
};

//---------------------------------------------------------------------
// primitive type field

template <class cpptype>
class CSoapPrimitiveField : public CSoapFieldBase
{
protected:
    cpptype m_value;
    bool m_isNil;
    nilBehavior m_nilBH;

public:
    CSoapPrimitiveField(const char* name, ISoapType* type) 
        : CSoapFieldBase(name, type), m_nilBH(nilIgnore), m_isNil(true), m_value(0)
    { }
    
    virtual ~CSoapPrimitiveField() { }
        
    FieldValue gtValue()      {  FieldValue fv(m_value); return fv; }

    void setNull()             { m_isNil = true; }
    bool isNull()              { return m_isNil; }
    
    void copy(CSoapPrimitiveField<cpptype> &from)
    {
        m_value = from.m_value;
        m_isNil = from.m_isNil;
        m_nilBH = from.m_nilBH;
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath,
            const char* itemname, const char *xsdtype="", const char *xmlns="")
    {
        if (!m_isNil || m_nilBH==nilIgnore)
            rpc_call.add_value(basepath, xmlns, tagname, xsdtype, m_value);
    }

    void marshall(StringBuffer &str, const char *tagname, const char *basepath, 
            const char* itemname, const char *xsdtype="", const char *xmlns="")
    {
        marshall(str,tagname,basepath,true,xsdtype,xmlns);
    }

    void marshall(StringBuffer &str, const char *tagname, const char *basepath, 
                                               bool encodeXml=true, const char *xsdtype="", const char *xmlns="")
    {
        if (!m_isNil || m_nilBH==nilIgnore)
        {
            StringBuffer temp;
            temp.append(m_value);
            str.appendf("<%s>", tagname);
            if (encodeXml)
                encodeUtf8XML(temp.str(), str);
            else
                str.append(temp);
            
            str.appendf("</%s>", tagname);
        }
    }

    void unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath,
        const char* itemname, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path(basepath);
        
        if (basepath!=NULL && basepath[0]!=0)
            path.append("/");
        
        path.append(tagname);
        
        m_isNil = !rpc_call.get_value(path.str(), m_value);
    }

    void unmarshall(CSoapValue &soapval, const char *tagname)
    {
        m_isNil = !soapval.get_value(tagname, m_value);
    }
    
    void unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path;
        if (basepath && *basepath)
            path.append(basepath).append(".");
        path.append(tagname);
        
        const char *pval=params.queryProp(path.str());
        m_isNil= !pval || !*pval;
        esp_convert(pval, m_value);
    }
    
    void serializeContent(StringBuffer& buffer)
    { buffer.append(m_value); }
    
    void unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath, const char* itemname=NULL)
    { unmarshall(rpc_request, queryFieldName(), basepath, itemname); }
    
    void unserialize(CSoapValue& soapval)
    { unmarshall(soapval, queryFieldName()); }
    
    void unserialize(IProperties& params, MapStrToBuf *attachments, const char *basepath, const char* itemname=NULL)
    { unmarshall(params, attachments, queryFieldName(), basepath, itemname); }
};

class ESPHTTP_API CSoapIntField : public CSoapPrimitiveField<int>
{
public:
    CSoapIntField(const char* name) : CSoapPrimitiveField<int>(name,CSoapIntType::instance()) { }
    void  stValue(FieldValue val)  { m_value = val.intV; m_isNil = false; }
};

class ESPHTTP_API CSoapInt64Field : public CSoapPrimitiveField<__int64>
{
public:
    CSoapInt64Field(const char* name) : CSoapPrimitiveField<__int64>(name,CSoapInt64Type::instance()) { }
    void  stValue(FieldValue val)  { m_value = val.int64V; m_isNil = false; }
};

class ESPHTTP_API CSoapUIntField : public CSoapPrimitiveField<unsigned int>
{
public:
    CSoapUIntField(const char* name) : CSoapPrimitiveField<unsigned int>(name,CSoapUIntType::instance()) { }
    void  stValue(FieldValue val)  { m_value = val.uintV; m_isNil = false; }
};

class ESPHTTP_API CSoapUCharField : public CSoapPrimitiveField<unsigned char>
{
public:
    CSoapUCharField(const char* name) : CSoapPrimitiveField<unsigned char>(name,CSoapUCharType::instance()) { }
    void  stValue(FieldValue val)  { m_value = val.uintV; m_isNil = false; }
};

class ESPHTTP_API CSoapShortField : public CSoapPrimitiveField<short>
{
public:
    CSoapShortField(const char* name) : CSoapPrimitiveField<short>(name,CSoapIntType::instance()) { }
    void  stValue(FieldValue val)  { m_value = val.shortV; m_isNil = false; }
};

class ESPHTTP_API CSoapBoolField : public CSoapPrimitiveField<bool>
{
public:
    CSoapBoolField(const char* name) : CSoapPrimitiveField<bool>(name,CSoapBoolType::instance()) { }
    void  stValue(FieldValue val)  {  m_value = val.boolV; m_isNil = false; }
};

class ESPHTTP_API CSoapDoubleField : public CSoapPrimitiveField<double>
{
public:
    CSoapDoubleField(const char* name) : CSoapPrimitiveField<double>(name,CSoapDoubleType::instance()) { }
    void  stValue(FieldValue val)  {  m_value = val.doubleV; m_isNil = false; }
};

class ESPHTTP_API CSoapFloatField : public CSoapPrimitiveField<float>
{
public:
    CSoapFloatField(const char* name) : CSoapPrimitiveField<float>(name,CSoapFloatType::instance()) { }
    void  stValue(FieldValue val)  {  m_value = val.doubleV; m_isNil = false; }
};

//---------------------------------------------------------------------
// binary field

class ESPHTTP_API CSoapBinaryType : public CSoapTypeBase
{
public:
    virtual bool isPrimitiveType()  { return true; }

    virtual const char* queryTypeName() { return "binary"; }

    virtual StringBuffer& getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns="xsd", const char *wsns="tns", unsigned flags=1);

    static CSoapBinaryType* instance() {  static CSoapBinaryType type; return &type; }

    virtual ISoapField* createField(const char* name);
};

class ESPHTTP_API CSoapBinaryField : public CSoapFieldBase
{
private:
   MemoryBuffer value_;

public:
    CSoapBinaryField(const char* name, ISoapType* type, nilBehavior nb=nilIgnore)
        : CSoapFieldBase(name,type)
    { }

    virtual ~CSoapBinaryField(){}

    //const MemoryBuffer & getValue() const {return value_;}
    //operator MemoryBuffer& () {return value_;}
    //const MemoryBuffer * operator ->() const {return &value_;}
    //MemoryBuffer * operator ->() {return &value_;}
    //void operator=(MemoryBuffer &val) { value_.clear().append(val);};

    FieldValue gtValue() {  FieldValue fv(this); return fv; }
    void stValue(FieldValue fv) 
    { 
        CSoapBinaryField* f = dynamic_cast<CSoapBinaryField*>(fv.structV); 
        value_.clear().append(f->value_); 
    }


    void copy(CSoapBinaryField &from);

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    void marshall(StringBuffer &str, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    void marshall(StringBuffer &str, const char *tagname, const char *basepath, 
        const char* itemname, bool encodeXml=true, const char *xsdtype="", const char *xmlns="");


    void unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath,
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    void unmarshall(CSoapValue &soapval, const char *tagname);
    void unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath,
        const char* itemname, const char *xsdtype="", const char *xmlns="");

    void serializeContent(StringBuffer &) { assertex(false); }

    void unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath, const char* itemname=NULL)
    { unmarshall(rpc_request, queryFieldName(), basepath, itemname); }
    
    void unserialize(CSoapValue& soapval)
    { unmarshall(soapval, queryFieldName()); }
    
    void unserialize(IProperties& params, MapStrToBuf *attachments, const char *basepath, const char* itemname=NULL)
    { unmarshall(params, attachments, queryFieldName(), basepath, itemname); }

};

//---------------------------------------------------------------------
// array field

class ESPHTTP_API CSoapStringArrayType : public CSoapTypeBase
{
public:
    CSoapStringArrayType() { }

    bool isPrimitiveType()           {  return true; } // the element type
    bool isArrayType()               {  return true;  }
    const char* queryTypeName()      {  return "string"; }
    
    StringBuffer& getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns="xsd", const char *wsns="tns", unsigned flags=1);

    static CSoapStringArrayType* instance() {  static CSoapStringArrayType type; return &type; }

    ISoapField* createField(const char* name);
};

class ESPHTTP_API CSoapStringArrayField : public CSoapFieldBase
{
private:
    StringArray array_;

public:
    CSoapStringArrayField(const char* name)
    : CSoapFieldBase(name, CSoapStringArrayType::instance()) { }

    virtual ~CSoapStringArrayField() { }

    operator StringArray ()    { return array_;  }
    operator StringArray&()    { return array_;  }
    StringArray* operator ->() { return &array_; }

    FieldValue gtValue() { FieldValue fv(&array_); return fv; }
    void stValue(FieldValue fv);

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath,
        const char* itemname, const char *xsdtype="", const char *xmlns="")
    {
        rpc_call.add_value(basepath, xmlns, tagname, xmlns, itemname, xsdtype, array_);
    }
    void marshall(StringBuffer &str, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");

    void unmarshall(CSoapValue &soapval, const char *tagname);
    void unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    void unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");

    void unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath, const char* itemname=NULL) 
    {  unmarshall(rpc_request,tagname,basepath,itemname); }
    virtual void unserialize(CSoapValue& soapval)
    {  unmarshall(soapval,queryFieldName()); }
    void unserialize(IProperties& params, MapStrToBuf *attachments, const char *basepath, const char* itemname=NULL);

    void unmarshallAttach(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""); 
    bool unmarshallRawArray(IProperties &params, const char *tagname, const char *basepath);

    void serializeContent(StringBuffer& buffer)  { assert(false); }
};

//---------------------------------------------------------------------
//  Struct field

class ESPHTTP_API CSoapStructField : public CSoapFieldBase
{
protected:
//  bool m_isNil;
    nilBehavior m_nilBH;
    ISoapField** m_fields;
    size_t      m_fldCount;
    
    void setDerivedType(CSoapStructType* type);
    CSoapStructType* structType()   { return dynamic_cast<CSoapStructType*>(m_type); }

public:
    CSoapStructField(const char* name, CSoapStructType* type);
    virtual ~CSoapStructField();
    ISoapField* queryField(int idx) { return m_fields[idx]; }
    int getFieldCount() { return m_fldCount; }
    const char* queryFieldName(int idx)  { return queryField(idx)->queryFieldName(); }
    const char* queryItemName(int idx);

    FieldValue gtValue() { FieldValue v(this); return v; }
    void  stValue(FieldValue from);

    void serializeContent(StringBuffer& buffer);
    
    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    void marshall(StringBuffer &str, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    
    void unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    void unmarshall(CSoapValue &soapval, const char *tagname);
    void unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath,
        const char* itemname, const char *xsdtype="", const char *xmlns="");
    
    void unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath, const char* itemname=NULL);
    void unserialize(CSoapValue& soapval);
    void unserialize(IProperties& params, MapStrToBuf *attachments, const char *basepath, const char* itemname=NULL);
};


//---------------------------------------------------------------------
//  Struct array 

//TODO: can we set m_baseType to ISoapType, so we can handle any array???
// 
class ESPHTTP_API CSoapStructArrayType : public CSoapTypeBase
{
protected:
    CSoapStructType* m_baseType;

    CSoapStructArrayType(CSoapStructType* base) : m_baseType(base) { }

public:
    bool isArrayType()               {  return true;  }
    const char* queryTypeName()      {  return m_baseType->queryTypeName(); }
    
    StringBuffer& getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns="xsd", const char *wsns="tns", unsigned flags=1);

    static CSoapStructArrayType* instance(CSoapStructType* base);
    CSoapStructType* queryBaseType() {  return m_baseType; }

    // field factory
    ISoapField* createField(const char* name);
};

class ESPHTTP_API CSoapStructArrayField : public CSoapFieldBase
{
private:
    IArrayOf<CSoapStructField> array_;

public:
    CSoapStructArrayField(const char* name, CSoapStructArrayType* type) : CSoapFieldBase(name,type) { }
    virtual ~CSoapStructArrayField() {  }

//  IArrayOf<basetype>  &getValue(){return array_;}
//  operator IArrayOf<basetype>  & () {return  array_;}
//  IArrayOf<basetype>  * operator ->() {return &array_;}

    CSoapStructType* queryBaseType() {  return dynamic_cast<CSoapStructArrayType*>(queryFieldType())->queryBaseType(); }
    FieldValue gtValue() { FieldValue fv(&array_); return fv; }
    void stValue(FieldValue v);

    void copy(CSoapStructArrayField &from);

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                          const char* itemname,const char *xsdtype="",const char *xmlns="");
    void marshall(StringBuffer &xml, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");

    void unmarshall(CSoapValue &soapval, const char *tagname);
    void unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath,
        const char* itemname, const char* xsdtype="", const char *xmlns="");
    void unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, 
        const char* itemname, const char *xsdtype="", const char *xmlns="");

    void unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath, const char* itemname=NULL)
    { unmarshall(rpc_request,tagname,basepath,itemname); }

    void unserialize(CSoapValue& soapval)
    { unmarshall(soapval, queryFieldName()); }

    void unserialize(IProperties& params, MapStrToBuf *attachments, const char *basepath, const char* itemname=NULL)
    { unmarshall(params,attachments,queryFieldName(),basepath,itemname); }

    void serializeContent(StringBuffer& s) { assertex(false); }

protected:
    void unmarshallArrayItem(IProperties &params,MapStrToBuf *attachments,const char* tagname,const char* basepath);
};


//---------------------------------------------------------------------
//  Request

class ESPHTTP_API CSoapRequestField : public CSoapStructField, 
    implements IRpcRequestBinding, implements IEspRequest
{
private:
    StringBuffer url_;
    StringBuffer proxy_;
    StringBuffer userid_;
    StringBuffer password_;
    StringBuffer realm_;
    unsigned clvalue_;
    unsigned msg_id_;
    void *thunk_;
    
    IInterface* m_RequestState;
    StringBuffer m_serviceName;
    StringBuffer m_methodName;
    StringBuffer m_msgName;

    void *m_eventSink;
    long m_reqId;
    Mutex m_mutex;
    
    void init()
    {
        clvalue_=0;
        msg_id_=0;
        thunk_=NULL;
        setMsgName("TestRequest");
    }
    
public:
    IMPLEMENT_IINTERFACE;

    CSoapRequestField(CSoapStructType* type, const char *serviceName, IRpcMessage* rpcmsg) 
        : CSoapStructField("UnnamedRequest", type), m_serviceName(serviceName)
    {
        init();
        unserialize(*rpcmsg,NULL,NULL);
    }
    
    CSoapRequestField(CSoapStructType* type, const char *serviceName, IProperties *params, MapStrToBuf *attachments)
        : CSoapStructField("UnnamedRequest", type)
    {
        init();
        unserialize(*params,attachments, NULL);
    }
    
    const char* getServiceName() { return m_serviceName; }
    void setMsgName(const char *msgname) { m_msgName.clear().append(msgname); }
    const char* queryMsgName() {  return m_msgName; }

    //---------------------------------
    // IRpcRequestBinding
    void serialize(IRpcMessage & rpc_response)
    {
        assert(false);
    }
    
    void serialize(StringBuffer & buffer, const char * rootname)
    {
        assert(false);
    }
    
    
    void setReqId(unsigned val) { m_reqId=val;}
    unsigned getReqId()         { return m_reqId;}
    
    
    void setEventSink(void * val){m_eventSink=val;}
    void * getEventSink(){return m_eventSink;}
    
    void setState(IInterface * val){m_RequestState = val;}  
    IInterface * queryState(){return m_RequestState;}
    
    void setMethod(const char * method){m_methodName.clear().append(method);}   
    const char * getMethod(){return m_methodName.str();}
    
    void lock(){m_mutex.lock();}
    void unlock(){m_mutex.unlock();}
    
    void setUrl(const char *url){url_.clear().append(url);} 
    const char * getUrl(){return url_.str();}
    
    void setProxyAddress(const char *proxy){proxy_.clear().append(proxy);}  
    const char * getProxyAddress(){return proxy_.str();}
    
    void setUserId(const char *userid){userid_.clear().append(userid);} 
    const char * getUserId(){return userid_.str();}
    
    void setPassword(const char *password){password_.clear().append(password);} 
    const char * getPassword(){return password_.str();}
    
    void setRealm(const char *realm){realm_.clear().append(realm);} 
    const char * getRealm(){return realm_.str();}
    
    void setClientValue(unsigned val){clvalue_=val;}
    unsigned getClientValue(){return clvalue_;}
    
    void setThunkHandle(void * val){thunk_=val;}
    void * getThunkHandle(){return thunk_;}
    
    void setMessageId(unsigned val){msg_id_=val;}
    unsigned getMessageId(){return msg_id_;}
    
    void post(const char *proxy, const char* url, IRpcResponseBinding& response);
    
    void post(IRpcResponseBinding& response)
    {
        post(getProxyAddress(), getUrl(), response);
    }
    void serializeContent(StringBuffer& buffer)
    {
        assert(false);
    }
};

//---------------------------------------------------------------------
//  Response

class ESPHTTP_API CSoapResponseField : public CSoapStructField, 
    implements IRpcResponseBinding, implements IEspResponse
{
private:
    RpcMessageState state_;
    unsigned clvalue_;
    unsigned msg_id_;
    void *thunk_;
    StringBuffer redirectUrl_;
    Owned<IMultiException> exceptions_;

    void *m_eventSink;
    long m_reqId;
    Mutex m_mutex;
    IInterface* m_RequestState;
    StringBuffer m_serviceName;
    StringBuffer m_methodName;
    StringBuffer m_msgName;
    
    void doInit()
    {
        state_ = RPC_MESSAGE_OK;
        clvalue_ = 0;
        msg_id_ = 0;
        thunk_ = NULL;
        exceptions_.setown(MakeMultiException("CSoapResponseField"));
    }

public:
    IMPLEMENT_IINTERFACE;
    
    CSoapResponseField(CSoapStructType* type,const char *serviceName, IRpcMessageBinding *init);
    CSoapResponseField(CSoapStructType* type, const char *serviceName, IRpcMessage* rpcmsg);
    CSoapResponseField(CSoapStructType* type, const char *serviceName, IProperties *params, MapStrToBuf *attachments);

    // resolve ambiguous
    void unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath)
    {  CSoapStructField::unserialize(rpc_request,tagname,basepath); }

    void serialize(IRpcMessage& rpc_resp);
    void serialize(MemoryBuffer& buffer, StringBuffer &mimetype);
    void serialize(StringBuffer& buffer, const char *name=NULL);

    void setMsgName(const char *msgname) { m_msgName.clear().append(msgname); }
    const char* queryMsgName() {  return m_msgName; }
    //const char* queryMsgName() {  return queryFieldName(); }

    void setRedirectUrl(const char * url)  { redirectUrl_.clear().append(url);  }

    int queryClientStatus()        {  return getRpcState(); }

    void setReqId(unsigned val) { m_reqId=val;}
    unsigned getReqId()         { return m_reqId;}

    void setEventSink(void * val){m_eventSink=val;}
    void * getEventSink(){return m_eventSink;}

    void setState(IInterface * val){m_RequestState = val;}  
    IInterface * queryState(){return m_RequestState;}

    void setMethod(const char * method){m_methodName.clear().append(method);}   
    const char * getMethod(){return m_methodName.str();}

    void lock(){m_mutex.lock();}
    void unlock(){m_mutex.unlock();}

    void setClientValue(unsigned val) { clvalue_=val;}
    unsigned getClientValue()       { return clvalue_;}

    void setMessageId(unsigned val) { msg_id_=val;}
    unsigned getMessageId()         { return msg_id_;}

    void setThunkHandle(void * val) { thunk_=val; }
    void * getThunkHandle()         { return thunk_; }

    void setRpcState(RpcMessageState state)  { state_=state; }
    RpcMessageState getRpcState()            { return state_;}

    const char *getRedirectUrl()                   { return redirectUrl_.str(); }
    const IMultiException& getExceptions() { return *exceptions_;       }
    void  noteException(IException& e)     { exceptions_->append(e);    }
};


#define IMPLEMENT_ESPRESPONSE \
public: \
    int queryClientStatus()  { return CSoapResponseField::queryClientStatus(); } \
    void setRedirectUrl(const char * url)  { CSoapResponseField::setRedirectUrl(url);  } \
    const IMultiException& getExceptions() { return CSoapResponseField::getExceptions(); } \
    void  noteException(IException& e)     { CSoapResponseField::noteException(e);  } 

//---------------------------------------------------------------------
// ISoap

/*
interface IMyEspService : public IEspService
{
    virtual bool init(const char * service, const char * type, IPropertyTree * cfg, const char * process) = 0;
    virtual bool runQuery(int queryId, IEspContext& ctx, IEspRequest& req, IEspResponse& resp) = 0;
};
*/

#define IMPLEMENT_IESPSERVICE \
protected: \
    IEspContainer* m_container; \
public: \
    void setContainer(IEspContainer *c) { m_container = c; } \
    IEspContainer *queryContainer() { return m_container; }  

/*
// Tricky: this implements IEspService but we don't want to declare it since
// that will cause multiple inheritance to IEspService
class CMyEspService : public CInterface 
{
protected:
    IEspContainer* m_container;

public:
    IMPLEMENT_IINTERFACE;

    CMyEspService() {}
    virtual ~CMyEspService(){}

    //---
    virtual bool runQuery(int queryId, IEspContext& ctx, IEspRequest& req, IEspResponse& resp) = 0;

    //--- IEspService
    bool init(const char * service, const char * type, IPropertyTree * cfg, const char * process)
    {
        return true;
    }
    void setContainer(IEspContainer *c)
    {
        m_container = c;
    }
    IEspContainer *queryContainer()
    {
        return m_container;
    }
};
*/

//---------------------------------------------------------------------
// soap binding

class CEspMethod
{
public:
    int         m_queryId;
    const char* m_name;
    ISoapType*  m_request;
    ISoapType*  m_response;
    

    CEspMethod(int id, const char* name, ISoapType* req, ISoapType* resp)
        : m_queryId(id), m_name(name), m_request(req), m_response(resp)
    {
    }
    
    //TODO:
    // version, attribute, help, description etc
};

class ESPHTTP_API CMySoapBinding : public CHttpSoapBinding
{
protected:
    StringBuffer  m_serviceName;
    
    typedef std::map<std::string, CEspMethod*> MethodMap;
    MethodMap m_methods;

public:
    CMySoapBinding(const char* svcName, http_soap_log_level level=hsl_none);
    CMySoapBinding(const char* svcName, IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL, http_soap_log_level level=hsl_none);

    void addMethod(int id, const char* name, ISoapType* req, ISoapType* resp)
    {
        m_methods.insert(MethodMap::value_type(name, new CEspMethod(id, name, req,resp))); 
    }

    CEspMethod* CMySoapBinding::queryMethod(const char* name);

    virtual int processRequest(IRpcMessage* rpc_call, IRpcMessage* rpc_response);
    virtual int getXsdDefinition(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda);
    virtual int getMethodHtmlForm(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &page, bool bIncludeFormTag);
    virtual int onGetFile(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *pathex);
    virtual int onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
    virtual int onGetService(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method, const char *pathex);
    virtual int onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);

    int getQualifiedNames(IEspContext& ctx, MethodInfoArray & methods);
    StringBuffer & getServiceName(StringBuffer &resp)   { return resp.append(m_serviceName); }
    
    bool isValidServiceName(IEspContext &context, const char *name) { return (Utils::strcasecmp(name, m_serviceName)==0); }

    bool qualifyMethodName(IEspContext &context, const char *methname, StringBuffer *methQName);

    bool qualifyServiceName(IEspContext &context, const char *servname, const char *methname, StringBuffer &servQName, StringBuffer *methQName);
    void setXslProcessor(IInterface *xslp){}
};

#endif
