/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#ifndef __ESDL_TRANSFORMER2_IPP__
#define __ESDL_TRANSFORMER2_IPP__

#pragma warning(disable : 4786)

#include "jliball.hpp"
#include "espcontext.hpp"
#include "esdl_transformer.hpp"
#include <xpp/XmlPullParser.h>
#include <map>
#include "esp.hpp"
#include "soapesp.hpp"
#include "ws_ecl_client.hpp"
#include "esdl_def.hpp"
#include "eclhelper.hpp"

class Esdl2Base;

typedef Esdl2Base * EsdlBasePtr;
typedef IPropertyTree * IPTreePtr;

typedef CopyReferenceArrayOf<Esdl2Base> EsdlBaseArray;
typedef MapStringTo<EsdlBasePtr> EsdlBaseMap;

typedef EsdlBaseArray * EsdlBaseArrayPtr;
typedef MapStringTo<EsdlBaseArrayPtr> EsdlBaseArrayMap;
typedef MapStringTo<IPTreePtr> AddedList;

// ======================================================================================

// ======================================================================================
// Context for ESDL transformer

class Esdl2Transformer;

class Esdl2TransformerContext
{
public:
    Esdl2Transformer& xformer;
    IXmlWriterExt *writer;
    EsdlProcessMode mode;
    unsigned int flags;
    XmlPullParser *xppp;
    double client_ver;
    IProperties* param_groups;
    StringAttr root_type;
    bool skip_root;
    int counter;
    StringBuffer ns;
    StringBuffer schemaLocation;
    bool do_output_ns;

public:


    Esdl2TransformerContext(Esdl2Transformer& xformer_, IXmlWriterExt *writer_ , double client_ver_, IProperties *pgs, EsdlProcessMode mode_, unsigned int flags_=0, const char *nsp=NULL, const char *schema_location=NULL)
        : xformer(xformer_), xppp(NULL), client_ver(client_ver_), mode(mode_), param_groups(pgs), skip_root(false), counter(0), flags(flags_), ns(nsp),schemaLocation(schema_location)
    {
        writer = writer_;
        do_output_ns = (nsp && *nsp) ? true : false;
    }

    Esdl2TransformerContext(Esdl2Transformer& xformer_, IXmlWriterExt *writer_ , XmlPullParser &xppx_, double client_ver_, IProperties *pgs, EsdlProcessMode mode_, unsigned int flags_=0, const char *nsp=NULL, const char *schema_location=NULL)
        : xformer(xformer_), xppp(&xppx_), client_ver(client_ver_), mode(mode_), param_groups(pgs), skip_root(false), counter(0), flags(flags_), ns(nsp), schemaLocation(schema_location)
    {
            writer = writer_;
            do_output_ns = (nsp && *nsp) ? true : false;
    }

    virtual ~Esdl2TransformerContext(){}

    Esdl2Base* queryType(const char* name);
};

// ======================================================================================
//  class Esdl2LocalContext

class Esdl2LocalContext
{
    typedef StringAttrMapping StringMap;
    StringMap   *m_dataFor;
    StringArray *m_dataOrig;

public:
    Esdl2LocalContext() : dataForProcessed(false), m_dataFor(NULL), m_dataOrig(NULL),m_startTag(NULL)  { }
    ~Esdl2LocalContext() { delete m_dataFor; delete m_dataOrig; }

    bool dataForProcessed;
    StartTag* m_startTag;

    void setDataFor(const char* name, const char* val)
    {
        if (!m_dataFor)
            m_dataFor = new StringMap();
        m_dataFor->setValue(name,val);
    }
    void setDataOrig(const char* name)
    {
        if (!m_dataOrig)
            m_dataOrig = new StringArray();
        m_dataOrig->append(name);
    }

    //void handleDataFor(StringBuffer& out);
    void handleDataFor(IXmlWriterExt & writer);
};

// ======================================================================================
// class Esdl2Base

class Esdl2Base : public CInterface
{
protected:
    IEsdlDefObject* m_def;
    EsdlBasicElementType type_id;

    StringAttr xml_tag;
    StringAttr param_group;

    Esdl2Base* data_for; // only support one for now.
    bool m_hasDataFrom;

    bool might_skip_root;
    bool count_output;
    bool count_value;

public:
    IMPLEMENT_IINTERFACE;

    Esdl2Base(Esdl2Transformer *xformer, IEsdlDefObject* def, EsdlBasicElementType t=ESDLT_UNKOWN, bool might_skip_root_=false);
    virtual ~Esdl2Base();

    virtual bool hasChild() { return false; }
    virtual bool hasDefaults() { return false; }
    virtual Esdl2Base* queryChild(const char* name, bool nocase=false);
    virtual void addChildren(Esdl2Transformer *xformer, IEsdlDefObjectIterator *it);
    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false)=0;
    virtual void process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false){}
    virtual void processElement(Esdl2TransformerContext &ctx)
    {
        throw MakeStringException(-1, "ESDL Error: processElement not implemented for %s", queryName());
    }

    virtual void serialize(StringBuffer &out)=0;
    virtual void buildDefaults(Esdl2Transformer *xformer, StringBuffer &path, IProperties *defvals){}
    virtual void serialize(StringBuffer &out, const char *type);
    virtual void serialize_children(StringBuffer &out) { }
    void serialize_attributes(StringBuffer &out);

    void setMightSkipRoot(bool skip) { might_skip_root = skip; }
    IEsdlDefObject* queryEsdlDefObject() { return m_def; }
    void mergeBaseType(Esdl2Transformer *xformer, const char *base_type);

    const char *queryName(){ return m_def->queryName();}
    const char* queryDefaultValue() { return m_def->queryProp("default"); }
    // return "" for NULL
    const char* queryDefaultValueS() { const char* s = m_def->queryProp("default"); return s?s:""; }

    const char *queryEclName() { return m_def->queryProp("ecl_name"); }
    const char* queryEclNull() { return m_def->queryProp("ecl_null"); }
    const char* queryInputName(Esdl2TransformerContext &ctx){if (ctx.flags & ESDL_TRANS_INPUT_XMLTAG) return xml_tag.get(); else return queryName();}
    const char* queryOutputName(Esdl2TransformerContext &ctx){if (ctx.flags & ESDL_TRANS_OUTPUT_XMLTAG) return xml_tag.get(); else return queryName();}
    Esdl2Base*   queryDataFor() { return data_for; }
    void setDataFor(Esdl2Base* node)
    {
        if (data_for)
            throw MakeStringException(-1, "Feature not supported: data-for only for single field. Field: %s", queryName());
        data_for = node;
    }
    bool hasDataFrom() { return m_hasDataFrom; }
    void setHasDataFrom() { m_hasDataFrom = true; }

    // return true if it passed version check
    bool checkVersion(Esdl2TransformerContext &ctx);

    void countContent(Esdl2TransformerContext &ctx);

    void output_content(Esdl2TransformerContext &ctx, const char *tagname);
    void output_content(Esdl2TransformerContext &ctx);
    void output_content(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *tagname);
    void output_content(Esdl2TransformerContext &ctx, IPropertyTree *pt);
    void output_content(Esdl2TransformerContext &ctx, const char * content, const char *tagname, unsigned leadinzeros);


    void output_ecl_date(Esdl2TransformerContext &ctx, const char *tagname);
};

Esdl2Base *createEsdlObject(Esdl2Transformer *xformer, IEsdlDefObject *def);


// ======================================================================================
// class Esdl2EnumItem

class Esdl2EnumItem : public Esdl2Base
{
    StringAttr value;
public:
    Esdl2EnumItem(Esdl2Transformer *xformer, IEsdlDefObject *def) : Esdl2Base(xformer, def)
    {
        value.set(def->queryProp("enum"));
    }
    virtual ~Esdl2EnumItem(){}
    virtual void process(Esdl2TransformerContext &ctx, const char *out_name,Esdl2LocalContext* local=NULL, bool count=false){}

    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlEnumItem");
    }

};

class Esdl2EnumType : public Esdl2Base
{
    StringAttr base_type;

public:
    Esdl2EnumType(Esdl2Transformer *xformer, IEsdlDefObject *def) : Esdl2Base(xformer, def)
    {
        base_type.set(def->queryProp("base_type"));
    }
    virtual ~Esdl2EnumType(){}
    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false){}

    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlEnumType");
    }

};

class Esdl2EnumRef : public Esdl2Base
{
    StringAttr enum_type;

public:
    Esdl2EnumRef(Esdl2Transformer *xformer, IEsdlDefObject *def) : Esdl2Base(xformer, def)
    {
        enum_type.set(def->queryProp("enum_type"));
    }

    virtual ~Esdl2EnumRef(){}
    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);
    virtual void process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);

    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlEnumRef");
    }
};


class Esdl2Attribute : public Esdl2Base
{
public:
    Esdl2Attribute(Esdl2Transformer *xformer, IEsdlDefObject *def) : Esdl2Base(xformer, def){}
    virtual ~Esdl2Attribute(){}

    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false)
    {
        ctx.xppp->skipSubTree();
    }
    virtual void process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);

    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlAttribute");
    }
};

class Esdl2Element : public Esdl2Base
{
    StringAttr complex_type;
    StringAttr simple_type;
    Esdl2Base *esdl_type;

public:
    Esdl2Element(Esdl2Transformer *xformer, IEsdlDefObject *def);

    virtual void buildDefaults(Esdl2Transformer *xformer, StringBuffer &path, IProperties *defvals);

    virtual ~Esdl2Element(){}
    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);
    virtual void process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);
    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlElement");
    }
};

class Esdl2Array : public Esdl2Base
{
    StringAttr type;
    StringAttr item_tag;
    Esdl2Base *esdl_type;

    bool inited;
    bool type_unknown;
public:
    Esdl2Array(Esdl2Transformer *xformer, IEsdlDefObject *def);
    virtual ~Esdl2Array(){}

    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlArray");
    }

    void init(Esdl2TransformerContext &ctx);

    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);
    virtual void process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);
};

class Esdl2Struct : public Esdl2Base
{
protected:
    EsdlBaseArray m_children;  //elements and arrays
    EsdlBaseArrayMap xml_tags;
    bool isElement;

private:
    EsdlBaseMap   m_child_map;

public:
    Esdl2Struct(Esdl2Transformer *xformer, IEsdlDefStruct *def, EsdlBasicElementType t=ESDLT_STRUCT);
    virtual ~Esdl2Struct();

    virtual bool hasChild() { return !m_children.empty(); }
    virtual void serialize_children(StringBuffer &out)
    {
        ForEachItemIn(idx, m_children)
        {
            Esdl2Base &child = m_children.item(idx);
            child.serialize(out);
        }

    }

    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);
    virtual void process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);
    virtual void processElement(Esdl2TransformerContext &ctx);

    virtual void buildDefaults(Esdl2Transformer *xformer, StringBuffer &path, IProperties *defvals);

    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "Esdl2Struct");
    }

    virtual void addChildren(Esdl2Transformer *xformer, IEsdlDefObjectIterator *it);
    Esdl2Base*  queryChild(const char* name, bool nocase=false);
};

class Esdl2Request : public Esdl2Struct
{
    Owned<IProperties> defvals;

public:
    Esdl2Request(Esdl2Transformer *xformer, IEsdlDefStruct *def) : Esdl2Struct(xformer, def, ESDLT_REQUEST)
    {
        //might_skip_root = true; --> this is not true for Engine 1
        buildDefaults(xformer);
    }

    virtual ~Esdl2Request(){}

    void process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local, bool count);

    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlRequest");
    }

    virtual void buildDefaults(Esdl2Transformer *xformer);

    virtual void addDefaults(IPropertyTree *req);
};

class Esdl2Response: public Esdl2Struct
{
public:
    Esdl2Response(Esdl2Transformer *xformer, IEsdlDefStruct *def) : Esdl2Struct(xformer, def, ESDLT_RESPONSE)
    {
        might_skip_root=true;
    }

    virtual ~Esdl2Response(){}
    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlResponse");
    }

    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false);
    void processChildNamedResponse(Esdl2TransformerContext &ctx, const char *out_name);
};

class Esdl2Method : public Esdl2Base, implements IEsdlMethodInfo
{
private:
    IEsdlDefMethod* m_methodDef;

public:
    IMPLEMENT_IINTERFACE;

    Esdl2Method(Esdl2Transformer *xformer, IEsdlDefMethod *def)
    : Esdl2Base(xformer, def), m_methodDef(dynamic_cast<IEsdlDefMethod*>(def))
    {
    }

    virtual ~Esdl2Method(){}

    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false){}
    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlMethod");
    }

    virtual const char *queryMethodName() { return queryName();}
    virtual const char *queryRequestType() { return m_methodDef->queryRequestType(); }
    virtual const char *queryResponseType() { return m_methodDef->queryResponseType(); }
};


typedef Esdl2Method * EsdlMethodPtr;
typedef CopyReferenceArrayOf<Esdl2Method> EsdlMethodArray;
typedef MapStringTo<EsdlMethodPtr> EsdlMethodMap;


class Esdl2Service : public Esdl2Base
{
    EsdlMethodArray methods;  //elements and arrays
    EsdlMethodMap meth_map;
    float default_client_version;
public:

    Esdl2Service(Esdl2Transformer *xformer, IEsdlDefObject *def) : Esdl2Base(xformer, def), default_client_version(1.0f){}
    virtual ~Esdl2Service(){}

    virtual void process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local=NULL,bool count=false){}
    void serialize(StringBuffer &out)
    {
        Esdl2Base::serialize(out, "EsdlEnumItem");
    }
};

class Esdl2Transformer : public CInterface, implements IEsdlTransformer
{
private:
    Owned<IEsdlDefinition> m_def;

    EsdlBaseArray types;  //elements and arrays
    EsdlBaseMap type_map;

    EsdlMethodArray methods;  //elements and arrays
    EsdlMethodMap meth_map;

    void addType(Esdl2Base* type);
    void addMethod(Esdl2Base *item);

public:
    IMPLEMENT_IINTERFACE;

    Esdl2Transformer(IEsdlDefinition* def);

    virtual ~Esdl2Transformer();

    Esdl2Base* queryType(const char* name);
    IEsdlMethodInfo *queryMethodInfo(const char* service,const char *method);

    void serialize(StringBuffer &out);

    //NOT BEING USED ANYWHERE
    //virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char* service, const char *method, IClientWsEclRequest &clReq, IEspStruct& r, REQUEST_HOOK* reqHook);

    virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char* service, const char *method, StringBuffer &xmlout, const char *xmlin, unsigned int flags = 0, const char *ns=NULL, const char *schema_location=NULL);
    virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char* service, const char *method, IPropertyTree &in, IXmlWriterExt * writer, unsigned int flags, const char *ns);
    virtual int processElement(IEspContext &ctx, const char* service, const char *parentStructName, IXmlWriterExt * writer, const char *in);
    virtual void processHPCCResult(IEspContext &ctx, IEsdlDefMethod &mthdef, const char *xml, IXmlWriterExt * writer, StringBuffer &logdata, unsigned int flags = 0, const char *ns=NULL, const char *schema_location=NULL);
};

#endif
