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

#pragma warning(disable : 4786)

#include "jliball.hpp"
#include "espcontext.hpp"
#include "esdl_transformer.hpp"
#include <xpp/XmlPullParser.h>

using namespace xpp;

class EsdlBase;

typedef EsdlBase * EsdlBasePtr;
typedef IPropertyTree * IPTreePtr;

MAKEPointerArray(EsdlBase, EsdlBaseArray);
typedef MapStringTo<EsdlBasePtr> EsdlBaseMap;
typedef MapStringTo<IPTreePtr> AddedList;

typedef enum
{
    ESDLT_UNKOWN,
    ESDLT_STRUCT,
    ESDLT_REQUEST,
    ESDLT_RESPONSE,
    ESDLT_COMPLEX,
    ESDLT_STRING,
    ESDLT_INT8,
    ESDLT_INT16,
    ESDLT_INT32,
    ESDLT_INT64,
    ESDLT_UINT8,
    ESDLT_UINT16,
    ESDLT_UINT32,
    ESDLT_UINT64,
    ESDLT_BOOL,
    ESDLT_FLOAT,
    ESDLT_DOUBLE,
    ESDLT_BYTE,
    ESDLT_UBYTE
} EsdlType;


MAKESTRINGMAPPING(EsdlType, EsdlType, EsdlTypeList);

static bool type_list_inited = false;
static EsdlTypeList TypeList;

void init_type_list(EsdlTypeList &list)
{
    static CriticalSection crit;
    CriticalBlock block(crit);
    if (!type_list_inited)
    {
        //strings:
        list.setValue("StringBuffer", ESDLT_STRING);
        list.setValue("string", ESDLT_STRING);
        list.setValue("binary", ESDLT_STRING);
        list.setValue("base64Binary", ESDLT_STRING);
        list.setValue("normalizedString", ESDLT_STRING);
        list.setValue("xsdString", ESDLT_STRING);
        list.setValue("xsdBinary", ESDLT_STRING);
        list.setValue("xsdDuration", ESDLT_STRING);
        list.setValue("xsdDateTime", ESDLT_STRING);
        list.setValue("xsdTime", ESDLT_STRING);
        list.setValue("xsdDate", ESDLT_STRING);
        list.setValue("xsdYearMonth", ESDLT_STRING);
        list.setValue("xsdYear", ESDLT_STRING);
        list.setValue("xsdMonthDay", ESDLT_STRING);
        list.setValue("xsdDay", ESDLT_STRING);
        list.setValue("xsdMonth", ESDLT_STRING);
        list.setValue("xsdAnyURI", ESDLT_STRING);
        list.setValue("xsdQName", ESDLT_STRING);
        list.setValue("xsdNOTATION", ESDLT_STRING);
        list.setValue("xsdToken", ESDLT_STRING);
        list.setValue("xsdLanguage", ESDLT_STRING);
        list.setValue("xsdNMTOKEN", ESDLT_STRING);
        list.setValue("xsdNMTOKENS", ESDLT_STRING);
        list.setValue("xsdName", ESDLT_STRING);
        list.setValue("xsdNCName", ESDLT_STRING);
        list.setValue("xsdID", ESDLT_STRING);
        list.setValue("xsdIDREF", ESDLT_STRING);
        list.setValue("xsdIDREFS", ESDLT_STRING);
        list.setValue("xsdENTITY", ESDLT_STRING);
        list.setValue("xsdENTITIES", ESDLT_STRING);
        list.setValue("xsdBase64Binary", ESDLT_STRING);
        list.setValue("xsdNormalizedString", ESDLT_STRING);
        list.setValue("EspTextFile", ESDLT_STRING);
        list.setValue("EspResultSet", ESDLT_STRING);
    //numeric
        list.setValue("bool", ESDLT_BOOL);
        list.setValue("boolean", ESDLT_BOOL);
        list.setValue("decimal", ESDLT_FLOAT);
        list.setValue("float", ESDLT_FLOAT);
        list.setValue("double", ESDLT_DOUBLE);
        list.setValue("integer", ESDLT_INT32);
        list.setValue("int64", ESDLT_INT64);
        list.setValue("long", ESDLT_INT32);
        list.setValue("int", ESDLT_INT32);
        list.setValue("short", ESDLT_INT16);
        list.setValue("nonPositiveInteger", ESDLT_INT32);
        list.setValue("negativeInteger", ESDLT_INT32);
        list.setValue("nonNegativeInteger", ESDLT_UINT32);
        list.setValue("unsignedLong", ESDLT_UINT32);
        list.setValue("unsignedInt", ESDLT_UINT32);
        list.setValue("unsignedShort", ESDLT_UINT16);
        list.setValue("unsignedByte", ESDLT_UBYTE);
        list.setValue("positiveInteger", ESDLT_UINT32);
        list.setValue("xsdBoolean", ESDLT_BOOL);
        list.setValue("xsdDecimal", ESDLT_FLOAT);
        list.setValue("xsdInteger", ESDLT_INT32);
        list.setValue("xsdByte", ESDLT_INT8);
        list.setValue("xsdNonPositiveInteger", ESDLT_INT32);
        list.setValue("xsdNegativeInteger", ESDLT_INT32);
        list.setValue("xsdNonNegativeInteger", ESDLT_UINT32);
        list.setValue("xsdUnsignedLong", ESDLT_UINT32);
        list.setValue("xsdUnsignedInt", ESDLT_UINT32);
        list.setValue("xsdUnsignedShort", ESDLT_UINT16);
        list.setValue("xsdUnsignedByte", ESDLT_UINT8);
        list.setValue("xsdPositiveInteger", ESDLT_UINT64);
        
        type_list_inited=true;
    }
}

EsdlType simpleType(const char *type)
{
    if (!type || !*type)
        return ESDLT_STRING;
    
    EsdlType *val = TypeList.getValue(type);
    if (val)
        return *val;
    return ESDLT_COMPLEX;
}

class EsdlTransformerContext
{
public:
    EsdlBaseMap &type_map;
    StringBuffer &out;
    EsdlProcessMode mode;
    XmlPullParser &xppx;
    double client_ver;
    IProperties* param_groups;
    StringAttr root_type;
    bool skip_root;
    int counter;


public:
    EsdlTransformerContext(EsdlBaseMap &type_map_, StringBuffer &out_, XmlPullParser &xppx_, double client_ver_, IProperties *pgs, EsdlProcessMode mode_) : 
        type_map(type_map_), out(out_), xppx(xppx_), client_ver(client_ver_), mode(mode_), param_groups(pgs), skip_root(false), counter(0)
        {
        }

    virtual ~EsdlTransformerContext(){}
};

class EsdlTransformer;

class EsdlBase : public CInterface
{
public:
    StringAttr name;
    StringAttr xml_tag;
    StringAttr ecl_name;
    StringAttr ecl_null;
    StringAttr param_group;
    StringAttr default_value;

    Owned<IProperties> metatags;

    EsdlBaseArray children;  //elements and arrays
    EsdlBaseMap child_map;
    EsdlType type_id;

    double min_ver;
    double max_ver;
    bool min_inclusive;
    bool max_inclusive;
    bool versioned;
    bool might_skip_root;
    bool count_output;
    bool count_value;

public:
    IMPLEMENT_IINTERFACE;

    EsdlBase(EsdlTransformer *xformer, IPropertyTree *item, EsdlType t=ESDLT_UNKOWN, bool might_skip_root_=false);
    virtual ~EsdlBase();
    void addChildren(EsdlTransformer *xformer, IPropertyTreeIterator *it);
    void mergeBaseType(EsdlTransformer *xformer, const char *base_type);
    
    const char *queryName(){return name.get();}
    const char *queryEclName(){return ecl_name.get();}

    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false)=0;
    virtual void addEsdlType(EsdlTransformer *xformer)=0;
    virtual void serialize(StringBuffer &out)=0;
    virtual void addChildTypes(EsdlTransformer *xformer);

    virtual void buildDefaults(EsdlTransformer *xformer, StringBuffer &path, IProperties *defvals){}


    virtual void serialize(StringBuffer &out, const char *type)
    {
        out.appendf("<%s", type);
        serialize_attributes(out);
        if (children.empty())
            out.append("/>");
        else
        {
            out.append(">");
            serialize_children(out);
            out.appendf("</%s>", type);
        }
    }

    virtual void serialize_children(StringBuffer &out)
    {
        ForEachItemIn(idx, children)
        {
            EsdlBase &child = children.item(idx);
            child.serialize(out);
        }

    }
    
    virtual void serialize_attributes(StringBuffer &out)
    {
        Owned<IPropertyIterator> piter = metatags->getIterator();
        ForEach(*piter)
        {
            const char *key = piter->getPropKey();
            if (key && *key)
            {
                if (stricmp(key, "base_type"))
                {
                    const char *value = metatags->queryProp(key);
                    if (value && *value)
                        out.appendf(" %s=\"%s\"", key, value);
                }
            }
        }

    }

    bool checkVersion(EsdlTransformerContext &ctx)
    {
        if (!param_group.isEmpty())
            if (!ctx.param_groups || !ctx.param_groups->hasProp(param_group.get()))
                return false;
        return (!versioned ||
            (min_ver == 0.0f || (ctx.client_ver > min_ver || (min_inclusive && ctx.client_ver == min_ver)))  &&
            (max_ver == 0.0f || (ctx.client_ver < max_ver || (max_inclusive && ctx.client_ver == max_ver)))
            );
    }

    void count_content(EsdlTransformerContext &ctx)
    {
        StringBuffer content;
        for (int type=ctx.xppx.next(); type!=XmlPullParser::END_TAG; type=ctx.xppx.next())
        {
            switch(type)
            {
                case XmlPullParser::START_TAG:  //shouldn't have nested tags, skip
                {
                    StartTag temp;
                    ctx.xppx.readStartTag(temp);
                    ctx.xppx.skipSubTree();
                    break;
                }
                case XmlPullParser::CONTENT:  //support multiple content items, append
                {
                    encodeUtf8XML(ctx.xppx.readContent(), content);
                    break;
                }
            }
        }
        if (content.trim().length() && (ecl_null.isEmpty() || strcmp(ecl_null.get(), content.str())))
        {
            if (type_id==ESDLT_BOOL)
            {
                if (!stricmp(content.str(), "true")||!strcmp(content.str(), "1"))
                    content.clear().append('1');
                else
                    content.clear().append('0');
            }
            ctx.counter=atoi(content.str());
        }
    }

    void output_content(EsdlTransformerContext &ctx, const char *tagname)
    {
        StringBuffer content;
        for (int type=ctx.xppx.next(); type!=XmlPullParser::END_TAG; type=ctx.xppx.next())
        {
            switch(type)
            {
                case XmlPullParser::START_TAG:  //shouldn't have nested tags, skip
                {
                    StartTag temp;
                    ctx.xppx.readStartTag(temp);
                    ctx.xppx.skipSubTree();
                    break;
                }
                case XmlPullParser::CONTENT:  //support multiple content items, append
                {
                    encodeUtf8XML(ctx.xppx.readContent(), content);
                    break;
                }
            }
        }
        if (content.trim().length() && (ecl_null.isEmpty() || strcmp(ecl_null.get(), content.str())))
        {
            if (type_id==ESDLT_BOOL)
            {
                if (!stricmp(content.str(), "true")||!strcmp(content.str(), "1"))
                    content.clear().append('1');
                else
                    content.clear().append('0');
            }

            ctx.out.appendf("<%s>%s</%s>", tagname, content.str(), tagname);
            if (count_output)
                ctx.counter++;
            if (count_value)
                ctx.counter=atoi(content.str());
        }
    }

    void output_content(EsdlTransformerContext &ctx)
    {
        output_content(ctx, xml_tag.sget());
    }

    void output_ecl_date(EsdlTransformerContext &ctx, const char *tagname)
    {
        int Month=0;
        int Day=0;
        int Year=0;

        StartTag child;
        StringBuffer content;
        for (int type=ctx.xppx.next(); type!=XmlPullParser::END_TAG; type=ctx.xppx.next())
        {
            switch(type)
            {
                case XmlPullParser::START_TAG:  //shouldn't have nested tags, skip
                {
                    ctx.xppx.readStartTag(child);
                    readFullContent(ctx.xppx, content.clear());
                    const char *tag = child.getLocalName();
                    if (!stricmp(tag, "Month"))
                        Month = atoi(content.str());
                    else if (!stricmp(tag, "Day"))
                        Day = atoi(content.str());
                    else if (!stricmp(tag, "Year"))
                        Year = atoi(content.str());
                    break;
                }
            }
        }
        
        StringBuffer date;
        if (Year>1000 && Year<9999)
            date.append(Year);
        else
            date.append("0000");
    
        if (Month<1 || Month>12)
            date.append("00");
        else
        {
            if (Month<10)
                date.append('0');
            date.append(Month);
        }

        if (Day<1 || Day>31)
            date.append("00");
        else
        {
            if (Day<10)
                date.append('0');
            date.append(Day);
        }
            
        
        if (date.length())
        {
            ctx.out.appendf("<%s>%s</%s>", tagname, date.str(), tagname);
            if (count_output)
                ctx.counter++;
        }
    }

};

class EsdlEnumItem : public EsdlBase
{
    StringAttr value;
public:
    EsdlEnumItem(EsdlTransformer *xformer, IPropertyTree *item) : EsdlBase(xformer, item)
    {
        value.set(item->queryProp("@enum"));
    }
    virtual ~EsdlEnumItem(){}
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false){}
    virtual void addEsdlType(EsdlTransformer *xformer){}

    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlEnumItem");
    }

};

class EsdlEnumType : public EsdlBase
{
    StringAttr base_type;

public:
    EsdlEnumType(EsdlTransformer *xformer, IPropertyTree *item) : EsdlBase(xformer, item)
    {
        base_type.set(item->queryProp("@base_type"));
    }
    virtual ~EsdlEnumType(){}
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false){}
    virtual void addEsdlType(EsdlTransformer *xformer){}

    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlEnumType");
    }

};

class EsdlEnumRef : public EsdlBase
{
    StringAttr enum_type;

public:
    EsdlEnumRef(EsdlTransformer *xformer, IPropertyTree *item) : EsdlBase(xformer, item)
    {
        enum_type.set(item->queryProp("@enum_type"));
    }
    
    virtual ~EsdlEnumRef(){}
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false);
    virtual void addEsdlType(EsdlTransformer *xformer){}

    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlEnumRef");
    }
};


class EsdlAttribute : public EsdlBase
{
public:
    EsdlAttribute(EsdlTransformer *xformer, IPropertyTree *item) : EsdlBase(xformer, item){}
    virtual ~EsdlAttribute(){}
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false)
    {
        ctx.xppx.skipSubTree();
    }
    virtual void addEsdlType(EsdlTransformer *xformer){}

    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlAttribute");
    }

};

class EsdlElement : public EsdlBase
{
    StringAttr complex_type;
    StringAttr simple_type;
    EsdlBase *esdl_type;

public:
    EsdlElement(EsdlTransformer *xformer, IPropertyTree *item) : EsdlBase(xformer, item), esdl_type(NULL)
    {
        complex_type.set(item->queryProp("@complex_type"));
        simple_type.set(item->queryProp("@type"));
        if (!simple_type.isEmpty())
        {
            type_id = simpleType(simple_type.get());
            if (type_id==ESDLT_COMPLEX)
                DBGLOG("ESDL simple type not defined: %s", simple_type.get());
        }
        else
            type_id=ESDLT_COMPLEX;
    }
    
    virtual void buildDefaults(EsdlTransformer *xformer, StringBuffer &path, IProperties *defvals);

    virtual ~EsdlElement(){}
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false);
    virtual void addEsdlType(EsdlTransformer *xformer);
    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlElement");
    }

};

class EsdlArray : public EsdlBase
{
    StringAttr type;
    StringAttr item_tag;
    EsdlBase *esdl_type;

    bool inited;
    EsdlType type_id;
    bool type_unknown;
public:
    EsdlArray(EsdlTransformer *xformer, IPropertyTree *item) : EsdlBase(xformer, item), esdl_type(NULL)
    {
        type_unknown=false;
        inited=false;
        item_tag.set(item->queryProp("@item_tag"));
        const char *atype = item->queryProp("@type");
        if (atype)
        {
            type.set(atype);
            type_id = simpleType(atype);
        }
    }
    virtual ~EsdlArray(){}

    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlArray");
    }

    void init(EsdlTransformerContext &ctx)
    {
        if (!inited)
        {
            if (type_id==ESDLT_COMPLEX && !esdl_type)
            {
                EsdlBase **type_entry = ctx.type_map.getValue(type.get());
                if (type_entry)
                    esdl_type = *type_entry;
                else
                    DBGLOG("%s: ESDL Type not defined \"%s\" in \"%s\"", ctx.root_type.get(), type.get(), name.get());
            }
            if (type_id==ESDLT_COMPLEX && !esdl_type)
                type_unknown=true;
            inited=true;
        }
    }
    
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false);
    virtual void addEsdlType(EsdlTransformer *xformer);
};

class EsdlStruct : public EsdlBase
{
public:
    EsdlStruct(EsdlTransformer *xformer, IPropertyTree *item, EsdlType t=ESDLT_STRUCT);
    virtual ~EsdlStruct(){}
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false);
    virtual void addEsdlType(EsdlTransformer *xformer){}

    virtual void buildDefaults(EsdlTransformer *xformer, StringBuffer &path, IProperties *defvals)
    {
        ForEachItemIn(idx, children)
        {
            EsdlBase &child = children.item(idx);
            child.buildDefaults(xformer, path, defvals);
        }
    }

    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlStruct");
    }

};

class EsdlRequest : public EsdlStruct
{
    Owned<IProperties> defvals;

public:
    EsdlRequest(EsdlTransformer *xformer, IPropertyTree *item) : EsdlStruct(xformer, item, ESDLT_REQUEST){}
    virtual ~EsdlRequest(){}
    virtual void addEsdlType(EsdlTransformer *xformer){}
    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlRequest");
    }

    virtual void buildDefaults(EsdlTransformer *xformer)
    {
        defvals.setown(createProperties(false));
        ForEachItemIn(idx, children)
        {
            EsdlBase &child = children.item(idx);
            StringBuffer path;
            child.buildDefaults(xformer, path, defvals.get());
        }
    }

    virtual void addDefaults(IPropertyTree *req)
    {
        Owned<IPropertyIterator> iter = defvals->getIterator();
        ForEach(*iter)
        {
            const char *path = iter->getPropKey();
            if (!req->hasProp(path))
            {
                const char *value = defvals->queryProp(path);
                ensurePTree(req, path);
                req->setProp(path, value);
            }
        }
    }
};

class EsdlResponse: public EsdlStruct
{
public:
    EsdlResponse(EsdlTransformer *xformer, IPropertyTree *item) : EsdlStruct(xformer, item, ESDLT_RESPONSE){}
    virtual ~EsdlResponse(){}
    virtual void addEsdlType(EsdlTransformer *xformer){}
    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlResponse");
    }

};


class EsdlMethod : public EsdlBase, implements IEsdlMethodInfo
{
private:
    StringAttr request_type;
    StringAttr response_type;
public:
    IMPLEMENT_IINTERFACE;

    EsdlMethod(EsdlTransformer *xformer, IPropertyTree *item) : EsdlBase(xformer, item)
    {
        request_type.set(item->queryProp("@request_type"));
        response_type.set(item->queryProp("@response_type"));
    }
    virtual ~EsdlMethod(){}
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false){}
    virtual void addEsdlType(EsdlTransformer *xformer){}
    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlMethod");
    }

    virtual const char *queryMethodName(){return queryName();}
    virtual const char *queryRequestType(){return request_type.get();}
    virtual const char *queryResponseType(){return response_type.get();}
};


typedef EsdlMethod * EsdlMethodPtr;
MAKEPointerArray(EsdlMethod, EsdlMethodArray);
MAKESTRINGMAPPING(EsdlMethodPtr, EsdlMethodPtr, EsdlMethodMap);


class EsdlService : public EsdlBase
{
    EsdlMethodArray methods;  //elements and arrays
    EsdlMethodMap meth_map;
    float default_client_version;

public:
    EsdlService(EsdlTransformer *xformer, IPropertyTree *item) : EsdlBase(xformer, item), default_client_version(1.0f){}
    virtual ~EsdlService(){}
    virtual void process(EsdlTransformerContext &ctx, const char *out_name, bool count=false){}
    virtual void addEsdlType(EsdlTransformer *xformer){}
    void serialize(StringBuffer &out)
    {
        EsdlBase::serialize(out, "EsdlEnumItem");
    }
};




class EsdlTransformer : public CInterface, implements IEsdlTransformer
{
public:
    EsdlBaseArray types;  //elements and arrays
    EsdlBaseMap type_map;
    Owned<IProperties> versions;
    
//  EsdlService *service;
    EsdlMethodArray methods;  //elements and arrays
    EsdlMethodMap meth_map;

    bool specifiedTypesOnly;

    StringArray include_names;
    AddedList included;
    AddedList index;

    void addType(EsdlBase* type)
    {
        types.append(*type); 
        type_map.setValue(type->queryName(), type);
        type->addChildTypes(this);
    }
    public:
        IMPLEMENT_IINTERFACE;

        EsdlTransformer(bool specifiedTypesOnly_) : specifiedTypesOnly(specifiedTypesOnly_)
        {
            if (!type_list_inited)
                init_type_list(TypeList);
            versions.setown(createProperties());
        }

        virtual ~EsdlTransformer();

        void serialize(StringBuffer &out)
        {
            out.append("<esxdl>");
            ForEachItemIn(idx, types)
            {
                EsdlBase &type = types.item(idx);
                type.serialize(out);
            }
            out.append("</esxdl>");
        }

        bool shouldLoadChildren(){return specifiedTypesOnly;}
        void loadFromFile(const char *file, StringArray *types);
        void loadFromFiles(StringArray &files, StringArray *types);

        void loadFromString(const char *xml);
        void load(StringArray *types);
        void ptreeLoadInclude(const char *srcfile);

        void addItem(IPropertyTree *item);
        void addItem(const char *name);

        void addMethod(EsdlBase *item)
        {
            if (item)
            {
                EsdlMethod *method=dynamic_cast<EsdlMethod *>(item);
                if (method)
                {
                    methods.append(*method); 
                    meth_map.setValue(method->queryName(), method);
                }
            }
        }

        virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char *method, IClientWsEclRequest &clReq, IEspStruct& r);
        virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char *method, StringBuffer &xmlout, const char *xmlin);

        virtual IEsdlMethodInfo *queryMethodInfo(const char *method)
        {
            EsdlMethod **mt = meth_map.getValue(method);
            return (mt && *mt) ? dynamic_cast<IEsdlMethodInfo *>(*mt) : NULL;
        }

        double queryVersion(const char *verstr)
        {
            double rv=0.0f;
            if (verstr)
            {
                if (*verstr >= '0' && *verstr <='9')
                    rv=atof(verstr);
                else
                {
                    const char *verdef = versions->queryProp(verstr);
                    if (!verdef)
                        throw MakeStringException(-1, "ESDL Version Definiton not found: %s", verstr);
                    rv=atof(verdef);
                }
            }
            return rv;
        }

};

EsdlBase *createEsdlObject(EsdlTransformer *xformer, IPropertyTree *pt)
{
    const char *type_name=pt->queryName();
    if (stricmp(type_name,"EsdlStruct")==0)
        return new EsdlStruct(xformer, pt);
    else if (stricmp(type_name,"EsdlElement")==0)
        return new EsdlElement(xformer, pt);
    else if (stricmp(type_name,"EsdlArray")==0)
        return new EsdlArray(xformer, pt);
    else if (stricmp(type_name,"EsdlEnumItem")==0)
        return new EsdlEnumItem(xformer, pt);
    else if (stricmp(type_name,"EsdlEnumType")==0)
        return new EsdlEnumType(xformer, pt);
    else if (stricmp(type_name,"EsdlEnum")==0)
        return new EsdlEnumRef(xformer, pt);
    else if (stricmp(type_name,"EsdlAttribute")==0)
        return new EsdlAttribute(xformer, pt);
    else if (stricmp(type_name,"EsdlRequest")==0)
        return new EsdlRequest(xformer, pt);
    else if (stricmp(type_name,"EsdlResponse")==0)
        return new EsdlResponse(xformer, pt);
    else if (stricmp(type_name,"EsdlMethod")==0)
        return new EsdlMethod(xformer, pt);
    else if (stricmp(type_name,"EsdlService")==0)
        return new EsdlService(xformer, pt);
    return NULL;
}

void EsdlBase::addChildren(EsdlTransformer *xformer, IPropertyTreeIterator *it)
{
    ForEach(*it)
    {
        Owned<IPropertyTree> item = &it->get();
        const char *name=item->queryName();
        EsdlBase *obj=createEsdlObject(xformer, item.get());
        if (obj)
        {
            children.append(*obj); 
            child_map.setValue(obj->queryName(), obj);
            if (!obj->ecl_name.isEmpty())
                child_map.setValue(obj->ecl_name.get(), obj);
            if (might_skip_root && !stricmp(obj->name.get(), "response"))
                obj->might_skip_root=true;
        }
    }
}

void EsdlBase::mergeBaseType(EsdlTransformer *xformer, const char *base_type)
{
    if (simpleType(base_type)==ESDLT_COMPLEX)
    {
        IPropertyTree **base_tree = xformer->index.getValue(base_type);
        if (!base_tree || !*base_tree)
        {
            StringBuffer msg;
            msg.appendf("ESDL Initialization Error: BaseType=%s, not found for item %s", base_type, name.get());
            throw MakeStringException(-1, msg.str());
        }

        if ((*base_tree)->hasProp("@base_type"))
            mergeBaseType(xformer, (*base_tree)->queryProp("@base_type"));
        
        //merge base children
        Owned<IPropertyTreeIterator> it = (*base_tree)->getElements("*");
        addChildren(xformer, it.get());
    }
}

EsdlBase::EsdlBase(EsdlTransformer *xformer, IPropertyTree *ptree, EsdlType t, bool skip_response_root_) : 
    min_ver(0.0f), max_ver(0.0f), min_inclusive(true), max_inclusive(true), 
    versioned(false), might_skip_root(skip_response_root_), count_output(false), count_value(false),
    type_id(t)
{
    name.set(ptree->queryProp("@name"));
    if (ptree->hasProp("@default"))
        default_value.set(ptree->queryProp("@default"));
    if (ptree->hasProp("@min_ver"))
    {
        versioned=true;
        min_ver = xformer->queryVersion(ptree->queryProp("@min_ver"));
    }
    if (ptree->hasProp("@max_ver"))
    {
        versioned=true;
        max_ver = xformer->queryVersion(ptree->queryProp("@max_ver"));
    }
    if (ptree->hasProp("@depr_ver"))
    {
        versioned=true;
        max_ver = xformer->queryVersion(ptree->queryProp("@depr_ver"));
        max_inclusive=false;
    }
    if (ptree->hasProp("@optional"))
        param_group.set(ptree->queryProp("@optional"));
    if (ptree->hasProp("@counter"))
        count_output = true;

    const char *count_flag = ptree->queryProp("@count_val");
    if (!stricmp(name.get(), "RecordCount") || count_flag)
    {
        if (count_flag==NULL || *count_flag!='0')
            count_value = true;
    }
    if (ptree->hasProp("@ecl_name"))
        ecl_name.set(ptree->queryProp("@ecl_name"));
    if (ptree->hasProp("@ecl_null"))
        ecl_null.set(ptree->queryProp("@ecl_null"));

    if (ptree->hasProp("@xml_tag"))
        xml_tag.set(ptree->queryProp("@xml_tag"));
    if (xml_tag.isEmpty())
        xml_tag.set(name.get());

    metatags.setown(createProperties());
    Owned<IAttributeIterator> attrs = ptree->getAttributes();
    ForEach(*attrs)
        metatags->setProp(attrs->queryName()+1, attrs->queryValue());

    Owned<IPropertyTreeIterator> it = ptree->getElements("*");
    addChildren(xformer, it.get());
}

EsdlBase::~EsdlBase()
{
    ForEachItemIn(idx, children)
    {
        EsdlBase *child = &children.item(idx);
        child->Release();
    }
}


void EsdlBase::addChildTypes(EsdlTransformer *xformer)
{
    if (xformer->shouldLoadChildren())
    {
        ForEachItemIn(idx, children)
            children.item(idx).addEsdlType(xformer);
    }
}

    
EsdlStruct::EsdlStruct(EsdlTransformer *xformer, IPropertyTree *item, EsdlType t) : EsdlBase(xformer, item, t, t==ESDLT_RESPONSE)
{
    if (item->hasProp("@base_type"))
        mergeBaseType(xformer, item->queryProp("@base_type"));
}

void EsdlElement::buildDefaults(EsdlTransformer *xformer, StringBuffer &path, IProperties *defvals)
{
    if (type_id==ESDLT_COMPLEX)
    {
        int curlen = path.length();
        if (path.length())
            path.append("/");
        path.append(name.sget());
        if (!esdl_type)
        {
            EsdlBase **type_entry = xformer->type_map.getValue(complex_type.get());
            if (type_entry)
                esdl_type = *type_entry;
            else
                DBGLOG("ESDL Type not defined \"%s\" in \"%s\"", complex_type.get(), name.get());
        }
        if (esdl_type)
            esdl_type->buildDefaults(xformer, path, defvals);
        path.setLength(curlen);
    }
    else if (!default_value.isEmpty())
    {
        int curlen = path.length();
        if (path.length())
            path.append("/");
        path.append(name.sget());
        defvals->setProp(path.str(), default_value.sget());
        path.setLength(curlen);
    }

}


void EsdlElement::addEsdlType(EsdlTransformer *xformer)
{
    if (!complex_type.isEmpty())
        xformer->addItem(complex_type.get());
}

void EsdlArray::addEsdlType(EsdlTransformer *xformer)
{
    if (!type.isEmpty() && type_id==ESDLT_COMPLEX)
        xformer->addItem(type.get());
}

void EsdlEnumRef::process(EsdlTransformerContext &ctx, const char *out_name, bool count)
{
    if (!checkVersion(ctx))
        ctx.xppx.skipSubTree();
    else
        output_content(ctx);
}


void EsdlArray::process(EsdlTransformerContext &ctx, const char *out_name, bool count)
{
    if (!inited)
        init(ctx);

    if (type_unknown)
    {
        ctx.xppx.skipSubTree();
        DBGLOG("EsdlArray type (%s) unknown", type.get());
    }
    else if (!checkVersion(ctx))
        ctx.xppx.skipSubTree();
    else
    {
        ctx.out.appendf("<%s>", xml_tag.get());
        int curlen = ctx.out.length();
        for (int type=ctx.xppx.next(); type!=XmlPullParser::END_TAG; type=ctx.xppx.next())
        {
            if (type == XmlPullParser::START_TAG)
            {
                StartTag child_start;
                ctx.xppx.readStartTag(child_start);
                if (!stricmp(child_start.getLocalName(), item_tag.get()))
                {
                    if (esdl_type)  
                        esdl_type->process(ctx, item_tag.get(), count_output);
                    else
                        output_content(ctx, item_tag.get());
                }
                else
                    ctx.xppx.skipSubTree();
            }
        }
        if (ctx.out.length()==curlen) //nothing was added... empty content remove opening tag
            ctx.out.setLength(ctx.out.length()-xml_tag.length()-2); //rewind
        else
        {
            ctx.out.appendf("</%s>", xml_tag.get());
            if (count)
                ctx.counter++;
        }
    }
}


void EsdlStruct::process(EsdlTransformerContext &ctx, const char *out_name, bool count)
{
    if (!checkVersion(ctx))
        ctx.xppx.skipSubTree();
    else
    {
        if (out_name && *out_name)
            if (!might_skip_root || !ctx.skip_root)
                ctx.out.appendf("<%s>", out_name);
        int curlen = ctx.out.length();
        for (int type=ctx.xppx.next(); type!=XmlPullParser::END_TAG; type=ctx.xppx.next())
        {
            switch(type)
            {
                case XmlPullParser::START_TAG:
                {
                    StartTag child_start;
                    ctx.xppx.readStartTag(child_start);
                    EsdlBase **child = child_map.getValue(child_start.getLocalName());
                    if (!(child && *child) && type_id==ESDLT_RESPONSE && !stricmp("Row", child_start.getLocalName()))
                        child = child_map.getValue("response");
                    if (child && *child)
                        (*child)->process(ctx, NULL);
                    else
                        ctx.xppx.skipSubTree();

                    break;
                }
            }
        }
        if (count && ctx.out.length()>curlen)
            ctx.counter++;

        if (out_name && *out_name)
            if (!ctx.skip_root || !might_skip_root)
            {
                if (ctx.out.length()==curlen) //nothing was added, empty content, remove open tag
                    ctx.out.setLength(ctx.out.length()-strlen(out_name)-2); //rewind
                else
                    ctx.out.appendf("</%s>", out_name);
            }
    }
}

void EsdlElement::process(EsdlTransformerContext &ctx, const char *out_name, bool count)
{
    if (!checkVersion(ctx))
    {
        if (count_value)
            count_content(ctx);
        else
            ctx.xppx.skipSubTree();
    }
    else if (complex_type.isEmpty())
    {
        output_content(ctx);
    }
    else
    {
        if (!esdl_type)
        {
            EsdlBase **type_entry = ctx.type_map.getValue(complex_type.get());
            if (type_entry)
                esdl_type = *type_entry;
            else
                DBGLOG("%s: ESDL Type not defined \"%s\" in \"%s\"", ctx.root_type.get(), complex_type.get(), name.get());
        }
        if (esdl_type)
        {
            esdl_type->process(ctx, (ctx.skip_root && might_skip_root) ? NULL : xml_tag.get(), count_output);
        }
        else
            ctx.xppx.skipSubTree();
    }
}


bool gotoStartTag(XmlPullParser &xppx, const char *name, const char *dsname)
{
    int level = 1;
    int type = XmlPullParser::END_TAG;
    StartTag stag;
    while(level > 0) 
    {
        type = xppx.next();
        switch(type) 
        {
            case XmlPullParser::START_TAG:
            {
                xppx.readStartTag(stag);
                ++level;
                const char *tag = stag.getLocalName();
                if (!stricmp(name, tag))
                    return true;
                else if (dsname && *dsname && !stricmp(tag, "Dataset"))
                {
                    const char *nametag=stag.getValue("name");
                    if (nametag && *nametag && !stricmp(nametag, dsname))
                        return true;
                }
                break;
            }
            case XmlPullParser::END_TAG:
                --level;
            break;
        }
    }

    return false;
}

EsdlTransformer::~EsdlTransformer()
{
    ForEachItemIn(idx, types)
    {
        EsdlBase *type = &types.item(idx);
        type->Release();
    }

    ForEachItemIn(mdx, methods)
    {
        EsdlMethod *meth = &methods.item(mdx);
        meth->Release();
    }

}

int EsdlTransformer::process(IEspContext &ctx, EsdlProcessMode mode, const char *method, StringBuffer &out, const char *in)
{
    int rc = 0;
    IEsdlMethodInfo *mi = queryMethodInfo(method);
    if (!mi)
        throw MakeStringException(-1, "Error processing ESDL - method '%s'not found", method);

    const char *root_type=NULL;
    const char *root_name=NULL;
    if (mode==EsdlRequestMode)
    {
        root_name="row";
        root_type=mi->queryRequestType();
    }
    else if (mode==EsdlResponseMode)
    {
        root_type=mi->queryResponseType();
    }

    if (!root_type)
        throw MakeStringException(-1, "Error processing ESDL - starting type not defined");

    EsdlBase **root = type_map.getValue(root_type);
    if (!root || !*root)
        throw MakeStringException(-1, "Error processing ESDL - root type '%s' not found", root_type);

    IProperties *param_groups = ctx.queryRequestParameters();

    try
    {
        XmlPullParser xppx(in, strlen(in)+1);
        if (gotoStartTag(xppx, (*root)->name.get(), "Results"))
        {
            EsdlTransformerContext tctx(type_map, out, xppx, ctx.getClientVersion(), param_groups, mode);
            tctx.skip_root=true;
            tctx.root_type.set((*root)->name.get());
            (*root)->process(tctx, (root_name) ? root_name : root_type);
            rc = tctx.counter;
            if (mode==EsdlRequestMode)
            {
                EsdlRequest *rootreq = dynamic_cast<EsdlRequest *>(*root);
                if (rootreq)
                {
                    Owned<IPropertyTree> req=createPTreeFromXMLString(out.str());
                    rootreq->addDefaults(req);
                    toXML(req.get(), out.clear());
                }
            }
        }
    }
    catch (XmlPullParserException &xpexp)
    {
        StringBuffer text;
        text.appendf("EsdlTransformer XPP exception: %s", xpexp.what());
        DBGLOG("%s", text.str());
        throw MakeStringException(5001, text.str());
    }
    catch (...)
    {
        throw MakeStringException(5001, "EsdlTransformer error parsing xml");
    }

    return rc;
};

int EsdlTransformer::process(IEspContext &ctx, EsdlProcessMode mode, const char *method, IClientWsEclRequest &clReq, IEspStruct& r)
{
    IEsdlMethodInfo *mi = queryMethodInfo(method);
    if (!mi)
        throw MakeStringException(-1, "Error processing ESDL - method '%s'not found", method);

    const char *root_type=NULL;
    if (mode==EsdlRequestMode)
        root_type=mi->queryRequestType();
    else if (mode==EsdlResponseMode)
        root_type=mi->queryResponseType();

    if (!root_type)
        throw MakeStringException(-1, "Error processing ESDL - starting type not defined for method '%s'", method);
    
    IRpcSerializable &rpc = dynamic_cast<IRpcSerializable&>(r);

    StringBuffer reqstr;
    rpc.serialize(&ctx, reqstr, root_type);

    StringBuffer out;
    int rc = process(ctx, mode, method, out, reqstr.str());
    clReq.addDataset(root_type, out.str());
    return rc;
}

void EsdlTransformer::addItem(IPropertyTree *item)
{
    const char *name = item->queryProp("@name");
    const char *elem_type=item->queryName();
    if (elem_type && *elem_type && !type_map.getValue(name))
    {
        if (stricmp(elem_type,"EsdlStruct")==0)
            addType(new EsdlStruct(this, item));
        else if (stricmp(elem_type,"EsdlEnumType")==0)
            addType(new EsdlEnumType(this, item));
        else if (stricmp(elem_type,"EsdlResponse")==0)
            addType(new EsdlResponse(this, item));
        else if (stricmp(elem_type,"EsdlRequest")==0)
        {
            EsdlRequest *req = new EsdlRequest(this, item);
            addType(req);
            req->buildDefaults(this);
        }
    }
}

void EsdlTransformer::addItem(const char *name)
{
    if (name && *name && !type_map.getValue(name))
    {
        IPropertyTree **item = index.getValue(name);
        if (item && *item)
            addItem(*item);
    }
}


void EsdlTransformer::load(StringArray *types)
{
    if (types)
    {
        ForEachItemIn(idx, *types)
        {
            IPropertyTree **item = index.getValue(types->item(idx));
            if (!item || !*item)
            {
                StringBuffer msg;
                msg.appendf("ESDL type not found: %s", types->item(idx));
                throw MakeStringException(-1, msg.str());
            }

            addItem(*item);
        }
    }
    else
    {
    }

    ForEachItemIn(idx, include_names)
    {
        const char *iname = include_names.item(idx);
        IPropertyTree **ppt = included.getValue(iname);
        if (ppt && *ppt)
        {
            Owned<IPropertyTreeIterator> methods = (*ppt)->getElements("EsdlService/EsdlMethod");
            ForEach(*methods)
            {
                IPropertyTree &method = methods->get();
                addMethod(createEsdlObject(this, &method));
            }
        }
    }

    ForEachItemIn(cdx, include_names)
    {
        const char *iname = include_names.item(cdx);
        IPropertyTree **ppt = included.getValue(iname);
        if (ppt && *ppt)
            (*ppt)->Release();
    }

}


void EsdlTransformer::ptreeLoadInclude(const char *srcfile)
{
    if (!srcfile || !*srcfile)
            throw MakeStringException(-1, "EsdlInclude no file name");
    if (!included.getValue(srcfile))
    {
        DBGLOG("ESDL Loading include: %s", srcfile);

        StringBuffer FileName("esdl_files/");
        FileName.append(srcfile).append(".xml");

        IPropertyTree *src = createPTreeFromXMLFile(FileName.str());
        if (!src)
        {
            StringBuffer msg("EsdlInclude file not found - ");
            msg.append(FileName);
            throw MakeStringException(-1, msg.str());
        }

        included.setValue(srcfile, src);
        include_names.append(srcfile);

        StringArray add_includes;
        {
            Owned<IPropertyTreeIterator> iter = src->getElements("*");
            ForEach (*iter)
            {
                IPropertyTree &e = iter->query();
                const char *tag = e.queryName();
                if (!stricmp(tag, "EsdlInclude"))
                    add_includes.append(e.queryProp("@file"));
                else if (!stricmp(tag, "EsdlVersion"))
                    versions->setProp(e.queryProp("@name"), e.queryProp("@version"));
                else
                {
                    const char *name = e.queryProp("@name");
                    if (name && *name)
                        index.setValue(name, &e);
                }
            }
        }

        ForEachItemIn(idx, add_includes)
        {
            const char *file=add_includes.item(idx);
            ptreeLoadInclude(file);
        }
    }
}

void EsdlTransformer::loadFromFile(const char *file, StringArray *types)
{
    ptreeLoadInclude(file);
    load(types);
}

void EsdlTransformer::loadFromFiles(StringArray &files, StringArray *types)
{
    Owned<IPropertyTree> esdlxml = createPTree();
    ForEachItemIn(idx, files)
        ptreeLoadInclude(files.item(idx));
    load(types);
}

void EsdlTransformer::loadFromString(const char *xml)
{
    DBGLOG("ESDL Loading from xml string");

    IPropertyTree *src = createPTreeFromXMLString(xml);

    if (!src)
        throw MakeStringException(-1, "ESDL Initialization Error loading xml string: %s", xml);

    included.setValue("xmlstring", src);

    StringArray add_includes;

    Owned<IPropertyTreeIterator> iter = src->getElements("*");
    ForEach (*iter)
    {
        IPropertyTree &e = iter->query();
        const char *tag = e.queryName();
        if (!stricmp(tag, "EsdlInclude"))
            add_includes.append(e.queryProp("@file"));
        else if (!stricmp(tag, "EsdlVersion"))
            versions->setProp(e.queryProp("@name"), e.queryProp("@version"));
        else
        {
            const char *name = e.queryProp("@name");
            if (name && *name)
                index.setValue(name, &e);
        }
    }

    ForEachItemIn(idx, add_includes)
    {
        const char *file=add_includes.item(idx);
        ptreeLoadInclude(file);
    }

    load(NULL);
}

IEsdlTransformer *createEsdlXFormerFromXMLString(const char *xml)
{
    EsdlTransformer *etx = new EsdlTransformer(false);
    etx->loadFromString(xml);
    return etx;
}

IEsdlTransformer *createEsdlXFormerFromXMLFile(const char *file)
{
    EsdlTransformer *etx = new EsdlTransformer(false);
    etx->loadFromFile(file, NULL);
    return etx;
}

IEsdlTransformer *createEsdlXFormerFromXMLFile(const char *file, StringArray &types)
{
    EsdlTransformer *etx = new EsdlTransformer(true);
    etx->loadFromFile(file, &types);
    //StringBuffer out;
    //etx->serialize(out);
    //DBGLOG("%s", out.str());
    return etx;
}

IEsdlTransformer *createEsdlXFormerFromXMLFiles(StringArray &files, StringArray &types)
{
    EsdlTransformer *etx = new EsdlTransformer(true);
    etx->loadFromFiles(files, &types);
    //StringBuffer out;
    //etx->serialize(out);
    //DBGLOG("serialized esdl xformer: %s", out.str());
    return etx;
}

