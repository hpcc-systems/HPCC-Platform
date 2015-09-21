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

#pragma warning(disable : 4786)

#include "esdl_transformer2.ipp"
#include "xpp/xpputils.h"
#include <memory>

#include "eclhelper.hpp"    //IXMLWriter
#include "thorxmlwrite.hpp" //JSON WRITER

using namespace std;

// Uncomment this to debug ESDL issues
//#define DEBUG_ESDL

#ifdef _WIN32

#ifdef DEBUG_ESDL
#define ESDL_DBG DBGLOG
#elif _MSC_VER>1200
#define ESDL_DBG __noop
#else

#if _MSC_VER<=1300
#define ESDL_DBG (void)0
#else
#define ESDL_DBG __noop
#endif

#endif

#else

#ifdef DEBUG_ESDL
#define ESDL_DBG(format,...) DBGLOG(format,##__VA_ARGS__)
#else
#define ESDL_DBG(format,...)
#endif

#endif

using namespace xpp;

// ======================================================================================
// primitive type lookup

static bool gotoStartTag(XmlPullParser &xppx, const char *name, const char *dsname)
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
                if (name && !stricmp(name, tag))
                    return true;
                else if (dsname && *dsname && !stricmp(tag, "Dataset"))
                {
                    const char *nametag=stag.getValue("name");
                    if (nametag && *nametag)
                    {
                        if (!stricmp(nametag, dsname))
                            return true;
                        else if (name)
                        {
                            if (!stricmp(nametag, name))
                                return true;
                            int len = strlen(name);
                            if (len > 2 && !stricmp(name+len-2, "ex") && !strnicmp(nametag, name, len-2))
                                return true;
                        }
                    }
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

Esdl2Base* createEsdlObject(Esdl2Transformer *xformer, IEsdlDefObject *def)
{
    EsdlDefTypeId type = def->getEsdlType();
    switch(type)
    {
    case EsdlTypeStruct:
        return new Esdl2Struct(xformer, dynamic_cast<IEsdlDefStruct*>(def));
    case EsdlTypeElement:
        return new Esdl2Element(xformer, def);
    case EsdlTypeArray:
        return new Esdl2Array(xformer, def);
    case EsdlTypeEnumDef:
        return new Esdl2EnumItem(xformer, def);
    case EsdlTypeEnumRef:
        return new Esdl2EnumRef(xformer, def);
    case EsdlTypeAttribute:
        return new Esdl2Attribute(xformer, def);
    case EsdlTypeRequest:
        return new Esdl2Request(xformer, dynamic_cast<IEsdlDefStruct*>(def));
    case EsdlTypeResponse:
        return new Esdl2Response(xformer, dynamic_cast<IEsdlDefStruct*>(def));
    case EsdlTypeMethod:
        return new Esdl2Method(xformer, dynamic_cast<IEsdlDefMethod*>(def));
    case EsdlTypeService:
        return new Esdl2Service(xformer, def);
    default:
        return NULL;
    }
}

// ======================================================================================
//  class Esdl2LocalContext

void Esdl2LocalContext::handleDataFor(IXmlWriterExt & writer)
{
    if (m_dataFor || m_dataOrig)
    {
        // values available in both orig and dataFor: keep the orig
        if (m_dataOrig && m_dataFor)
        {
            for (unsigned i=0; i<m_dataOrig->ordinality(); i++)
            {
                const char* name = m_dataOrig->item(i);
                if (m_dataFor->remove(name))
                    ESDL_DBG("Data available for '%s' twice: data_for and orig; the orig wins", name);
            }
        }

        // only available in dataFor
        if (m_dataFor)
        {
            HashIterator it(*m_dataFor);
            for (it.first(); it.isValid(); it.next())
            {
                IMapping& et = it.query();
                writer.outputCString(m_dataFor->mapToValue(&et)->get(), NULL);
            }
        }
    }
}

// ======================================================================================
// class Esdl2Base

Esdl2Base::Esdl2Base(Esdl2Transformer *xformer, IEsdlDefObject* def, EsdlBasicElementType t, bool might_skip_root_)
: m_def(def), might_skip_root(might_skip_root_), data_for(NULL), type_id(t)
{
    if (def->queryProp("optional"))
        param_group.set(def->queryProp("optional"));
    if (def->queryProp("counter"))
        count_output = true;

    const char *count_flag = def->queryProp("count_val");
    if (!stricmp(queryName(), "RecordCount") || count_flag)
    {
        if (count_flag==NULL || *count_flag!='0')
            count_value = true;
    }

    if (def->queryProp("xml_tag"))
        xml_tag.set(def->queryProp("xml_tag"));
    if (xml_tag.isEmpty())
        xml_tag.set(queryName());
}

Esdl2Base::~Esdl2Base()
{
}

void Esdl2Base::serialize(StringBuffer &out, const char *type)
{
    out.appendf("<%s", type);
    serialize_attributes(out);
    if (!hasChild())
        out.append("/>");
    else
    {
        out.append(">");
        serialize_children(out);
        out.appendf("</%s>", type);
    }
}

void Esdl2Base::serialize_attributes(StringBuffer &out)
{
    Owned<IPropertyIterator> piter = m_def->getProps();
    ForEach(*piter)
    {
        const char *key = piter->getPropKey();
        if (key && *key)
        {
            if (stricmp(key, "base_type"))
            {
                const char *value = m_def->queryProp(key);
                if (value && *value)
                    out.appendf(" %s=\"%s\"", key, value);
            }
        }
    }
}

// return true if it passed version check
bool Esdl2Base::checkVersion(Esdl2TransformerContext &ctx)
{
    if (!param_group.isEmpty())
        if (!ctx.param_groups || !ctx.param_groups->hasProp(param_group.get()))
            return false;

    return m_def->checkVersion(ctx.client_ver);
}

void Esdl2Base::countContent(Esdl2TransformerContext &ctx)
{
    StringBuffer content;
    for (int type=ctx.xppp->next(); type!=XmlPullParser::END_TAG; type=ctx.xppp->next())
    {
        switch(type)
        {
            case XmlPullParser::START_TAG:  //shouldn't have nested tags, skip
            {
                StartTag temp;
                ctx.xppp->readStartTag(temp);
                ctx.xppp->skipSubTree();
                break;
            }
            case XmlPullParser::CONTENT:  //support multiple content items, append
            {
                content.append(ctx.xppp->readContent());
                break;
            }
        }
    }

    if (content.trim().length() && (queryEclNull()==NULL || strcmp(queryEclNull(), content.str())))
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

void Esdl2Base::output_content(Esdl2TransformerContext &ctx, IPropertyTree *pt)
{
    output_content(ctx, pt, xml_tag.str());
}

void Esdl2Base::output_content(Esdl2TransformerContext &ctx, const char * content, const char *tagname, unsigned leadinzeros)
{
    if (content && *content && tagname && *tagname && (queryEclNull()==NULL || !streq(queryEclNull(), content)))
    {
        unsigned contentlen = strlen(content);

            if (leadinzeros > 0 )
            {
                StringBuffer padded;
                if (leadinzeros > contentlen)
                    padded.appendN(leadinzeros - contentlen,'0');
                padded.append(content);

                ctx.writer->outputCString(padded.str(), tagname);
            }
            else
            {
                switch (type_id)
                {
                    case ESDLT_BOOL:
                        ctx.writer->outputBool(strToBool(content), tagname);
                        break;
                    case ESDLT_INT8:
                    case ESDLT_INT16:
                    case ESDLT_INT32:
                    case ESDLT_INT64:
                        ctx.writer->outputInt(atoi(content), sizeof(int), tagname);
                        break;
                    case ESDLT_UINT8:
                    case ESDLT_UINT16:
                    case ESDLT_UINT32:
                    case ESDLT_UINT64:
                        ctx.writer->outputUInt(atoi(content), sizeof(unsigned), tagname);
                        break;
                    case ESDLT_BYTE:
                    case ESDLT_UBYTE:
                        ctx.writer->outputData(1, content, tagname);
                        break;
                    case ESDLT_FLOAT:
                    case ESDLT_DOUBLE:
                        ctx.writer->outputNumericString(content, tagname);
                        break;
                    case ESDLT_STRING:
                    default:
                        ctx.writer->outputCString(content, tagname);
                        break;
                }
            }

            if (count_output)
                ctx.counter++;
            if (count_value)
                ctx.counter=atoi(content);
    }
}

void Esdl2Base::output_content(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *tagname)
{
    StringBuffer content(pt->queryProp(NULL));
    const char* leadingZeroStr = m_def->queryProp("leading_zero");
    unsigned leadingZero =  (leadingZeroStr && *leadingZeroStr) ? atoi(leadingZeroStr) : 0;

    output_content(ctx, content.str(), tagname, leadingZero);
}

void Esdl2Base::output_content(Esdl2TransformerContext &ctx, const char *tagname)
{
    StringBuffer content;
    for (int type=ctx.xppp->next(); type!=XmlPullParser::END_TAG; type=ctx.xppp->next())
    {
        switch(type)
        {
            case XmlPullParser::START_TAG:  //shouldn't have nested tags, skip
            {
                StartTag temp;
                ctx.xppp->readStartTag(temp);
                ctx.xppp->skipSubTree();
                break;
            }
            case XmlPullParser::CONTENT:  //support multiple content items, append
            {
                content.append(ctx.xppp->readContent());
                break;
            }
        }
    }

    const char* lz = m_def->queryProp("leading_zero");
    unsigned leadingZero =  (lz && *lz) ? atoi(lz) : 0;

    output_content(ctx, content.str(), tagname, leadingZero);
}

void Esdl2Base::output_content(Esdl2TransformerContext &ctx)
{
    output_content(ctx, xml_tag.str());
}

void Esdl2Base::output_ecl_date(Esdl2TransformerContext &ctx, const char *tagname)
{
    int Month=0;
    int Day=0;
    int Year=0;

    StartTag child;
    StringBuffer content;
    for (int type=ctx.xppp->next(); type!=XmlPullParser::END_TAG; type=ctx.xppp->next())
    {
        switch(type)
        {
        case XmlPullParser::START_TAG:  //shouldn't have nested tags, skip
            {
                ctx.xppp->readStartTag(child);
                readFullContent(*ctx.xppp, content.clear());
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
        ctx.writer->outputCString(date.str(), tagname);
        if (count_output)
            ctx.counter++;
    }
}

void Esdl2Base::addChildren(Esdl2Transformer *xformer, IEsdlDefObjectIterator *it)
{
    assert(false);
}

Esdl2Base* Esdl2Base::queryChild(const char* name, bool nocase)
{
    assert(false);
    return NULL;
}

void Esdl2Base::mergeBaseType(Esdl2Transformer *xformer, const char *base_type)
{
    if (esdlSimpleType(base_type)==ESDLT_COMPLEX)
    {
        Esdl2Base *esdlBase = xformer->queryType(base_type);

        if (!esdlBase)
        {
            StringBuffer msg;
            msg.appendf("ESDL Initialization Error: BaseType=%s, not found for item %s", base_type, queryName());
            throw MakeStringExceptionDirect(-1, msg.str());
        }

        IEsdlDefObject*  base = esdlBase->queryEsdlDefObject();

        // recursive
        if (base->queryProp("base_type"))
            mergeBaseType(xformer, base->queryProp("base_type"));

        //merge base children
        IEsdlDefStruct* st = dynamic_cast<IEsdlDefStruct*>(base);
        if (st)
        {
            Owned<IEsdlDefObjectIterator> it = st->getChildren();
            addChildren(xformer, it.get());
        }
    }
}

// ======================================================================================
// class Esdl2Element

Esdl2Element::Esdl2Element(Esdl2Transformer *xformer, IEsdlDefObject *def) : Esdl2Base(xformer, def), esdl_type(NULL)
{
    complex_type.set(def->queryProp("complex_type"));
    simple_type.set(def->queryProp("type"));
    if (!simple_type.isEmpty())
    {
        type_id = esdlSimpleType(simple_type.get());
        if (type_id==ESDLT_COMPLEX)
        {
            DBGLOG("ESDL simple type not defined: %s", simple_type.get());
            assert(!"Error");
        }
    }
    else
    {
        type_id=ESDLT_COMPLEX;
        assert(complex_type.get());
    }
}

void Esdl2Element::buildDefaults(Esdl2Transformer *xformer, StringBuffer &path, IProperties *defvals)
{
    if (type_id==ESDLT_COMPLEX)
    {
        int curlen = path.length();
        if (path.length())
            path.append("/");
        path.append(queryName());
        if (!esdl_type)
        {
            assert(complex_type.get());
            Esdl2Base *type_entry = xformer->queryType(complex_type.get());
            if (type_entry)
                esdl_type = type_entry;
            else
                DBGLOG("ESDL Type not defined \"%s\" in \"%s\"", complex_type.get(), queryName());
        }
        if (esdl_type)
            esdl_type->buildDefaults(xformer, path, defvals);
        path.setLength(curlen);
    }
    else if (!queryDefaultValue())
    {
        int curlen = path.length();
        if (path.length())
            path.append("/");
        path.append(queryName());
        defvals->setProp(path.str(), queryDefaultValueS());
        path.setLength(curlen);
    }

}

void Esdl2Element::process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local, bool count)
{
    ESDL_DBG("EsdlElement::process(pt): %s", queryName());

    if (data_for && data_for->checkVersion(ctx))
    {
        throw MakeStringException(-1, "EsdlElement::process(pt):%s: IPTree version of data_for not implemented", queryName());
    }
    else if (!checkVersion(ctx))
    {
        if (count_value)
            countContent(ctx);
    }
    else if (complex_type.isEmpty())
    {
        output_content(ctx, pt);
    }
    else
    {
        if (!esdl_type)
        {
            Esdl2Base *type_entry = ctx.queryType(complex_type.get());
            if (type_entry)
                esdl_type = type_entry;
            else
                DBGLOG("%s: ESDL Type not defined \"%s\" in \"%s\"", ctx.root_type.get(), complex_type.get(), queryName());
        }

        if (esdl_type)
        {
            const char* tag = NULL;
            if(ctx.skip_root && might_skip_root)
                tag = NULL;
            else
                tag = queryOutputName(ctx);

            esdl_type->process(ctx, pt, tag, NULL, count_output);
        }
    }
}

void Esdl2Element::process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local,bool count)
{
    ESDL_DBG("EsdlElement::process: %s", queryName());

    if (data_for && data_for->checkVersion(ctx))
    {
        ESDL_DBG("DataFor %s processed", data_for->queryName());
        if (local)
            local->dataForProcessed = true;
        data_for->process(ctx,out_name,NULL,count);
    }
    else if (!checkVersion(ctx))
    {
        if (count_value)
            countContent(ctx);
        else
            ctx.xppp->skipSubTree();
    }
    else if (complex_type.isEmpty())
    {
        output_content(ctx);
    }
    else
    {
        if (!esdl_type)
        {
            Esdl2Base *type_entry = ctx.queryType(complex_type.get());
            if (type_entry)
                esdl_type = type_entry;
            else
                DBGLOG("%s: ESDL Type not defined \"%s\" in \"%s\"", ctx.root_type.get(), complex_type.get(), queryName());
        }

        if (esdl_type)
        {
            const char* tag = NULL;
            if(ctx.skip_root && might_skip_root)
                tag = NULL;
            else if(ctx.mode==EsdlRequestMode)
                tag = queryName();
            else
                tag = xml_tag.get();

            esdl_type->process(ctx,tag,local,count_output);
        }
        else
            ctx.xppp->skipSubTree();
    }
}

// ======================================================================================
// class Esdl2Array

Esdl2Array::Esdl2Array(Esdl2Transformer *xformer, IEsdlDefObject *def) : Esdl2Base(xformer, def), esdl_type(NULL)
{
    type_unknown=false;
    inited=false;

    const char *atype = def->queryProp("type");
    if (atype)
    {
        type.set(atype);
        type_id = esdlSimpleType(atype);
    }

    const char* itag = def->queryProp("item_tag");
    if (itag)
        item_tag.set(itag);
    else if (!atype || !stricmp(atype, "string")) // defuault for simple type
        item_tag.set("Item");
    else // defuault for complex type
        item_tag.set(atype);

    // flat_array
    //if (def->queryProp("@flat_array"))
    //  name.set(item_tag.get());
}

void Esdl2Array::init(Esdl2TransformerContext &ctx)
{
    if (!inited)
    {
        if (type_id==ESDLT_COMPLEX && !esdl_type)
        {
            Esdl2Base *type_entry = ctx.queryType(type.get());
            if (type_entry)
                esdl_type = type_entry;
            else
                DBGLOG("%s: ESDL Type not defined \"%s\" in \"%s\"", ctx.root_type.get(), type.get(), queryName());
        }
        if (type_id==ESDLT_COMPLEX && !esdl_type)
            type_unknown=true;
        inited=true;
    }
}

void Esdl2Array::process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local, bool count)
{
    ESDL_DBG("EsdlArray::process(pt): %s", queryName());
    if (!inited)
        init(ctx);

    bool flat_array = m_def->hasProp("flat_array");

    if (data_for && data_for->checkVersion(ctx))
    {
        throw MakeStringException(-1, "EsdlElement::process(pt):%s: IPTree version of data_for not implemented", queryName());
    }
    else if (type_unknown)
    {
        DBGLOG("EsdlArray type (%s) unknown", type.get());
    }
    else if (!checkVersion(ctx))
    {
        ctx.xppp->skipSubTree();
    }
    else if (flat_array)
    {
        int curlen = ctx.writer->length();
        if (esdl_type)
            esdl_type->process(ctx, item_tag.get(), local, count_output);
        else
            output_content(ctx, item_tag.get());

        if (ctx.writer->length() != curlen && count)
            ctx.counter++;
    }
    else
    {
        const char *tagname = queryOutputName(ctx);
        if (pt->hasChildren())
        {
            int prevlen = ctx.writer->length();

            ctx.writer->outputBeginNested(tagname, true);
            ctx.writer->outputBeginArray(item_tag.get());
            Owned<IPropertyTreeIterator> it = pt->getElements(item_tag.get());

            int curlen = ctx.writer->length();
            ForEach(*it)
            {
                if (esdl_type)
                {
                    esdl_type->process(ctx, &it->query(), item_tag.get(), NULL, count_output);
                }
                else
                    output_content(ctx, &it->query(), item_tag.get());
            }

            if (ctx.writer->length() == curlen) //nothing was added... empty content remove opening tag
            {
                ctx.writer->outputEndArray(item_tag.get());
                ctx.writer->outputEndNested(tagname); //we need to close out the nested area first
                ctx.writer->rewindTo(prevlen); //rewind
            }
            else
            {
                ctx.writer->outputEndArray(item_tag.get());
                ctx.writer->outputEndNested(tagname);
                if (count)
                    ctx.counter++;
            }
        }
    }
}

void Esdl2Array::process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local,bool count)
{
    ESDL_DBG("EsdlArray::process: %s", queryName());
    if (!inited)
        init(ctx);

    bool flat_array = m_def->hasProp("flat_array");

    if (data_for && data_for->checkVersion(ctx))
    {
        ESDL_DBG("DataFor %s processed", data_for->queryName());
        if (local)
            local->dataForProcessed = true;
        data_for->process(ctx,out_name,NULL,count);

    }
    else if (type_unknown)
    {
        ctx.xppp->skipSubTree();
        DBGLOG("EsdlArray type (%s) unknown", type.get());
    }
    else if (!checkVersion(ctx))
        ctx.xppp->skipSubTree();
    else if (flat_array)
    {
        int curlen = ctx.writer->length();
        if (esdl_type)
            esdl_type->process(ctx, item_tag.get(), local, count_output);
        else
            output_content(ctx, item_tag.get());

        if (ctx.writer->length() != curlen && count)
            ctx.counter++;
    }
    else
    {
        int prevlen = ctx.writer->length();
        ctx.writer->outputBeginNested(xml_tag.get(), true);
        ctx.writer->outputBeginArray(item_tag.get());
        int curlen = ctx.writer->length();

        for (int type=ctx.xppp->next(); type!=XmlPullParser::END_TAG; type=ctx.xppp->next())
        {
            if (type == XmlPullParser::START_TAG)
            {
                StartTag child_start;
                ctx.xppp->readStartTag(child_start);
                if (!stricmp(child_start.getLocalName(), item_tag.get()))
                {
                    if (esdl_type)
                        esdl_type->process(ctx, item_tag.get(), NULL, count_output);
                    else
                        output_content(ctx, item_tag.get());
                }
                else
                    ctx.xppp->skipSubTree();
            }
        }


        if (ctx.writer->length()==curlen) //nothing was added... empty content remove opening tag
        {
            ctx.writer->outputEndArray(item_tag.get());
            ctx.writer->outputEndNested(xml_tag.get()); // we need to close out this section first
            ctx.writer->rewindTo(prevlen); //rewind
        }
        else
        {
            ctx.writer->outputEndArray(item_tag.get());
            ctx.writer->outputEndNested(xml_tag.get());
            if (count)
                ctx.counter++;
        }
    }
}

// ======================================================================================
// class Esdl2Struct

Esdl2Struct::Esdl2Struct(Esdl2Transformer *xformer, IEsdlDefStruct *def, EsdlBasicElementType t)
 : Esdl2Base(xformer, def, t, t==ESDLT_RESPONSE),isElement(false)
{
    ESDL_DBG("Esdl2Struct: %s", def->queryName());
    if (def->queryProp("base_type"))
    {
        DBGLOG("mergeBaseType: %s", def->queryProp("base_type"));
        mergeBaseType(xformer, def->queryProp("base_type"));
    }
    if (def->hasProp("@element"))
        isElement = true;

    // add children
    Owned<IEsdlDefObjectIterator> chds = def->getChildren();
    addChildren(xformer,chds.get());
}

Esdl2Struct::~Esdl2Struct()
{
    ForEachItemIn(idx, m_children)
    {
        Esdl2Base *child = &m_children.item(idx);
        child->Release();
    }

    HashIterator xti(xml_tags);
    for (xti.first(); xti.isValid(); xti.next())
    {
        IMapping& m = xti.query();
        EsdlBaseArrayPtr *ebap = xml_tags.mapToValue(&m);
        if (ebap && *ebap)
            delete *ebap;
    }
}

void Esdl2Struct::buildDefaults(Esdl2Transformer *xformer, StringBuffer &path, IProperties *defvals)
{
    ForEachItemIn(idx, m_children)
    {
        Esdl2Base &child = m_children.item(idx);
        child.buildDefaults(xformer, path, defvals);
    }
}

Esdl2Base* Esdl2Struct::queryChild(const char* name, bool nocase)
{
    ForEachItemIn(idx, m_children)
    {
        Esdl2Base *child = &m_children.item(idx);
        if ((nocase && strieq(child->queryName(), name)) || streq(child->queryName(),name))
            return child;
    }

    return NULL;
}

void Esdl2Struct::process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local_in, bool count)
{
    ESDL_DBG("Esdl2Struct::process(pt): %s", queryName());

    if (checkVersion(ctx))
    {
        unsigned prevlen = ctx.writer->length();
        if (out_name && *out_name) {
            if (!might_skip_root || !ctx.skip_root)
                ctx.writer->outputBeginNested(out_name, true);
        }
        unsigned curlen = ctx.writer->length();

        Esdl2LocalContext local;
        ForEachItemIn(idx, m_children)
        {
            Esdl2Base &child = m_children.item(idx);
            if (child.checkVersion(ctx))
            {
                const char *tagname = child.queryInputName(ctx);
                if (pt->hasProp(tagname)||child.hasDefaults())
                    child.process(ctx, pt->queryPropTree(tagname), child.queryOutputName(ctx), &local, count);
            }
        }

        if (count && ctx.writer->length() > curlen)
            ctx.counter++;

        if (out_name && *out_name)
        {
            if (!ctx.skip_root || !might_skip_root)
            {
                if (ctx.writer->length() == curlen) //nothing was added, empty content, remove open tag
                {
                    ctx.writer->outputEndNested(out_name); //we need to close out current section first
                    ctx.writer->rewindTo(prevlen); //rewind
                }
                else
                    ctx.writer->outputEndNested(out_name);
            }
        }
    }
}

void Esdl2Struct::process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local_in, bool count)
{
    ESDL_DBG("Esdl2Struct::process: %s", queryName());

    if (!checkVersion(ctx))
        ctx.xppp->skipSubTree();
    else
    {
        unsigned prevlen = ctx.writer->length();
        unsigned curlen = prevlen;

        if (out_name && *out_name)
        {
            if (!might_skip_root || !ctx.skip_root)
            {
                ctx.writer->outputBeginNested(out_name, true);

                curlen = ctx.writer->length();

                if (local_in && local_in->m_startTag)
                {
                    int attcount = local_in->m_startTag->getLength();
                    if (attcount)
                    {
                        for (int idx=0; idx<attcount; idx++)
                        {
                            StringBuffer attname("@");
                            attname.append(local_in->m_startTag->getLocalName(idx));

                            ctx.writer->outputCString(local_in->m_startTag->getValue(idx), attname.str());
                        }
                    }
                }
            }
        }

        Esdl2LocalContext local;
        StringBuffer completeContent;
        for (int type=ctx.xppp->next(); type!=XmlPullParser::END_TAG; type=ctx.xppp->next())
        {
            switch(type)
            {
                case XmlPullParser::CONTENT:
                {
                    if(isElement)
                    {
                        completeContent.append(ctx.xppp->readContent());
                    }
                    break;
                }
                case XmlPullParser::START_TAG:
                {
                    StartTag child_start;
                    ctx.xppp->readStartTag(child_start);

                    if (isElement)
                        ctx.xppp->skipSubTree();
                    else
                    {
                        local.m_startTag = &child_start;

                        Esdl2Base *child = queryChild(child_start.getLocalName());
                        if (!child || !child->checkVersion(ctx))
                        {
                            if(ctx.mode==EsdlRequestMode)
                            {
                                EsdlBaseArrayPtr * arrayPtr = xml_tags.getValue(child_start.getLocalName());
                                if(arrayPtr && *arrayPtr)
                                {
                                    EsdlBaseArray& array = **arrayPtr;

                                    ForEachItemIn(idx, array)
                                    {
                                        Esdl2Base *base = &array.item(idx);
                                        if(base->checkVersion(ctx))
                                        {
                                            child = base;
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        if (!child && type_id==ESDLT_RESPONSE && strieq("Row", child_start.getLocalName()))
                            child = queryChild("response", true);
                        if (child)
                        {
                            Esdl2Base& chd = *child;

                            unsigned len = ctx.writer->length();
                            local.dataForProcessed = false;

                            chd.process(ctx, NULL, &local);

                            if (local.dataForProcessed)
                            {
                                assertex(chd.queryDataFor()!=NULL);

                                if (ctx.writer->length() > len)
                                {
                                    ESDL_DBG("Taking out data for DataFor '%s' from out buffer", chd.queryDataFor()->queryName());
                                    local.setDataFor(chd.queryDataFor()->queryName(), ctx.writer->str()+len);
                                    ctx.writer->rewindTo(len);
                                }
                            }

                            if (chd.hasDataFrom())
                            {
                                if (ctx.writer->length() > len)
                                {
                                    ESDL_DBG("Orig data '%s' is not empty", chd.queryName());
                                    local.setDataOrig(chd.queryName());
                                }
                            }
                        }
                        else
                            ctx.xppp->skipSubTree();
                    }
                    break;
                }
            }
        }

        if (completeContent.length()>0)
            ctx.writer->outputCString(completeContent.str(), NULL);

        local.handleDataFor(*(ctx.writer));

        if (count && ctx.writer->length() > curlen)
            ctx.counter++;

        if (out_name && *out_name)
        {
            if (!ctx.skip_root || !might_skip_root)
            {
                if (ctx.writer->length() == curlen) //nothing was added, empty content, remove open tag
                {
                    ctx.writer->outputEndNested(out_name); //we need to close out current section first
                    ctx.writer->rewindTo(prevlen); //rewind
                }
                else
                {
                    ctx.writer->outputEndNested(out_name);
                }
            }
        }
    }
}

void Esdl2Struct::processElement(Esdl2TransformerContext &ctx)
{
    ESDL_DBG("Esdl2Struct::processElement: %s", queryName());

    for (int type=ctx.xppp->next(); type!=XmlPullParser::END_TAG; type=ctx.xppp->next())
    {
        if (type==XmlPullParser::START_TAG)
        {
            StartTag child_start;
            ctx.xppp->readStartTag(child_start);
            Esdl2Base **child = m_child_map.getValue(child_start.getLocalName());
            if (child && *child)
                (*child)->process(ctx, NULL);
            else
                ctx.xppp->skipSubTree();
            break;
        }
    }
}

void Esdl2Struct::addChildren(Esdl2Transformer *xformer, IEsdlDefObjectIterator *it)
{
    ForEach(*it)
    {
        IEsdlDefObject* item = &it->query();
        const char *name = item->queryName();
        Esdl2Base *obj = createEsdlObject(xformer, item);
        if (obj)
        {
            m_children.append(*obj);
            m_child_map.setValue(obj->queryName(), obj);
            if (obj->queryEclName())
                m_child_map.setValue(obj->queryEclName(), obj);
            if (might_skip_root && !stricmp(obj->queryName(), "response"))
                obj->setMightSkipRoot(true);
        }
    }

    // resolve get_data_from
    ForEach(*it)
    {
        const char* dataFrom = it->query().queryProp("get_data_from");
        if (dataFrom)
        {
            const char* name = it->query().queryProp("name");
            Esdl2Base* self = queryChild(name);
            Esdl2Base* dataSrc = queryChild(dataFrom);

            if (dataSrc && self) {
                self->setHasDataFrom();
                dataSrc->setDataFor(self);
            }
            else
            {
                VStringBuffer msg("Can not find element: %s for %s as data_for target", dataFrom, self ? self->queryName() : "UNKNOWN");
                ERRLOG("%s", msg.str());
                throw MakeStringException(-1, "Internal Error: %s", msg.str());
            }
        }
    }

    // resolve xml_tag
    ForEach(*it)
    {
        const char* xmlTag = it->query().queryProp("xml_tag");
        if (xmlTag)
        {
            const char* name = it->query().queryProp("name");
            Esdl2Base* self = queryChild(name);

            EsdlBaseArrayPtr * arrayPtr= xml_tags.getValue(xmlTag);
            if (arrayPtr)
                (*arrayPtr)->append(*self);
            else
            {
                EsdlBaseArray* array = new EsdlBaseArray;
                array->append(*self);
                xml_tags.setValue(xmlTag, array);
            }
        }
    }
}

// ======================================================================================
// class Esdl2Request

void Esdl2Request::process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local, bool count)
{
    ESDL_DBG("Esdl2Request::process(pt): %s", queryName());

    if (checkVersion(ctx))
    {
        if (out_name && *out_name)
        {
            if (!ctx.skip_root)
                ctx.writer->outputBeginNested(out_name, true);
            if (ctx.flags & ESDL_TRANS_ROW_OUT)
                ctx.writer->outputBeginNested("Row", true);
        }

        Esdl2Struct::process(ctx, pt, NULL, local, count);

        if (out_name && *out_name)
        {
            if (ctx.flags & ESDL_TRANS_ROW_OUT)
                ctx.writer->outputEndNested("Row");
            if (!ctx.skip_root)
                ctx.writer->outputEndNested(out_name);
        }
    }
}

void Esdl2Request::buildDefaults(Esdl2Transformer *xformer)
{
    defvals.setown(createProperties(false));
    ForEachItemIn(idx, m_children)
    {
        Esdl2Base &child = m_children.item(idx);
        StringBuffer path;
        child.buildDefaults(xformer, path, defvals.get());
    }
}

void Esdl2Request::addDefaults(IPropertyTree *req)
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

void Esdl2Response::process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local, bool count)
{
    ESDL_DBG("EsdlResponse::processRespChild: %s", queryName());

    if (!checkVersion(ctx))
        ctx.xppp->skipSubTree();
    else
    {
        if (out_name && *out_name) {
            if (!might_skip_root || !ctx.skip_root)
            {
                if (ctx.do_output_ns)
                {
                    ctx.writer->outputBeginNested(out_name, true);
                    ctx.writer->outputXmlns("xmlns", ctx.ns.str());

                    if (ctx.schemaLocation.length() > 0 )
                    {
                        ctx.writer->outputXmlns("xsi", "http://www.w3.org/2001/XMLSchema-instance");
                        ctx.writer->outputCString(ctx.schemaLocation.str(), "@xsi:schemaLocation");
                    }

                    ctx.do_output_ns=false;
                }
                else
                {
                    ctx.writer->outputBeginNested(out_name, true);
                }
            }
        }

        if (ctx.flags & ESDL_TRANS_ROW_IN)
        {
            StartTag stag;
            if (xppGotoTag(*ctx.xppp, "Row",stag))
                Esdl2Struct::process(ctx, NULL, local, count);
        }
        else
            Esdl2Struct::process(ctx, NULL, local, count);

        if (out_name && *out_name)
        {
            if (!ctx.skip_root || !might_skip_root)
            {
                ctx.writer->outputEndNested(out_name);
            }
        }
    }
}

void Esdl2Response::processChildNamedResponse(Esdl2TransformerContext &ctx, const char *out_name)
{
    ESDL_DBG("Esdl2Struct::processRespChild: %s", queryName());

    Esdl2Base *child = queryChild("response", true);

    if (!child || !checkVersion(ctx))
        ctx.xppp->skipSubTree();
    else
    {
        if (out_name && *out_name) {
            if (!might_skip_root || !ctx.skip_root)
            {
                if (ctx.do_output_ns)
                {
                    ctx.writer->outputBeginNested(out_name, true);
                    ctx.writer->outputXmlns(out_name, ctx.ns.str());
                    ctx.do_output_ns=false;
                }
                else
                {
                    ctx.writer->outputBeginNested(out_name, true);
                }
            }
        }

        if (ctx.flags & ESDL_TRANS_ROW_IN)
        {
            StartTag stag;
            if (xppGotoTag(*ctx.xppp, "Row",stag))
                child->process(ctx, NULL);
        }
        else
            child->process(ctx, NULL);

        if (out_name && *out_name)
            if (!ctx.skip_root || !might_skip_root)
            {
                ctx.writer->outputEndNested(out_name);
            }
    }
}

// ======================================================================================
// class Esdl2EnumRef

void Esdl2Attribute::process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local, bool count)
{
    //need to make sure these are output inside the parent brackets
    //if (checkVersion(ctx))
    //    output_content(ctx, pt);
}

// ======================================================================================
// class Esdl2EnumRef

void Esdl2EnumRef::process(Esdl2TransformerContext &ctx, IPropertyTree *pt, const char *out_name, Esdl2LocalContext* local, bool count)
{
    if (checkVersion(ctx))
        output_content(ctx, pt);
}

void Esdl2EnumRef::process(Esdl2TransformerContext &ctx, const char *out_name, Esdl2LocalContext* local,bool count)
{
    if (!checkVersion(ctx))
        ctx.xppp->skipSubTree();
    else
        output_content(ctx);
}

// ======================================================================================
// class Esdl2Transformer

Esdl2Transformer::Esdl2Transformer(IEsdlDefinition* def)
{
    m_def.setown(LINK(def));
    initEsdlTypeList();
}

void Esdl2Transformer::serialize(StringBuffer &out)
{
    out.append("<esxdl>");
    ForEachItemIn(idx, types)
    {
        Esdl2Base &type = types.item(idx);
        type.serialize(out);
    }
    out.append("</esxdl>");
}

void Esdl2Transformer::addMethod(Esdl2Base *item)
{
    if (item)
    {
        Esdl2Method *method=dynamic_cast<Esdl2Method *>(item);
        if (method)
        {
            methods.append(*method);
            meth_map.setValue(method->queryName(), method);
        }
    }
}

IEsdlMethodInfo* Esdl2Transformer::queryMethodInfo(const char* service,const char *method)
{
    Esdl2Method **mt = meth_map.getValue(method);
    if (mt && *mt)
        return dynamic_cast<IEsdlMethodInfo *>(*mt);

    IEsdlDefService* svc = m_def->queryService(service);
    if (!svc)
        return NULL;
    IEsdlDefMethod* mth = svc->queryMethodByName(method);

    Esdl2Method* m = new Esdl2Method(this,mth);
    meth_map.setValue(method,m);
    return m;
}

void Esdl2Transformer::addType(Esdl2Base* type)
{
    types.append(*type);
    type_map.setValue(type->queryName(), type);
}

Esdl2Base* Esdl2Transformer::queryType(const char* name)
{
    ESDL_DBG("queryType(%s)", name);
    Esdl2Base** typ = type_map.getValue(name);
    if (typ && *typ)
        return *typ;

    IEsdlDefObject* obj = m_def->queryObj(name);
    if (obj)
    {
        Esdl2Base* type = createEsdlObject(this,obj);
        ESDL_DBG("addType(%s)", name);
        addType(type);
        return type;
    }

    return NULL;
}

Esdl2Transformer::~Esdl2Transformer()
{
    ForEachItemIn(idx, types)
    {
        Esdl2Base *type = &types.item(idx);
        type->Release();
    }

    ForEachItemIn(mdx, methods)
    {
        Esdl2Method *meth = &methods.item(mdx);
        meth->Release();
    }

    HashIterator mmit(meth_map);
    for (mmit.first(); mmit.isValid(); mmit.next())
    {
        IMapping& et = mmit.query();
           Esdl2Method *em = (*meth_map.mapToValue(&et));
        if (em)
            em->Release();

    }

}

int Esdl2Transformer::process(IEspContext &ctx, EsdlProcessMode mode, const char* service, const char *method, StringBuffer &out, const char *in, unsigned int flags, const char *ns, const char *schema_location)
{
    int rc = 0;
    IEsdlMethodInfo *mi = queryMethodInfo(service,method);
    if (!mi)
        throw MakeStringException(-1, "Error processing ESDL - method '%s'not found", method);

    const char *root_type=NULL;
    const char *root_name=NULL;
    if (mode==EsdlRequestMode)
    {
        root_name = "row";
        root_type = mi->queryRequestType();
    }
    else if (mode==EsdlResponseMode)
    {
        root_type = mi->queryResponseType();
    }

    if (!root_type)
        throw MakeStringException(-1, "Error processing ESDL - starting type not defined");

    Esdl2Base* root = queryType(root_type);
    if (!root)
        throw MakeStringException(-1, "Error processing ESDL - root type '%s' not found", root_type);

    IProperties *param_groups = ctx.queryRequestParameters();

    try
    {
        XmlPullParser xppx(in, strlen(in)+1);
        if (gotoStartTag(xppx, root->queryName(), "Results"))
        {
            Owned<IXmlWriterExt> respWriter = createIXmlWriterExt(0, 0, NULL, WTStandard);
            Esdl2TransformerContext tctx(*this, respWriter, xppx, ctx.getClientVersion(), param_groups, mode, 0, ns,schema_location);

            tctx.flags = flags;
            tctx.skip_root = !(flags & ESDL_TRANS_OUTPUT_ROOT);
            tctx.root_type.set(root->queryName());
            root->process(tctx, (root_name) ? root_name : root_type);
            rc = tctx.counter;
            if (mode==EsdlRequestMode)
            {
                Esdl2Request *rootreq = dynamic_cast<Esdl2Request *>(root);
                if (rootreq)
                {
                    DBGLOG("XML: %s", respWriter->str());
                    OwnedPTree req = createPTreeFromXMLString(respWriter->str(), false);
                    if (!req.get())
                        req.setown(createPTree(root_type,false));
                    rootreq->addDefaults(req);
                    toXML(req.get(), out.clear());
                }
            }
        }
        else
        {
            const char* errorMsg = "Internal Server Error: Results dataset not available.";
            DBGLOG("%s", errorMsg);
            throw MakeStringExceptionDirect(5001, errorMsg);
        }
    }
    catch (XmlPullParserException  &xpexp)
    {
        StringBuffer text;
        text.appendf("EsdlTransformer XPP exception: %s", xpexp.what());
        DBGLOG("%s", text.str());
        throw MakeStringExceptionDirect(5001, text.str());
    }
    catch (IException* e)
    {
        throw e;
    }
    catch (...)
    {
        throw MakeStringException(5001, "EsdlTransformer error parsing xml");
    }

    return rc;
};

int Esdl2Transformer::process(IEspContext &ctx, EsdlProcessMode mode, const char* service, const char *method, IPropertyTree &in, IXmlWriterExt* writer, unsigned int flags, const char *ns)
{
    IEsdlMethodInfo *mi = queryMethodInfo(service,method);
    if (!mi)
        throw MakeStringException(-1, "ESDL - method '%s::%s'not found", service, method);

    const char *root_type=NULL;
    if (mode==EsdlRequestMode)
        root_type=mi->queryRequestType();
    else if (mode==EsdlResponseMode)
        root_type=mi->queryResponseType();
    if (!root_type)
        throw MakeStringException(-1, "ESDL - starting type not defined for method '%s::%s'", service, method);

    Esdl2Base* root = queryType(root_type);
    if (!root)
        throw MakeStringException(-1, "Error processing ESDL - root type '%s' not found", root_type);

    IPropertyTree *finger = &in;
    if (!(flags & ESDL_TRANS_START_AT_ROOT))
    {
        if (flags & ESDL_TRANS_SOAP_IN)
        {
            if (finger->hasProp("Envelope"))
                finger=finger->queryPropTree("Envelope");
            if (finger->hasProp("Body"))
                finger=finger->queryPropTree("Body");
        }
        if (!strieq(finger->queryName(), root_type))
        {
            if (!finger->hasProp(root_type))
                throw MakeStringException(-1, "root element not found: %s", root_type);
            finger = finger->queryPropTree(root_type);
        }
    }

    IProperties *param_groups = ctx.queryRequestParameters();

    int rc = 0;
    try
    {
        Esdl2TransformerContext tctx(*this, writer, ctx.getClientVersion(), param_groups, mode, flags, ns);
        tctx.skip_root = false;
        tctx.root_type.set(root->queryName());

        root->process(tctx, &in, root_type);
        rc = tctx.counter;
    }
    catch (...)
    {
        throw MakeStringException(5001, "Esdl2Transformer error property tree xml");
    }

    return rc;
}

int Esdl2Transformer::processElement(IEspContext &ctx, const char* service, const char *parentStructName, IXmlWriterExt* writer, const char *in)
{
    if (!in || !*in)
        return 0;

    Esdl2Base* type = queryType(parentStructName);
    if (!type)
        throw MakeStringException(-1, "Error processing ESDL - type '%s' not found", parentStructName);

    try
    {
        XmlPullParser xppx(in, strlen(in));

        IProperties *param_groups = ctx.queryRequestParameters();
        Esdl2TransformerContext tctx(*this, writer, xppx, ctx.getClientVersion(), param_groups, EsdlResponseMode);
        tctx.skip_root = true;

        type->processElement(tctx);
        return tctx.counter;
    }
    catch (XmlPullParserException &xpexp)
    {
        StringBuffer text;
        text.appendf("Esdl2Transformer XPP exception: %s", xpexp.what());
        DBGLOG("%s", text.str());
        throw MakeStringExceptionDirect(5001, text.str());
    }
    catch (...)
    {
        throw MakeStringException(5001, "Esdl2Transformer error parsing xml");
    }

    return 0;
}

static const char * gotoNextHPCCDataset(XmlPullParser &xppx, StartTag &stag)
{
    int depth = 1;
    int level = 0;
    int type = XmlPullParser::END_TAG;

    bool looking = true;
    while (looking)
    {
        type = xppx.next();
        switch(type)
        {
            case XmlPullParser::START_TAG:
            case XmlPullParser::END_DOCUMENT:
                looking=false;
            break;
        }
    }

    do
    {
        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                xppx.readStartTag(stag);
                ++level;
                const char *tag = stag.getLocalName();
                if (!stricmp(tag, "Dataset"))
                    return stag.getValue("name");
                else if (!stricmp(tag, "Exception"))
                {
                    throw xppMakeException(xppx);
                }
                break;
            }
            case XmlPullParser::END_TAG:
                --level;
            break;

            case XmlPullParser::END_DOCUMENT:
                level=0;
            break;
        }
        type = xppx.next();
    }
    while (level > 0);

    return NULL;
}

void Esdl2Transformer::processHPCCResult(IEspContext &ctx, IEsdlDefMethod &mthdef, const char *xml, IXmlWriterExt* writer, StringBuffer &logdata, unsigned int flags, const char *ns, const char *schema_location)
{
    auto_ptr<XmlPullParser> xpp(new XmlPullParser());

    xpp->setSupportNamespaces(true);
    xpp->setInput(xml, strlen(xml));

    StartTag stag;
    int depth=1;

    IEsdlDefinition *esdl = m_def.get();

    if (!esdl)
        throw MakeStringExceptionDirect(-1, "ESDL transformer error: could not access ESDL definition object");

    const char *restype = mthdef.queryResponseType();
    if (!restype)
        throw MakeStringException(-1, "ESDL method %s, response type not declared", mthdef.queryName());

    IEsdlDefStruct *resdef = esdl->queryStruct(restype);
    if (!resdef)
        throw MakeStringException(-1, "ESDL method %s, response type %s not defined", mthdef.queryName(), restype);

    const char *resdsname = restype;
    const char *subresdsname = NULL;
    if (resdef->queryChild("response", true)) //indicates we're at the ResponseEx level
    {
        IEsdlDefObject *respobj = resdef->queryChild("response", true);
        EsdlDefTypeId tid = respobj->getEsdlType();
        if (tid==EsdlTypeElement)
        {
            IEsdlDefElement *subrespel = dynamic_cast<IEsdlDefElement*>(respobj);
            if (subrespel)
            {
                const char *eltype=subrespel->queryProp("complex_type");
                if (eltype)
                    subresdsname=eltype;
            }
        }
    }
    logdata.append("<LogDatasets>");
    const char * dataset;
    try
    {
        while((dataset = gotoNextHPCCDataset(*xpp, stag)) != NULL)
        {
            if ( strieq(dataset, resdsname)
                ||(subresdsname && strieq(dataset, subresdsname)) //Only allow correctly named dataset?
                || stricmp(dataset, "FinalResults")==0
                || stricmp(dataset, "Results")==0
               )
            {
                Esdl2TransformerContext tctx(*this, writer, *xpp, ctx.getClientVersion(), ctx.queryRequestParameters(), EsdlResponseMode, 0, ns,schema_location);
                tctx.flags = flags | ESDL_TRANS_ROW_IN;
                tctx.skip_root = !(flags & ESDL_TRANS_OUTPUT_ROOT);
                tctx.root_type.set(restype);
                Esdl2Base* root = queryType(restype);
                if (root)
                {
                    Esdl2Response *resp = dynamic_cast<Esdl2Response *>(root);
                    if (resp)
                    {
                        if (subresdsname)
                            resp->processChildNamedResponse(tctx, restype);
                        else
                            resp->process(tctx, restype);
                    }
                }
            }
            else if (strnicmp(dataset, "VendorGatewayRecords", 20)==0)
                xppToXmlString(*xpp, stag, logdata);
            else if (strnicmp(dataset, "LOG_", 4)==0)
                xppToXmlString(*xpp, stag, logdata);
            else
            {
                WARNLOG("ESDL processing HPCC Result: Dataset ignored: %s", dataset);
                xpp->skipSubTree();
            }
        }
        logdata.append("</LogDatasets>");
    }
    catch (...)
    {
        logdata.append("</LogDatasets>");
        throw;
    }
}

Esdl2Base* Esdl2TransformerContext::queryType(const char* name)
{
    return xformer.queryType(name);
}

// ======================================================================================
// factory

esdl_decl IEsdlTransformer *createEsdlXFormerFromXMLFilesV2(StringArray &files)
{
    Owned<IEsdlDefinition> esdl = createEsdlDefinition();

    ForEachItemIn(idx, files)
    {
        const char* file = files.item(idx);
        esdl->addDefinitionsFromFile(file);
    }

    return new Esdl2Transformer(esdl.get());
}

esdl_decl IEsdlTransformer *createEsdlXFormer(IEsdlDefinition *def)
{
    return new Esdl2Transformer(def);
}

// end
// ======================================================================================
