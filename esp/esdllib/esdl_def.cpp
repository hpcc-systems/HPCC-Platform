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

#include "jliball.hpp"
#include "espcontext.hpp"
#include "esdl_def.hpp"
#include <xpp/XmlPullParser.h>
#include <memory>

using namespace xpp;
using namespace std;

// Used as the Array type for holding the EsdlDefObjectWrappers.
// Ensures they get released when the array is destroyed.
typedef IEsdlDefFile * IEsdlDefFilePtr;

typedef MapStringTo<IEsdlDefObjPtr> AddedObjs;
typedef CopyReferenceArrayOf<IEsdlDefObject> EsdlDefObjectArray;
typedef CopyReferenceArrayOf<IEsdlDefMethod> EsdlDefMethodArray;

#define IMPLEMENT_ESDL_DEFOBJ \
    virtual const char *queryName() { return EsdlDefObject::queryName(); } \
    virtual const char *queryProp(const char *name) { return EsdlDefObject::queryProp(name); } \
virtual bool hasProp(const char* name) { return EsdlDefObject::hasProp(name); } \
    virtual int getPropInt(const char *pname, int def=0){return props->getPropInt(pname,def);} \
    virtual bool checkVersion(double ver) { return EsdlDefObject::checkVersion(ver); } \
    virtual bool checkFLVersion(double ver) { return EsdlDefObject::checkFLVersion(ver); } \
    virtual bool checkOptional(IProperties *opts ) { return EsdlDefObject::checkOptional(opts); }\
    virtual IPropertyIterator* getProps() { return EsdlDefObject::getProps(); }

class EsdlDefinition;
class EsdlDefObject;

class EsdlDefObjectArrayIterator : public CInterface, implements IEsdlDefObjectIterator
{
private:
      EsdlDefObjectArray & array;
      aindex_t             cur;

public:
    IMPLEMENT_IINTERFACE;

    EsdlDefObjectArrayIterator(EsdlDefObjectArray &a) : array(a)
    {
    }

    bool        first(void)             { cur = 0; return isValid(); }
    bool        isValid(void)           { return array.isItem(cur); }
    IEsdlDefObject &query()                { assertex(isValid()); return array.item(cur); }
    bool        hasNext(void)           { return array.isItem(cur+1); }
    bool        hasPrev(void)           { return array.isItem(cur-1); }
    bool        last(void)              { cur = array.ordinality()-1; return isValid(); }
    bool        next(void)              { ++cur; return isValid(); }
    bool        prev(void)              { --cur; return isValid(); }
    bool        select(aindex_t seek)   { cur = seek; return isValid(); }

    // Because this implementation doesn't have an array of wrappers
    // these accessors aren't supposed to be useable. However we needed
    // to place the accessors in the IEsdlDefObjectIterator to maintain
    // compatibility with users of the code who don't need these features
    unsigned getFlags() { return 0; }
    IEsdlDefObjectIterator* queryBaseTypesIterator()
    {
        return NULL;
    }
};


class EsdlDefObjectWrapper : public CInterface
{
protected:
    unsigned flags;
    IEsdlDefObject& object;
    EsdlDefObjectArray baseArray;
    IEsdlDefObjectIterator* baseIterator;

public:
    IMPLEMENT_IINTERFACE;

    EsdlDefObjectWrapper( IEsdlDefObject& _obj, unsigned _flags = 0 ) : object(_obj), flags(_flags), baseIterator(NULL)
    {
    }

    ~EsdlDefObjectWrapper()
    {
        if( baseIterator != NULL )
            baseIterator->Release();
    }

    unsigned getFlags() { return flags; }
    void setFlag( unsigned flag ) { flags |= flag; }
    void unsetFlag( unsigned flag ) { flags &= ~flag; }
    void appendBaseType( IEsdlDefObject* baseType ) { baseArray.append(*baseType); }
    IEsdlDefObject& queryObject() { return object; }
    IEsdlDefObjectIterator* queryBaseTypesIterator()
    {
        if( baseIterator == NULL )
        {
            baseIterator = new EsdlDefObjectArrayIterator(this->baseArray);
        }
        return baseIterator;
    }
};

typedef OwnedReferenceArrayOf<EsdlDefObjectWrapper> EsdlDefObjectWrapperArray;

class OwnedEsdlDefObjectArrayIterator : public CInterface, implements IEsdlDefObjectIterator
{
private:
      EsdlDefObjectWrapperArray* array;
      aindex_t             cur;

public:
    IMPLEMENT_IINTERFACE;

    OwnedEsdlDefObjectArrayIterator( EsdlDefObjectWrapperArray* a ) : array(a) {}
    ~OwnedEsdlDefObjectArrayIterator() { delete array; }

    bool        first(void)             { cur = 0; return isValid(); }
    bool        isValid(void)           { return array->isItem(cur); }
    IEsdlDefObject &query()                { assertex(isValid()); return array->item(cur).queryObject(); }
    bool        hasNext(void)           { return array->isItem(cur+1); }
    bool        hasPrev(void)           { return array->isItem(cur-1); }
    bool        last(void)              { cur = array->ordinality()-1; return isValid(); }
    bool        next(void)              { ++cur; return isValid(); }
    bool        prev(void)              { --cur; return isValid(); }
    bool        select(aindex_t seek)   { cur = seek; return isValid(); }

    IEsdlDefObjectIterator* queryBaseTypesIterator() { assertex(isValid()); return array->item(cur).queryBaseTypesIterator(); }
    unsigned getFlags()         { return array->item(cur).getFlags(); }


};

class EsdlDefMethodArrayIterator : public CInterface, implements IEsdlDefMethodIterator
{
private:
      EsdlDefObjectArray & array;
      aindex_t             cur;

public:
    IMPLEMENT_IINTERFACE;

    EsdlDefMethodArrayIterator(EsdlDefObjectArray &a) : array(a)
    {
    }

    bool        first(void)             { cur = 0; return isValid(); }
    bool        isValid(void)           { return array.isItem(cur); }
    IEsdlDefMethod &query()                { assertex(isValid()); return static_cast<IEsdlDefMethod&>(array.item(cur)); }
    bool        hasNext(void)           { return array.isItem(cur+1); }
    bool        hasPrev(void)           { return array.isItem(cur-1); }
    bool        last(void)              { cur = array.ordinality()-1; return isValid(); }
    bool        next(void)              { ++cur; return isValid(); }
    bool        prev(void)              { --cur; return isValid(); }
    bool        select(aindex_t seek)   { cur = seek; return isValid(); }
};

class EsdlDefObject : public CInterface
{
protected:
    Owned<IProperties> props;

public:
    IMPLEMENT_IINTERFACE;

    EsdlDefObject(StartTag &tag, EsdlDefinition *esdl);

    ~EsdlDefObject()
    {
    }

    const char *queryProp(const char *pname){return props->queryProp(pname);}
    bool hasProp(const char*pname) { return props->hasProp(pname); }
    int getPropInt(const char *pname, int def=0){return props->getPropInt(pname,def);}
    const char *queryName(){return queryProp("name");}

    virtual IPropertyIterator* getProps()
    {
        return props->getIterator();
    }

    virtual void toXMLAttributes(StringBuffer &xml)
    {
        Owned<IPropertyIterator> it = props->getIterator();
        ForEach(*it)
        {
            const char *name = it->getPropKey();
            const char *val = props->queryProp(name);
            xml.appendf(" %s=\"", name);
            encodeUtf8XML(val, xml);
            xml.append('\"');
        }
    }

    virtual bool checkFLVersion(double ver)
    {
        const char *deprstr = props->queryProp("depr_flver");
        if (deprstr)
            if (ver >= atof(deprstr))
                return false;

        const char *maxstr = props->queryProp("max_flver");
        if (maxstr)
            if (ver > atof(maxstr))
                return false;

        const char *minstr = props->queryProp("min_flver");
        if (minstr)
            if (ver < atof(minstr))
                return false;

        return true;
    }

    virtual bool checkVersion(double ver)
    {
        if (ver >= 0.0f)
        {
            const char *deprstr = props->queryProp("depr_ver");
            if (deprstr)
                if (ver >= atof(deprstr))
                    return false;

            const char *maxstr = props->queryProp("max_ver");
            if (maxstr)
                if (ver > atof(maxstr))
                    return false;

            const char *minstr = props->queryProp("min_ver");
            if (minstr){
                float minFloat = (float)atof(minstr);
                // I need to assign the value of the atof() call to a float variable
                // because otherwise the if statement will return true when the float
                // and double are equal (eg 1.3f < 1.3)
                // I haven't been able to figure out the precise reason for this behavior
                // but I've been able to confirm it and reproduce it on Windows even in a
                // simple default 'windows console' project.

                //minFloat least significant value is rounded up, this comparison fails if ver = minfloat
                if (ver < minFloat)
                    return false;
            }
        }

        return true;
    }

    virtual bool checkOptional( IProperties *opts )
    {
        // paramOptional==NULL means we dont' want to filter by
        // 'optional' tag value defined in the EsdlDef. This ensures
        // default behavior doesn't change- it won't filter unless paramOptional
        // is explicitly set to a non-null value.
        // For paramOptional of no value we should pass in an empty string ""

        const char* defOptional = props->queryProp("optional");

        // If and only if the ESDL definition has an optional attribute set then it
        // must match the optional parameter value passed in (optVal).
        // If it doesn't match then this element will not be 'output'

        // An example of this is those elements marked 'optional("internal")'. You
        // must provide a parameter on the URL '?internal' to see those elements
        // in the WSDL or web form.

        if( opts && defOptional )
        {
            if( opts->hasProp(defOptional) )
            {
                return true;
            } else {
                return false;
            }
        }

        return true;
    }
};


IProperties *createPropSelectors(const char *defstr, const char *transstr)
{
    IProperties *defaults = NULL;
    if (defstr)
    {
        defaults = createProperties(false);
        const char *finger=defstr;
        while (*finger)
        {
            StringBuffer name;
            StringBuffer vals;
            StringBuffer negvals;

            while (*finger && !strchr(":=", *finger))
                name.append(*finger++);

            if (*finger)
                finger++;

            bool neg = false;
            while (true)
            {
                if (!*finger || *finger==';')
                    break;

                if (*finger =='-')
                    neg = true;

                if (*finger == ',')
                {
                    if (neg)
                        negvals.append(',');
                    else
                        vals.append(',');
                    neg = false;

                    finger++;
                    continue;
                }

                if (neg)
                {
                    negvals.append(*finger++);
                }
                else
                    vals.append(*finger++);
            }

            if (*finger)
                finger++;

            defaults->setProp(name.str(), vals.length()>0? vals.str() : negvals.str());
        }
        if (transstr)
        {
            finger=transstr;
            while (*finger)
            {
                StringBuffer name;
                StringBuffer vals;
                while (*finger && !strchr(":=", *finger))
                    name.append(*finger++);
                if (*finger)
                    finger++;
                while (*finger && *finger!=';')
                    vals.append(*finger++);
                if (*finger)
                    finger++;

                if (defaults->hasProp(name))
                {

                    StringBuffer origprop(defaults->queryProp(name));
                    StringBuffer addtoprop;

                    const char *valsfinger=vals.str();

                    StringBuffer original;
                    StringBuffer alias;
                    while (*valsfinger)
                    {
                        while (*valsfinger && *valsfinger!=',')
                        {
                            original.clear();
                            alias.clear();

                            while (*valsfinger && *valsfinger!='=')
                                alias.append(*valsfinger++);
                            if(*valsfinger)
                                valsfinger++;
                            while (*valsfinger && *valsfinger!=',')
                                original.append(*valsfinger++);
                            if (*valsfinger)
                                valsfinger++;

                                StringBuffer tmp;
                                const char * pairfinger = origprop.str();

                                while (*pairfinger)
                                {
                                    tmp.clear();
                                    while (*pairfinger && *pairfinger!=',')
                                        tmp.append(*pairfinger++);

                                    if(!strcmp(original, tmp.str()))
                                        addtoprop.appendf(",%s", alias.str());

                                    if (*pairfinger)
                                        pairfinger++;
                                }
                        }
                    }
                    if (addtoprop.length() > 0)
                    {
                        defaults->removeProp(name);
                        defaults->setProp(name, origprop.append(addtoprop.str()).str());
                    }
                }
            }
        }
    }
    return defaults;
}

IPropertyTree *createPropPtdSelectors(const char *selstr)
{
    IPropertyTree *selectors = NULL;
    if (selstr)
    {
        selectors = createPTree();

        const char *finger=selstr;
        while (*finger)
        {
            StringBuffer name;
            StringBuffer vals;

            while (*finger && !strchr(":=", *finger))
                name.append(*finger++);
            if (*finger)
                finger++;
            while (*finger && *finger!=';')
                vals.append(*finger++);
            if (*finger)
                finger++;

            const char *valsfinger=vals.str();

            StringBuffer val;
            while (*valsfinger)
            {
                val.clear();
                while (*valsfinger && *valsfinger!=',')
                    val.append(*valsfinger++);

                selectors->addProp(name.str(), val.str());

                if (*valsfinger)
                valsfinger++;

            }

            //selectors->setProp(name.str(), vals.str());
        }
    }
    return selectors;
}


class EsdlDefElement : public EsdlDefObject, implements IEsdlDefElement
{
private:
    Owned<IProperties> recSelectors;
    Owned<IPropertyTree> ptdSelectors;

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefElement(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl){}

    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeElement;}

    void load(XmlPullParser *xpp, StartTag &struct_tag)
    {
        xpp->skipSubTree();
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags )
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlElement ");
                toXMLAttributes(xml);
                xml.append("/>");
            }
        }
    }

    IProperties *queryRecSelectors()
    {
        if (!recSelectors)
        {
            const char *defstr = queryProp("selectors");
            const char *trans = queryProp("translations");
            if (defstr && *defstr)
                recSelectors.setown(createPropSelectors(defstr, trans));
        }

        return recSelectors.get();
    }

    IPropertyTree *queryPtdSelectors()
    {
        if (!ptdSelectors)
        {
            const char *sel = queryProp("ptdselectors");
            if (sel && *sel)
                ptdSelectors.setown(createPropPtdSelectors(sel));
        }

        return ptdSelectors.get();
    }
};

class EsdlDefArray : public EsdlDefObject, implements IEsdlDefArray
{
private:
    Owned<IProperties> recSelectors;

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefArray(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl){}
    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeArray;}

    void load(XmlPullParser *xpp, StartTag &struct_tag)
    {
        xpp->skipSubTree();
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags)
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlArray ");
                toXMLAttributes(xml);
                xml.append("/>");
            }
        }
    }

    IProperties *queryRecSelectors()
    {
        if (!recSelectors)
        {
            const char *defstr = queryProp("selectors");
            const char *trans = queryProp("translations");

            if (defstr && *defstr)
                recSelectors.setown(createPropSelectors(defstr, trans));
        }

        return recSelectors.get();
    }
};

class EsdlDefAttribute : public EsdlDefObject, implements IEsdlDefAttribute
{
private:

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefAttribute(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl){}
    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeAttribute;}

    void load(XmlPullParser *xpp, StartTag &struct_tag)
    {
        xpp->skipSubTree();
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags)
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlAttribute ");
                toXMLAttributes(xml);
                xml.append("/>");
            }
        }
    }
};

class EsdlDefVersion : public EsdlDefObject, implements IEsdlDefVersion
{
private:

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefVersion(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl){}
    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeVersion;}


    void load(XmlPullParser *xpp, StartTag &struct_tag)
    {
        xpp->skipSubTree();
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags)
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlVersion ");
                toXMLAttributes(xml);
                xml.append("/>");
            }
        }
    }
};

class EsdlDefEnumRef : public EsdlDefObject, implements IEsdlDefEnumRef
{
private:

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefEnumRef(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl){}
    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeEnumRef;}


    void load(XmlPullParser *xpp, StartTag &struct_tag)
    {
        xpp->skipSubTree();
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags )
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlEnum ");
                toXMLAttributes(xml);
                xml.append("/>");
            }
        }
    }
};

class EsdlDefEnumDefItem : public EsdlDefObject, implements IEsdlDefEnumDefItem
{
private:

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefEnumDefItem(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl){}
    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeEnumDefItem;}


    void load(XmlPullParser *xpp, StartTag &struct_tag)
    {
        xpp->skipSubTree();
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags )
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlEnumItem ");
                toXMLAttributes(xml);
                xml.append("/>");
            }
        }
    }
};

class EsdlDefEnumDef : public EsdlDefObject, implements IEsdlDefEnumDef
{
private:
    EsdlDefObjectArray children;

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefEnumDef(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl){}

    ~EsdlDefEnumDef()
    {
        ForEachItemIn(x, children)
        {
            IEsdlDefObject &obj = children.item(x);
            EsdlDefObject *edo = dynamic_cast<EsdlDefObject *>(&obj);
            if( edo )
            {
                edo->Release();
            }
        }
    }

    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeEnumDef;}


    void loadEnumDefItem(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &tag)
    {
        EsdlDefEnumDefItem *ei = new EsdlDefEnumDefItem(tag, esdl);
        children.append(*dynamic_cast<IEsdlDefObject*>(ei));
        ei->load(xpp, tag);
    }

    void load(EsdlDefinition* def, XmlPullParser *xpp, StartTag &struct_tag)
    {
        int level = 1;
        StartTag stag;
        EndTag etag;

        while(level>0)
        {
            int type = xpp->next();

            switch(type)
            {
            case XmlPullParser::START_TAG:
                {
                    xpp->readStartTag(stag);
                    const char *localname = stag.getLocalName();
                    if(!stricmp(localname, "EsdlEnumItem"))
                        loadEnumDefItem(def, xpp, stag);
                    else
                        xpp->skipSubTree();
                    break;
                }
            case XmlPullParser::END_TAG:
                --level;
                break;
            }
        }
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags )
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlEnumType ");
                toXMLAttributes(xml);
                xml.append(">");

                //serialize child EsdlEnumItems
                ForEachItemIn(x, children)
                {
                    IEsdlDefObject &obj = children.item(x);
                    obj.toXML(xml, version, opts);
                }

                xml.append("</EsdlEnumType>");
            }
        }
    }
};


class EsdlDefStruct : public EsdlDefObject, implements IEsdlDefStruct
{
private:
    EsdlDefObjectArray children;
    EsdlDefTypeId type;

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefStruct(StartTag &tag, EsdlDefinition *esdl, EsdlDefTypeId type_) : EsdlDefObject(tag, esdl), type(type_){}

    ~EsdlDefStruct()
    {
        ForEachItemIn(x, children)
        {
            IEsdlDefObject &obj = children.item(x);
            EsdlDefObject *edo = dynamic_cast<EsdlDefObject *>(&obj);
            if (edo)
                edo->Release(); //ForceRelease();
        }
    }

    virtual EsdlDefTypeId getEsdlType(){return type;}
    virtual IEsdlDefObjectIterator *getChildren(){return new EsdlDefObjectArrayIterator(children);}
    virtual IEsdlDefObject *queryChild(const char *name, bool nocase)
    {
        ForEachItemIn(x, children)
        {
            IEsdlDefObject &obj = children.item(x);
            if ((nocase && strieq(obj.queryName(), name)) || streq(obj.queryName(), name))
                return &obj;
        }
        return NULL;
    }



    void load(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &struct_tag);


    void loadElement(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &tag)
    {
        //DBGLOG("   Loading Element %s", method_tag.getValue("name"));
        EsdlDefElement *et = new EsdlDefElement(tag, esdl);
        children.append(*dynamic_cast<IEsdlDefObject*>(et));
        et->load(xpp, tag);
    }

    void loadArray(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &tag)
    {
        EsdlDefArray *art = new EsdlDefArray(tag, esdl);
        children.append(*dynamic_cast<IEsdlDefObject*>(art));
        art->load(xpp, tag);
    }

    void loadAttribute(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &tag)
    {
        EsdlDefAttribute *at = new EsdlDefAttribute(tag, esdl);
        children.append(*dynamic_cast<IEsdlDefObject*>(at));
        at->load(xpp, tag);
    }

    void loadEnumRef(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &tag)
    {
        EsdlDefEnumRef *er = new EsdlDefEnumRef(tag, esdl);
        children.append(*dynamic_cast<IEsdlDefObject*>(er));
        er->load(xpp,tag);
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags )
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                // When the DEPFLAG_COLLAPSE flag is set then we're serializing structures
                // in the 'collapsed' format, meaning all ancestors' elements are included
                // directly into the child. In those cases we want to omit the opening tag
                // attributes an closing tag because this output is going to be collapsed
                // into a child structure.
                if( !(flags & DEPFLAG_COLLAPSE) )
                {
                    switch (type)
                    {
                    case EsdlTypeStruct:
                        xml.append("<EsdlStruct");
                        break;
                    case EsdlTypeRequest:
                        xml.append("<EsdlRequest");
                        break;
                    case EsdlTypeResponse:
                        xml.append("<EsdlResponse");
                        break;
                    default: // EsdlTypeElement, EsdlTypeAttribute, EsdlTypeArray etc
                        break;
                    }

                    toXMLAttributes(xml);
                    xml.append(">");
                }

                ForEachItemIn(x, children)
                {
                    IEsdlDefObject &obj = children.item(x);
                    obj.toXML(xml, version, opts);
                }

                if( !(flags & DEPFLAG_COLLAPSE) )
                {
                    switch (type)
                    {
                    case EsdlTypeStruct:
                        xml.append("</EsdlStruct>");
                        break;
                    case EsdlTypeRequest:
                        xml.append("</EsdlRequest>");
                        break;
                    case EsdlTypeResponse:
                        xml.append("</EsdlResponse>");
                        break;
                    default:
                        break;
                    }
                }
            }
        }
    }

};


void EsdlDefStruct::load(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &struct_tag)
{
    int level = 1;
    StartTag stag;
    EndTag etag;

    while(level>0)
    {
        int type = xpp->next();

        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                xpp->readStartTag(stag);
                const char *localname = stag.getLocalName();
                if(!stricmp(localname, "EsdlElement"))
                    loadElement(esdl, xpp, stag);
                else if(!stricmp(localname, "EsdlEnum"))
                    loadEnumRef(esdl, xpp, stag);
                else if(!stricmp(localname, "EsdlArray"))
                    loadArray(esdl, xpp, stag);
                else if(!stricmp(localname, "EsdlAttribute"))
                    loadAttribute(esdl, xpp, stag);
                else
                    xpp->skipSubTree();
                break;
            }
            case XmlPullParser::END_TAG:
                --level;
                break;
        }
    }
}



class EsdlDefMethod : public EsdlDefObject, implements IEsdlDefMethod
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;


    EsdlDefMethod(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl)
    {
        const char *product = queryProp("productAssociation");
        if (product && *product)
        {
            if (strstr(product, ":default"))
            {
                StringBuffer val;
                const char * finger = product;
                while(*finger && *finger !=':'){val.append(*finger++);}

                props->setProp("product_", val.str());
                props->setProp("productdefault_", true);
            }
            else
                props->setProp("product_", product);
        }

    }
    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeMethod;}

    void load(XmlPullParser *xpp, StartTag &struct_tag)
    {
        xpp->skipSubTree();
    }

    virtual const char *queryRequestType(){return queryProp("request_type");}
    virtual const char *queryResponseType(){return queryProp("response_type");}
    virtual const char *queryProductAssociation(){return queryProp("product_");}
    /*{
        String prod(queryProp("productAssociation"));
        if (prod.length() > 0)
        {
            if(prod.endsWith(":default"))
                return prod.substring(0, prod.indexOf(":"))->str();
            return prod.str();
        }
        else
            return NULL;
    }*/
    virtual bool isProductDefault(){return queryProp("productdefault_") ? true : false;}
    /*{
        const char * prod = queryProp("productAssociation");
        return (prod && strstr(prod, ":default")==0) ? true : false;
    }*/
    virtual const char *queryLogMethodName(){return queryProp("logMethod");}
    virtual const char *queryMethodFLReqFormat(){return queryProp("flReqFormat");}
    virtual const char *queryMethodFLRespFormat(){return queryProp("flRespFormat");}
    virtual const char *queryMethodName(){return queryProp("name");}
    virtual const char *queryMbsiProductAssociations(){return queryProp("mbsiProductAssociations");}

    virtual int queryVariableLengthRecordProcessing() {
        return getPropInt("variableRecordLengthProcessing");
    }

    virtual int queryAllowMultipleEntryPerUnitNumber() {
        return getPropInt("allowMultipleEntryPerUnitNumber");
    }

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags )
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlMethod");
                toXMLAttributes(xml);
                xml.append("/>");
            }
        }
    }


};

class EsdlDefService : public EsdlDefObject, implements IEsdlDefService
{
public:
    EsdlDefObjectArray children;
    AddedObjs methodsByName;
    AddedObjs methodsByReqname;

public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ESDL_DEFOBJ;

    EsdlDefService(StartTag &tag, EsdlDefinition *esdl) : EsdlDefObject(tag, esdl){}

    ~EsdlDefService()
    {
        methodsByName.kill();
        methodsByReqname.kill();
        ForEachItemIn(x, children)
        {
            IEsdlDefObject &obj = children.item(x);
            EsdlDefObject *edo = dynamic_cast<EsdlDefObject *>(&obj);
            if (edo)
                edo->Release(); //ForceRelease();
        }
    }

    virtual EsdlDefTypeId getEsdlType(){return EsdlTypeService;}

    void load(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &struct_tag);

    void loadMethod(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &tag)
    {
        EsdlDefMethod *mt = new EsdlDefMethod(tag, esdl);
        children.append(*dynamic_cast<IEsdlDefObject*>(mt));
        mt->load(xpp, tag);

        const char *reqname = mt->queryRequestType();
        StringBuffer namestr(reqname);
        if (reqname)
            methodsByReqname.setValue(namestr.toLowerCase().str(), static_cast<IEsdlDefObject*>(mt));

        namestr.clear().append(mt->queryName());
        methodsByName.setValue(namestr.toLowerCase().str(), static_cast<IEsdlDefObject*>(mt));
    }

    virtual IEsdlDefMethodIterator *getMethods()
    {
        // Currently the children array contains only method objects - all others are filtered
        // out when the EsdlDefinition is loaded

        return new EsdlDefMethodArrayIterator( children );
    }

    virtual void methodsNamesToXML(StringBuffer& xml, const char* ver, IProperties* opts);

    virtual void toXML(StringBuffer &xml, double version, IProperties *opts, unsigned flags )
    {
        if( version == 0 || this->checkVersion(version) )
        {
            if( this->checkOptional(opts) )
            {
                xml.append("<EsdlService");
                toXMLAttributes(xml);
                xml.append(">");
                ForEachItemIn(x, children)
                {
                    IEsdlDefObject &obj = children.item(x);
                    obj.toXML(xml, version, opts);
                }
                xml.append("</EsdlService>");
            }
        }
    }

    IEsdlDefMethod *queryMethodByName(const char *name)
    {
        StringBuffer strname(name);
        IEsdlDefObject **obj = methodsByName.getValue(strname.toLowerCase().str());
        return (obj) ? dynamic_cast<IEsdlDefMethod*>(*obj) : NULL;
    }

    IEsdlDefMethod *queryMethodByRequest(const char *reqname)
    {
        StringBuffer strname(reqname);
        IEsdlDefObject **obj = methodsByReqname.getValue(strname.toLowerCase().str());
        return (obj) ? dynamic_cast<IEsdlDefMethod*>(*obj) : NULL;
    }

};

typedef CopyReferenceArrayOf<IEsdlDefService> ESDLServiceArray;


void EsdlDefService::load(EsdlDefinition *esdl, XmlPullParser *xpp, StartTag &struct_tag)
{
    int level = 1;
    StartTag stag;
    EndTag etag;

    while(level>0)
    {
        int type = xpp->next();

        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                xpp->readStartTag(stag);
                // Note that if we ever add anything other than an EsdlMethod to the
                // 'EsdlDefObjectArray children' member, then we'll need to change
                // the implementation of EsdlDefService::getMethods
                if(!stricmp(stag.getLocalName(), "EsdlMethod"))
                    loadMethod(esdl, xpp, stag);
                else
                    xpp->skipSubTree();
                break;
            }
            case XmlPullParser::END_TAG:
                --level;
                break;
        }
    }
}

void EsdlDefService::methodsNamesToXML(StringBuffer& xml, const char* ver, IProperties* opts)
{
    IEsdlDefMethodIterator* methodIter = NULL;

    xml.appendf("<Service name=\"%s\">\n", queryName());
    xml.append("       <methods>\n");
    methodIter = this->getMethods();

    IEsdlDefinition *defobj = queryEsdlDefinition();

    for( methodIter->first(); methodIter->isValid(); methodIter->next() )
    {
        IEsdlDefMethod& methodObj = methodIter->query();
        const char* methodName = methodObj.queryMethodName();
        if(methodName)
        {
           double fver = 9999.0;
           if(ver)
               fver = atof(ver);

           xml.append("            <method>\n");
           xml.appendf("                  <name>%s</name>\n", methodName);

           const char* minver = methodObj.queryProp("min_ver");
           if(minver)
           {
               double fminver =  atof(minver);
               if(fminver <= fver)
               {
                   IEsdlDefStruct *defstruct = defobj->queryStruct(minver);
                   const char *vminver = 0;
                   if(defstruct)
                   {
                       vminver = defstruct->queryProp("version");//translate versiondef
                   }
                   if(vminver)
                       xml.appendf("                  <minver>%s</minver>\n", vminver);
                   else
                       xml.appendf("                  <minver>%s</minver>\n", minver);
               }
           }
           //"optional" can only have one value, either "internal" or "indayton" or "fcra"
           const char* setOpt = methodObj.queryProp("optional");
           if(setOpt)
           {
                bool hasopt = opts->hasProp(setOpt);
                if(hasopt)
                {
                    xml.appendf("                  <optional>%s</optional>\n", setOpt);
                }
           }
        }
        xml.append("            </method>\n");
    }
    xml.append("      </methods>\n");
    xml.append("</Service>");
}

//static int file_counter = 0;

class EsdlDefFile : public CInterface, implements IEsdlDefFile
{
public:
    StringAttr name;
    IArrayOf<IEsdlDefObject> objs;
    StringArray includes;

public:
    IMPLEMENT_IINTERFACE;

    EsdlDefFile(const char *name_) : name(name_) {}//file_counter++;}

    ~EsdlDefFile()
    {
    //    file_counter--;
        //ForEachItemIn(y, objs)
        //{
        //    IEsdlDefObject &obj = objs.item(y);
        //    obj.Release();
        //}
    }

    IEsdlDefObjectIterator *getChildren(){return NULL;}

    void saveXML(StringBuffer &xml)
    {
        xml.appendf("<esxdl name=\"%s\">\n", name.get());
        ForEachItemIn(x, includes)
        {
            const char *incname = includes.item(x);
            xml.appendf("<EsdlInclude file=\"%s\"/>\n", incname);
        }
        ForEachItemIn(y, objs)
        {
            IEsdlDefObject &obj = objs.item(y);
            obj.toXML(xml);
        }

        xml.append("</esxdl>");

    }

};

typedef MapStringTo<bool> AddedHash;

class EsdlDefinition : public CInterface, implements IEsdlDefinition
{
public:
    AddedHash added;
    AddedObjs objs;

    ESDLServiceArray services;
    Owned<IProperties> verdefs;

    Owned<IProperties> optionals;

    Owned<IPropertyTree> flConfig;

public:
    IMPLEMENT_IINTERFACE;

    EsdlDefinition()
    {
        verdefs.setown(createProperties(true));
        optionals.setown(createProperties(true));
    }

    ~EsdlDefinition()
    {
        ForEachItemIn(idxsrv, services)
        {
            IEsdlDefService *isrv = &services.item(idxsrv);
            EsdlDefService *osrv = dynamic_cast<EsdlDefService*>(isrv);
            if (osrv)
                osrv->Release(); //ForceRelease();
        }

        HashIterator oit(objs);
        for (oit.first(); oit.isValid(); oit.next())
        {
            IMapping& et = oit.query();
            IEsdlDefObject **pobj = objs.mapToValue(&et);
            if (pobj && *pobj)
            {
                EsdlDefObject *edo = dynamic_cast<EsdlDefObject *>(*pobj);
                if (edo)
                    edo->Release(); //ForceRelease();
            }
        }
    }

    void addDefinitionFromXML(const StringBuffer & xmlDef, const char * esdlDefName, int ver);
    void addDefinitionFromXML(const StringBuffer & xmlDef, const char * esdlDefId);
    void addDefinitionsFromFile(const char *filename);
    bool hasFileLoaded(const char *filename);
    bool hasXMLDefintionLoaded(const char *esdlDefName, int ver);
    bool hasXMLDefintionLoaded(const char *esdlDefId);
    EsdlBasicElementType translateSimpleType(const char *type)
    {
        return esdlSimpleType(type);
    }


    IEsdlDefObject *queryObj(const char *name)
    {
        IEsdlDefObject **obj = objs.getValue(name);
        return (obj) ? *obj : NULL;
    }
    IEsdlDefStruct *queryStruct(const char *name)
    {
        IEsdlDefObject **obj = objs.getValue(name);
        return (obj) ? dynamic_cast<IEsdlDefStruct*>(*obj) : NULL;
    }

    IEsdlDefService *queryService(const char *name)
    {
        if (name && *name)
        {
            ForEachItemIn(idx, services)
            {
                IEsdlDefService &srv = services.item(idx);
                if (!stricmp(srv.queryName(), name))
                    return &srv;
            }
        }
        return NULL;
    }

    void addOptionalName(const char *name)
    {
        if (!optionals->hasProp(name))
            optionals->setProp(name, "");
    }

    virtual IProperties *queryOptionals()
    {
        return optionals.get();
    }


    IEsdlDefFileIterator *getFiles(){return NULL;}

    virtual void setFlConfig(IPropertyTree* p){flConfig.setown(p);}
    virtual IPropertyTree* getFlConfig(){return flConfig.get();}

    // To support the calculation and return of just the dependent elements for a given service/method(s).
    virtual IEsdlDefObjectIterator* getDependencies( const char* service, const char* method, double requestedVersion, IProperties *opts, unsigned flags=0 );
    virtual IEsdlDefObjectIterator* getDependencies( const char* service, StringArray &methods, double requestedVersion, IProperties *opts, unsigned flags=0 );
    virtual IEsdlDefObjectIterator* getDependencies( const char* service, const char* delimethodlist, const char* delim, double requestedVer, IProperties *opts, unsigned flags );

    void gatherMethodDependencies( EsdlDefObjectWrapperArray& dependencies, EsdlDefObjectArray& methods, double requestedVer, IProperties *opts, unsigned flags=0 );
    void walkDefinitionDepthFirst( AddedObjs& foundByName, EsdlDefObjectWrapperArray& dependencies, IEsdlDefObject* esdlObj, double requestedVer, IProperties *opts, int level=0, unsigned flags=0, unsigned state=0);
    unsigned walkChildrenDepthFirst( AddedObjs& foundByName, EsdlDefObjectWrapperArray& dependencies, IEsdlDefObject* esdlObj, double requestedVer, IProperties *opts, int level=0, unsigned flags=0);
    bool shouldGenerateArrayOf( IEsdlDefObject& child, unsigned flags, unsigned& stateOut );
    bool setWrapperFlagStringArray( AddedObjs& foundByName, EsdlDefObjectWrapper* wrapper, unsigned state );
};

bool EsdlDefinition::shouldGenerateArrayOf( IEsdlDefObject& child, unsigned flags, unsigned& stateOut )
{
    bool result = false;

    // Assumes the child object is an EsdlArray element.
    // If it has no 'item_tag' attribute, but has a 'type' attribute then
    // we must generate an 'ArrayOf...' defn for that type.

    if( flags & DEPFLAG_ARRAYOF)
    {
        const char* item_tag = child.queryProp("item_tag");
        const char* type = child.queryProp("type");

        if( item_tag == NULL && type && *type )
        {
            result = true;
            stateOut |= DEPFLAG_ARRAYOF;

            if( stricmp(type, "string")==0 )
            {
                stateOut |= DEPFLAG_STRINGARRAY;
            }
        }
    }

    return result;
}

bool EsdlDefinition::setWrapperFlagStringArray( AddedObjs& foundByName, EsdlDefObjectWrapper* wrapper, unsigned state )
{
    bool result = false;

    if( state & DEPFLAG_STRINGARRAY )
    {
        IMapping* foundStringArray = foundByName.find( "EspStringArray" );
        if( foundStringArray == NULL )
        {
            foundByName.setValue("EspStringArray", NULL);
            wrapper->setFlag(DEPFLAG_STRINGARRAY);
            result = true;
        }
    }

    return result;
}

IEsdlDefObjectIterator* EsdlDefinition::getDependencies( const char* service, const char* delimethodlist, const char* delim, double requestedVer, IProperties *opts, unsigned flags )
{
    StringArray methods;
    if(delimethodlist && *delimethodlist)
    {
        methods.appendList(delimethodlist, delim);
    }

    return getDependencies( service, methods, requestedVer, opts, flags );
}

IEsdlDefObjectIterator* EsdlDefinition::getDependencies( const char* service, const char* method, double requestedVer, IProperties *opts, unsigned flags )
{
    StringArray methods;
    StringBuffer m( method );
    if(method && *method)
    {
        methods.append( m );
    }

    return getDependencies( service, methods, requestedVer, opts, flags );
}

IEsdlDefObjectIterator* EsdlDefinition::getDependencies( const char* service, StringArray &methods, double requestedVer, IProperties *opts, unsigned flags)
{
    IEsdlDefService* serviceDef;
    IEsdlDefMethod* methodDef;
    EsdlDefObjectArray serviceArray;
    EsdlDefObjectArray methodArray;
    EsdlDefObjectWrapperArray* dependencies = new EsdlDefObjectWrapperArray();
    Owned<IEsdlDefMethodIterator> methodIter;

    // I think it's just a namespace/xml_tag/structure name naming difference
    // but be sure to know where these structures listed in the schema are
    // coming from and how they're defined:
    // Exceptions, ArrayOfEspException, EspException
    TimeSection ts("EsdlDefinition::getDependencies");
    if(service && *service)
    {
        serviceDef = this->queryService(service);

        if( NULL == serviceDef )
        {
            throw( MakeStringException(0, "ESDL Service Definition not found for %s", service) );
        }
    }
    else
    {
        throw( MakeStringException(0, "No ESDL Service Definition provided, need to have a service to search in for methods") );
    }

    if( methods.length() < 1 )
    {
        methodIter.setown(serviceDef->getMethods());
        for( methodIter->first(); methodIter->isValid(); methodIter->next() )
        {
            methodArray.append( methodIter->query() );
        }
    }
    else
    {
        ForEachItemIn( i, methods )
        {
            methodDef = serviceDef->queryMethodByName(methods.item(i));

            if( NULL == methodDef )
            {
                throw( MakeStringException(0, "ESDL Method Definition not found for %s in service %s", methods.item(i), service) );
            }

            methodArray.append( *methodDef );
        }
    }

    this->gatherMethodDependencies( *dependencies, methodArray, requestedVer, opts, flags );

    // Put the highest level objects on last- the services
    // Tony did say that these will be useful
    if( serviceDef != NULL )
    {
        EsdlDefObjectWrapper* wrapper = new EsdlDefObjectWrapper( *serviceDef );
        dependencies->append( *wrapper );
    }

    return new OwnedEsdlDefObjectArrayIterator(dependencies);
}

void EsdlDefinition::gatherMethodDependencies( EsdlDefObjectWrapperArray& dependencies, EsdlDefObjectArray& methods, double requestedVer, IProperties *opts, unsigned flags )
{
    AddedObjs foundByName;

    // At this point all the Methods we want to walk are in this array-
    // either explicitly passed in to the array, or added by walking the
    // service definitions passed in and adding all of the services' methods

    // Q:
    // What circumstances do we want to throw an unhandled exception
    // vs. those that we may just want to log or DBGLOG a warning?
    int numMethods = methods.ordinality();
    for( int i = 0; i<numMethods; i++ )
    {
        IEsdlDefObject* methodObj = &(methods.item(i));
        IEsdlDefMethod* method = dynamic_cast<IEsdlDefMethod *>(methodObj);

        if( methodObj->checkOptional(opts) && (requestedVer==0.0 || method->checkVersion(requestedVer)) )
        {
            const char* request = method->queryRequestType();
            IEsdlDefObject* requestObj = this->queryObj( request );
            if( NULL == requestObj )
            {
                throw( MakeStringException(0, "Request struct %s not found in ESDL Definition", request) );
            } else {
                this->walkDefinitionDepthFirst( foundByName, dependencies, requestObj, requestedVer, opts, 0, flags );
            }

            const char* response = method->queryResponseType();
            IEsdlDefObject* responseObj = this->queryObj( response );
            if( NULL == responseObj )
            {
                throw( MakeStringException(0, "Response struct %s not found in ESDL Definition", response) );
            } else {
                this->walkDefinitionDepthFirst( foundByName, dependencies, responseObj, requestedVer, opts, 0, flags );
            }

            EsdlDefObjectWrapper* wrapper = new EsdlDefObjectWrapper( *methodObj );
            dependencies.append( *wrapper );
        }
    }
}

unsigned EsdlDefinition::walkChildrenDepthFirst( AddedObjs& foundByName, EsdlDefObjectWrapperArray& dependencies, IEsdlDefObject* esdlObj, double requestedVer, IProperties *opts, int level, unsigned flags)
{
    IEsdlDefStruct* esdlStruct = dynamic_cast<IEsdlDefStruct *>(esdlObj);
    IEsdlDefObject* composedObject;
    bool genArrayOf = false;
    unsigned stateOut = 0;

    //DBGLOG("%s<%s> walking children", StringBuffer(level*2, " ").str(), esdlObj->queryName() );
    if( esdlStruct != NULL )
    {
        Owned<IEsdlDefObjectIterator> children = esdlStruct->getChildren();

        if( children != NULL )
        {
            children->first();
            level++;
            while( children->isValid() )
            {
                IEsdlDefObject& child = children->query();
                const char *childname = child.queryName();
                EsdlDefTypeId childType = child.getEsdlType();
                //DBGLOG("  %s<%s> child", StringBuffer(level*2, " ").str(), childname );
                unsigned childState = 0;

                const char* complexType = NULL;
                if( childType == EsdlTypeElement || childType == EsdlTypeArray || childType == EsdlTypeEnumRef)
                {
                    // Arrays and Elements both can be composed of complex types
                    // Unfortunately, the Esdl property name is different in each case
                    switch( childType)
                    {
                        case EsdlTypeElement:
                            complexType = child.queryProp("complex_type");
                            break;

                        case EsdlTypeArray:
                            complexType = child.queryProp("type");
                            this->shouldGenerateArrayOf( child, flags, childState );
                            stateOut |= childState;
                            break;

                        case EsdlTypeEnumRef:
                            complexType = child.queryProp("enum_type");
                            break;
                        default:
                            break;
                    }

                    // Should this element be included based on ESP version & optional tags?
                    if( child.checkOptional(opts) && ( requestedVer==0.0 || child.checkVersion(requestedVer)) )
                    {
                        if( complexType != NULL )
                        {
                            composedObject = this->queryObj(complexType);

                            // Because Arrays could be composed of a simple type, we may get NULL for
                            // composedObject in that situation. So, only continue walking if we get
                            // a valid composed object, and dont' throw an exception if we don't.
                            if( composedObject != NULL )
                            {
                                this->walkDefinitionDepthFirst( foundByName, dependencies, composedObject, requestedVer, opts, level, flags, childState );
                            }
                        }
                    }
                }   //else { DBGLOG(" -- <%s> is not Element or Array", child.queryName() ); }
                children->next();
            }
        } //else { DBGLOG("%s<%s> no Children", StringBuffer(level*2, " ").str(), esdlObj->queryName()); }
    }  //else { DBGLOG("%s<%s> is not Struct",  StringBuffer(level*2, " ").str(), esdlObj->queryName()); }

    return stateOut;
}

void EsdlDefinition::walkDefinitionDepthFirst( AddedObjs& foundByName, EsdlDefObjectWrapperArray& dependencies, IEsdlDefObject* esdlObj, double requestedVer, IProperties *opts, int level, unsigned flags, unsigned state)
{
    // Perform a depth-first walk of the structures.
    //
    // This is not a general-purpose solution for walking the EsdlDefintion
    // it's tailored to the specific need of gathering all structures that
    // the 'esdlObj' root structure parameter is dependent upon.
    // If more need for EsdlDefinition traversal comes up it would be
    // beneficial to provide it with a more standard 'tree' interface.

    // The traversal is performed:
    // First in terms of the inheritence hierarchy
    //   (eg AddressEx inherits from Address)
    // Then in terms of the hierarchy of composed structures
    //   (eg InstantIDSearchBy contains an Address)

    //StringBuffer indent;
    //indent.appendN( level*2, ' ' );
    //DBGLOG( "%s<%s>", indent.str(), esdlObj->queryName() );

    IEsdlDefObject* baseObject;

    EsdlDefTypeId esdlType = esdlObj->getEsdlType();
    const char* name = esdlObj->queryName();
    IMapping* found = foundByName.find( name );

    // If the current esdlObject has already been added to the list of dependencies,
    // then just exit now - there is no need to traverse it's children or it's
    // ancestors- they've already been added.
    // Checking up front here will also allow us to detect cycles in the strcuture
    // graph -such as we have with BpsReportRelative- and avoid an infinite loop
    if( found || esdlObj->checkOptional(opts) == false || esdlObj->checkVersion(requestedVer) == false )
    {
        //DBGLOG("%s</%s><Skipped/>", indent.str(), esdlObj->queryName());
        return;
    }

    if( esdlType == EsdlTypeRequest ||
        esdlType == EsdlTypeResponse ||
        esdlType == EsdlTypeStruct ||
        esdlType == EsdlTypeEnumDef ||
        esdlType == EsdlTypeEnumRef)
    {
        // The current structure is a complex type not seen yet,
        // so add it to the list of found strucutres. Allows us to avoid cycles.
        foundByName.setValue( name, esdlObj );

        EsdlDefObjectWrapper* wrapper = new EsdlDefObjectWrapper( *esdlObj );

        // First check to see if the current esdlObj is inherited from a Base Type.
        // If so, descend the inheritance hierarchy first. This is to preserve
        // the inheritence order in output as per Tony's request.

        // Skip this inheritence check for Enums because they are only made of simple types
        const char* baseType = esdlObj->queryProp("base_type");
        if( baseType != NULL && esdlType != EsdlTypeEnumDef )
        {
            // If DEPFLAG_COLLAPSE is set the we want to filter out from the dependencies list all
            // EsdlStruct/Request/Response objects which are only used as base_types by other objects.
            // Instead, their elements are to be 'collapsed' into the structure defn of the child.
            // In that case we populate the wrapper for the current esdlDefObject with all of it's
            // ancestor base_types. Then during serialization and transformation we have the ancestor
            // objects required to collapse into this current esdlDefObject.

            baseObject = this->queryObj( baseType );
            if( NULL == baseObject )
            {
                wrapper->Release();
                throw( MakeStringException(0, "ESDL base type defintion %s not found for %s", baseType, esdlObj->queryName()) );
            }

            if( flags & DEPFLAG_COLLAPSE )
            {
                while( baseObject )
                {
                    wrapper->appendBaseType(baseObject);

                    // This will walk all the children of baseObject but it won't add baseObject
                    // itself in the list of dependencies. If baseObject is in fact used directly
                    // as an EsdlElement in some other structure, then it will be added to the
                    // dependency list

                    // If a child includes an EspStringArray, then walkChildrenDepthFirst will return
                    // a state flags with the DEPFLAG_STRINGARRAY set. This indicates that the parent
                    // object's wrapper should be decorated with the DEPFLAG_STRINGARRAY. That will be
                    // an indication to the esdl_def_helper to add an espStringArray='1' attribute to
                    // the paren't objects XML output. That attribute will trigger the stylesheet to
                    // generate a standalone EspStringArray structure defn. However, only one should
                    // be generated for the first occurrence, so we'll keep track of wether or not one
                    // has been added yet by adding to the AddedObj mapping with the 'EspStringArray' name.
                    unsigned stateOut = this->walkChildrenDepthFirst( foundByName, dependencies, baseObject, requestedVer, opts, level, flags );

                    if( (flags & DEPFLAG_ARRAYOF) && (state & DEPFLAG_ARRAYOF) )
                    {
                        const char* baseObjectName = baseObject->queryName();
                        found = foundByName.find( baseObjectName );
                        if( found == NULL )
                        {
                            EsdlDefObjectWrapper* wrapper = new EsdlDefObjectWrapper( *baseObject );
                            wrapper->setFlag(DEPFLAG_ARRAYOF);

                            this->setWrapperFlagStringArray( foundByName, wrapper, stateOut );

                            dependencies.append(*wrapper);
                        }
                    }

                    baseType = baseObject->queryProp("base_type");
                    baseObject = NULL;
                    if( baseType && *baseType )
                    {
                        baseObject = this->queryObj( baseType );
                        if( NULL == baseObject )
                        {
                            wrapper->Release();
                            throw( MakeStringException(0, "ESDL base type defintion %s not found for %s", baseType, esdlObj->queryName()) );
                        }
                    }
                }

                wrapper->setFlag( DEPFLAG_COLLAPSE );

            } else {

                // By recursively calling walkDefinition here, we're adding the baseObject
                // (and all it's ancestors) to the dependencies list, regardless if this baseObject is
                // explicitly referenced as the 'type' attribute of another included EsdlElement.

                //DBGLOG("Into base -><%s><%s>", esdlObj->queryName(), baseObject->queryName() );
                this->walkDefinitionDepthFirst(foundByName, dependencies, baseObject, requestedVer, opts, ++level, flags);
                //DBGLOG("Out of base <-<%s><%s>", esdlObj->queryName(), baseObject->queryName() );
            }
        }

        // We've gotten to the bottom of the inheritance hierarchy
        // Now walk the child elements of esdlObj and where it's a complex_type,

        unsigned stateOut = this->walkChildrenDepthFirst( foundByName, dependencies, esdlObj, requestedVer, opts, level, flags );

        // Once we get here it is either because esdlStruct had no children or
        // we have  processed (recursively) all the children for esdlStruct above
        // If esdlStruct is not already added to dependencies, add it.

        this->setWrapperFlagStringArray( foundByName, wrapper, stateOut );
        if( (flags & DEPFLAG_ARRAYOF) && (state & DEPFLAG_ARRAYOF) )
        {
            wrapper->setFlag(DEPFLAG_ARRAYOF);
        }

        dependencies.append( *wrapper );
        //DBGLOG("%s</%s><Added/>", indent.str(), esdlObj->queryName());
    }
}


class EsdlDefLoader : public CInterface
{
public:
    EsdlDefinition *def;

public:
    IMPLEMENT_IINTERFACE;

    EsdlDefLoader(EsdlDefinition *_def) : def(_def) {}

    void loadMethod(XmlPullParser *xpp, StartTag &method_tag)
    {
        EsdlDefMethod *obj= new EsdlDefMethod(method_tag, def);
        def->objs.setValue(obj->queryName(), static_cast<IEsdlDefObject*>(obj));
        obj->load(xpp, method_tag);
    }


    EsdlDefStruct *loadStruct(XmlPullParser *xpp, StartTag &tag, EsdlDefTypeId tid)
    {
        //DBGLOG("Loading Struct %s", struct_tag.getValue("name"));
        EsdlDefStruct *obj= new EsdlDefStruct(tag, def, tid);
        IEsdlDefObject*iobj = dynamic_cast<IEsdlDefObject*>(obj);

        def->objs.setValue(obj->queryName(), iobj);
        obj->load(def, xpp, tag);

        return obj;
    }

    EsdlDefService *loadService(XmlPullParser *xpp, StartTag &tag)
    {
        //DBGLOG("Loading Service %s", service_tag.getValue("name"));
        EsdlDefService *st = new EsdlDefService(tag, def);
        def->services.append(*dynamic_cast<IEsdlDefService*>(st));
        st->load(def, xpp, tag);
        return st;
    }

    EsdlDefVersion *loadVersion(XmlPullParser *xpp, StartTag &tag)
    {
        EsdlDefVersion *ver = new EsdlDefVersion(tag, def);
        ver->load(xpp, tag);
        const char *vername = ver->queryProp("name");
        const char *verval =  ver->queryProp("version");

        if (vername && *vername && verval && *verval)
            def->verdefs->setProp(vername, verval);
        return ver;
    }

    EsdlDefEnumDef *loadEnumDef(XmlPullParser *xpp, StartTag &tag)
    {
        EsdlDefEnumDef *enumDef = new EsdlDefEnumDef( tag, def );
        def->objs.setValue(enumDef->queryName(), static_cast<IEsdlDefObject*>(enumDef));
        enumDef->load(def, xpp, tag);
        return enumDef;
    }

    bool loadXMLDefinitionFrombuffer(const StringBuffer & xmlDef, const char * path="", int indent = 0)
    {
        bool esxdlFound = false;

        if (xmlDef.length())
        {
            auto_ptr<XmlPullParser> xpp(new XmlPullParser());

            int bufSize = xmlDef.length();
            xpp->setSupportNamespaces(true);
            xpp->setInput(xmlDef.str(), bufSize);

            int type;
            StartTag stag;
            EndTag etag;

            while((type = xpp->next()) != XmlPullParser::END_DOCUMENT)
            {
                if(type == XmlPullParser::START_TAG)
                {
                    xpp->readStartTag(stag);
                    if(stricmp(stag.getLocalName(), "esxdl")==0)
                    {
                        esxdlFound = true;
                        while((type = xpp->next()) != XmlPullParser::END_DOCUMENT)
                        {
                            if(type == XmlPullParser::START_TAG)
                            {
                                xpp->readStartTag(stag);
                                if(!stricmp(stag.getLocalName(), "EsdlInclude"))
                                {
                                    const char *incname = stag.getValue("file");
                                    loadfile(path, incname, indent+3);
                                    xpp->skipSubTree();
                                }
                                else if(!stricmp(stag.getLocalName(), "EsdlStruct"))
                                    loadStruct(xpp.get(), stag, EsdlTypeStruct);
                                else if(!stricmp(stag.getLocalName(), "EsdlRequest"))
                                    loadStruct(xpp.get(), stag, EsdlTypeRequest);
                                else if(!stricmp(stag.getLocalName(), "EsdlResponse"))
                                    loadStruct(xpp.get(), stag, EsdlTypeResponse);
                                else if(!stricmp(stag.getLocalName(), "EsdlService"))
                                    loadService(xpp.get(), stag);
                                else if(!stricmp(stag.getLocalName(), "EsdlVersion"))
                                    loadVersion(xpp.get(), stag);
                                else if(!stricmp(stag.getLocalName(), "EsdlEnumType"))
                                    loadEnumDef(xpp.get(), stag);
                            }
                        }
                    }
                }
            }
        }
        return esxdlFound;
    }

    void loadfile(const char *path, const char *filename, int indent)
    {
        if (filename && *filename)
        {
            if (def->added.getValue(filename) == false)
            {
                StringBuffer dbgstr;
                dbgstr.appendN(indent, ' ').append("Adding: ").append(filename);
                DBGLOG("%s", dbgstr.str());

                def->added.setValue(filename, true);

                StringBuffer fullname(path);
                if (fullname.length() && !strchr("\\/", fullname.charAt(fullname.length()-1)))
                    fullname.append('/');
                fullname.append(filename);
                fullname.append(".xml");

                //for now just load into StringBuffer
                StringBuffer buf;
                buf.loadFile(fullname);

                loadXMLDefinitionFrombuffer(buf, path, indent);
            }
            else
                DBGLOG("Already loaded %s", filename);
        }
        else
            throw MakeStringException(-1, "Could not load file, name not available");
    }

};

bool EsdlDefinition::hasFileLoaded(const char *filename)
{
    //The existance of the entry indicates whether it has been added or not
    return added.getValue(filename) != NULL;
}
bool EsdlDefinition::hasXMLDefintionLoaded(const char *esdlDefName, int ver)
{
    if (!esdlDefName || !*esdlDefName)
        return false;
    if (ver <= 0)
        return false;

    VStringBuffer id("%s.%d", esdlDefName, ver);
    return hasXMLDefintionLoaded(id.str());
}

bool EsdlDefinition::hasXMLDefintionLoaded(const char *esdlDefId)
{
    //The existance of the entry indicates whether it has been added or not
    return added.getValue(esdlDefId) != NULL;
}

void EsdlDefinition::addDefinitionsFromFile(const char *filename)
{
    DBGLOG("Loading ESDL files: %s", filename);
    TimeSection ts("adding ESDL File");

    StringBuffer path;
    StringBuffer name;

    splitFilename(filename, NULL, &path, &name, NULL);

    EsdlDefLoader loader(this);
    loader.loadfile(path.str(), name.str(), 0);
}

void EsdlDefinition::addDefinitionFromXML(const StringBuffer & xmlDef, const char * esdlDefId)
{
    if (!esdlDefId || !*esdlDefId)
        DBGLOG("XML ESDL definition cannot be loaded with out definition ID.");

    if (added.getValue(esdlDefId) == false)
    {
        DBGLOG("Loading XML ESDL definition: %s",esdlDefId);
        TimeSection ts("adding XML ESDL definition");

        EsdlDefLoader loader(this);
        if (loader.loadXMLDefinitionFrombuffer(xmlDef))
            added.setValue(esdlDefId, true);
        else
            throw MakeStringException(-1, "Could not load XML ESDL def: %s", xmlDef.str());
    }
    else
        DBGLOG("XML ESDL definition: %s has already been loaded!", esdlDefId);
}

void EsdlDefinition::addDefinitionFromXML(const StringBuffer & xmlDef, const char * esdlDefName, int ver)
{
    if (!esdlDefName || !*esdlDefName)
    {
        DBGLOG("XML ESDL definition cannot be loaded with out definition name.");
        return;
    }

    if (ver <= 0 )
    {
        DBGLOG("XML ESDL definition cannot be loaded with out valid definition version.");
        return;
    }

    VStringBuffer esdlDefId("%s.%d", esdlDefName, ver);
    addDefinitionFromXML(xmlDef, esdlDefName);
}

EsdlDefObject::EsdlDefObject(StartTag &tag, EsdlDefinition *esdl)
{
    IProperties *verdefs = (esdl) ? esdl->verdefs.get() : NULL;

    props.setown(createProperties(true));
    int count = tag.getLength();
    while (count--)
    {
        const char * localname = tag.getLocalName(count);
        const char * value = tag.getValue(count);
        //if (localname && *localname && value && *value)

        // There are cases where an EnumRef has a default value of "" (@default="")
        // In order to correctly re-serialze such an element we can't exclude
        // props here just because the value may be an empty string.

        if (localname && *localname && value)
        {
            if ((strieq(localname, "min_ver") || strieq(localname, "max_ver") || strieq(localname, "depr_ver")) && (*value<'0' || *value>'9'))
            {
                const char *verdefval=(verdefs) ? verdefs->queryProp(value) : NULL;
                if (verdefval && *verdefval)
                    props->setProp(localname, verdefval);
                else
                    throw MakeStringException(-1, "Error! EsdlDefVersion %s not found", value);
            }
            else if (strieq(localname, "optional"))
            {
                esdl->addOptionalName(value);
                props->setProp(localname, value);
            }
            else
                props->setProp(localname, value);
        }
    }
}


static Owned<IEsdlDefinition> default_ns;

typedef IEsdlDefinition * IEsdlDefinitionPtr;

typedef MapStringTo<IEsdlDefinitionPtr> EsdlNamespaceMap;

EsdlNamespaceMap esdl_namespaces;

esdl_decl IEsdlDefinition *createEsdlDefinition(const char *esdl_ns)
{
    if (esdl_ns && *esdl_ns)
    {
        IEsdlDefinition **ptns = esdl_namespaces.getValue(esdl_ns);
        if (!ptns)
        {
            IEsdlDefinition *tns = new EsdlDefinition();
            esdl_namespaces.setValue(esdl_ns, tns);
            return LINK<IEsdlDefinition>(tns);
        }
        else
            return LINK<IEsdlDefinition>(*ptns);
    }
    else if (!default_ns)
        default_ns.setown(new EsdlDefinition());
    return default_ns.getLink();
}

esdl_decl IEsdlDefinition *createNewEsdlDefinition(const char *esdl_ns)
{
    if (esdl_ns && *esdl_ns)
        return createEsdlDefinition(esdl_ns);
    else
        return new EsdlDefinition();
}

esdl_decl IEsdlDefinition *queryEsdlDefinition(const char *esdl_ns)
{
    if (esdl_ns && *esdl_ns)
    {
        IEsdlDefinition **ptns = esdl_namespaces.getValue(esdl_ns);
        return (ptns) ? *ptns : NULL;
    }

    return default_ns.get();
}

esdl_decl void releaseEsdlDefinition(const char *esdl_ns)
{
    if (esdl_ns && *esdl_ns)
    {
        IEsdlDefinition **ptns = esdl_namespaces.getValue(esdl_ns);
        if (ptns && *ptns && (*ptns)->Release())
            esdl_namespaces.remove(esdl_ns);
    }
    else
        default_ns.clear();
}

static bool type_list_inited = false;

typedef MapStringTo<EsdlBasicElementType> EsdlTypeList;
static EsdlTypeList esdlTypeList;

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
        list.setValue("unsigned", ESDLT_UINT32);
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
        list.setValue("unsigned8", ESDLT_UINT64);

        type_list_inited=true;
    }
}

esdl_decl void initEsdlTypeList()
{
    if (!type_list_inited)
        init_type_list(esdlTypeList);
}

esdl_decl EsdlBasicElementType esdlSimpleType(const char *type)
{
    if (!type || !*type)
        return ESDLT_STRING;
    initEsdlTypeList();
    EsdlBasicElementType *val = esdlTypeList.getValue(type);
    if (val)
        return *val;
    return ESDLT_COMPLEX;
}
