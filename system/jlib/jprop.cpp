/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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


#include "jiface.hpp"
#include "jprop.hpp"
#include "jhash.hpp"
#include "jhash.ipp"
#include "jexcept.hpp"
#include "jiter.ipp"
#include "jregexp.hpp"
#include "jmisc.hpp"
#include <stdio.h>

const char *conv2char_ptr(const char *p) { return p; }
const char *convchar_ptr2(const char *p) { return p; }
const char *tokvchar_ptr(const void *p) { return (const char *)p; }


template <class PTYPE, class PITER>
class PropertyIteratorOf : implements PITER, public CInterface
{
protected:
    HashIterator *piter;
    const HashTable &properties;

public:
    IMPLEMENT_IINTERFACE; 

    PropertyIteratorOf(const HashTable &_properties) : properties(_properties)
    {
        properties.Link();
        piter = new HashIterator(properties);
    }
    ~PropertyIteratorOf()
    {
        properties.Release();
        piter->Release();
    }
    virtual bool first()
    {
        return piter->first();
    }
    virtual bool next()
    {
        return piter->next();
    }
    virtual bool isValid()
    {
        return piter->isValid();
    }
    virtual PTYPE getPropKey() = 0;
};

typedef IPropertyIterator char_ptrIPropertyIterator;
class char_ptrPropertyIterator : public PropertyIteratorOf<const char *, char_ptrIPropertyIterator>
{
public:
    char_ptrPropertyIterator(const HashTable &_properties) : PropertyIteratorOf<const char *, char_ptrIPropertyIterator>(_properties) { }
    virtual const char *getPropKey()
    {
        IMapping &cur = piter->query();
        const char *key = (const char *) (cur.getKey());
        return key;
    }
};

template <class PTYPE, class MAPPING, class IPROP, class IPROPITER, class PROPITER>
class CPropertiesBase : implements IPROP, public CInterface
{
private:
    MAPPING properties;

public:
    IMPLEMENT_IINTERFACE;

    CPropertiesBase(bool nocase) : properties(nocase)
    {
    }
    virtual ~CPropertiesBase()
    {
        properties.kill();
    }
    void loadProp(const char *finger)
    {
        StringBuffer prop, val;
        while (*finger && *finger != '=')
            prop.append(*finger++);
        if (*finger)
        {
            finger++;
            while (isspace(*finger))
                finger++;
            while (*finger)
                val.append(*finger++);
            prop.clip();
            val.clip();
            if (prop.length())
                setProp(toPType(prop.str()), val.str());
        }
    }
    void loadProp(const char *finger, int dft)
    {
        if (strchr(finger, '='))
            loadProp(finger);
        else
            setProp(toPType(finger), dft);
    }
    void loadProp(const char *finger, bool dft)
    {
        if (strchr(finger, '='))
            loadProp(finger);
        else
            setProp(toPType(finger), dft);
    }
    void loadProp(const char *finger, const char * dft)
    {
        if (strchr(finger, '='))
            loadProp(finger);
        else
            setProp(toPType(finger), dft);
    }
    void loadSystem()
    {
        char **e = getSystemEnv();
        while (*e)
        {
            loadProp (*e);
            e++;
        }
    }
    void loadFile(const char *filename)
    {
        FILE *inFile = fopen(filename, "r" TEXT_TRANS);
        if (inFile)
        {
            StringBuffer sbuff; 
            char buf[1024];
            while (fgets(buf,1024,inFile))
            {
                sbuff.clear();
                do // handle lines longer than 1024 (bit kludgy)
                {
                    size32_t l=(size32_t)strlen(buf);
                    if (!l || (buf[l-1]=='\n')) // should end in \n unless overflowed
                        break;
                    sbuff.append(buf);
                } while (fgets(buf,1024,inFile));

                const char *s=buf;
                if (sbuff.length())
                    s = sbuff.append(buf).str();

                if (*s == '#')
                    continue;
                char *comment = (char *) strstr(s, "#");
                if (comment != NULL)
                    *comment = 0;
                loadProp(s);
                sbuff.setLength(0);
            }
            fclose(inFile);
        }
    }
    void loadProps(const char *finger)
    {
        while (*finger)
        {
            StringBuffer prop, val;
            while (*finger && *finger != '\n' && *finger != '=')
                prop.append(*finger++);
            if (*finger && *finger != '\n')
            {
                finger++;
                while (isspace(*finger) && *finger != '\n')
                    finger++;
                while (*finger && *finger != '\n')
                    val.append(*finger++);
                prop.clip();
                val.clip();
                if (prop.length())
                    setProp(toPType(prop.str()), val.str());
            }
            if (*finger)
                finger++;
        }
    }
    virtual PTYPE toPType(const char *p) const = 0;
    virtual const char *fromPType(PTYPE p) const = 0;
    virtual PTYPE toKeyVal(const void *) const = 0;
    virtual int getPropInt(PTYPE propname, int dft) const override
    {
        if (propname)
        {
            const char *val = queryProp(propname);
            if (val)
                return (int)_atoi64(val);
        }
        return dft;
    }
    virtual bool getPropBool(PTYPE propname, bool dft) const override
    {
        if (propname)
        {
            const char *val = queryProp(propname);
            if (val&&*val)
                return strToBool(val);
        }
        return dft;
    }
    virtual bool getProp(PTYPE propname, StringBuffer &ret) const override
    {
        if (propname)
        {
            StringAttr * match = properties.getValue(propname);
            if (match)
            {
                ret.append(*match);
                return true;
            }
        }
        return false;
    }
    virtual const char *queryProp(PTYPE propname) const override
    {
        if (propname)
        {
            StringAttr * match = properties.getValue(propname);
            if (match)
                return(*match);
        }
        return NULL;
    }
    virtual void saveFile(const char *filename) const override
    {
        FILE *outFile = fopen(filename, "w" TEXT_TRANS);
        if (outFile)
        {
            HashIterator it(properties);
            StringBuffer line;

            for (it.first();it.isValid();it.next())
            {
                IMapping &cur = it.query();
                PTYPE key = toKeyVal(cur.getKey());
                StringAttr &attr = * (StringAttr *) properties.getValue(key);
                line.clear().append(key).append('=').append(attr).newline();
                fwrite(line.str(),line.length(),1,outFile);
            }
            fclose(outFile);
        }
    }
    virtual void setProp(PTYPE propname, int val)
    {
        char buf[15];
        sprintf(buf, "%d", val);
        setProp(propname, buf);
    }
    virtual void setProp(PTYPE propname, const char *val)
    {
        if (propname)
        {
            if (val)
                properties.setValue(propname, val);
            else
                properties.remove(propname);
        }
    }
    virtual void appendProp(PTYPE propname, const char *val)
    {
        if (propname && val)
        {
            StringAttr * mapping = properties.getValue(propname);
            if (mapping)
            {
                if (*val)
                {
                    char * str = mapping->detach();
                    size_t len1 = strlen(str);
                    size_t len2 = strlen(val);
                    char * newstr = (char *)realloc(str, len1 + len2+1);
                    assertex(newstr);
                    memcpy(newstr+len1, val, len2);
                    newstr[len1+len2] = '\0';
                    mapping->setown(newstr);
                }
            }
            else
                properties.setValue(propname, val);
        }
    }
    virtual bool removeProp(PTYPE propname)
    {
        if (propname)
            return properties.remove(propname);
        return false;
    }
    virtual bool hasProp(PTYPE propname) const override
    {
        if (propname)
        {
            StringAttr * match = properties.getValue(propname);
            if (match)
                return true;
        }
        return false;
    }

// serializable impl.
    virtual void serialize(MemoryBuffer &tgt)
    {
        HashIterator it(properties);

        tgt.append((unsigned) properties.count());
        it.first();
        while (it.isValid())
        {
            IMapping &cur = it.query();
            PTYPE key = toKeyVal(cur.getKey());
            StringAttr &attr = * (StringAttr *) properties.getValue(key);
            char_ptr cp = fromPType(key);
            tgt.append((size32_t) strlen(cp)+1);
            tgt.append(cp);
            tgt.append(attr);
            it.next();
        }
    }
    virtual void deserialize(MemoryBuffer &src)
    {
        unsigned count;
        src.read(count);
        for (; count>0; count--)
        {
            char *key;
            StringAttr value;
            size32_t sz;
            src.read(sz);
            key = (char *) alloca(sz);
            src.read(sz, key);
            src.read(value);
            PTYPE ptype = toPType(key);
            setProp(ptype, value);
        }
    }
    virtual IPROPITER *getIterator() const override
    {
        return new PROPITER(properties);
    }
};

#define MAKECPropertyOf(PHTYPE, PTYPE, MAPPING, PCLASS) \
class PCLASS : public CPropertiesBase<PTYPE, MAPPING, PTYPE##IProperties, PTYPE##IPropertyIterator, PTYPE##PropertyIterator>                            \
{                                                                                                       \
public:                                                                                                 \
    PCLASS(const char *filename, bool nocase) : CPropertiesBase<PTYPE, MAPPING, PTYPE##IProperties, PTYPE##IPropertyIterator, PTYPE##PropertyIterator>(nocase) { loadFile(filename); } \
    PCLASS(bool nocase) : CPropertiesBase<PTYPE, MAPPING, PTYPE##IProperties, PTYPE##IPropertyIterator, PTYPE##PropertyIterator>(nocase)    { }         \
    virtual PTYPE toPType(const char *p) const override { return conv2##PTYPE(p); }                                    \
    virtual const char *fromPType(PTYPE p) const override { return conv##PTYPE##2(p); }                                \
    virtual PTYPE toKeyVal(const void *p) const override { return tokv##PTYPE(p); }                                \
};
typedef IProperties char_ptrIProperties;
MAKECPropertyOf(Atom, char_ptr, StringAttrMapping, CProperties);

extern jlib_decl IProperties *createProperties(bool nocase)
{
    return new CProperties(nocase);
}
extern jlib_decl IProperties *createProperties(const char *filename, bool nocase)
{
    if (filename)
        return new CProperties(filename, nocase);
    else
        return new CProperties(nocase);
}
static CProperties *sysProps = NULL;

extern jlib_decl IProperties *querySystemProperties()
{
    if (!sysProps)
    {
        sysProps = new CProperties(false);
        sysProps->loadSystem();
    }
    return sysProps;
}

extern jlib_decl IProperties *getSystemProperties()
{
    IProperties *p = querySystemProperties();
    p->Link();
    return p;
}

MODULE_INIT(INIT_PRIORITY_JPROP)
{
    return true;
}

MODULE_EXIT()
{
    ::Release(sysProps);
}
