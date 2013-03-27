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


#include <stdio.h>

#include "jarray.hpp"
#include "jdebug.hpp"
#include "jhash.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "jlzw.hpp"
#include "jregexp.hpp"
#include "jstring.hpp"

#include <algorithm>

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #undef new
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif


#define MAKE_LSTRING(name,src,length) \
    const char *name = (const char *) alloca((length)+1); \
    memcpy((char *) name, (src), (length)); \
    *(char *) (name+(length)) = '\0';

#include "jfile.hpp"
#include "jlog.hpp"
#include "jptree.ipp"

#define WARNLEGACYCOMPARE
#define XMLTAG_CONTENT "<>"

#undef UNIMPLEMENTED
#define UNIMPLEMENTED throw MakeIPTException(-1, "UNIMPLEMENTED")
#define CHECK_ATTRIBUTE(X) if (X && isAttribute(X)) throw MakeIPTException(PTreeExcpt_XPath_Unsupported, "Attribute usage invalid here");
#define AMBIGUOUS_PATH(X,P) { StringBuffer buf; buf.append(X": ambiguous xpath \"").append(P).append("\"");  throw MakeIPTException(PTreeExcpt_XPath_Ambiguity,buf.str()); }

#define PTREE_COMPRESS_THRESHOLD (4*1024)    // i.e. only use compress if > threshold
#define PTREE_COMPRESS_BOTHER_PECENTAGE (80) // i.e. if it doesn't compress to <80 % of original size don't bother


class NullPTreeIterator : public CInterface, implements IPropertyTreeIterator
{
public:
    IMPLEMENT_IINTERFACE;
// IPropertyTreeIterator
    virtual bool first() { return false; }
    virtual bool next() { return false; }
    virtual bool isValid() { return false; }
    virtual IPropertyTree & query() { assertex(false); return *(IPropertyTree *)NULL; }
} *nullPTreeIterator;

IPropertyTreeIterator *createNullPTreeIterator() { return LINK(nullPTreeIterator); } // initialize in init mod below.

//===================================================================

struct AttrStrC: public AttrStr
{

    static inline unsigned getHash(const char *k)
    {
        return hashc((const byte *)k,strlen(k),17);
    }

    inline bool eq(const char *k)
    {
        return strcmp(k,str)==0;
    }


    static AttrStrC *create(const char *k)
    {
        size32_t kl = (k?strlen(k):0);
        AttrStrC *ret = (AttrStrC *)malloc(sizeof(AttrStrC)+kl);
        memcpy(ret->str,k,kl);
        ret->str[kl] = 0;
        ret->hash = hashc((const byte *)k,kl,17);
        ret->linkcount = 0;
        return ret;
    }

    static void destroy(AttrStrC *a) 
    {
        free(a);
    }
};

struct AttrStrNC: public AttrStr
{
    static inline unsigned getHash(const char *k)
    {
        return hashnc((const byte *)k,strlen(k),17);
    }

    inline bool eq(const char *k)
    {
        return stricmp(k,str)==0;
    }


    static AttrStrNC *create(const char *k)
    {
        size32_t kl = (k?strlen(k):0);
        AttrStrNC *ret = (AttrStrNC *)malloc(sizeof(AttrStrNC)+kl);
        memcpy(ret->str,k,kl);
        ret->str[kl] = 0;
        ret->hash = hashnc((const byte *)k,kl,17);
        ret->linkcount = 0;
        return ret;
    }

    static void destroy(AttrStrNC *a) 
    {
        free(a);
    }

};

class CAttrValHashTable
{
    CMinHashTable<AttrStrC>  htc;
    CMinHashTable<AttrStrNC> htnc;
    CMinHashTable<AttrStrC>  htv;

public:
    inline AttrStr *addkey(const char *v,bool nc)
    {

        AttrStr * ret;
        if (nc)
            ret = htnc.find(v,true);
        else
            ret = htc.find(v,true);
        if (ret->linkcount!=(unsigned short)-1)
            ret->linkcount++;
        return ret;
    }

    inline AttrStr *addval(const char *v)
    {

        AttrStr * ret = htv.find(v,true);
        if (ret->linkcount!=(unsigned short)-1)
            ret->linkcount++;
        return ret;
    }

    inline void removekey(AttrStr *a,bool nc)
    {
        if (a->linkcount!=(unsigned short)-1)
            if (--(a->linkcount)==0) 
                if (nc)
                    htnc.remove((AttrStrNC *)a);
                else
                    htc.remove((AttrStrC *)a);
    }

    inline void removeval(AttrStr *a)
    {
        if (a->linkcount!=(unsigned short)-1)
            if (--(a->linkcount)==0) 
                htv.remove((AttrStrC *)a);
    }

};

//===================================================================
static CriticalSection hashcrit;
static AtomRefTable *keyTable, *keyTableNC;
static CAttrValHashTable *attrHT=NULL;
AttrValue **AttrMap::freelist=NULL; 
unsigned AttrMap::freelistmax=0; 
CLargeMemoryAllocator AttrMap::freeallocator((size32_t)-1,0x1000*sizeof(AttrValue),true);


MODULE_INIT(INIT_PRIORITY_JPTREE)
{
    nullPTreeIterator = new NullPTreeIterator;
    keyTable = new AtomRefTable;
    keyTableNC = new AtomRefTable(true);
    attrHT = new CAttrValHashTable;
    return true;
}

MODULE_EXIT()
{
    nullPTreeIterator->Release();
    keyTable->Release();
    keyTableNC->Release();
    delete attrHT;
    AttrMap::killfreelist();
}


static int comparePropTrees(IInterface **ll, IInterface **rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    return stricmp(l->queryName(), r->queryName());
};

//////////////////

unsigned ChildMap::getHashFromElement(const void *e) const
{
    PTree &elem= (PTree &) (*(IPropertyTree *)e);
    return elem.queryKey()->queryHash();
}

unsigned ChildMap::numChildren()
{
    SuperHashIteratorOf<IPropertyTree> iter(*this);
    if (!iter.first()) return 0;
    unsigned count = 0;
    do
    {
        PTree *element = (PTree *) &iter.query();
        if (element->value && element->value->isArray())
            count += element->value->elements();
        else
            ++count;
    }
    while (iter.next());
    return count;
}

IPropertyTreeIterator *ChildMap::getIterator(bool sort)
{
    class CPTHashIterator : public CInterface, implements IPropertyTreeIterator
    {
        SuperHashIteratorOf<IPropertyTree> *hiter;
    public:
        IMPLEMENT_IINTERFACE;

        CPTHashIterator(SuperHashTable &table) { hiter = new SuperHashIteratorOf<IPropertyTree>(table); }
        ~CPTHashIterator() { hiter->Release(); }
    // IPropertyTreeIterator
        virtual bool first() { return hiter->first(); }
        virtual bool next() { return hiter->next(); }
        virtual bool isValid() { return hiter->isValid(); }
        virtual IPropertyTree & query() { return hiter->query(); }
    };
    class CPTArrayIterator : public ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>
    {
        IArrayOf<IPropertyTree> elems;
    public:
        CPTArrayIterator(IPropertyTreeIterator &iter) : ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>(elems)
        {
            ForEach(iter)
                elems.append(iter.get());
            elems.sort(comparePropTrees);
        }
    };
    IPropertyTreeIterator *baseIter = new CPTHashIterator(*this);
    if (!sort)
        return baseIter;
    IPropertyTreeIterator *it = new CPTArrayIterator(*baseIter);
    baseIter->Release();
    return it;
}

///////////

AttrValue *AttrMap::newArray(unsigned n)
{
    // NB crit must be locked
    if (!n)
        return NULL;
    if (freelistmax<=n) {
        freelist = (AttrValue **)realloc(freelist,sizeof(AttrValue *)*(n+1));
        while (freelistmax<=n)
            freelist[freelistmax++] = NULL;
    }
    AttrValue *&p = freelist[n];
    AttrValue *ret = p;
    if (ret) 
        p = *(AttrValue **)ret;
    else 
        ret = (AttrValue *)freeallocator.alloc(sizeof(AttrValue)*n);
    return ret;
}   

void AttrMap::freeArray(AttrValue *a,unsigned n)
{
    // NB crit must be locked
    if (a) {
        AttrValue *&p = freelist[n];
        *(AttrValue **)a = p;
        p = a;
    }
}


void AttrMap::set(const char *key, const char *val)
{
    if (!key)
        return;
    if (!val)
        val = "";  // cannot have NULL value
    AttrValue *a = attrs+count();
    AttrValue *v = NULL;
    bool nc = isNoCase();
    if (nc) {
        while (a--!=attrs) 
            if (stricmp(a->key->str,key)==0) {
                v = a;
                break;
            }
    }
    else {
        while (a--!=attrs) 
            if (strcmp(a->key->str,key)==0) {
                v = a;
                break;
            }
    }
    if (v) {
        if (strcmp(v->value->str,val)==0)
            return;
        CriticalBlock block(hashcrit);
        attrHT->removeval(v->value);
        v->value = attrHT->addval(val);
    }
    else {
        CriticalBlock block(hashcrit);
        unsigned n = count();
        AttrValue *newattrs = newArray(n+1);
        memcpy(newattrs,attrs,n*sizeof(AttrValue));
        newattrs[n].key = attrHT->addkey(key,nc);
        newattrs[n].value = attrHT->addval(val);
        numattrs++;
        freeArray(attrs,n);
        attrs = newattrs;       
    }
}

void AttrMap::kill()
{
    if (!attrs)
        return;
    CriticalBlock block(hashcrit);
    AttrValue *a = attrs+count();
    bool nc = isNoCase();
    while (a--!=attrs) {
        attrHT->removekey(a->key,nc);
        attrHT->removeval(a->value);
    }
    freeArray(attrs,count());
    attrs = NULL;
    numattrs &= AM_NOCASE_FLAG;     // clear num attrs
}


const char *AttrMap::find(const char *key) const
{
    AttrValue *a = attrs+count();
    if (isNoCase()) {
        while (a--!=attrs) 
            if (stricmp(a->key->str,key)==0)
                return a->value->str;
    }
    else {
        while (a--!=attrs) 
            if (strcmp(a->key->str,key)==0)
                return a->value->str;
    }
    return NULL;
}

bool AttrMap::remove(const char *key)
{
    unsigned n = count();
    AttrValue *a = attrs+n;
    AttrValue *del = NULL;
    if (isNoCase()) {
        while (a--!=attrs) 
            if (stricmp(a->key->str,key)==0) {
                del = a;
                break;
            }
    }
    else {
        while (a--!=attrs) 
            if (strcmp(a->key->str,key)==0) {
                del = a;
                break;
            }
    }
    if (!del)
        return false;
    CriticalBlock block(hashcrit);
    numattrs--;
    n--;
    AttrValue *newattrs = newArray(n);
    if (newattrs) {
        size32_t ls = (byte *)a-(byte *)attrs;
        memcpy(newattrs,attrs,ls);
        memcpy(((byte *)newattrs)+ls,((byte *)attrs)+ls+sizeof(AttrValue),n*sizeof(AttrValue)-ls);
    }
    freeArray(attrs,n+1);
    attrs = newattrs;       
    return true;
}


void AttrMap::swap(AttrMap &other)
{
    AttrValue *ta = attrs;  
    attrs = other.attrs;
    other.attrs = ta;
    unsigned short tn = numattrs;   
    numattrs = other.numattrs;
    other.numattrs = tn;
}


///////////

bool validateXMLTag(const char *name)
{
    if (!isValidXPathStartChr(*name)) return false;
    ++name;
    while (*name != '\0')
    {
        if (!isValidXPathChr(*name)) return false;
        ++name;
    }
    return true;
}

class jlib_thrown_decl CPTreeException : public CInterface, implements IPTreeException
{
    int errCode;
    StringBuffer errMsg;
public:
    IMPLEMENT_IINTERFACE;

    CPTreeException(int _errCode, const char *_errMsg, va_list &args) : errCode(_errCode)
    {
        if (_errMsg)
            errMsg.valist_appendf(_errMsg, args);
    }

    StringBuffer &translateCode(StringBuffer &out) const
    {
        out.append("IPropertyTree: ");
        switch (errCode)
        {
            case PTreeExcpt_XPath_Ambiguity:
                return out.append("Ambiguous xpath used");
            case PTreeExcpt_XPath_ParseError:
                return out.append("xpath parse error");
            case PTreeExcpt_XPath_Unsupported:
                return out.append("unsupported xpath syntax used");
            case PTreeExcpt_InvalidTagName:
                return out.append("Invalid tag name");
            default:
                return out.append("UNKNOWN ERROR CODE: ").append(errCode);
        }
    }

// IException
    int errorCode() const { return errCode; }
    StringBuffer &errorMessage(StringBuffer &out) const
    {
        return translateCode(out).append("\n").append(errMsg.str());
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }
};

IPTreeException *MakeIPTException(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IPTreeException *e = new CPTreeException(code, format, args);
    va_end(args);
    return e;
}

IPTreeException *MakeXPathException(const char *xpath, int code, unsigned pos, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    StringBuffer s("XPath Exception: ");
    s.valist_appendf(format, args);
    va_end(args);
#ifdef _DEBUG
    PrintStackReport();
#endif
    const char *msg = "in xpath = ";
    s.append("\n").append(msg).append(xpath);
    s.append("\n").appendN((size32_t)strlen(msg)+pos, ' ').append("^");
    return MakeIPTException(code, s.str());
}

inline static void readID(const char *&xxpath, bool started)
{
    const char *xpath = xxpath;
    if (isValidXPathStartChr(*xpath) || (started && isValidXPathChr(*xpath)))
    {
        do
        {
            xpath++;
        } while (isValidXPathChr(*xpath));
        xxpath = xpath;
    }
}

inline static void readWildId(const char *&xpath, bool &wild)
{
    const char *start = xpath;
    wild = false;
    loop
    {
        readID(xpath, wild);
        if ('*' != *xpath)
            break;
        wild = true;
        ++xpath;
    }
}

inline const char * readIndex(const char *xpath, StringAttr &index)
{
    const char *start = xpath;
    do { xpath++; } while (isdigit(*xpath));
    index.set(start, (xpath - start));
    return xpath;
}



inline static void readWildIdIndex(const char *&xpath, bool &wild)
{
    const char *_xpath = xpath;
    readWildId(xpath, wild);
    if ('[' == *xpath) // check for local index not iterative qualifier.
    {
        const char *end = xpath+1;
        if (isdigit(*end)) {
            StringAttr index;
            end = readIndex(end, index);
            if (']' != *end)
                throw MakeXPathException(_xpath, PTreeExcpt_XPath_ParseError, xpath-_xpath, "Qualifier brace unclosed");
            xpath = end+1;
        }
    }
}

inline static unsigned getTailIdLength(const char *xxpath, unsigned xxpathlength)
{
    const char *xpath = xxpath+xxpathlength;
    const char *end = xpath;
    while (xpath != xxpath)
    {
        --xpath;
        if (!isValidXPathChr(*xpath)) break;
    }

    if (!isAttribute(xpath) && xpath != xxpath) ++xpath;
    return end-xpath;
}

const char *splitXPathUQ(const char *xpath, StringBuffer &path)
{
    size32_t xpathSize = (size32_t) strlen(xpath);
    size32_t idSize = getTailIdLength(xpath, xpathSize);
    path.append(xpathSize-idSize, xpath);
    return xpath + (xpathSize-idSize);
}

const char *splitXPathX(const char *xpath)
{
    size32_t xpathSize = (size32_t) strlen(xpath);
    size32_t idSize = getTailIdLength(xpath, xpathSize);
    return xpath + (xpathSize-idSize);
}

// similar to above, splitXPathUQ doesn't split if qualified
const char *splitXPath(const char *xpath, StringBuffer &headPath)
{
    StringBuffer path;
    const char *end = xpath+strlen(xpath);
    const char *prop = end;
    bool quote = false;
    bool braced = false;
    while (xpath != prop)
    {
        --prop;
        if (*prop == '"')
        {
            if (quote) quote = false;
            else quote = true;
        }
        else if (*prop == ']' && !quote)
        {
            assertex(!braced);
            braced = true;
        }
        else if (*prop == '[' && !quote)
        {
            assertex(braced);
            braced = false;
        }
        else if (*prop == '/' && !quote && !braced)
        {
            ++prop;
            break;
        }
    }
    if (prop == end)
        return NULL;
    else if (xpath != prop)
    {
        size32_t ps = prop-xpath-1;
        headPath.append(ps, xpath);
    }
    return prop;
}

const char *queryNextUnquoted(const char *str, char c)
{
    const char *end = str+strlen(str);
    bool quote = false;
    while (end != str)
    {
        ++str;
        if ('"' == *str)
        {
            if (quote) quote = false;
            else quote = true;
        }
        else if (c == *str && !quote)
            break;
    }
    return str==end?NULL:str;
}

const char *queryHead(const char *xpath, StringBuffer &head)
{
    if (!xpath) return NULL;
    StringBuffer path;
    const char *start = xpath;
    const char *end = xpath+strlen(xpath);
    bool quote = false;
    bool braced = false;
    while (end != xpath)
    {
        ++xpath;
        if (*xpath == '"')
        {
            if (quote) quote = false;
            else quote = true;
        }
        else if (*xpath == ']' && !quote)
        {
            assertex(braced);
            braced = false;
        }
        else if (*xpath == '[' && !quote)
        {
            assertex(!braced);
            braced = true;
        }
        else if (*xpath == '/' && !quote && !braced)
        {
            if ('/' == *start) // so leading '//'
                return start;
            else if ('/' == *(xpath+1)) // in middle of path
            {
                head.append(xpath-start, start);
                return xpath;
            }
            break;
        }
    }
    if (xpath == end)
        return NULL;
    head.append(xpath-start, start);
    return xpath+1;
}

///////////////////

class SeriesPTIterator : public CInterface, implements IPropertyTreeIterator
{
public:
    IMPLEMENT_IINTERFACE;

    SeriesPTIterator() : current(NULL), cp(0)
    {
    }

    void addIterator(IPropertyTreeIterator *iter) { iters.append(*iter); }

// IPropertyTreeIterator impl.
    virtual bool first()
    {
        cp = 0;
        iterCount = iters.ordinality();
        if (nextIterator())
            return true;
        else
            return false;
    }

    virtual bool next()
    {
        while (currentIter)
        {
            if (currentIter->next())
            {
                current = &currentIter->query();
                return true;
            }
            if (nextIterator())
                return true;
        }
        current = NULL;
        return false;
    }

    virtual bool isValid() { return (NULL != current); }

    virtual IPropertyTree & query() { assertex(current); return *current; }

private:
    bool nextIterator()
    {
        while (cp<iterCount)
        {
            currentIter = (IPropertyTreeIterator *) &iters.item(cp++);
            if (currentIter->first())
            {
                current = &currentIter->query();
                return true;
            }
        }
        current = NULL;
        currentIter = NULL;
        return false;
    }

    Array iters;
    IPropertyTreeIterator *currentIter;
    IPropertyTree *current;
    unsigned cp, iterCount;
};

///////////////////

CPTValue::CPTValue(size32_t size, const void *data, bool binary, bool raw, bool _compressed)
{
    compressed = _compressed;
    if (!raw && binary && size > PTREE_COMPRESS_THRESHOLD)
    {
        unsigned newSize = size * PTREE_COMPRESS_BOTHER_PECENTAGE / 100;
        void *newData = NULL;
        ICompressor *compressor = NULL;
        try
        {
            newData = malloc(sizeof(size32_t) + newSize);
            compressor = createLZWCompressor();
            compressor->open(((char *)newData) + sizeof(size32_t), newSize);
            if (compressor->write(data, size)==size)
            {
                compressor->close();
                memcpy(newData, &size, sizeof(size32_t));
                newSize = sizeof(size32_t) + compressor->buflen();
                compressed = true;
                set(newSize, newData);
            }
            free(newData);
            compressor->Release();  
        }
        catch (...)
        {
            if (newData)
                free(newData);
            if (compressor) compressor->Release();
            throw;
        }
    }
    if (raw || !compressed)
        set(size, data);
}

static void *uncompress(const void *src, size32_t &sz)
{
    IExpander *expander = NULL;
    void *uncompressedValue = NULL;
    try
    {
        memcpy(&sz, src, sizeof(size32_t));
        assertex(sz);
        expander = createLZWExpander();
        src = ((const char *)src) + sizeof(size32_t);
        void *uncompressedValue = malloc(sz); assertex(uncompressedValue);
        expander->init(src);
        expander->expand(uncompressedValue);
        expander->Release();

        return uncompressedValue;
    }
    catch (...)
    {
        if (expander) expander->Release();
        if (uncompressedValue) free(uncompressedValue);
        throw;
    }
}

const void *CPTValue::queryValue() const
{
    if (compressed)
    {
        size32_t sz;
        void *uncompressedValue = uncompress(get(), sz);
        ((MemoryAttr *)this)->setOwn(sz, uncompressedValue);
        compressed = false;
    }
    return get();
}

void CPTValue::serialize(MemoryBuffer &tgt)
{
    tgt.append(length());
    if (length())
    {
        tgt.append(compressed);
        tgt.append(length(), get());
    }
}

void CPTValue::deserialize(MemoryBuffer &src)
{
    size32_t sz;
    src.read(sz);
    if (sz)
    {
        src.read(compressed);
        set(sz, src.readDirect(sz));
    }
    else
    {
        compressed = false;
        clear();
    }
}

MemoryBuffer &CPTValue::getValue(MemoryBuffer &tgt, bool binary) const
{
    if (compressed)
    {
        size32_t sz;
        void *uncompressedValue = uncompress(get(), sz);
        if (!binary) sz -= 1;
        tgt.append(sz, uncompressedValue);
        if (uncompressedValue)
            free(uncompressedValue);
    }
    else
    {
        if (binary)
            tgt.append(length(), get());
        else
            tgt.append(length()-1, get());
    }

    return tgt;
}

StringBuffer &CPTValue::getValue(StringBuffer &tgt, bool binary) const
{
    if (compressed)
    {
        size32_t sz;
        void *uncompressedValue = NULL;
        try
        {
            uncompressedValue = uncompress(get(), sz);
            if (!binary) sz -= 1;
            tgt.append(sz, (const char *)uncompressedValue);
            free(uncompressedValue);
        }
        catch (IException *)
        {
            if (uncompressedValue) free(uncompressedValue);
            throw;
        }
    }
    else
    {
        if (binary) // this should probably be an assert?
            tgt.append(length(), (const char *)get());
        else if (length())
            tgt.append(length()-1, (const char *)get());
    }

    return tgt;
}

size32_t CPTValue::queryValueSize() const
{
    if (compressed)
    {
        size32_t sz;
        memcpy(&sz, get(), sizeof(size32_t));
        return sz;
    }
    else
        return length();
}

///////////////////

PTree::PTree(MemoryBuffer &src)
{
    init();
    deserialize(src);
}

PTree::PTree(const char *_name, byte _flags, IPTArrayValue *_value, ChildMap *_children)
{
    init();
    flags = _flags;
    if (isnocase())
        attributes.setNoCase(true);
    if (_name) setName(_name);
    children = LINK(_children);
    value = _value;
}

PTree::~PTree()
{
    if (value) delete value;
    ::Release(children);
    if (name)
    {
        AtomRefTable *kT = isnocase()?keyTableNC:keyTable;
        kT->releaseKey(name);
    }
}

IPropertyTree *PTree::queryChild(unsigned index) 
{
    if (!value) return NULL;
    if (!value->isArray()) return this;

    IPropertyTree *v = value->queryElement(index);
    return v;
}

aindex_t PTree::findChild(IPropertyTree *child, bool remove)
{
    if (value && value->isArray())
    {
        unsigned i;
        for (i=0; i<value->elements(); i++)
        {
            IPropertyTree *_child = value->queryElement(i);
            if (_child == child)
            {
                if (remove)
                {
                    assertex(value);
                    value->removeElement(i);
                }
                return i;
            }
        }
    }
    else if (children)
    {
        IPropertyTree *_child = children->query(child->queryName());
        if (_child == child)
        {
            if (remove)
                children->removeExact(_child);
            return 0;
        }
        else if (_child)
        {
            PTree *__child = (PTree *) _child;
            return __child->findChild(child, remove);
        }
    }
    return NotFound;
}

ChildMap *PTree::checkChildren() const
{
    return children;
}

void PTree::setLocal(size32_t l, const void *data, bool _binary)
{
    if (value) delete value;
    if (l)
        value = new CPTValue(l, data, _binary);
    else
        value = NULL;
    if (_binary)
        IptFlagSet(flags, ipt_binary);
    else
        IptFlagClr(flags, ipt_binary);
}

void PTree::appendLocal(size32_t l, const void *data, bool binary)
{
    if (0 == l) return;
    MemoryBuffer mb;
    if (value)
    {
        assertex(!value->isArray());
        assertex(binary == IptFlagTst(flags, ipt_binary));
        value->getValue(mb, binary);
        mb.append(l, data);
        delete value;
        
        l = mb.length();
        data = mb.toByteArray();
    }
    if (l)
        value = new CPTValue(l, data, binary);
    else
        value = NULL;
    if (binary)
        IptFlagSet(flags, ipt_binary);
    else
        IptFlagClr(flags, ipt_binary);
}

void PTree::setName(const char *_name)
{
    AtomRefTable *kT = isnocase()?keyTableNC:keyTable;
    HashKeyElement *oname = name;
    if (!_name)
        name = NULL;
    else
    {
        if (!validateXMLTag(_name)) 
            throw MakeIPTException(PTreeExcpt_InvalidTagName, ": %s", _name);
        name = kT->queryCreate(_name);
    }
    if (oname)
        kT->releaseKey(oname);
}

// IPropertyTree impl.
bool PTree::hasProp(const char * xpath) const
{
    const char *prop = splitXPathX(xpath);
    if (isAttribute(prop)) // JCS - note no wildcards on attributes
    {
        if (prop != xpath)
        {
            MAKE_LSTRING(path, xpath, prop-xpath);
            Owned<IPropertyTreeIterator> iter = getElements(path);
            if (iter->first())
            {
                do
                {
                    IPropertyTree &branch = iter->query();
                    if (branch.hasProp(prop))
                        return true;
                }
                while (iter->next());
            }
            return false;
        }
        else
            return (NULL != attributes.find(xpath));
    }
    else
    {
        IPropertyTreeIterator *iter = getElements(xpath);
        bool res = iter->first();
        iter->Release();
        return res;
    }
}

const char *PTree::queryProp(const char *xpath) const
{
    if (!xpath)
    {
        if (!value) return NULL;
        return (const char *) value->queryValue();
    }
    else if (isAttribute(xpath))
    {
        return attributes.find(xpath);
    }
    else
    {
        const char *prop = splitXPathX(xpath);
        if (isAttribute(prop))
        {
            MAKE_LSTRING(path, xpath, prop-xpath);
            IPropertyTree *branch = queryPropTree(path);
            if (!branch) return NULL;
            return branch->queryProp(prop);
        }
        else
        {
            IPropertyTree *branch = queryPropTree(xpath);
            if (!branch) return NULL;
            return branch->queryProp(NULL);
        }
    }
}

bool PTree::getProp(const char *xpath, StringBuffer &ret) const
{
    if (!xpath)
    {
        if (!value) return false;
        value->getValue(ret, IptFlagTst(flags, ipt_binary));
        return true;
    }
    else if (isAttribute(xpath))
    {
        const char *value = attributes.find(xpath);
        if (!value) return false;
        ret.append(value);
        return true;
    }
    else
    {
        const char *prop = splitXPathX(xpath);
        if (isAttribute(prop))
        {
            MAKE_LSTRING(path, xpath, prop-xpath)
            IPropertyTree *branch = queryPropTree(path);
            if (!branch) return false;
            return branch->getProp(prop, ret);
        }
        else
        {
            IPropertyTree *branch = queryPropTree(xpath);
            if (!branch) return false;
            return branch->getProp(NULL, ret);
        }
    }
}

bool PTree::removeAttr(const char *attr)
{
    return attributes.remove(attr);
}


void PTree::setAttr(const char *attr, const char *val)
{
    if (!validateXMLTag(attr+1)) 
        throw MakeIPTException(-1, "Invalid xml attribute: %s", attr);
    attributes.set(attr, val);
}

void PTree::setProp(const char *xpath, const char *val)
{
    if (!xpath || '\0' == *xpath)
    {
        if (!val)
        {
            if (value) delete value;
            value = NULL;
        }
        else
        {
            size32_t l=(size32_t)strlen(val);
            if (!l)
            {
                if (value) delete value;
                value = NULL;
            }
            else
                setLocal(l+1, val);
        }
    }
    else if (isAttribute(xpath))
    {
        if (!val)
            removeAttr(xpath);
        else
            setAttr(xpath, val);
    }
    else
    {
        const char *prop;
        IPropertyTree *branch = splitBranchProp(xpath, prop, true);

        if (isAttribute(prop))
            branch->setProp(prop, val);
        else
        {
            if (val)
            {
                IPropertyTree *propBranch = queryCreateBranch(branch, prop);
                propBranch->setProp(NULL, val);
            }
            else
                branch->removeProp(prop);
        }
    }
}

aindex_t PTree::getChildMatchPos(const char *xpath)
{
    Owned<IPropertyTreeIterator> childIter = getElements(xpath);
    if (!childIter->first())
        return (aindex_t)-1;
    IPropertyTree &childMatch = childIter->query();
    if (childIter->next())
        AMBIGUOUS_PATH("addPropX", xpath);

    if (value)
        if (value->isArray())
            return findChild(&childMatch);
        else
            return 0;
    else
        return 0;
}

void PTree::resolveParentChild(const char *xpath, IPropertyTree *&parent, IPropertyTree *&child, StringAttr &path, StringAttr &qualifier)
{
    parent = child = NULL;
    if (!xpath)
        throw MakeIPTException(-1, "No path to resolve parent from");
    const char *end = xpath+strlen(xpath);
    const char *prop = end;

    while (prop != xpath && *(prop-1) != '/')
        --prop;

    size32_t ps = prop-xpath;
    if (ps)
    {
        path.set(xpath, ps);

        Owned<IPropertyTreeIterator> pathIter = getElements(path);
        if (!pathIter->first())
            throw MakeIPTException(-1, "resolveParentChild: path not found %s", xpath);

        IPropertyTree *currentPath = NULL;
        bool multiplePaths = false;
        bool multipleChildMatches = false;
        loop
        {
            // JCSMORE - a bit annoying has to be done again once path has been established
            currentPath = &pathIter->query();
            Owned<IPropertyTreeIterator> childIter = currentPath->getElements(prop);
            if (childIter->first())
            {
                child = &childIter->query();
                if (parent)
                    AMBIGUOUS_PATH("resolveParentChild", xpath);
                if (!multipleChildMatches && childIter->next())
                    multipleChildMatches = true;

                parent = currentPath;
            }
            if (pathIter->next())
                multiplePaths = true;
            else break;
        }
        if (!parent)
        {
            if (multiplePaths) // i.e. no unique path to child found and multiple parent paths
                AMBIGUOUS_PATH("resolveParentChild", xpath);
            parent = currentPath;
        }
        if (multipleChildMatches)
            child = NULL; // single parent, but no single child.
        path.set(prop);
        const char *pstart = prop;
        bool wild;
        readWildId(prop, wild);
        size32_t s = prop-pstart;
        if (wild)
            throw MakeXPathException(pstart, PTreeExcpt_XPath_ParseError, s-1, "Wildcards not permitted on add");
        assertex(s);
        path.set(pstart, s);
        qualifier.set(prop);
    }
    else
    {
        assertex(prop && *prop);
        parent = this;
        const char *pstart = prop;
        bool wild;
        readWildId(prop, wild);
        assertex(!wild);
        size32_t s = prop-pstart;
        if (*prop && *prop != '[')
            throw MakeXPathException(pstart, PTreeExcpt_XPath_ParseError, s, "Qualifier expected e.g. [..]");
        path.set(pstart, s);
        if (checkChildren())
            child = children->query(path);
        if (child)
            qualifier.set(prop);
        else
            qualifier.clear();
    }               
}

void PTree::addProp(const char *xpath, const char *val)
{
    if (!xpath || '\0' == *xpath)
        addLocal((size32_t)strlen(val)+1, val);
    else if (isAttribute(xpath))
        setAttr(xpath, val);
    else if ('[' == *xpath)
    {
        aindex_t pos = getChildMatchPos(xpath);
        if (-1 == pos)
            throw MakeIPTException(-1, "addProp: qualifier unmatched %s", xpath);
        addLocal((size32_t)strlen(val)+1, val, false, pos);
    }
    else
    {
        IPropertyTree *parent, *child;
        StringAttr path, qualifier;
        resolveParentChild(xpath, parent, child, path, qualifier);
        if (parent != this)
            parent->addProp(path, val);
        else if (child)
            child->addProp(qualifier, val);
        else
            setProp(path, val);
    }
}

void PTree::appendProp(const char *xpath, const char *val)
{
    if (!xpath || '\0' == *xpath)
        appendLocal((size_t)strlen(val)+1, val, false);
    else if (isAttribute(xpath))
    {
        StringBuffer newVal;
        getProp(xpath, newVal);
        newVal.append(val);
        setAttr(xpath, newVal.str());
    }
    else if ('[' == *xpath)
    {
        aindex_t pos = getChildMatchPos(xpath);
        if (-1 == pos)
            throw MakeIPTException(-1, "appendProp: qualifier unmatched %s", xpath);
        appendLocal((size_t)strlen(val)+1, val, false);
    }
    else
    {
        IPropertyTree *parent, *child;
        StringAttr path, qualifier;
        resolveParentChild(xpath, parent, child, path, qualifier);
        if (parent != this)
            parent->appendProp(path, val);
        else if (child)
            child->appendProp(qualifier, val);
        else
            setProp(path, val);
    }
}

bool PTree::getPropBool(const char *xpath, bool dft) const
{
    const char *val = queryProp(xpath);
    if (val && *val)
        return strToBool(val);
    else
        return dft;
}

__int64 PTree::getPropInt64(const char *xpath, __int64 dft) const
{
    if (!xpath)
    {
        if (!value) return dft;
        else
        {
            const char *v = (const char *)value->queryValue();
            if (!v || !*v) return dft;
            else return _atoi64(v);
        }
    }
    else if (isAttribute(xpath))
    {
        const char *a = attributes.find(xpath);
        if (!a || !*a)
            return dft;
        return _atoi64(a);
    }
    else
    {
        const char *prop = splitXPathX(xpath);
        if (isAttribute(prop))
        {
            MAKE_LSTRING(path, xpath, prop-xpath);
            IPropertyTree *branch = queryPropTree(path);
            if (!branch) return dft;
            return branch->getPropInt64(prop, dft);
        }
        else
        {
            IPropertyTree *branch = queryPropTree(xpath);
            if (!branch) return dft;
            return branch->getPropInt64(NULL, dft);
        }
    }
}

void PTree::setPropInt64(const char * xpath, __int64 val)
{
    if (!xpath || '\0' == *xpath)
    {
        char buf[23];
        numtostr(buf, val);
        setLocal((size32_t)strlen(buf)+1, buf);
    }
    else if (isAttribute(xpath))
    {
        char buf[23];
        numtostr(buf, val);
        setAttr(xpath, buf);
    }
    else
    {
        const char *prop;
        IPropertyTree *branch = splitBranchProp(xpath, prop, true);

        if (isAttribute(prop))
            branch->setPropInt64(prop, val);
        else
        {
            IPropertyTree *propBranch = queryCreateBranch(branch, prop);
            propBranch->setPropInt64(NULL, val);
        }
    }
}

void PTree::addPropInt64(const char *xpath, __int64 val)
{
    if (!xpath || '\0' == *xpath)
    {
        char buf[23];
        numtostr(buf,val);
        addLocal((size32_t)strlen(buf)+1, buf);
    }
    else if (isAttribute(xpath))
    {
        char buf[23];
        numtostr(buf, val);
        setAttr(xpath, buf);
    }
    else if ('[' == *xpath)
    {
        char buf[23];
        numtostr(buf, val);
        aindex_t pos = getChildMatchPos(xpath);
        if (-1 == pos)
            throw MakeIPTException(-1, "addPropInt64: qualifier unmatched %s", xpath);
        addLocal((size32_t)strlen(buf)+1, buf, false, pos);
    }
    else
    {
        IPropertyTree *parent, *child;
        StringAttr path, qualifier;
        resolveParentChild(xpath, parent, child, path, qualifier);
        if (parent != this)
            parent->addPropInt64(path, val);
        else if (child)
            child->addPropInt64(qualifier, val);
        else
            setPropInt64(path, val);
    }
}

int PTree::getPropInt(const char *xpath, int dft) const
{
    return (int) getPropInt64(xpath, dft); // underlying type always __int64 (now)
}

void PTree::setPropInt(const char *xpath, int val)
{
    setPropInt64(xpath, val); // underlying type always __int64 (now)
}

void PTree::addPropInt(const char *xpath, int val)
{
    addPropInt64(xpath, val); // underlying type always __int64 (now)
}

bool PTree::isCompressed(const char *xpath) const
{
    if (!xpath)
        return (value && value->isCompressed());
    else if (isAttribute(xpath))
        return false;
    else
    {
        const char *prop = splitXPathX(xpath);
        if (prop && '\0' != *prop && !isAttribute(prop))
        {
            IPropertyTree *branch = queryPropTree(xpath);
            if (branch)
                return branch->isCompressed(prop);
        }
    }
    return false;
}

bool PTree::isBinary(const char *xpath) const
{
    if (!xpath)
        return IptFlagTst(flags, ipt_binary);
    else if (isAttribute(xpath)) // still positing that attr cannot be binary for now.
        return false;
    else
    {
        const char *prop = splitXPathX(xpath);
        if (prop && '\0' != *prop && !isAttribute(prop))
        {
            IPropertyTree *branch = queryPropTree(xpath);
            if (branch)
                return branch->isBinary(NULL);
        }
    }
    return false;
}

bool PTree::renameTree(IPropertyTree *child, const char *newName) // really here for hook for SDS (can substationally optimize remote action)
{
    if (0==strcmp(newName, child->queryName()) && NotFound!=findChild(child)) return false;
    Linked<IPropertyTree> tmp = child;
    if (removeTree(child))
    {
        addPropTree(newName, child);
        tmp.getClear(); // addPropTree has taken ownership.
        return true;
    }
    return false;
}

bool PTree::renameProp(const char *xpath, const char *newName)
{
    if (!xpath || '\0' == *xpath) 
        throw MakeIPTException(-1, "renameProp: cannot rename self, renameProp has to rename in context of a parent");
    if (strcmp(xpath,"/")==0)   // rename of self allowed assuming no parent
        setName(newName);
    else if ('[' == *xpath)
        UNIMPLEMENTED;
    else if (isAttribute(xpath))
    {
        StringBuffer val;
        if (!getProp(xpath, val))
            return false;
        removeProp(xpath);
        addProp(newName, val.str());
    }
    else
    {
        StringBuffer path;
        const char *prop = splitXPath(xpath, path);
        assertex(prop);
        if (path.length())
        {
            Owned<IPropertyTreeIterator> iter = getElements(path.str());
            if (!iter->first())
                return false;
            IPropertyTree &branch = iter->query();
            if (iter->next())
                AMBIGUOUS_PATH("renameProp", xpath);
            return branch.renameProp(prop, newName);
        }
        else
        {
            IPropertyTree *old = queryPropTree(xpath);
            if (!old)
                return false;
            return renameTree(old, newName);
        }
    }
    return true;
}

bool PTree::getPropBin(const char *xpath, MemoryBuffer &ret) const
{
    CHECK_ATTRIBUTE(xpath);
    if (!xpath)
    {
        if (!value) return true; // exists, but no value
        value->getValue(ret, IptFlagTst(flags, ipt_binary));
        return true;
    }
    else
    {
        const char *prop = splitXPathX(xpath);
        if (isAttribute(prop))
        {
            MAKE_LSTRING(path, xpath, prop-xpath);
            IPropertyTree *branch = queryPropTree(path);
            if (!branch) return false;
            return branch->getPropBin(prop, ret);
        }
        else
        {
            IPropertyTree *branch = queryPropTree(xpath);
            if (!branch) return false;
            return branch->getPropBin(NULL, ret);
        }
    }
}

void PTree::setPropBin(const char * xpath, size32_t size, const void *data)
{
    CHECK_ATTRIBUTE(xpath);
    if (!xpath || '\0' == *xpath)
        setLocal(size, data, true);
    else
    {
        const char *prop;
        IPropertyTree *branch = splitBranchProp(xpath, prop, true);
        if (isAttribute(prop))
            branch->setPropBin(prop, size, data);
        else
        {
            IPropertyTree *propBranch = queryCreateBranch(branch, prop);
            propBranch->setPropBin(NULL, size, data);
        }
    }
}

void PTree::addPropBin(const char *xpath, size32_t size, const void *data)
{
    CHECK_ATTRIBUTE(xpath);
    if (!xpath || '\0' == *xpath)
        addLocal(size, data, true);
    else if ('[' == *xpath)
    {
        aindex_t pos = getChildMatchPos(xpath);
        if (-1 == pos)
            throw MakeIPTException(-1, "addPropBin: qualifier unmatched %s", xpath);
        addLocal(size, data, true, pos);
    }
    else
    {
        IPropertyTree *parent, *child;
        StringAttr path, qualifier;
        resolveParentChild(xpath, parent, child, path, qualifier);
        if (parent != this)
            parent->addPropBin(path, size, data);
        else if (child)
            child->addPropBin(qualifier, size, data);
        else
            setPropBin(path, size, data);
    }
}

void PTree::appendPropBin(const char *xpath, size32_t size, const void *data)
{
    CHECK_ATTRIBUTE(xpath);
    if (!xpath || '\0' == *xpath)
        appendLocal(size, data, true);
    else if ('[' == *xpath)
    {
        aindex_t pos = getChildMatchPos(xpath);
        if (-1 == pos)
            throw MakeIPTException(-1, "appendPropBin: qualifier unmatched %s", xpath);
        appendLocal(size, data, true);
    }
    else
    {
        IPropertyTree *parent, *child;
        StringAttr path, qualifier;
        resolveParentChild(xpath, parent, child, path, qualifier);
        if (parent != this)
            parent->appendPropBin(path, size, data);
        else if (child)
            child->appendPropBin(qualifier, size, data);
        else
            setPropBin(path, size, data);
    }
}

IPropertyTree *PTree::getPropTree(const char *xpath) const
{
    IPropertyTree *tree = queryPropTree(xpath);
    return LINK(tree);
}

IPropertyTree *PTree::queryPropTree(const char *xpath) const
{
    Owned<IPropertyTreeIterator> iter = getElements(xpath);
    IPropertyTree *element = NULL;
    if (iter->first())
    {
        element = &iter->query();
        if (iter->next())
            AMBIGUOUS_PATH("getProp",xpath);
    }
    return element;
}

void PTree::replaceSelf(IPropertyTree *val)
{
    Owned<IAttributeIterator> aiter = getAttributes();
    StringArray attrs;
    ForEach (*aiter)
        attrs.append(aiter->queryName());
    ForEachItemIn(a, attrs)
        removeProp(attrs.item(a));
    ICopyArrayOf<IPropertyTree> elems;
    Owned<IPropertyTreeIterator> iter = getElements("*");
    ForEach(*iter)
        elems.append(iter->query());
    ForEachItemIn(e, elems)
        removeTree(&elems.item(e));
    aiter.setown(val->getAttributes());
    ForEach(*aiter)
        setProp(aiter->queryName(), aiter->queryValue());
    iter.setown(val->getElements("*"));
    ForEach(*iter)
    {
        IPropertyTree &node = iter->query();
        node.Link();
        addPropTree(node.queryName(), &node);
    }
    val->Release();
}

IPropertyTree *PTree::setPropTree(const char *xpath, IPropertyTree *val)
{
    CHECK_ATTRIBUTE(xpath);

    if (NULL == xpath)
    {
        replaceSelf(val);
        return this;
    }
    else
    {
        StringAttr prop, qualifier;
        IPropertyTree *branch, *child;
        resolveParentChild(xpath, branch, child, prop, qualifier);
        if (branch == this)
        {   
            IPropertyTree *_val = ownPTree(val);
            PTree *__val = QUERYINTERFACE(_val, PTree); assertex(__val);
            __val->setName(prop);
            addingNewElement(*_val, ANE_SET);
            if (!checkChildren()) createChildMap();
            children->set(prop, _val);
            return _val;
        }
        else
            return branch->setPropTree(prop, val);
    }
}

IPropertyTree *PTree::addPropTree(const char *xpath, IPropertyTree *val)
{
    if (!xpath || '\0' == *xpath)
        throw MakeIPTException(PTreeExcpt_InvalidTagName, "Invalid xpath for property tree insertion specified");
    else
    {
        CHECK_ATTRIBUTE(xpath);
        const char *x = xpath;
        loop
        {
            if (!*x++)
            {
                IPropertyTree *_val = ownPTree(val);
                PTree *__val = QUERYINTERFACE(_val, PTree); assertex(__val);
                __val->setName(xpath);
                addingNewElement(*_val, -1);
                if (checkChildren())
                {
                    IPropertyTree *child = children->query(xpath);
                    if (child)
                    {
                        __val->setParent(this);
                        PTree *tree = QUERYINTERFACE(child, PTree); assertex(tree);
                        if (tree->value && tree->value->isArray())
                            tree->value->addElement(_val);
                        else
                        {
                            IPTArrayValue *array = new CPTArray();
                            array->addElement(LINK(child));
                            array->addElement(_val);
                            IPropertyTree *container = create(xpath, array);
                            PTree *_tree = QUERYINTERFACE(child, PTree); assertex(_tree); _tree->setParent(this);
                            children->replace(xpath, container);
                        }
                        return _val;
                    }
                }
                else
                    createChildMap();
                children->set(xpath, _val);
                return _val;
            }
            if ('/' == *x || '[' == *x)
                break;
        }
        IPropertyTree *parent, *child;
        StringAttr path, qualifier;
        resolveParentChild(xpath, parent, child, path, qualifier);
        if (parent != this)
            return parent->addPropTree(path, val);
        else
        {
            aindex_t pos = (aindex_t)-1;
            if (qualifier.length())
            {
                pos = ((PTree *)child)->getChildMatchPos(qualifier);
                if (-1 == pos)
                    throw MakeIPTException(-1, "addPropTree: qualifier unmatched %s", xpath);
            }
            IPropertyTree *_val = ownPTree(val);
            PTree *__val = QUERYINTERFACE(_val, PTree); assertex(__val);
            __val->setName(path);
            addingNewElement(*_val, pos);
            if (child)
            {
                __val->setParent(this);
                PTree *tree = QUERYINTERFACE(child, PTree); assertex(tree);
                if (tree->value && tree->value->isArray())
                {
                    if (-1 == pos)
                        tree->value->addElement(_val);
                    else
                        tree->value->setElement(pos, _val);
                }
                else
                {
                    IPTArrayValue *array = new CPTArray();
                    array->addElement(LINK(child));
                    assertex(-1 == pos || 0 == pos);
                    if (-1 == pos)
                        array->addElement(_val);
                    else
                        array->setElement(0, _val);
                    IPropertyTree *container = create(path, array);
                    tree->setParent(this);
                    children->replace(path, container);         
                }
            }
            else
            {
                if (!checkChildren()) createChildMap();
                children->set(path, _val);
            }
            return _val;
        }
    }
}

bool PTree::removeTree(IPropertyTree *child)
{
    if (children)
    {
        Owned<IPropertyTreeIterator> iter = children->getIterator(false);
        if (iter->first())
        {
            do
            {
                PTree *element = (PTree *) &iter->query();
                if (element == child)
                    return children->removeExact(element);

                if (element->value && element->value->isArray())
                {
                    Linked<PTree> tmp = (PTree*) child;
                    aindex_t i = element->findChild(child, true);
                    if (NotFound != i)
                    {
                        removingElement(child, i);
                        if (0 == element->value->elements())
                            children->removeExact(element);
                        return true;
                    }
                }
            }
            while (iter->next());
        }
    }
    return false;
}

bool PTree::removeProp(const char *xpath)
{
    if (xpath && isAttribute(xpath))
        return removeAttr(xpath);

    StringBuffer path;
    const char *prop = splitXPath(xpath, path);
    if (!prop)
        throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, 0, "Invalid xpath for property deletion");

    if (path.length())
    {
        Owned<IPropertyTreeIterator> iter = getElements(path.str());

        if (!iter)
            return false;
        
        bool res = false;
        if (iter->first())
        {
            do
            {
                IPropertyTree *branch = &iter->query();

                if (branch) {
                    res = branch->removeProp(prop);
                    if (res)
                        break; // deleted first may be another
                }
            }
            while (iter->next());
        }
        return res;
    }
    else
    {
        if (!queryNextUnquoted(xpath, '[') && !strchr(prop, '*')) // have to work hard to locate qualified prop tree from parent.
        {
            if (!checkChildren()) return false;
            return children->remove(prop); // NB: might be multivalued.
        }

        const char *xxpath = prop;
        readID(xxpath, false);
        const char *idEnd = xxpath;
        if ('[' == *xxpath)
        {
            ++xxpath;
            const char *digitStart = xxpath;
            while (*xxpath && ']' != *xxpath && isdigit(*xxpath)) xxpath++;
            assertex(*xxpath != '\0');
            if (']' == *xxpath) // so it's a digit index!
            {
                StringAttr id(prop, idEnd-prop);
                PTree *child = children?(PTree *)children->query(id):NULL;
                if (child)
                {
                    if (child->value && child->value->isArray() && child->value->elements()>1)
                    {
                        StringAttr digit(digitStart, xxpath-digitStart);
                        unsigned i = atoi(digit);
                        if (i <= child->value->elements())
                        {
                            removingElement(child->value->queryElement(i-1), i-1);
                            child->value->removeElement(i-1);
                            return true;
                        }
                    }
                    else
                        return children->removeExact(child);
                }
                return false;
            }
        }
        // JCSMORE - This is ridiculous for qualifier have to iterate to find match ok, but then finding where that *was* gees!
        Owned <IPropertyTreeIterator> iter = getElements(prop);
        if (!iter->first())
            return false;
        IPropertyTree *match = &iter->query();
#if 0 // intentionally removes first encountered
        if (iter->next())
        {
            AMBIGUOUS_PATH("removeProp",xpath);
        }
#endif
        return removeTree(match);
    }
    return false;
}

aindex_t PTree::queryChildIndex(IPropertyTree *child)
{
    return findChild(child);
}

const char *PTree::queryName() const
{
    return name?name->get():NULL;
}

StringBuffer &PTree::getName(StringBuffer &ret) const
{
    ret.append(queryName());
    return ret;
}

MAKEPointerArray(AttrValue, AttrArray);
IAttributeIterator *PTree::getAttributes(bool sorted) const
{
    class CAttributeIterator : public CInterface, implements IAttributeIterator
    {
    public:
        IMPLEMENT_IINTERFACE;

        CAttributeIterator(const PTree *_parent) : parent(_parent), cur(NULL) 
        {
            index = 0;
            cur = NULL;
        }

        ~CAttributeIterator()
        {
        }


    // IAttributeIterator impl.
        virtual bool first()
        {
            index = 0;
            if (!parent->queryAttributes().count()) {
                cur = NULL;
                return false;
            }
            cur = parent->queryAttributes().item(0);
            return true;
        }
        virtual bool next()
        {
            index++;
            if (index>=parent->queryAttributes().count()) {
                cur = NULL;
                return false;
            }
            cur = parent->queryAttributes().item(index);
            return true;
        }
        virtual bool isValid() { return cur!=NULL; }
        virtual const char *queryName() const
        {
            assertex(cur);
            return cur->key->get();
        }
        virtual const char *queryValue() const
        {
            assertex(cur);
            return cur->value->get();
        }

        virtual StringBuffer &getValue(StringBuffer &out)
        {
            assertex(cur);
            out.append(cur->value->get());
            return out;
        }

        virtual unsigned count() { return parent->queryAttributes().count(); }

    private:
        AttrValue *cur;
        unsigned index;
        Linked<const PTree> parent;
    };
    class CSortedAttributeIterator : public CInterface, implements IAttributeIterator
    {
        typedef ArrayIteratorOf<AttrArray, AttrValue &> AttrIterator;
    public:
        IMPLEMENT_IINTERFACE;

        static int compareAttrs(AttrValue **ll, AttrValue **rr)
        {
            return stricmp((*ll)->key->get(), (*rr)->key->get());
        };

        CSortedAttributeIterator(const PTree *_parent) : parent(_parent), iter(NULL), cur(NULL)
        {
            unsigned i = parent->queryAttributes().count();
            if (i)
            {
                attrs.ensure(i);
                while (i--) 
                    attrs.append(*parent->queryAttributes().item(i));
                attrs.sort(compareAttrs);
                iter = new AttrIterator(attrs);
            }
        }

        ~CSortedAttributeIterator()
        {
            if (iter)
                delete iter;
        }

    // IAttributeIterator impl.
        virtual bool first()
        {
            if (!iter) return false;
            if (!iter->first()) { cur = NULL; return false; }
            cur = &iter->query();
            return true;
        }
        virtual bool next()
        {
            if (!iter) return false;
            if (!iter->next()) { cur = NULL; return false; }
            cur = &iter->query();
            return true;
        }
        virtual bool isValid() { return cur!=NULL; }
        virtual const char *queryName() const
        {
            assertex(cur);
            return cur->key->get();
        }
        virtual const char *queryValue() const
        {
            assertex(cur);
            return cur->value->get();
        }

        virtual StringBuffer &getValue(StringBuffer &out)
        {
            assertex(cur);
            out.append(cur->value->get());
            return out;
        }

        virtual unsigned count() { return attrs.ordinality(); }

    private:
        AttrArray attrs;
        AttrValue *cur;
        AttrIterator *iter;
        Linked<const PTree> parent;
    };
    if (sorted)
        return new CSortedAttributeIterator(this);
    else
        return new CAttributeIterator(this);
}

///////////////////
class CIndexIterator : public CInterface, implements IPropertyTreeIterator
{
    Owned<IPropertyTreeIterator> subIter;
    IPropertyTree *celem;
    unsigned index, current;
public:
    IMPLEMENT_IINTERFACE;

    CIndexIterator(IPropertyTreeIterator *_subIter, unsigned _index) : subIter(_subIter), index(_index)
    {
    }

// IPropertyTreeIterator
    virtual bool first()
    {
        if (!index)
            return false;
        if (!subIter->first())
            return false;
        current = 1;
        celem = NULL;
        do
        {
            if (current == index)
            {
                celem = &subIter->query();
                return true;
            }
            if (!subIter->next())
                return false;
        } while (++current <= index);
        return false;
    }
    virtual bool isValid()
    {
        return celem && (index >= current);
    }
    virtual bool next()
    {
        celem = NULL;
        return false;
    }
    virtual IPropertyTree & query()
    {
        return *celem;
    }
};

IPropertyTreeIterator *PTree::getElements(const char *xpath, IPTIteratorCodes flags) const
{
    // NULL iterator for local value (i.e. maybe be single value or array)
    if (NULL == xpath || '\0' == *xpath)
        return new SingleIdIterator(*this);
    Owned<IPropertyTreeIterator> iter;
    const char *_xpath = xpath;
    bool root=true;
restart:
    switch (*xpath)
    {
        case '.':
            root=false;
            ++xpath;
            if ('\0' == *xpath)
                return new SingleIdIterator(*this);
            else if ('/' != *xpath)
                throw MakeXPathException(xpath-1, PTreeExcpt_XPath_Unsupported, 0, "");
            goto restart;
        case '/':
            ++xpath;
            if ('/' == *xpath)
            {
                iter.setown(getElements(xpath+1));
                if (checkChildren())
                {
                    IPropertyTreeIterator *iter2 = new PTIdMatchIterator(this, "*", isnocase(), flags & iptiter_sort);
                    iter2 = new PTStackIterator(iter2, xpath-1);

                    SeriesPTIterator *series = new SeriesPTIterator();
                    series->addIterator(iter.getClear());
                    series->addIterator(iter2);

                    return series;
                }
                else
                    return iter.getClear();
            }
            else if (root)
                throw MakeXPathException(xpath, PTreeExcpt_XPath_Unsupported, 0, "Root specifier \"/\" specifier is not supported");
            else if ('\0' == *xpath)
                return new SingleIdIterator(*this);
            goto restart;
        case '[':
        {
            ++xpath;
            if (isdigit(*xpath)) {
                StringAttr index;
                xpath = readIndex(xpath, index);
                unsigned i = atoi(index.get());
                if (i)
                {
                    if (value && value->isArray())
                    {
                        IPropertyTree *element = value->queryElement(--i);
                        if (element)
                        {
                            iter.setown(element->getElements(NULL));
                        }
                    }
                    else if (i == 1)
                        iter.setown(new SingleIdIterator(*this));
                }
            }
            else 
            {
                if (checkPattern(xpath))
                    iter.setown(new SingleIdIterator(*this));
            }
            if (']' != *xpath)
                throw MakeXPathException(_xpath, PTreeExcpt_XPath_ParseError, xpath-_xpath, "Qualifier brace unclosed");
            ++xpath;
            break;
        }
        default:
        {
            bool wild;
            const char *start = xpath;
            readWildId(xpath, wild);
            size32_t s = xpath-start;
            if (s)
            {
                MAKE_LSTRING(id, start, s);
                if (checkChildren())
                {
                    IPropertyTree *child = NULL;
                    if (!wild)
                        child = children->query(id);

                    if ((wild || child) && '[' == *xpath) // check for local index not iterative qualifier.
                    {
                        const char *xxpath = xpath+1;
                        if (isdigit(*xxpath)) {
                            StringAttr idxstr;
                            xxpath = readIndex(xxpath, idxstr);
                            if (']' != *xxpath)
                                throw MakeXPathException(_xpath, PTreeExcpt_XPath_ParseError, xpath-_xpath, "Qualifier brace unclosed");
                            ++xxpath;
                            unsigned index = atoi(idxstr.get());
                            if (index)
                            {
                                Owned<IPropertyTreeIterator> _iter = getElements(id);
                                if (_iter->first())
                                {
                                    do
                                    {
                                        if (0 == --index)
                                        {
                                            iter.setown(new SingleIdIterator((PTree &)_iter->query()));
                                            break;
                                        }
                                    }
                                    while (_iter->next());
                                }
                            }
                            xpath = xxpath; 
                        }
                        else
                        {
                            if (wild)
                                iter.setown(new PTIdMatchIterator(this, id, isnocase(), flags & iptiter_sort));
                            else
                                iter.setown(child->getElements(NULL));
                            const char *start = xxpath-1;
                            loop
                            {
                                char quote = 0;
                                while (']' != *(++xxpath) || quote)
                                {
                                    switch (*xxpath) {
                                    case '\"':
                                    case '\'':
                                    {
                                        if (quote)
                                        {
                                            if (*xxpath == quote)
                                                quote = 0;
                                        }
                                        else
                                            quote = *xxpath;
                                        break;
                                    }
                                    case '\0':
                                        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xxpath-start, "Qualifier brace unclosed");
                                    }
                                }
                                ++xxpath;
                                if ('[' == *xxpath)
                                {
                                    ++xxpath;
                                    if (isdigit(*xxpath))
                                    {
                                        StringAttr qualifier(start, (xxpath-1)-start);
                                        Owned<PTStackIterator> siter = new PTStackIterator(iter.getClear(), qualifier.get());
                                        StringAttr index;
                                        xxpath = readIndex(xxpath, index);
                                        unsigned i = atoi(index.get());
                                        iter.setown(new CIndexIterator(siter.getClear(), i));
                                        ++xxpath;
                                        break;
                                    }
                                }
                                else
                                {
                                    StringAttr qualifier(start, xxpath-start);
                                    iter.setown(new PTStackIterator(iter.getClear(), qualifier.get()));
                                    break;
                                }
                            }
                            xpath = xxpath;
                        }
                    }
                    else
                    {
                        if (wild)
                            iter.setown(new PTIdMatchIterator(this, id, isnocase(), flags & iptiter_sort));
                        else if (child)
                            iter.setown(child->getElements(NULL));
                    }
                }
            }
            break;
        }
    }

    if (!iter)
        iter.setown(LINK(nullPTreeIterator));
    if (*xpath == '\0' || (*xpath == '/' && '\0' == *(xpath+1)))
        return iter.getClear();
    else
        return new PTStackIterator(iter.getClear(), xpath);
}

void PTree::localizeElements(const char *xpath, bool allTail)
{
    // null action for local ptree
}

unsigned PTree::numChildren()
{
    if (!checkChildren()) return 0;
    return children->numChildren();
}

unsigned PTree::getCount(const char *xpath)
{
    unsigned c=0;
    Owned<IPropertyTreeIterator> iter = getElements(xpath);
    ForEach(*iter)
        ++c;
    return c;
}

void getXPathMatchTree(IPropertyTree &parentContext, const char *xpath, IPropertyTree *&matchContainer)
{
    if (!xpath || !*xpath)
    {
        matchContainer = createPTree(parentContext.queryName());
        return;
    }
    StringBuffer head;
    const char *str = xpath;
    const char *end = str+strlen(xpath);
    bool quote = false;
    bool inQualifier = false;
    bool done = false;
    bool recurse = false;
    while (end != str)
    {
        switch (*str) {
        case '"':
            if (quote) quote = false;
            else quote = true;
            break;
        case '[':
            if (inQualifier)
            {
                if (!quote) 
                    throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, str-xpath, "Unclosed qualifier detected");
            }
            else
                inQualifier = true;
            break;
        case ']':
            if (inQualifier)
            {
                if (!quote)
                    inQualifier = false;
            }
            else if (!quote)
                    throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, str-xpath, "Unopened qualifier detected");
            break;
        case '/':
            if (!quote && !inQualifier)
            {
                if ('/' == *(str+1))
                    recurse = true;
                done = true;
            }
            break;
        }
        if (done) break;
        ++str;
    }
    const char *tail;
    if (str==end) // top-level matches
    {
        head.append(xpath);
        if (0 == head.length())
        {
            matchContainer = createPTree(xpath);
            return;
        }
        tail = NULL;
    }
    else
    {
        head.append(str-xpath, xpath);
        if (recurse)
            tail = str+2;
        else
            tail = str+1;
    }

    Owned<IPropertyTreeIterator> parentIter = parentContext.getElements(head.str());
    Owned<IPropertyTree> matchParent;
    ForEach (*parentIter)
    {
        IPropertyTree &parent = parentIter->query();
        if (!matchParent)
            matchParent.setown(createPTree(parentContext.queryName()));
        if (tail && *tail)
        {
            IPropertyTree *childContainer = NULL;
            getXPathMatchTree(parent, tail, childContainer);
            if (childContainer)
            {
                if (!head.length())
                    matchParent.setown(childContainer);
                else
                {
                    unsigned pos = ((PTree &)parentContext).findChild(&parent);
                    matchParent->addPropTree(childContainer->queryName(), childContainer);
                    childContainer->setPropInt("@pos", pos+1);
                }
                if (!matchContainer)
                    matchContainer = LINK(matchParent);
            }
            if (recurse)
            {
                Owned<IPropertyTreeIterator> iter = parent.getElements("*");
                ForEach (*iter)
                {
                    IPropertyTree *childContainer = NULL;
                    IPropertyTree &child = iter->query();
                    getXPathMatchTree(child, xpath, childContainer);
                    if (childContainer)
                    {
                        unsigned pos = ((PTree &)parent).findChild(&child);
                        matchParent->addPropTree(childContainer->queryName(), childContainer);
                        childContainer->setPropInt("@pos", pos+1);
                        if (!matchContainer)
                            matchContainer = LINK(matchParent);
                    }
                }
            }
        }
        else
        {
            if (&parent != &parentContext)
            {
                IPropertyTree *childContainer = matchParent->addPropTree(parent.queryName(), createPTree());
                unsigned pos = ((PTree &)parentContext).findChild(&parent);
                childContainer->setPropInt("@pos", pos+1);
            }
            if (!matchContainer)
                matchContainer = LINK(matchParent);
        }
    }
}

IPropertyTree *getXPathMatchTree(IPropertyTree &parent, const char *xpath)
{
    IPropertyTree *matchTree = NULL;
    getXPathMatchTree(parent, xpath, matchTree);
    return matchTree;
}

void PTree::serializeAttributes(MemoryBuffer &tgt)
{
    IAttributeIterator *aIter = getAttributes();
    if (aIter->first())
    {
        do
        {
            tgt.append(aIter->queryName());
            tgt.append(aIter->queryValue());
        }
        while (aIter->next());
    }
    tgt.append(""); // attribute terminator. i.e. blank attr name.
    aIter->Release();
}

void PTree::serializeSelf(MemoryBuffer &tgt)
{
    tgt.append(name ? name->get() : "");
    tgt.append(flags);
    serializeAttributes(tgt);
    if (value)
        value->serialize(tgt);
    else
        tgt.append((size32_t)0);
}

void PTree::serializeCutOff(MemoryBuffer &tgt, int cutoff, int depth)
{
    serializeSelf(tgt);

    if (-1 == cutoff || depth<cutoff)
    {
        Owned<IPropertyTreeIterator> iter = getElements("*");
        if (iter->first())
        {
            do
            {
                IPropertyTree *_child = &iter->query();
                PTree *child = QUERYINTERFACE(_child, PTree); assertex(child);
                child->serializeCutOff(tgt, cutoff, depth+1);
            }
            while (iter->next());
        }
    }
    tgt.append(""); // element terminator. i.e. blank child name.
}

// serializable impl.
void PTree::serialize(MemoryBuffer &tgt)
{
    serializeCutOff(tgt, -1, 0);
}

void PTree::deserialize(MemoryBuffer &src)
{
    deserializeSelf(src);

    StringAttr eName;
    loop
    {
        size32_t pos = src.getPos();
        src.read(eName);
        if (!eName.length())
            break;
        src.reset(pos); // reset to re-read tree name
        IPropertyTree *child = create(src);
        addPropTree(eName, child);
    }
}

void PTree::deserializeSelf(MemoryBuffer &src)
{
    setName(NULL);  // needs to be cleared before flags changed
    StringAttr _name;
    src.read(_name);
    src.read(flags);
    if (_name[0]==0) 
        setName(NULL);
    else
        setName(_name);
    attributes.setNoCase(isnocase());
    StringAttr attrName, attrValue;
    loop
    {
        src.read(attrName);
        if (!attrName.length())
            break;
        src.read(attrValue);
        setProp(attrName, attrValue);
    }

    size32_t size;
    unsigned pos = src.getPos();
    src.read(size);
    if (value) delete value;
    if (size)
    {
        src.reset(pos); 
        value = new CPTValue(src);
    }
    else value = NULL;
}

void PTree::init()
{
    flags = 0;
    name = NULL;
    value = NULL;
    children = NULL;
    parent = NULL;
}

void PTree::clear()
{
    attributes.kill(); 
    if (children) { children->Release(); children = NULL; }
    if (value) { delete value; value = NULL; }  
}

IPropertyTree *PTree::clone(IPropertyTree &srcTree, bool self, bool sub)
{
    IPropertyTree *_dstTree = self ? this : create(srcTree.queryName());
    PTree *dstTree = QUERYINTERFACE(_dstTree, PTree);
    assertex(dstTree);
    dstTree->setName(srcTree.queryName());
    clone(srcTree, *dstTree, sub);
    return _dstTree;
}

void PTree::clone(IPropertyTree &srcTree, IPropertyTree &dstTree, bool sub)
{
    PTree *_dstTree = QUERYINTERFACE((&dstTree), PTree); assertex(_dstTree); //JCSMORE
    flags = _dstTree->flags;
    if (srcTree.isBinary(NULL))
    {
        MemoryBuffer mb;
        verifyex(srcTree.getPropBin(NULL, mb));
        dstTree.setPropBin(NULL, mb.length(), mb.toByteArray());
    }
    else if (srcTree.isCompressed(NULL))
    {
        StringBuffer s;
        verifyex(srcTree.getProp(NULL, s));
        dstTree.setProp(NULL, s.toCharArray());
    }
    else
        dstTree.setProp(NULL, srcTree.queryProp(NULL));

    IAttributeIterator *attrs = srcTree.getAttributes();
    if (attrs->first())
    {
        do
        {
            dstTree.setProp(attrs->queryName(), attrs->queryValue());
        }
        while (attrs->next());
    }
    attrs->Release();

    if (sub)
    {
        Owned<IPropertyTreeIterator> iter = srcTree.getElements("*");
        if (iter->first())
        {
            do
            {
                IPropertyTree &child = iter->query();
                IPropertyTree *newChild = clone(child, false, sub);
                StringAttr name(newChild->queryName());
                dstTree.addPropTree(name, newChild);
            }
            while (iter->next());
        }
    }
}

IPropertyTree *PTree::ownPTree(IPropertyTree *tree)
{
    if (!isEquivalent(tree) || tree->IsShared() || isCaseInsensitive() != tree->isCaseInsensitive())
    {
        IPropertyTree *newTree = clone(*tree);
        tree->Release();
        return newTree;
    }
    else
        return tree;
}

IPropertyTree *PTree::queryCreateBranch(IPropertyTree *branch, const char *prop, bool *newBranch)
{
    IPropertyTree *childBranch = branch->queryPropTree(prop);
    if (!childBranch)
    {
        if (newBranch) *newBranch = true;
        childBranch = create(prop);
        branch->setPropTree(prop, childBranch);
    }
    else if (newBranch) *newBranch = false;
    return childBranch;
}

IPropertyTree *PTree::splitBranchProp(const char *xpath, const char *&prop, bool error)
{
    prop = splitXPathX(xpath);
    MAKE_LSTRING(path, xpath, prop-xpath);
    IPropertyTree *branch = queryPropTree(path);
    if (!branch && error)
        throw MakeIPTException(-1, "path %s not found, when setting prop %s", path, xpath);
    return branch;
}

IPropertyTree *_createPropBranch(IPropertyTree *tree, const char *xpath, bool createIntermediates, IPropertyTree *&created, IPropertyTree *&createdParent)
{
    const char *prop;
    StringBuffer path;
    prop = splitXPathUQ(xpath, path);
    IPropertyTree *branch = tree->queryPropTree(path.str());
    if (!branch)
    {
        if (path.length() == strlen(xpath))
            throw MakeIPTException(-1, "createPropBranch: cannot create path : %s", xpath);
        if (!createIntermediates)
            throw MakeIPTException(-1, "createPropBranch: no path found for : %s", path.str());
    
        if ('/' == path.charAt(path.length()-1))
            path.remove(path.length()-1, 1);
        branch = _createPropBranch(tree, path.str(), createIntermediates, created, createdParent);
        assertex(branch);
    }
    if (prop && '\0' != *prop && '@' != *prop)
    {
        IPropertyTree *_branch = branch->queryPropTree(prop);
        if (_branch)
            branch = _branch;
        else
        {
            IPropertyTree *p = branch;
            branch = branch->addPropTree(prop, createPTree());
            if (!created) { created = branch; createdParent = p; }
        }
    }
    return branch;
}

IPropertyTree *createPropBranch(IPropertyTree *tree, const char *xpath, bool createIntermediates, IPropertyTree **created, IPropertyTree **createdParent)
{
    IPropertyTree *_created = NULL, *_createdParent = NULL;
    try
    {
        IPropertyTree *ret = _createPropBranch(tree, xpath, createIntermediates, _created, _createdParent);
        if (created) *created = _created;
        if (createdParent) *createdParent = _createdParent;
        return ret;
    }
    catch (...)
    {
        if (_created) (_createdParent)->removeTree(_created);
        throw;
    }
}

void PTree::addLocal(size32_t l, const void *data, bool _binary, int pos)
{
    if (!l) return; // right thing to do on addProp("x", NULL) ?
    IPTArrayValue *newValue = new CPTValue(l, data, _binary);
    Owned<IPropertyTree> tree = create(queryName(), newValue);
    PTree *_tree = QUERYINTERFACE(tree.get(), PTree); assertex(_tree); _tree->setParent(this);
    addingNewElement(*tree, pos);

    IPTArrayValue *array;
    if (value && value->isArray())
    {
        array = value;
        if (pos != -1 && ((unsigned)pos > array->elements()))
            throw MakeIPTException(-1, "Error trying to insert element at %d of %d", pos, array->elements());
    }
    else
    {
        if (pos > 0)
            throw MakeIPTException(-1, "Error trying to insert element at %d of 0", pos);
        array = new CPTArray();

        // detach children and attributes of this branch now owned by element of newly created array.
        IPropertyTree *tree = create(queryName(), value, children, true);
        PTree *_tree = QUERYINTERFACE(tree, PTree); assertex(_tree); _tree->setParent(this);
        ::Release(children);
        attributes.swap(_tree->attributes);
        children = NULL;
        addingNewElement(*tree, ANE_APPEND);
        array->addElement(tree);
        value = array;
    }
    tree->Link();
    if (-1 == pos)
        array->addElement(tree);
    else
        array->setElement(pos, tree);

    if (_binary)
        IptFlagSet(flags, ipt_binary);
    else
        IptFlagClr(flags, ipt_binary);
}

enum exprType { t_none, t_equality, t_inequality, t_lteq, t_lt, t_gt, t_gteq } tType;
inline bool match(bool wild, bool numeric, const char *xpath, exprType t, const char *value, unsigned len, const char *pat, unsigned patLen, bool nocase)
{
    int m;
    if (numeric)
    {
        __int64 lhsN = atoi64_l(value, len);
        __int64 rhsN = atoi64_l(pat, patLen);
        m = lhsN<rhsN?-1:lhsN>rhsN?1:0;
    }
    else if (wild)
        m = false==WildMatch(value, len, pat, patLen, nocase);
    else
    {
        if (len == patLen)
            m = nocase ? memicmp(value, pat, len) : memcmp(value, pat, len);
        else if (len < patLen)
            m = -1;
        else
            m = 1;
    }
    switch (t)
    {
        case t_inequality:
            return m!=0;
        case t_lt:
            return m<0;
        case t_lteq:
            return m<=0;
        case t_equality:
            return m==0;
        case t_gteq:
            return m>=0;
        case t_gt:
            return m>0;
    }
    throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, 0, "Invalid xpath qualifier expression in xpath: %s", xpath);
}

bool PTree::checkPattern(const char *&xxpath) const
{
    // Pattern is an additional filter at the current node level
    // It can be [condition], or it can be empty (we don't support anything else)
    // supported conditions are: 
    //    tag - must have child called tag
    //    @attr - must have attribute called attr
    //    tag="value" - must have child called tag with given value
    //    @attr="value" - must have attribute called attr with given value
    const char *xpath = xxpath;
    while (*xpath == ' ' || *xpath == '\t') xpath++;
    const char *start = xpath;
    bool wild = false, nocase = isnocase();
    if (*xpath=='@')
        xpath++;
    char quote = 0;
    const char *lhsEnd, *quoteBegin, *quoteEnd, *rhsBegin, *rhsEnd;
    lhsEnd = quoteBegin = quoteEnd = rhsBegin = rhsEnd = NULL;
    exprType tType = t_none;
    bool numeric=false;
#ifdef WARNLEGACYCOMPARE
    bool legacynumeric=false;
#endif
    loop
    {
        switch (*xpath) {
        case '"':
        case '\'':
            if (quote)
            {
                if (*xpath == quote)
                {
                    quote = 0;
                    quoteEnd = xpath;
                }
            }
            else
            {
                if (quoteBegin)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Quoted left hand side already seen");
                quote = *xpath;
                quoteBegin = xpath+1;
            }
            break;
        case '[':
            if (!quote)
                throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Unclosed qualifier detected");
            break;
        case ']':
            if (!quote)
            {
                if (!lhsEnd)
                    lhsEnd = xpath;
                rhsEnd = xpath;
            }
            break;
        case ' ':
        case '\t':
            if (!lhsEnd)
                lhsEnd = xpath;
            break;
        case '!':
            if (!quote)
            {
                if (tType)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Unexpected expression operator xpath");
                if ('=' != *(xpath+1))
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Invalid xpath qualifier expression in xpath");
                if (!lhsEnd)
                    lhsEnd = xpath;
                ++xpath;
                tType = t_inequality;
                wild = true; // true by default now, introduced  ~ syntax, to denote wild string
            }
            break;
        case '=':
            if (!quote)
            {
                if (wild)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Wildcard match '~' makes no sense in this context");
                if (tType)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Unexpected expression operator xpath");
                tType = t_equality;
                wild = true; // true by default now, introduced  ~ syntax, to denote wild string
                if (!lhsEnd)
                    lhsEnd = xpath;
            }
            break;
        case '>':
            if (!quote)
            {
                if (wild)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Wildcard match '~' makes no sense in this context");
                if (tType)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Unexpected expression operator in xpath");
                if (!lhsEnd)
                    lhsEnd = xpath;
#ifdef WARNLEGACYCOMPARE
                legacynumeric = true;
#endif
                if ('=' == *(xpath+1))
                {
                    ++xpath;
                    tType = t_gteq;
                }
                else
                    tType = t_gt;
            }
            break;
        case '<':
            if (!quote)
            {
                if (tType)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Unexpected expression operator in xpath");
                if (!lhsEnd)
                    lhsEnd = xpath;
#ifdef WARNLEGACYCOMPARE
                legacynumeric = true;
#endif
                if ('=' == *(xpath+1))
                {
                    ++xpath;
                    tType = t_lteq;
                }
                else
                    tType = t_lt;
            }
            break;
        case '~':
            if (!quote)
            {
                if (!tType)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Unexpected wild operator in xpath");
                wild = true;
            }
            break;
        case '?':
            if (!quote)
            {
                if (!tType)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Unexpected case-insensitive operator in xpath");
                nocase = true;
            }
            break;
        case '\0':
            rhsEnd = xpath;
            break;
        }
        if (rhsEnd)
            break;
        xpath++;
        if (!rhsBegin && tType && !isspace(*xpath))
            rhsBegin = xpath;
    }
    if (quote)
        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Parse error, unclosed quoted content");
    if (tType)
    {
        if (quoteBegin && !quoteEnd)
            throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Parse error, RHS missing closing quote");
        if (rhsBegin && !rhsEnd)
            throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Parse error, RHS missing closing quote");
        if (!quoteBegin && rhsEnd) // validate it's a numeric
        {
            const char *c = rhsBegin;
            loop
            {
                if (!isdigit(*c++))
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Parse error, RHS is an unquoted string");
                if (c==rhsEnd) break;
            }
        }
    }

    MAKE_LSTRING(lhs, start, lhsEnd-start);
    bool ret = false;

    const char *tProp = splitXPathX(lhs);
    MAKE_LSTRING(head, lhs, tProp-lhs);
    Owned<IPropertyTreeIterator> iter = getElements(head);
    ForEach (*iter)
    {
        IPropertyTree &found = iter->query();
        if (t_none == tType)
        {
            if (found.hasProp(tProp))
            {
                ret = true;
                break;
            }
        }
        else
        {
            Owned<IPropertyTreeIterator> _iter2;
            IPropertyTreeIterator *iter2;
            IPropertyTree *matchElem;
            if (isAttribute(tProp))
            {
                matchElem = &found;
                iter2 = NULL;
            }
            else
            {
                _iter2.setown(found.getElements(tProp));
                iter2 = _iter2;
                if (iter2->first())
                    matchElem = &iter2->query();
                else
                    continue;
                tProp = NULL;
            }
            loop
            {
                if (matchElem->isBinary(tProp))
                    UNIMPLEMENTED;
                const char *rhs;
                unsigned rhslength;
                if (quoteEnd)
                {
                    rhs = quoteBegin;
                    rhslength = quoteEnd-quoteBegin;
#ifdef WARNLEGACYCOMPARE
                    if (legacynumeric)
                    {
                        if (isdigit(*rhs))
                            WARNLOG("Possible deprecated use of quoted numeric comparison operation: %s", xxpath);
                    }
#endif
                }
                else if (rhsEnd)
                {
                    rhs = rhsBegin;
                    rhslength = rhsEnd-rhsBegin;
                    numeric = true;
                }
                else
                {
                    rhs = NULL;
                    rhslength = 0;
                }
                if (matchElem->isCompressed(tProp))
                {
                    StringBuffer s;
                    matchElem->getProp(tProp, s);
                    ret = match(wild, numeric, xxpath, tType, s.toCharArray(), s.length(), rhs, rhslength, nocase);
                }
                else
                {
                    const char *value = matchElem->queryProp(tProp);
                    if (value)
                        ret = match(wild, numeric, xxpath, tType, value, value?(size32_t)strlen(value):0, rhs, rhslength, nocase);
                    else if (tType == t_equality)
                        ret = (NULL == rhs || '\0' == *rhs);
                    else if (tType == t_inequality)
                        ret = (NULL != rhs && '\0' != *rhs);
                }
                if (ret)
                    break;
                if (!iter2 || !iter2->next())
                    break;
                matchElem = &iter2->query();
            }
            if (ret)
                break;
        }
    }
    xxpath = xpath;
    return ret;
}


bool isEmptyPTree(IPropertyTree *t)
{
    if (!t)
        return true;
    if (t->numUniq())
        return false;
    Owned<IAttributeIterator> ai = t->getAttributes();
    if (ai->first())
        return false;
    const char *s = t->queryProp(NULL);
    if (s&&*s)
        return false;
    return true;
}


///////////////////

PTLocalIteratorBase::PTLocalIteratorBase(const PTree *_tree, const char *_id, bool _nocase, bool _sort) : tree(_tree), id(_id), nocase(_nocase), sort(_sort)
{
    class CPTArrayIterator : public ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>
    {
    public:
        CPTArrayIterator(IPropertyTreeIterator &src) : ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>(elems)
        {
            ForEach(src)
                elems.append(src.get());
            elems.sort(comparePropTrees);
        }
        IArrayOf<IPropertyTree> elems;
    };
    tree->Link();
    baseIter = tree->checkChildren()->getIterator(sort);
    iter = NULL;
    current = NULL;
}

PTLocalIteratorBase::~PTLocalIteratorBase()
{
    baseIter->Release();
    ::Release(iter);
    tree->Release();
}

// IPropertyTreeIterator
bool PTLocalIteratorBase::first()
{
    ::Release(iter); iter=NULL;
    if (!baseIter || !baseIter->first()) return false;
    return _next();
}

bool PTLocalIteratorBase::_next()
{
    if (iter && iter->isValid() && iter->next())
        return true;

    loop
    {
        loop
        {
            if (!baseIter->isValid())
            {
                current = NULL;
                return false;
            }
            else if (match())
                break;
            baseIter->next();
        }
        IPropertyTree *element = &baseIter->query();
        baseIter->next();
        if (iter)
            iter->Release();
        iter = element->getElements(NULL);
        if (iter->first())
        {
            current = &iter->query();
            return true;
        }
    }
}

bool PTLocalIteratorBase::next()
{
    return _next();
}

bool PTLocalIteratorBase::isValid()
{
    return (current != NULL);
}

/////////////////////////////

bool PTIdMatchIterator::match()
{
    IPropertyTree &tree = baseIter->query();
    const char *key = tree.queryName();
    return (0 != WildMatch(key, id, nocase));
}

////////////////////////////

SingleIdIterator::SingleIdIterator(const PTree &_tree, unsigned pos, unsigned _many) : tree(_tree), many(_many), start(pos-1), whichNext(start), count(0), current(NULL)
{
    tree.Link();
}

SingleIdIterator::~SingleIdIterator()
{
    tree.Release();
}

void SingleIdIterator::setCurrent(unsigned pos)
{
    current = tree.value->queryElement(pos);
}

// IInterface impl.
bool SingleIdIterator::first()
{
    whichNext = start;
    if (!tree.value || !tree.value->isArray())
    {
        if (0 == whichNext)
        {
            current = const_cast<PTree*>(&tree);
            count = 1;
        }
    }
    else
    {
        count = tree.value->elements();
        if (whichNext < count)
            setCurrent(whichNext);
        else
            return false;
    }
    ++whichNext;            
    return true;
}

bool SingleIdIterator::next()
{
    if ((whichNext>=count) || (-1 != many && whichNext>start+many))
    {
        current = NULL;
        return false;
    }
    setCurrent(whichNext++);
    return true;
}

bool SingleIdIterator::isValid()
{
    return (NULL != current);
}

//////////////

class StackElement
{
public:
    void init(IPropertyTreeIterator *_iter, const char *_xpath) 
    {
        xpath = (char *)strdup(_xpath);
        iter=LINK(_iter); 
    }
    void clear()
    {
        ::Release(iter);
        if (xpath)
            free(xpath);
    }
    IPropertyTreeIterator *get(StringAttr &str) 
    { 
        str.setown(xpath); return iter; // NB used in place of pop, as element invalid after call
    } 
    IPropertyTreeIterator *iter;
    char * xpath;
};

///////////////////

PTStackIterator::PTStackIterator(IPropertyTreeIterator *_iter, const char *_xpath) : rootIter(_iter), xpath(_xpath)
{
    iter = NULL;
    xxpath = "";
    current = NULL;
    stacklen = 0;
    stackmax = 4;
    stack = (StackElement *)malloc(sizeof(StackElement)*stackmax);
}

PTStackIterator::~PTStackIterator()
{
    while (stacklen)
        stack[--stacklen].clear();
    ::Release(iter);
    ::Release(rootIter);
    free(stack);
}

void PTStackIterator::setIterator(IPropertyTreeIterator *_iter)
{
    assertex(_iter);
    if (iter)
        iter->Release();
    iter = _iter;
    iter->first();
}

// IIterator impl.
bool PTStackIterator::first() 
{
    while (stacklen)
        stack[--stacklen].clear();
    current = NULL;
    xxpath = xpath;
    rootIter->Link();
    setIterator(rootIter);
    return next();
}

bool PTStackIterator::isValid()
{
    return (current != NULL);
}

IPropertyTree &PTStackIterator::query()
{
    assertex(current);
    return *current;
}

bool PTStackIterator::next()
{
    bool separator = false;
    if (iter)
    {
        IPropertyTree *element = NULL;
        StringBuffer qualifierText;
        loop
        {
            while (!iter->isValid())
            {
                if (iter) iter->Release();
                iter = popFromStack(stackPath); // leaves linked

                if (!iter)
                {
                    current = NULL;
                    return false;
                }
                xxpath = stackPath;
                element = NULL;
            }
            if (!element)
            {
                element = &iter->query();
                iter->next();
            }
            while (element)
            {
                switch (*xxpath)
                {
                case '\0':
                    current = element;
                    return true;
                case '.':
                    if (separator) throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, 0, "Syntax error");
                    separator=false;
                    ++xxpath;
                    if (*xpath && '/' != *xpath)
                        throw MakeXPathException(xpath-1, PTreeExcpt_XPath_Unsupported, 0, "");
                    break;
                case '/':
                    ++xxpath;
                    if ('/' == *xxpath)
                    {
                        --xxpath;
                        if (iter->isValid()) 
                            pushToStack(iter, xxpath);
                        setIterator(element->getElements(xxpath));
                        xxpath = "";
                        element = NULL;
                    }
                    separator=true;
                    break;
                default:
                    separator=false;
                    if (iter->isValid()) 
                        pushToStack(iter, xxpath);

                    bool wild;
                    const char *start = xxpath;
                    readWildIdIndex(xxpath, wild);
                    size32_t s = xxpath-start;
                    if (s)
                    {
                        qualifierText.clear().append(s, start);
                        setIterator(element->getElements(qualifierText));
                    }
                    else // must be qualifier.
                    {
                        if ('[' != *xxpath)
                            throw MakeXPathException(xxpath, PTreeExcpt_XPath_ParseError, 0, "Qualifier expected e.g. [..]");
                        const char *start = xxpath;
                        char quote = 0;
                        while (']' != *(++xxpath) || quote)
                        {
                            switch (*xxpath) {
                            case '\"':
                            case '\'':
                            {
                                if (quote)
                                {
                                    if (*xxpath == quote)
                                        quote = 0;
                                }
                                else
                                    quote = *xxpath;
                                break;
                            }
                            case '\0':
                                throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xxpath-start, "Qualifier brace unclosed");
                            }
                        }
                        ++xxpath;
                        qualifierText.clear().append(xxpath-start, start);
                        setIterator(element->getElements(qualifierText.str()));
                    }

                    element = NULL;
                    break;
                }
            }
        }
    }
    return false;
}

void PTStackIterator::pushToStack(IPropertyTreeIterator *iter, const char *xpath)
{
    if (stacklen==stackmax) {
        stackmax *= 2;
        stack = (StackElement *)realloc(stack, sizeof(StackElement)*stackmax);
    }
    stack[stacklen++].init(iter, xpath);
}

IPropertyTreeIterator *PTStackIterator::popFromStack(StringAttr &path)
{
    if (!stacklen) 
        return NULL;
    return stack[--stacklen].get(path);
}


// factory methods

IPropertyTree *createPTree(MemoryBuffer &src)
{
    IPropertyTree *tree = new LocalPTree();
    tree->deserialize(src);
    return tree;
}

IPropertyTree *createPTreeFromIPT(const IPropertyTree *srcTree, ipt_flags flags)
{
    Owned<PTree> tree = (PTree *)createPTree(NULL, flags);
    return tree->clone(*srcTree->queryBranch(NULL));
}

void mergePTree(IPropertyTree *target, IPropertyTree *toMerge)
{
    Owned<IAttributeIterator> aiter = toMerge->getAttributes();
    ForEach (*aiter)
        target->addProp(aiter->queryName(), aiter->queryValue());
    Owned<IPropertyTreeIterator> iter = toMerge->getElements("*");
    ForEach (*iter)
    {
        IPropertyTree &e = iter->query();
        target->addPropTree(e.queryName(), LINK(&e));
    }
}

void _synchronizePTree(IPropertyTree *target, IPropertyTree *source)
{
    Owned<IAttributeIterator> aiter = target->getAttributes();
    StringArray targetAttrs;
    ForEach (*aiter)
        targetAttrs.append(aiter->queryName());

    aiter.setown(source->getAttributes());
    ForEach (*aiter)
    {
        const char *attr = aiter->queryName();
        if (!target->hasProp(attr))
            target->setProp(attr, aiter->queryValue());
        else
        {
            const char *sValue = aiter->queryValue();
            const char *tValue = target->queryProp(attr);
            if (NULL == sValue)
            {
                if (NULL != tValue)
                    target->setProp(attr, sValue);
            }
            else if (NULL == tValue ||0 != strcmp(sValue, tValue))
                target->setProp(attr, sValue);

            targetAttrs.zap(attr);
        }
    }
    // remaining
    ForEachItemIn (a, targetAttrs)
        target->removeProp(targetAttrs.item(a));
    
    bool equal = true;
    MemoryBuffer srcMb;
    const char *src = NULL;
    if (target->isBinary())
    {
        MemoryBuffer tgtMb;
        target->getPropBin(NULL, tgtMb);
        source->getPropBin(NULL, srcMb);
        if (tgtMb.length() != srcMb.length())
            equal = false;
        else if (0 != memcmp(tgtMb.toByteArray(), srcMb.toByteArray(), tgtMb.length()))
            equal = false;
    }
    else
    {
        const char *tgt = target->queryProp(NULL);
        src = source->queryProp(NULL);
        unsigned lTgt = tgt?(size32_t)strlen(tgt):0;
        unsigned lSrc = src?(size32_t)strlen(src):0;
        if (lTgt != lSrc)
            equal = false;
        else if (0 != lTgt && (0 != strcmp(tgt, src)))
            equal = false;
    }
    if (!equal)
    {
        if (target->isBinary())
            target->setPropBin(NULL, srcMb.length(), srcMb.toByteArray());
        else
            target->setProp(NULL, src);
    }
    
    ICopyArrayOf<IPropertyTree> toProcess;
    Owned<IPropertyTreeIterator> iter = source->getElements("*");
    ForEach (*iter)
        toProcess.append(iter->query());
    iter.setown(target->getElements("*"));
    ICopyArrayOf<IPropertyTree> removeTreeList;
    Owned<IPropertyTreeIterator> srcTypeIter;
    StringAttr firstOfType;
    ForEach (*iter)
    {
        IPropertyTree &e = iter->query();
        const char *name = e.queryName();
        IPropertyTree *sourceCompare;
        if (!source->hasProp(name))
        {
            removeTreeList.append(e);
            firstOfType.clear();
            srcTypeIter.clear();
        }
        else
        {
            if (!firstOfType.length() || 0 != strcmp(firstOfType, e.queryName()))
            {
                if (firstOfType.length() && srcTypeIter)
                {
                    // add remaining
                    while (srcTypeIter->next())
                    {
                        sourceCompare =  &srcTypeIter->query();
                        target->addPropTree(sourceCompare->queryName(), LINK(sourceCompare));
                        toProcess.zap(*sourceCompare);
                    }
                }
                srcTypeIter.setown(source->getElements(e.queryName()));
                firstOfType.set(e.queryName());
                assertex(srcTypeIter->first());
                sourceCompare =  &srcTypeIter->query();
            }
            else // 2nd of type etc..
                sourceCompare = srcTypeIter->next() ? &srcTypeIter->query() : NULL;
            if (sourceCompare)
            {
                toProcess.zap(*sourceCompare);
                _synchronizePTree(&e, sourceCompare);
            }
            else
                removeTreeList.append(e);
        }
    }
    ForEachItemIn (rt, removeTreeList)
        target->removeTree(&removeTreeList.item(rt));

    // add unprocessed source elements, not reference by name in target
    ForEachItemIn (s, toProcess)
    {
        IPropertyTree &e = toProcess.item(s);
        target->addPropTree(e.queryName(), LINK(&e));
    }
}

// ensure target is equivalent to source whilst retaining elements already present in target.
// presevers ordering of matching elements.
void synchronizePTree(IPropertyTree *target, IPropertyTree *source)
{
    const char *srcName = source->queryName();
    const char *tgtName = target->queryName();
    if (0 != strcmp(srcName, tgtName))
        throw MakeIPTException(PTreeExcpt_Unsupported, "Cannot synchronize if root nodes mismatch");
    _synchronizePTree(target, source);
}

IPropertyTree *ensurePTree(IPropertyTree *root, const char *xpath)
{
    return createPropBranch(root, xpath, true);
}

IPTreeReadException *createPTreeReadException(int code, const char *msg, const char *context, unsigned line, offset_t offset)
{
    class jlib_thrown_decl CPTreeReadException : public CInterface, implements IPTreeReadException
    {
        int code;
        StringAttr msg;
        StringAttr context;
        unsigned line;
        offset_t offset;

        StringBuffer &getErrorMessage(StringBuffer &out) const
        {
            switch (code)
            {
                case PTreeRead_EOS:
                    return out.append("Error - end of stream");
                case PTreeRead_syntax:
                    return out.append("Error - syntax error");
            }
            return out;
        }
    public:
        IMPLEMENT_IINTERFACE;

        CPTreeReadException(int _code, const char *_msg, const char *_context, unsigned _line, offset_t _offset) : code(_code), msg(_msg), context(_context), line(_line), offset(_offset) { }

        // IException
        int errorCode() const { return code; }
        StringBuffer &errorMessage(StringBuffer &str) const
        {
            getErrorMessage(str);
            if (msg.length())
                str.append(" \"").append(msg).append("\"");
            str.append(" [");
            if (line>1) // don't bother with line 1, there may be no line breaks.
                str.append("line ").append(line).append(", ");
            str.append("file offset ").append(offset).append("]");
            if (context.length())
                str.newline().append(context);
            return str;
        }
        MessageAudience errorAudience() const { return MSGAUD_user; }
    
        const char *queryDescription() { return msg; }
        unsigned queryLine() { return line; }
        offset_t queryOffset() { return offset; }
        const char *queryContext() { return context.get(); }
    };
    return new CPTreeReadException(code, msg, context, line, offset);
}

template <typename T>
class CommonReaderBase : public CInterface
{
    Linked<ISimpleReadStream> lstream;
    ISimpleReadStream *stream;
    bool bufOwned, nullTerm;
    byte *buf, *bufPtr;
    size32_t bufSize, bufRemaining;
protected:
    PTreeReaderOptions readerOptions;
    bool ignoreWhiteSpace, noRoot;
    Linked<IPTreeNotifyEvent> iEvent;
    offset_t curOffset;
    unsigned line;
    char nextChar;

private:
    void init()
    {
        ignoreWhiteSpace = 0 != ((unsigned)readerOptions & (unsigned)ptr_ignoreWhiteSpace);
        noRoot = 0 != ((unsigned)readerOptions & (unsigned)ptr_noRoot);
    }
    void resetState()
    {
        bufPtr = buf;
        nextChar = 0;
        if (nullTerm || stream)
            bufRemaining = 0;
        curOffset = 0;
        line = 0;
    }

public:
    CommonReaderBase(ISimpleReadStream &_stream, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _readerOptions, size32_t _bufSize=0) :
        readerOptions(_readerOptions), iEvent(&_iEvent), bufSize(_bufSize)
    {
        if (!bufSize) bufSize = 0x8000;
        buf = new byte[bufSize];
        bufRemaining = 0;
        curOffset = 0;
        bufOwned = true;
        nullTerm = false;
        lstream.set(&_stream);
        stream = &_stream; // for efficiency
        init();
        resetState();
    }
    CommonReaderBase(const void *_buf, size32_t bufLength, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _readerOptions) :
        readerOptions(_readerOptions), iEvent(&_iEvent)
    {
        bufSize = 0; // not used for direct reads
        stream = NULL;  // not used for direct reads
        bufRemaining = bufLength;
        nullTerm = false;
        buf = (byte *)_buf;
        bufOwned = false;
        init();
        resetState();
    }
    CommonReaderBase(const void *_buf, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _readerOptions) :
        readerOptions(_readerOptions), iEvent(&_iEvent)
    {
        bufSize = 0; // not used for direct reads
        stream = NULL;  // not used for direct reads
        curOffset = 0;
        bufRemaining = 0;
        nullTerm = true;
        buf = (byte *)_buf;
        bufOwned = false;
        init();
        resetState();
    }
    ~CommonReaderBase()
    {
        if (bufOwned)
            delete [] buf;
    }
    IMPLEMENT_IINTERFACE;
protected:
    virtual void reset()
    {
        resetState();
    }
    void rewind(size32_t n)
    {
        assertex(curOffset >= n);
        if (!n) return;
        curOffset -= n;
        size32_t d = (size32_t)(bufPtr-buf);
        if (n > d) n = d;
        if (!nullTerm)
            bufRemaining += n;
        loop
        {
            --bufPtr;
            if (!--n) break;
            if (10 == *bufPtr) --line;
        }
    }
    bool checkBOM()
    {
        bool unsupportedUnicode = false;
        bool utf8 = false;
        switch ((unsigned char)nextChar)
        {
        case 0xff:
            readNext();
            if (0xfe == (unsigned char)nextChar)
                unsupportedUnicode = true;
            break;
        case 0xfe:
            readNext();
            if (0xff == (unsigned char)nextChar)
                unsupportedUnicode = true;
            break;
        case 0xef:
            readNext();
            if (0xbb == (unsigned char)nextChar)
            {
                readNext();
                if (0xbf == (unsigned char)nextChar)
                    utf8 = true;
            }
            break;
        default:
            break;
        }
        if (utf8)
            return true;
        else if (unsupportedUnicode)
            error("Unsupported unicode detected in BOM header", false);
        return false;
    }
    inline void expecting(const char *str)
    {
        StringBuffer errorMsg("Expecting \"");
        error(errorMsg.append(str).append("\"").str());
    }
    inline void eos()
    {
        error("String terminator hit");
    }
    void match(const char *txt, const char *msg=NULL)
    {
        const char *c = txt;
        loop
        {
            if (*c == '\0') break;
            readNext();
            if (toupper(nextChar) != toupper(*c))
            {
                if (msg)
                    error(msg);
                throw c;
            }
            c++;
        }
    }
    void error(const char *msg=NULL, bool giveContext=true, PTreeReadExcptCode code=PTreeRead_syntax)
    {
        StringBuffer context;
        if (giveContext)
        {
            size32_t bufPos = (size32_t)(bufPtr-buf);
            unsigned preLen = std::min(40U, bufPos);
            size32_t bR = bufRemaining;
            if (nullTerm)
            {
                byte *tPtr = bufPtr;
                while (bR<40)
                {
                    if ('\0' == *tPtr++) break;
                    bR++;
                }
            }
            unsigned postLen = std::min(80-preLen, bR);
            const char *bufferContext = (const char *)(bufPtr - preLen);
            context.append(preLen, bufferContext);
            context.append("*ERROR*");
            context.append(postLen, bufferContext+preLen);
        }
        throw createPTreeReadException(code, msg, context.str(), line+1, curOffset);
    }
    inline void readNext()
    {
        if (!readNextToken())
            error("End of stream encountered whilst parsing", true, PTreeRead_EOS);
        curOffset++;
    }
    inline bool checkReadNext()
    {
        if (!readNextToken())
            return false;
        curOffset++;
        return true;
    }
    inline bool readNextToken();
    inline bool checkSkipWS()
    {
        while (isspace(nextChar)) if (!checkReadNext()) return false;
        return true;
    }
    inline void skipWS()
    {
        while (isspace(nextChar)) readNext();
    }
};

class CInstStreamReader { public: }; // only used to ensure different template definitions.
class CInstBufferReader { public: };
class CInstStringReader { public: };

template <> inline bool CommonReaderBase<CInstStreamReader>::readNextToken()
{
    // do own buffering, to have reasonable error context.
    if (0 == bufRemaining)
    {
        size32_t _bufRemaining = stream->read(bufSize, buf);
        if (!_bufRemaining)
            return false;
        bufRemaining = _bufRemaining;
        bufPtr = buf;
    }
    --bufRemaining;
    nextChar = *bufPtr++;
    if (10 == nextChar)
        line++;
    return true;
}

template <> inline bool CommonReaderBase<CInstBufferReader>::readNextToken()
{
    if (0 == bufRemaining)
        return false;
    --bufRemaining;
    nextChar = *bufPtr++;
    if (10 == nextChar)
        line++;
    return true;
}

template <> inline bool CommonReaderBase<CInstStringReader>::readNextToken()
{
    nextChar = *bufPtr++;
    if ('\0' == nextChar)
    {
        --bufPtr;
        return false;
    }
    if (10 == nextChar)
        line++;
    return true;
}

template <typename X>
class CXMLReaderBase : public CommonReaderBase<X>, implements IEntityHelper
{
    StringAttrMapping entityTable;
protected:
    bool ignoreNameSpaces;
    bool hadXMLDecl;

private:
    void init()
    {
        ignoreNameSpaces = 0 != ((unsigned) readerOptions & (unsigned)ptr_ignoreNameSpaces);
    }
    void resetState()
    {
        hadXMLDecl = false;
    }
public:
    typedef CommonReaderBase<X> PARENT;
    using PARENT::nextChar;
    using PARENT::readNext;
    using PARENT::expecting;
    using PARENT::match;
    using PARENT::error;
    using PARENT::skipWS;
    using PARENT::rewind;
    using PARENT::readerOptions;

    CXMLReaderBase(ISimpleReadStream &_stream, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _xmlReaderOptions, size32_t _bufSize=0)
        : CommonReaderBase<X>(_stream, _iEvent, _xmlReaderOptions, _bufSize)
    {
        init();
        resetState();
    }
    CXMLReaderBase(const void *_buf, size32_t bufLength, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _xmlReaderOptions)
        : CommonReaderBase<X>(_buf, bufLength, _iEvent, _xmlReaderOptions)
    {
        init();
        resetState();
    }
    CXMLReaderBase(const void *_buf, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _xmlReaderOptions)
        : CommonReaderBase<X>(_buf, _iEvent, _xmlReaderOptions)
    {
        init();
        resetState();
    }
    IMPLEMENT_IINTERFACE;
protected:
    virtual void reset()
    {
        resetState();
        PARENT::reset();
    }
    void readID(StringBuffer &id)
    {
        if (isValidXPathStartChr(nextChar))
        {
            loop
            {
                id.append(nextChar);
                readNext();
                if (!isValidXPathChr(nextChar)) break;
            }
        }
    }
    void skipString()
    {
        if ('"' == nextChar)
        {
            do { readNext(); } while ('"' != nextChar);
        }
        else if ('\'' == nextChar)
        {
            do { readNext(); } while ('\'' != nextChar);
        }
        else expecting("\" or '");
    }
    bool lookupRefValue(const char *name, StringBuffer &value)
    {
        StringAttr *val = entityTable.getValue(name);
        if (!val) return false;
        value.append(*val);
        return true;
    }
    void storeEntity(const char *name, const char *value)
    {
        entityTable.setValue(name, value);
    }
    void parseEntity()
    {
        try { match("NTITY"); }
        catch (const char *) { error("Bad syntax"); }
        readNext();
        skipWS();
        StringBuffer entityName;
        if ('%' != nextChar) 
        {
            readID(entityName);
            skipWS();
            if ('"' == nextChar)
            {
                StringBuffer refValue;
                loop
                {
                    readNext();
                    if (!nextChar || '"' == nextChar)
                        break;
                    if ('&' == nextChar)
                    {
                        readNext();
                        StringBuffer ref;
                        if ('#' == nextChar)
                        {
                            ref.append("&#");
                            loop
                            {
                                readNext();
                                if (!nextChar)
                                    expecting(";");
                                if (';' == nextChar) break;
                                ref.append(nextChar);
                            }
                            ref.append(";");
                            decodeXML(ref, refValue);
                        }
                        else
                        {
                            readID(ref);
                            if (';' != nextChar)
                                expecting(";");
                            if (!lookupRefValue(ref, refValue))
                            {
                                StringBuffer _ref("&");
                                _ref.append(ref).append(';');
                                decodeXML(ref, refValue); // try inbuilts
                            }
                        }
                    }
                    else
                        refValue.append(nextChar);
                }
                storeEntity(entityName, refValue);
            }
        }
        do { readNext(); }
        while (nextChar && nextChar != '>');
    }
    void parseIntSubset()
    {
        loop
        {
            readNext();
            skipWS();
            if (']'== nextChar) break;
            if ('<' == nextChar)
            {
                readNext();
                switch (nextChar)
                {
                    case '!':
                    {
                        readNext();
                        switch (nextChar)
                        {
                            case '-':
                                parseComment();
                                break;
                            case 'E':
                                parseEntity();
                                break;
                            default: // ignore anything else
                                do { readNext(); }
                                while (nextChar && nextChar != '>');
                                break;
                        }
                        break;
                    }
                    case '?':
                    {
                        StringBuffer pi;
                        parsePI(pi);
                        break;
                    }
                }
            }
        }
    }
    void parseOther()
    {
        switch (nextChar)
        {
            case '-':
                parseComment2();
                break;
            case 'D':
            {
                try { match("OCTYPE"); }
                catch (const char *) { error("Bad syntax"); }
                readNext();
                skipWS();
                StringBuffer doctypeid;
                readID(doctypeid);
                loop
                {
                    skipWS();
                    if ('>' == nextChar) break;
                    if ('[' == nextChar)
                    {
                        parseIntSubset();
                        if (']' != nextChar)
                            expecting("]");
                    }
                    else if ('S'  == nextChar)
                    {
                        match("YSTEM");
                        readNext();
                        skipWS();
                        skipString();
                    }
                    else if ('P' == nextChar)
                    {
                        match("UBLIC");
                        readNext();
                        skipWS();
                        skipString();
                        readNext();
                        skipWS();
                        skipString();
                    }
                    readNext();
                }
                break;
            }
            default:
                error("Invalid information tag");
        }
    }
    void parsePIOrDecl()
    {
        StringBuffer target;
        parsePI(target);
        if (0 == strcmp("xml", target.str()))
        {
            if (hadXMLDecl)
                error("Only one XML declartion permitted");
            hadXMLDecl = true;
        }
    }
    void parseCData(StringBuffer &text)
    {
        try { match("CDATA["); }
        catch (const char *) { error("Bad CDATA syntax"); }
        loop
        {
            readNext();
            while (']' == nextChar)
            {
                char c1 = nextChar;
                readNext();
                if (']' == nextChar)
                {
                    char c2 = nextChar;
                    readNext();
                    if ('>' == nextChar)
                        return;
                    else
                        text.append(c1).append(c2);
                }
                else
                    text.append(c1);
            }
            text.append(nextChar);
        }           
    }
    void parsePI(StringBuffer &target)
    {
        readNext();
        if (!isValidXPathStartChr(nextChar))
            error("Invalid PI target");

        loop
        {
            target.append(nextChar);
            readNext();
            if (!isValidXPathChr(nextChar))
                break;
        }
        skipWS();
        unsigned closeTag=0;
        loop
        {
            if (!nextChar)
                error("Missing closing PI tag ?>");
            if (1 == closeTag)
            {
                if ('>' == nextChar)
                    break;
                closeTag = 0;
            }
            else if ('?' == nextChar)
                closeTag = 1;
            readNext();
        }
    }
    void parseDirective(StringBuffer &res)
    {
        readNext();
        switch (nextChar) {
        case '-':
            parseComment2();
            break;
        case '[':
            parseCData(res);
            break;
        default:
            error("Unrecognised syntax");
        }
    }
    void parseComment()
    {
        readNext();
        if (nextChar != '-') error("Bad comment syntax");
        parseComment2();
    }
    void parseComment2()
    {
        readNext();
        if (nextChar != '-') error("Bad comment syntax");
        readNext();
        unsigned seen = 0;
        while (nextChar)
        {
            if (seen==2)
            {
                if (nextChar=='>')
                    return;
                else if (nextChar != '-') // should be syntax error really.
                    seen = 0; 
            }
            else if (nextChar=='-')
                seen++;
            else
                seen = 0;
            readNext();
        }
        error("Bad comment syntax");
    }
    const char *_decodeXML(unsigned read, const char *startMark, StringBuffer &ret)
    {
        const char *errMark = NULL;
        try { return decodeXML(startMark, ret, &errMark, this); }
        catch (IException *e)
        {
            if (errMark)
                rewind((unsigned)(errMark-startMark));
            StringBuffer errMsg;
            e->errorMessage(errMsg);
            e->Release();
            error(errMsg.str());
        }
        return NULL; // will never get here.
    }

// IEntityHelper impl.
    virtual bool find(const char *entity, StringBuffer &value)
    {
        return lookupRefValue(entity, value);
    }
};


template <class X>
class CXMLReader : public CXMLReaderBase<X>, implements IPTreeReader
{
    bool rootTerminated;
    StringBuffer attrName, attrval;
    StringBuffer tmpStr;

    void init()
    {
        attrName.append('@');
    }
    void resetState()
    {
        rootTerminated = false;
    }

public:
    typedef CXMLReaderBase<X> PARENT;
    using PARENT::nextChar;
    using PARENT::readNext;
    using PARENT::expecting;
    using PARENT::match;
    using PARENT::error;
    using PARENT::skipWS;
    using PARENT::checkBOM;
    using PARENT::checkReadNext;
    using PARENT::checkSkipWS;
    using PARENT::eos;
    using PARENT::curOffset;
    using PARENT::noRoot;
    using PARENT::ignoreWhiteSpace;
    using PARENT::iEvent;
    using PARENT::parseDirective;
    using PARENT::parseOther;
    using PARENT::parsePI;
    using PARENT::parsePIOrDecl;
    using PARENT::parseComment;
    using PARENT::_decodeXML;
    using PARENT::ignoreNameSpaces;
    using PARENT::hadXMLDecl;

    IMPLEMENT_IINTERFACE;

    CXMLReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions, size32_t bufSize=0)
        : PARENT(stream, iEvent, xmlReaderOptions, bufSize)
    {
        init();
        resetState();
    }
    CXMLReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions)
        : PARENT(buf, bufLength, iEvent, xmlReaderOptions)
    {
        init();
        resetState();
    }
    CXMLReader(const void *buf, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions)
        : PARENT(buf, iEvent, xmlReaderOptions)
    {
        init();
        resetState();
    }

    virtual void reset()
    {
        resetState();
        PARENT::reset();
    }

// IPTreeReader
    virtual void load() { loadXML(); }
    virtual offset_t queryOffset() { return curOffset; }

    void loadXML()
    {
        bool head=true;
restart:
        if (!checkReadNext()) return;
        if (head)
        {
            head = false;
            if (checkBOM())
                if (!checkReadNext()) return;
        }
        if (!checkSkipWS()) return;
        if ('<' != nextChar)
            expecting("<");
        readNext();
        if ('!' == nextChar)
        {
            readNext();
            parseOther();
            goto restart;
        }
        else if ('?' == nextChar)
        {
            parsePIOrDecl();
            goto restart;
        }
        if (!noRoot && rootTerminated)
        {
            if (ignoreWhiteSpace)
                if (!checkSkipWS()) return;
            error("Trailing xml after close of root tag");
        }
        _loadXML();
        if (noRoot)
        {
            head = true;
            hadXMLDecl = false;
        }
        else
            rootTerminated = true;
        goto restart;
    }
    void _loadXML()
    {
restart:
        offset_t startOffset = curOffset-2;
        if ('!' == nextChar) // not sure this branch can ever be hit.
        {
            parseComment();
            readNext();
            if ('<' != nextChar)
                expecting("<");
            goto restart;
        }
        StringBuffer tagName;
        if (ignoreWhiteSpace)
            skipWS();
        while (!isspace(nextChar) && nextChar != '>' && nextChar != '/')
        {
            tagName.append(nextChar);
            readNext();
            if ('<' == nextChar)
                error("Unmatched close tag encountered");
        }
        StringBuffer completeTagname = tagName;
        if (ignoreNameSpaces)
        {
            const char *colon;
            if ((colon = strchr(tagName.str(), ':')) != NULL)
                tagName.remove(0, (size32_t)(colon - tagName.str() + 1));
        }
        iEvent->beginNode(tagName.toCharArray(), startOffset);
        skipWS();
        bool endTag = false;
        bool base64 = false;
        while(nextChar != '>')
        {
            skipWS();
            if (nextChar=='/')
            {
                readNext();
                if (nextChar != '>')
                    expecting(">");
                endTag = true;
                break;
            }

            attrName.setLength(1);
            attrval.clear();
            while (nextChar && !isspace(nextChar) && nextChar != '=' && nextChar != '>' && nextChar != '/')
            {
                attrName.append(nextChar);
                readNext();
            }
            skipWS();
            if (nextChar == '=') readNext(); else expecting("=");
            skipWS();
            if (nextChar == '"')
            {
                readNext();
                while (nextChar != '"')
                {
                    if (!nextChar)
                        eos();
                    attrval.append(nextChar);
                    readNext();
                }
            }
            else if (nextChar == '\'')
            {
                readNext();
                while (nextChar != '\'')
                {
                    attrval.append(nextChar);
                    readNext();
                }
            }
            else 
                error();

            _decodeXML(0, attrval.str(), tmpStr.clear());

            if (0 == strcmp(attrName.str(), "@xsi:type") &&
               (0 == stricmp(tmpStr.str(),"SOAP-ENC:base64")))
               base64 = true;
            else
                iEvent->newAttribute(attrName.str(), tmpStr.str());
            readNext();
            skipWS();
        }
        iEvent->beginNodeContent(tagName.toCharArray());
        StringBuffer tagText;
        bool binary = base64;
        if (!endTag)
        {
            if (nextChar == '>')
            {
                loop
                {
                    loop
                    {
                        readNext();
                        if (ignoreWhiteSpace)
                            skipWS();
                        if ('\0' == nextChar)
                            eos();
                        StringBuffer mark;
                        while (nextChar && nextChar !='<') { mark.append(nextChar); readNext(); }
                        size32_t l = mark.length();
                        size32_t r = l+1;
                        if (l)
                        {
                            if (ignoreWhiteSpace)
                            {
                                while (l-- && isspace(mark.charAt(l)));
                                mark.setLength(l+1);
                            }
                            tagText.ensureCapacity(mark.length());
                            _decodeXML(r, mark.toCharArray(), tagText);
                        }
                        readNext();
                        if ('!' == nextChar)
                            parseDirective(tagText);
                        else if ('?' == nextChar)
                        {
                            parsePI(tmpStr.clear());
#ifdef STRICT_PI
                            if (0 == stricmp(tmpStr.str(), "xml"))
                                error("Reserved PI target used");
#endif
                        }
                        else
                            break;
                    }
                    if (nextChar=='/')
                    {
                        if (base64)
                        {
                            JBASE64_Decode(tagText.str(), tmpStr.clear());
                            tagText.swapWith(tmpStr);
                        }
                        else
                        {
                            if (strlen(tagText.str()) != tagText.length())
                                binary = true;
                        }
                        break; // exit 
                    }
                    else
                        _loadXML();
                }
                readNext();
                unsigned i = 0;
                while (!isspace(nextChar) && nextChar != '>')
                {
                    if ((i >= completeTagname.length()) ||
                        (nextChar != completeTagname.charAt(i++)))
                        error("Mismatched opening and closing tags");
                    readNext();
                }
                if (i != completeTagname.length())
                    error("Mismatched opening and closing tags");
                skipWS();
                if (nextChar != '>')
                    expecting(">");
            }
        }
        iEvent->endNode(tagName.toCharArray(), tagText.length(), tagText.toCharArray(), binary, curOffset);
    }
};

template <class X>
class CPullXMLReader : public CXMLReaderBase<X>, implements IPullPTreeReader
{
    typedef CXMLReaderBase<X> PARENT;
    using PARENT::nextChar;
    using PARENT::readNext;
    using PARENT::expecting;
    using PARENT::match;
    using PARENT::error;
    using PARENT::skipWS;
    using PARENT::checkBOM;
    using PARENT::checkReadNext;
    using PARENT::checkSkipWS;
    using PARENT::eos;
    using PARENT::curOffset;
    using PARENT::noRoot;
    using PARENT::ignoreWhiteSpace;
    using PARENT::iEvent;
    using PARENT::parseDirective;
    using PARENT::parseOther;
    using PARENT::parsePI;
    using PARENT::parsePIOrDecl;
    using PARENT::parseComment;
    using PARENT::_decodeXML;
    using PARENT::ignoreNameSpaces;
    using PARENT::hadXMLDecl;

    class CStateInfo : public CInterface
    {
    public:
        CStateInfo() 
        {
            tag.ensureCapacity(15);
            binary = base64 = false;
        }
        inline void reset()
        {
            binary = base64 = false;
            tag.clear();
            tagText.clear();
        }

        const char *wnsTag;
        StringBuffer tag;
        StringBuffer tagText;
        bool binary, base64;
    };
    CopyCIArrayOf<CStateInfo> stack, freeStateInfo;

    CStateInfo *stateInfo;

    enum ParseStates { headerStart, tagStart, tagAttributes, tagContent, tagContent2, tagClose, tagEnd, tagMarker } state;
    bool endOfRoot;
    StringBuffer attrName, attrval, mark, tmpStr;

    void resetState()
    {
        stack.kill();
        state = headerStart;
        stateInfo = NULL;
        endOfRoot = false;
        attrName.append('@');
    }

public:
    IMPLEMENT_IINTERFACE;

    CPullXMLReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions, size32_t bufSize=0)
        : CXMLReaderBase<X>(stream, iEvent, xmlReaderOptions, bufSize)
    {
        resetState();
    }
    CPullXMLReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions)
        : CXMLReaderBase<X>(buf, bufLength, iEvent, xmlReaderOptions)
    {
        resetState();
    }
    CPullXMLReader(const void *buf, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions)
        : CXMLReaderBase<X>(buf, iEvent, xmlReaderOptions)
    {
        resetState();
    }

    ~CPullXMLReader()
    {
        ForEachItemIn(i, stack)
            delete &stack.item(i);
        ForEachItemIn(i2, freeStateInfo)
            delete &freeStateInfo.item(i2);
    }

// IPullPTreeReader
    virtual void load()
    {
        while (next()) {}
    }

    virtual void reset()
    {
        PARENT::reset();
        resetState();
    }

    virtual offset_t queryOffset() { return curOffset; }

    virtual bool next()
    {
        switch (state)
        {
            case headerStart:
            {
                if (!checkReadNext()) return false;
                if (checkBOM())
                    if (!checkReadNext()) return false;
                loop
                {
                    if (!checkSkipWS()) return false;
                    if ('<' != nextChar)
                        expecting("<");
                    readNext();
                    if ('!' == nextChar)
                    {
                        readNext();
                        parseOther();
                    }
                    else if ('?' == nextChar)
                        parsePIOrDecl();
                    else
                        break;
                    if (!checkReadNext()) return false;
                }
                state = tagStart;
                break;
            }
            case tagStart:
            {
                offset_t startOffset;
                loop
                {
                    if ('!' != nextChar)
                        break;
                    parseComment();
                    readNext();
                    if ('<' != nextChar)
                        expecting("<");
                }
                startOffset = curOffset-2;
                if (freeStateInfo.ordinality())
                {
                    stateInfo = &freeStateInfo.pop();
                    stateInfo->reset();
                }
                else
                    stateInfo = new CStateInfo;
                stack.append(*stateInfo);
                if ('/' == nextChar)
                    error("Unmatched close tag encountered");
                while (!isspace(nextChar) && nextChar != '>')
                {
                    stateInfo->tag.append(nextChar);
                    readNext();
                    if ('/' == nextChar) break;
                    if ('<' == nextChar)
                        error("Unmatched close tag encountered");
                }
                stateInfo->wnsTag = stateInfo->tag.str();
                if (ignoreNameSpaces)
                {
                    const char *colon;
                    if ((colon = strchr(stateInfo->wnsTag, ':')) != NULL)
                        stateInfo->wnsTag = colon+1;
                }
                endOfRoot = false;
                try
                {
                    iEvent->beginNode(stateInfo->wnsTag, startOffset);
                }
                catch (IPTreeException *pe)
                {
                    if (PTreeExcpt_InvalidTagName == pe->errorCode())
                    {
                        pe->Release();
                        StringBuffer msg("Expecting valid start tag, but got \"");
                        error(msg.append(stateInfo->wnsTag).append("\"").str());
                    }
                    throw;
                }
                state = tagAttributes;
                break;
            }
            case tagAttributes:
            {
                skipWS();
                bool base64 = false;
                if (nextChar == '>')
                    state = tagContent;
                else
                {
                    skipWS();
                    if (nextChar=='/')
                    {
                        readNext();
                        if (nextChar != '>')
                            expecting(">");
                        // no actual content
                        iEvent->beginNodeContent(stateInfo->wnsTag);
                        state = tagEnd;
                        break;
                    }

                    attrName.setLength(1);
                    attrval.clear();
                    while (nextChar && !isspace(nextChar) && nextChar != '=' && nextChar != '>' && nextChar != '/')
                    {
                        attrName.append(nextChar);
                        readNext();
                    }
                    skipWS();
                    if (nextChar == '=') readNext(); else expecting("=");
                    skipWS();
                    if (nextChar == '"')
                    {
                        readNext();
                        while (nextChar != '"')
                        {
                            if (!nextChar)
                                eos();
                            attrval.append(nextChar);
                            readNext();
                        }
                    }
                    else if (nextChar == '\'')
                    {
                        readNext();
                        while (nextChar != '\'')
                        {
                            attrval.append(nextChar);
                            readNext();
                        }
                    }
                    else 
                        error();

                    _decodeXML(0, attrval.str(), tmpStr.clear());

                    if (0 == strcmp(attrName.str(), "@xsi:type") &&
                       (0 == stricmp(tmpStr.str(),"SOAP-ENC:base64")))
                       stateInfo->base64 = true;
                    else
                        iEvent->newAttribute(attrName.str(), tmpStr.str());
                    readNext();
                    skipWS();
                }
                break;
            }
            case tagContent:
            {
                iEvent->beginNodeContent(stateInfo->wnsTag);
                if ('>' != nextChar)
                    state = tagEnd;
                else
                    state = tagContent2;
                break;
            }
            case tagContent2:
            {
                try
                {
                    loop
                    {
                        if (endOfRoot)
                        {
                            if (!checkReadNext()) return false;
                            if (!checkSkipWS()) return false;
                        }
                        else
                        {
                            readNext();
                            if (ignoreWhiteSpace)
                                skipWS();
                        }
                        if ('\0' == nextChar)
                            eos();
                        mark.clear();
                        state = tagMarker;
                        while (nextChar && nextChar !='<') { mark.append(nextChar); readNext(); }
                        if (!nextChar)
                            break;
                        size32_t l = mark.length();
                        size32_t r = l+1;
                        if (l && stateInfo)
                        {
                            if (ignoreWhiteSpace)
                            {
                                const char *tb = mark.toCharArray();
                                const char *t = tb+l-1;
                                if (isspace(*t))
                                {
                                    while (t != tb && isspace(*(--t)));
                                    mark.setLength((size32_t)(t-tb+1));
                                }
                            }   
                            stateInfo->tagText.ensureCapacity(mark.length());
                            _decodeXML(r, mark.toCharArray(), stateInfo->tagText);
                        }
                        if (endOfRoot && mark.length())
                        {
                            const char *m = mark.toCharArray();
                            const char *e = m+mark.length();
                            do { if (!isspace(*m++)) error("Trailing content after close of root tag"); }
                            while (m!=e);
                        }
                        readNext();
                        if ('!' == nextChar)
                        {
                            parseDirective(stateInfo->tagText);
                            state = tagContent2;
                        }
                        else if ('?' == nextChar)
                        {
                            parsePI(tmpStr.clear());
#ifdef STRICT_PI
                            if (0 == stricmp(tmpStr.str(), "xml"))
                                error("Reserved PI target used");
#endif
                            state = tagContent2;
                        }
                        else
                            break;
                    }
                }
                catch (IPTreeReadException *e)
                {
                    if (endOfRoot && PTreeRead_EOS == e->errorCode() && (state != tagContent2 && mark.length())) // only to provide more meaningful error
                    {
                        const char *m = mark.toCharArray();
                        const char *es = m+mark.length();
                        do
                        { 
                            if (!isspace(*m++))
                            {
                                e->Release();
                                error("Trailing content after close of root tag");
                            }
                        }
                        while (m!=es);
                    }
                    throw;
                }
                if ('/' == nextChar)
                {
                    if (endOfRoot && !noRoot)
                        error("Trailing tag close after close of root tag");
                    if (stateInfo->base64)
                    {
                        JBASE64_Decode(stateInfo->tagText.str(), tmpStr.clear());
                        stateInfo->tagText.swapWith(tmpStr);
                        stateInfo->binary = true;

                        // next state tagContent2 still
                    }
                    else
                    {
                        if (strlen(stateInfo->tagText.str()) != stateInfo->tagText.length())
                            stateInfo->binary = true;
                    }
                    state = tagClose;
                    break; // exit 
                }
                else
                {
                    if (endOfRoot && !noRoot)
                        error("Trailing tag open after close of root tag");
                    state = tagStart;
                }
                break;
            }
            case tagClose:
            {
                readNext();
                const char *t = stateInfo->tag.toCharArray();
                const char *te = t+stateInfo->tag.length();
                loop
                {
                    if (nextChar == '>' || isspace(nextChar))
                    {
                        if (t != te)
                            error("Mismatched opening and closing tags");
                        break;
                    }
                    else if (nextChar != *t++)
                        error("Mismatched opening and closing tags");
                    readNext();
                }
                skipWS();
                if (nextChar != '>')
                    expecting(">");
                state = tagEnd;
                break;
            }
            case tagEnd:
            {
                iEvent->endNode(stateInfo->wnsTag, stateInfo->tagText.length(), stateInfo->tagText.toCharArray(), stateInfo->binary, curOffset);
                freeStateInfo.append(*stateInfo);
                stack.pop();
                endOfRoot = 0==stack.ordinality();
                stateInfo = stack.ordinality()?&stack.tos():NULL;
                if (endOfRoot && noRoot)
                {
                    state = headerStart;
                    hadXMLDecl = false;
                    endOfRoot = false;
                }
                else
                    state = tagContent2;
                break;
            }
        }
        return true;
    }
};

IPTreeReader *createXMLStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions, size32_t bufSize)
{
    class CXMLStreamReader : public CXMLReader<CInstStreamReader>
    {
    public:
        CXMLStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions, size32_t bufSize=0) : CXMLReader<CInstStreamReader>(stream, iEvent, xmlReaderOptions, bufSize) { }
    };
    return new CXMLStreamReader(stream, iEvent, xmlReaderOptions, bufSize);
}

IPTreeReader *createXMLStringReader(const char *xml, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions)
{
    class CXMLStringReader : public CXMLReader<CInstStringReader>
    {
    public:
        CXMLStringReader(const void *xml, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions) : CXMLReader<CInstStringReader>(xml, iEvent, xmlReaderOptions) { }
    };
    if (NULL == xml)
        throw createPTreeReadException(PTreeRead_syntax, "Null string passed to createXMLStringReader", NULL, 0, 0);
    return new CXMLStringReader(xml, iEvent, xmlReaderOptions);
}

IPTreeReader *createXMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions)
{
    class CXMLBufferReader : public CXMLReader<CInstBufferReader>
    {
    public:
        CXMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions) : CXMLReader<CInstBufferReader>(buf, bufLength, iEvent, xmlReaderOptions) { }
    };
    return new CXMLBufferReader(buf, bufLength, iEvent, xmlReaderOptions);
}


IPullPTreeReader *createPullXMLStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions, size32_t bufSize)
{
    class CXMLStreamReader : public CPullXMLReader<CInstStreamReader>
    {
    public:
        CXMLStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions, size32_t bufSize=0) : CPullXMLReader<CInstStreamReader>(stream, iEvent, xmlReaderOptions, bufSize) { }
    };
    return new CXMLStreamReader(stream, iEvent, xmlReaderOptions, bufSize);
}

IPullPTreeReader *createPullXMLStringReader(const char *xml, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions)
{
    class CXMLStringReader : public CPullXMLReader<CInstStringReader>
    {
    public:
        CXMLStringReader(const void *xml, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions) : CPullXMLReader<CInstStringReader>(xml, iEvent, xmlReaderOptions) { }
    };
    return new CXMLStringReader(xml, iEvent, xmlReaderOptions);
}

IPullPTreeReader *createPullXMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions)
{
    class CXMLBufferReader : public CPullXMLReader<CInstBufferReader>
    {
    public:
        CXMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions) : CPullXMLReader<CInstBufferReader>(buf, bufLength, iEvent, xmlReaderOptions) { }
    };
    return new CXMLBufferReader(buf, bufLength, iEvent, xmlReaderOptions);
}

IPTreeMaker *createPTreeMaker(byte flags, IPropertyTree *root, IPTreeNodeCreator *nodeCreator)
{
    return new CPTreeMaker(flags, nodeCreator, root);
}

IPTreeMaker *createRootLessPTreeMaker(byte flags, IPropertyTree *root, IPTreeNodeCreator *nodeCreator)
{
    return new CPTreeMaker(flags, nodeCreator, root, true);
}


////////////////////////////
///////////////////////////
IPropertyTree *createPTree(ISimpleReadStream &stream, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IPTreeMaker> _iMaker;
    if (!iMaker)
    {
        iMaker = new CPTreeMaker(flags);
        _iMaker.setown(iMaker);
    }
    Owned<IPTreeReader> reader = createXMLStreamReader(stream, *iMaker, readFlags);
    reader->load();
    if (iMaker->queryRoot())
        return LINK(iMaker->queryRoot());
    else
        return iMaker->create(NULL);
}

IPropertyTree *createPTree(IFileIO &ifileio, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    OwnedIFileIOStream stream = createIOStream(&ifileio);
    return createPTree(*stream, flags, readFlags, iMaker);
}

IPropertyTree *createPTree(IFile &ifile, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    OwnedIFileIO ifileio = ifile.open(IFOread);
    if (!ifileio)
        throw MakeStringException(0, "Could not locate filename: %s", ifile.queryFilename());
    return createPTree(*ifileio, flags, readFlags, iMaker);
}

IPropertyTree *createPTreeFromXMLFile(const char *filename, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    OwnedIFile ifile = createIFile(filename);
    return createPTree(*ifile, flags, readFlags, iMaker);
}

IPropertyTree *createPTreeFromXMLString(const char *xml, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IPTreeMaker> _iMaker;
    if (!iMaker)
    {
        iMaker = new CPTreeMaker(flags);
        _iMaker.setown(iMaker);
    }
    Owned<IPTreeReader> reader = createXMLStringReader(xml, *iMaker, readFlags);
    reader->load();
    return LINK(iMaker->queryRoot());
}

IPropertyTree *createPTreeFromXMLString(unsigned len, const char *xml, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IPTreeMaker> _iMaker;
    if (!iMaker)
    {
        iMaker = new CPTreeMaker(flags);
        _iMaker.setown(iMaker);
    }
    Owned<IPTreeReader> reader = createXMLBufferReader(xml, len, *iMaker, readFlags);
    reader->load();
    return LINK(iMaker->queryRoot());
}

//////////////////////////
/////////////////////////

static void _toXML(const IPropertyTree *tree, IIOStream &out, unsigned indent, byte flags)
{
    const char *name = tree->queryName();
    if (!name) name = "__unnamed__";
    bool isBinary = tree->isBinary(NULL);
    bool inlinebody = true;
    if (flags & XML_Format) writeCharsNToStream(out, ' ', indent);
    writeCharToStream(out, '<');
    writeStringToStream(out, name);
    Owned<IAttributeIterator> it = tree->getAttributes(true);

    if (it->first())
    {
        unsigned attributeindent = indent+2+(size32_t)strlen(name);
        bool first = true;

        do
        {
            const char *key = it->queryName();
            if (!isBinary || stricmp(key, "@xsi:type")!=0)
            {
                if (first)
                {
                    if (flags & XML_Format) inlinebody = false;
                    first = false;
                    writeCharToStream(out, ' ');
                }
                else if ((flags & XML_Format) && it->count() > 3)
                {
                    writeStringToStream(out, "\n");
                    writeCharsNToStream(out, ' ', attributeindent);
                }
                else
                    writeCharToStream(out, ' ');

                writeStringToStream(out, key+1);
                if (flags & XML_SingleQuoteAttributeValues)
                    writeStringToStream(out, "='");
                else
                    writeStringToStream(out, "=\"");
                const char *val = it->queryValue();
                if (val)
                {
                    if (flags & XML_SanitizeAttributeValues)
                    {
                        if (strcmp(val, "0")==0 || strcmp(val, "1")==0 || stricmp(val, "true")==0 || stricmp(val, "false")==0 || stricmp(val, "yes")==0 || stricmp(val, "no")==0)
                            encodeXML(val, out, ENCODE_NEWLINES, (unsigned)-1, true);
                        else
                        {
                            writeCharsNToStream(out, '*', strlen(val));
                        }
                    }
                    else
                        encodeXML(val, out, ENCODE_NEWLINES, (unsigned)-1, true);
                }

                if (flags & XML_SingleQuoteAttributeValues)
                    writeCharToStream(out, '\'');
                else
                    writeCharToStream(out, '"');
            }
        }
        while (it->next());
    }
    Owned<IPropertyTreeIterator> sub = tree->getElements("*", 0 != (flags & XML_SortTags) ? iptiter_sort : iptiter_null);
    MemoryBuffer thislevelbin;
    StringBuffer _thislevel;
    const char *thislevel = NULL; // to avoid uninitialized warning
    bool empty;
    if (isBinary)
    {
        if (flags & XML_Format) inlinebody = false;
        writeStringToStream(out, " xsi:type=\"SOAP-ENC:base64\"");
        empty = (!tree->getPropBin(NULL, thislevelbin))||(thislevelbin.length()==0);
    }
    else
    {
        if (tree->isCompressed(NULL))
        {
            empty = false; // can't be empty if compressed;
            verifyex(tree->getProp(NULL, _thislevel));
            thislevel = _thislevel.toCharArray();
        }
        else
            empty = (NULL == (thislevel = tree->queryProp(NULL)));
    }
    if (sub->first())
    {
        if (flags & XML_Format) inlinebody = false;
    }
    else if (empty && !(flags & XML_Sanitize))
    {
        if (flags & XML_Format)
            writeStringToStream(out, "/>\n");
        else
            writeStringToStream(out, "/>");
        return;
    }
    writeCharToStream(out, '>');
    if (!inlinebody)
        writeStringToStream(out, "\n");

    for(; sub->isValid(); sub->next())
        _toXML(&sub->query(), out, indent+1, flags);
    if (!empty)
    {
        if (!inlinebody)
            writeCharsNToStream(out, ' ', indent+1);
        if (flags & XML_Sanitize)
        {
            // NOTE - we don't output anything for binary.... is that ok?
            if (thislevel)
            {
                if (strcmp(thislevel, "0")==0 || strcmp(thislevel, "1")==0 || stricmp(thislevel, "true")==0 || stricmp(thislevel, "false")==0 || stricmp(thislevel, "yes")==0 || stricmp(thislevel, "no")==0)
                    writeStringToStream(out, thislevel);
                else
                    writeCharsNToStream(out, '*', strlen(thislevel));
            }
        }
        else if (isBinary)
        {
            if (flags & XML_NoBinaryEncode64)
            {
                if (flags & XML_NoEncode)
                {
                    out.write(thislevelbin.length(), thislevelbin.toByteArray());
                }
                else
                {
                    const char * buff = static_cast<const char *>(thislevelbin.toByteArray());
                    const unsigned len = thislevelbin.length();
                    unsigned prefix = 0;
                    while ((prefix < len) && isspace(buff[prefix]))
                        prefix++;
                    encodeXML(buff, out, ENCODE_WHITESPACE, prefix, true);
                    if (prefix != len) {    // check not all spaces
                        unsigned suffix = len;
                        while (isspace(buff[suffix-1]))
                            suffix--;
                        encodeXML(buff+prefix, out, 0, suffix-prefix, true);
                        encodeXML(buff+suffix, out, ENCODE_WHITESPACE, len-suffix, true);
                    }
                }
            }
            else
                JBASE64_Encode(thislevelbin.toByteArray(), thislevelbin.length(), out);
        }
        else {
            if (flags & XML_NoEncode)
            {
                writeStringToStream(out, thislevel);
            }
            else
            {
                const char *m = thislevel;
                const char *p = m;
                while (isspace(*p)) 
                    p++;
                encodeXML(m, out, ENCODE_WHITESPACE, p-m, true);
                if (*p) {   // check not all spaces
                    const char *s = p+strlen(p);
                    while (isspace(*(s-1)))
                        s--;
                    assertex(s>p);
                    encodeXML(p, out, 0, s-p, true);
                    encodeXML(s, out, ENCODE_WHITESPACE, (unsigned)-1, true);
                }
            }
            
            if (!inlinebody)
                writeStringToStream(out, "\n");
        }
    }
    if (!inlinebody)
        writeCharsNToStream(out, ' ', indent);

    writeStringToStream(out, "</");
    writeStringToStream(out, name);
    if (flags & XML_Format)
        writeStringToStream(out, ">\n");
    else
        writeCharToStream(out, '>');
}

jlib_decl StringBuffer &toXML(const IPropertyTree *tree, StringBuffer &ret, unsigned indent, byte flags)
{
    class CAdapter : public CInterface, implements IIOStream
    {
        StringBuffer &out;
    public:
        IMPLEMENT_IINTERFACE;
        CAdapter(StringBuffer &_out) : out(_out) { }
        virtual void flush() { }
        virtual size32_t read(size32_t len, void * data) { UNIMPLEMENTED; return 0; }
        virtual size32_t write(size32_t len, const void * data) { out.append(len, (const char *)data); return len; }
    } adapter(ret);
    _toXML(tree->queryBranch(NULL), adapter, indent, flags);
    return ret;
}

void toXML(const IPropertyTree *tree, IIOStream &out, unsigned indent, byte flags)
{
    _toXML(tree, out, indent, flags);
}

void saveXML(const char *filename, const IPropertyTree *tree, unsigned indent, byte flags)
{
    OwnedIFile ifile = createIFile(filename);
    saveXML(*ifile, tree, indent, flags);
}

void saveXML(IFile &ifile, const IPropertyTree *tree, unsigned indent, byte flags)
{
    OwnedIFileIO ifileio = ifile.open(IFOcreate);
    if (!ifileio)
        throw MakeStringException(0, "saveXML: could not find %s to open", ifile.queryFilename());
    saveXML(*ifileio, tree, indent, flags);
}

void saveXML(IFileIO &ifileio, const IPropertyTree *tree, unsigned indent, byte flags)
{
    Owned<IIOStream> stream = createIOStream(&ifileio);
    stream.setown(createBufferedIOStream(stream));
    saveXML(*stream, tree, indent, flags);
}

void saveXML(IIOStream &stream, const IPropertyTree *tree, unsigned indent, byte flags)
{
    toXML(tree, stream, indent, flags);
}

static inline void skipWS(const char *&xpath)
{
    while (isspace(*xpath)) xpath++;
}

static void _validateXPathSyntax(const char *xpath);
static void validateQualifier(const char *&xxpath)
{
    const char *xpath = xxpath;
    bool wild = true; // true by default now, introduced  ~ syntax, to denote wild string
    const char *start = xpath;
    skipWS(xpath);
    const char *lhsStart = xpath;
    loop
    {
        switch (*xpath) {
        case ']':
        case '!':
        case '=':
        case '\0':
            break;
        default:
            if (!isspace(*xpath))
            {
                xpath++;
                continue;
            }
        }
        break;
    }
    StringAttr lhs(lhsStart, xpath-lhsStart);
    _validateXPathSyntax(lhs);
    skipWS(xpath);
    exprType tType = t_none;
    if ('=' == *xpath)
    {
         ++xpath;
         tType = t_equality;
    }
    else if ('!' == *xpath)
    {
        ++xpath;
        if (*xpath && '=' == *xpath)
        {
            tType = t_inequality;
            ++xpath;
        }
        else
            throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, 0, "Invalid qualifier expression");
    }
    if (t_none != tType)
    {
        skipWS(xpath);
        if ('~' == *xpath)
        {
            wild = true;
            ++xpath;
        }
        skipWS(xpath);
        char qu = *xpath;
        if (qu != '\'' && qu != '\"')
            throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, 0, "Syntax error - no opening \" or \'");
        const char *textStart = ++xpath;
        while (*xpath && *xpath != qu)
            xpath++;
        if (!*xpath)
            throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, 0, "Syntax error - no closing \" or \'");
        xpath++;
    }
    skipWS(xpath);
    if (']' != *xpath)
        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, 0, "No closing brace to qualifier");
    xxpath = xpath;
}

static void _validateXPathSyntax(const char *xpath)
{
    if (NULL == xpath || '\0' == *xpath)
        return;
    else
    {
        const char *_xpath = xpath;
restart:
        if (NULL == xpath || '\0' == *xpath)
            return;
        switch (*xpath)
        {
            case '.':
                ++xpath;
                goto restart;
            case '/':
                ++xpath;
                if ('/' == *xpath)
                {
                    _validateXPathSyntax(xpath+1);
                    return;
                }
                goto restart;
            case '[':
            {
                ++xpath;
                if (isdigit(*xpath))
                {
                    StringAttr index;
                    xpath = readIndex(xpath, index);
                    unsigned i = atoi(index.get());
                    if (i)
                    {
                    }
                    else
                    {
                        // should be syntax error
                    }
                    if (']' != *xpath)
                        throw MakeXPathException(_xpath, PTreeExcpt_XPath_ParseError, xpath-_xpath, "Qualifier brace unclosed");
                }
                else 
                    validateQualifier(xpath);
                ++xpath;
                break;
            }
            default:
            {
                bool wild;
                const char *start = xpath;
                readWildId(xpath, wild); // validates also
                size32_t s = xpath-start;
                if (s)
                {
                    StringAttr id(start, s);
                    if ('[' == *xpath) // check for local index not iterative qualifier.
                    {
                        const char *xxpath = xpath+1;
                        if (isdigit(*xxpath))
                        {
                            StringAttr idxstr;
                            xxpath = readIndex(xxpath, idxstr);
                            if (']' != *xxpath)
                                throw MakeXPathException(_xpath, PTreeExcpt_XPath_ParseError, xpath-_xpath, "Qualifier brace unclosed");
                            ++xxpath;
                            unsigned index = atoi(idxstr.get());
                            if (index)
                            {
                            }
                            xpath = xxpath; 
                        }
                    }
                }
                else if ('@' == *xpath)
                {
                    ++xpath;
                    const char *start = xpath;
                    readID(xpath, false);
                    size32_t s = xpath-start;
                    if (!s)
                        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Missing attribute?");
                    StringAttr id(start, s);
                    if (!validateXMLTag(id))
                        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Invalid xml tag: %s", id.get());
                    while (isspace(*xpath)) xpath++;
                    if ('\0' != *xpath)
                        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Cannot have embedded attribute within path (must be tail component)");
                }
                else
                {
                    if ('[' != *xpath)
                        throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, 0, "Qualifier expected e.g. [..]");
                    validateQualifier(xpath);
                }
                break;
            }
        }
    }

    if (*xpath == '\0' || (*xpath == '/' && '\0' == *(xpath+1)))
        return;
    else
        _validateXPathSyntax(xpath);
}

bool validateXPathSyntax(const char *xpath, StringBuffer *error)
{
    try
    {
        if (xpath && '/' == *xpath && *(xpath+1) != '/')
            throw MakeXPathException(xpath, PTreeExcpt_XPath_Unsupported, 0, "Root specifier \"/\" specifier is not supported");
        _validateXPathSyntax(xpath);
        return true;
    }
    catch (IException *e)
    {
        if (error)
            e->errorMessage(*error);
        e->Release();
        return false;
    }
}

static bool isContentXPath(const char *xpath, StringBuffer &head)
{
    unsigned l = xpath?strlen(xpath):0;
    const char *x = xpath+l-2;
    if (l>=2 && 0==strcmp(XMLTAG_CONTENT, x))
    {
        if (x != xpath)
            head.append(x-xpath, xpath);
        return true;
    }
    return false;
}

bool validateXMLParseXPath(const char *xpath, StringBuffer *error)
{
    if (!xpath || !*xpath)
        return true;
    StringBuffer head;
    if (isContentXPath(xpath, head))
    {
        if (head.length())
        {
            if ('/' == *xpath && '/' != *(xpath+1))
            {
                if (error)
                {
                    Owned<IException> e = MakeStringException(0, "Invalid extract xml text '<>' usage, xpath cannot from be absolute: %s", xpath);
                    e->errorMessage(*error);
                }
                return false;
            }
            return validateXPathSyntax(head.toCharArray(), error);
        }
        return true;
    }
    else
        return validateXPathSyntax('/' == *xpath && '/' != *(xpath+1) ? xpath+1 : xpath, error);
    return true;
}

bool areMatchingPTrees(IPropertyTree * left, IPropertyTree * right)
{
    if (left == right)
        return true;

    if (!left || !right)
        return false;

    bool isCaseInsensitive = left->isCaseInsensitive();
    const char * lname = left->queryName();
    const char * rname = right->queryName();
    if (!lname || !rname)
    {
        if (lname || rname)
            return false;
    }
    else if ((isCaseInsensitive ? stricmp(lname, rname) : strcmp(lname, rname)) != 0)
        return false;

    Owned<IAttributeIterator> leftAttrIter = left->getAttributes(true);
    Owned<IAttributeIterator> rightAttrIter = right->getAttributes(true);
    rightAttrIter->first();
    ForEach(*leftAttrIter)
    {
        if (!rightAttrIter->isValid()) return false;
        const char * lname = leftAttrIter->queryName();
        const char * rname = rightAttrIter->queryName();
        if ((isCaseInsensitive ? stricmp(lname, rname) : strcmp(lname, rname)) != 0)
            return false;
        if (strcmp(leftAttrIter->queryValue(), rightAttrIter->queryValue()) != 0)
            return false;
        rightAttrIter->next();
    }
    if (rightAttrIter->isValid()) return false;

    Owned<IPropertyTreeIterator> leftElemIter = left->getElements("*", iptiter_sort);
    Owned<IPropertyTreeIterator> rightElemIter = right->getElements("*", iptiter_sort);
    rightElemIter->first();
    ForEach(*leftElemIter)
    {
        if (!rightElemIter->isValid()) return false;
        if (!areMatchingPTrees(&leftElemIter->query(), &rightElemIter->query()))
            return false;
        rightElemIter->next();
    }
    if (rightElemIter->isValid()) return false;
    return true;
}


/////////////////////

static const char * skipWhitespace(const char * text)
{
    while ((*text==' ') || (*text=='\t'))
        text++;
    return text;
}

static const char * skipAsterisk(const char * text)
{
    if (*text=='*')
        return skipWhitespace(text+1);
    return text;
}

static const char * skipToNewline(const char * text)
{
    while (*text && (*text != '\r') && (*text != '\n'))
        text++;
    return text;
}

static const char * skipNewline(const char * text)
{
    if (*text == '\r')
        text++;
    if (*text == '\n')
        text++;
    return text;
}

void extractJavadoc(IPropertyTree * result, const char * text)
{
    //Skip a leading blank line
    text = skipWhitespace(text);
    text = skipNewline(text);

    //Now process each of the parameters...
    StringBuffer tagname;
    StringBuffer tagtext;
    loop
    {
        text = skipWhitespace(text);
        text = skipAsterisk(text);
        if ((*text == 0) || (*text == '@'))
        {
            if (tagtext.length())
            {
                if (tagname.length())
                    result->addProp(tagname.str(), tagtext.str());
                else
                    result->setProp("", tagtext.str());
                tagtext.clear();
            }

            if (*text == 0)
                return;

            text++;
            const char * start = text;
            while (isalnum(*text))
                text++;
            if (start != text)
                tagname.clear().append(text-start, start);
            text = skipWhitespace(text);
        }
        const char * start = text;
        text = skipToNewline(text);
        if (start != text)
        {
            if (tagtext.length())
                tagtext.append(" ");
            tagtext.append(text-start, start);
        }
        text = skipNewline(text);
    }
}


/////////////////////

#ifdef _DEBUG

jlib_decl void validatePTree()
{

    Owned<IPropertyTree> testTree = createPTreeFromXMLString(
"<ROOT>"    \
"   <E a=\"av1\" b=\"bv1\" c=\"cv1\"></E>"  \
"   <E a=\"av1\" b=\"bv1\" c=\"cv2\"></E>"  \
"   <E a=\"av2\" b=\"bv1\"></E>"            \
"       <SE c=\"cv1\"></SE>"                \
"   <E a=\"av1\" b=\"bv2\"></E>"            \
"   <E a=\"av2\" b=\"bv2\" c=\"cv3\">ev1</E>"   \
        "</ROOT>"
    );

    Owned<IPropertyTreeIterator> iter = testTree->getElements("E[@a=\"av1\"][@b=\"bv2\"]");
    unsigned c = 0;
    ForEach (*iter)
        ++c;
    assertex(1 == c);

    int v = strcmp("bv1", testTree->queryProp("E[@a=\"av1\"][2]/@b"));
    assertex(0 == v);
    v = strcmp("cv2", testTree->queryProp("E[@a=\"av1\"][@b=\"bv1\"][2]/@c"));
    assertex(0 == v);
    v = strcmp("cv2", testTree->queryProp("E[@a=\"av1\"][2]/@c"));
    assertex(0 == v);
    v = strcmp("ev1", testTree->queryProp("E[@a=\"av2\"][@c]"));
    assertex(0 == v);
}

jlib_decl void testValidateXPathSyntax()
{
    verifyex(validateXPathSyntax("@abc"));
    verifyex(validateXPathSyntax("prop"));
    verifyex(validateXPathSyntax("a/b"));
    verifyex(validateXPathSyntax("a/@b"));
    const char *s = "a[@a=\"blah\"]/b";
    verifyex(validateXPathSyntax(s));
    s = "a/b[@b=\"blah\"]";
    verifyex(validateXPathSyntax(s));
    verifyex(validateXPathSyntax(s));
    s = "a/b[b=\"blah\"]";
    verifyex(validateXPathSyntax(s));
    s = "a/b[a/b=\"blah\"]";
    verifyex(validateXPathSyntax(s));
    verifyex(validateXPathSyntax("a[1]/b[2]"));
    s = "a[b]/b[c=\"a\"]/c";
    verifyex(validateXPathSyntax(s));
    verifyex(validateXPathSyntax("//a/b/c"));

    verifyex(!validateXPathSyntax("a[b"));
    verifyex(!validateXPathSyntax("a["));
    verifyex(!validateXPathSyntax("a]"));
    verifyex(!validateXPathSyntax("a[b=blah]"));
    verifyex(!validateXPathSyntax("@a/b"));
    verifyex(!validateXPathSyntax("a[b[c]]"));



    verifyex(validateXMLParseXPath("<>"));
    verifyex(validateXMLParseXPath("a/b/c<>"));
    verifyex(validateXMLParseXPath("a/b/<>"));
    verifyex(validateXMLParseXPath("/a/b"));

    verifyex(!validateXMLParseXPath("a/b[\"]/<>"));
    verifyex(!validateXMLParseXPath("/<>"));
}

jlib_decl void testJdocCompare()
{
    Owned<IPropertyTree> t1 = createPTree();
    Owned<IPropertyTree> t2 = createPTree();
    Owned<IPropertyTree> t3 = createPTree();
    Owned<IPropertyTree> t4 = createPTree();
    Owned<IPropertyTree> t5 = createPTree();
    extractJavadoc(t1, "Defines a record that contains information about a person");
    extractJavadoc(t2, "Allows the name table to be filtered.\n\n@param ages\tThe ages that are allowed to be processed.\n\t\tbadForename Forname to avoid.\n\n@return\tthe filtered dataset.");
    extractJavadoc(t3, "Allows the name table to be filtered.\n\n@param ages\tThe ages that are allowed to be processed.\n\t\tbadForename Forname to avoid.\n\n@return\tthe filtered dataset.");
    extractJavadoc(t4, "Allows the name table to be filtered.\n\n@param ages\tThe ages that are allowed to be processed.\n\t\tbadForename Forname to avoid.\n\n@return\tthe filtered dataset.");
    extractJavadoc(t5, "Allows the name table to be filtered.\n\n@param ages\tThe ages that are allowed to be processed.\n\t\tbadForename Forname to avoid.\n\n@return\tthe filtered dataset.");
    IPropertyTree * t2c = t2->addPropTree("Child1", createPTree());
    extractJavadoc(t2c, "This is some child data\n\n@param  ages\tThe ages that are allowed to be processed.");
    IPropertyTree * t3c = t3->addPropTree("Child1", createPTree());
    extractJavadoc(t3c, "This is some child data\n\n@param  ages\tThe ages that are allowed to be processed.");
    IPropertyTree * t4c = t4->addPropTree("Child1", createPTree());
    extractJavadoc(t4c, "This is some child data\n\n@param  ages\tThe ages that are allowed to be processed, but differs.");
    IPropertyTree * t5c = t5->addPropTree("Child1", createPTree());
    extractJavadoc(t5c, "This is some child data\n\n@param  ages\tThe ages that are allowed to be processed.");
    t2->setProp("@childAttr", "1");
    t3->setProp("@childAttr", "1");
    t4->setProp("@childAttr", "1");
    t5->setProp("@childAttr", "2");

    verifyex(areMatchingPTrees(NULL, NULL));
    verifyex(!areMatchingPTrees(NULL, t1));
    verifyex(!areMatchingPTrees(t1, NULL));
    verifyex(areMatchingPTrees(t1, t1));
    verifyex(areMatchingPTrees(t2, t3));
    verifyex(!areMatchingPTrees(t2, t4));
    verifyex(!areMatchingPTrees(t2, t5));
}

#endif


class COrderedPTree : public PTree
{
    template <class BASECHILDMAP>
    class jlib_decl COrderedChildMap : public BASECHILDMAP
    {
        typedef COrderedChildMap<BASECHILDMAP> SELF;
        ICopyArrayOf<IPropertyTree> order;
    public:
        IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(IPropertyTree, constcharptr);

        COrderedChildMap<BASECHILDMAP>() : BASECHILDMAP() { }
        ~COrderedChildMap<BASECHILDMAP>() { SELF::kill(); }

        virtual unsigned numChildren() { return order.ordinality(); }
        virtual IPropertyTreeIterator *getIterator(bool sort)
        {
            class CPTArrayIterator : public ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>
            {
                IArrayOf<IPropertyTree> elems;
            public:
                CPTArrayIterator(ICopyArrayOf<IPropertyTree> &order, bool sort) : ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>(elems)
                {
                    ForEachItemIn(e, order)
                        elems.append(*LINK(&order.item(e)));
                    if (sort)
                        elems.sort(comparePropTrees);
                }
            };
            return new CPTArrayIterator(order, sort);
        }
        virtual bool set(const char *key, IPropertyTree *tree)
        {
            IPropertyTree *existing = find(*key);
            if (existing)
            {
                unsigned pos = order.find(*existing);
                BASECHILDMAP::set(key, tree);
                order.replace(*tree, pos);
            }
            else
            {
                BASECHILDMAP::set(key, tree);
                order.append(*tree);
            }
            return true;

        }
        virtual bool replace(const char *key, IPropertyTree *tree) // provides different semantics, used if element being replaced is not to be treated as deleted.
        {
            return set(key, tree);
        }
        virtual bool remove(const char *key)
        {
            IPropertyTree *child = BASECHILDMAP::find(*key);
            if (!child)
                return false;
            order.zap(*child);
            return BASECHILDMAP::removeExact(child);
        }
        virtual bool removeExact(IPropertyTree *child)
        {
            order.zap(*child);
            return BASECHILDMAP::removeExact(child);
        }
    };
public:
    COrderedPTree(const char *name=NULL, byte flags=ipt_none, IPTArrayValue *value=NULL, ChildMap *children=NULL)
        : PTree(name, flags|ipt_ordered, value, children) { }

    virtual bool isEquivalent(IPropertyTree *tree) { return (NULL != QUERYINTERFACE(tree, COrderedPTree)); }
    virtual IPropertyTree *create(const char *name=NULL, IPTArrayValue *value=NULL, ChildMap *children=NULL, bool existing=false)
    {
        return new COrderedPTree(name, flags, value, children);
    }
    virtual IPropertyTree *create(MemoryBuffer &mb)
    {
        IPropertyTree *tree = new COrderedPTree();
        tree->deserialize(mb);
        return tree;
    }
    virtual void createChildMap()
    {
        if (isnocase())
            children = new COrderedChildMap<ChildMapNC>();
        else
            children = new COrderedChildMap<ChildMap>();
    }
};

IPropertyTree *createPTree(byte flags)
{
    if (flags & ipt_ordered)
        return new COrderedPTree(NULL, flags);
    else
        return new LocalPTree(NULL, flags);
}

IPropertyTree *createPTree(const char *name, byte flags)
{
    if (flags & ipt_ordered)
        return new COrderedPTree(name, flags);
    else
        return new LocalPTree(name, flags);
}

typedef enum _ptElementType
{
    elementTypeUnknown,
    elementTypeNull,
    elementTypeString,
    elementTypeBool,
    elementTypeInteger,
    elementTypeReal,
    elementTypeObject,
    elementTypeArray
} ptElementType;

template <typename X>
class CJSONReaderBase : public CommonReaderBase<X>
{
public:
    typedef CommonReaderBase<X> PARENT;
    using PARENT::reset;
    using PARENT::nextChar;
    using PARENT::readNext;
    using PARENT::expecting;
    using PARENT::match;
    using PARENT::error;
    using PARENT::skipWS;
    using PARENT::rewind;
    using PARENT::ignoreWhiteSpace;

    CJSONReaderBase(ISimpleReadStream &_stream, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _readerOptions, size32_t _bufSize=0) :
      CommonReaderBase<X>(_stream, _iEvent, _readerOptions, _bufSize)
    {
    }
    CJSONReaderBase(const void *_buf, size32_t bufLength, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _readerOptions) :
        CommonReaderBase<X>(_buf, bufLength, _iEvent, _readerOptions)
    {
    }
    CJSONReaderBase(const void *_buf, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _readerOptions) :
        CommonReaderBase<X>(_buf, _iEvent, _readerOptions)
    {
    }
    ~CJSONReaderBase()
    {
    }

    IMPLEMENT_IINTERFACE;

protected:
    inline StringBuffer &appendChar(StringBuffer &id, char c)
    {
        int charlen = validJSONUtf8ChrLen(c);
        if (!charlen)
            error("invalid JSON character", true);
        id.append(nextChar);
        while (--charlen)
        {
            readNext();
            id.append(nextChar);
        }
        return id;
    }
    void readString(StringBuffer &value)
    {
        readNext();
        StringBuffer s;
        bool decode=false;
        while ('\"'!=nextChar)
        {
            if (nextChar=='\\')
                decode=true;
            appendChar(s, nextChar);
            readNext();
        }
        size32_t r = s.length();
        if (ignoreWhiteSpace)
            s.trimRight();
        if (decode)
            _decodeJSON(r, s.str(), value, s.length()+1);
        else
            value.swapWith(s);
    }
    void readName(StringBuffer &name)
    {
        if ('\"'!=nextChar)
            expecting("\"");
        readString(name);
        if (!name.length())
            error("empty JSON id");
        readNext();
        skipWS();
        if (':'!=nextChar)
            expecting(":");
        readNext();
    }
    ptElementType readValue(StringBuffer &value)
    {
        ptElementType type = elementTypeUnknown;
        switch (nextChar)
        {
        case '\"':
        {
            readString(value);
            type = elementTypeString;
            break;
        }
        case 't':
            match("rue", "Bad value");
            value.append("true");
            type = elementTypeBool;
            break;
        case 'f':
            match("alse", "Bad value");
            value.append("false");
            type = elementTypeBool;
            break;
        case 'n':
            match("ull", "Bad value");
            type = elementTypeNull;
            break;
        default:
            if (!isdigit(nextChar))
                error("Bad value");
            type = elementTypeInteger;
            while (isdigit(nextChar) || '.'==nextChar)
            {
                if ('.'==nextChar)
                    type = elementTypeReal;
                value.append(nextChar);
                readNext();
            }
            rewind(1);

            break;
        }
        return type;
    }

    const char *_decodeJSON(unsigned read, const char *startMark, StringBuffer &ret, unsigned len)
    {
        const char *errMark = NULL;
        try { return decodeJSON(startMark, ret, len, &errMark); }
        catch (IException *e)
        {
            if (errMark)
            {
                if (read>(unsigned)(errMark-startMark))
                    rewind((unsigned)(read - (errMark-startMark)));
                else
                    rewind((unsigned)(errMark-startMark));
            }
            StringBuffer errMsg;
            e->errorMessage(errMsg);
            e->Release();
            error(errMsg.str());
        }
        return NULL; // will never get here.
    }
};

template <class X>
class CJSONReader : public CJSONReaderBase<X>, implements IPTreeReader
{
    typedef CJSONReaderBase<X> PARENT;
    using PARENT::checkBOM;
    using PARENT::rewind;
    using PARENT::readNext;
    using PARENT::readValue;
    using PARENT::readName;
    using PARENT::checkReadNext;
    using PARENT::checkSkipWS;
    using PARENT::expecting;
    using PARENT::error;
    using PARENT::eos;
    using PARENT::_decodeJSON;

    using PARENT::skipWS;
    using PARENT::nextChar;
    using PARENT::curOffset;
    using PARENT::noRoot;
    using PARENT::ignoreWhiteSpace;
    using PARENT::iEvent;

//    StringBuffer tmpStr;

public:
    IMPLEMENT_IINTERFACE;

    CJSONReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions, size32_t bufSize=0)
        : PARENT(stream, iEvent, readerOptions, bufSize)
    {
    }
    CJSONReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
        : PARENT(buf, bufLength, iEvent, readerOptions)
    {
    }
    CJSONReader(const void *buf, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
        : PARENT(buf, iEvent, readerOptions)
    {
    }
    void readValueNotify(const char *name, bool skipAttributes)
    {
        StringBuffer value;
        readValue(value);
        if ('@'==*name)
        {
            if (!skipAttributes)
                iEvent->newAttribute(name, value.str());
            return;
        }

        iEvent->beginNode(name, curOffset);
        iEvent->beginNodeContent(name);
        iEvent->endNode(name, value.length(), value.str(), false, curOffset);

    }
    void readArray(const char *name)
    {
        if ('@'==*name)
            name++;
        readNext();
        skipWS();
        while (']' != nextChar)
        {
            switch (nextChar)
            {
            case '[':
                iEvent->beginNode(name, curOffset);
                iEvent->beginNodeContent(name);
                readArray(name);
                iEvent->endNode(name, 0, "", false, curOffset);
                break;
            case '{':
                readObject(name);
                break;
            default:
                readValueNotify(name, true);
                break;
            }
            readNext();
            skipWS();
            if (','==nextChar)
                readNext();
            else if (']'!=nextChar)
                error("expected ',' or ']'");
            skipWS();
        }
    }
    void readChild(const char *name, bool skipAttributes)
    {
        skipWS();
        switch (nextChar)
        {
        case '}':
            {
            VStringBuffer msg("named item with no value defined %s [%d]", name, (int) curOffset);
            error(msg.str());
            }
            break;
        case '{':
            readObject(name);
            break;
        case '[':
            readArray(name);
            break;
        default:
            readValueNotify(name, skipAttributes);
            break;
        }
    }

    void readObject(const char *name)
    {
        if ('@'==*name)
            name++;
        iEvent->beginNode(name, curOffset);
        readNext();
        skipWS();
        bool attributesFinalized=false;
        while ('}' != nextChar)
        {
            StringBuffer tagName;
            readName(tagName);
            //internal convention so we can convert to and from xml
            //values at top of object with names starting with '@' become ptree attributes
            if (*tagName.str()!='@')
                attributesFinalized=true;
            readChild(tagName.str(), attributesFinalized);
            readNext();
            skipWS();
            if (','==nextChar)
                readNext();
            else if ('}'!=nextChar)
                error("expected ',' or '}'");
            skipWS();
        }
        iEvent->endNode(name, 0, "", false, curOffset);
    }
    void loadJSON()
    {
        if (!checkReadNext())
            return;
        if (checkBOM() && !checkReadNext())
            return;
        if (!checkSkipWS())
            return;
        if (noRoot)
        {
            StringBuffer tagName;
            loop
            {
                switch (nextChar)
                {
                case '\"':  //treat named objects like we're in a noroot object
                    readName(tagName.clear());
                    readChild(tagName.str(), true);
                    break;
                case '{':  //treat unnamed objects like we're in a noroot array
                    readObject("__item__");
                    break;
                case '[':  //treat unnamed arrays like we're in a noroot array
                    iEvent->beginNode("__item__", curOffset);
                    readArray("__item__");
                    iEvent->endNode("__item__", 0, "", false, curOffset);
                    break;
                default:
                    expecting("{[ or \"");
                }
                if (!checkReadNext() || !checkSkipWS())
                    break;
                if (','!=nextChar)
                    expecting(",");
                readNext();
                skipWS();
            }
        }
        else
        {
            if ('{' == nextChar)
                readObject("__object__");
            else if ('[' == nextChar)
            {
                iEvent->beginNode("__array__", curOffset);
                readArray("__item__");
                iEvent->endNode("__array__", 0, "", false, curOffset);
            }
            else
                error("expected '{' or '['");
            if (checkReadNext() && checkSkipWS())
                error("trailing content after JSON closed");
        }
    }

// IPTreeReader
    virtual void load() { loadJSON(); }
    virtual offset_t queryOffset() { return curOffset; }
};

template <class X>
class CPullJSONReader : public CJSONReaderBase<X>, implements IPullPTreeReader
{
    typedef CJSONReaderBase<X> PARENT;
    using PARENT::checkBOM;
    using PARENT::rewind;
    using PARENT::readNext;
    using PARENT::readValue;
    using PARENT::readName;
    using PARENT::checkReadNext;
    using PARENT::checkSkipWS;
    using PARENT::expecting;
    using PARENT::error;
    using PARENT::eos;
    using PARENT::_decodeJSON;

    using PARENT::skipWS;
    using PARENT::nextChar;
    using PARENT::curOffset;
    using PARENT::noRoot;
    using PARENT::ignoreWhiteSpace;
    using PARENT::iEvent;

    class CStateInfo : public CInterface
    {
    public:
        CStateInfo()
        {
            tag.ensureCapacity(15);
            type = elementTypeUnknown;
            childCount = 0;
            wnsTag = NULL;
        }
        inline void reset()
        {
            wnsTag = NULL;
            tag.clear();
            tagText.clear();
            type = elementTypeUnknown;
            childCount = 0;
        }

        StringBuffer tag;
        StringBuffer tagText;
        ptElementType type;
        const char *wnsTag;
        unsigned childCount;
    };
    CopyCIArrayOf<CStateInfo> stack, freeStateInfo;

    CStateInfo *stateInfo;

    enum ParseStates { headerStart, nameStart, valueStart, itemStart, objAttributes, itemContent, itemEnd } state;
    bool endOfRoot;
    StringBuffer tag, value;

    void init()
    {
        state = headerStart;
        stateInfo = NULL;
        endOfRoot = false;
    }

    virtual void resetState()
    {
        stack.kill();
    }
public:
    IMPLEMENT_IINTERFACE;

    CPullJSONReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions, size32_t bufSize=0)
        : CJSONReaderBase<X>(stream, iEvent, readerOptions, bufSize)
    {
        init();
    }
    CPullJSONReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
        : CJSONReaderBase<X>(buf, bufLength, iEvent, readerOptions)
    {
        init();
    }
    CPullJSONReader(const void *buf, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
        : CJSONReaderBase<X>(buf, iEvent, readerOptions)
    {
        init();
    }

    ~CPullJSONReader()
    {
        ForEachItemIn(i, stack)
            delete &stack.item(i);
        ForEachItemIn(i2, freeStateInfo)
            delete &freeStateInfo.item(i2);
    }

    inline void checkDelimiter(const char *msg=",")
    {
        if (stateInfo && stateInfo->childCount > 0)
        {
            if (','!=nextChar)
                expecting(msg);
            readNext();
            skipWS();
        }
    }

    inline ptElementType getParentType()
    {
        if (stack.ordinality()<2)
            return stateInfo->type;
        return ((CStateInfo *)&stack.tos(1))->type;
    }

    void beginNode(const char *name, offset_t offset, ptElementType jsonType, bool notify=true)
    {
        if (stateInfo)
            stateInfo->childCount++;
        if (freeStateInfo.ordinality())
        {
            stateInfo = &freeStateInfo.pop();
            stateInfo->reset();
        }
        else
            stateInfo = new CStateInfo;
        stack.append(*stateInfo);
        stateInfo->type=jsonType;
        if (name)
            stateInfo->tag.set(name);
        else
            stateInfo->tag.swapWith(tag);
        stateInfo->wnsTag = stateInfo->tag.str();
        if (!notify)
            return;
        try
        {
            iEvent->beginNode(stateInfo->wnsTag, offset);
        }
        catch (IPTreeException *pe)
        {
            if (PTreeExcpt_InvalidTagName == pe->errorCode())
            {
                pe->Release();
                StringBuffer msg("Expecting valid start tag, but got \"");
                error(msg.append(name).append("\"").str());
            }
            throw;
        }
    }

    inline const char *arrayItemName()
    {
        if (stack.ordinality()>1)
            return stateInfo->wnsTag;
        return "__item__";
    }

    bool arrayItem(offset_t offset)
    {
        skipWS();
        switch (nextChar)
        {
        case ']':
            state=itemContent;
            if (stack.ordinality()>1)
                readNext();
            if (!endNode(curOffset, getParentType()==elementTypeArray))
                return false;
            break;
        case '{':
            state=objAttributes;
            readNext();
            beginNode(arrayItemName(), offset, elementTypeObject);
            break;
        case '[':
            state=valueStart;
            readNext();
            beginNode(arrayItemName(), offset, elementTypeArray, true);
            break;
        default:
            state=valueStart;
            ptElementType type = readValue(value.clear());
            readNext();
            beginNode(arrayItemName(), offset, type, true);
            stateInfo->tagText.swapWith(value);
            break;
        }
        return true;
    }

    void namedItem()
    {
        readName(tag.clear());
        skipWS();
        switch (nextChar)
        {
        case '}':
            error("unexpected object close marker");
        case ']':
            error("unexpected array close marker");
        case '{':
            state=objAttributes;
            readNext();
            beginNode(NULL, curOffset, elementTypeObject);
            break;
        case '[':
            readNext();
            beginNode(NULL, curOffset, elementTypeArray, false); //false because items present events, not the array
            arrayItem(curOffset); //so process the first item so every next() has event
            break;
        default:
            state=valueStart;
            ptElementType type = readValue(value.clear());
            readNext();
            beginNode(NULL, curOffset, type);
            stateInfo->tagText.swapWith(value);
            break;
        }
    }
    void rootItem()
    {
        if ('\"'==nextChar)
            namedItem();
        else if ('{'==nextChar || '['==nextChar)
            arrayItem(curOffset);
        else
            expecting("[{ or \"");
    }
    bool rootNext()
    {
        if (!noRoot || !checkReadNext() || !checkSkipWS())
            return false;
        if (','!=nextChar)
            expecting(",");
        return true;
    }
    void newAttribute()
    {
        readName(tag.clear());
        skipWS();
        readValue(value.clear());
        readNext();
        stateInfo->childCount++;
        iEvent->newAttribute(tag.str(), value.str());
    }
    bool endNode(offset_t offset, bool notify=true)
    {
        bool more = true;
        if (stack.ordinality()<2)
        {
            state = headerStart;
            more = rootNext();
        }
        if (notify)
        {
            if (stateInfo->type==elementTypeNull)
                iEvent->endNode(stateInfo->wnsTag, 0, "", false, offset);
            else
                iEvent->endNode(stateInfo->wnsTag, stateInfo->tagText.length(), stateInfo->tagText.toCharArray(), false, offset);
        }
        freeStateInfo.append(*stateInfo);
        stack.pop();
        stateInfo = (stack.ordinality()) ? &stack.tos() : NULL;
        return more;
    }

    // IPullPTreeReader
    virtual void load()
    {
        while (next()) {}
    }

    virtual void reset()
    {
        PARENT::reset();
        resetState();
    }

    virtual offset_t queryOffset() { return curOffset; }

    virtual bool next()
    {
        skipWS();
        switch (state)
        {
            case headerStart:
            {
                if (!checkReadNext())
                    return false;
                if (checkBOM())
                    if (!checkReadNext())
                        return false;
                if (!checkSkipWS())
                    return false;
                if (noRoot)
                    rootItem();
                else
                {
                    switch (nextChar)
                    {
                    case '{':
                        state=objAttributes;
                        readNext();
                        beginNode("__root__", curOffset, elementTypeObject);
                        break;
                    case '[':
                        state=valueStart;
                        readNext();
                        beginNode("__root__", curOffset, elementTypeArray);
                        break;
                    default:
                        expecting("{ or [");
                        break;
                    }
                }
                break;
            }
            case nameStart:
                namedItem();
                break;
            case objAttributes:
            {
                if ('}'==nextChar)
                {
                    state=itemEnd;
                    iEvent->beginNodeContent(stateInfo->wnsTag);
                    break;
                }
                checkDelimiter(", or }");
                if (nextChar != '\"')
                    expecting("\"");
                readNext();
                bool att = '@' == nextChar;
                rewind(2);
                readNext();
                if (att)
                    newAttribute();
                else
                {
                    state=itemContent;
                    stateInfo->childCount=0;
                    iEvent->beginNodeContent(stateInfo->wnsTag);
                }
                break;
            }
            case valueStart:
                state=itemContent;
                iEvent->beginNodeContent(stateInfo->wnsTag);
                break;
            case itemContent:
            {
                switch (stateInfo->type)
                {
                    case elementTypeBool:
                    case elementTypeString:
                    case elementTypeInteger:
                    case elementTypeReal:
                    case elementTypeNull:
                        return endNode(curOffset);
                        break;
                    case elementTypeArray:
                        if (']'!=nextChar)
                            checkDelimiter(", or ]");
                        return arrayItem(curOffset);
                    case elementTypeObject:
                        if ('}'!=nextChar)
                        {
                            checkDelimiter(", or }");
                            namedItem();
                        }
                        else
                        {
                            if (stack.ordinality()>1)
                                readNext();
                            return endNode(curOffset);
                        }
                        break;
                }
                break;
            }
            case itemEnd:
            {
                if (!stack.length())
                {
                    if (!noRoot || !rootNext())
                        return false;
                    readNext();
                    skipWS();
                    rootItem();
                }
                else
                {
                    readNext();
                    state = itemContent;
                    return endNode(curOffset);
                }
                break;
            }
        }
        return true;
    }
};

IPTreeReader *createJSONStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions, size32_t bufSize)
{
    class CJSONStreamReader : public CJSONReader<CInstStreamReader>
    {
    public:
        CJSONStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions, size32_t bufSize=0) : CJSONReader<CInstStreamReader>(stream, iEvent, readerOptions, bufSize) { }
    };
    return new CJSONStreamReader(stream, iEvent, readerOptions, bufSize);
}

IPTreeReader *createJSONStringReader(const char *json, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
{
    class CJSONStringReader : public CJSONReader<CInstStringReader>
    {
    public:
        CJSONStringReader(const void *json, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions) : CJSONReader<CInstStringReader>(json, iEvent, readerOptions) { }
    };
    if (NULL == json)
        throw createPTreeReadException(PTreeRead_syntax, "Null string passed to createJSONStringReader", NULL, 0, 0);
    return new CJSONStringReader(json, iEvent, readerOptions);
}

IPTreeReader *createJSONBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
{
    class CJSONBufferReader : public CJSONReader<CInstBufferReader>
    {
    public:
        CJSONBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions) : CJSONReader<CInstBufferReader>(buf, bufLength, iEvent, readerOptions) { }
    };
    return new CJSONBufferReader(buf, bufLength, iEvent, readerOptions);
}


IPullPTreeReader *createPullJSONStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions, size32_t bufSize)
{
    class CJSONStreamReader : public CPullJSONReader<CInstStreamReader>
    {
    public:
        CJSONStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions, size32_t bufSize=0) : CPullJSONReader<CInstStreamReader>(stream, iEvent, readerOptions, bufSize) { }
    };
    return new CJSONStreamReader(stream, iEvent, readerOptions, bufSize);
}

IPullPTreeReader *createPullJSONStringReader(const char *json, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
{
    class CJSONStringReader : public CPullJSONReader<CInstStringReader>
    {
    public:
        CJSONStringReader(const void *json, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions) : CPullJSONReader<CInstStringReader>(json, iEvent, readerOptions) { }
    };
    return new CJSONStringReader(json, iEvent, readerOptions);
}

IPullPTreeReader *createPullJSONBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
{
    class CJSONBufferReader : public CPullJSONReader<CInstBufferReader>
    {
    public:
        CJSONBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions) : CPullJSONReader<CInstBufferReader>(buf, bufLength, iEvent, readerOptions) { }
    };
    return new CJSONBufferReader(buf, bufLength, iEvent, readerOptions);
}

IPropertyTree *createPTreeFromJSONString(const char *json, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IPTreeMaker> _iMaker;
    if (!iMaker)
    {
        iMaker = new CPTreeMaker(flags);
        _iMaker.setown(iMaker);
    }
    Owned<IPTreeReader> reader = createJSONStringReader(json, *iMaker, readFlags);
    reader->load();
    return LINK(iMaker->queryRoot());
}

IPropertyTree *createPTreeFromJSONString(unsigned len, const char *json, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IPTreeMaker> _iMaker;
    if (!iMaker)
    {
        iMaker = new CPTreeMaker(flags);
        _iMaker.setown(iMaker);
    }
    Owned<IPTreeReader> reader = createJSONBufferReader(json, len, *iMaker, readFlags);
    reader->load();
    return LINK(iMaker->queryRoot());
}
