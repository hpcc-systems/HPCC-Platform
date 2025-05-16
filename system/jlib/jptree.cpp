
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

#include <unordered_map>
#include <unordered_set>
#include <string>
#include <tuple>
#include <algorithm>

#include "platform.h"
#include "jarray.hpp"
#include "jdebug.hpp"
#include "jhash.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "jlzw.hpp"
#include "jregexp.hpp"
#include "jstring.hpp"
#include "jutil.hpp"
#include "jmisc.hpp"
#include "yaml.h"

#include <initializer_list>

#define MAKE_LSTRING(name,src,length) \
    const char *name = (const char *) alloca((length)+1); \
    memcpy((char *) name, (src), (length)); \
    *(char *) (name+(length)) = '\0';

#include "jfile.hpp"
#include "jlog.hpp"
#include "jptree.ipp"

#define WARNLEGACYCOMPARE
#define XMLTAG_CONTENT "<>"

#define UNIMPLEMENTED_IPT throw MakeIPTException(-1, "UNIMPLEMENTED feature in function %s() at %s(%d)", __func__, sanitizeSourceFile(__FILE__), __LINE__)


#define CHECK_ATTRIBUTE(X) if (X && isAttribute(X)) throw MakeIPTException(PTreeExcpt_XPath_Unsupported, "Attribute usage invalid here");
#define AMBIGUOUS_PATH(X,P) { StringBuffer buf; buf.append(X": ambiguous xpath \"").append(P).append("\"");  throw MakeIPTException(PTreeExcpt_XPath_Ambiguity,"%s",buf.str()); }

#define PTREE_COMPRESS_THRESHOLD (4*1024)    // i.e. only use compress if > threshold
#define PTREE_COMPRESS_BOTHER_PECENTAGE (80) // i.e. if it doesn't compress to <80 % of original size don't bother

constexpr CompressionMethod defaultBinaryCompressionMethod = COMPRESS_METHOD_LZW_LITTLE_ENDIAN;

class NullPTreeIterator final : implements IPropertyTreeIterator
{
public:
    virtual ~NullPTreeIterator() {}
    virtual void Link() const override {}
    virtual bool Release() const override { return true; }
// IPropertyTreeIterator
    virtual bool first() override { return false; }
    virtual bool next() override { return false; }
    virtual bool isValid() override { return false; }
    virtual IPropertyTree & query() override { throwUnexpected(); }
} *nullPTreeIterator;

IPropertyTreeIterator *createNullPTreeIterator() { return LINK(nullPTreeIterator); } // initialized in init mod below.

//===================================================================

#ifdef USE_READONLY_ATOMTABLE
RONameTable *AttrStrUnionWithTable::roNameTable = nullptr;
RONameTable *AttrStrUnionWithValueTable::roValueTable = nullptr;
#endif
static AtomRefTable *keyTable = nullptr;
static AtomRefTable *keyTableNC = nullptr;

static CriticalSection hashcrit;
static CAttrValHashTable *attrHT = nullptr;
static AttrValue **freelist = nullptr;
static unsigned freelistmax = 0;
static CLargeMemoryAllocator freeallocator((memsize_t)-1, 0x1000*sizeof(AttrValue), true);

#ifdef USE_READONLY_ATOMTABLE
static const char * roAttributes[] =
{
#include "jptree-attrs.hpp"    // potentially auto-generated
    nullptr
};
static const char * roAttributeValues[] =
{
#include "jptree-attrvalues.hpp"    // potentially auto-generated
    nullptr
};

void initializeRoTable()
{
    for (const char **attr = roAttributes; *attr; attr++)
    {
        AttrStrUnionWithTable::roNameTable->find(*attr, true);
    }
    for (const char **value = roAttributeValues; *value; value++)
    {
        AttrStrUnionWithValueTable::roValueTable->find(*value, true);
    }
    // also populate read-only value table by generating some common constants
    StringBuffer constStr;
    for (unsigned c=0; c<1000; c++) // common unsigned values in attributes
    {
        constStr.clear().append(c);
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
    }
    for (unsigned c=1; c<=400; c++) // outer graphs
    {
        constStr.clear().append("graph").append(c);
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
        constStr.clear().append("Graph graph ").append(c);
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
    }
    for (unsigned c=1; c<=200; c++) // subgraphs
    {
        constStr.clear().append("sg").append(c);
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
    }
    for (unsigned c=1; c<=200; c++) // Edge 0
    {
        constStr.clear().append(c).append("_0");
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
    }
    for (unsigned c=0; c<35; c++)
    {
        char ch = c<9 ? ('1' + c) : ('A' + (c-9));
        constStr.clear().append("~spill::").append(ch); // spills
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
        constStr.clear().append("gl").append(ch); // graph results
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
        constStr.clear().append("mf").append(ch); // meta factories
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
    }
    for (unsigned c=1; c<=10; c++) // global auto attributes
    {
        constStr.clear().append("auto").append(c);
        AttrStrUnionWithValueTable::roValueTable->find(constStr.str(), true);
    }
#ifdef TRACE_ATOM_SIZE
    // If you are wanting an idea of the savings from use of the RO hash table, it may be useful to reset
    // the counts here. But it's more correct to actually leave them in place.
    //AttrStrAtom::totsize = 0;
    //AttrStrAtom::maxsize = 0;
#endif
#ifdef _DEBUG
    for (const char **a = roAttributes; *a; a++)
    {
        // sanity check
        unsigned idx = AttrStrUnionWithTable::roNameTable->findIndex(*a, AttrStrC::getHash(*a));
        AttrStrC *val = AttrStrUnionWithTable::roNameTable->getIndex(idx);
        assert(val && val->eq(*a));
    }
    for (const char **v = roAttributeValues; *v; v++)
    {
        // sanity check
        unsigned idx = AttrStrUnionWithValueTable::roValueTable->findIndex(*v, AttrStrC::getHash(*v));
        AttrStrC *val = AttrStrUnionWithValueTable::roValueTable->getIndex(idx);
        assert(val && val->eq(*v));
    }
#endif
}
#endif

MODULE_INIT(INIT_PRIORITY_JPTREE)
{
    nullPTreeIterator = new NullPTreeIterator;
#ifdef USE_READONLY_ATOMTABLE
    AttrStrUnionWithTable::roNameTable = new RONameTable(255);
    AttrStrUnionWithValueTable::roValueTable = new RONameTable(4095);
    initializeRoTable();
#endif
    keyTable = new AtomRefTable;
    keyTableNC = new AtomRefTable(true);
    attrHT = new CAttrValHashTable;
    return true;
}

MODULE_EXIT()
{
    delete nullPTreeIterator;
    delete attrHT;
    keyTable->Release();
    keyTableNC->Release();
#ifdef USE_READONLY_ATOMTABLE
    delete AttrStrUnionWithTable::roNameTable;
    delete AttrStrUnionWithValueTable::roValueTable;
#endif
    free(freelist);
    freelist = NULL;
}

static int comparePropTrees(IInterface * const *ll, IInterface * const *rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    return stricmp(l->queryName(), r->queryName());
};

class CPTArrayIterator : public ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>
{
    IArrayOf<IPropertyTree> elems;
public:
    CPTArrayIterator(IPropertyTreeIterator &iter, TreeCompareFunc compare) : ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>(elems)
    {
        ForEach(iter)
            elems.append(iter.get());
        elems.sort(compare);
    }
    CPTArrayIterator(IArrayOf<IPropertyTree> & ownedElems, TreeCompareFunc compare) : ArrayIIteratorOf<IArrayOf<IPropertyTree>, IPropertyTree, IPropertyTreeIterator>(elems)
    {
        elems.swapWith(ownedElems);
        elems.sort(compare);
    }
};
IPropertyTreeIterator * createSortedIterator(IPropertyTreeIterator & iter)
{
    return new CPTArrayIterator(iter, comparePropTrees);
}
IPropertyTreeIterator * createSortedIterator(IPropertyTreeIterator & iter, TreeCompareFunc compare)
{
    return new CPTArrayIterator(iter, compare);
}
IPropertyTreeIterator * createSortedIterator(IArrayOf<IPropertyTree> & ownedElems, TreeCompareFunc compare)
{
    return new CPTArrayIterator(ownedElems, compare);
}
//////////////////

unsigned ChildMap::getHashFromElement(const void *e) const
{
    PTree &elem = (PTree &) (*(IPropertyTree *)e);
    return elem.queryHash();
}

unsigned ChildMap::numChildren() const
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
    class CPTHashIterator : implements IPropertyTreeIterator, public CInterface
    {
        SuperHashIteratorOf<IPropertyTree> *hiter;
    public:
        IMPLEMENT_IINTERFACE;

        CPTHashIterator(SuperHashTable &table) { hiter = new SuperHashIteratorOf<IPropertyTree>(table); }
        ~CPTHashIterator() { hiter->Release(); }
    // IPropertyTreeIterator
        virtual bool first() override { return hiter->first(); }
        virtual bool next() override { return hiter->next(); }
        virtual bool isValid() override { return hiter->isValid(); }
        virtual IPropertyTree & query() override { return hiter->query(); }
    };
    Owned<IPropertyTreeIterator> baseIter = new CPTHashIterator(*this);
    if (!sort)
        return baseIter.getClear();
    return createSortedIterator(*baseIter);
}

void ChildMap::onRemove(void *e)
{
    PTree &elem = static_cast<PTree &>(*(IPropertyTree *)e);
    IPTArrayValue *value = elem.queryValue();
    if (value && value->isArray())
    {
        IPropertyTree **elems = value->getRawArray();
        IPropertyTree **elemsEnd = elems+value->elements();
        while (elems != elemsEnd)
        {
            PTree *pElem = static_cast<PTree *>(*elems++);
            pElem->setOwner(nullptr);
        }
    }
    elem.Release();
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

class jlib_thrown_decl CPTreeException : implements IPTreeException, public CInterface
{
    int errCode;
    StringBuffer errMsg;
public:
    IMPLEMENT_IINTERFACE;

    CPTreeException(int _errCode, const char *_errMsg, va_list &args) __attribute__((format(printf,3,0))) : errCode(_errCode)
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

static IPTreeException *MakeIPTException(int code, const char *format, ...) __attribute__((format(printf,2,3)));
static IPTreeException *MakeXPathException(const char *xpath, int code, size_t pos, const char *format, ...) __attribute__((format(printf,4,5)));

IPTreeException *MakeIPTException(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IPTreeException *e = new CPTreeException(code, format, args);
    va_end(args);
    return e;
}

IPTreeException *MakeXPathException(const char *xpath, int code, size_t pos, const char *format, ...)
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
    s.append("\n").appendN((size32_t)(strlen(msg)+pos), ' ').append("^");
    return MakeIPTException(code, "%s", s.str());
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
    wild = false;
    for (;;)
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



inline static void readWildIdIndex(const char *&xpath, bool &wild, bool &numeric)
{
    const char *_xpath = xpath;
    readWildId(xpath, wild);
    if ('[' == *xpath) // check for local index not iterative qualifier.
    {
        const char *end = xpath+1;
        if (isdigit(*end))
        {
            StringAttr index;
            end = readIndex(end, index);
            if (']' != *end)
                throw MakeXPathException(_xpath, PTreeExcpt_XPath_ParseError, xpath-_xpath, "Qualifier brace unclosed");
            xpath = end+1;
            numeric = true;
        }
        else
            numeric = false;
    }
    else
        numeric = false;
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
    bool quote = false;
    for (;;)
    {
        char next = *str;
        if (next == '\0')
            return NULL;
        if ('"' == next)
            quote = !quote;
        else if (c == next && !quote)
            return str;
        ++str;
    }
}

const char *queryHead(const char *xpath, StringBuffer &head)
{
    if (!xpath) return NULL;
    const char *start = xpath;
    bool quote = false;
    bool braced = false;
    for (;;)
    {
        if (*xpath == '\0')
            return NULL;
        ++xpath;
        char next = *xpath;
        if ('"' == next)
            quote = !quote;
        else if (next == ']' && !quote)
        {
            assertex(braced);
            braced = false;
        }
        else if (next == '[' && !quote)
        {
            assertex(!braced);
            braced = true;
        }
        else if (next == '/' && !quote && !braced)
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
    head.append(xpath-start, start);
    return xpath+1;
}

///////////////////

static constexpr unsigned defaultSiblingMapThreshold = 100;
static unsigned siblingMapThreshold = (unsigned)-1; // off until configuration default it on.

void setPTreeMappingThreshold(unsigned threshold)
{
    /*
    * NB: setPTreeMappingThreshold() will automatically be called via loadConfiguration
    * Redefining this limit, will not effect existing maps, and should generally only be called once during startup.
    */
    if (0 == threshold)
        threshold = (unsigned)-1;
    siblingMapThreshold = threshold;
}

class CValueMap : public std::unordered_multimap<std::string, const IPropertyTree *>
{
public:
    CValueMap(const char *_lhs, IPTArrayValue &array)
    {
        IPropertyTree **elements = array.getRawArray();
        IPropertyTree **last = elements+array.elements();
        dbgassertex(elements != last);
        while (true)
        {
            const char *v = (*elements)->queryProp(_lhs);
            if (v)
                emplace(std::make_pair(std::string(v), *elements));
            elements++;
            if (last == elements)
                break;
        }
    }
    std::pair<CValueMap::iterator, CValueMap::iterator> find(const char *rhs)
    {
        return equal_range(std::string(rhs));
    }
    void insertEntry(const char *v, const IPropertyTree *tree)
    {
        emplace(std::make_pair(std::string(v), tree));
    }
    bool removeEntry(const char *v, const IPropertyTree *tree)
    {
        auto range = equal_range(std::string(v));
        if (range.first == range.second)
            return false;

        auto it = range.first;
        while (true)
        {
            if (it->second == tree)
            {
                it = erase(it);
                return true;
            }
            ++it;
            if (it == range.second)
                break;
        }
        throwUnexpected();
    }
    void replaceEntry(const char *oldV, const char *newV, const IPropertyTree *tree)
    {
        verifyex(removeEntry(oldV, tree));
        if (newV)
            insertEntry(newV, tree);
    }
};

class CQualifierMap
{
    std::unordered_map<std::string, CValueMap *> attrValueMaps;
    CriticalSection crit;

public:
    CQualifierMap()
    {
    }
    ~CQualifierMap()
    {
        for (auto &e: attrValueMaps)
            delete e.second;
    }
    CValueMap *addMapping(const char *lhs, IPTArrayValue &array)
    {
        CValueMap *valueMap = new CValueMap(lhs, array);
        attrValueMaps.emplace(std::make_pair(std::string(lhs), valueMap));
        return valueMap;
    }
    CValueMap *addMappingIfNew(const char *lhs, IPTArrayValue &array)
    {
        CriticalBlock b(crit);
        auto it = attrValueMaps.find(lhs);
        if (it == attrValueMaps.end())
            return addMapping(lhs, array);
        else
            return it->second;
    }
    void addMatchingValues(const IPropertyTree *tree)
    {
        for (auto &e: attrValueMaps)
        {
            const char *v = tree->queryProp(e.first.c_str());
            if (v)
                e.second->insertEntry(v, tree);
        }
    }
    void removeMatchingValues(const IPropertyTree *tree)
    {
        for (auto &e: attrValueMaps)
        {
            const char *lhsp = e.first.c_str();
            const char *oldV = tree->queryProp(lhsp);
            if (oldV)
                verifyex(e.second->removeEntry(oldV, tree));
        }
    }
    void replaceMatchingValues(const IPropertyTree *oldTree, const IPropertyTree *newTree)
    {
        for (auto &e: attrValueMaps)
        {
            const char *lhsp = e.first.c_str();
            const char *oldV = oldTree->queryProp(lhsp);
            if (oldV)
            {
                verifyex(e.second->removeEntry(oldV, oldTree));
                const char *newV = newTree->queryProp(lhsp);
                if (newV)
                    e.second->insertEntry(newV, newTree);
            }
        }
    }
    CValueMap *find(const char *lhs)
    {
        auto it = attrValueMaps.find(lhs);
        if (it == attrValueMaps.end())
            return nullptr;
        return it->second;
    }
    void removeEntryIfMapped(const char *lhs, const char *v, const IPropertyTree *tree)
    {
        auto it = attrValueMaps.find(lhs);
        if (it != attrValueMaps.end())
            it->second->removeEntry(v, tree);
    }
    void insertEntryIfMapped(const char *lhs, const char *v, const IPropertyTree *tree)
    {
        auto it = attrValueMaps.find(lhs);
        if (it != attrValueMaps.end())
            it->second->insertEntry(v, tree);
    }
    void replaceEntryIfMapped(const char *lhs, const char *oldv, const char *newv, const IPropertyTree *tree)
    {
        auto it = attrValueMaps.find(lhs);
        if (it != attrValueMaps.end())
            it->second->replaceEntry(oldv, newv, tree);
    }
};

// parse qualifier, returns true if simple equality expression found
static bool parseEqualityQualifier(const char *&xxpath, unsigned &lhsLen, const char *&rhsBegin, unsigned &rhsLen)
{
    const char *xpath = xxpath;
    while (*xpath == ' ' || *xpath == '\t') xpath++;
    if ('@' != *xpath) // only attributes supported
        return false;
    const char *start = xpath;
    char quote = 0;
    const char *lhsEnd, *quoteBegin, *quoteEnd, *rhsEnd;
    lhsEnd = quoteBegin = quoteEnd = rhsBegin = rhsEnd = NULL;
    bool equalSignFound = false;
    for (;;)
    {
        switch (*xpath)
        {
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
        case '>':
        case '<':
        case '~':
        case '/':
            if (!quote)
                return false;
            break;
        case '=':
            if (!quote)
            {
                if (equalSignFound)
                    throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Unexpected expression operator xpath");
                equalSignFound = true;
                if (!lhsEnd)
                    lhsEnd = xpath;
            }
            break;
        case '?':
        case '*':
            return false;
        case '\0':
            rhsEnd = xpath;
            break;
        }
        if (rhsEnd)
            break;
        xpath++;
        if (!rhsBegin && equalSignFound && !isspace(*xpath))
            rhsBegin = xpath;
    }
    if (quote)
        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Parse error, unclosed quoted content");
    if (!equalSignFound)
        return false;

    lhsLen = lhsEnd-start;
    if (quoteBegin && !quoteEnd)
        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Parse error, RHS missing closing quote");
    if (rhsBegin && !rhsEnd)
        throw MakeXPathException(start, PTreeExcpt_XPath_ParseError, xpath-start, "Parse error, RHS missing closing quote");
    if (!quoteBegin && rhsEnd) // only if numeric
        return false;
    else // quoted
    {
        rhsBegin = quoteBegin;
        rhsLen = quoteEnd - rhsBegin;
    }
    if (rhsEnd && *xpath == ']')
        xpath++;
    xxpath = xpath;
    return true;
}

class CMapQualifierIterator : public CInterfaceOf<IPropertyTreeIterator>
{
    CValueMap::iterator startRange, endRange;
    CValueMap::iterator currentIter;
public:
    CMapQualifierIterator(CQualifierMap &_map, CValueMap::iterator _startRange, CValueMap::iterator _endRange)
        : startRange(_startRange), endRange(_endRange)
    {
    }

// IPropertyTreeIterator
    virtual bool first() override
    {
        currentIter = startRange;
        return currentIter != endRange;
    }
    virtual bool next() override
    {
        currentIter++;
        return currentIter != endRange;
    }
    virtual bool isValid() override
    {
        return currentIter != endRange;
    }
    virtual IPropertyTree & query() override { return const_cast<IPropertyTree &>(*currentIter->second); }
};

IPropertyTreeIterator *checkMapIterator(const char *&xxpath, IPropertyTree &child)
{
    /*
    * NB: IPT's are not thread safe. It is up to the caller to ensure multiple writers do not contend.
    * ( Dali for example ensures writer threads are exclusive )
    *
    * That means multiple reader threads could be here concurrently.
    * >1 could be constructing the qualifier map for the 1st time.
    * For new attr updates (where map already exists), it will block on map::crit,
    * so that there is at most 1 thread updating the map. The underlying unordered_multiset
    * is thread safe if 1 writer, and multiple readers.
    *
    * On initial map creation, allow concurrency, but only 1 will succeed to swap in the new active map.
    * That could mean a new attr/prop. mapping is lost, until next used.
    * NB: once the map is live, updates are write ops. and so, just as with the IPT
    * itself, it is expected that something will keep it thread safe (as Dali does)
    *
    */

    // NB: only support simple @<attrname>=<value> qualifiers

    if (((unsigned)-1) == siblingMapThreshold) // disabled
        return nullptr;

    PTree &_child = (PTree &)child;
    if (child.isCaseInsensitive()) // NB: could support but not worth it.
        return nullptr;

    IPTArrayValue *value = _child.queryValue();
    if (!value)
        return nullptr;
    CQualifierMap *map = value->queryMap();
    if (!map)
    {
        if (!value->isArray() || (value->elements() < siblingMapThreshold))
            return nullptr;
    }

    unsigned lhsLen, rhsLen;
    const char *rhsStart;
    const char *xpath = xxpath;
    if (!parseEqualityQualifier(xpath, lhsLen, rhsStart, rhsLen))
        return nullptr;
    MAKE_LSTRING(lhs, xxpath, lhsLen);
    MAKE_LSTRING(rhs, rhsStart, rhsLen);

    // NB: there can be a race here where >1 reader is constructing new map
    CValueMap *valueMap = nullptr;
    if (map)
        valueMap = map->addMappingIfNew(lhs, *value);
    else
    {
        OwnedPtr<CQualifierMap> newMap = new CQualifierMap();
        valueMap = newMap->addMapping(lhs, *value);

        /*
        * NB: it's possible another read thread got here 1st, and swapped in a map.
        * setMap returns the existing map, and the code below checks to see if it already
        * handles the 'lhs' we're adding, if it doesn't it re-adds the qualifier mappings.
        */
        map = value->setMap(newMap);
        if (!map) // successfully swapped newMap in.
            map = newMap.getClear(); // NB: setMap owns
        else // another thread has swapped in a map whilst I was creating new one
            valueMap = map->addMappingIfNew(lhs, *value);
    }

    xxpath = xpath; // update parsed position

    auto range = valueMap->find(rhs);
    if (range.first != range.second)
        return new CMapQualifierIterator(*map, range.first, range.second);
    else
        return LINK(nullPTreeIterator);
}

///////////////////

class SeriesPTIterator : implements IPropertyTreeIterator, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    SeriesPTIterator() : current(NULL), cp(0)
    {
    }

    void addIterator(IPropertyTreeIterator *iter) { iters.append(*iter); }

// IPropertyTreeIterator impl.
    virtual bool first() override
    {
        cp = 0;
        iterCount = iters.ordinality();
        if (nextIterator())
            return true;
        else
            return false;
    }

    virtual bool next() override
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

    virtual bool isValid() override { return (NULL != current); }

    virtual IPropertyTree & query() override { assertex(current); return *current; }

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

    IArray iters;
    IPropertyTreeIterator *currentIter;
    IPropertyTree *current;
    unsigned cp, iterCount;
};

///////////////////

CPTValue::CPTValue(size32_t size, const void *data, bool binary, CompressionMethod currentCompressType, CompressionMethod preferredCompressType)
{
    compressType = currentCompressType;
    if (binary && (currentCompressType == COMPRESS_METHOD_NONE) && (preferredCompressType != COMPRESS_METHOD_NONE) && (size > PTREE_COMPRESS_THRESHOLD))
    {
        unsigned newSize = size * PTREE_COMPRESS_BOTHER_PECENTAGE / 100;
        void *newData = NULL;
        ICompressor *compressor = NULL;
        try
        {
            newData = malloc(sizeof(size32_t) + newSize);
            CompressionMethod compressMethod = (preferredCompressType != COMPRESS_METHOD_DEFAULT) ? preferredCompressType : defaultBinaryCompressionMethod;
            ICompressHandler * handler = queryCompressHandler(compressMethod);
            compressor = handler->getCompressor();
            compressor->open(((char *)newData) + sizeof(size32_t), newSize);
            if (compressor->write(data, size)==size)
            {
                compressor->close();
                memcpy(newData, &size, sizeof(size32_t));
                newSize = sizeof(size32_t) + compressor->buflen();
                compressType = compressMethod;
                set(newSize, newData);
            }
            free(newData);
            newData = NULL;
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
    if (size && !get())
        set(size, data);
}

static void *uncompress(const void *src, size32_t &sz, byte compressType)
{
    dbgassertex(compressType != COMPRESS_METHOD_LZWLEGACY);
    if (compressType == COMPRESS_METHOD_LZWLEGACY)
        compressType = COMPRESS_METHOD_LZW_LITTLE_ENDIAN;
    IExpander *expander = NULL;
    void *uncompressedValue = NULL;
    try
    {
        memcpy(&sz, src, sizeof(size32_t));
        assertex(sz);

        ICompressHandler * handler = queryCompressHandler((CompressionMethod)compressType);
        expander = handler->getExpander();
        src = ((const char *)src) + sizeof(size32_t);
        uncompressedValue = malloc(sz);
        assertex(uncompressedValue);
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
    if (compressType)
    {
        size32_t sz;
        void *uncompressedValue = uncompress(get(), sz, compressType);
        ((MemoryAttr *)this)->setOwn(sz, uncompressedValue);
        compressType = COMPRESS_METHOD_NONE;
    }
    return get();
}

void CPTValue::serialize(MemoryBuffer &tgt)
{
    //Retain backward compatibility for the serialization format.
    size32_t serialLen = (size32_t)length();
    tgt.append(serialLen);
    if (serialLen)
    {
        tgt.append(compressType);
        tgt.append(serialLen, get());
    }
}

void CPTValue::deserialize(MemoryBuffer &src)
{
    size32_t sz;
    src.read(sz);
    if (sz)
    {
        src.read(compressType);
        if (compressType == COMPRESS_METHOD_LZWLEGACY)
            compressType = COMPRESS_METHOD_LZW_LITTLE_ENDIAN;
        set(sz, src.readDirect(sz));
    }
    else
    {
        compressType = COMPRESS_METHOD_NONE;
        clear();
    }
}

MemoryBuffer &CPTValue::getValue(MemoryBuffer &tgt, bool binary) const
{
    if (compressType)
    {
        size32_t sz;
        void *uncompressedValue = uncompress(get(), sz, compressType);
        if (!binary) sz -= 1;
        tgt.append(sz, uncompressedValue);
        if (uncompressedValue)
            free(uncompressedValue);
    }
    else
    {
        if (binary)
            tgt.append((size32_t)length(), get());
        else
            tgt.append((size32_t)length()-1, get());
    }

    return tgt;
}

StringBuffer &CPTValue::getValue(StringBuffer &tgt, bool binary) const
{
    if (compressType)
    {
        size32_t sz;
        void *uncompressedValue = NULL;
        try
        {
            uncompressedValue = uncompress(get(), sz, compressType);
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
            tgt.append((size32_t)length(), (const char *)get());
        else if (length())
            tgt.append((size32_t)length()-1, (const char *)get());
    }

    return tgt;
}

size32_t CPTValue::queryValueSize() const
{
    if (compressType)
    {
        size32_t sz;
        memcpy(&sz, get(), sizeof(size32_t));
        return sz;
    }
    else
        return (size32_t)length();
}

///////////////////


CPTArray::~CPTArray()
{
    if (map.load())
        delete map.load();
}

CQualifierMap *CPTArray::setMap(CQualifierMap *_map)
{
    CQualifierMap *expected = nullptr;
    if (map.compare_exchange_strong(expected, _map))
        return nullptr;
    else
        return expected;
}

void CPTArray::addElement(IPropertyTree *tree)
{
    append(*tree);
    CQualifierMap *map = queryMap();
    if (map)
    {
        if (tree->getAttributeCount())
            map->addMatchingValues(tree);
    }
}

void CPTArray::setElement(unsigned idx, IPropertyTree *tree)
{
    CQualifierMap *map = queryMap();
    if (map)
    {
        // remove any mappings for existing element.
        if (isItem(idx))
        {
            IPropertyTree *existing = &((IPropertyTree &)item(idx));
            map->replaceMatchingValues(existing, tree);
        }
        else
            map->addMatchingValues(tree);
    }
    add(*tree, idx);
}

void CPTArray::removeElement(unsigned idx)
{
    CQualifierMap *map = queryMap();
    IPropertyTree *existing = &((IPropertyTree &)item(idx));
    if (map)
        map->removeMatchingValues(existing);
    ((PTree *)existing)->setOwner(nullptr);
    remove(idx);
}

unsigned CPTArray::find(const IPropertyTree *search) const
{
    IInterface **start = getArray();
    IInterface **last = start + ordinality();
    IInterface **members = start;
    while (true)
    {
        if (*members == search)
            return members-start;
        members++;
        if (members == last)
            break;
    }
    return NotFound;
}

//////////////////


PTree::PTree(byte _flags, IPTArrayValue *_value, ChildMap *_children)
{
    flags = _flags;
    children = LINK(_children);
    value = _value;
}

PTree::~PTree()
{
    if (value) delete value;
    ::Release(children);
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
        unsigned pos = value->find(child);
        if (remove && NotFound != pos)
            value->removeElement(pos);
        return pos;
    }
    else if (checkChildren())
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

void PTree::setLocal(size32_t l, const void *data, bool _binary, CompressionMethod compressType)
{
    if (value) delete value;
    if (l)
        value = new CPTValue(l, data, _binary, COMPRESS_METHOD_NONE, compressType);
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
        value = new CPTValue(l, data, binary, COMPRESS_METHOD_NONE, COMPRESS_METHOD_DEFAULT);
    else
        value = NULL;
    if (binary)
        IptFlagSet(flags, ipt_binary);
    else
        IptFlagClr(flags, ipt_binary);
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
            return nullptr != findAttribute(xpath);
    }
    else
    {
        Owned<IPropertyTreeIterator> iter = getElements(xpath);
        bool res = iter->first();
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
        return getAttributeValue(xpath);
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
        const char *value = getAttributeValue(xpath);
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
                setLocal(l+1, val, false, COMPRESS_METHOD_NONE);
        }
    }
    else if (isAttribute(xpath))
    {
        if (!val)
            removeAttribute(xpath);
        else
            setAttribute(xpath, val, false);
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
#ifdef _DEBUG
    if (childIter->next())
        AMBIGUOUS_PATH("addPropX", xpath);
#endif

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

        /* If 'path' resolves to iterator of this, then treat as if no leading path
         * i.e. "./x", or "././.x" is equivalent to "x"
         */
        if (this != &pathIter->query())
        {
            IPropertyTree *currentPath = NULL;
#ifdef _DEBUG
            bool multiplePaths = false;
#endif
            bool multipleChildMatches = false;
            for (;;)
            {
                // JCSMORE - a bit annoying has to be done again once path has been established
                currentPath = &pathIter->query();
                Owned<IPropertyTreeIterator> childIter = currentPath->getElements(prop);
                if (childIter->first())
                {
                    child = &childIter->query();
#ifdef _DEBUG
                    if (parent)
                        AMBIGUOUS_PATH("resolveParentChild", xpath);
#endif
                    if (!multipleChildMatches && childIter->next())
                        multipleChildMatches = true;

                    parent = currentPath;
                }
                if (pathIter->next())
                {
#ifdef _DEBUG
                    multiplePaths = true;
#endif
                }
                else
                    break;
            }
            if (!parent)
            {
#ifdef _DEBUG
                if (multiplePaths) // i.e. no unique path to child found and multiple parent paths
                    AMBIGUOUS_PATH("resolveParentChild", xpath);
#endif
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
            return;
        }
    }
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

void PTree::addProp(const char *xpath, const char *val)
{
    if (!xpath || '\0' == *xpath)
        addLocal((size32_t)strlen(val)+1, val);
    else if (isAttribute(xpath))
        setAttribute(xpath, val, false);
    else if ('[' == *xpath)
    {
        aindex_t pos = getChildMatchPos(xpath);
        if ((aindex_t) -1 == pos)
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
        setAttribute(xpath, newVal.str(), false);
    }
    else if ('[' == *xpath)
    {
        IPropertyTree *qualified = queryPropTree(xpath);
        if (!qualified)
            throw MakeIPTException(-1, "appendProp: qualifier unmatched %s", xpath);
        qualified->appendProp(nullptr, val);
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
        const char *v = getAttributeValue(xpath);
        if (!v || !*v) // intentional return dft if attribute equals ""
            return dft;
        return _atoi64(v);
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
        setLocal((size32_t)strlen(buf)+1, buf, false, COMPRESS_METHOD_NONE);
    }
    else if (isAttribute(xpath))
    {
        char buf[23];
        numtostr(buf, val);
        setAttribute(xpath, buf, false);
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
        setAttribute(xpath, buf, false);
    }
    else if ('[' == *xpath)
    {
        char buf[23];
        numtostr(buf, val);
        aindex_t pos = getChildMatchPos(xpath);
        if ((aindex_t) -1 == pos)
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

void PTree::setPropReal(const char * xpath, double val)
{
    if (!xpath || '\0' == *xpath)
    {
        std::string s = std::to_string(val);
        setLocal((size32_t)s.length()+1, s.c_str(), false, COMPRESS_METHOD_NONE);
    }
    else if (isAttribute(xpath))
    {
        std::string s = std::to_string(val);
        setAttribute(xpath, s.c_str(), false);
    }
    else
    {
        const char *prop;
        IPropertyTree *branch = splitBranchProp(xpath, prop, true);

        if (isAttribute(prop))
            branch->setPropReal(prop, val);
        else
        {
            IPropertyTree *propBranch = queryCreateBranch(branch, prop);
            propBranch->setPropReal(NULL, val);
        }
    }
}

void PTree::addPropReal(const char *xpath, double val)
{
    if (!xpath || '\0' == *xpath)
    {
        std::string s = std::to_string(val);
        addLocal((size32_t)s.length()+1, s.c_str());
    }
    else if (isAttribute(xpath))
    {
        std::string s = std::to_string(val);
        setAttribute(xpath, s.c_str(), false);
    }
    else if ('[' == *xpath)
    {
        std::string s = std::to_string(val);
        aindex_t pos = getChildMatchPos(xpath);
        if ((aindex_t) -1 == pos)
            throw MakeIPTException(-1, "addPropInt64: qualifier unmatched %s", xpath);
        addLocal((size32_t)s.length()+1, s.c_str(), false, pos);
    }
    else
    {
        IPropertyTree *parent, *child;
        StringAttr path, qualifier;
        resolveParentChild(xpath, parent, child, path, qualifier);
        if (parent != this)
            parent->addPropReal(path, val);
        else if (child)
            child->addPropReal(qualifier, val);
        else
            setPropReal(path, val);
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

double PTree::getPropReal(const char *xpath, double dft) const
{
    const char *val = queryProp(xpath);
    return val?atof(val):dft;
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
        if (!isAttribute(prop))
        {
            IPropertyTree *branch = queryPropTree(xpath);
            if (branch)
                return branch->isCompressed(nullptr);
        }
    }
    return false;
}

CompressionMethod PTree::getCompressionType() const
{
    return value ? value->getCompressionType() : COMPRESS_METHOD_NONE;
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
        if (!isAttribute(prop))
        {
            IPropertyTree *branch = queryPropTree(xpath);
            if (branch)
                return branch->isBinary(nullptr);
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
        UNIMPLEMENTED_IPT;
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
#ifdef _DEBUG
            if (iter->next())
                AMBIGUOUS_PATH("renameProp", xpath);
#endif
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

void PTree::setPropBin(const char * xpath, size32_t size, const void *data, CompressionMethod preferredCompression)
{
    CHECK_ATTRIBUTE(xpath);
    if (!xpath || '\0' == *xpath)
        setLocal(size, data, true, preferredCompression);
    else
    {
        const char *prop;
        IPropertyTree *branch = splitBranchProp(xpath, prop, true);
        if (isAttribute(prop))
            branch->setPropBin(prop, size, data, preferredCompression);
        else
        {
            IPropertyTree *propBranch = queryCreateBranch(branch, prop);
            propBranch->setPropBin(NULL, size, data, preferredCompression);
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
        if ((aindex_t) -1 == pos)
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
            setPropBin(path, size, data, COMPRESS_METHOD_DEFAULT);
    }
}

void PTree::appendPropBin(const char *xpath, size32_t size, const void *data)
{
    CHECK_ATTRIBUTE(xpath);
    if (!xpath || '\0' == *xpath)
        appendLocal(size, data, true);
    else if ('[' == *xpath)
    {
        IPropertyTree *qualified = queryPropTree(xpath);
        if (!qualified)
            throw MakeIPTException(-1, "appendPropBin: qualifier unmatched %s", xpath);
        qualified->appendPropBin(nullptr, size, data);
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
            setPropBin(path, size, data, COMPRESS_METHOD_DEFAULT);
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
#ifdef _DEBUG
        //The following call can double the cost of finding a match from an IPropertyTree
        if (iter->next())
            AMBIGUOUS_PATH("getProp",xpath);
#endif
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
            dbgassertex(QUERYINTERFACE(_val, PTree));
            PTree *__val = static_cast<PTree *>(_val);
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

bool PTree::isArray(const char *xpath) const
{
    if (!xpath || !*xpath) //item in an array child of parent? I don't think callers ever access array container directly
        return arrayOwner && arrayOwner->isArray();
    else if (isAttribute(xpath))
        return false;
    else
    {
        StringBuffer path;
        const char *prop = splitXPath(xpath, path);
        assertex(prop);
        if (!isAttribute(prop))
        {
            if (path.length())
            {
                Owned<IPropertyTreeIterator> iter = getElements(path.str());
                if (!iter->first())
                    return false;
                IPropertyTree &branch = iter->query();
#ifdef _DEBUG
                if (iter->next())
                    AMBIGUOUS_PATH("isArray", xpath);
#endif
                return branch.isArray(prop);
            }
            else
            {
                IPropertyTree *child = children->query(xpath);
                if (child)
                {
                    PTree *tree = static_cast<PTree *>(child);
                    return (tree && tree->value && tree->value->isArray());
                }
            }
        }
    }
    return false;
}

void PTree::addPTreeArrayItem(IPropertyTree *existing, const char *xpath, PTree *val, aindex_t pos)
{
    IPropertyTree *iptval = static_cast<IPropertyTree *>(val);
    PTree *tree = nullptr;
    if (existing)
    {
        dbgassertex(QUERYINTERFACE(existing, PTree));
        tree = static_cast<PTree *>(existing);
        if (tree->value && tree->value->isArray())
        {
            val->setOwner(tree->value);
            if ((aindex_t) -1 == pos)
                tree->value->addElement(iptval);
            else
                tree->value->setElement(pos, iptval);
            return;
        }
    }

    IPTArrayValue *array = new CPTArray();
    IPropertyTree *container = create(xpath, array);
    val->setOwner(array);
    if (existing)
    {
        array->addElement(LINK(existing));
        assertex((aindex_t) -1 == pos || 0 == pos);
        if ((aindex_t) -1 == pos)
            array->addElement(iptval);
        else
            array->setElement(0, iptval);
        tree->setOwner(array);
        children->replace(xpath, container);
    }
    else
    {
        array->addElement(iptval);
        children->set(xpath, container);
    }
}

IPropertyTree *PTree::addPropTree(const char *xpath, IPropertyTree *val, bool alwaysUseArray)
{
    if (!xpath || '\0' == *xpath)
        throw MakeIPTException(PTreeExcpt_InvalidTagName, "Invalid xpath for property tree insertion specified");
    else
    {
        CHECK_ATTRIBUTE(xpath);
        const char *x = xpath;
        for (;;)
        {
            if (!*x++)
            {
                IPropertyTree *_val = ownPTree(val);
                dbgassertex(QUERYINTERFACE(_val, PTree));
                PTree *__val = static_cast<PTree *>(_val);

                /* NB: potentially param xpath is a reference to the existing name.
                 * So fetch new name ptr after set.
                 */
                __val->setName(xpath);
                xpath = __val->queryName();

                addingNewElement(*_val, -1);
                if (checkChildren())
                {
                    IPropertyTree *child = children->query(xpath);
                    if (child)
                    {
                        addPTreeArrayItem(child, xpath, __val);
                        return _val;
                    }
                }
                else
                    createChildMap();
                if (alwaysUseArray)
                    addPTreeArrayItem(nullptr, xpath, __val);
                else
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
            if (!qualifier.isEmpty())
            {
                pos = ((PTree *)child)->getChildMatchPos(qualifier);
                if ((aindex_t) -1 == pos)
                    throw MakeIPTException(-1, "addPropTree: qualifier unmatched %s", xpath);
            }
            IPropertyTree *_val = ownPTree(val);
            dbgassertex(QUERYINTERFACE(_val, PTree));
            PTree *__val = static_cast<PTree *>(_val);
            __val->setName(path);
            addingNewElement(*_val, pos);
            if (child)
            {
                addPTreeArrayItem(child, path, __val, pos);
            }
            else
            {
                if (!checkChildren()) createChildMap();
                if (alwaysUseArray)
                    addPTreeArrayItem(nullptr, path, __val);
                else
                    children->set(path, _val);
                children->set(path, _val);
            }
            return _val;
        }
    }
}

IPropertyTree *PTree::addPropTree(const char *xpath, IPropertyTree *val)
{
    return addPropTree(xpath, val, false);
}

IPropertyTree *PTree::addPropTreeArrayItem(const char *xpath, IPropertyTree *val)
{
    return addPropTree(xpath, val, true);
}

bool PTree::removeTree(IPropertyTree *child)
{
    if (child == this)
        throw MakeIPTException(-1, "Cannot remove self");

    if (checkChildren())
    {
        IPropertyTree *_child = children->query(child->queryName());
        if (_child)
        {
            if (child == _child)
                return children->removeExact(child);
            else
            {
                IPTArrayValue *value = ((PTree *)_child)->queryValue();
                if (value && value->isArray())
                {
                    unsigned pos = value->find(child);
                    if (NotFound != pos)
                    {
                        removingElement(child, pos);
                        value->removeElement(pos);
                        if (0 == value->elements())
                            children->removeExact(_child);
                        return true;
                    }
                }
            }
        }
    }
    return false;
}

bool PTree::removeProp(const char *xpath)
{
    if (xpath && isAttribute(xpath))
        return removeAttribute(xpath);

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
                        if (i && i <= child->value->elements())
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

StringBuffer &PTree::getName(StringBuffer &ret) const
{
    ret.append(queryName());
    return ret;
}

typedef CopyReferenceArrayOf<AttrValue> AttrArray;
IAttributeIterator *PTree::getAttributes(bool sorted) const
{
    class CAttributeIterator : implements IAttributeIterator, public CInterface
    {
        Linked<const PTree> parent;
        AttrValue *cur = nullptr;
    public:
        IMPLEMENT_IINTERFACE;

        CAttributeIterator(const PTree *_parent) : parent(_parent)
        {
        }
    // IAttributeIterator impl.
        virtual bool first() override
        {
            cur = parent->getNextAttribute(nullptr);
            return cur ? true : false;
        }
        virtual bool next() override
        {
            cur = parent->getNextAttribute(cur);
            return cur ? true : false;
        }
        virtual bool isValid() override { return cur ? true : false; }
        virtual const char *queryName() const override
        {
            return cur->key.get();
        }
        virtual const char *queryValue() const override
        {
            return cur->value.get();
        }
        virtual StringBuffer &getValue(StringBuffer &out) override
        {
            out.append(queryValue());
            return out;
        }
        virtual unsigned count() override { return parent->getAttributeCount(); }
    };
    class CSortedAttributeIterator : implements IAttributeIterator, public CInterface
    {
        typedef ArrayIteratorOf<AttrArray, AttrValue &> AttrIterator;

        AttrArray attrs;
        AttrValue *cur;
        AttrIterator *iter;
        Linked<const PTree> parent;
    public:
        IMPLEMENT_IINTERFACE;

        static int compareAttrs(AttrValue * const *ll, AttrValue * const *rr)
        {
            return stricmp((*ll)->key.get(), (*rr)->key.get());
        };

        CSortedAttributeIterator(const PTree *_parent) : cur(NULL), iter(NULL), parent(_parent)
        {
            AttrValue *cur = parent->getNextAttribute(nullptr);
            if (cur)
            {
                do
                {
                    attrs.append(*cur);
                    cur = parent->getNextAttribute(cur);
                }
                while (cur);
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
        virtual bool first() override
        {
            if (!iter) return false;
            if (!iter->first()) { cur = NULL; return false; }
            cur = &iter->query();
            return true;
        }
        virtual bool next() override
        {
            if (!iter) return false;
            if (!iter->next()) { cur = NULL; return false; }
            cur = &iter->query();
            return true;
        }
        virtual bool isValid() override { return cur!=NULL; }
        virtual const char *queryName() const override
        {
            assertex(cur);
            return cur->key.get();
        }
        virtual const char *queryValue() const override
        {
            assertex(cur);
            return cur->value.get();
        }
        virtual StringBuffer &getValue(StringBuffer &out) override
        {
            assertex(cur);
            return out.append(queryValue());
        }
        virtual unsigned count() override { return attrs.ordinality(); }
    };
    if (sorted)
        return new CSortedAttributeIterator(this);
    else
        return new CAttributeIterator(this);
}

///////////////////
class CIndexIterator : implements IPropertyTreeIterator, public CInterface
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
    virtual bool first() override
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
    virtual bool isValid() override
    {
        return celem && (index >= current);
    }
    virtual bool next() override
    {
        celem = NULL;
        return false;
    }
    virtual IPropertyTree & query() override
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
                throw MakeXPathException(xpath-1, PTreeExcpt_XPath_Unsupported, 0, "\"/\" expected");
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
                            const char *start = xxpath-1;
                            for (;;)
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
                                        const char *lhsStart = start+1;
                                        Owned<IPropertyTreeIterator> siter = checkMapIterator(lhsStart, *child);
                                        if (!siter)
                                        {
                                            if (wild)
                                                iter.setown(new PTIdMatchIterator(this, id, isnocase(), flags & iptiter_sort));
                                            else
                                                iter.setown(child->getElements(NULL));
                                            StringAttr qualifier(start, (xxpath-1)-start);
                                            siter.setown(new PTStackIterator(iter.getClear(), qualifier.get()));
                                        }
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
                                    if (!wild)
                                    {
                                        const char *lhsStart = start+1;
                                        Owned<IPropertyTreeIterator> mapIter = checkMapIterator(lhsStart, *child);
                                        if (mapIter)
                                        {
                                            xxpath = lhsStart;
                                            iter.swap(mapIter);
                                            break;
                                        }
                                    }
                                    if (wild)
                                        iter.setown(new PTIdMatchIterator(this, id, isnocase(), flags & iptiter_sort));
                                    else
                                        iter.setown(child->getElements(NULL));
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

unsigned PTree::numChildren() const
{
    if (!checkChildren()) return 0;
    return children->numChildren();
}

unsigned PTree::getCount(const char *xpath) const
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
    const char *_name = queryName();
    tgt.append(_name ? _name : "");
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
    DeserializeContext deserializeContext;
    deserializeSelf(src, deserializeContext);

    for (;;)
    {
        size32_t pos = src.getPos();
        deserializeContext.name.clear();
        src.read(deserializeContext.name);
        if (deserializeContext.name.isEmpty())
            break;
        src.reset(pos); // reset to re-read tree name
        IPropertyTree *child = create(src);
        addPropTree(deserializeContext.name, child);
    }
}

void PTree::deserializeSelf(MemoryBuffer &src, DeserializeContext &deserializeContext)
{
    setName(nullptr); // needs to be cleared before flags changed

    src.read(deserializeContext.name);
    src.read(flags);
    if (deserializeContext.name.isEmpty())
        setName(nullptr);
    else
        setName(deserializeContext.name);

    for (;;)
    {
        deserializeContext.name.clear();
        src.read(deserializeContext.name);
        if (deserializeContext.name.isEmpty())
            break;
        deserializeContext.value.clear();
        src.read(deserializeContext.value);
        setProp(deserializeContext.name, deserializeContext.value);
    }

    size32_t size;
    unsigned pos = src.getPos();
    src.read(size);
    if (value)
        delete value;
    if (size)
    {
        src.reset(pos);
        value = new CPTValue(src);
    }
    else
        value = nullptr;
}

IPropertyTree *PTree::clone(const IPropertyTree &srcTree, bool sub)
{
    Owned<IPropertyTree> _dstTree = create(srcTree.queryName());
    PTree *dstTree = static_cast<PTree *>(_dstTree.get());
    dstTree->cloneContents(srcTree, sub);
    return _dstTree.getClear();
}

void PTree::cloneIntoSelf(const IPropertyTree &srcTree, bool sub)
{
    setName(srcTree.queryName());
    cloneContents(srcTree, sub);
}

void PTree::cloneContents(const IPropertyTree &srcTree, bool sub)
{
    //MORE: Should any flags be cloned from the srcTree?

    bool srcBinary = srcTree.isBinary(NULL);
    //All implementations of IPropertyTree have PTree as a base class, therefore static cast is ok.
    IPTArrayValue *v = static_cast<const PTree &>(srcTree).queryValue();
    setValue(v?new CPTValue(v->queryValueRawSize(), v->queryValueRaw(), srcBinary, v->getCompressionType(), COMPRESS_METHOD_NONE):NULL, srcBinary);

    Owned<IAttributeIterator> attrs = srcTree.getAttributes();
    if (attrs->first())
    {
        do
        {
            setProp(attrs->queryName(), attrs->queryValue());
        }
        while (attrs->next());
    }

    if (sub)
    {
        Owned<IPropertyTreeIterator> iter = srcTree.getElements("*");
        if (iter->first())
        {
            do
            {
                IPropertyTree &child = iter->query();
                IPropertyTree *newChild = clone(child, sub);
                addPropTree(newChild->queryName(), newChild);
            }
            while (iter->next());
        }
    }
}

IPropertyTree *PTree::ownPTree(IPropertyTree *tree)
{
    if (!isEquivalent(tree) || tree->IsShared() || isCaseInsensitive() != tree->isCaseInsensitive())
    {
        IPropertyTree *newTree = clone(*tree, true);
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
    IPTArrayValue *newValue = new CPTValue(l, data, _binary, COMPRESS_METHOD_NONE, COMPRESS_METHOD_DEFAULT);
    Owned<IPropertyTree> tree = create(queryName(), newValue);
    PTree *_tree = QUERYINTERFACE(tree.get(), PTree); assertex(_tree);

    if (_binary)
        IptFlagSet(_tree->flags, ipt_binary);
    else
        IptFlagClr(_tree->flags, ipt_binary);

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

        // detach children and attributes of this branch now owned by element of newly created array.
        IPropertyTree *element1 = detach();
        array = new CPTArray();
        addingNewElement(*element1, ANE_APPEND);
        static_cast<PTree *>(element1)->setOwner(array);
        array->addElement(element1);
        value = array;
    }
    _tree->setOwner(array);
    tree->Link();
    if (-1 == pos)
        array->addElement(tree);
    else
        array->setElement(pos, tree);
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
    for (;;)
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
            for (;;)
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
            for (;;)
            {
                if (matchElem->isBinary(tProp))
                    UNIMPLEMENTED_IPT;
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
                            IWARNLOG("Possible deprecated use of quoted numeric comparison operation: %s", xxpath);
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
                    ret = match(wild, numeric, xxpath, tType, s.str(), s.length(), rhs, rhslength, nocase);
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

AttrValue *PTree::findAttribute(const char *key) const
{
    if (attrs)
    {
        AttrValue *a = attrs+numAttrs;
        if (isnocase())
        {
            while (a-- != attrs)
            {
                if (strieq(a->key.get(), key))
                    return a;
            }
        }
        else
        {
            while (a-- != attrs)
            {
                if (streq(a->key.get(), key))
                    return a;
            }
        }
    }
    return nullptr;
}

const char *PTree::getAttributeValue(const char *key) const
{
    AttrValue *e = findAttribute(key);
    if (e)
        return e->value.get();
    return nullptr;
}

unsigned PTree::getAttributeCount() const
{
    return numAttrs;
}

AttrValue *PTree::getNextAttribute(AttrValue *cur) const
{
    if (0 == numAttrs)
        return nullptr;
    else if (nullptr == cur)
        return attrs;
    else
    {
        if (cur == (attrs+(numAttrs-1)))
            return nullptr;
        return ++cur;
    }
}


//////////////////////

// LocalPTree

static RelaxedAtomic<unsigned> numLocalTrees;
unsigned queryNumLocalTrees()
{
    return numLocalTrees;
}

LocalPTree::LocalPTree(const char *_name, byte _flags, IPTArrayValue *_value, ChildMap *_children) : PTree(_flags|ipt_fast, _value, _children)
{
    if (_name)
        setName(_name);
    numLocalTrees++;
}

LocalPTree::~LocalPTree()
{
    numLocalTrees--;
    name.destroy();
    if (!attrs)
        return;
    AttrValue *a = attrs+numAttrs;
    while (a--!=attrs)
    {
        a->key.destroy();
        a->value.destroy();
    }
    free(attrs);
}

const char *LocalPTree::queryName() const
{
    return name.get();
}

void LocalPTree::setName(const char *_name)
{
    if (_name==name.get())
        return;
    AttrStr *oname = name.getPtr();  // Don't free until after we copy - they could overlap
    if (!name.set(_name))
        name.setPtr(AttrStr::create(_name));
    if (oname)
        AttrStr::destroy(oname);
}

bool LocalPTree::removeAttribute(const char *key)
{
    AttrValue *del = findAttribute(key);
    if (!del)
        return false;
    if (arrayOwner)
    {
        CQualifierMap *map = arrayOwner->queryMap();
        if (map)
            map->removeEntryIfMapped(key, del->value.get(), this);
    }
    numAttrs--;
    unsigned pos = del-attrs;
    del->key.destroy();
    del->value.destroy();
    memmove(attrs+pos, attrs+pos+1, (numAttrs-pos)*sizeof(AttrValue));
    return true;
}

void LocalPTree::setAttribute(const char *inputkey, const char *val, bool encoded)
{
    if (!inputkey)
        return;
    const char *key = inputkey;
    if (encoded)
    {
        if (*key!='~')
            encoded=false;
        else
            key++;
    }
    if (!validateXMLTag(key+1))
        throw MakeIPTException(-1, "Invalid xml attribute: %s", key);
    if (!val)
        val = "";  // cannot have NULL value
    AttrValue *v = findAttribute(key);
    AttrStr *goer = nullptr;
    if (v)
    {
        if (streq(v->value.get(), val))
            return;
        goer = v->value.getPtr();
    }
    else
    {
        attrs = (AttrValue *)realloc(attrs, (numAttrs+1)*sizeof(AttrValue));
        v = new(&attrs[numAttrs++]) AttrValue;  // Initialize new AttrValue
        if (!v->key.set(inputkey)) //AttrStr will not return encoding marker when get() is called
            v->key.setPtr(isnocase() ? AttrStr::createNC(inputkey) : AttrStr::create(inputkey));
    }
    if (arrayOwner)
    {
        CQualifierMap *map = arrayOwner->queryMap();
        if (map)
        {
            if (goer)
                map->replaceEntryIfMapped(key, v->value.get(), val, this);
            else
                map->insertEntryIfMapped(key, val, this);
        }
    }

    if (!v->value.set(val))
        v->value.setPtr(AttrStr::create(val));
    if (goer)
        AttrStr::destroy(goer);
}

#ifdef TRACE_STRING_SIZE
std::atomic<__int64> AttrStr::totsize { 0 };
std::atomic<__int64> AttrStr::maxsize { 0 };
#endif

#ifdef TRACE_ATOM_SIZE
std::atomic<__int64> AttrStrAtom::totsize { 0 };
std::atomic<__int64> AttrStrAtom::maxsize { 0 };
#endif

///////////////////

static RelaxedAtomic<unsigned> numAtomTrees;
unsigned queryNumAtomTrees()
{
    return numAtomTrees;
}

CAtomPTree::CAtomPTree(const char *_name, byte _flags, IPTArrayValue *_value, ChildMap *_children) : PTree(_flags|ipt_lowmem, _value, _children)
{
    numAtomTrees++;
    if (_name)
        setName(_name);
}

CAtomPTree::~CAtomPTree()
{
    numAtomTrees--;
    bool nc = isnocase();
    HashKeyElement *name_ptr = name.getPtr();
    if (name_ptr)
    {
        AtomRefTable *kT = nc?keyTableNC:keyTable;
#ifdef TRACE_ATOM_SIZE
        size_t gosize = sizeof(HashKeyElement)+strlen(name_ptr->get())+1;
        if (kT->releaseKey(name_ptr))
            AttrStrAtom::totsize -= gosize;
#else
        kT->releaseKey(name_ptr);
#endif
    }
    if (!attrs)
        return;
    AttrValue *a = attrs+numAttrs;
    {
        CriticalBlock block(hashcrit);
        while (a--!=attrs)
        {
            if (a->key.isPtr())
                attrHT->removekey(a->key.getPtr(), nc);
            if (a->value.isPtr())
                attrHT->removeval(a->value.getPtr());
        }
        freeAttrArray(attrs, numAttrs);
    }
}

void CAtomPTree::setName(const char *_name)
{
    AtomRefTable *kT = isnocase()?keyTableNC:keyTable;
    HashKeyElement *oname = name.getPtr(); // NOTE - don't release yet as could overlap source name
    if (!_name)
        name.setPtr(nullptr);
    else
    {
        if (!validateXMLTag(_name))
            throw MakeIPTException(PTreeExcpt_InvalidTagName, ": %s", _name);
        if (!name.set(_name))
        {
#ifdef TRACE_ALL_ATOM
            DBGLOG("TRACE_ALL_ATOM: %s", _name);
#endif
#ifdef TRACE_ATOM_SIZE
            bool didCreate;
            name.setPtr(kT->queryCreate(_name, didCreate));
            if (didCreate)
            {
                AttrStrAtom::totsize += sizeof(HashKeyElement)+strlen(_name)+1;
                if (AttrStrAtom::totsize > AttrStrAtom::maxsize)
                {
                    AttrStrAtom::maxsize.store(AttrStrAtom::totsize);
                    DBGLOG("TRACE_ATOM_SIZE: total size now %" I64F "d", AttrStrAtom::maxsize.load());
                }
            }
#else
            name.setPtr(kT->queryCreate(_name));
#endif
        }
    }
    if (oname)
    {
#ifdef TRACE_ATOM_SIZE
        size_t gosize = sizeof(HashKeyElement)+strlen(oname->get())+1;
        if (kT->releaseKey(oname))
            AttrStrAtom::totsize -= gosize;
#else
        kT->releaseKey(oname);
#endif
    }
}

const char *CAtomPTree::queryName() const
{
    return name.get();
}

unsigned CAtomPTree::queryHash() const
{
    if (name.isPtr())
    {
        assert(name.getPtr());
        return name.getPtr()->queryHash();
    }
    else
    {
        const char *_name = name.get();
        size32_t nl = strlen(_name);
        return isnocase() ? hashnc_fnv1a((const byte *) _name, nl, fnvInitialHash32): hashc_fnv1a((const byte *) _name, nl, fnvInitialHash32);
    }
}

AttrValue *CAtomPTree::newAttrArray(unsigned n)
{
    // NB crit must be locked
    if (!n)
        return nullptr;
    if (freelistmax<=n)
    {
        freelist = (AttrValue **)realloc(freelist, sizeof(AttrValue *)*(n+1));
        while (freelistmax<=n)
            freelist[freelistmax++] = nullptr;
    }
    AttrValue *&p = freelist[n];
    AttrValue *ret = p;
    if (ret)
        p = *(AttrValue **)ret;
    else
        ret = (AttrValue *)freeallocator.alloc(sizeof(AttrValue)*n);
    return ret;
}

void CAtomPTree::freeAttrArray(AttrValue *a, unsigned n)
{
    // NB crit must be locked
    if (a)
    {
        AttrValue *&p = freelist[n];
        *(AttrValue **)a = p;
        p = a;
    }
}

void CAtomPTree::setAttribute(const char *key, const char *val, bool encoded)
{
    if (!key)
        return;
    if (!validateXMLTag(key+1))
        throw MakeIPTException(-1, "Invalid xml attribute: %s", key);
    if (!val)
        val = "";  // cannot have NULL value
    AttrValue *v = findAttribute(key);
    if (v)
    {
        if (streq(v->value.get(), val))
            return;
        if (arrayOwner)
        {
            CQualifierMap *map = arrayOwner->queryMap();
            if (map)
                map->replaceEntryIfMapped(key, v->value.get(), val, this);
        }

        AttrStr * goer = v->value.getPtr();
        if (!v->value.set(val))
        {
            CriticalBlock block(hashcrit);
            if (goer)
                attrHT->removeval(goer);
            v->value.setPtr(attrHT->addval(val));
        }
        else if (goer)
        {
            CriticalBlock block(hashcrit);
            attrHT->removeval(goer);
        }
    }
    else
    {
        CriticalBlock block(hashcrit);
        AttrValue *newattrs = newAttrArray(numAttrs+1);
        if (attrs)
        {
            memcpy(newattrs, attrs, numAttrs*sizeof(AttrValue));
            freeAttrArray(attrs, numAttrs);
        }
        if (arrayOwner)
        {
            CQualifierMap *map = arrayOwner->queryMap();
            if (map)
                map->insertEntryIfMapped(key, val, this);
        }
        v = &newattrs[numAttrs];
        if (!v->key.set(key))
            v->key.setPtr(attrHT->addkey(key, isnocase()));
        //shared via atom table, may want to add this later... escaped and unescaped versions should be considered unique
        //if (encoded)
        //    v->key.setEncoded();
        if (!v->value.set(val))
            v->value.setPtr(attrHT->addval(val));
        numAttrs++;
        attrs = newattrs;
    }
}

bool CAtomPTree::removeAttribute(const char *key)
{
    AttrValue *del = findAttribute(key);
    if (!del)
        return false;
    numAttrs--;
    if (arrayOwner)
    {
        CQualifierMap *map = arrayOwner->queryMap();
        if (map)
            map->removeEntryIfMapped(key, del->value.get(), this);
    }
    CriticalBlock block(hashcrit);
    if (del->key.isPtr())
        attrHT->removekey(del->key.getPtr(), isnocase());
    if (del->value.isPtr())
        attrHT->removeval(del->value.getPtr());
    AttrValue *newattrs = newAttrArray(numAttrs);
    if (newattrs)
    {
        unsigned pos = del-attrs;
        memcpy(newattrs, attrs, pos*sizeof(AttrValue));
        memcpy(newattrs+pos, attrs+pos+1, (numAttrs-pos)*sizeof(AttrValue));
    }
    freeAttrArray(attrs, numAttrs+1);
    attrs = newattrs;
    return true;
}


///////////////////


bool isEmptyPTree(const IPropertyTree *t)
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

PTLocalIteratorBase::PTLocalIteratorBase(const PTree *_tree, const char *_id, bool _nocase, bool _sort) : nocase(_nocase), sort(_sort), id(_id), tree(_tree)
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

    for (;;)
    {
        for (;;)
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

SingleIdIterator::SingleIdIterator(const PTree &_tree, unsigned pos, unsigned _many) : many(_many), count(0), whichNext(pos-1), start(pos-1), current(NULL), tree(_tree)
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
    if ((whichNext>=count) || ((unsigned) -1 != many && whichNext>start+many))
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
        for (;;)
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
                        throw MakeXPathException(xpath-1, PTreeExcpt_XPath_Unsupported, 0, "\"/\" expected");
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

                    bool wild, numeric;
                    const char *start = xxpath;
                    readWildIdIndex(xxpath, wild, numeric);
                    size32_t s = xxpath-start;

                    if (s)
                    {
                         // NB: actually an id not qualifier, just sharing var.
                        qualifierText.clear().append(s, start);

                        bool mapped = false;
                        if (!wild && !numeric)
                        {
                            ChildMap *children = ((PTree *)element)->checkChildren();
                            if (children)
                            {
                                IPropertyTree *child = children->query(qualifierText);
                                if (child)
                                {
                                    if ('[' == *xxpath)
                                    {
                                        const char *newXXPath = xxpath+1;
                                        Owned<IPropertyTreeIterator> mapIter = checkMapIterator(newXXPath, *child);
                                        if (mapIter)
                                        {
                                            setIterator(mapIter.getClear());
                                            mapped = true;
                                            xxpath = newXXPath;
                                        }
                                    }
                                }
                            }
                        }
                        if (!mapped)
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

#define DEFAULT_PTREE_TYPE LocalPTree

// factory methods

IPropertyTree *createPTree(MemoryBuffer &src, byte flags)
{
    IPropertyTree *tree = createPTree(nullptr, flags);
    tree->deserialize(src);
    return tree;
}

IPropertyTree *createPTreeFromIPT(const IPropertyTree *srcTree, ipt_flags flags)
{
    Owned<PTree> tree = (PTree *)createPTree(NULL, flags);
    return tree->clone(*srcTree->queryBranch(NULL), true);
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

void _synchronizePTree(IPropertyTree *target, const IPropertyTree *source, bool removeTargetsNotInSource)
{
    Owned<IAttributeIterator> aiter = target->getAttributes();
    StringArray targetAttrs;
    if (removeTargetsNotInSource)
    {
        ForEach (*aiter)
            targetAttrs.append(aiter->queryName());
    }

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

            if (removeTargetsNotInSource)
                targetAttrs.zap(attr);
        }
    }

    if (removeTargetsNotInSource)
    {
        // remaining
        ForEachItemIn (a, targetAttrs)
            target->removeProp(targetAttrs.item(a));
    }

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
            target->setPropBin(NULL, srcMb.length(), srcMb.toByteArray(), COMPRESS_METHOD_DEFAULT);
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
            if (firstOfType.isEmpty() || 0 != strcmp(firstOfType, e.queryName()))
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
                _synchronizePTree(&e, sourceCompare, removeTargetsNotInSource);
            }
            else
                removeTreeList.append(e);
        }
    }

    if (removeTargetsNotInSource)
    {
        ForEachItemIn (rt, removeTreeList)
            target->removeTree(&removeTreeList.item(rt));
    }

    // add unprocessed source elements, not reference by name in target
    ForEachItemIn (s, toProcess)
    {
        IPropertyTree &e = toProcess.item(s);
        target->addPropTree(e.queryName(), LINK(&e));
    }
}

/* ensure target is equivalent to source whilst retaining elements already present in target.
 * presevers ordering of matching elements.
 * If removeTargetsNotInSource = true (default) elements in the target not present in the source will be removed
 */
void synchronizePTree(IPropertyTree *target, const IPropertyTree *source, bool removeTargetsNotInSource, bool rootsMustMatch)
{
    if (rootsMustMatch)
    {
        const char *srcName = source->queryName();
        const char *tgtName = target->queryName();
        if (0 != strcmp(srcName, tgtName))
            throw MakeIPTException(PTreeExcpt_Unsupported, "Cannot synchronize if root nodes mismatch");
    }
    _synchronizePTree(target, source, removeTargetsNotInSource);
}


IPropertyTree *ensurePTree(IPropertyTree *root, const char *xpath)
{
    return createPropBranch(root, xpath, true);
}

IPTreeReadException *createPTreeReadException(int code, const char *msg, const char *context, unsigned line, offset_t offset)
{
    //Do not use jlib_thrown_decl because it causes problems with VS2017 - I think because of beforeDispose() in CInterfaceOf.
    //The type of the object actually thrown is IPTreeReadException  which does have a jlib_thrown_decl - so it will still be caught.
    class CPTreeReadException : implements CInterfaceOf<IPTreeReadException>
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
        bufSize(_bufSize), readerOptions(_readerOptions), iEvent(&_iEvent)
    {
        if (!bufSize) bufSize = 0x20000;
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
        for (;;)
        {
            --bufPtr;
            if (!--n) break;
            if (10 == *bufPtr) --line;
        }
    }
    bool checkBOM()
    {
        bool utf16 = false;
        bool utf8 = false;
        // Note - technically the utf16 LE case could also be utf32 LE (utf32 BE would be 0x00 0x00 0xfe 0xff)
        // But utf32 is so rare that we ignore it for now
        switch ((unsigned char)nextChar)
        {
        case 0xff:
            readNext();
            if (0xfe == (unsigned char)nextChar)
                utf16 = true;
            break;
        case 0xfe:
            readNext();
            if (0xff == (unsigned char)nextChar)
                utf16 = true;
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
        else if (utf16)
            error("Unsupported utf16 format detected in BOM header", false);
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
        for (;;)
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
    void error(const char *msg=NULL, bool giveContext=true, PTreeReadExcptCode code=PTreeRead_syntax) __attribute__((noreturn))
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
    inline bool checkStartReadNext()
    {
        if (curOffset || nextChar) //not at starting state
            return true;
        return readNextToken();
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
protected:
    virtual void reset() override
    {
        resetState();
        PARENT::reset();
    }
    void readID(StringBuffer &id)
    {
        if (isValidXPathStartChr(nextChar))
        {
            for (;;)
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
                for (;;)
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
                            for (;;)
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
        for (;;)
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
                for (;;)
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
        for (;;)
        {
            readNext();
            while (']' == nextChar)
            {
                readNext();
                while (']' == nextChar)
                {
                    readNext();
                    if ('>' == nextChar)
                        return;
                    else
                        text.append(']');
                }
                text.append(']');
            }
            text.append(nextChar);
        }
    }
    void parsePI(StringBuffer &target)
    {
        readNext();
        if (!isValidXPathStartChr(nextChar))
            error("Invalid PI target");

        for (;;)
        {
            target.append(nextChar);
            readNext();
            if (!isValidXPathChr(nextChar))
                break;
        }
        skipWS();
        unsigned closeTag=0;
        for (;;)
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
    virtual bool find(const char *entity, StringBuffer &value) override
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

    virtual void reset() override
    {
        resetState();
        PARENT::reset();
    }

// IPTreeReader
    virtual void load() override { loadXML(); }
    virtual offset_t queryOffset() override { return curOffset; }

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
        StringBuffer completeTagname(tagName);
        if (ignoreNameSpaces)
        {
            const char *colon;
            if ((colon = strchr(tagName.str(), ':')) != NULL)
                tagName.remove(0, (size32_t)(colon - tagName.str() + 1));
        }
        iEvent->beginNode(tagName.str(), false, startOffset);
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
        iEvent->beginNodeContent(tagName.str());
        StringBuffer tagText;
        bool binary = base64;
        if (!endTag)
        {
            if (nextChar == '>')
            {
                for (;;)
                {
                    for (;;)
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
                            _decodeXML(r, mark.str(), tagText);
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
        iEvent->endNode(tagName.str(), tagText.length(), tagText.str(), binary, curOffset);
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
    CICopyArrayOf<CStateInfo> stack, freeStateInfo;

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
    virtual void load() override
    {
        while (next()) {}
    }

    virtual void reset() override
    {
        PARENT::reset();
        resetState();
    }

    virtual offset_t queryOffset() override { return curOffset; }

    virtual bool next() override
    {
        switch (state)
        {
            case headerStart:
            {
                if (!checkReadNext()) return false;
                if (checkBOM())
                    if (!checkReadNext()) return false;
                for (;;)
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
                for (;;)
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
                    stateInfo = &freeStateInfo.popGet();
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
                    iEvent->beginNode(stateInfo->wnsTag, false, startOffset);
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
                    for (;;)
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
                                const char *tb = mark.str();
                                const char *t = tb+l-1;
                                if (isspace(*t))
                                {
                                    while (t != tb && isspace(*(--t)));
                                    mark.setLength((size32_t)(t-tb+1));
                                }
                            }
                            stateInfo->tagText.ensureCapacity(mark.length());
                            _decodeXML(r, mark.str(), stateInfo->tagText);
                        }
                        if (endOfRoot && mark.length())
                        {
                            const char *m = mark.str();
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
                        const char *m = mark.str();
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
                const char *t = stateInfo->tag.str();
                const char *te = t+stateInfo->tag.length();
                for (;;)
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
                iEvent->endNode(stateInfo->wnsTag, stateInfo->tagText.length(), stateInfo->tagText.str(), stateInfo->binary, curOffset);
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

static IPTreeMaker *createDefaultPTreeMaker(byte flags, PTreeReaderOptions readFlags)
{
    bool noRoot = 0 != ((unsigned)readFlags & (unsigned)ptr_noRoot);
    if (0 != ((unsigned)readFlags & (unsigned)ptr_encodeExtNames))
        return new CPTreeEncodeNamesMaker(flags, NULL, NULL, noRoot);
    return new CPTreeMaker(flags, NULL, NULL, noRoot);
}

IPropertyTree *createPTree(ISimpleReadStream &stream, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IPTreeMaker> _iMaker;
    if (!iMaker)
    {
        iMaker = createDefaultPTreeMaker(flags, readFlags);
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
        iMaker = createDefaultPTreeMaker(flags, readFlags);
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
        iMaker = createDefaultPTreeMaker(flags, readFlags);
        _iMaker.setown(iMaker);
    }
    Owned<IPTreeReader> reader = createXMLBufferReader(xml, len, *iMaker, readFlags);
    reader->load();
    return LINK(iMaker->queryRoot());
}


void addPTreeItem(IPropertyTree *ptree, const char * name, const char * value)
{
    ptree->addPropTreeArrayItem(name, createPTree())->setProp(nullptr, value);
}

//////////////////////////
/////////////////////////

inline bool isHiddenWhenSanitized(const char *val)
{
    if (!val || !*val)
        return false;
    return !(streq(val, "0") || streq(val, "1") || strieq(val, "true") || strieq(val, "false") || strieq(val, "yes") || strieq(val, "no"));
}

inline bool isSanitizedAndHidden(const char *val, byte flags, bool attribute)
{
    bool sanitize = (attribute) ? ((flags & YAML_SanitizeAttributeValues)!=0) : ((flags & YAML_Sanitize)!=0);
    if (sanitize)
        return isHiddenWhenSanitized(val);
    return false;
}

static void _toXML(const IPropertyTree *tree, IIOStream &out, unsigned indent, unsigned flags)
{
    const char *name = tree->queryName();
    if (!name) name = "__unnamed__";
    bool isBinary = tree->isBinary(NULL);
    bool inlinebody = true;
    if (flags & XML_Embed) writeCharsNToStream(out, ' ', indent);
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
                    if (flags & XML_LineBreak) inlinebody = false;
                    first = false;
                    writeCharToStream(out, ' ');
                }
                else if ((flags & XML_LineBreakAttributes) && it->count() > 3)
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
                    if (isSanitizedAndHidden(val, flags, true))
                        writeCharsNToStream(out, '*', strlen(val));
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
        if (flags & XML_LineBreak) inlinebody = false;
        writeStringToStream(out, " xsi:type=\"SOAP-ENC:base64\"");
        empty = (!tree->getPropBin(NULL, thislevelbin))||(thislevelbin.length()==0);
    }
    else
    {
        if (tree->isCompressed(NULL))
        {
            empty = false; // can't be empty if compressed;
            verifyex(tree->getProp(NULL, _thislevel));
            thislevel = _thislevel.str();
        }
        else
            empty = (NULL == (thislevel = tree->queryProp(NULL)));
    }
    if (sub->first())
    {
        if (flags & XML_LineBreak) inlinebody = false;
    }
    else if (empty && !(flags & XML_Sanitize))
    {
        if (flags & XML_LineBreak)
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
                if (isHiddenWhenSanitized(thislevel))
                    writeCharsNToStream(out, '*', strlen(thislevel));
                else
                    writeStringToStream(out, thislevel);
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
                JBASE64_Encode(thislevelbin.toByteArray(), thislevelbin.length(), out, true);
        }
        else
        {
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
    if (flags & XML_LineBreak)
        writeStringToStream(out, ">\n");
    else
        writeCharToStream(out, '>');
}

class CStringBufferMarkupIOAdapter : public CInterfaceOf<IIOStream>
{
    StringBuffer &out;
public:
    CStringBufferMarkupIOAdapter(StringBuffer &_out) : out(_out) { }
    virtual void flush() override { }
    virtual size32_t read(size32_t len, void * data) override { UNIMPLEMENTED_IPT; }
    virtual size32_t write(size32_t len, const void * data) override { out.append(len, (const char *)data); return len; }
};

jlib_decl StringBuffer &toXML(const IPropertyTree *tree, StringBuffer &ret, unsigned indent, unsigned flags)
{
    CStringBufferMarkupIOAdapter adapter(ret);
    _toXML(tree->queryBranch(NULL), adapter, indent, flags);
    return ret;
}

void toXML(const IPropertyTree *tree, IIOStream &out, unsigned indent, unsigned flags)
{
    _toXML(tree, out, indent, flags);
}

void printXML(const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    StringBuffer xml;
    toXML(tree, xml, indent, flags);
    printf("%s", xml.str());
}

void dbglogXML(const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    StringBuffer xml;
    toXML(tree, xml, indent, flags);
    DBGLOG("%s", xml.str());
}

void saveXML(const char *filename, const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    OwnedIFile ifile = createIFile(filename);
    saveXML(*ifile, tree, indent, flags);
}

void saveXML(IFile &ifile, const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    OwnedIFileIO ifileio = ifile.open(IFOcreate);
    if (!ifileio)
        throw MakeStringException(0, "saveXML: could not find %s to open", ifile.queryFilename());
    saveXML(*ifileio, tree, indent, flags);
    ifileio->close(); // Ensure errors are reported
}

void saveXML(IFileIO &ifileio, const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    Owned<IIOStream> stream = createIOStream(&ifileio);
    stream.setown(createBufferedIOStream(stream));
    saveXML(*stream, tree, indent, flags);
}

void saveXML(IIOStream &stream, const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    toXML(tree, stream, indent, flags);
}

/////////////////////////

void checkWriteJSONDelimiter(IIOStream &out, bool &delimit)
{
    if (delimit)
        writeCharToStream(out, ',');
    delimit = false;
}

static void writeJSONNameToStream(IIOStream &out, const char *name, unsigned indent, bool &delimit)
{
    if (!name)
        return;
    checkWriteJSONDelimiter(out, delimit);
    if (indent)
    {
        writeCharToStream(out, '\n');
        writeCharsNToStream(out, ' ', indent);
    }
    else
        writeCharToStream(out, ' ');

    writeCharToStream(out, '"');
    writeStringToStream(out, name);
    writeStringToStream(out, "\": ");
    delimit = false;
}

static void writeJSONValueToStream(IIOStream &out, const char *val, bool &delimit, bool hidden=false)
{
    checkWriteJSONDelimiter(out, delimit);
    delimit = true;
    if (!val)
    {
        writeStringToStream(out, "null");
        return;
    }
    writeCharToStream(out, '"');
    if (hidden)
        writeCharsNToStream(out, '*', strlen(val));
    else
    {
        StringBuffer s;
        writeStringToStream(out, encodeJSON(s, val));
    }
    writeCharToStream(out, '"');
}

static void writeJSONBase64ValueToStream(IIOStream &out, const char *val, size32_t len, bool &delimit, bool hidden)
{
    checkWriteJSONDelimiter(out, delimit);
    delimit = true;
    if (!val)
    {
        writeStringToStream(out, "null");
        return;
    }
    writeCharToStream(out, '"');
    if (hidden)
        JBASE64_Encode("****", strlen("****"), out, false);
    else
        JBASE64_Encode(val, len, out, false);
    writeCharToStream(out, '"');
}
bool isRootArrayObjectHidden(bool root, const char *name, byte flags)
{
    return ((flags & JSON_HideRootArrayObject) && root && name && streq(name,"__array__"));
}
static void _toJSON(const IPropertyTree *tree, IIOStream &out, unsigned indent, byte flags, bool &delimit, bool root=false, bool isArrayItem=false)
{
    Owned<IAttributeIterator> it = tree->getAttributes(true);
    bool hasAttributes = it->first();
    bool complex = (hasAttributes || tree->hasChildren() || tree->isBinary());
    bool isBinary = tree->isBinary(NULL);

    const char *name = tree->queryName();
    if (!root && !isArrayItem)
    {
        if (!name || !*name)
            name = "__unnamed__";
        if (!isPTreeNameEncoded(tree))
            writeJSONNameToStream(out, name, (flags & JSON_Format) ? indent : 0, delimit);
        else
        {
            StringBuffer decoded;
            decodePtreeName(decoded, name);
            writeJSONNameToStream(out, decoded.str(), (flags & JSON_Format) ? indent : 0, delimit);
        }
    }

    checkWriteJSONDelimiter(out, delimit);

    if (isArrayItem && (flags & JSON_Format))
    {
        writeCharToStream(out, '\n');
        writeCharsNToStream(out, ' ', indent);
    }

    bool hiddenRootArrayObject = isRootArrayObjectHidden(root, name, flags);
    if (!hiddenRootArrayObject)
    {
        if (root || complex)
        {
            writeCharToStream(out, '{');
            delimit = false;
        }

        if (hasAttributes)
        {
            ForEach(*it)
            {
                const char *key = it->queryName();
                if (!isBinary || stricmp(key, "@xsi:type")!=0)
                {
                    const char *val = it->queryValue();
                    if (val)
                    {
                        if (!isPTreeAttributeNameEncoded(tree, key))
                            writeJSONNameToStream(out, key, (flags & JSON_Format) ? indent+1 : 0, delimit);
                        else
                        {
                            StringBuffer decoded;
                            decodePtreeName(decoded, key);
                            writeJSONNameToStream(out, decoded.str(), (flags & JSON_Format) ? indent+1 : 0, delimit);
                        }
                        if (flags & JSON_SanitizeAttributeValues)
                            writeJSONValueToStream(out, val, delimit, isHiddenWhenSanitized(val));
                        else
                        {
                            StringBuffer encoded;
                            encodeJSON(encoded, val);
                            writeJSONValueToStream(out, encoded.str(), delimit);
                        }
                    }
                }
            }
        }
    }
    MemoryBuffer thislevelbin;
    StringBuffer _thislevel;
    const char *thislevel = NULL; // to avoid uninitialized warning
    bool isNull = true;
    if (!hiddenRootArrayObject)
    {
        if (isBinary)
        {
            isNull = (!tree->getPropBin(NULL, thislevelbin))||(thislevelbin.length()==0);
        }
        else
        {
            if (tree->isCompressed(NULL))
            {
                isNull = false; // can't be empty if compressed;
                verifyex(tree->getProp(NULL, _thislevel));
                thislevel = _thislevel.str();
            }
            else
                isNull = (NULL == (thislevel = tree->queryProp(NULL)));
        }

        if (isNull && !root && !complex)
        {
            writeJSONValueToStream(out, NULL, delimit);
            return;
        }
    }

    Owned<IPropertyTreeIterator> sub = tree->getElements(hiddenRootArrayObject ? "__item__" : "*", 0 != (flags & JSON_SortTags) ? iptiter_sort : iptiter_null);
    //note that detection of repeating elements relies on the fact that ptree elements
    //of the same name will be grouped together
    bool repeatingElement = false;
    sub->first();
    while(sub->isValid())
    {
        Linked<IPropertyTree> element = &sub->query();
        const char *name = element->queryName();
        sub->next();
        if (!repeatingElement)
        {
            if (hiddenRootArrayObject)
            {
                writeCharToStream(out, '[');
                repeatingElement = true;
                delimit = false;
            }
            else if (sub->isValid() && streq(name, sub->query().queryName()))
            {
                if (flags & JSON_Format)
                    indent++;
                writeJSONNameToStream(out, name, (flags & JSON_Format) ? indent : 0, delimit);
                writeCharToStream(out, '[');
                repeatingElement = true;
                delimit = false;
            }
        }

        _toJSON(element, out, indent+1, flags, delimit, false, repeatingElement);

        if (repeatingElement && (!sub->isValid() || !streq(name, sub->query().queryName())))
        {
            if (flags & JSON_Format)
            {
                writeCharToStream(out, '\n');
                writeCharsNToStream(out, ' ', indent);
                indent--;
            }
            writeCharToStream(out, ']');
            repeatingElement = false;
            delimit = true;
        }
    }


    if (!hiddenRootArrayObject && !isNull)
    {
        if (complex)
            writeJSONNameToStream(out, isBinary ? "#valuebin" : "#value", (flags & JSON_Format) ? indent+1 : 0, delimit);
        if (isBinary)
            writeJSONBase64ValueToStream(out, thislevelbin.toByteArray(), thislevelbin.length(), delimit, flags & XML_Sanitize);
        else
        {
            writeJSONValueToStream(out, thislevel, delimit, isSanitizedAndHidden(thislevel, flags, false));
        }
    }

    if (!hiddenRootArrayObject)
    {
        if (root || complex)
        {
            if (flags & JSON_Format)
            {
                writeCharToStream(out, '\n');
                writeCharsNToStream(out, ' ', indent);
            }
            writeCharToStream(out, '}');
            delimit = true;
        }
    }
}

jlib_decl StringBuffer &toJSON(const IPropertyTree *tree, StringBuffer &ret, unsigned indent, byte flags)
{
    CStringBufferMarkupIOAdapter adapter(ret);
    bool delimit = false;
    _toJSON(tree->queryBranch(NULL), adapter, indent, flags, delimit, true);
    return ret;
}

void toJSON(const IPropertyTree *tree, IIOStream &out, unsigned indent, byte flags)
{
    bool delimit = false;
    _toJSON(tree, out, indent, flags, delimit, true);
}

void printJSON(const IPropertyTree *tree, unsigned indent, byte flags)
{
    StringBuffer json;
    toJSON(tree, json, indent, flags);
    printf("%s", json.str());
}

void dbglogJSON(const IPropertyTree *tree, unsigned indent, byte flags)
{
    StringBuffer json;
    toJSON(tree, json, indent, flags);
    DBGLOG("%s", json.str());
}


static inline void skipWS(const char *&xpath)
{
    while (isspace(*xpath)) xpath++;
}

static void _validateXPathSyntax(const char *xpath);
static void validateQualifier(const char *&xxpath)
{
    const char *xpath = xxpath;
    const char *start = xpath;
    skipWS(xpath);
    const char *lhsStart = xpath;
    for (;;)
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
            ++xpath;  // Signifies wild (now always true but still accepted...)
        }
        skipWS(xpath);
        char qu = *xpath;
        if (qu != '\'' && qu != '\"')
            throw MakeXPathException(xpath, PTreeExcpt_XPath_ParseError, 0, "Syntax error - no opening \" or \'");
        ++xpath;
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
            return validateXPathSyntax(head.str(), error);
        }
        return true;
    }
    else
        return validateXPathSyntax('/' == *xpath && '/' != *(xpath+1) ? xpath+1 : xpath, error);
    return true;
}

bool areMatchingPTrees(const IPropertyTree * left, const IPropertyTree * right)
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
    for (;;)
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

template <class BASE_PTREE>
class COrderedPTree : public BASE_PTREE
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

        virtual unsigned numChildren() const override { return order.ordinality(); }
        virtual IPropertyTreeIterator *getIterator(bool sort) override
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
        virtual bool set(const char *key, IPropertyTree *tree) override
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
        virtual bool replace(const char *key, IPropertyTree *tree) override // provides different semantics, used if element being replaced is not to be treated as deleted.
        {
            return set(key, tree);
        }
        virtual bool remove(const char *key) override
        {
            IPropertyTree *child = BASECHILDMAP::find(*key);
            if (!child)
                return false;
            order.zap(*child);
            return BASECHILDMAP::removeExact(child);
        }
        virtual bool removeExact(IPropertyTree *child) override
        {
            order.zap(*child);
            return BASECHILDMAP::removeExact(child);
        }
    };
public:
    typedef COrderedPTree<BASE_PTREE> SELF;
    COrderedPTree<BASE_PTREE>(const char *name=NULL, byte flags=ipt_none, IPTArrayValue *value=NULL, ChildMap *children=NULL)
        : BASE_PTREE(name, flags|ipt_ordered, value, children) { }

    virtual bool isEquivalent(IPropertyTree *tree) const override { return (NULL != QUERYINTERFACE(tree, COrderedPTree<BASE_PTREE>)); }
    virtual IPropertyTree *create(const char *name=NULL, IPTArrayValue *value=NULL, ChildMap *children=NULL, bool existing=false) override
    {
        return new COrderedPTree<BASE_PTREE>(name, SELF::flags, value, children);
    }
    virtual IPropertyTree *create(MemoryBuffer &mb) override
    {
        IPropertyTree *tree = new COrderedPTree<BASE_PTREE>();
        tree->deserialize(mb);
        return tree;
    }
    virtual void createChildMap() override
    {
        if (SELF::isnocase())
            SELF::children = new COrderedChildMap<ChildMapNC>();
        else
            SELF::children = new COrderedChildMap<ChildMap>();
    }
};

IPropertyTree *createPTree(byte flags)
{
    return createPTree(NULL, flags);
}

IPropertyTree *createPTree(const char *name, byte flags)
{
    switch (flags & (ipt_ordered|ipt_fast|ipt_lowmem))
    {
    case ipt_ordered|ipt_fast:
        return new COrderedPTree<LocalPTree>(name, flags);
    case ipt_ordered|ipt_lowmem:
        return new COrderedPTree<CAtomPTree>(name, flags);
    case ipt_ordered:
        return new COrderedPTree<DEFAULT_PTREE_TYPE>(name, flags);
    case ipt_fast:
        return new LocalPTree(name, flags);
    case ipt_lowmem:
        return new CAtomPTree(name, flags);
    case 0:
        return new DEFAULT_PTREE_TYPE(name, flags);
    default:
        throwUnexpectedX("Invalid flags - ipt_fast and ipt_lowmem should not be specified together");
    }
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
    using PARENT::readNextToken;
    using PARENT::checkReadNext;
    using PARENT::checkStartReadNext;
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
        case '-':
            value.append(nextChar);
            readNext();
            //fall through
        default:
            if (!isdigit(nextChar))
                error("Bad value");
            type = elementTypeInteger;
            bool exponent = false;
            while (isdigit(nextChar) || '.'==nextChar || 'e'==nextChar || 'E'==nextChar)
            {
                if ('e'==nextChar || 'E'==nextChar)
                {
                    if (exponent)
                        error("Bad value");
                    exponent=true;
                    value.append(nextChar);
                    readNext();
                    if ('-'==nextChar)
                        type=elementTypeReal;
                    else if (!isdigit(nextChar) && '+'!=nextChar)
                        error("Bad value");
                }
                if ('.'==nextChar)
                {
                    if (exponent || type==elementTypeReal) //already found decimal
                        error("Bad value");
                    type = elementTypeReal;
                }
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
    using PARENT::checkStartReadNext;
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
    void readValueNotify(const char *name, bool skipAttributes, StringBuffer *retValue, bool *isValueBinary)
    {
        offset_t startOffset = curOffset;
        StringBuffer value;
        if (readValue(value)!=elementTypeNull)
        {
            if ('@'==*name)
            {
                if (!skipAttributes)
                    iEvent->newAttribute(name, value.str());
                return;
            }
            else if ('#'==*name)
            {
                dbgassertex(retValue && isValueBinary);
                if (isValueBinary)
                    *isValueBinary = false;
                if (0 == strncmp(name+1, "value", 5)) // this is a special IPT JSON prop name, representing a 'complex' value
                {
                    if ('\0' == *(name+6)) // #value
                    {
                        if (retValue)
                            retValue->swapWith(value);
                        return;
                    }
                    else if (streq(name+6, "bin")) // #valuebin
                    {
                        if (isValueBinary)
                            *isValueBinary = true;
                        if (retValue)
                            JBASE64_Decode(value.str(), *retValue);
                        return;
                    }
                }
            }
        }
        iEvent->beginNode(name, false, startOffset);
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
                iEvent->beginNode(name, false, curOffset);
                iEvent->beginNodeContent(name);
                readArray(name);
                iEvent->endNode(name, 0, "", false, curOffset);
                break;
            case '{':
                readObject(name);
                break;
            default:
                readValueNotify(name, true, nullptr, nullptr);
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
    void readChild(const char *name, bool skipAttributes, StringBuffer *value, bool *isValueBinary)
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
            readValueNotify(name, skipAttributes, value, isValueBinary);
            break;
        }
    }

    void readObject(const char *name)
    {
        if ('@'==*name)
            name++;
        iEvent->beginNode(name, false, curOffset);
        readNext();
        skipWS();
        bool attributesFinalized=false;
        StringBuffer childValue;  // for #value
        bool isChildValueBinary = false; // for #value
        while ('}' != nextChar)
        {
            StringBuffer tagName;
            readName(tagName);
            //internal convention so we can convert to and from xml
            //values at top of object with names starting with '@' become ptree attributes
            if (*tagName.str()!='@')
                attributesFinalized=true;
            readChild(tagName.str(), attributesFinalized, &childValue.clear(), &isChildValueBinary);
            readNext();
            skipWS();
            if (','==nextChar)
                readNext();
            else if ('}'!=nextChar)
                error("expected ',' or '}'");
            skipWS();
        }
        iEvent->endNode(name, childValue.length(), childValue.str(), isChildValueBinary, curOffset);
    }

    void loadJSON()
    {
        if (!checkStartReadNext())
            return;
        if (checkBOM() && !checkReadNext())
            return;
        if (!checkSkipWS())
            return;
        if (noRoot)
        {
            StringBuffer tagName;
            for (;;)
            {
                switch (nextChar)
                {
                case '\"':  //treat named objects like we're in a noroot object
                    readName(tagName.clear());
                    readChild(tagName.str(), true, nullptr, nullptr);
                    break;
                case '{':  //treat unnamed objects like we're in a noroot array
                    readObject("__object__");
                    break;
                case '[':  //treat unnamed arrays like we're in a noroot array
                    iEvent->beginNode("__array__", false, curOffset);
                    readArray("__item__");
                    iEvent->endNode("__array__", 0, "", false, curOffset);
                    break;
                default:
                    expecting("{[ or \"");
                }
                if (!checkReadNext() || !checkSkipWS())
                    break;
                switch (nextChar)
                {
                case '{': //support file formats with whitespace (usually \n) seperated objects at the root
                case '[':
                    break;
                case ',':
                    readNext();
                    skipWS();
                    break;
                default:
                    expecting(",");
                    break;
                }
            }
        }
        else
        {
            if ('{' == nextChar)
                readObject("__object__");
            else if ('[' == nextChar)
            {
                iEvent->beginNode("__array__", false, curOffset);
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
    using PARENT::checkStartReadNext;
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
    CICopyArrayOf<CStateInfo> stack, freeStateInfo;

    CStateInfo *stateInfo;

    enum ParseStates { headerStart, nameStart, valueStart, itemStart, objAttributes, itemContent, itemEnd } state;
    bool endOfRoot;
    bool preReadItemName;
    bool more;
    StringBuffer tag, value;

    void init()
    {
        state = headerStart;
        stateInfo = NULL;
        endOfRoot = false;
        preReadItemName = false;
        more = true;
    }

    virtual void resetState()
    {
        stack.kill();
        more = true;
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
            stateInfo = &freeStateInfo.popGet();
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
            iEvent->beginNode(stateInfo->wnsTag, false, offset);
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

    inline const char *arrayItemName(const char *defaultName)
    {
        if (stack.ordinality()>1)
            return stateInfo->wnsTag;
        return defaultName;
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
            beginNode(arrayItemName("__object__"), offset, elementTypeObject);
            break;
        case '[':
            state=valueStart;
            readNext();
            beginNode(arrayItemName("__array__"), offset, elementTypeArray, true);
            break;
        default:
            state=valueStart;
            ptElementType type = readValue(value.clear());
            readNext();
            beginNode(arrayItemName("__item__"), offset, type, true);
            stateInfo->tagText.swapWith(value);
            break;
        }
        return true;
    }

    void namedItem()
    {
        if (!preReadItemName)
            readName(tag.clear());
        else
            preReadItemName = false;
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
        if (!noRoot)
            return false;
        if (!checkReadNext() || !checkSkipWS())
            return true;
        switch (nextChar)
        {
        case '{':  //support files where root level objects are separated by whitespace (usually \n)
        case '[':
        case ',':
            break;
        default:
            expecting(",");
        }
        return true;
    }
    void newNamedAttribute()
    {
        skipWS();
        readValue(value.clear());
        readNext();
        stateInfo->childCount++;
        iEvent->newAttribute(tag.str(), value.str());
    }
    bool endNode(offset_t offset, bool notify=true)
    {
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
                iEvent->endNode(stateInfo->wnsTag, stateInfo->tagText.length(), stateInfo->tagText.str(), false, offset);
        }
        freeStateInfo.append(*stateInfo);
        stack.pop();
        stateInfo = (stack.ordinality()) ? &stack.tos() : NULL;
        return true;
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
        if (!more)
            return false;
        checkStartReadNext();
        checkSkipWS();
        switch (state)
        {
            case headerStart:
            {
                if (nextChar!='{' && nextChar!='[') //already positioned at start
                {
                    if (!checkReadNext())
                        return false;
                    if (checkBOM())
                        if (!checkReadNext())
                            return false;
                    if (!checkSkipWS())
                        return false;
                }
                if (noRoot)
                    rootItem();
                else
                {
                    switch (nextChar)
                    {
                    case '{':
                        state=objAttributes;
                        readNext();
                        beginNode("__object__", curOffset, elementTypeObject);
                        break;
                    case '[':
                        state=valueStart;
                        readNext();
                        beginNode("__array__", curOffset, elementTypeArray);
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
                readName(tag.clear());
                if (tag.charAt(0)=='@')
                    newNamedAttribute();
                else
                {
                    preReadItemName = true;
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
        iMaker = createDefaultPTreeMaker(flags, (PTreeReaderOptions)(readFlags|ptr_encodeExtNames));
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
        iMaker = createDefaultPTreeMaker(flags, (PTreeReaderOptions)(readFlags|ptr_encodeExtNames));
        _iMaker.setown(iMaker);
    }
    Owned<IPTreeReader> reader = createJSONBufferReader(json, len, *iMaker, readFlags);
    reader->load();
    return LINK(iMaker->queryRoot());
}


static const char * nextHttpParameterTag(StringBuffer &tag, const char *path)
{
    while (*path=='.')
        path++;
    const char *finger = strchr(path, '.');
    if (finger)
    {
        tag.clear().append(finger - path, path);
        finger++;
    }
    else
        tag.set(path);
    return finger;
}

static void ensureHttpParameter(IPropertyTree *pt, StringBuffer &tag, const char *path, const char *value, const char *fullpath)
{
    if (!tag.length())
        return;

    unsigned idx = 1;
    if (path && isdigit(*path))
    {
        StringBuffer pos;
        path = nextHttpParameterTag(pos, path);
        idx = (unsigned) atoi(pos.str())+1;
    }

    if ('@'==*tag)
    {
        if (path && *path)
            throw MakeStringException(-1, "'@' not allowed in parent node of parameter path: %s", fullpath);
        pt->setProp(tag, value);
        return;
    }
    if (tag.charAt(tag.length()-1)=='$')
    {
        if (path && *path)
            throw MakeStringException(-1, "'$' not allowed in parent node of parameter path: %s", fullpath);
        tag.setLength(tag.length()-1);
        StringArray values;
        values.appendList(value, "\r");
        ForEachItemIn(pos, values)
        {
            const char *itemValue = values.item(pos);
            while (*itemValue=='\n')
                itemValue++;
            pt->addProp(tag, itemValue);
        }
        return;
    }
    unsigned count = pt->getCount(tag);
    while (count++ < idx)
        pt->addPropTree(tag, createPTree(tag));
    StringBuffer xpath(tag);
    xpath.append('[').append(idx).append(']');
    pt = pt->queryPropTree(xpath);

    if (!path || !*path)
    {
        pt->setProp(NULL, value);
        return;
    }

    StringBuffer nextTag;
    path = nextHttpParameterTag(nextTag, path);
    ensureHttpParameter(pt, nextTag, path, value, fullpath);
}

static void ensureHttpParameter(IPropertyTree *pt, const char *path, const char *value)
{
    const char *fullpath = path;
    StringBuffer tag;
    path = nextHttpParameterTag(tag, path);
    ensureHttpParameter(pt, tag, path, value, fullpath);
}

bool checkParseUrlPathNodeValue(const char *s, StringBuffer &name, StringAttr &value)
{
    s = skipWhitespace(s);
    const char *pn = strchr(s, '(');
    if (pn) //strict format param('value') so we can extend later
    {
        const char *vp = pn + 1;
        if (*vp!='\'')
            return false;
        const char *end =strchr(++vp, '\'');
        if (!end || *(end+1)!=')')
            return false;
        if (!validateXMLTag(name.append(pn-s, s).trim()))
            return false;
        value.set(vp, end-vp);
    }
    else
    {
        if (!validateXMLTag(name.append(s).trim()))
            return false;
    }
    return true;
}
IPropertyTree *createPTreeFromHttpPath(const char *nameWithAttrs, IPropertyTree *content, bool nestedRoot, ipt_flags flags)
{
    StringArray nameAttrList;
    nameAttrList.appendList(nameWithAttrs, "/");
    if (!nameAttrList.ordinality())
        return NULL;
    Owned<IPropertyTree> pt = createPTree(nameAttrList.item(0), flags);
    for (aindex_t pos=1; nameAttrList.isItem(pos); pos++)
    {
        StringBuffer name;
        StringAttr value;
        if (!checkParseUrlPathNodeValue(nameAttrList.item(pos), name, value))
            throw MakeStringException(-1, "Invalid URL parameter format %s", nameAttrList.item(pos));
        StringBuffer xpath("@");
        xpath.append(name.str());
        if (!value.get())
            pt->setPropBool(xpath, true);
        else
            pt->setProp(xpath, value);
    }
    IPropertyTree *parent = pt;
    const char *input = pt->queryProp("@input");
    if (input)
    {
        StringArray inputNodes;
        inputNodes.appendList(input, ".");
        ForEachItemIn(in, inputNodes)
        {
            const char *tag = inputNodes.item(in);
            if (!validateXMLTag(tag))
                throw MakeStringException(-1, "Invalid REST query input specifier %s", input);
            parent = parent->addPropTree(tag, createPTree(tag, flags));
        }
    }

    if (streq("__array__", content->queryName()))
    {
        Owned<IAttributeIterator> aiter = content->getAttributes();
        ForEach (*aiter)
            parent->addProp(aiter->queryName(), aiter->queryValue());
        Owned<IPropertyTreeIterator> iter = content->getElements("__item__");
        ForEach (*iter)
        {
            IPropertyTree &e = iter->query();
            e.renameProp("/", "Row");
            parent->addPropTree("Row", LINK(&e));
        }
    }
    else
        mergePTree(parent, content);

    if (nestedRoot)
    {
        Owned<IPropertyTree> root = createPTree(flags);
        root->setPropTree(nameAttrList.item(0), pt.getClear());
        return root.getClear();
    }

    return pt.getClear();
}

//URL node nameWithAttrs is of the form: "TagName/attr1('abc')/attr2/attr3('xyz')"
IPropertyTree *createPTreeFromHttpParameters(const char *nameWithAttrs, IProperties *parameters, bool skipLeadingDotParameters, bool nestedRoot, ipt_flags flags)
{
    Owned<IPropertyTree> content = createPTree("content", flags);
    Owned<IPropertyIterator> iter = parameters->getIterator();
    ForEach(*iter)
    {
        StringBuffer key(iter->getPropKey());
        if (!key.length() || key.charAt(key.length()-1)=='!')
            continue;
        if (skipLeadingDotParameters && key.charAt(0)=='.')
            continue;
        const char *value = iter->queryPropValue();
        if (!value || !*value)
            continue;
        ensureHttpParameter(content, key, value);
    }

    return createPTreeFromHttpPath(nameWithAttrs, content.getClear(), nestedRoot, flags);
}


IPropertyTree *createPTreeFromJSONFile(const char *filename, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IFile> in = createIFile(filename);
    if (!in->exists())
        return nullptr;

    StringBuffer contents;
    try
    {
        contents.loadFile(in);
    }
    catch (IException * e)
    {
        EXCLOG(e);
        e->Release();
        return nullptr;
    }

    return createPTreeFromJSONString(contents.length(), contents.str(), flags, readFlags, iMaker);
}

//---------------------------------------------------------------------------------------------------------------------

static constexpr const char * currentVersion = "1.0";

//---------------------------------------------------------------------------------------------------------------------
/*
 * Use source to overwrite any changes in target
 *   Attributes are replaced
 *   Singleton elements are replaced.
 *   Entire arrays of scalar elements are replaced.
 *   Entire arrays of elements with no name attribute are replaced.
 *   Elements with a name attribute are matched by name.  If there is a match it is merged.  If there is no match it is added.
*/

static bool checkInSequence(IPropertyTree & child, StringAttr &seqname, bool &first, bool &endprior)
{
    first = false;
    endprior = false;
    if (seqname.length() && streq(seqname, child.queryName()))
        return true;
    endprior = !seqname.isEmpty();
    if (child.isArray(nullptr))
    {
        first=true;
        seqname.set(child.queryName());
        return true;
    }
    seqname.clear();
    return false;
}

inline bool isScalarItem(IPropertyTree &child)
{
    if (child.hasChildren())
        return false;
    return child.getAttributeCount()==0;
}

static IPropertyTree *ensureMergeConfigTarget(IPropertyTree &target, const char *tag, const char *nameAttribute, const char *name, bool sequence)
{
    StringBuffer tempPath;
    const char * path = (sequence) ? nullptr : tag;
    if (name && nameAttribute && *nameAttribute)
    {
        tempPath.append(tag).append("[").append(nameAttribute).append("=\'").append(name).append("']");
        path = tempPath;
    }

    IPropertyTree * match = (path) ? target.queryPropTree(path) : nullptr;
    if (!match)
    {
        if (sequence)
            match = target.addPropTreeArrayItem(tag, createPTree(tag));
        else
            match = target.addPropTree(tag);
    }
    return match;
}

void mergeConfiguration(IPropertyTree & target, const IPropertyTree & source, const char *altNameAttribute, bool overwriteAttr)
{
    Owned<IAttributeIterator> aiter = source.getAttributes();
    ForEach(*aiter)
    {
        if (overwriteAttr || !target.hasProp(aiter->queryName()))
            target.addProp(aiter->queryName(), aiter->queryValue());
    }

    StringAttr seqname;
    Owned<IPropertyTreeIterator> iter = source.getElements("*");
    ForEach(*iter)
    {
        IPropertyTree & child = iter->query();
        const char * tag = child.queryName();
        const char * name = child.queryProp("@name");
        bool altname = false;

        //Legacy support for old component configuration files that have repeated elements with no name tag but another unique id
        if (!name && altNameAttribute && *altNameAttribute)
        {
            name = child.queryProp(altNameAttribute);
            altname = name!=nullptr;
        }

        bool first = false;
        bool endprior = false;
        bool sequence = checkInSequence(child, seqname, first, endprior);
        if (first && (!name || isScalarItem(child))) //arrays of unnamed objects or scalars are replaced
            target.removeProp(tag);

        IPropertyTree * match = ensureMergeConfigTarget(target, tag, altname ? altNameAttribute : "@name", name, sequence);
        mergeConfiguration(*match, child, altNameAttribute, overwriteAttr);
    }

    const char * sourceValue = source.queryProp("");
    target.setProp("", sourceValue);
}

/*
 * Load a json/yaml configuration file.
 * If there is an extends tag in the root of the file then this file is applied as a delta to the base file
 * the configuration is the contents of the tag within the file that matches the component tag.
*/
static IPropertyTree * loadConfiguration(const char * filename, const char * componentTag, bool required, const char *altNameAttribute)
{
    if (!checkFileExists(filename))
        throw makeStringExceptionV(99, "Configuration file %s not found", filename);

    const char * ext = pathExtension(filename);
    Owned<IPropertyTree> configTree;
    if (!ext || strieq(ext, ".yaml"))
    {
        try
        {
            configTree.setown(createPTreeFromYAMLFile(filename, 0, ptr_ignoreWhiteSpace, nullptr));
        }
        catch (IException *E)
        {
            StringBuffer msg;
            E->errorMessage(msg);
            ::Release(E);
            throw makeStringExceptionV(99, "Error loading configuration file %s (invalid yaml): %s", filename, msg.str());
        }
    }
    else
        throw makeStringExceptionV(99, "Unrecognised file extension %s", ext);

    if (!configTree)
        throw makeStringExceptionV(99, "Error loading configuration file %s", filename);

    IPropertyTree * config = configTree->queryPropTree(componentTag);
    if (!config)
    {
        if (required)
            throw makeStringExceptionV(99, "Section %s is missing from file %s", componentTag, filename);
        return nullptr;
    }
    const char * base = configTree->queryProp("@extends");
    if (!base)
        return LINK(config);

    StringBuffer baseFilename;
    splitFilename(filename, &baseFilename, &baseFilename, nullptr, nullptr, false);
    addNonEmptyPathSepChar(baseFilename);
    baseFilename.append(base);

    Owned<IPropertyTree> baseTree = loadConfiguration(baseFilename, componentTag, required, altNameAttribute);
    mergeConfiguration(*baseTree, *config, altNameAttribute);
    return LINK(baseTree);
}

static constexpr const char * envPrefix = "HPCC_CONFIG_";
static void applyEnvironmentConfig(IPropertyTree & target, const char * cptPrefix, const char * value)
{
    const char * name = value;
    if (!startsWith(name, envPrefix))
        return;

    name += strlen(envPrefix);
    if (cptPrefix)
    {
        if (!startsWith(name, cptPrefix))
            return;
        name += strlen(cptPrefix);
        if (*name++ != '_')
            return;
    }

    StringBuffer propName;
    if (startsWith(name, "PROP_"))
    {
        propName.append("@");
        name += 5;
    }
    const char * eq = strchr(value, '=');
    if (eq)
    {
        propName.append(eq - name, name);
        target.setProp(propName, eq + 1);
    }
    else
    {
        propName.append(name);
        target.setProp(propName, nullptr);
    }
}

void applyProperties(IPropertyTree * target, const IProperties * source)
{
    Owned<IPropertyIterator> iter = source->getIterator();
    ForEach(*iter)
    {
        const char * name = iter->getPropKey();
        const char * value = iter->queryPropValue();
        target->setProp(name, value);
    }
}

void applyProperty(IPropertyTree * target, const char * source)
{
    const char * equals = strchr(source, '=');
    if (equals)
    {
        StringBuffer prop(equals - source, source);
        target->setProp(prop, equals + 1);
    }
    else
        target->setPropBool(source, true);
}

IPropertyTree * createPTreeFromYAML(const char * yaml)
{
    if (*yaml == '{')
        return createPTreeFromJSONString(yaml, 0, ptr_ignoreWhiteSpace, nullptr);

    return createPTreeFromYAMLString(yaml, 0, ptr_ignoreWhiteSpace, nullptr);
}

static const char * extractOption(const char * option, const char * cur)
{
    if (startsWith(cur, option))
    {
        cur += strlen(option);
        if (*cur == '=')
            return cur + 1;
        if (*cur)
            return nullptr;
        return "1";
    }
    return nullptr;
}

static void applyCommandLineOption(IPropertyTree * config, const char * option, const char * value)
{
    //Ignore -- with no following option.
    if (isEmptyString(option))
        return;

    const char *tail;
    while ((tail = strchr(option, '.')) != nullptr)
    {
        StringAttr elemName(option, tail-option);
        if (!config->hasProp(elemName))
            config = config->addPropTree(elemName);
        else
        {
            config = config->queryPropTree(elemName);
            if (!config)
                throw makeStringExceptionV(99, "Cannot override scalar configuration element %s with structure", elemName.get());
        }
        option = tail+1;
    }

    if (!validateXMLTag(option))
        throw makeStringExceptionV(99, "Invalid option name '%s'", option);

    StringBuffer path;
    path.append('@').append(option);
    config->setProp(path, value);
}

static void applyCommandLineOption(IPropertyTree * config, const char * option, std::initializer_list<const std::string> ignoreOptions)
{
    const char * eq = strchr(option, '=');
    StringBuffer name;
    const char *val = nullptr;
    if (eq)
    {
        name.append(eq - option, option);
        option = name;
        val = eq + 1;
    }
    else
    {
        unsigned len = strlen(option);
        if (len == 0)
            return;

        //support --option+ as --option=1 and --option- as --option=0 for eclcc compatibility
        char last = option[len-1];
        if ((last == '+') || (last == '-'))
        {
            name.append(len-1, option);
            option = name;
            val = (last == '+') ? "1" : "0";
        }
        else
            val = "1";
    }
    if (stdContains(ignoreOptions, option))
        return;
    applyCommandLineOption(config, option, val);
}

static CriticalSection configCS;
static Owned<IPropertyTree> componentConfiguration;
static Owned<IPropertyTree> globalConfiguration;
static Owned<IPropertyTree> nullConfiguration;
static StringBuffer componentName;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    componentConfiguration.clear();
    globalConfiguration.clear();
}

IPropertyTree * getComponentConfig()
{
    CriticalBlock b(configCS);
    if (!componentConfiguration)
    {
        if (!nullConfiguration)
        {
            PrintStackReport(); // deliberately emitting here, so only reported once per process
            nullConfiguration.setown(createPTree());
        }
        IERRLOG(99, "getComponentConfig() called - configuration file has not yet been processed");
        return nullConfiguration.getLink();
    }
    return componentConfiguration.getLink();
}

IPropertyTree * getGlobalConfig()
{
    CriticalBlock b(configCS);
    if (!globalConfiguration)
    {
        if (!nullConfiguration)
        {
            PrintStackReport(); // deliberately emitting here, so only reported once per process
            nullConfiguration.setown(createPTree());
        }
        IERRLOG(99, "getGlobalConfig() called - configuration file has not yet been processed");
        return nullConfiguration.getLink();
    }
    return globalConfiguration.getLink();
}

void initNullConfiguration()
{
    CriticalBlock b(configCS);
    if (componentConfiguration || globalConfiguration)
        throw makeStringException(99, "Configuration has already been initialised");
    if (!nullConfiguration)
        nullConfiguration.setown(createPTree());
    componentConfiguration.set(nullConfiguration);
    globalConfiguration.set(nullConfiguration);
}

Owned<IPropertyTree> getComponentConfigSP()
{
    return getComponentConfig();
}

Owned<IPropertyTree> getGlobalConfigSP()
{
    return getGlobalConfig();
}

template <typename T>
static T getConfigValue(const char *xpath, T defaultValue, T (IPropertyTree::*getPropFunc)(const char *, T) const)
{
    return (getComponentConfigSP()->*getPropFunc)(xpath, (getGlobalConfigSP()->*getPropFunc)(xpath, defaultValue));
}

bool getConfigBool(const char *xpath, bool defaultValue)
{
    return getConfigValue(xpath, defaultValue, &IPropertyTree::getPropBool);
}

__int64 getConfigInt64(const char *xpath, __int64 defaultValue)
{
    return getConfigValue(xpath, defaultValue, &IPropertyTree::getPropInt64);
}

double getConfigReal(const char *xpath, double defaultValue)
{
    return getConfigValue(xpath, defaultValue, &IPropertyTree::getPropReal);
}

bool getConfigString(const char *xpath, StringBuffer &result)
{
    if (getComponentConfigSP()->getProp(xpath, result))
        return true;
    else
        return getGlobalConfigSP()->getProp(xpath, result);
}

const char * queryComponentName()
{
    //componentName is thread safe, since initialised when config is first loaded, and not modified afterwards
    return componentName.str();
}

jlib_decl IPropertyTree * loadArgsIntoConfiguration(IPropertyTree *config, const char * * argv, std::initializer_list<const std::string> ignoreOptions)
{
    for (const char * * pArg = argv; *pArg; pArg++)
    {
        const char * cur = *pArg;
        if (startsWith(cur, "--"))
            applyCommandLineOption(config, cur + 2, ignoreOptions);
    }
    return config;
}

#ifdef _DEBUG
static void holdLoop()
{
    DBGLOG("Component paused for debugging purposes, attach and set held=false to release");
    bool held = true;
    while (held)
        Sleep(5);
}
#endif

static std::tuple<std::string, IPropertyTree *, IPropertyTree *> doLoadConfiguration(IPropertyTree *componentDefault, IPropertyTree *globalDefault, const char * * argv, const char * componentTag, const char * envPrefix, const char * legacyFilename, IPropertyTree * (mapper)(IPropertyTree *), const char *altNameAttribute);

class CConfigUpdater : public CInterface
{
    StringAttr absoluteConfigFilename;
    StringAttr configFilename;
    Linked<IPropertyTree> componentDefault, globalDefault;
    StringArray args;
    StringAttr componentTag, envPrefix, legacyFilename;
    IPropertyTree * (*mapper)(IPropertyTree *);
    StringAttr altNameAttribute;
    Owned<IFileEventWatcher> fileWatcher; // null if updates to the config file are not allowed
    CriticalSection notifyFuncCS;
    unsigned notifyFuncId = 0;
    std::unordered_map<unsigned, ConfigUpdateFunc> notifyConfigUpdates;
    std::unordered_map<unsigned, ConfigModifyFunc> modifyConfigUpdates;

public:
    CConfigUpdater()
    {
    }
    bool isInitialized() const
    {
        return args.ordinality(); // NB: null terminated, so always >=1 if initialized
    }
    void init(IPropertyTree *_componentDefault, IPropertyTree *_globalDefault, const char * * argv, const char * _componentTag, const char * _envPrefix, const char *_legacyFilename, IPropertyTree * (_mapper)(IPropertyTree *), const char *_altNameAttribute)
    {
        dbgassertex(!isInitialized());
        componentDefault.set(_componentDefault);
        globalDefault.set(_globalDefault);
        componentTag.set(_componentTag);
        envPrefix.set(_envPrefix);
        legacyFilename.set(_legacyFilename);
        mapper = _mapper;
        altNameAttribute.set(_altNameAttribute);
        while (const char *arg = *argv++)
            args.append(arg);
        args.append(nullptr);

        refreshConfiguration(true, true);
    }
    bool startMonitoring()
    {
#if !defined(__linux__) // file moinitoring only supported in Linux (because createFileEventWatcher only implemented in Linux at the moment)
        return false;
#endif
        if (absoluteConfigFilename.isEmpty() || (nullptr != fileWatcher.get()))
            return false;

        auto updateFunc = [&](const char *filename, FileWatchEvents events)
        {
            bool changed = containsFileWatchEvents(events, FileWatchEvents::closedWrite) && streq(filename, configFilename);

            // NB: in k8s, it's a little strange, the config file is in a linked dir, a new dir is created and swapped in.
            if (isContainerized())
                changed = changed | containsFileWatchEvents(events, FileWatchEvents::movedTo) && streq(filename, "..data");

            if (changed)
                refreshConfiguration(false, false);
        };

        try
        {
            fileWatcher.setown(createFileEventWatcher(updateFunc));

            // watch the path, not the filename, because the filename might not be seen if directories are moved, softlinks are changed..
            StringBuffer path, filename;
            splitFilename(absoluteConfigFilename, nullptr, &path, &filename, &filename);
            configFilename.set(filename);
            fileWatcher->add(path, FileWatchEvents::anyChange);
            fileWatcher->start();
        }
        catch (IException * e)
        {
            OERRLOG(e, "Failed to start file watcher");
            e->Release();
        }
        return true;
    }

    void refreshConfiguration(IPropertyTree * newComponentConfiguration, IPropertyTree * newGlobalConfiguration)
    {
        CriticalBlock b(notifyFuncCS);
        //Ensure all modifications to the config take place before the config is updated, and the monitoring/caching functions are called.
        executeModifyCallbacks(newComponentConfiguration, newGlobalConfiguration);

        // NB: block calls to get*Config*() from other threads until callbacks notified and new swapped in, destroy old config outside the critical section
        Owned<IPropertyTree> oldComponentConfiguration;
        Owned<IPropertyTree> oldGlobalConfiguration;
        {
            CriticalBlock b(configCS);
            oldComponentConfiguration.setown(componentConfiguration.getClear());
            oldGlobalConfiguration.setown(globalConfiguration.getClear());

            /* swapin before callbacks called, but before releasing crit.
            * That way the CB can see the old/diffs and act on them, but any
            * code calling e.g. getComponentConfig() will see new.
            */
            componentConfiguration.set(newComponentConfiguration);
            globalConfiguration.set(newGlobalConfiguration);

            /* NB: we are still holding 'configCS' at this point, blocking all other thread access.
            However code in callbacks may call e.g. getComponentConfig() and re-enter the crit */
            executeNotifyCallbacks(oldComponentConfiguration, oldGlobalConfiguration);
        }
    }

    void refreshConfiguration(bool firstTime, bool avoidClone)
    {
        if (firstTime || fileWatcher)
        {
            auto result = doLoadConfiguration(componentDefault, globalDefault, args.getArray(), componentTag, envPrefix, legacyFilename, mapper, altNameAttribute);
            IPropertyTree * newComponentConfiguration = std::get<1>(result);
            IPropertyTree * newGlobalConfiguration = std::get<2>(result);
            refreshConfiguration(newComponentConfiguration, newGlobalConfiguration);

            if (firstTime)
            {
                absoluteConfigFilename.set(std::get<0>(result).c_str());
                newGlobalConfiguration->getProp("@name", componentName);
            }
        }
        else if (avoidClone)
        {
            //This is added during the initialiation phase, so no danger of other threads accesing the global information
            //while it is updated.  Thor currently relies on the pointer not changing.
            Owned<IPropertyTree> newComponentConfiguration = getComponentConfigSP();
            Owned<IPropertyTree> newGlobalConfiguration = getGlobalConfigSP();
            refreshConfiguration(newComponentConfiguration, newGlobalConfiguration);
        }
        else
        {
            // File monitor is disabled - no updates to the configuration files are supported.
            //So clone the existing configuration and use that to refresh the config - update fucntions may perform differently.
            Owned<IPropertyTree> newComponentConfiguration = createPTreeFromIPT(getComponentConfigSP());
            Owned<IPropertyTree> newGlobalConfiguration = createPTreeFromIPT(getGlobalConfigSP());
            refreshConfiguration(newComponentConfiguration, newGlobalConfiguration);
        }
    }

    void executeNotifyCallbacks()
    {
        CriticalBlock notifyBlock(notifyFuncCS);
        CriticalBlock configBlock(configCS);
        executeNotifyCallbacks(componentConfiguration, globalConfiguration);
    }

    void executeNotifyCallbacks(IPropertyTree *oldComponentConfiguration, IPropertyTree *oldGlobalConfiguration)
    {
        for (const auto &item: notifyConfigUpdates)
        {
            try
            {
                item.second(oldComponentConfiguration, oldGlobalConfiguration);
            }
            catch (IException *e)
            {
                EXCLOG(e, "CConfigUpdater callback");
                e->Release();
            }
        }
    }

    void executeModifyCallbacks(IPropertyTree * newComponentConfiguration, IPropertyTree * newGlobalConfiguration)
    {
        for (auto & modifyFunc : modifyConfigUpdates)
        {
            try
            {
                modifyFunc.second(newComponentConfiguration, newGlobalConfiguration);
            }
            catch (IException *e)
            {
                EXCLOG(e, "CConfigUpdater callback");
                e->Release();
            }
        }
    }

    unsigned addNotifyFunc(ConfigUpdateFunc notifyFunc, bool callWhenInstalled)
    {
        CriticalBlock b(notifyFuncCS);
        notifyFuncId++;
        notifyConfigUpdates[notifyFuncId] = notifyFunc;
        if (callWhenInstalled)
        {
            if (isInitialized())
                notifyFunc(getComponentConfigSP(), getGlobalConfigSP());
        }
        return notifyFuncId;
    }
    unsigned addModifyFunc(ConfigModifyFunc notifyFunc, bool threadSafe)
    {
        CriticalBlock b(notifyFuncCS);
        notifyFuncId++;
        modifyConfigUpdates[notifyFuncId] = notifyFunc;
        if (isInitialized())
        {
            //Force all cached values to be recalculated, do not reload the config
            //This is only legal if no other threads are accessing the config yet - otherwise the reading thread
            //could crash when the global configuration is updated.
            refreshConfiguration(false, threadSafe);
            DBGLOG("Modify functions should be registered before the configuration is loaded");
        }
        return notifyFuncId;
    }
    bool removeNotifyFunc(unsigned funcId)
    {
        CriticalBlock b(notifyFuncCS);
        if (modifyConfigUpdates.erase(funcId) != 0)
            return true;

        auto it = notifyConfigUpdates.find(funcId);
        if (it == notifyConfigUpdates.end())
            return false;

        ConfigUpdateFunc notifyFunc = it->second;
        notifyConfigUpdates.erase(it);
        return true;
    }
};

static CConfigUpdater *configFileUpdater = nullptr;
MODULE_INIT(INIT_PRIORITY_JPTREE)
{
    configFileUpdater = new CConfigUpdater();
    return true;
}

MODULE_EXIT()
{
    ::Release(configFileUpdater);
    configFileUpdater = nullptr;
}

unsigned installConfigUpdateHook(ConfigUpdateFunc notifyFunc, bool callWhenInstalled)
{
    if (!configFileUpdater) // NB: installConfigUpdateHook should always be called after configFileUpdater is initialized
        return 0;
    return configFileUpdater->addNotifyFunc(notifyFunc, callWhenInstalled);
}

jlib_decl unsigned installConfigUpdateHook(ConfigModifyFunc notifyFunc, bool threadSafe)  // This function must be called before the configuration is loaded.
{
    if (!configFileUpdater) // NB: installConfigUpdateHook should always be called after configFileUpdater is initialized
        return 0;
    return configFileUpdater->addModifyFunc(notifyFunc, threadSafe);
}

void removeConfigUpdateHook(unsigned notifyFuncId)
{
    if (0 == notifyFuncId)
        return;
    if (!configFileUpdater) // NB: removeConfigUpdateHook should always be called after configFileUpdater is initialized
        return;
    if (!configFileUpdater->removeNotifyFunc(notifyFuncId))
        WARNLOG("removeConfigUpdateHook(): notifyFuncId %u not installed", notifyFuncId);
}

void refreshConfiguration()
{
    if (!configFileUpdater) // NB: refreshConfiguration() should always be called after configFileUpdater is initialized
        return;
    configFileUpdater->refreshConfiguration(false, false);
}

void CConfigUpdateHook::clear()
{
    unsigned id = configCBId.exchange((unsigned)-1);
    if ((unsigned)-1 != id)
        removeConfigUpdateHook(id);
}

void CConfigUpdateHook::installOnce(ConfigUpdateFunc callbackFunc, bool callWhenInstalled)
{
    unsigned id = configCBId.load(std::memory_order_acquire);
    if ((unsigned)-1 == id) // avoid CS in common case
    {
        CriticalBlock b(crit);
        // check again now in CS
        id = configCBId.load(std::memory_order_acquire);
        if ((unsigned)-1 == id)
        {
            id = installConfigUpdateHook(callbackFunc, callWhenInstalled);
            configCBId.store(id, std::memory_order_release);
        }
    }
}


void CConfigUpdateHook::installModifierOnce(ConfigModifyFunc callbackFunc, bool threadSafe)
{
    unsigned id = configCBId.load(std::memory_order_acquire);
    if ((unsigned)-1 == id) // avoid CS in common case
    {
        CriticalBlock b(crit);
        // check again now in CS
        id = configCBId.load(std::memory_order_acquire);
        if ((unsigned)-1 == id)
        {
            id = installConfigUpdateHook(callbackFunc, threadSafe);
            configCBId.store(id, std::memory_order_release);
        }
    }
}

static std::tuple<std::string, IPropertyTree *, IPropertyTree *> doLoadConfiguration(IPropertyTree *componentDefault, IPropertyTree *globalDefault, const char * * argv, const char * componentTag, const char * envPrefix, const char * legacyFilename, IPropertyTree * (mapper)(IPropertyTree *), const char *altNameAttribute)
{
    Owned<IPropertyTree> newComponentConfig = createPTreeFromIPT(componentDefault);
    Linked<IPropertyTree> newGlobalConfig = globalDefault ? createPTreeFromIPT(globalDefault) : nullptr;
    StringBuffer absConfigFilename;
    const char * optConfig = nullptr;
    bool outputConfig = false;
#ifdef _DEBUG
    bool held = false;
#endif
    for (const char * * pArg = argv; *pArg; pArg++)
    {
        const char * cur = *pArg;
        const char * matchConfig = extractOption("--config", cur);
        if (matchConfig)
            optConfig = matchConfig;
        else if (strsame(cur, "--help"))
        {
#if 0
            //Better not to include this until it has been implemented, since it breaks eclcc
            //MORE: displayHelp(config);
            printf("%s <options>\n", argv[0]);
            exit(0);
#endif
        }
        else if (strsame(cur, "--init"))
        {
            StringBuffer yamlText;
            toYAML(componentDefault, yamlText, 0, YAML_SortTags);
            printf("%s\n", yamlText.str());
            exit(0);
        }
        else if (strsame(cur, "--outputconfig"))
        {
            outputConfig = true;
        }
        else
        {
            matchConfig = extractOption("--componentTag", cur);
            if (matchConfig)
                componentTag = matchConfig;
#ifdef _DEBUG
            else
            {
                const char *matchHold = extractOption("--hold", cur);
                if (matchHold)
                {
                    if (strToBool(matchHold))
                    {
                        held = true;
                        holdLoop();
                    }
                }
            }
#endif
        }
    }

    const char *config = optConfig ? optConfig : legacyFilename;
    if (!isEmptyString(config) && !isAbsolutePath(config))
    {
        appendCurrentDirectory(absConfigFilename, false);
        addNonEmptyPathSepChar(absConfigFilename);
    }
    absConfigFilename.append(config);

    Owned<IPropertyTree> configGlobal;
    Owned<IPropertyTree> delta;
    if (optConfig)
    {
        if (streq(optConfig, "1"))
            throw makeStringExceptionV(99, "Name of configuration file omitted (use --config=<filename>)");

        //--config= with no filename can be used to ignore the legacy configuration file
        if (!isEmptyString(optConfig))
        {
            delta.setown(loadConfiguration(absConfigFilename, componentTag, true, altNameAttribute));
            configGlobal.setown(loadConfiguration(absConfigFilename, "global", false, altNameAttribute));
        }
    }
    else
    {
        if (legacyFilename && checkFileExists(legacyFilename))
        {
            delta.setown(createPTreeFromXMLFile(legacyFilename, ipt_caseInsensitive));
            configGlobal.set(delta->queryPropTree("global"));
        }

        if (delta && mapper)
            delta.setown(mapper(delta));
    }

    if (configGlobal)
    {
        if (newGlobalConfig)
            mergeConfiguration(*newGlobalConfig, *configGlobal);
        else
            newGlobalConfig.setown(configGlobal.getClear());
    }

    if (delta)
        mergeConfiguration(*newComponentConfig, *delta, altNameAttribute);

    const char * * environment = const_cast<const char * *>(getSystemEnv());
    for (const char * * cur = environment; *cur; cur++)
    {
        applyEnvironmentConfig(*newComponentConfig, envPrefix, *cur);
    }

    if (outputConfig)
    {
        loadArgsIntoConfiguration(newComponentConfig, argv, { "config", "outputconfig" });

        Owned<IPropertyTree> recreated = createPTree();
        recreated->setProp("@version", currentVersion);
        recreated->addPropTree(componentTag, LINK(newComponentConfig));
        if (newGlobalConfig)
            recreated->addPropTree("global", newGlobalConfig.getLink());
        StringBuffer yamlText;
        toYAML(recreated, yamlText, 0, YAML_SortTags);
        printf("%s\n", yamlText.str());
        exit(0);
    }
    else
        loadArgsIntoConfiguration(newComponentConfig, argv);

    //For legacy (and other weird cases) ensure there is a global section
    if (!newGlobalConfig)
        newGlobalConfig.setown(createPTree("global"));

#ifdef _DEBUG
    // NB: don't re-hold, if CLI --hold already held.
    if (!held && newComponentConfig->getPropBool("@hold"))
    {
        holdLoop();
        held = true;
    }
#endif

    unsigned ptreeMappingThreshold = newGlobalConfig->getPropInt("@ptreeMappingThreshold", defaultSiblingMapThreshold);
    setPTreeMappingThreshold(ptreeMappingThreshold);

    return std::make_tuple(std::string(absConfigFilename.str()), newComponentConfig.getClear(), newGlobalConfig.getClear());
}

IPropertyTree * loadConfiguration(IPropertyTree *componentDefault, IPropertyTree *globalDefault, const char * * argv, const char * componentTag, const char * envPrefix, const char * legacyFilename, IPropertyTree * (mapper)(IPropertyTree *), const char *altNameAttribute, bool monitor)
{
    assertex(configFileUpdater); // NB: loadConfiguration should always be called after configFileUpdater is initialized
    if (configFileUpdater->isInitialized())
        throw makeStringExceptionV(99, "Configuration for component %s has already been initialised", componentTag);

    /* In k8s, pods auto-restart by default on monitored ConfigMap settings/areas
     * ConfigMap settings/areas deliberately not monitored will rely on this config updater mechanism,
     * and if necessary, installed hooks that are called on an update, to perform updates of cached state.
     * See installConfigUpdateHook()
     *
     * NB: most uses of config do not rely on being notified to update state, i.e. most query the config
     * on-demand, which means this mechanism is sufficient to cover most cases.
     *
     * In bare-metal the monitoring mechanism will similarly spot any legacy config file changes, e.g.
     * that would be refreshed during a 'service hpcc-init setup'.
     * Some bare-metal code relies on directly interrogating the environment, for those situations, there is
     * also a default triggering mechanism (see executeConfigUpdaterCallbacks in daclient.cpp) that will cause any
     * installed config hooks to be called when an environment change is detected e.g when pushed to Dali)
     */

    configFileUpdater->init(componentDefault, globalDefault, argv, componentTag, envPrefix, legacyFilename, mapper, altNameAttribute);
    if (monitor)
        configFileUpdater->startMonitoring();

    initTraceManager(componentTag, componentConfiguration.get(), globalConfiguration.get());
    return componentConfiguration.getLink();
}

IPropertyTree * loadConfiguration(const char * defaultYaml, const char * * argv, const char * componentTag, const char * envPrefix, const char * legacyFilename, IPropertyTree * (mapper)(IPropertyTree *), const char *altNameAttribute, bool monitor)
{
    if (componentConfiguration)
        throw makeStringExceptionV(99, "Configuration for component %s has already been initialised", componentTag);

    Owned<IPropertyTree> componentDefault;
    Owned<IPropertyTree> defaultGlobalConfig;
    if (defaultYaml)
    {
        Owned<IPropertyTree> defaultConfig = createPTreeFromYAML(defaultYaml);
        componentDefault.set(defaultConfig->queryPropTree(componentTag));
        if (!componentDefault)
            throw makeStringExceptionV(99, "Default configuration does not contain the tag %s", componentTag);
        defaultGlobalConfig.set(defaultConfig->queryPropTree("global"));
    }
    else
        componentDefault.setown(createPTree(componentTag));

    return loadConfiguration(componentDefault, defaultGlobalConfig, argv, componentTag, envPrefix, legacyFilename, mapper, altNameAttribute, monitor);
}

void replaceComponentConfig(IPropertyTree *newComponentConfig, IPropertyTree *newGlobalConfig)
{
    assertex (configFileUpdater); // NB: replaceComponentConfig should always be called after configFileUpdater is initialized
    configFileUpdater->refreshConfiguration(newComponentConfig, newGlobalConfig);
}


class CYAMLBufferReader : public CInterfaceOf<IPTreeReader>
{
protected:
    Linked<IPTreeNotifyEvent> iEvent;
    yaml_parser_t parser;
    PTreeReaderOptions readerOptions = ptr_none;
    bool noRoot = false;

public:
    CYAMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &_iEvent, PTreeReaderOptions _readerOptions) :
        iEvent(&_iEvent), readerOptions(_readerOptions)
    {
        if (!yaml_parser_initialize(&parser))
            throw makeStringException(99, "Filed to initialize libyaml parser");
        yaml_parser_set_input_string(&parser, (const unsigned char *)buf, bufLength);
        noRoot = 0 != ((unsigned)readerOptions & (unsigned)ptr_noRoot);
    }
    ~CYAMLBufferReader()
    {
        yaml_parser_delete(&parser);
    }

    yaml_event_type_t nextEvent(yaml_event_t &event, yaml_event_type_t final=YAML_NO_EVENT, yaml_event_type_t expected=YAML_NO_EVENT, const char *error="")
    {
        if (!yaml_parser_parse(&parser, &event))
            throw makeStringExceptionV(99, "libyaml parser error %s", parser.problem);
        if (event.type!=final && expected!=YAML_NO_EVENT && event.type!=expected)
            throw makeStringExceptionV(99, "libyaml parser %s", error);
        return event.type;
    }

    virtual void loadSequence(const char *tagname)
    {
        if (!tagname)
            throw makeStringException(99, "libyaml parser expected sequence name");

        yaml_event_t event;
        yaml_event_type_t eventType = YAML_NO_EVENT;

        while (eventType!=YAML_SEQUENCE_END_EVENT)
        {
            eventType = nextEvent(event);

            switch (eventType)
            {
            case YAML_MAPPING_START_EVENT: //child map
                loadMap(tagname, true);
                break;
            case YAML_SEQUENCE_START_EVENT:
                //todo
                break;
            case YAML_SCALAR_EVENT:
                iEvent->beginNode(tagname, true, parser.offset);
                iEvent->endNode(tagname, event.data.scalar.length, (const void *)event.data.scalar.value, false, parser.offset);
                break;
            case YAML_ALIAS_EVENT: //reference to an anchor, ignore for now
                iEvent->beginNode(tagname, true, parser.offset);
                iEvent->endNode(tagname, 0, nullptr, false, parser.offset);
                break;
            case YAML_SEQUENCE_END_EVENT: //done
                break;
            case YAML_NO_EVENT:
            case YAML_MAPPING_END_EVENT:
            case YAML_STREAM_START_EVENT:
            case YAML_STREAM_END_EVENT:
            case YAML_DOCUMENT_START_EVENT:
            case YAML_DOCUMENT_END_EVENT:
            default:
                //shouldn't be here
                break;
            }

            yaml_event_delete(&event);
        }
    }
    virtual void loadMap(const char *tagname, bool sequence)
    {
        bool binaryContent = false;
        StringBuffer content;
        if (tagname)
            iEvent->beginNode(tagname, sequence, parser.offset);

        yaml_event_t event;
        yaml_event_type_t eventType = YAML_NO_EVENT;

        while (eventType!=YAML_MAPPING_END_EVENT)
        {
            eventType = nextEvent(event, YAML_MAPPING_END_EVENT, YAML_SCALAR_EVENT, "expected map to start with scalar name");
            if (eventType==YAML_MAPPING_END_EVENT)
            {
                yaml_event_delete(&event);
                continue;
            }
            StringBuffer attname('@');
            attname.append(event.data.scalar.length, (const char *)event.data.scalar.value);
            const char *elname = attname.str()+1;
            yaml_event_delete(&event);

            eventType = nextEvent(event);

            switch (eventType)
            {
            case YAML_MAPPING_START_EVENT: //child map
                loadMap(elname, false);
                break;
            case YAML_SEQUENCE_START_EVENT:
                loadSequence(elname);
                break;
            case YAML_SCALAR_EVENT:
            {
                //!el or !element will be our local tag (custom schema type) for an element
                //ptree toYAML should set this for element scalars, and parent text content
                const char *tag = (const char *)event.data.scalar.tag;
                if (tag && (streq(tag, "!binary") || streq(tag, "!!binary")))
                {
                    if (streq(elname, "^")) //text content of parent node
                    {
                        binaryContent = true;
                        JBASE64_Decode((const char *) event.data.scalar.value, content.clear());
                    }
                    else
                    {
                        StringBuffer decoded;
                        JBASE64_Decode((const char *) event.data.scalar.value, decoded);
                        iEvent->beginNode(elname, false, parser.offset);
                        iEvent->endNode(elname, decoded.length(), (const void *) decoded.str(), true, parser.offset);
                    }
                }
                else if (streq(elname, "^")) //text content of parent node
                {
                    content.set((const char *) event.data.scalar.value);
                }
                else if (tag && (streq(tag, "!el") || streq(tag, "!element")))
                {
                    iEvent->beginNode(elname, false, parser.offset);
                    iEvent->endNode(elname, event.data.scalar.length, (const void *) event.data.scalar.value, false, parser.offset);
                }
                else //by default all named scalars are ptree attributes
                {
                    iEvent->newAttribute(attname, (const char *)event.data.scalar.value);
                }
                break;
            }
            case YAML_ALIAS_EVENT: //reference to an anchor, ignore for now
                iEvent->beginNode(elname, false, parser.offset);
                iEvent->endNode(elname, 0, nullptr, false, parser.offset);
                break;
            case YAML_MAPPING_END_EVENT: //done
                break;
            case YAML_NO_EVENT:
            case YAML_SEQUENCE_END_EVENT:
            case YAML_STREAM_START_EVENT:
            case YAML_STREAM_END_EVENT:
            case YAML_DOCUMENT_START_EVENT:
            case YAML_DOCUMENT_END_EVENT:
            default:
                //shouldn't be here
                break;
            }

            yaml_event_delete(&event);
        }
        if (tagname && *tagname)
            iEvent->endNode(tagname, content.length(), content, binaryContent, parser.offset);
    }
    virtual void load() override
    {
        yaml_event_t event;
        yaml_event_type_t eventType = YAML_NO_EVENT;
        bool doc = false;
        bool content = false;

        while (eventType!=YAML_STREAM_END_EVENT)
        {
            eventType = nextEvent(event);

            switch (eventType)
            {
            case YAML_MAPPING_START_EVENT:
                //root content, the start of all mappings, should be only one at the root
                if (content)
                    throw makeStringException(99, "YAML: Currently only support one content section (map) per stream");
                loadMap(noRoot ? nullptr : "__object__", false); //root map
                content=true;
                break;
            case YAML_SEQUENCE_START_EVENT:
                //root content, sequence (array), should be only one at the root and can't mix with mappings
                if (content)
                    throw makeStringException(99, "YAML: Currently only support one content section (sequence) per stream");
                if (!noRoot)
                    iEvent->beginNode("__array__", false, 0);
                loadSequence("__item__");
                if (!noRoot)
                    iEvent->endNode("__array__", 0, nullptr, false, parser.offset);
                content=true;
                break;
            case YAML_STREAM_START_EVENT:
            case YAML_STREAM_END_EVENT:
                //don't think we need to do anything... unless we start saving hints
                break;
            case YAML_DOCUMENT_START_EVENT:
                //should only support one?  multiple documents would imply an extra level of nesting (future flag?)
                if (doc)
                    throw makeStringException(99, "YAML: Currently only support one document per stream");
                doc=true;
                break;
            case YAML_DOCUMENT_END_EVENT:
                break;
            case YAML_NO_EVENT:
            case YAML_ALIAS_EVENT: //root alias?
            case YAML_MAPPING_END_EVENT:
            case YAML_SCALAR_EVENT: //root unmapped (unnamed) scalars?
            case YAML_SEQUENCE_END_EVENT:
                //shouldn't be here
                break;
            default:
                break;
            }

            yaml_event_delete(&event);
        }
    }
    virtual offset_t queryOffset() override
    {
        return parser.offset;
    }

};


IPTreeReader *createYAMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions)
{
    return new CYAMLBufferReader(buf, bufLength, iEvent, readerOptions);
}


IPropertyTree *createPTreeFromYAMLString(unsigned len, const char *yaml, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IPTreeMaker> _iMaker;
    if (!iMaker)
    {
        iMaker = createDefaultPTreeMaker(flags, (PTreeReaderOptions)(readFlags|ptr_encodeExtNames));
        _iMaker.setown(iMaker);
    }
    Owned<IPTreeReader> reader = createYAMLBufferReader(yaml, len, *iMaker, readFlags);
    reader->load();
    return LINK(iMaker->queryRoot());
}

IPropertyTree *createPTreeFromYAMLString(const char *yaml, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    return createPTreeFromYAMLString(strlen(yaml), yaml, flags, readFlags, iMaker);
}

IPropertyTree *createPTreeFromYAMLFile(const char *filename, byte flags, PTreeReaderOptions readFlags, IPTreeMaker *iMaker)
{
    Owned<IFile> in = createIFile(filename);
    if (!in->exists())
        return nullptr;

    StringBuffer contents;
    try
    {
        contents.loadFile(in);
    }
    catch (IException * e)
    {
        EXCLOG(e);
        e->Release();
        return nullptr;
    }

    return createPTreeFromYAMLString(contents.length(), contents, flags, readFlags, iMaker);
}

static int yaml_write_iiostream(void *data, unsigned char *buffer, size_t size)
{
    IIOStream *out = (IIOStream *) data;
    out->write(size, (void *)buffer);
    out->flush();
    return 1;
}

class YAMLEmitter
{
    yaml_emitter_t emitter;
    yaml_event_t event;
    IIOStream &out;
public:
    YAMLEmitter(IIOStream &ios, int indent) : out(ios)
    {
        if (!yaml_emitter_initialize(&emitter))
            throw MakeStringException(0, "YAMLEmitter: failed to initialize");
        yaml_emitter_set_output(&emitter, yaml_write_iiostream, &out);
        yaml_emitter_set_canonical(&emitter, false);
        yaml_emitter_set_unicode(&emitter, true);
        yaml_emitter_set_indent(&emitter, indent);

        beginStream();
        beginDocument();
    }
    ~YAMLEmitter()
    {
        endDocument();
        endStream();
        yaml_emitter_delete(&emitter);
    }
    yaml_char_t *getTag(bool binary, bool element)
    {
        if (binary)
            return (yaml_char_t *) "!binary";
        if (element)
            return (yaml_char_t *) "!el";
        return nullptr;
    }
    void emit()
    {
        yaml_emitter_emit(&emitter, &event);
    }
    void checkInit(int success, const char *descr)
    {
        if (success==0)
            throw MakeStringException(0, "YAMLEmitter: %s failed", descr);
    }
    void writeValue(const char *value, bool element, bool hidden, bool binary)
    {
        yaml_scalar_style_t style = binary ? YAML_LITERAL_SCALAR_STYLE : YAML_ANY_SCALAR_STYLE;
        const yaml_char_t *tag = getTag(binary, element);
        bool implicit = tag==nullptr;
        StringBuffer s;
        if (!value)
            value = "null";
        else if (hidden)
            value = (binary) ? "KioqKg==" : s.appendN(strlen(value), '*').str(); //KioqKg== is base64 of ****
        checkInit(yaml_scalar_event_initialize(&event, nullptr, tag, (yaml_char_t *) value, -1, implicit, implicit, style), "yaml_scalar_event_initialize");
        emit();
    }
    void writeName(const char *name)
    {
        dbgassertex(name!=nullptr);
        return writeValue(name, false, false,false);
    }
    void writeNamedValue(const char *name, const char *value, bool element, bool hidden)
    {
        writeName(name);
        writeValue(value, element, hidden, false);
    }
    void writeAttribute(const char *name, const char *value, bool hidden)
    {
        writeNamedValue(name, value, false, hidden);
    }
    void beginMap()
    {
        checkInit(yaml_mapping_start_event_initialize(&event, nullptr, nullptr, 0, YAML_BLOCK_MAPPING_STYLE), "yaml_mapping_start_event_initialize");
        emit();
    }
    void endMap()
    {
        checkInit(yaml_mapping_end_event_initialize(&event), "yaml_mapping_end_event_initialize");
        emit();
    }
    void beginSequence(const char *name)
    {
        if (name)
            writeName(name);

        checkInit(yaml_sequence_start_event_initialize(&event, nullptr, nullptr, 0, YAML_ANY_SEQUENCE_STYLE), "yaml_sequence_start_event_initialize");
        emit();
    }
    void endSequence()
    {
        checkInit(yaml_sequence_end_event_initialize(&event), "yaml_sequence_end_event_initialize");
        emit();
    }
    void beginDocument()
    {
        checkInit(yaml_document_start_event_initialize(&event, nullptr, nullptr, nullptr, true), "yaml_document_start_event_initialize");
        emit();
    }
    void endDocument()
    {
        checkInit(yaml_document_end_event_initialize(&event, true), "yaml_document_end_event_initialize");
        emit();
    }

    void beginStream()
    {
        checkInit(yaml_stream_start_event_initialize(&event, YAML_UTF8_ENCODING), "yaml_stream_start_event_initialize");
        emit();
    }
    void endStream()
    {
        checkInit(yaml_stream_end_event_initialize(&event), "yaml_stream_end_event_initialize");
        emit();
    }
};

static void _toYAML(const IPropertyTree *tree, YAMLEmitter &yaml, byte flags, bool root=false, bool isArrayItem=false)
{
    bool hiddenRootArrayObject = false;
    //Having to decode is the uncommon case.  Keep overhead low by only using a StringAttr here, and if we do have to decode, take the hit then and use an adapter (StringAttrBuilder).
    StringAttr decoded;
    {
        const char *name = tree->queryName();
        hiddenRootArrayObject = isRootArrayObjectHidden(root, name, flags);
        if (!root && !isArrayItem)
        {
            if (!name || !*name)
                name = "__unnamed__";
            else if (isPTreeNameEncoded(tree))
            {
                {
                    StringAttrBuilder decodedBuilder(decoded);
                    decodePtreeName(decodedBuilder, name);
                }
                name = decoded.str();
            }
            yaml.writeName(name);
        }
    }

    Owned<IAttributeIterator> it = tree->getAttributes(true);
    bool hasAttributes = it->first();
    bool complex = (hasAttributes || tree->hasChildren());

    if (!hiddenRootArrayObject)
    {
        if (complex)
            yaml.beginMap();

        if (hasAttributes)
        {
            ForEach(*it)
            {
                const char *key = it->queryName();
                if (isPTreeAttributeNameEncoded(tree, key))
                {
                    {
                        decoded.clear();
                        StringAttrBuilder decodedAtBuilder(decoded);
                        decodePtreeName(decodedAtBuilder, key);
                    }
                    key = decoded.str();
                }
                const char *val = it->queryValue();
                yaml.writeAttribute(key+1, val, isSanitizedAndHidden(val, flags, true));
            }
        }
    }
    StringBuffer _content;
    const char *content = nullptr; // to avoid uninitialized warning
    bool isBinary = tree->isBinary(NULL);
    bool isNull = true;

    if (!hiddenRootArrayObject)
    {
        if (isBinary)
        {
            MemoryBuffer thislevelbin;
            isNull = (!tree->getPropBin(NULL, thislevelbin))||(thislevelbin.length()==0);
            if (!isNull)
                JBASE64_Encode(thislevelbin.toByteArray(), thislevelbin.length(), _content, true);
            content = _content.str();
        }
        else if (tree->isCompressed(NULL))
        {
            isNull = false; // can't be empty if compressed;
            verifyex(tree->getProp(NULL, _content));
            content = _content.str();
        }
        else
            isNull = (NULL == (content = tree->queryProp(NULL)));

        if (isNull && !root && !complex)
        {
            yaml.writeValue("null", false, false, false);
            return;
        }
    }

    Owned<IPropertyTreeIterator> sub = tree->getElements(hiddenRootArrayObject ? "__item__" : "*", 0 != (flags & YAML_SortTags) ? iptiter_sort : iptiter_null);
    //note that detection of repeating elements relies on the fact that ptree elements
    //of the same name will be grouped together
    StringAttr seqname;
    bool sequence = false;
    ForEach(*sub)
    {
        IPropertyTree &element = sub->query();
        bool first = false;
        bool endprior = false;
        sequence = checkInSequence(element, seqname, first, endprior);
        if (endprior)
            yaml.endSequence();
        if (first)
        {
            const char *name = nullptr;
            if (!hiddenRootArrayObject)
            {
                if (!isPTreeNameEncoded(&element))
                    name = element.queryName();
                else
                {
                    {
                        decoded.clear();
                        StringAttrBuilder decodedBuilder(decoded);
                        decodePtreeName(decodedBuilder, element.queryName());
                    }
                    name = decoded.str();
                }
            }

            yaml.beginSequence(name);
        }

        _toYAML(&element, yaml, flags, false, sequence);
    }
    if (sequence)
        yaml.endSequence();

    if (!isNull)
    {
        if (complex)
            yaml.writeName("^");
        //repeating/array/sequence items are implicitly elements, no need for tag
        yaml.writeValue(content, isArrayItem ? false : true, isSanitizedAndHidden(content, flags, false), isBinary);
    }

    if (!hiddenRootArrayObject && complex)
        yaml.endMap();
}


static void _toYAML(const IPropertyTree *tree, IIOStream &out, unsigned indent, byte flags, bool root=false, bool isArrayItem=false)
{
    YAMLEmitter yaml(out, indent);
    _toYAML(tree, yaml, flags, true, false);
}

jlib_decl StringBuffer &toYAML(const IPropertyTree *tree, StringBuffer &ret, unsigned indent, byte flags)
{
    CStringBufferMarkupIOAdapter adapter(ret);
    _toYAML(tree->queryBranch(NULL), adapter, indent, flags, true);
    return ret;
}

void toYAML(const IPropertyTree *tree, IIOStream &out, unsigned indent, byte flags)
{
    _toYAML(tree, out, indent, flags, true);
}

void printYAML(const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    StringBuffer yaml;
    toYAML(tree, yaml, indent, flags);
    printf("%s", yaml.str());
}

void dbglogYAML(const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    StringBuffer yaml;
    toYAML(tree, yaml, indent, flags);
    DBGLOG("%s", yaml.str());
}

void saveYAML(const char *filename, const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    OwnedIFile ifile = createIFile(filename);
    saveYAML(*ifile, tree, indent, flags);
}

void saveYAML(IFile &ifile, const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    OwnedIFileIO ifileio = ifile.open(IFOcreate);
    if (!ifileio)
        throw MakeStringException(0, "saveXML: could not find %s to open", ifile.queryFilename());
    saveYAML(*ifileio, tree, indent, flags);
    ifileio->close();
}

void saveYAML(IFileIO &ifileio, const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    Owned<IIOStream> stream = createIOStream(&ifileio);
    stream.setown(createBufferedIOStream(stream));
    saveYAML(*stream, tree, indent, flags);
}

void saveYAML(IIOStream &stream, const IPropertyTree *tree, unsigned indent, unsigned flags)
{
    toYAML(tree, stream, indent, flags);
}


void copyPropIfMissing(IPropertyTree & target, const char * targetName, IPropertyTree & source, const char * sourceName)
{
    if (source.hasProp(sourceName) && !target.hasProp(targetName))
    {
        if (source.isBinary(sourceName))
        {
            MemoryBuffer value;
            source.getPropBin(sourceName, value);
            //MORE: Avoid decompression?
            target.setPropBin(targetName, value.length(), value.toByteArray(), COMPRESS_METHOD_DEFAULT);
        }
        else
            target.setProp(targetName, source.queryProp(sourceName));
    }
}

void copyProp(IPropertyTree & target, IPropertyTree & source, const char * name)
{
    if (source.hasProp(name))
    {
        if (source.isBinary(name))
        {
            MemoryBuffer value;
            source.getPropBin(name, value);
            target.setPropBin(name, value.length(), value.toByteArray(), COMPRESS_METHOD_DEFAULT);
        }
        else
            target.setProp(name, source.queryProp(name));
    }
}

inline void checkEndHexSequence(StringBuffer &s, bool &hexSequence)
{
    if (hexSequence)
    {
        //close current hex stream sequence
        s.append('_');
        hexSequence = false;
    }
}
inline bool isValidPTreeNameChar(const char *ch, unsigned remaining, unsigned &chlen, bool isatt, bool first)
{
    chlen = 0;
    if (!ch || !*ch || !remaining)
        return false;
    chlen = utf8CharLen((const unsigned char *)ch, remaining);
    if (0==chlen)
    {
        chlen=1;
        return false;
    }
    if (chlen>1)
        return !isatt;  //attributes do not seem to currently support multibyte characters in their names
    if (first)
        return isValidXPathStartChr(*ch);
    return isValidXPathChr(*ch);
}

const char *findFirstInvalidPTreeNameChar(const char *ch, unsigned remaining)
{
    if (!ch)
        return nullptr;
    if (remaining==0 || *ch=='\0') //empty name still needs to trigger a special encoding
        return ch;
    bool isatt;
    if ('@'==*ch)
    {
        isatt=true;
        ch++;
        remaining--;
    }
    else
        isatt=false;

    if (!remaining)
        return nullptr;

    unsigned chlen = 0;
    if (!isValidPTreeNameChar(ch, remaining, chlen, isatt, true))
        return ch;
    if (remaining<chlen)
        return nullptr;
    ch += chlen;
    remaining -= chlen;
    while (remaining)
    {
        if (!isValidPTreeNameChar(ch, remaining, chlen, isatt, false))
            return ch;
        if (remaining<=chlen)
            return nullptr;
        ch += chlen;
        remaining -= chlen;
    }
    return nullptr;
}

const char *findFirstInvalidPTreeNameChar(const char *ch)
{
    if (!ch)
        return nullptr;
    return findFirstInvalidPTreeNameChar(ch, strlen(ch));
}

static inline bool checkAppendValidPTreeNameChar(StringBuffer &s, const char *ch, unsigned remaining, unsigned &chlen, bool isatt, bool first, bool &hexSequence)
{
    if (!isValidPTreeNameChar(ch, remaining, chlen, isatt, first))
        return false;
    checkEndHexSequence(s, hexSequence);
    s.append(chlen, ch);
    return true;
}

inline void appendPTreeNameSpecialEncoding(StringBuffer &s, const char *enc, bool &hexSequence)
{
    checkEndHexSequence(s, hexSequence);
    s.append('_');
    s.append(enc);
}

inline StringBuffer &encodePTreeNameUtf8Char(StringBuffer &s, const char *&ch, unsigned &remaining, bool isatt, bool &hexSequence, bool &first)
{
    unsigned chlen = 1;
    if (first && (*ch=='@'))
    {
        s.append('@');
        ch++;
        remaining--;
        if (remaining==0 || *ch=='\0')
            return s;
    }
    if (*ch!='_' && checkAppendValidPTreeNameChar(s, ch, remaining, chlen, isatt, first, hexSequence))
    {
        remaining -= chlen;
        ch += chlen;
        first = false;
        return s;
    }
    switch (*ch)
    {
        case '_':
            appendPTreeNameSpecialEncoding(s, "_", hexSequence);
            break;
        case ' ':
            appendPTreeNameSpecialEncoding(s, "s", hexSequence);
            break;
        case '@':
            appendPTreeNameSpecialEncoding(s, "a", hexSequence);
            break;
        case '\\':
            appendPTreeNameSpecialEncoding(s, "b", hexSequence);
            break;
        case '/':
            appendPTreeNameSpecialEncoding(s, "f", hexSequence);
            break;
        case '\n':
            appendPTreeNameSpecialEncoding(s, "n", hexSequence);
            break;
        case '\"':
            appendPTreeNameSpecialEncoding(s, "Q", hexSequence);
            break;
        case '\r':
            appendPTreeNameSpecialEncoding(s, "r", hexSequence);
            break;
        case '\'':
            appendPTreeNameSpecialEncoding(s, "q", hexSequence);
            break;
        case '\t':
            appendPTreeNameSpecialEncoding(s, "t", hexSequence);
            break;
        default:
            {
                if (!hexSequence)
                {
                    s.append("_x");
                    hexSequence = true;
                }
                appendDataAsHex(s, chlen, (const void *)ch);
            }
            break;
    }
    ch += chlen;
    remaining -= chlen;
    if (first)
        first = false;
    return s;
}

StringBuffer &encodePTreeName(StringBuffer &s, unsigned size, const char *value, const char *startEncoding)
{
    if (!value || !size)
        return s.append("_0");
    bool isattr = ('@'==*value);
    if (isattr && size==1)
        return s.append("@_0");
    //preallocate some space, with expansion of characters to avoid constant reallocation while appending below
    s.ensureCapacity(size + size/2);
    if (startEncoding)
    {
        //see if we have any existing escape characters to expand ('_' ==> "__")
        const void *realStart = memchr(value, '_', startEncoding - value);
        if (realStart)
            startEncoding = (const char *) realStart;
        unsigned skipSize = startEncoding-value;
        if (skipSize > size)
            return s;
        s.append(skipSize, value);
        value = startEncoding;
        size -= skipSize;
    }
    bool hexSequence = false;
    bool first = true;
    while (size)
        encodePTreeNameUtf8Char(s, value, size, isattr, hexSequence, first);
    checkEndHexSequence(s, hexSequence);
    return s;
}

StringBuffer &encodePTreeName(StringBuffer &s, const char *value, const char *startEncoding)
{
    if (!value)
        return s.append("_0");
    return encodePTreeName(s, strlen(value), value, startEncoding);
}

//Only encode the PTREE XPATH name if necessary. That matches the way internal PTREE names are stored allowing the resulting XPATH to be used directly
StringBuffer &appendPTreeXPathName(StringBuffer &s, unsigned size, const char *value)
{
    if (!size || !value)
        return s.append("_0");
    const char *startEncoding = findFirstInvalidPTreeNameChar(value, size);
    if (!startEncoding)
        return s.append(value);
    return encodePTreeName(s, size, value, startEncoding);
}

StringBuffer &appendPTreeXPathName(StringBuffer &s, const char *value)
{
    if (isEmptyString(value))
        return s;
    return appendPTreeXPathName(s, strlen(value), value);
}

void markPTreeNameEncoded(IPropertyTree *tree)
{
    if (!tree)
        return;
    PTree *pt = static_cast<PTree*>(tree);
    pt->markNameEncoded();
}

bool isPTreeNameEncoded(const IPropertyTree *tree)
{
    if (!tree)
        return false;
    const PTree *pt = static_cast<const PTree*>(tree);
    return pt->isNameEncoded();
}

void setPTreeAttribute(IPropertyTree *tree, const char *name, const char *value, bool markEncoded)
{
    if (!tree || isEmptyString(name))
        return;
    PTree *pt = static_cast<PTree*>(tree);
    pt->setAttribute(name, value, markEncoded);
}

bool isPTreeAttributeNameEncoded(const IPropertyTree *tree, const char *name)
{
    if (!tree || isEmptyString(name))
        return false;
    const PTree *pt = static_cast<const PTree*>(tree);
    return pt->isAttributeNameEncoded(name);
}

bool isNullPtreeName(const char * name, bool isEncoded)
{
    if (isEmptyString(name))
        return true;
    return isEncoded && streq(name, "_0");
}

static void decodePTreeNameHexEncoding(StringBuffer &s, const char *&input, unsigned &_remaining)
{
    //use local variables to avoid indirect reference
    unsigned remaining = _remaining;
    byte ch = 0;
    while (remaining >= 2 && *input!='_')
    {
        byte high = hex2num(*input++);
        ch = (high << 4) | hex2num(*input++);
        remaining -= 2;
        s.append(ch);
    }
    if (remaining && (*input=='_'))
    {
        input++;
        remaining--;
    }
    _remaining=remaining;
}

static void decodePTreeNameEncoding(StringBuffer &s, const char *&input, unsigned &_remaining)
{
    //use local variables to avoid indirect reference
    unsigned remaining = _remaining;
    if (*input!='_')
        return;
    input++;
    remaining--;
    if (remaining==0)
    {
        _remaining=remaining;
        return;
    }
    if (*input=='x')
    {
        input++;
        remaining--;
        decodePTreeNameHexEncoding(s, input, remaining);
        _remaining=remaining;
        return;
    }
    switch (*input)
    {
        case '0': //we only currently use this for empy names (including "@" with no tail)
            break;
        case '_':
            s.append('_');
            break;
        case 'a':
            s.append('@');
            break;
        case 'b':
            s.append('\\');
            break;
        case 'f':
            s.append('/');
            break;
        case 'n':
            s.append('\n');
            break;
        case 'Q':
            s.append('\"');
            break;
        case 'r':
            s.append('\r');
            break;
        case 's':
            s.append(' ');
            break;
        case 'q':
            s.append('\'');
            break;
        case 't':
            s.append('\t');
            break;
        default: //shouldn't get here since encoding can't be done externally, but treat as if not encoded
            s.append('_').append(*input);
            break;
    }
    input++;
    remaining--;
    _remaining=remaining;
}

StringBuffer &decodePtreeName(StringBuffer &s, const char *input, unsigned remaining)
{
    if (!input || !remaining)
        return s;
    s.ensureCapacity(remaining); //preallocate for the multiple appends to follow.. decode should generally be less than or equal to number of input bytes
    while (remaining)
    {
        if ('_'!=*input)
        {
            s.append(*input);
            input++;
            remaining--;
        }
        else
            decodePTreeNameEncoding(s, input, remaining);
    }
    return s;
}

StringBuffer &decodePtreeName(StringBuffer &s, const char *name)
{
    if (isEmptyString(name))
        return s;
    return decodePtreeName(s, name, strlen(name));
}

// 'expert' config helper methods
StringBuffer &getExpertOptPath(const char *opt, StringBuffer &out)
{
#ifdef _CONTAINERIZED
    if (opt)
        return out.append("expert/@").append(opt);
    return out.append("expert");
#else
    if (opt)
        return out.append("Debug/@").append(opt);
    return out.append("Debug");
#endif
}

bool hasExpertOpt(const char *opt)
{
    StringBuffer xpath;
    getExpertOptPath(opt, xpath);
    return getComponentConfigSP()->hasProp(xpath);
}

bool getExpertOptBool(const char *opt, bool dft)
{
    StringBuffer xpath;
    getExpertOptPath(opt, xpath);
    return getComponentConfigSP()->getPropBool(xpath, dft);
}

__int64 getExpertOptInt64(const char *opt, __int64 dft)
{
    StringBuffer xpath;
    getExpertOptPath(opt, xpath);
    return getComponentConfigSP()->getPropInt64(xpath, dft);
}

double getExpertOptReal(const char *opt, double dft)
{
    StringBuffer xpath;
    getExpertOptPath(opt, xpath);
    return getComponentConfigSP()->getPropReal(xpath, dft);
}

StringBuffer &getExpertOptString(const char *opt, StringBuffer &out)
{
    StringBuffer xpath;
    getExpertOptPath(opt, xpath);
    getComponentConfigSP()->getProp(xpath, out);
    return out;
}

void setExpertOpt(const char *opt, const char *value)
{
    StringBuffer xpath;
    getExpertOptPath(nullptr, xpath);
    Owned<IPropertyTree> config = getComponentConfigSP();
    if (!config->hasProp(xpath))
        config->setPropTree(xpath);
    getExpertOptPath(opt, xpath.clear());
    config->setProp(xpath, value);
}

//---------------------------------------------------------------------------------------------------------------------

//HPCC-30752 This should move inside PTree to allow a more efficient hash calculation and possible caching.
//Currently the values are not persisted so the implementation could change.  That may change in the future.
unsigned getPropertyTreeHash(const IPropertyTree & source, unsigned hashcode)
{
    if (source.isBinary())
    {
        MemoryBuffer mb;
        source.getPropBin(nullptr, mb);
        hashcode = hashc((const byte *)mb.bufferBase(), mb.length(), hashcode);
    }
    else
    {
        const char * value = source.queryProp(nullptr);
        if (value)
            hashcode = hashcz((const byte *)value, hashcode);
    }

    Owned<IAttributeIterator> aiter = source.getAttributes();
    ForEach(*aiter)
    {
        hashcode = hashcz((const byte *)aiter->queryName(), hashcode);
        hashcode = hashcz((const byte *)aiter->queryValue(), hashcode);
    }

    Owned<IPropertyTreeIterator> iter = source.getElements("*");
    ForEach(*iter)
    {
        IPropertyTree & child = iter->query();
        hashcode = hashcz((const byte *)child.queryName(), hashcode);
        hashcode = getPropertyTreeHash(child, hashcode);
    }
    return hashcode;
}

class SyncedPropertyTreeWrapper : extends CInterfaceOf<ISyncedPropertyTree>
{
public:
    SyncedPropertyTreeWrapper(IPropertyTree * _tree) : tree(_tree)
    {
    }

    virtual const IPropertyTree * getTree() const override
    {
        return LINK(tree);
    }

    virtual bool getProp(MemoryBuffer & result, const char * xpath) const override
    {
        if (!tree)
            return false;
        return tree->getPropBin(xpath, result);
    }

    virtual bool getProp(StringBuffer & result, const char * xpath) const override
    {
        if (!tree)
            return false;
        return tree->getProp(xpath, result);
    }

    virtual unsigned getVersion() const override
    {
        return 0;
    }

    virtual bool isStale() const override
    {
        return false;
    }

    virtual bool isValid() const override
    {
        return tree != nullptr;
    }

protected:
    Linked<IPropertyTree> tree;
};

ISyncedPropertyTree * createSyncedPropertyTree(IPropertyTree * tree)
{
    return new SyncedPropertyTreeWrapper(tree);
}

/*
 * PTree Timing tests
 *
 */

#ifdef _USE_CPPUNIT
#include <cmath>
#include "unittests.hpp"

class PTreeDeserializeTimingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(PTreeDeserializeTimingTest);
    CPPUNIT_TEST(testSmallTreeDeserialize);
    CPPUNIT_TEST(testMediumTreeDeserialize);
    CPPUNIT_TEST(testLargeTreeDeserialize);
    CPPUNIT_TEST(testExtraLargeTreeDeserialize);
    CPPUNIT_TEST(testHugeTreeDeserialize);
    CPPUNIT_TEST(testDeepVsWideTreeDeserialize);
    CPPUNIT_TEST_SUITE_END();

public:
    // Helper method to create a tree with the specified number of nodes
    IPropertyTree *createTree(unsigned numNodes, unsigned depth, bool binary = false)
    {
        Owned<IPropertyTree> tree = createPTree("root");

        if (depth <= 1) // Wide tree
        {
            for (unsigned i = 0; i < numNodes; i++)
            {
                StringBuffer name;
                name.append("node").append(i);
                Owned<IPropertyTree> child = createPTree(name.str());

                // Add attributes
                child->setProp("@attr1", "value1");
                child->setProp("@attr2", "value2");

                // Add some text content
                child->setProp(NULL, "Some text content for this node");

                // Add binary content for some nodes
                if (binary && (i % 5 == 0))
                {
                    MemoryBuffer mb;
                    mb.append(100, "binary data");
                    child->setPropBin("binaryProp", mb.length(), mb.toByteArray());
                }

                tree->addPropTree(name.str(), child.getClear());
            }
        }
        else // Deep tree
        {
            unsigned nodesPerLevel = (numNodes > depth) ? (numNodes / depth) : 1;

            std::function<void(IPropertyTree *, unsigned, unsigned)> buildLevel =
                [&](IPropertyTree *parent, unsigned level, unsigned remaining)
            {
                if (level >= depth || remaining == 0)
                    return;

                unsigned nodeCount = std::min(nodesPerLevel, remaining);

                for (unsigned i = 0; i < nodeCount; i++)
                {
                    StringBuffer name;
                    name.append("level").append(level).append("_").append(i);

                    Owned<IPropertyTree> child = createPTree(name.str());
                    child->setPropInt("@level", level);
                    child->setPropInt("@index", i);
                    child->setProp(NULL, "Node content");

                    parent->addPropTree(name.str(), child.getClear());

                    // Recursively build the next level
                    buildLevel(parent->queryPropTree(name.str()), level + 1,
                               (remaining - 1) / nodeCount);
                }
            };

            buildLevel(tree, 1, numNodes);
        }

        return tree.getClear();
    }

    void testDeserializePerformance(const char *description, unsigned numNodes, unsigned depth, bool binary, unsigned iterations)
    {
        // Create and serialize tree
        Owned<IPropertyTree> originalTree = createTree(numNodes, depth, binary);
        MemoryBuffer serialized;
        originalTree->serialize(serialized);

        // Measure deserialization time
        CCycleTimer timer;
        cycle_t totalCycles{0};
        unsigned __int64 totalSize{0};

        for (unsigned i = 0; i < iterations; i++)
        {
            serialized.reset();
            timer.reset();

            Owned<IPropertyTree> deserializedTree = createPTree();
            deserializedTree->deserialize(serialized);

            cycle_t elapsed = timer.elapsedCycles();
            totalCycles += elapsed;
            totalSize += serialized.length();

            // Verify first and last iterations
            if (i == 0 || i == iterations - 1)
            {
                // Simple verification - check some key attributes
                bool match = (deserializedTree->hasProp("@attr1") == originalTree->hasProp("@attr1")) &&
                             (deserializedTree->numChildren() == originalTree->numChildren());
                CPPUNIT_ASSERT_MESSAGE("Deserialized tree doesn't match original", match);
            }
        }

        // Calculate and report results
        StringBuffer formattedResult_numNodes;
        StringBuffer formattedResult_depth;
        StringBuffer formattedResult_iterations;
        StringBuffer formattedResult_serializedLength;
        double runTimeSecs = cycle_to_millisec(totalCycles) / 1000.0;
        DBGLOG("Test %s: nodes %s; depth %s; content: %s; iterations: %s\tDeserialized: %s bytes\tRuntime: %.2f s",
               description,
               formatWithCommas(numNodes, formattedResult_numNodes).str(),
               formatWithCommas(depth, formattedResult_depth).str(),
               binary ? "binary" : "text",
               formatWithCommas(iterations, formattedResult_iterations).str(),
               formatWithCommas(serialized.length(), formattedResult_serializedLength).str(),
               runTimeSecs);
    }

    void testSmallTreeDeserialize()
    {
        testDeserializePerformance("Small Tree (wide)", 100, 1, false, 1000);
        testDeserializePerformance("Small Tree (deep)", 100, 1000, false, 1000);
    }

    void testMediumTreeDeserialize()
    {
        testDeserializePerformance("Medium Tree (wide)", 1000, 1, false, 100);
        testDeserializePerformance("Medium Tree (deep)", 1000, 1000, false, 100);
    }

    void testLargeTreeDeserialize()
    {
        testDeserializePerformance("Large Tree (wide)", 10000, 1, false, 100);
        testDeserializePerformance("Large Tree (wide)", 10000, 1, true, 100);
    }

    void testExtraLargeTreeDeserialize()
    {
        testDeserializePerformance("Extra Large Tree (wide)", 100000, 1, false, 50);
        testDeserializePerformance("Extra Large Tree (wide)", 100000, 1, true, 50);
    }

    void testHugeTreeDeserialize()
    {
        testDeserializePerformance("Huge Tree (wide)", 1000000, 1, false, 5);
        testDeserializePerformance("Huge Tree (wide)", 1000000, 1, true, 5);
    }

    void testDeepVsWideTreeDeserialize()
    {
        // Compare trees with same node count but different structures
        unsigned nodeCount = 5000;

        testDeserializePerformance("Deep Tree", nodeCount, 5000, false, 100);
        testDeserializePerformance("Wide Tree", nodeCount, 1, false, 100);
        testDeserializePerformance("Balanced Tree", nodeCount, (unsigned)sqrt(nodeCount), false, 100);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(PTreeDeserializeTimingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(PTreeDeserializeTimingTest, "PTreeDeserializeTimingTest");

#endif // _USE_CPPUNIT
