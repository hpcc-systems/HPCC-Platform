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


#ifndef _PTREE_IPP
#define _PTREE_IPP

#include "jarray.hpp"
#include "jexcept.hpp"
#include "jhash.hpp"
#include "jmutex.hpp"
#include "jsuperhash.hpp"


#include "jptree.hpp"
#include "jbuff.hpp"

#ifdef __64BIT__
#pragma pack(push,1)    // 64bit pack PTree's    (could possibly do for 32bit also but may be compatibility problems)
#endif



#define ANE_APPEND -1
#define ANE_SET -2
///////////////////
class MappingStringToOwned : public MappingStringTo<IInterfacePtr,IInterfacePtr>
{
public:
  MappingStringToOwned(const char * k, IInterfacePtr a) :
      MappingStringTo<IInterfacePtr,IInterfacePtr>(k,a) { }
  ~MappingStringToOwned() { ::Release(val); }
};

typedef MapStringTo<IInterfacePtr, IInterfacePtr, MappingStringToOwned> MapStringToOwned;

// case sensitive childmap
class jlib_decl ChildMap : protected SuperHashTableOf<IPropertyTree, constcharptr>
{
protected:
// SuperHashTable definitions
    virtual void onAdd(void *) {}

    virtual void onRemove(void *e)
    {
        IPropertyTree &elem= *(IPropertyTree *)e;
        elem.Release();
    }
    virtual unsigned getHashFromElement(const void *e) const;
    virtual unsigned getHashFromFindParam(const void *fp) const
    {
        return hashc((const unsigned char *)fp, (size32_t)strlen((const char *)fp), 0);
    }
    virtual const void *getFindParam(const void *e) const
    {
        const IPropertyTree &elem=*(const IPropertyTree *)e;
        return (void *)elem.queryName();
    }
    virtual bool matchesFindParam(const void *e, const void *fp, unsigned fphash) const
    {
        return (0 == strcmp(((IPropertyTree *)e)->queryName(), (const char *)fp));
    }
public: 
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(IPropertyTree, constcharptr);

    inline unsigned count() const { return SuperHashTableOf<IPropertyTree, constcharptr>::count(); }

    ChildMap() : SuperHashTableOf<IPropertyTree, constcharptr>(4)
    { 
    }
    ~ChildMap() 
    { 
        kill(); 
    }
    virtual unsigned numChildren();
    virtual IPropertyTreeIterator *getIterator(bool sort);
    virtual bool set(const char *key, IPropertyTree *tree)
    {
        return SuperHashTableOf<IPropertyTree, constcharptr>::replace(* tree);
    }
    virtual bool replace(const char *key, IPropertyTree *tree) // provides different semantics, used if element being replaced is not to be treated as deleted.
    {
        return SuperHashTableOf<IPropertyTree, constcharptr>::replace(* tree);
    }
    virtual IPropertyTree *query(const char *key)
    {
        return find(*key);
    }
    virtual bool remove(const char *key)
    {
        return SuperHashTableOf<IPropertyTree, constcharptr>::remove(key);
    }
    virtual bool removeExact(IPropertyTree *child)
    {
        return SuperHashTableOf<IPropertyTree, constcharptr>::removeExact(child);
    }
};

// case insensitive childmap
class jlib_decl ChildMapNC : public ChildMap
{
public:
// SuperHashTable definitions
    virtual unsigned getHashFromFindParam(const void *fp) const
    {
        return hashnc((const unsigned char *)fp, (size32_t)strlen((const char *)fp), 0);
    }
    virtual bool matchesFindParam(const void *e, const void *fp, unsigned fphash) const
    {
        return (0 == stricmp(((IPropertyTree *)e)->queryName(), (const char *)fp));
    }
};


// http://www.w3.org/TR/REC-xml#xml-names
static const char *validStartChrs = ":_";
inline static bool isValidXPathStartChr(char c)
{
    return ('\0' != c && (isalpha(c) || strchr(validStartChrs, c)));
}

static const char *validChrs = ":_.-";
inline static bool isValidXPathChr(char c)
{
    return ('\0' != c && (isalnum(c) || strchr(validChrs, c)));
}

inline static int validJSONUtf8ChrLen(unsigned char c)
{
    if (c <= 31)
        return 0;
    if ('\"' == c)
        return 0;
    if ('\\' == c)
        return 2;
    return utf8CharLen(&c);
}

inline static bool isAttribute(const char *xpath) { return (xpath && *xpath == '@'); }

jlib_decl const char *splitXPathUQ(const char *xpath, StringBuffer &path);
jlib_decl const char *queryHead(const char *xpath, StringBuffer &head);
jlib_decl const char *queryNextUnquoted(const char *str, char c);

interface IPTArrayValue
{
    virtual ~IPTArrayValue() { }

    virtual bool isArray() const = 0;
    virtual bool isCompressed() const = 0;
    virtual const void *queryValue() const = 0;
    virtual MemoryBuffer &getValue(MemoryBuffer &tgt, bool binary) const = 0;
    virtual StringBuffer &getValue(StringBuffer &tgt, bool binary) const = 0;
    virtual size32_t queryValueSize() const = 0;
    virtual IPropertyTree *queryElement(unsigned idx) const = 0;
    virtual void addElement(IPropertyTree *e) = 0;
    virtual void setElement(unsigned idx, IPropertyTree *e) = 0;
    virtual void removeElement(unsigned idx) = 0;
    virtual unsigned elements() const = 0;
    virtual const void *queryValueRaw() const = 0;
    virtual unsigned queryValueRawSize() const = 0;

    virtual void serialize(MemoryBuffer &tgt) = 0;
    virtual void deserialize(MemoryBuffer &src) = 0;
};

class CPTArray : implements IPTArrayValue, private Array
{
public:
    virtual bool isArray() const { return true; }
    virtual bool isCompressed() const { return false; }
    virtual const void *queryValue() const { UNIMPLEMENTED; }
    virtual MemoryBuffer &getValue(MemoryBuffer &tgt, bool binary) const { UNIMPLEMENTED; }
    virtual StringBuffer &getValue(StringBuffer &tgt, bool binary) const { UNIMPLEMENTED; }
    virtual size32_t queryValueSize() const { UNIMPLEMENTED; }
    virtual IPropertyTree *queryElement(unsigned idx) const { return (idx<ordinality()) ? &((IPropertyTree &)item(idx)) : NULL; }
    virtual void addElement(IPropertyTree *tree) { append(*tree); }
    virtual void setElement(unsigned idx, IPropertyTree *tree) { add(*tree, idx); }
    virtual void removeElement(unsigned idx) { remove(idx); }
    virtual unsigned elements() const { return ordinality(); }
    virtual const void *queryValueRaw() const { UNIMPLEMENTED; return NULL; }
    virtual unsigned queryValueRawSize() const { UNIMPLEMENTED; return 0; }

// serializable
    virtual void serialize(MemoryBuffer &tgt) { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &src) { UNIMPLEMENTED; }
};


class jlib_decl CPTValue : implements IPTArrayValue, private MemoryAttr
{
public:
    CPTValue(MemoryBuffer &src)
    { 
        deserialize(src); 
    }
    CPTValue(size32_t size, const void *data, bool binary=false, bool raw=false, bool compressed=false);

    virtual bool isArray() const { return false; }
    virtual bool isCompressed() const { return compressed; }
    virtual const void *queryValue() const;
    virtual MemoryBuffer &getValue(MemoryBuffer &tgt, bool binary) const;
    virtual StringBuffer &getValue(StringBuffer &tgt, bool binary) const;
    virtual size32_t queryValueSize() const;
    virtual IPropertyTree *queryElement(unsigned idx) const { UNIMPLEMENTED; return NULL; }
    virtual void addElement(IPropertyTree *tree) { UNIMPLEMENTED; }
    virtual void setElement(unsigned idx, IPropertyTree *tree) { UNIMPLEMENTED; }
    virtual void removeElement(unsigned idx) { UNIMPLEMENTED; }
    virtual unsigned elements() const {  UNIMPLEMENTED; return (unsigned)-1; }
    virtual const void *queryValueRaw() const { return get(); }
    virtual unsigned queryValueRawSize() const { return length(); }

// serilizable
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);

private:
    mutable bool compressed;
};

#define IptFlagTst(fs, f) (0!=(fs&(f)))
#define IptFlagSet(fs, f) (fs |= (f))
#define IptFlagClr(fs, f) (fs &= (~f))

struct AttrStr
{
    unsigned hash;
    unsigned short linkcount;
    char str[1];
    const char *get() const { return str; }
};

struct AttrValue
{
    AttrStr *key;
    AttrStr *value;
};

#define AM_NOCASE_FLAG (0x8000)
#define AM_NOCASE_MASK (0x7fff)

class jlib_decl AttrMap
{
    AttrValue *attrs;
    unsigned short numattrs;    // use top bit for nocase flag
    static AttrValue **freelist; // entry 0 not used
    static unsigned freelistmax; 
    static CLargeMemoryAllocator freeallocator;

    AttrValue *newArray(unsigned n);
    void freeArray(AttrValue *a,unsigned n);

public:
    AttrMap()
    { 
        attrs = NULL;
        numattrs = 0;
    }
    ~AttrMap() 
    { 
        kill();
    }
    inline unsigned count() const
    { 
        return numattrs&AM_NOCASE_MASK; 
    }
    inline AttrValue *item(unsigned i) const
    { 
        return attrs+i; 
    }
    void setNoCase(bool nc) 
    { 
        if (nc) 
            numattrs |= AM_NOCASE_FLAG; 
        else 
            numattrs &= AM_NOCASE_MASK; 
    }
    inline bool isNoCase() const 
    { 
        return (numattrs&AM_NOCASE_FLAG)!=0; 
    }
    void kill();
    void set(const char *key, const char *val);
    const char *find(const char *key) const;
    bool remove(const char *key);
    void swap(AttrMap &other);
    static inline void killfreelist()
    {
        free(freelist);
        freelist = NULL;
    }
};


class jlib_decl PTree : public CInterface, implements IPropertyTree
{
friend class SingleIdIterator;
friend class PTLocalIteratorBase;
friend class PTIdMatchIterator;
friend class ChildMap;

public:
    IMPLEMENT_IINTERFACE;
    virtual bool IsShared() const { return CInterface::IsShared(); }

    PTree(MemoryBuffer &mb);
    PTree(const char *_name=NULL, byte _flags=0, IPTArrayValue *_value=NULL, ChildMap *_children=NULL);
    ~PTree();

    IPropertyTree *queryParent() { return parent; }
    IPropertyTree *queryChild(unsigned index);
    ChildMap *queryChildren() { return children; }
    aindex_t findChild(IPropertyTree *child, bool remove=false);
    inline bool isnocase() const { return IptFlagTst(flags, ipt_caseInsensitive); }
    const ipt_flags queryFlags() const { return (ipt_flags) flags; }
public:
    void serializeCutOff(MemoryBuffer &tgt, int cutoff=-1, int depth=0);
    void serializeAttributes(MemoryBuffer &tgt);
    virtual void serializeSelf(MemoryBuffer &tgt);
    virtual void deserializeSelf(MemoryBuffer &src);
    virtual void createChildMap() { children = isnocase()?new ChildMapNC():new ChildMap(); }


    HashKeyElement *queryKey() { return name; }
    void setName(const char *_name);
    IPropertyTree *clone(IPropertyTree &srcTree, bool self=false, bool sub=true);
    void clone(IPropertyTree &srcTree, IPropertyTree &dstTree, bool sub=true);
    inline void setParent(IPropertyTree *_parent) { parent = _parent; }
    IPropertyTree *queryCreateBranch(IPropertyTree *branch, const char *prop, bool *existing=NULL);
    IPropertyTree *splitBranchProp(const char *xpath, const char *&_prop, bool error=false);
    IPTArrayValue *queryValue() { return value; }
    IPTArrayValue *detachValue() { IPTArrayValue *v = value; value = NULL; return v; }
    void setValue(IPTArrayValue *_value, bool binary) { if (value) delete value; value = _value; if (binary) IptFlagSet(flags, ipt_binary); }
    bool checkPattern(const char *&xxpath) const;
    void clear();

// IPropertyTree impl.
    virtual bool hasProp(const char * xpath) const;
    virtual bool isBinary(const char *xpath=NULL) const;
    virtual bool isCompressed(const char *xpath=NULL) const;
    virtual bool renameProp(const char *xpath, const char *newName);
    virtual bool renameTree(IPropertyTree *tree, const char *newName);
    virtual const char *queryProp(const char *xpath) const;
    virtual bool getProp(const char *xpath, StringBuffer &ret) const;
    virtual void setProp(const char *xpath, const char *val);
    virtual void addProp(const char *xpath, const char *val);
    virtual void appendProp(const char *xpath, const char *val);
    virtual bool getPropBool(const char *xpath, bool dft=false) const;
    virtual void setPropBool(const char *xpath, bool val) { setPropInt(xpath, val); }
    virtual void addPropBool(const char *xpath, bool val) { addPropInt(xpath, val); }
    virtual __int64 getPropInt64(const char *xpath, __int64 dft=0) const;
    virtual void setPropInt64(const char * xpath, __int64 val);
    virtual void addPropInt64(const char *xpath, __int64 val);
    virtual int getPropInt(const char *xpath, int dft=0) const;
    virtual void setPropInt(const char *xpath, int val);
    virtual void addPropInt(const char *xpath, int val);
    virtual bool getPropBin(const char * xpath, MemoryBuffer &ret) const;
    virtual void setPropBin(const char * xpath, size32_t size, const void *data);
    virtual void appendPropBin(const char *xpath, size32_t size, const void *data);
    virtual void addPropBin(const char *xpath, size32_t size, const void *data);
    virtual IPropertyTree *getPropTree(const char *xpath) const;
    virtual IPropertyTree *queryPropTree(const char *xpath) const;
    virtual IPropertyTree *getBranch(const char *xpath) const { return LINK(queryBranch(xpath)); }
    virtual IPropertyTree *queryBranch(const char *xpath) const { return queryPropTree(xpath); }
    virtual IPropertyTree *setPropTree(const char *xpath, IPropertyTree *val);
    virtual IPropertyTree *addPropTree(const char *xpath, IPropertyTree *val);
    virtual bool removeTree(IPropertyTree *child);
    virtual bool removeProp(const char *xpath);
    virtual aindex_t queryChildIndex(IPropertyTree *child);
    virtual const char *queryName() const;
    virtual StringBuffer &getName(StringBuffer &ret) const;
    virtual IAttributeIterator *getAttributes(bool sorted=false) const;
    virtual IPropertyTreeIterator *getElements(const char *xpath, IPTIteratorCodes flags = iptiter_null) const;
    virtual void localizeElements(const char *xpath, bool allTail=false);
    virtual bool hasChildren() const { return children && children->count()?true:false; }
    virtual unsigned numUniq() { return checkChildren()?children->count():0; }  
    virtual unsigned numChildren();
    virtual bool isCaseInsensitive() { return isnocase(); }
    virtual unsigned getCount(const char *xpath);
// serializable impl.
    virtual void serialize(MemoryBuffer &tgt);
    virtual void deserialize(MemoryBuffer &src);
    const AttrMap &queryAttributes() const { return attributes; }
    
protected:
    virtual ChildMap *checkChildren() const;
    virtual bool isEquivalent(IPropertyTree *tree) { return (NULL != QUERYINTERFACE(tree, PTree)); }
    virtual void setLocal(size32_t l, const void *data, bool binary=false);
    virtual void appendLocal(size32_t l, const void *data, bool binary=false);
    virtual void setAttr(const char *attr, const char *val);
    virtual bool removeAttr(const char *attr);
    virtual void addingNewElement(IPropertyTree &child, int pos) { }
    virtual void removingElement(IPropertyTree *tree, unsigned pos) { }
    virtual IPropertyTree *create(const char *name=NULL, IPTArrayValue *value=NULL, ChildMap *children=NULL, bool existing=false) = 0;
    virtual IPropertyTree *create(MemoryBuffer &mb) = 0;
    virtual IPropertyTree *ownPTree(IPropertyTree *tree);
    aindex_t getChildMatchPos(const char *xpath);

private:
    void init();
    void addLocal(size32_t l, const void *data, bool binary=false, int pos=-1);
    void resolveParentChild(const char *xpath, IPropertyTree *&parent, IPropertyTree *&child, StringAttr &path, StringAttr &qualifier);
    void replaceSelf(IPropertyTree *val);

protected: // data
    IPropertyTree *parent; // ! currently only used if tree embedded into array, used to locate position.

    HashKeyElement *name;
    ChildMap *children;
    IPTArrayValue *value;
    AttrMap attributes;
    byte flags;
};

jlib_decl IPropertyTree *createPropBranch(IPropertyTree *tree, const char *xpath, bool createIntermediates=false, IPropertyTree **created=NULL, IPropertyTree **createdParent=NULL);

class LocalPTree : public PTree
{
public:
    LocalPTree(const char *name=NULL, byte flags=ipt_none, IPTArrayValue *value=NULL, ChildMap *children=NULL)
        : PTree(name, flags, value, children) { }

    virtual bool isEquivalent(IPropertyTree *tree) { return (NULL != QUERYINTERFACE(tree, LocalPTree)); }
    virtual IPropertyTree *create(const char *name=NULL, IPTArrayValue *value=NULL, ChildMap *children=NULL, bool existing=false)
    {
        return new LocalPTree(name, flags, value, children);
    }
    virtual IPropertyTree *create(MemoryBuffer &mb)
    {
        IPropertyTree *tree = new LocalPTree();
        tree->deserialize(mb);
        return tree;
    }
};

class PTree;
class SingleIdIterator : public CInterface, implements IPropertyTreeIterator
{
public:
    IMPLEMENT_IINTERFACE;

    SingleIdIterator(const PTree &_tree, unsigned pos=1, unsigned _many=(unsigned)-1);
    ~SingleIdIterator();
    void setCurrent(unsigned pos);

// IPropertyTreeIterator
    virtual bool first();
    virtual bool next();
    virtual bool isValid();
    virtual IPropertyTree & query() { return * current; }

private:
    unsigned many, count, whichNext, start;
    IPropertyTree *current;
    const PTree &tree;
};


class PTLocalIteratorBase : public CInterface, implements IPropertyTreeIterator
{
public:
    IMPLEMENT_IINTERFACE;

    PTLocalIteratorBase(const PTree *tree, const char *_id, bool _nocase, bool sort);

    ~PTLocalIteratorBase();
    virtual bool match() = 0;

// IPropertyTreeIterator
    virtual bool first();
    virtual bool next();
    virtual bool isValid();
    virtual IPropertyTree & query() { return iter->query(); }

protected:
    IPropertyTreeIterator *baseIter;
    StringAttr id;
    bool nocase, sort;
private:
    const PTree *tree;
    IPropertyTreeIterator *iter;
    IPropertyTree *current;
    bool _next();
};

class PTIdMatchIterator : public PTLocalIteratorBase
{
public:
    IMPLEMENT_IINTERFACE;

    PTIdMatchIterator(const PTree *tree, const char *id, bool nocase, bool sort) : PTLocalIteratorBase(tree, id, nocase, sort) { }

    virtual bool match();
};

class StackElement;

class PTStackIterator : public CInterface, implements IPropertyTreeIterator
{
public:
    IMPLEMENT_IINTERFACE;

    PTStackIterator(IPropertyTreeIterator *_iter, const char *_xpath);
    ~PTStackIterator();

// IPropertyTreeIterator
    virtual bool first();
    virtual bool isValid();
    virtual bool next();
    virtual IPropertyTree & query();

private:
    void setIterator(IPropertyTreeIterator *iter);
    void pushToStack(IPropertyTreeIterator *iter, const char *xpath);
    IPropertyTreeIterator *popFromStack(StringAttr &path);

private: // data
    IPropertyTreeIterator *rootIter, *iter;
    const char *xxpath;
    IPropertyTree *current;
    StringAttr xpath, stackPath;
    unsigned stacklen;
    unsigned stackmax;
    StackElement *stack;
};

class CPTreeMaker : public CInterface, implements IPTreeMaker
{
    IPropertyTree *root;
    ICopyArrayOf<IPropertyTree> ptreeStack;
    bool rootProvided, noRoot;
    IPTreeNodeCreator *nodeCreator;
    class CDefaultNodeCreator : public CInterface, implements IPTreeNodeCreator
    {
        byte flags;
    public:
        IMPLEMENT_IINTERFACE;

        CDefaultNodeCreator(byte _flags) : flags(_flags) { }

        virtual IPropertyTree *create(const char *tag) { return createPTree(tag, flags); }
    };
protected:
    IPropertyTree *currentNode;
public:
    IMPLEMENT_IINTERFACE;

    CPTreeMaker(byte flags=ipt_none, IPTreeNodeCreator *_nodeCreator=NULL, IPropertyTree *_root=NULL, bool _noRoot=false) : noRoot(_noRoot)
    {
        if (_nodeCreator)
            nodeCreator = LINK(_nodeCreator);
        else
            nodeCreator = new CDefaultNodeCreator(flags);
        if (_root)
        { 
            root = LINK(_root);
            rootProvided = true;
        }
        else
        {
            root = NULL;    
            rootProvided = false;
        }
        reset();
    }
    ~CPTreeMaker()
    {
        ::Release(nodeCreator);
        ::Release(root);
    }

// IPTreeMaker
    virtual void beginNode(const char *tag, offset_t startOffset)
    {
        if (rootProvided)
        {
            currentNode = root;
            rootProvided = false;
        }
        else
        {
            IPropertyTree *parent = currentNode;
            if (!root)
            {
                currentNode = nodeCreator->create(tag);
                root = currentNode;
            }
            else
                currentNode = nodeCreator->create(NULL);
            if (parent)
                parent->addPropTree(tag, currentNode);
            else if (noRoot)
                root->addPropTree(tag, currentNode);
        }
        ptreeStack.append(*currentNode);
    }
    virtual void newAttribute(const char *name, const char *value)
    {
        currentNode->setProp(name, value);
    }
    virtual void beginNodeContent(const char *name) { }
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
    {
        if (binary)
            currentNode->setPropBin(NULL, length, value);
        else
            currentNode->setProp(NULL, (const char *)value);
        unsigned c = ptreeStack.ordinality();
        if (c==1 && !noRoot && currentNode != root) 
            ::Release(currentNode);
        ptreeStack.pop();
        currentNode = (c>1) ? &ptreeStack.tos() : NULL;
    }
    virtual IPropertyTree *queryRoot() { return root; }
    virtual IPropertyTree *queryCurrentNode() { return currentNode; }
    virtual void reset()
    {
        if (!rootProvided)
        {
            ::Release(root);
            if (noRoot)
                root = nodeCreator->create("__NoRoot__");
            else
                root = NULL;
        }
        currentNode = NULL;
    }
    virtual IPropertyTree *create(const char *tag)
    {
        return nodeCreator->create(tag);
    }
};

#ifdef __64BIT__
#pragma pack(pop)   
#endif


#endif
