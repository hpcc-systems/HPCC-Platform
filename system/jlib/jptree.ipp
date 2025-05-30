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


#ifndef _PTREE_IPP
#define _PTREE_IPP

#include "jarray.hpp"
#include "jexcept.hpp"
#include "jhash.hpp"
#include "jmutex.hpp"
#include "jsuperhash.hpp"
#include "jlzw.hpp"

#include "jptree.hpp"
#include "jbuff.hpp"
#include "jlog.hpp"

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
    virtual void onAdd(void *) override {}
    virtual void onRemove(void *e) override;
    virtual const void *getFindParam(const void *e) const override
    {
        const IPropertyTree &elem = *(const IPropertyTree *)e;
        return elem.queryName();
    }
    virtual unsigned getHashFromElement(const void *e) const override;
    virtual unsigned getHashFromFindParam(const void *fp) const override
    {
        return hashcz_fnv1a((const unsigned char *)fp, fnvInitialHash32);
    }
    virtual bool matchesFindParam(const void *e, const void *fp, unsigned fphash) const override
    {
        return streq(((IPropertyTree *)e)->queryName(), (const char *)fp);
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
        _releaseAll();
    }
    virtual unsigned numChildren() const;
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
    virtual unsigned getHashFromFindParam(const void *fp) const override
    {
        return hashncz_fnv1a((const unsigned char *)fp, fnvInitialHash32);
    }
    virtual bool matchesFindParam(const void *e, const void *fp, unsigned fphash) const override
    {
        return strieq(((IPropertyTree *)e)->queryName(), (const char *)fp);
    }
};


inline static int validJSONUtf8ChrLen(unsigned char c)
{
    if (c <= 31)
        return 0;
    if ('\"' == c)
        return 0;
    if ('\\' == c)
        return 2;
    return utf8CharLen(c);
}

inline static bool isAttribute(const char *xpath) { return (xpath && *xpath == '@'); }

jlib_decl const char *splitXPathUQ(const char *xpath, StringBuffer &path);
jlib_decl const char *queryHead(const char *xpath, StringBuffer &head);
jlib_decl const char *queryNextUnquoted(const char *str, char c);

class CQualifierMap;
interface IPTArrayValue
{
    virtual ~IPTArrayValue() { }

    virtual bool isArray() const = 0;
    virtual CQualifierMap *queryMap() = 0;
    virtual CQualifierMap *setMap(CQualifierMap *_map) = 0;
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
    virtual size32_t queryValueRawSize() const = 0;
    virtual unsigned find(const IPropertyTree *search) const = 0;
    virtual IPropertyTree **getRawArray() const = 0;
    virtual CompressionMethod getCompressionType() const = 0;

    virtual void serialize(MemoryBuffer &tgt) = 0;
    virtual void deserialize(MemoryBuffer &src) = 0;
};

class CPTArray final : implements IPTArrayValue, private IArray
{
    std::atomic<CQualifierMap *> map{nullptr};
public:
    ~CPTArray();
    virtual bool isArray() const override { return true; }
    virtual CQualifierMap *queryMap() override { return map.load(); }
    virtual CQualifierMap *setMap(CQualifierMap *_map) override;
    virtual bool isCompressed() const override { return false; }
    virtual CompressionMethod getCompressionType() const override { return COMPRESS_METHOD_NONE; }
    virtual const void *queryValue() const override { UNIMPLEMENTED; }
    virtual MemoryBuffer &getValue(MemoryBuffer &tgt, bool binary) const override { UNIMPLEMENTED; }
    virtual StringBuffer &getValue(StringBuffer &tgt, bool binary) const override { UNIMPLEMENTED; }
    virtual size32_t queryValueSize() const override { UNIMPLEMENTED; }
    virtual IPropertyTree *queryElement(unsigned idx) const override { return (idx<ordinality()) ? &((IPropertyTree &)item(idx)) : NULL; }
    virtual void addElement(IPropertyTree *tree) override;
    virtual void setElement(unsigned idx, IPropertyTree *tree) override;
    virtual void removeElement(unsigned idx) override;
    virtual unsigned elements() const override { return ordinality(); }
    virtual const void *queryValueRaw() const override { UNIMPLEMENTED; return NULL; }
    virtual size32_t queryValueRawSize() const override { UNIMPLEMENTED; return 0; }
    virtual unsigned find(const IPropertyTree *search) const override;
    virtual IPropertyTree **getRawArray() const override
    {
        return (IPropertyTree **)getArray();
    }
// serializable
    virtual void serialize(MemoryBuffer &tgt) override { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &src) override { UNIMPLEMENTED; }
};


class jlib_decl CPTValue final : implements IPTArrayValue, private MemoryAttr
{
public:
    CPTValue(MemoryBuffer &src)
    {
        deserialize(src);
    }
    explicit CPTValue() = default;   //MORE: Should there be a single shared null instance?
    CPTValue(size32_t size, const void *data) : CPTValue(size, data, false, COMPRESS_METHOD_NONE, COMPRESS_METHOD_DEFAULT)
    {}
    CPTValue(size32_t size, const void *data, bool binary, CompressionMethod currentCompressType, CompressionMethod preferredCompressType);

    virtual bool isArray() const override { return false; }
    virtual CQualifierMap *queryMap() override { return nullptr; }
    virtual CQualifierMap *setMap(CQualifierMap *_map) { UNIMPLEMENTED; }
    virtual bool isCompressed() const override { return compressType != COMPRESS_METHOD_NONE; }
    virtual CompressionMethod getCompressionType() const override { assertex(compressType != COMPRESS_METHOD_LZWLEGACY); return (CompressionMethod)compressType; }
    virtual const void *queryValue() const override;
    virtual MemoryBuffer &getValue(MemoryBuffer &tgt, bool binary) const override;
    virtual StringBuffer &getValue(StringBuffer &tgt, bool binary) const override;
    virtual size32_t queryValueSize() const override;
    virtual IPropertyTree *queryElement(unsigned idx) const override { UNIMPLEMENTED; return NULL; }
    virtual void addElement(IPropertyTree *tree) override { UNIMPLEMENTED; }
    virtual void setElement(unsigned idx, IPropertyTree *tree) override { UNIMPLEMENTED; }
    virtual void removeElement(unsigned idx) override { UNIMPLEMENTED; }
    virtual unsigned elements() const override {  UNIMPLEMENTED; return (unsigned)-1; }
    virtual const void *queryValueRaw() const override { return get(); }
    virtual size32_t queryValueRawSize() const override { return (size32_t)length(); }
    virtual unsigned find(const IPropertyTree *search) const override { throwUnexpected(); }
    virtual IPropertyTree **getRawArray() const override { throwUnexpected(); }


// serializable
    virtual void serialize(MemoryBuffer &tgt) override;
    virtual void deserialize(MemoryBuffer &src) override;

private:
    mutable byte compressType = COMPRESS_METHOD_NONE;
};

#define IptFlagTst(fs, f) (0!=(fs&(f)))
#define IptFlagSet(fs, f) (fs |= (f))
#define IptFlagClr(fs, f) (fs &= (~f))

// NOTE - hairy code alert!
// In order to keep code common between atom and local versions of ptree, we store the atom-specific information BEFORE the this pointer of AttrStr
// (see class AttrStrAtom below).
// This requires some care - in particular must use the right method to destroy the objects, and must not add any virtual methods to either class

//#define TRACE_STRING_SIZE
//#define TRACE_ATOM_SIZE
//#define TRACE_ALL_STRING
//#define TRACE_ALL_ATOM

struct AttrStr
{
    const char *get() const
    {
        return str_DO_NOT_USE_DIRECTLY;
    }
    char str_DO_NOT_USE_DIRECTLY[1];  // Actually [n] - null terminated

    static AttrStr *create(const char *k)
    {
        size32_t kl = k ? strlen(k) : 0;
#ifdef TRACE_ALL_STRING
        DBGLOG("TRACE_ALL_STRING: %s", k);
#endif
#ifdef TRACE_STRING_SIZE
        totsize += kl+1;
        if (totsize > maxsize)
        {
            maxsize.store(totsize);
            DBGLOG("TRACE_STRING_SIZE: total size now %" I64F "d", maxsize.load());
        }
#endif
        AttrStr *ret = (AttrStr *) malloc(kl+1);
        memcpy(ret->str_DO_NOT_USE_DIRECTLY, k, kl);
        ret->str_DO_NOT_USE_DIRECTLY[kl] = 0;
        return ret;
    }
    static inline AttrStr *createNC(const char *k)
    {
        // If we started to use a static hash table for common values, we would probably want to use a different one here for case-insensitive matches
        return create(k);
    }

    static void destroy(AttrStr *a)
    {
#ifdef TRACE_STRING_SIZE
        totsize -= strlen(a->str_DO_NOT_USE_DIRECTLY)+1;
#endif
        free(a);
    }

#ifdef TRACE_STRING_SIZE
    static std::atomic<__int64> totsize;
    static std::atomic<__int64> maxsize;
#endif
};


// In order to keep code common between atom and local versions of ptree, we store the atom-specific information BEFORE the this pointer of AttrStr
// This requires some care - in particular must use the right method to destroy the objects, and must not add any virtual methods to either class
// Note that memory usage is significant as we create literally millions of these objects

typedef unsigned hashfunc( const unsigned char *k, unsigned length, unsigned initval);

struct AttrStrAtom
{
    unsigned hash;
    unsigned short linkcount;
    char str_DO_NOT_USE_DIRECTLY[1];  // Actually N

    static AttrStrAtom *create(const char *k, size32_t kl, hashfunc _hash)
    {
#ifdef TRACE_ALL_ATOM
        DBGLOG("TRACE_ALL_ATOM: %s", k);
#endif
#ifdef TRACE_ATOM_SIZE
        totsize += sizeof(AttrStrAtom)+kl+1;
        if (totsize > maxsize)
        {
            maxsize.store(totsize);
            DBGLOG("TRACE_ATOM_SIZE: total size now %" I64F "d", maxsize.load());
        }
#endif
        AttrStrAtom *ret = (AttrStrAtom *) malloc(offsetof(AttrStrAtom, str_DO_NOT_USE_DIRECTLY)+kl+1);
        memcpy(ret->str_DO_NOT_USE_DIRECTLY, k, kl);
        ret->str_DO_NOT_USE_DIRECTLY[kl] = 0;
        ret->hash = _hash((const unsigned char *) k, kl, 17);
        ret->linkcount = 0;
        return ret;
    }
    static void destroy(AttrStrAtom *a)
    {
#ifdef TRACE_ATOM_SIZE
        totsize -= sizeof(AttrStrAtom)+strlen(a->str_DO_NOT_USE_DIRECTLY)+1;
#endif
        free(a);
    }

    AttrStr *toAttrStr()
    {
        return (AttrStr *) &str_DO_NOT_USE_DIRECTLY;
    }
    static AttrStrAtom *toAtom(AttrStr *a)
    {
        return (AttrStrAtom *)(&a->str_DO_NOT_USE_DIRECTLY - offsetof(AttrStrAtom, str_DO_NOT_USE_DIRECTLY));
    }
#ifdef TRACE_ATOM_SIZE
    static std::atomic<__int64> totsize;
    static std::atomic<__int64> maxsize;
#endif
 };

struct AttrStrC : public AttrStrAtom
{
    static inline unsigned getHash(const char *k)
    {
        return hashcz((const byte *)k, 17);
    }
    inline bool eq(const char *k)
    {
        return streq(k,str_DO_NOT_USE_DIRECTLY);
    }
    static AttrStrC *create(const char *k)
    {
        size32_t kl = k ? strlen(k) : 0;
        return (AttrStrC *) AttrStrAtom::create(k, kl, hashc);
    }
};

struct AttrStrNC : public AttrStrAtom
{
    static inline unsigned getHash(const char *k)
    {
        return hashncz((const byte *)k, 17);
    }
    inline bool eq(const char *k)
    {
        return strieq(k,str_DO_NOT_USE_DIRECTLY);
    }
    static AttrStrNC *create(const char *k)
    {
        size32_t kl = k ? strlen(k) : 0;
        return (AttrStrNC *) AttrStrAtom::create(k, kl, hashnc);
    }
};

typedef CMinHashTable<AttrStrC> RONameTable;

// NOTE - hairy code alert!
// To save on storage (and contention) we store short string values in same slot as the pointer to longer
// ones would occupy. This relies on the assumption that the pointers you want to store are always AT LEAST
// 2-byte aligned. This should be the case on anything coming from malloc on any modern architecture.

#define USE_STRUNION

template<class PTR>
struct PtrStrUnion
{
#ifdef USE_STRUNION
    union
    {
        PTR *ptr;
#if __BYTE_ORDER == __LITTLE_ENDIAN
        struct
        {
            char flag;
            char chars[sizeof(PTR *)-1];
        };
        struct
        {
            char flagx;
            int8_t idx1;
            int16_t idx2;
        };
#else
        struct
        {
            char chars[sizeof(PTR *)-1];
            char flag;
        };
        struct
        {
            int16_t idx2;
            int8_t idx1;
            char flagx;
        };
#endif
    };

    inline PtrStrUnion<PTR>() : ptr(nullptr) {}
    inline bool isPtr() const
    {
        return (flag&1) == 0;
    }
    inline const char *get() const
    {
        if (!isPtr())
        {
            assert(flag==1);
            return chars;
        }
        else if (ptr)
            return ptr->get();
        else
            return nullptr;
    }
    inline void destroy()
    {
        if (isPtr() && ptr)
            PTR::destroy(ptr);
    }
    bool set(const char *key)
    {
        if (key)
        {
            size32_t l = strnlen(key, sizeof(PTR *));  // technically sizeof(PTR)-1 would do, but I suspect 8 bytes is actually more optimal to search than 7
            if (l <= sizeof(PTR *)-2)
            {
                flag=1;
                memmove(chars, key, l);  // Technically, they could overlap
                chars[l]=0;
                return true;
            }
        }
        return false;
    }
    inline void setPtr(PTR *a)
    {
        ptr = a;
        assert(isPtr());
    }
#else
    PTR *ptr = nullptr;
    inline bool isPtr()
    {
        return true;
    }
    inline const char *get()
    {
        if (ptr)
            return ptr->get();
        else
            return nullptr;
    }
    inline void destroy()
    {
        if (ptr)
            PTR::destroy(ptr);
    }
    bool set(const char *key)
    {
        return false;
    }
    inline void setPtr(PTR *a)
    {
        ptr = a;
    }
#endif
    inline PTR *getPtr() const
    {
        return isPtr() ? ptr : nullptr;
    }
};

#ifdef USE_STRUNION
#define USE_READONLY_ATOMTABLE
#endif

inline bool hasPTreeEncodedMarker(const char *name)
{
    return (name && *name=='~');
}

inline const char *skipPTreeEncodedMarker(const char *name)
{
    if (hasPTreeEncodedMarker(name))
        name++;
    return name;
}

typedef PtrStrUnion<AttrStr> AttrStrUnion;
static_assert(sizeof(AttrStrUnion) == sizeof(AttrStr *), "AttrStrUnion size mismatch");  // Sanity check!

#ifdef USE_READONLY_ATOMTABLE
struct AttrStrUnionWithTable : public AttrStrUnion
{
    inline bool isEncoded() const
    {
        if (!isPtr() && flag==3)
            return (hasPTreeEncodedMarker(roNameTable->getIndex(idx2)->str_DO_NOT_USE_DIRECTLY));
        return hasPTreeEncodedMarker(AttrStrUnion::get());
    }
    inline const char *get() const
    {
        if (!isPtr() && flag==3)
            return skipPTreeEncodedMarker(roNameTable->getIndex(idx2)->str_DO_NOT_USE_DIRECTLY);  // Should probably rename this back now!
        return skipPTreeEncodedMarker(AttrStrUnion::get());
    }
    bool set(const char *key)
    {
        if (AttrStrUnion::set(key))
            return true;
        if (key && key[0]=='@')
        {
            unsigned idx = roNameTable->findIndex(key, AttrStrC::getHash(key));
            if (idx != (unsigned) -1)
            {
                assert(idx <= 0xffff);
                flag = 3;
                idx2 = idx;
                return true;
            }
        }
        return false;
    }
    static RONameTable *roNameTable;
};

struct AttrStrUnionWithValueTable : public AttrStrUnion
{
    inline const char *get() const
    {
        if (flag==3) // no point in also checking !isPtr() afaics
            return roValueTable->getIndex(idx2)->str_DO_NOT_USE_DIRECTLY;  // Should probably rename this back now!
        return AttrStrUnion::get();
    }
    bool set(const char *key)
    {
        if (key)
        {
            unsigned idx = roValueTable->findIndex(key, AttrStrC::getHash(key));
            if (idx != (unsigned) -1)
            {
                assert(idx <= 0xffff);
                flag = 3;
                idx2 = idx;
                return true;
            }
#ifdef _TRACE_VALUE_MISSES
            PROGLOG("notfound key = %s", key);
#endif
        }
        return false;
    }
    static RONameTable *roValueTable;
};
#else
typedef AttrStrUnion AttrStrUnionWithTable;
typedef AttrStrUnion AttrStrUnionWithValueTable;

#endif

struct AttrValue
{
    AttrStrUnionWithTable key;
    AttrStrUnionWithValueTable value;
};


struct DeserializeContext
{
    StringBuffer name;
    StringBuffer value;
};


class jlib_decl PTree : public CInterfaceOf<IPropertyTree>
{
friend class SingleIdIterator;
friend class PTLocalIteratorBase;
friend class PTIdMatchIterator;
friend class ChildMap;
friend class PTStackIterator;

public:
    PTree(byte _flags=ipt_none, IPTArrayValue *_value=nullptr, ChildMap *_children=nullptr);
    ~PTree();
    virtual void beforeDispose() override { }

    virtual unsigned queryHash() const = 0;
    IPTArrayValue *queryOwner() { return arrayOwner; }
    unsigned querySiblingPosition()
    {
        if (!arrayOwner)
            return 0;
        return arrayOwner->find(this);
    }
    IPropertyTree *queryChild(unsigned index);
    ChildMap *queryChildren() { return children; }
    aindex_t findChild(IPropertyTree *child, bool remove=false);
    inline bool isnocase() const { return IptFlagTst(flags, ipt_caseInsensitive); }
    ipt_flags queryFlags() const { return (ipt_flags) flags; }
    void serializeCutOff(MemoryBuffer &tgt, int cutoff=-1, int depth=0);
    void deserializeSelf(MemoryBuffer &src, DeserializeContext &deserializeContext);
    void serializeAttributes(MemoryBuffer &tgt);

    void cloneIntoSelf(const IPropertyTree &srcTree, bool sub);     // clone the name and contents of srcTree into "this" tree
    IPropertyTree * clone(const IPropertyTree &srcTree, bool sub);  // create a node (that matches the type of this) and clone the source
    void cloneContents(const IPropertyTree &srcTree, bool sub);

    inline void setOwner(IPTArrayValue *_arrayOwner) { arrayOwner = _arrayOwner; }
    IPropertyTree *queryCreateBranch(IPropertyTree *branch, const char *prop, bool *existing=NULL);
    IPropertyTree *splitBranchProp(const char *xpath, const char *&_prop, bool error=false);
    IPTArrayValue *queryValue() const { return value; }
    CQualifierMap *queryMap() { return value ? value->queryMap() : nullptr; }
    IPTArrayValue *detachValue() { IPTArrayValue *v = value; value = NULL; return v; }
    //This does not need to be virtual, because any remote PTree that the function is called on will already
    //have the connection locked
    void setValue(IPTArrayValue *_value, bool binary)
    {
        delete value;
        value = _value;
        if (binary)
            IptFlagSet(flags, ipt_binary);
        else
            IptFlagClr(flags, ipt_binary);
    }
    bool checkPattern(const char *&xxpath) const;
    IPropertyTree *detach()
    {
        IPropertyTree *tree = create(queryName(), value, children, true);
        PTree *_tree = QUERYINTERFACE(tree, PTree); assertex(_tree); _tree->setOwner(nullptr);

        std::swap(numAttrs, _tree->numAttrs);
        std::swap(attrs, _tree->attrs);

        ::Release(children);
        children = nullptr;
        return tree;
    }
    virtual void createChildMap() { children = isnocase()?new ChildMapNC():new ChildMap(); }
    virtual void setName(const char *name) = 0;
    virtual void serializeSelf(MemoryBuffer &tgt);
    inline void markNameEncoded() { IptFlagSet(flags, ipt_escaped); }
    inline bool isNameEncoded() const { return IptFlagTst(flags, ipt_escaped); }
    inline bool isAttributeNameEncoded(const char *key) const
    {
        AttrValue *a = findAttribute(key);
        if (!a)
            return false;
        return a->key.isEncoded();
    }
    virtual void setAttribute(const char *attr, const char *val, bool encoded) = 0;

// IPropertyTree impl.
    virtual bool hasProp(const char * xpath) const override;
    virtual bool isBinary(const char *xpath=NULL) const override;
    virtual bool isCompressed(const char *xpath=NULL) const override;
    virtual CompressionMethod getCompressionType() const override;
    virtual bool renameProp(const char *xpath, const char *newName) override;
    virtual bool renameTree(IPropertyTree *tree, const char *newName) override;
    virtual const char *queryProp(const char *xpath) const override;
    virtual bool getProp(const char *xpath, StringBuffer &ret) const override;
    virtual void setProp(const char *xpath, const char *val) override;
    virtual void addProp(const char *xpath, const char *val) override;
    virtual void appendProp(const char *xpath, const char *val) override;
    virtual bool getPropBool(const char *xpath, bool dft=false) const override;
    virtual void setPropBool(const char *xpath, bool val) override { setPropInt(xpath, val); }
    virtual void addPropBool(const char *xpath, bool val) override { addPropInt(xpath, val); }
    virtual __int64 getPropInt64(const char *xpath, __int64 dft=0) const override;
    virtual void setPropInt64(const char * xpath, __int64 val) override;
    virtual void addPropInt64(const char *xpath, __int64 val) override;
    virtual void setPropReal(const char * xpath, double val) override;
    virtual void addPropReal(const char *xpath, double val) override;
    virtual int getPropInt(const char *xpath, int dft=0) const override;
    virtual void setPropInt(const char *xpath, int val) override;
    virtual void addPropInt(const char *xpath, int val) override;
    virtual double getPropReal(const char *xpath, double dft=0.0) const override;
    virtual bool getPropBin(const char * xpath, MemoryBuffer &ret) const override;
    virtual void setPropBin(const char * xpath, size32_t size, const void *data, CompressionMethod preferredCompression) override;
    virtual void appendPropBin(const char *xpath, size32_t size, const void *data) override;
    virtual void addPropBin(const char *xpath, size32_t size, const void *data) override;
    virtual IPropertyTree *getPropTree(const char *xpath) const override;
    virtual IPropertyTree *queryPropTree(const char *xpath) const override;
    virtual IPropertyTree *getBranch(const char *xpath) const override { return LINK(queryBranch(xpath)); }
    virtual IPropertyTree *queryBranch(const char *xpath) const override { return queryPropTree(xpath); }
    virtual IPropertyTree *setPropTree(const char *xpath, IPropertyTree *val) override;
    virtual IPropertyTree *addPropTree(const char *xpath, IPropertyTree *val) override;
    virtual IPropertyTree *setPropTree(const char *xpath) override { return setPropTree(xpath, create()); }
    virtual IPropertyTree *addPropTree(const char *xpath) override { return addPropTree(xpath, create()); }
    virtual bool removeTree(IPropertyTree *child) override;
    virtual bool removeProp(const char *xpath) override;
    virtual aindex_t queryChildIndex(IPropertyTree *child) override;
    virtual StringBuffer &getName(StringBuffer &ret) const override;
    virtual IAttributeIterator *getAttributes(bool sorted=false) const override;
    virtual IPropertyTreeIterator *getElements(const char *xpath, IPTIteratorCodes flags = iptiter_null) const override;
    virtual void localizeElements(const char *xpath, bool allTail=false) override;
    virtual bool hasChildren() const override { return children && children->count()?true:false; }
    virtual unsigned numUniq() const override { return checkChildren()?children->count():0; }
    virtual unsigned numChildren() const override;
    virtual bool isCaseInsensitive() const override { return isnocase(); }
    virtual unsigned getCount(const char *xpath) const override;
    virtual IPropertyTree *addPropTreeArrayItem(const char *xpath, IPropertyTree *val) override;
    virtual bool isArray(const char *xpath=NULL) const override;
    virtual unsigned getAttributeCount() const override;

// serializable impl.
    virtual void serialize(MemoryBuffer &tgt) override;
    virtual void deserialize(MemoryBuffer &src) override;


protected:
    aindex_t getChildMatchPos(const char *xpath);

    virtual ChildMap *checkChildren() const;
    virtual bool isEquivalent(IPropertyTree *tree) const { return (nullptr != QUERYINTERFACE(tree, PTree)); }
    virtual void setLocal(size32_t l, const void *data, bool binary, CompressionMethod compressType);
    virtual void appendLocal(size32_t l, const void *data, bool binary=false);
    virtual void addingNewElement(IPropertyTree &child, int pos) { }
    virtual void removingElement(IPropertyTree *tree, unsigned pos) { }
    virtual IPropertyTree *create(const char *name=nullptr, IPTArrayValue *value=nullptr, ChildMap *children=nullptr, bool existing=false) = 0;
    virtual IPropertyTree *create(MemoryBuffer &mb) = 0;
    virtual IPropertyTree *ownPTree(IPropertyTree *tree);

    virtual bool removeAttribute(const char *k) = 0;

    AttrValue *findAttribute(const char *k) const;
    const char *getAttributeValue(const char *k) const;
    AttrValue *getNextAttribute(AttrValue *cur) const;

private:
    void addLocal(size32_t l, const void *data, bool binary=false, int pos=-1);
    void resolveParentChild(const char *xpath, IPropertyTree *&parent, IPropertyTree *&child, StringAttr &path, StringAttr &qualifier);
    void replaceSelf(IPropertyTree *val);
    IPropertyTree *addPropTree(const char *xpath, IPropertyTree *val, bool alwaysUseArray);
    void addPTreeArrayItem(IPropertyTree *peer, const char *xpath, PTree *val, aindex_t pos = (aindex_t) -1);

protected: // data
    /* NB: the order of the members here is important to reduce the size of the objects, because very large numbers of these are created.
     * The base CInterfaceOf contains it's VMT + a unsigned link count.
     * Therefore the short+byte follows the 4 byte link count.
     */

    unsigned short numAttrs = 0;
    byte flags;           // set by constructor
    IPTArrayValue *arrayOwner = nullptr; // ! currently only used if tree embedded into array, used to locate position.
    ChildMap *children;   // set by constructor
    IPTArrayValue *value; // set by constructor
    AttrValue *attrs = nullptr;
};


class CAttrValHashTable
{
    CMinHashTable<AttrStrC>  htc;
    CMinHashTable<AttrStrNC> htnc;
    CMinHashTable<AttrStrC>  htv;
public:
    inline AttrStr *addkey(const char *v,bool nc)
    {
        AttrStrAtom * ret;
        if (nc)
            ret = htnc.find(v,true);
        else
            ret = htc.find(v,true);
        if (ret->linkcount!=(unsigned short)-1)
            ret->linkcount++;
        return ret->toAttrStr();
    }
    inline AttrStr *addval(const char *v)
    {
        AttrStrAtom * ret = htv.find(v,true);
        if (ret->linkcount!=(unsigned short)-1)
            ret->linkcount++;
        return ret->toAttrStr();
    }
    inline void removekey(AttrStr *_a,bool nc)
    {
        AttrStrAtom *a = AttrStrAtom::toAtom(_a);
        if (a->linkcount!=(unsigned short)-1)
        {
            if (--(a->linkcount)==0)
            {
                if (nc)
                    htnc.remove((AttrStrNC *)a);
                else
                    htc.remove((AttrStrC *)a);
            }
        }
    }
    inline void removeval(AttrStr *_a)
    {
        AttrStrAtom *a = AttrStrAtom::toAtom(_a);
        if (a->linkcount!=(unsigned short)-1)
            if (--(a->linkcount)==0)
                htv.remove((AttrStrC *)a);
    }
};


class jlib_decl CAtomPTree : public PTree
{
    AttrValue *newAttrArray(unsigned n);
    void freeAttrArray(AttrValue *a, unsigned n);
    PtrStrUnion<HashKeyElement> name;
protected:
    virtual bool removeAttribute(const char *k) override;
public:
    CAtomPTree(const char *name=nullptr, byte flags=ipt_none, IPTArrayValue *value=nullptr, ChildMap *children=nullptr);
    ~CAtomPTree();
    const char *queryName() const override;
    virtual unsigned queryHash() const override;
    virtual void setName(const char *_name) override;
    virtual void setAttribute(const char *attr, const char *val, bool encoded) override;
    virtual bool isEquivalent(IPropertyTree *tree) const override { return (nullptr != QUERYINTERFACE(tree, CAtomPTree)); }
    virtual IPropertyTree *create(const char *name=nullptr, IPTArrayValue *value=nullptr, ChildMap *children=nullptr, bool existing=false) override
    {
        return new CAtomPTree(name, flags, value, children);
    }
    virtual IPropertyTree *create(MemoryBuffer &mb) override
    {
        IPropertyTree *tree = new CAtomPTree();
        tree->deserialize(mb);
        return tree;
    }
};


jlib_decl IPropertyTree *createPropBranch(IPropertyTree *tree, const char *xpath, bool createIntermediates=false, IPropertyTree **created=NULL, IPropertyTree **createdParent=NULL);

class jlib_decl LocalPTree : public PTree
{
protected:
    virtual bool removeAttribute(const char *k) override;
    AttrStrUnion name;
public:
    LocalPTree(const char *name=nullptr, byte flags=ipt_none, IPTArrayValue *value=nullptr, ChildMap *children=nullptr);
    ~LocalPTree();

    const char *queryName() const override;
    virtual unsigned queryHash() const override
    {
        const char *myname = queryName();
        assert(myname);
        size32_t nl = strlen(myname);
        return isnocase() ? hashnc_fnv1a((const byte *)myname, nl, fnvInitialHash32): hashc_fnv1a((const byte *)myname, nl, fnvInitialHash32);
    }
    virtual void setName(const char *_name) override;
    virtual void setAttribute(const char *attr, const char *val, bool encoded) override;
    virtual bool isEquivalent(IPropertyTree *tree) const override { return (nullptr != QUERYINTERFACE(tree, LocalPTree)); }
    virtual IPropertyTree *create(const char *name=nullptr, IPTArrayValue *value=nullptr, ChildMap *children=nullptr, bool existing=false) override
    {
        return new LocalPTree(name, flags, value, children);
    }
    virtual IPropertyTree *create(MemoryBuffer &mb) override
    {
        IPropertyTree *tree = new LocalPTree();
        tree->deserialize(mb);
        return tree;
    }
};

class SingleIdIterator : public CInterfaceOf<IPropertyTreeIterator>
{
public:
    SingleIdIterator(const PTree &_tree, unsigned pos=1, unsigned _many=(unsigned)-1);
    ~SingleIdIterator();
    void setCurrent(unsigned pos);

// IPropertyTreeIterator
    virtual bool first() override;
    virtual bool next() override;
    virtual bool isValid() override;
    virtual IPropertyTree & query() override { return * current; }

private:
    unsigned many, count, whichNext, start;
    IPropertyTree *current;
    const PTree &tree;
};


class PTLocalIteratorBase : public CInterfaceOf<IPropertyTreeIterator>
{
public:
    PTLocalIteratorBase(const PTree *tree, const char *_id, bool _nocase, bool sort);

    ~PTLocalIteratorBase();
    virtual bool match() = 0;

// IPropertyTreeIterator
    virtual bool first() override;
    virtual bool next() override;
    virtual bool isValid() override;
    virtual IPropertyTree & query() override { return iter->query(); }

protected:
    bool nocase, sort;  // pack with the link count
    IPropertyTreeIterator *baseIter;
    StringAttr id;
private:
    const PTree *tree;
    IPropertyTreeIterator *iter;
    IPropertyTree *current;
    bool _next();
};

class PTIdMatchIterator : public PTLocalIteratorBase
{
public:
    PTIdMatchIterator(const PTree *tree, const char *id, bool nocase, bool sort) : PTLocalIteratorBase(tree, id, nocase, sort) { }

    virtual bool match() override;
};

class StackElement;

class PTStackIterator : public CInterfaceOf<IPropertyTreeIterator>
{
public:
    PTStackIterator(IPropertyTreeIterator *_iter, const char *_xpath);
    ~PTStackIterator();

// IPropertyTreeIterator
    virtual bool first() override;
    virtual bool isValid() override;
    virtual bool next() override;
    virtual IPropertyTree & query() override;

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

class CPTreeMaker : public CInterfaceOf<IPTreeMaker>
{
protected:
    bool rootProvided, noRoot;  // pack into the space following the link count
    IPropertyTree *root;
    ICopyArrayOf<IPropertyTree> ptreeStack;
    IPTreeNodeCreator *nodeCreator;
    class CDefaultNodeCreator : implements IPTreeNodeCreator, public CInterface
    {
        byte flags;
    public:
        IMPLEMENT_IINTERFACE_O;

        CDefaultNodeCreator(byte _flags) : flags(_flags) { }

        virtual IPropertyTree *create(const char *tag) override { return createPTree(tag, flags); }
    };
protected:
    IPropertyTree *currentNode;
public:
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
    virtual void beginNode(const char *tag, bool arrayitem, offset_t startOffset) override
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
            if (!parent && noRoot)
                parent = root;
            if (parent)
            {
                if (arrayitem)
                    parent->addPropTreeArrayItem(tag, currentNode);
                else
                    parent->addPropTree(tag, currentNode);
            }
        }
        ptreeStack.append(*currentNode);
    }
    virtual void newAttribute(const char *name, const char *value) override
    {
        currentNode->setProp(name, value);
    }
    virtual void beginNodeContent(const char *name) override { }
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset) override
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
    virtual IPropertyTree *queryRoot() override { return root; }
    virtual IPropertyTree *queryCurrentNode() override { return currentNode; }
    virtual void reset() override
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
    virtual IPropertyTree *create(const char *tag) override
    {
        return nodeCreator->create(tag);
    }
};

class CPTreeEncodeNamesMaker : public CPTreeMaker
{
public:
    CPTreeEncodeNamesMaker(byte flags=ipt_none, IPTreeNodeCreator *_nodeCreator=NULL, IPropertyTree *_root=NULL, bool _noRoot=false) : CPTreeMaker(flags, _nodeCreator, _root, _noRoot)
    {
    }
    ~CPTreeEncodeNamesMaker()
    {
    }

// IPTreeMaker
    virtual void beginNode(const char *tag, bool arrayitem, offset_t startOffset) override
    {
        if (rootProvided)
        {
            currentNode = root;
            rootProvided = false;
        }
        else
        {
            //Having to encode is the uncommon case.  Keep overhead low by only using a StringAttr here. If we do have to encode, take the hit then and use an adapter (StringAttrBuilder).
            StringAttr encoded;
            const char *startEncoding = findFirstInvalidPTreeNameChar(tag);
            if (startEncoding)
            {
                {
                    StringAttrBuilder encodingBuilder(encoded);
                    encodePTreeName(encodingBuilder, tag, startEncoding);
                }
                tag = encoded.str();
            }

            IPropertyTree *parent = currentNode;
            if (!root)
            {
                currentNode = nodeCreator->create(tag);
                root = currentNode;
                if (encoded.length())
                    markPTreeNameEncoded(root);
            }
            else
                currentNode = nodeCreator->create(NULL);
            if (!parent && noRoot)
                parent = root;
            if (parent)
            {
                IPropertyTree *added = nullptr;
                if (arrayitem)
                    added = parent->addPropTreeArrayItem(tag, currentNode);
                else
                    added = parent->addPropTree(tag, currentNode);
                if (encoded.length())
                    markPTreeNameEncoded(added);
            }
        }
        ptreeStack.append(*currentNode);
    }
    virtual void newAttribute(const char *name, const char *value) override
    {
        StringAttr encoded;
        const char *startEncoding = findFirstInvalidPTreeNameChar(name);
        if (startEncoding)
        {
            {
                StringAttrBuilder encodingBuilder(encoded);
                encodingBuilder.append('~'); //used internally for marking the attribute, not used when building xpaths
                encodePTreeName(encodingBuilder, name, startEncoding);
            }
            name = encoded.str();
        }
        setPTreeAttribute(currentNode, name, value, !encoded.isEmpty());
    }
};

#endif
