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



#ifndef JHASH_HPP
#define JHASH_HPP

#include "platform.h"
#include <stdio.h>
#include "jiface.hpp"
#include "jobserve.hpp"
#include "jiter.hpp"
#include "jsuperhash.hpp"

#ifndef IHASH_DEFINED       // may be defined already elsewhere
#define IHASH_DEFINED
interface IHash
{
    virtual unsigned hash(const void *data)=0;
 protected:
    virtual ~IHash() {}
};
#endif

interface jlib_decl IMapping : extends IObservable
{
 public:
    virtual const void * getKey() const = 0;
    virtual unsigned     getHash() const = 0;
    virtual void         setHash(unsigned) = 0;
};

interface jlib_decl IAtom : extends IMapping
{
 public:
    virtual const char * queryStr() const = 0;
};
inline jlib_decl const char * str(const IAtom * atom) { return atom ? atom->queryStr() : NULL; }

//This interface represents an atom which preserves its case, but also stores a lower case representation
//for efficient case insensitive comparison.
//It is deliberately NOT derived from IAtom to avoid accidentally using the wrong interface
interface jlib_decl IIdAtom : extends IMapping
{
 public:
    virtual const char * queryStr() const = 0;
 public:
    virtual IAtom * queryLower() const = 0;
};
inline jlib_decl const char * str(const IIdAtom * atom) { return atom ? atom->queryStr() : NULL; }
inline jlib_decl IAtom * lower(const IIdAtom * atom) { return atom ? atom->queryLower() : NULL; }

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning( disable : 4275 )
#endif

class jlib_decl HashTable
    : public SuperHashTableOf<IMapping, const void>, implements IObserver
{
  public:
    HashTable(int _keysize, bool _ignorecase)
        : SuperHashTableOf<IMapping, const void>(),
        keysize(_keysize), ignorecase(_ignorecase) {}
    HashTable(unsigned initsize, int _keysize, bool _ignorecase)
        : SuperHashTableOf<IMapping, const void>(initsize),
        keysize(_keysize), ignorecase(_ignorecase) {}
    ~HashTable() { releaseAll(); }

    IMPLEMENT_IINTERFACE

    bool add(IMapping & donor)
      {
          donor.setHash(hash(donor.getKey(), keysize));
          return SuperHashTableOf<IMapping, const void>::add(donor);
      }
    bool replace(IMapping & donor)
      {
          donor.setHash(hash(donor.getKey(), keysize));
          return SuperHashTableOf<IMapping, const void>::replace(donor);
      }
    bool addOwn(IMapping & donor);
    bool replaceOwn(IMapping & donor);
    IMapping * findLink(const void *key) const;
    virtual bool onNotify(INotification & notify);
    IMapping * createLink(const void *key);

  protected:
    virtual IMapping *  newMapping(const void *key);

  private:
    unsigned   getHashFromElement(const void * et) const
      { return static_cast<const IMapping *>(et)->getHash(); }
    unsigned   getHashFromFindParam(const void *fp) const
      { return hash(fp, keysize); }
    const void * getFindParam(const void * et) const
      { return const_cast<const void *>(static_cast<const IMapping *>(et)->getKey()); }
    bool       matchesFindParam(const void * et, const void * key, unsigned fphash __attribute__((unused))) const
      { return keyeq(key, static_cast<const IMapping *>(et)->getKey(), keysize); }
    bool       keyeq(const void *key1, const void *key2, int ksize) const;
    unsigned   hash(const void *key, int ksize) const;

  private:
    int        keysize;
    bool       ignorecase;
};

class jlib_decl KeptHashTable : public HashTable
{
public:
    KeptHashTable(int _keysize, bool _ignorecase)
        : HashTable(_keysize, _ignorecase) {}
    KeptHashTable(unsigned _initsize, int _keysize, bool _ignorecase)
        : HashTable(_initsize, _keysize, _ignorecase) {}
    ~KeptHashTable() { releaseAll(); }

    IMapping * create(const void *key);

private:
    void       onAdd(void *et);
    void       onRemove(void *et);
};

class jlib_decl ObservedHashTable : public HashTable
{
public:
    ObservedHashTable(int _keysize, bool _ignorecase)
        : HashTable(_keysize, _ignorecase) {}
    ObservedHashTable(unsigned _initsize, int _keysize, bool _ignorecase)
        : HashTable(_initsize, _keysize, _ignorecase) {}
    ~ObservedHashTable() { releaseAll(); }

private:
    void       onAdd(void *et);
    void       onRemove(void *et);
};

class jlib_decl HashIterator : public SuperHashIteratorOf<IMapping>
{
  public:
    HashIterator(const HashTable & _table) : SuperHashIteratorOf<IMapping>(_table) {}

    IMapping & get() { IMapping & et = query(); et.Link(); return et; }
};

#ifdef _MSC_VER
#pragma warning (pop)
#endif

void IntersectHash(HashTable & h1, HashTable & h2);
void UnionHash(HashTable & h1, HashTable & h2);
void SubtractHash(HashTable & main, HashTable & sub);

#include "jhash.ipp"

typedef MappingStringTo<IInterfacePtr,IInterfacePtr> CopyMappingStringToIInterface;
typedef MapStringTo<IInterfacePtr,IInterfacePtr,CopyMappingStringToIInterface> CopyMapStringToIInterface;

template <class C> class CopyMapStringToMyClass : public CopyMapStringToIInterface
{
  public:
    CopyMapStringToMyClass() : CopyMapStringToIInterface() {};
    CopyMapStringToMyClass(bool _ignorecase) : CopyMapStringToIInterface(_ignorecase) {};
    static inline C * mapToValue(IMapping * _map)
      {
      CopyMappingStringToIInterface * map = (CopyMappingStringToIInterface *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *) *x : NULL;
      }
    C *getValue(const char *k) const
      {
      IInterface **x = CopyMapStringToIInterface::getValue(k);
      return x ? (C *) *x : NULL;
      }
};

template <class C, class BASE> class CopyMapStringToMyClassViaBase : public CopyMapStringToIInterface
{
  public:
    CopyMapStringToMyClassViaBase() : CopyMapStringToIInterface() {};
    CopyMapStringToMyClassViaBase(bool _ignorecase) : CopyMapStringToIInterface(_ignorecase) {};
    C *getValue(const char *k) const
      {
      IInterface **x = CopyMapStringToIInterface::getValue(k);
      return x ? (C *)(BASE *)*x : NULL;
      }
    bool setValue(const char *k, C * v)
      {
      return CopyMapStringToIInterface::setValue(k, (BASE *)v);
      }
};

//===========================================================================

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning( disable : 4275 ) // hope this warning not significant! (may get link errors I guess)
#endif

class jlib_decl MappingStringToIInterface : public MappingStringTo<IInterfacePtr,IInterfacePtr>
{
  public:
    MappingStringToIInterface(const char * k, IInterfacePtr a) :
    MappingStringTo<IInterfacePtr,IInterfacePtr>(k,a) { ::Link(a); }
    ~MappingStringToIInterface() { ::Release(val); }
};

#ifdef _MSC_VER
#pragma warning (pop)
#endif

typedef MapStringTo<IInterfacePtr,IInterfacePtr,MappingStringToIInterface> MapStringToIInterface;

template <class C> class MapStringToMyClass : public MapStringToIInterface
{
  public:
    MapStringToMyClass() : MapStringToIInterface() {};
    MapStringToMyClass(bool _ignorecase) : MapStringToIInterface(_ignorecase) {};
    static inline C * mapToValue(IMapping * _map)
      {
      MappingStringToIInterface * map = (MappingStringToIInterface *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *) *x : NULL;
      }
    C *getValue(const char *k) const
      {
      IInterface **x = MapStringToIInterface::getValue(k);
      return x ? (C *) *x : NULL;
      }
};

template <class C, class BASE> class MapStringToMyClassViaBase : public MapStringToIInterface
{
  public:
    MapStringToMyClassViaBase() : MapStringToIInterface() {};
    MapStringToMyClassViaBase(bool _ignorecase) : MapStringToIInterface(_ignorecase) {};
    C *getValue(const char *k) const
      {
      IInterface **x = MapStringToIInterface::getValue(k);
      return x ? (C *)(BASE *)*x : NULL;
      }
    bool setValue(const char *k, C * v)
      {
      return MapStringToIInterface::setValue(k, (BASE *)v);
      }
};

//===========================================================================

template <class KEY, class KEYINIT>
class CopyMapXToIInterface  : public MapBetween<KEY, KEYINIT, IInterfacePtr,IInterfacePtr,MappingBetween<KEY, KEYINIT, IInterfacePtr, IInterfacePtr> >
{
};

template <class KEY, class KEYINIT, class C> 
class CopyMapXToMyClass : public CopyMapXToIInterface<KEY, KEYINIT>
{
  public:
    CopyMapXToMyClass() : CopyMapXToIInterface<KEY, KEYINIT>() {};
    static inline C * mapToValue(IMapping * _map)
      {
      MappingBetween<KEY, KEYINIT, IInterfacePtr, IInterfacePtr> * map = (MappingBetween<KEY, KEYINIT, IInterfacePtr, IInterfacePtr> *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *) *x : NULL;
      }
    C *getValue(KEYINIT k) const
      {
      IInterface **x = CopyMapXToIInterface<KEY, KEYINIT>::getValue(k);
      return x ? (C *) *x : NULL;
      }
};

template <class KEY, class KEYINIT>
class MappingXToIInterface : public MappingBetween<KEY, KEYINIT, IInterfacePtr,IInterfacePtr>
{
    typedef MappingXToIInterface<KEY, KEYINIT> SELF;
  public:
    MappingXToIInterface(KEYINIT k, IInterfacePtr a) :
    MappingBetween<KEY, KEYINIT, IInterfacePtr,IInterfacePtr>(k,a) { ::Link(a); }
    ~MappingXToIInterface() { ::Release(SELF::val); }
};

template <class KEY, class KEYINIT>
class MapXToIInterface  : public MapBetween<KEY, KEYINIT, IInterfacePtr,IInterfacePtr,MappingXToIInterface<KEY, KEYINIT> >
{
};

template <class KEY, class KEYINIT, class C> 
class MapXToMyClass : public MapXToIInterface<KEY, KEYINIT>
{
  public:
    MapXToMyClass() : MapXToIInterface<KEY, KEYINIT>() {};
    static inline C * mapToValue(IMapping * _map)
      {
      MappingXToIInterface<KEY, KEYINIT> * map = (MappingXToIInterface<KEY, KEYINIT> *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *) *x : NULL;
      }
    C *getValue(KEYINIT k) const
      {
      IInterface **x = MapXToIInterface<KEY, KEYINIT>::getValue(k);
      return x ? (C *) *x : NULL;
      }
};

template <class KEY, class KEYINIT, class C, class BASE> 
class MapXToMyClassViaBase : public MapXToIInterface<KEY, KEYINIT>
{
  public:
    MapXToMyClassViaBase () : MapXToIInterface<KEY, KEYINIT>() {};
    static inline C * mapToValue(IMapping * _map)
      {
      MappingXToIInterface<KEY, KEYINIT> * map = (MappingXToIInterface<KEY, KEYINIT> *)_map;
      IInterface ** x = &map->getValue();
      return x ? (C *)(BASE *)*x : NULL;
      }
    C *getValue(KEYINIT k) const
      {
      IInterface **x = MapXToIInterface<KEY, KEYINIT>::getValue(k);
      return x ? (C *)(BASE *) *x : NULL;
      }
    bool setValue(KEYINIT k, BASE * v)
      {
      return MapXToIInterface<KEY, KEYINIT>::setValue(k, v);
      }
};


//===========================================================================
template <class KEY, class VALUE>
class MappingOwnedToOwned : public MappingBetween<Linked<KEY>, KEY *, Linked<VALUE>,VALUE *>
{
public:
    MappingOwnedToOwned(KEY * k, VALUE * a) : MappingBetween<Linked<KEY>, KEY *, Linked<VALUE>,VALUE *>(k, a) {}
};

template <class KEY, class VALUE>
class MapOwnedToOwned : public MapBetween<Linked<KEY>, KEY * , Linked<VALUE>,VALUE *,MappingOwnedToOwned<KEY, VALUE> >
{
public:
    inline MapOwnedToOwned() {}
    inline MapOwnedToOwned(unsigned initsize) : MapBetween<Linked<KEY>, KEY * , Linked<VALUE>,VALUE *,MappingOwnedToOwned<KEY, VALUE> >(initsize) {}
};


/*
 The hash tables can be used from the interfaces above.  However there are
 some helper classes/macros in jhash.ipp to make them easier to use:

 Element classes:
   HashMapping      - An implementation of IMapping that works in keep and
                      non kept hash tables.
   Atom             - An implementation of IAtom
   MappingKey       - A class for storing a hash key - used by above.

 There are also some classes to make creating hash tables easier:
 (They are actually implemented as macros to stop the compiler choking on
  the template classes).

 a) To create a hash table of a given class use:
    typedef HashTableOf<MAPPING-TYPE, HASH-KEY-SIZE> MapHashTable;

    Using a HashTableOf also adds the following method to the hash table class

    MAPPING-TYPE * CreateLink(const void * key);

    which creates entries in the hash table.

 b) To create a hash table that maps a string to another type use:

    typedef MapStringTo<TO-TYPE, TO-INIT> MyMappingName;
    where
       TO-TYPE - what is stored in the mapping
       TO-INIT - what you pass to the value to initialise it.
    e.g.
    typedef MapStringTo<StringAttr, const char *> MapStrToStr;

 c) To create a hash table that maps a non-string to another type use:

    typedef MapBetween<KEY-TYPE, KEY-INIT, MAP-TYPE, MAP-INIT> myMap;
    where MAP-... as above and
       KEY-TYPE - what is stored as the key
       KEY-INIT - what you pass to the key to initialise it.
    e.g.,
    typedef MapBetween<int, int, StringAttr, const char *> MapIntToStr;

 *** HashTable Key sizes ***
 The HashTable key size can have one of the following forms:

    +n  - The key is n bytes of data
    0   - The key is a null terminated string
    -n  - The key is n bytes of data followed by a null terminated string.
*/

// Create an atom in the global atom table
extern jlib_decl IAtom * createAtom(const char *value);
extern jlib_decl IAtom * createAtom(const char *value, size32_t len);

inline IAtom * createLowerCaseAtom(const char *value) { return createAtom(value); }
inline IAtom * createLowerCaseAtom(const char *value, size32_t len) { return createAtom(value, len); }

extern jlib_decl IIdAtom * createIdAtom(const char *value);
extern jlib_decl IIdAtom * createIdAtom(const char *value, size32_t len);

extern jlib_decl void releaseAtoms();
extern jlib_decl unsigned hashc( const unsigned char *k, unsigned length, unsigned initval);
extern jlib_decl unsigned hashnc( const unsigned char *k, unsigned length, unsigned initval);
extern jlib_decl unsigned hashvalue( unsigned value, unsigned initval);
extern jlib_decl unsigned hashvalue( unsigned __int64 value, unsigned initval);
extern jlib_decl unsigned hashvalue( const void * value, unsigned initval);

//================================================
// Minimal Hash table template - slightly less overhead that HashTable/SuperHashTable

template <class C> class CMinHashTable
{
protected:
    unsigned htn;
    unsigned n;
    C **table;

    void expand(bool expand=true)
    {
        C **t = table+htn; // more interesting going backwards
        if (expand)
            htn = htn*2+1;
        else
            htn /= 2;
        C **newtable = (C **)calloc(sizeof(C *),htn);
        while (t--!=table) {
            C *c = *t;
            if (c) {
                unsigned h = c->hash%htn;
                while (newtable[h]) {
                    h++;
                    if (h==htn)
                        h = 0;
                }
                newtable[h] = c;
            }
        }
        free(table);
        table = newtable;
    }       

public:
    CMinHashTable<C>()
    {
        htn = 7;
        n = 0;
        table = (C **)calloc(sizeof(C *),htn);
    }
    ~CMinHashTable<C>()
    {
        C **t = table+htn; 
        while (t--!=table) 
            if (*t) 
                C::destroy(*t);
        free(table);
    }

    void add(C *c)
    {
        if (n*4>htn*3)
            expand();
        unsigned i = c->hash%htn;
        while (table[i]) 
            if (++i==htn)
                i = 0;
        table[i] = c;
        n++;
    }

    C *findh(const char *key,unsigned h) 
    {
        unsigned i=h%htn;
        while (table[i]) {
            if ((table[i]->hash==h)&&table[i]->eq(key))
                return table[i];
            if (++i==htn)
                i = 0;
        }
        return NULL;
    }

    C *find(const char *key,bool add) 
    {
        unsigned h = C::getHash(key);
        unsigned i=h%htn;
        while (table[i]) {
            if ((table[i]->hash==h)&&table[i]->eq(key))
                return table[i];
            if (++i==htn)
                i = 0;
        }
        if (!add)
            return NULL;
        C *c = C::create(key);
        table[i] = c;
        n++;
        if (n*4>htn*3)
            expand();
        return c;
    }


    void remove(C *c)
    {
        unsigned i = c->hash%htn;
        C::destroy(c);
        while (table[i]!=c) {
            if (table[i]==NULL) {
                return;
            }
            if (++i==htn)
                i = 0;
        }
        unsigned j = i+1;
        loop {
            if (j==htn)
                j = 0;
            C *cn = table[j];
            if (cn==NULL) 
                break;
            unsigned k = cn->hash%htn;
            if (j>i) {
                if ((k<=i)||(k>j)) {
                    table[i] = cn;
                    i = j;
                }
            }
            else if ((k>j)&&(k<=i)) {
                table[i] = cn;
                i = j;
            }
            j++;
        }
        table[i] = NULL;
        n--;
        if ((n>1024)&&(n<htn/4))
            expand(false);
    }

    C *first(unsigned &i)
    {
        i = 0; 
        return next(i);
    }
    C *next(unsigned &i)
    {
        while (i!=htn) {
            C* r = table[i++];
            if (r)
                return r;
        }
        return NULL;
    }

    unsigned ordinality()
    {
        return n;
    }

};


#endif
