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



#ifndef JHASH_IPP
#define JHASH_IPP

#include "platform.h"
#include "jiface.hpp"
#include "jobserve.ipp"

#ifdef _WIN32
#pragma warning( push )
#pragma warning( disable : 4275 )
#endif

//NOTE on keysizes for hash tables
//>0 - fixed size key
//=0 - zero terminated key
//<0 - fixed size followed by zero terminated key
class jlib_decl MappingKey
{
  public:
    MappingKey(const void * key, int keysize);
    ~MappingKey()                           { free(key); }

  private:
    MappingKey(void);

  public:                       // Not being too pernickety
    void    *key;               /* points to the key for this element */
};

class jlib_decl MappingBase : public CInterfaceOf<IMapping>
{
  public:
    virtual unsigned            getHash() const;
    virtual void                setHash(unsigned);

protected:
    //Packs into the remainder of the 8byte value from CInterface in 64bit
    unsigned hash;
};

class jlib_decl Mapping : extends MappingBase
{
  public:
    Mapping(const void * k, int keysize) : key(k, keysize) {}

    virtual const void *        getKey() const;

  private:
    MappingKey          key;
};


class jlib_decl AtomBase : public CInterfaceOf<IAtom>
{
public:
    AtomBase(const void * k) : hash(0)
    { 
        key = strdup((const char *)k); 
    }
    ~AtomBase()               { free(key); }

//interface:IMapping
    virtual const char *        queryStr() const { return key; }
    virtual const void *        getKey() const;
    virtual unsigned            getHash() const;
    virtual void                setHash(unsigned);

  protected:
    unsigned hash;
    char * key;
};

class jlib_decl Atom : public AtomBase
{
public:
    Atom(const void * k) : AtomBase(k) {}
};

class jlib_decl ObservedAtom : public AtomBase
{
public:
    ObservedAtom(const void * k) : AtomBase(k) {}

    IMPLEMENT_IOBSERVABLE(AtomBase, observer)

protected:
    SingleObserver observer;
};

class jlib_decl LowerCaseAtom : public Atom
{
  public:
    LowerCaseAtom(const void * k) : Atom(k)
    { 
        for (byte * cur = (byte *)key; *cur; cur++)
            *cur = (byte)tolower(*cur);
    }
};

class jlib_decl CaseAtom : public CInterfaceOf<IIdAtom>
{
public:
    CaseAtom(const void * k);
    ~CaseAtom() { free(text); }

//interface:IMapping
    virtual const char *        queryStr() const { return text; }
    virtual const void *        getKey() const { return text; }
    virtual unsigned            getHash() const { return hash; }
    virtual void                setHash(unsigned _hash) { hash = _hash; }
    virtual IAtom *             queryLower() const { return lower; }

  protected:
    unsigned hash;
    char * text;
    IAtom * lower;
};

template <class KEY, class KEYPARM>
class MappingOf : extends MappingBase
{
  public:
    MappingOf(KEYPARM k) : key(k) { };

    virtual const void * getKey() const { return &key; };

  private:
    KEY key;
};

template <class KEY, class KEYINIT, class VALUE_T, class VALINIT>
class MappingBetween : extends MappingOf<KEY,KEYINIT>
{
  public:
    MappingBetween(KEYINIT k, VALINIT a) :
        MappingOf<KEY,KEYINIT>(k), val(a)       { };

    inline VALUE_T &           getValue()     { return val; };
  protected:
    VALUE_T val;
};


template <class VALUE_T,class VALINIT>
class MappingStringTo : extends Atom
{
   public:
     MappingStringTo(const char *k, VALINIT a) : Atom(k), val(a)
         { };

     inline VALUE_T &          getValue()      { return val; };

   protected:
     VALUE_T    val;
};


template <class T, unsigned int K> class KeptHashTableOf
 : public KeptHashTable
{
  public:
    inline KeptHashTableOf<T,K>(bool _ignorecase) : KeptHashTable(K, _ignorecase) {};
    inline T *create(const void *key) { return (T *) KeptHashTable::create(key); }
    inline T *createLink(const void *key) { return (T *) KeptHashTable::createLink(key); }
    inline T *find(const void *key) const { return (T *) KeptHashTable::find(key); }
    inline T *findLink(const void *key) const { return (T *) KeptHashTable::findLink(key); }
    inline T *next(const IMapping *i) const { return (T *) KeptHashTable::next(i); }

  protected:
    virtual IMapping *newMapping(const void * k) { return new T(k); }
};

class jlib_decl KeptAtomTable : public KeptHashTableOf<Atom, 0U>
{
  public:
    KeptAtomTable() : KeptHashTableOf<Atom, 0U>(false) {};
    KeptAtomTable(bool _ignorecase) : KeptHashTableOf<Atom, 0U>(_ignorecase) {};
    inline IAtom * addAtom(const char *name)
    { 
        return (IAtom *) create(name);
    };
};

class jlib_decl KeptLowerCaseAtomTable : public KeptHashTableOf<LowerCaseAtom, 0U>
{
  public:
    KeptLowerCaseAtomTable() : KeptHashTableOf<LowerCaseAtom, 0U>(true) {};
    inline IAtom * addAtom(const char *name)
    { 
        return (IAtom *) create(name);
    };
};

class jlib_decl KeptCaseAtomTable : public KeptHashTableOf<CaseAtom, 0U>
{
  public:
    KeptCaseAtomTable() : KeptHashTableOf<CaseAtom, 0U>(false) {};
    inline IIdAtom * addAtom(const char *name)
    {
        return create(name);
    };
};

template <class KEY>
class HashKeyOf : public MappingBase
{
  public:
    virtual const void *        getKey() const          { return &key; }

  protected:
     KEY               key;
};


template <class KEY, class MAPPING>
class MapOf : extends KeptHashTable
{
  private:
    bool remove(MAPPING * mem);
  public:
    MapOf() : KeptHashTable(sizeof(KEY), false) {}
    MapOf(unsigned initsize) : KeptHashTable(initsize, sizeof(KEY), false) {}

    ~MapOf()                                            {}

    inline bool add(MAPPING &mem)               { return KeptHashTable::add(mem); }
    inline bool remove(const KEY & key)         { return KeptHashTable::remove(&key); }
    inline bool removeExact(MAPPING * mem)           { return KeptHashTable::removeExact(mem); }
    inline MAPPING * find(const KEY & key) const     { return (MAPPING *)KeptHashTable::find(&key); }
    inline MAPPING * findLink(const KEY & key) const { return (MAPPING *)KeptHashTable::findLink(&key); }
};

template <class MAPPING>
class StringMapOf : extends KeptHashTable
{
  private:
    bool remove(MAPPING * mem);
  public:
    StringMapOf(bool _ignorecase) : KeptHashTable(0, _ignorecase) {}
    StringMapOf(unsigned initsize, bool _ignorecase) : KeptHashTable(initsize, 0, _ignorecase) {}
    ~StringMapOf()                                             {}

    inline bool add(MAPPING &mem)               { return KeptHashTable::add(mem); }
    inline bool remove(const char *key)         { return KeptHashTable::remove(key); }
    inline bool removeExact(MAPPING * mem)           { return KeptHashTable::removeExact(mem); }
    inline MAPPING * find(const char *key) const     { return (MAPPING *)KeptHashTable::find(key); }

};

template <class KEY, class KEYINIT, class VALUE_T, class VALINIT, class MAPPING = MappingBetween<KEY, KEYINIT, VALUE_T, VALINIT> >
class MapBetween : extends MapOf<KEY, MAPPING>
{
    typedef MapBetween<KEY, KEYINIT, VALUE_T, VALINIT, MAPPING> SELF;
  public:
    MapBetween():MapOf<KEY,MAPPING>(){};
    MapBetween(unsigned initsize):MapOf<KEY,MAPPING>(initsize){};
    VALUE_T *   getValue(KEYINIT k) const
    {
       KEY temp(k);
       MAPPING * map = SELF::find(temp);
       if (map)
          return &map->getValue();
       return NULL;
    }
    static inline VALUE_T * mapToValue(IMapping * _map)
    {
       MAPPING * map = (MAPPING *)_map;
       return &map->getValue();
    }

    /* k, v: not linked. v will be linked once. */
    bool        setValue(KEYINIT k, VALINIT v)
    {
       MAPPING * map = new MAPPING(k, v);
       return this->replaceOwn(*map);
    }
};

template <class VALUE_T, class VALINIT = VALUE_T, class MAPPING = MappingStringTo<VALUE_T, VALINIT> >
class MapStringTo : extends StringMapOf<MAPPING>
{
    typedef MapStringTo<VALUE_T, VALINIT, MAPPING> SELF;
  public:
    MapStringTo():StringMapOf<MAPPING>(false){};
    MapStringTo(unsigned initsize, bool _ignorecase):StringMapOf<MAPPING>(initsize, _ignorecase){};
    MapStringTo(bool _ignorecase):StringMapOf<MAPPING>(_ignorecase){};
    VALUE_T *   getValue(const char *k) const
    {
       MAPPING * map = SELF::find(k);
       if (map)
          return &map->getValue();
       return NULL;
    }
    static inline VALUE_T * mapToValue(IMapping * _map)
    {
       MAPPING * map = (MAPPING *)_map;
       return &map->getValue();
    }
    bool        setValue(const char *k, VALINIT v)
    {
       MAPPING * map = new MAPPING(k, v);
       return this->replaceOwn(*map);
    }
};

typedef const char * char_ptr;
typedef MapStringTo<StringAttr, char_ptr> StringAttrMapping;


#ifdef _WIN32
#pragma warning( pop )
#endif


#endif
