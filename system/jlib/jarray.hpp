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



#ifndef JARRAY_HPP
#define JARRAY_HPP



#include "platform.h"
#include "jiface.hpp"

typedef size32_t aindex_t;

#define NotFound   (aindex_t)-1

/************************************************************************
 *                            Copy Lists                                *
 ************************************************************************/

extern "C"
{
typedef int (* StdCompare)(const void *_e1, const void *_e2);
}

/*
 * Start of definition:
 */

class jlib_decl Allocator
{
public:
    void  kill();
    inline bool isItem(aindex_t pos = 0) const    { return pos < used; }      /* Is there an item at pos */
    inline aindex_t length() const                 { return used; } /* Return number of items  */
    inline aindex_t ordinality() const             { return used; } /* Return number of items  */
    inline bool empty() const                    { return (used==0); }

protected:
    void * _doBAdd(void *, size32_t size, StdCompare compare, bool & isNew);
    void * _doBSearch(const void *, size32_t size, StdCompare compare) const;
    void _doSort(size32_t size, StdCompare compare);
    void _ensure(aindex_t, size32_t);
    inline void _init() { max=0; _head=0; used=0; }                                 // inline because it is called a lot
    void _reallocate(aindex_t newlen, size32_t iSize);
    void _doRotateR(aindex_t pos1, aindex_t pos2, size32_t iSize, aindex_t num);
    void _doRotateL(aindex_t pos1, aindex_t pos2, size32_t iSize, aindex_t num);
    void _space(size32_t iSize);
    void _doSwap(aindex_t pos1, aindex_t pos2, size32_t iSize);
    void _doTransfer(aindex_t to, aindex_t from, size32_t iSize);
    void doSwapWith(Allocator & other);

protected:
    void * _head;
    aindex_t used;
    aindex_t max;
};


template <unsigned SIZE> class AllocatorOf : public Allocator
{
public:
    inline void ensure(aindex_t num)                        { _ensure(num, SIZE); }
    inline void rotateR(aindex_t pos1, aindex_t pos2)        { _doRotateR(pos1, pos2, SIZE, 1); }
    inline void rotateL(aindex_t pos1, aindex_t pos2)        { _doRotateL(pos1, pos2, SIZE, 1); }
    inline void rotateRN(aindex_t pos1, aindex_t pos2, aindex_t num)        { _doRotateR(pos1, pos2, SIZE, num); }
    inline void rotateLN(aindex_t pos1, aindex_t pos2, aindex_t num)        { _doRotateL(pos1, pos2, SIZE, num); }
    inline void swap(aindex_t pos1, aindex_t pos2)           { _doSwap(pos1, pos2, SIZE); }
    inline void transfer(aindex_t to, aindex_t from)         { _doTransfer(to, from, SIZE); }

protected:
    inline void _space(void)                                { Allocator::_space(SIZE); }
    inline void _move(aindex_t pos1, aindex_t pos2, aindex_t num) { memmove((char *)_head + pos1 * SIZE, (char *)_head + pos2 * SIZE, num * SIZE); }
};

/* inheritance structure:

  BaseArrayOf inherits from AllocatorOf
  CopyArrayOf (non-owning ref array) inherits from BaseArrayOf
  OwningArrayOf inherits from BaseArrayOf
  ArrayOf (owning ref array) inherits from OwningArrayOf
  PtrArrayOf (owning ptr array) inherits from OwningArrayOf

  the difference between the latter two is that they call different functions to convert MEMBER to PARAM (Array__Member2Param vs Array__Member2ParamPtr)
  this is necessary because of the use of global functions, to allow two arrays with the
  same MEMBER but different PARAMs (otherwise these functions would have the same prototype, because their arguments only involve MEMBER and not PARAM)

 */

template <class MEMBER, class PARAM>
class BaseArrayOf : public AllocatorOf<sizeof(MEMBER)>
{
    typedef AllocatorOf<sizeof(MEMBER)> PARENT;
    typedef BaseArrayOf<MEMBER,PARAM> SELF;

protected:
    typedef int (*CompareFunc)(MEMBER *, MEMBER *);

public:
    inline bool contains(PARAM search) const          { return find(search) != NotFound; }

    void add(PARAM, aindex_t pos);              /* Adds at pos             */
    void append(PARAM);                         /* Adds to end of array    */
    bool appendUniq(PARAM);                     /* Adds to end of array if not already in array - returns true if added*/
    aindex_t bAdd(MEMBER & newItem, CompareFunc, bool & isNew);
    aindex_t bSearch(const MEMBER & key, CompareFunc) const;
    aindex_t find(PARAM) const;
    MEMBER *getArray(aindex_t = 0) const;
    void sort(CompareFunc);
    void swap(aindex_t pos1, aindex_t pos2);
    void swapWith(SELF & other) { this->doSwapWith(other); }
};

template <class MEMBER, class PARAM>
class CopyArrayOf : public BaseArrayOf<MEMBER, PARAM>
{
    typedef BaseArrayOf<MEMBER, PARAM> PARENT;
    typedef CopyArrayOf<MEMBER, PARAM> SELF;
    
public:
    CopyArrayOf() { SELF::_init(); }
    ~CopyArrayOf();
    
    inline PARAM item(aindex_t pos) const;
    PARAM tos(void) const;
    PARAM tos(aindex_t) const;

    PARAM pop(void);
    void popn(aindex_t);
    void popAll(void);
    void remove(aindex_t pos);                  /* Remove (delete) item at pos */
    void removen(aindex_t pos, aindex_t num);   /* Remove (delete) item at pos */
    void replace(PARAM, aindex_t pos);          /* Replace an item at pos */
    void trunc(aindex_t);
    bool zap(PARAM);
};

template <class MEMBER, class PARAM>
class OwningArrayOf : public BaseArrayOf<MEMBER, PARAM>
{
    typedef BaseArrayOf<MEMBER, PARAM> PARENT;
    typedef OwningArrayOf<MEMBER,PARAM> SELF;
    
public:
    OwningArrayOf() { SELF::_init(); }
    ~OwningArrayOf();
    
    void kill(bool nodel = false);                   /* Remove all items        */
    void pop(bool nodel = false);
    void popn(aindex_t,bool nodel = false);
    void popAll(bool nodel = false);
    void replace(PARAM, aindex_t pos, bool nodel = false); /* Replace an item at pos */
    void remove(aindex_t pos, bool nodel = false);    /* Remove (delete) item at pos */
    void removen(aindex_t pos, aindex_t num, bool nodel = false);    /* Remove N items (delete) item at pos */
    void trunc(aindex_t pos, bool nodel = false);
    bool zap(PARAM, bool nodel = false);
};

template <class MEMBER, class PARAM>
class ArrayOf : public OwningArrayOf<MEMBER, PARAM>
{
    typedef OwningArrayOf<MEMBER, PARAM> PARENT;
    typedef ArrayOf<MEMBER,PARAM> SELF;

public:
    inline PARAM item(aindex_t pos) const; 
    PARAM popGet();
    PARAM tos(void) const;
    PARAM tos(aindex_t) const;
};

template <class MEMBER, class PARAM>
class PtrArrayOf : public OwningArrayOf<MEMBER, PARAM>
{
    typedef OwningArrayOf<MEMBER, PARAM> PARENT;
    typedef PtrArrayOf<MEMBER,PARAM> SELF;
    
public:
    inline PARAM item(aindex_t pos) const             { assertex(SELF::isItem(pos)); return Array__Member2ParamPtr(((MEMBER *)AllocatorOf<sizeof(MEMBER)>::_head)[pos]);}
    PARAM popGet();
    PARAM tos(void) const;
    PARAM tos(aindex_t) const;
};

template <class ARRAY, class PARAM> class ArrayIteratorOf
{
public:
      ArrayIteratorOf(ARRAY & _array) : array(_array) { cur = 0; }
      
      inline bool         first(void)             { cur = 0; return isValid(); }
      inline bool         isValid(void)           { return array.isItem(cur); }
      inline PARAM        query()                 { assertex(isValid()); return array.item(cur); }
      inline bool         hasNext(void)           { return array.isItem(cur+1); }
      inline bool         hasPrev(void)           { return array.isItem(cur-1); }
      inline bool         last(void)              { cur = array.ordinality()-1; return isValid(); }
      inline bool         next(void)              { ++cur; return isValid(); }
      inline bool         prev(void)              { --cur; return isValid(); }
      inline bool         select(aindex_t seek)    { cur = seek; return isValid(); }
      
private:
      ARRAY &             array;
      aindex_t             cur;
      
};



#define ForEachItem(x)      aindex_t numItems##x = ordinality();        \
                            aindex_t x = 0;             \
                            for (; x< numItems##x; ++x)
#define ForEachItemRev(x)   aindex_t x = ordinality();     \
                            for (;x--;)
#define ForEachItemIn(x,y)  aindex_t numItems##x = (y).ordinality();     \
                            aindex_t x = 0;             \
                            for (; x< numItems##x; ++x)
#define ForEachItemInRev(x,y)  aindex_t x = (y).ordinality();     \
                               for (;x--;)

#define CloneArray(TGT, SRC)                                 \
  { ForEachItemIn(idx##__LINE__, SRC)                        \
      (TGT).append((SRC).item(idx##__LINE__)); }

#define MAKEArrayOf(member, param, array)       class array : public ArrayOf<member, param> {};
#define MAKECopyArrayOf(member, param, array)   class array : public CopyArrayOf<member, param> {};
#define MAKEPtrArrayOf(member, param, array)    class array : public PtrArrayOf<member, param> {};

#define MAKEValueArray(simple, array)                                       \
inline simple Array__Member2Param(simple &src)              { return src; }  \
inline void Array__Assign(simple & dest, simple const & src){ dest = src; }  \
inline bool Array__Equal(simple const & m, simple const p)  { return m==p; } \
MAKECopyArrayOf(simple, simple, array)

#define MAKEPointerArray(simple, array)                                     \
inline simple & Array__Member2Param(simple *&src)              { return *src; }  \
inline void Array__Assign(simple * & dest, simple & src)       { dest = &src; }  \
inline bool Array__Equal(simple * const & m, simple const & p) { return m==&p; } \
MAKECopyArrayOf(simple *, simple &, array)

/* Documentation on the macros:

 MAKEValueArray(simple-type, array-name)
   Declare an array of a built in, or simple type.  It takes care of defining
   all the inline functions required. e.g. MAKEValueArray(unsigned, unsignedArray)

 MAKE[Copy]ArrayOf(member, param, array-name) 
   Declares an array class of members, where param is used as the parameter
   that is passed into and returned from the access functions.  Including Copy
   in the name means that no action is taken when the items are destroyed.

*/

#endif

