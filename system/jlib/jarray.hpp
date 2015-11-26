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



#ifndef JARRAY_HPP
#define JARRAY_HPP

#include <new>

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
    Allocator() { _init(); }
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

//--------------------------------------------------------------------------------------------------------------------

//Ugly - avoid problems with new being #defined in windows debug mode
#undef new

template <class MEMBER, class PARAM = MEMBER>
class SimpleArrayMapper
{
public:
    static void construct(MEMBER & member, PARAM newValue)
    {
        ::new (&member) MEMBER(newValue);
    }
    static inline void destruct(MEMBER & member)
    {
        member.~MEMBER();
    }
    static inline bool matches(const MEMBER & member, PARAM param)
    {
        return member == param;
    }
    static inline PARAM getParameter(const MEMBER & member)
    {
        return (PARAM)member;
    }
};

// An array which stores elements MEMBER, is passed and returns PARAMs
// The MAPPER class is used to map between these two different classes.
template <typename MEMBER, typename PARAM = MEMBER, class MAPPER = SimpleArrayMapper<MEMBER, PARAM> >
class ArrayOf : public AllocatorOf<sizeof(MEMBER)>
{
    typedef AllocatorOf<sizeof(MEMBER)> PARENT;
    typedef ArrayOf<MEMBER,PARAM,MAPPER> SELF;

protected:
    typedef int (*CompareFunc)(const MEMBER *, const MEMBER *);   // Should really be const, as should the original array functions

public:
    ~ArrayOf<MEMBER,PARAM,MAPPER>() { kill(); }

    MEMBER & operator[](size_t pos) { return element((aindex_t)pos); }
    const MEMBER & operator[](size_t pos) const { return element((aindex_t)pos); }

    inline bool contains(PARAM search) const          { return find(search) != NotFound; }

    void add(PARAM newValue, aindex_t pos)
    {
        aindex_t valid_above = SELF::used - pos;
        SELF::_space();
        SELF::_move(pos + 1, pos, valid_above);
        SELF::construct(newValue, pos);
    }
    void append(PARAM newValue)
    {
        SELF::_space();
        SELF::construct(newValue, SELF::used-1);
    }
    bool appendUniq(PARAM newValue)
    {
        if (contains(newValue))
            return false;
        append(newValue);
        return true;
    }
    aindex_t bAdd(MEMBER & newItem, CompareFunc cf, bool & isNew)
    {
        SELF::_space();

        //MORE: This should have a callback function/method that is used to transfer the new element instead of memcpy
        MEMBER * match = (MEMBER *) SELF::_doBAdd(&newItem, sizeof(MEMBER), (StdCompare)cf, isNew);
        if (!isNew)
            SELF::used--;

        MEMBER * head= (MEMBER *)SELF::_head;
        return (aindex_t)(match - head);
    }
    aindex_t bSearch(const MEMBER & key, CompareFunc cf) const
    {
        MEMBER * head= (MEMBER *)SELF::_head;
        MEMBER * match = (MEMBER *) SELF::_doBSearch(&key, sizeof(MEMBER), (StdCompare)cf);
        if (match)
            return (aindex_t)(match - (MEMBER *)head);
        return NotFound;
    }
    aindex_t find(PARAM searchValue) const
    {
        MEMBER * head= (MEMBER *)SELF::_head;
        for (aindex_t pos = 0; pos < SELF::used; ++pos)
        {
            if (MAPPER::matches(head[pos], searchValue))
                return pos;
        }
        return NotFound;
    }
    MEMBER * getArray(aindex_t pos = 0) const
    {
        MEMBER * head= (MEMBER *)SELF::_head;
        assertex(pos <= SELF::used);
        return &head[pos];
    }
    void sort(CompareFunc cf)
    {
        SELF::_doSort(sizeof(MEMBER), (StdCompare)cf);
    }
    void swap(aindex_t pos1, aindex_t pos2)
    {
        if (pos1 != pos2)
        {
            MEMBER * head= (MEMBER *)SELF::_head;
            byte temp[sizeof(MEMBER)];
            memcpy(temp, head + pos1, sizeof(MEMBER));
            memcpy(head + pos1, head + pos2, sizeof(MEMBER));
            memcpy(head + pos2, temp, sizeof(MEMBER));
        }
    }
    void swapWith(SELF & other)
    {
        SELF::doSwapWith(other);
    }
    void clear()
    {
        SELF::used = 0;
    }
    void kill(bool nodestruct = false)
    {
        aindex_t count = SELF::used;
        SELF::used = 0;
        if (!nodestruct)
        {
            for (aindex_t i=0; i<count; i++)
                 SELF::destruct(i);
        }
        PARENT::kill();
    }
    MEMBER & element(aindex_t pos)
    {
        assertex(SELF::isItem(pos));
        MEMBER * head = (MEMBER *)SELF::_head;
        return head[pos];
    }
    const MEMBER & element(aindex_t pos) const
    {
        assertex(SELF::isItem(pos));
        MEMBER * head = (MEMBER *)SELF::_head;
        return head[pos];
    }
    inline PARAM item(aindex_t pos) const
    {
        assertex(SELF::isItem(pos));
        MEMBER * head= (MEMBER *)SELF::_head;
        return MAPPER::getParameter(head[pos]);
    }
    void remove(aindex_t pos, bool nodestruct = false)
    {
        assertex(pos < SELF::used);
        SELF::used --;
        if (!nodestruct) SELF::destruct(pos);
        SELF::_move( pos, pos + 1, ( SELF::used - pos ) );
    }
    void removen(aindex_t pos, aindex_t num, bool nodestruct = false)
    {
        assertex(pos + num <= SELF::used);
        SELF::used -= num;
        if (!nodestruct)
        {
            unsigned idx = 0;
            for (;idx < num; idx++)
                SELF::destruct(pos+idx);
        }
        SELF::_move( pos, pos + num, ( SELF::used - pos ) );
    }
    void pop(bool nodestruct = false)
    {
        assertex(SELF::used);
        --SELF::used;
        if (!nodestruct)
            SELF::destruct(SELF::used);
    }
    PARAM popGet()
    {
        assertex(SELF::used);
        --SELF::used;
        MEMBER * head= (MEMBER *)SELF::_head;
        return MAPPER::getParameter(head[SELF::used]); // used already decremented so element() would assert
    }
    void popn(aindex_t n, bool nodestruct = false)
    {
        assertex(SELF::used>=n);
        while (n--)
        {
            --SELF::used;
            if (!nodestruct)
                SELF::destruct(SELF::used);
        }
    }
    void popAll(bool nodestruct = false)
    {
        if (!nodestruct)
        {
            while (SELF::used)
                SELF::destruct(--SELF::used);
        }
        else
            SELF::used = 0;
    }
    void replace(PARAM newValue, aindex_t pos, bool nodestruct = false)
    {
        if (!nodestruct)
            SELF::destruct(pos);
        SELF::construct(newValue, pos);
    }
    const MEMBER & last(void) const
    {
        return element(SELF::used-1);
    }
    const MEMBER & last(aindex_t n) const
    {
        return element(SELF::used-n-1);
    }
    MEMBER & last(void)
    {
        return element(SELF::used-1);
    }
    MEMBER & last(aindex_t n)
    {
        return element(SELF::used-n-1);
    }
    PARAM tos(void) const
    {
        return MAPPER::getParameter(element(SELF::used-1));
    }
    PARAM tos(aindex_t n) const
    {
        return MAPPER::getParameter(element(SELF::used-n-1));
    }
    void trunc(aindex_t limit, bool nodestruct = false)
    {
        while (limit < SELF::used)
            SELF::pop(nodestruct);
    }
    bool zap(PARAM searchValue, bool nodestruct = false)
    {
        MEMBER * head= (MEMBER *)SELF::_head;
        for (aindex_t pos= 0; pos < SELF::used; ++pos)
        {
            if (MAPPER::matches(head[pos], searchValue))
            {
                remove(pos, nodestruct);
                return true;
            }
        }
        return false;
    }

protected:
    inline void construct(PARAM newValue, unsigned pos)
    {
        MEMBER * head= (MEMBER *)SELF::_head;
        MAPPER::construct(head[pos], newValue);
    }
    inline void destruct(unsigned pos)
    {
        MEMBER * head= (MEMBER *)SELF::_head;
        MAPPER::destruct(head[pos]);
    }
};

//--------------------------------------------------------------------------------------------------------------------

template <typename ITEM>
class OwnedReferenceArrayMapper
{
    typedef ITEM * MEMBER;
    typedef ITEM & PARAM;
public:
    static void construct(MEMBER & member, PARAM newValue)
    {
        member = &newValue;
    }
    static void destruct(MEMBER & member)
    {
        member->Release();
    }
    static inline bool matches(const MEMBER & member, PARAM param)
    {
        return member == &param;
    }
    static inline PARAM getParameter(const MEMBER & member)
    {
        return *member;
    }
};

// An array which stores owned references to elements of type INTERFACE
template <class INTERFACE>
class OwnedReferenceArrayOf : public ArrayOf<INTERFACE *, INTERFACE &, OwnedReferenceArrayMapper<INTERFACE> >
{
};

//--------------------------------------------------------------------------------------------------------------------

template <typename ITEM>
class OwnedPointerArrayMapper : public SimpleArrayMapper<ITEM*>
{
    typedef ITEM * MEMBER;
    typedef ITEM * PARAM;
public:
    static void destruct(MEMBER & member)
    {
        ::Release(member);
    }
};

// An array which stores owned pointers to elements of type INTERFACE
template <typename INTERFACE>
class OwnedPointerArrayOf : public ArrayOf<INTERFACE *, INTERFACE *, OwnedPointerArrayMapper<INTERFACE> >
{
};

//--------------------------------------------------------------------------------------------------------------------

template <typename ITEM>
class SafePointerArrayMapper : public SimpleArrayMapper<ITEM*>
{
    typedef ITEM * MEMBER;
    typedef ITEM * PARAM;
public:
    static void destruct(MEMBER & member)
    {
        delete member;
    }
};

// An array which stores owned pointers to elements of type INTERFACE, and calls delete when they are released.
template <typename INTERFACE>
class SafePointerArrayOf : public ArrayOf<INTERFACE *, INTERFACE *, SafePointerArrayMapper<INTERFACE> >
{
};

//--------------------------------------------------------------------------------------------------------------------

template <class ITEM>
class ReferenceArrayMapper
{
public:
    static void construct(ITEM * & member, ITEM & newValue)
    {
        member = &newValue;
    }
    static inline void destruct(ITEM * & member __attribute__((unused)))
    {
    }
    static inline bool matches(ITEM * const & member, ITEM & param)
    {
        return member == &param;
    }
    static inline ITEM & getParameter(ITEM * const & member)
    {
        return *member;
    }
};

// An array which stores pointers to INTERFACE, but passes and returns references.
template <class INTERFACE>
class CopyReferenceArrayOf : public ArrayOf<INTERFACE *, INTERFACE &, ReferenceArrayMapper<INTERFACE> >
{
};

//--------------------------------------------------------------------------------------------------------------------

template <typename CLASS>
class StructArrayOf : public ArrayOf<CLASS, const CLASS &> { };

//--------------------------------------------------------------------------------------------------------------------


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

/*
    Documentation on the template classes:

    CopyReferenceArrayOf<X>
        An array of references to objects/interfaces of type X.  This array doesn't link count.
    OwnedReferenceArrayOf<X> {};
        An array of references to link counted objects/interfaces of type X.  The array takes ownership of the items
        that are added to it, and will be released when they are removed.
    OwnedPointerArrayOf<X> {};
        An array of pointers to link counted objects/interfaces of type X (which may be NULL).  The array takes
        ownership of the items that are added to it, and will be released when they are removed.
    SafePointerArrayOf<IInterface> {};
        An array of pointers to objects/interfaces of type X (may include NULL).  The array takes ownership of the
        items that are added to it, and will be deleted when they are removed.
    ArrayOf<X> {}
        An array of objects of type X.  This is normally used for simple types e.g., unsigned or pointers
    StructArrayOf<X> {}
        An array of objects of type X.  This is used where X is a class type because items are passed.
        Copy constructors are called when items are added, and destructors when they are removed.

    ArrayOf<MEMBER, PARAM, MAPPING> {}
        The base array implementation which stores elements of type MEMBER, and elements of type PARAM are added.
        The MAPPING class configures how MEMBERs are mapped to PARAMs, and the operations performed when items
        are added or removed from the array.

*/

#endif

