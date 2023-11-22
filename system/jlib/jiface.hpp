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



#ifndef JIFACE_HPP
#define JIFACE_HPP


#include "platform.h"
#include <string.h>
#include <atomic>
#include "jscm.hpp"
#include "jatomic.hpp"

__declspec(noreturn) void jlib_decl raiseAssertException(const char *assertion, const char *file, unsigned line) __attribute__((noreturn));
__declspec(noreturn) void jlib_decl raiseAssertCore(const char *assertion, const char *file, unsigned line) __attribute__((noreturn));

#undef assert
#undef assertex

#if defined(_DEBUG)||defined(_TESTING)
 #define assertex(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertException(#p, __FILE__, __LINE__))))
 #define verifyex(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertException(#p, __FILE__, __LINE__))))
#else
 #define assertex(p)
 #define verifyex(p) ((void) (p))
#endif

#ifdef _DEBUG
 #define dbgassertex(x) assertex(x)   //Use for asserts that are highly unlikely to occur, and would likely to be reproduced in debug mode.
 #define assert(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertCore(#p, __FILE__, __LINE__))))
 #define verify(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertCore(#p, __FILE__, __LINE__))))
#else
 #define dbgassertex(x)
 #define assert(p)
 #define verify(p) ((void) (p))
#endif

#define DEAD_PSEUDO_COUNT               0x3fffffff

class CEmptyClass
{
};

template <class INTERFACE>
class CInterfaceOf;

//The simplest implementation of IInterface.  Can be used in situations where speed is critical, and
//there will never be a need for the equivalent of beforeDispose().
//It is generally recommended to use CInterface for general classes since derived classes may need beforeDispose().
template <class INTERFACE>
class CSimpleInterfaceOf : public INTERFACE
{
    friend class CInterfaceOf<INTERFACE>;    // want to keep xxcount private outside this pair of classes
public:
    inline virtual ~CSimpleInterfaceOf() { }

    inline CSimpleInterfaceOf() : xxcount(1) { }
    inline bool IsShared(void) const    { return xxcount.load(std::memory_order_relaxed) > 1; }
    inline int getLinkCount(void) const { return xxcount.load(std::memory_order_relaxed); }

    inline void Link() const { xxcount.fetch_add(1,std::memory_order_relaxed); }

    inline bool Release(void) const
    {
        if (xxcount.fetch_sub(1,std::memory_order_release) == 1)
        {
            std::atomic_thread_fence(std::memory_order_acquire);  // Because Herb says so.
            delete this;
            return true;
        }
        return false;
    }

private:
    CSimpleInterfaceOf(const CSimpleInterfaceOf<INTERFACE>&) = delete;
    CSimpleInterfaceOf(CSimpleInterfaceOf<INTERFACE> &&) = delete;
    CSimpleInterfaceOf<INTERFACE> & operator = (const CSimpleInterfaceOf<INTERFACE> &) = delete;
    mutable std::atomic<unsigned> xxcount;
};

#ifdef _WIN32
#pragma warning(push)
#pragma warning(disable : 4251)
#endif
template class CSimpleInterfaceOf<CEmptyClass>;
class jlib_decl CSimpleInterface : public CSimpleInterfaceOf<CEmptyClass>
{
public:
    bool Release() const;   // Prevent Release() being inlined everywhere it is called
};
#ifdef _WIN32
#pragma warning(pop)
#endif

// A more general implementation of IInterface that includes a virtual function beforeDispose().
// beforeDispose() allows an a fully constructed object to be cleaned up (which means that virtual
// function calls still work (unlike a virtual destructor).
// It makes it possible to implement a cache which doesn't link count the object, but is cleared when
// the object is disposed without a critical section in Release().  (See pattern details below).
template <class INTERFACE>
class CInterfaceOf : public CSimpleInterfaceOf<INTERFACE>
{
public:
    virtual void beforeDispose() {}

    //Function to be called when checking if an entry in a non-linking cache is valid
    //must be called inside a critical section that protects access to the cache.
    //This function must only increment the link count if the object is valid - otherwise it
    //ends up being freed more than once.
    inline bool isAliveAndLink() const
    {
        unsigned expected = this->xxcount.load(std::memory_order_acquire);
        for (;;)
        {
            //If count is 0, or count >= DEAD_PSEUDO_COUNT then return false - combine both into a single test
            if ((expected-1) >= (DEAD_PSEUDO_COUNT-1))
                return false;

            //Avoid incrementing the link count if xxcount=0, otherwise it introduces a race condition
            //so use compare and exchange to increment it only if it hasn't changed
            if (this->xxcount.compare_exchange_weak(expected, expected+1, std::memory_order_acq_rel))
                return true;
        }
    }

    inline bool Release(void) const
    {
        if (this->xxcount.fetch_sub(1,std::memory_order_release) == 1)
        {
            //Overwrite with a special value so that any calls to Link()/Release within beforeDispose()
            //do not cause the object to be released again.
            this->xxcount.store(DEAD_PSEUDO_COUNT, std::memory_order_release);
            try
            {
                const_cast<CInterfaceOf<INTERFACE> *>(this)->beforeDispose();
            }
            catch (...)
            {
#if _DEBUG
                assert(!"ERROR - Exception in beforeDispose - object will be leaked");
#endif
                throw;
            }
            delete this;
            return true;
        }
        return false;
    }

};

class jlib_decl CInterface : public CInterfaceOf<CEmptyClass>
{
public:
    bool Release() const;   // Prevent Release() being inlined everwhere it is called
};

//---------------------------------------------------------------------------------------------------------------------

// An implementation of IInterface for objects that are never shared.  Only likely to be used in a few situations.
// It still allows use with IArrayOf and Owned classes etc.
template <class INTERFACE>
class CUnsharedInterfaceOf : public INTERFACE
{
public:
    inline virtual ~CUnsharedInterfaceOf() {}

    inline CUnsharedInterfaceOf()         { }
    inline bool IsShared(void) const    { return false; }
    inline int getLinkCount(void) const { return 1; }

    inline void Link() const            { assert(!"Unshared::Link"); }

    inline bool Release(void) const
    {
        delete this;
        return true;
    }
};

//- Template variants -------------------------------------------------------------------------------------------------

template <class INTERFACE>
class CSingleThreadInterfaceOf;

//An equivalent to CSimpleInterfaceOf that is not thread safe - but avoids the cost of atomic operations.
template <class INTERFACE>
class CSingleThreadSimpleInterfaceOf : public INTERFACE
{
    friend class CSingleThreadInterfaceOf<INTERFACE>;    // want to keep xxcount private outside this pair of classes
public:
    inline virtual ~CSingleThreadSimpleInterfaceOf() {}

    inline CSingleThreadSimpleInterfaceOf() { xxcount = 1; }
    inline bool IsShared(void) const    { return xxcount > 1; }
    inline int getLinkCount(void) const { return xxcount; }

    inline void Link() const            { ++xxcount; }

    inline bool Release(void) const
    {
        if (--xxcount == 0)
        {
            delete this;
            return true;
        }
        return false;
    }

private:
    mutable unsigned xxcount;
};

//An equivalent to CInterfaceOf that is not thread safe - but avoids the cost of atomic operations.
template <class INTERFACE>
class CSingleThreadInterfaceOf : public CSingleThreadSimpleInterfaceOf<INTERFACE>
{
public:
    virtual void beforeDispose() {}

    inline bool isAliveAndLink() const
    {
        this->Link();
        return true; // single threaded - must be valid if we have a pointer to it.
    }

    inline bool Release(void) const
    {
        if (--this->xxcount == 0)
        {
            //Because beforeDispose could cause this object to be linked/released or call isAlive(), xxcount is set
            //to a a high mid-point positive number to avoid poss. of releasing again.
            this->xxcount = DEAD_PSEUDO_COUNT;
            const_cast<CSingleThreadInterfaceOf<INTERFACE> *>(this)->beforeDispose();
            delete this;
            return true;
        }
        return false;
    }
};


/***
A pattern for implementing a non-linking cached to a shared object.  Hash table variants are a slightly more complex variant.
static CLASS * cache;
static CriticalSection * cs;

CLASS * getCached()
{
    CriticalBlock block(*cs);
    if (cache)
    {
        cache->Link();  // Must link before call to isAlive() to avoid race condition
        if (cache->isAlive())
            return cache;
        //No need for a matching Release since the object is dead and about to be disposed
    }
    //Create an item here if semantics require it. else return NULL;
    cache = new CLASS;
    return cache;
}

CLASS::beforeDispose()
{
    {
        CriticalBlock block(*cs);
        if (cache == this)
            cache = NULL;
    }
    BASECLASS::beforeDispose(); 
}
****/

typedef class CInterface * CInterfacePtr;

// Two versions of this macro - with and without override
// until such time as we have completely switched over to using override consistently
// otherwise it's hard to use the override consistency check error effectively.

#define IMPLEMENT_IINTERFACE_USING(x)                                                   \
    virtual void Link(void) const       { x::Link(); }                     \
    virtual bool Release(void) const    { return x::Release(); }

#define IMPLEMENT_IINTERFACE    IMPLEMENT_IINTERFACE_USING(CInterface)

#define IMPLEMENT_IINTERFACE_O_USING(x)                                                   \
    virtual void Link(void) const override { x::Link(); }                     \
    virtual bool Release(void) const override    { return x::Release(); }

#define IMPLEMENT_IINTERFACE_O    IMPLEMENT_IINTERFACE_O_USING(CInterface)

#endif
