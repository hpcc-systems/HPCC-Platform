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
#include "jscm.hpp"
#include "jatomic.hpp"

void jlib_decl raiseAssertException(const char *assertion, const char *file, unsigned line);
void jlib_decl raiseAssertCore(const char *assertion, const char *file, unsigned line);

#undef assert
#undef assertex

#ifdef _PREFAST_
 #pragma warning (disable : 6244)   // generates too much noise at the moment
 #define assertex(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertException(#p, __FILE__, __LINE__), __analysis_assume(p))))
 #define assert(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertCore(#p, __FILE__, __LINE__), __analysis_assume(p))))
#elif defined(_DEBUG)||defined(_TESTING)
 #define assertex(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertException(#p, __FILE__, __LINE__))))
 #define assert(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertCore(#p, __FILE__, __LINE__))))
#else
 #define assertex(p)
 #define assert(p)
#endif

#if defined(_DEBUG)||defined(_TESTING)
#define verifyex(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertException(#p, __FILE__, __LINE__))))
#define verify(p) (likely(p) ? ((void) 0) : (( (void) raiseAssertCore(#p, __FILE__, __LINE__))))
#else
#define verifyex(p) ((void) (p))
#define verify(p) ((void) (p))
#endif

//Use for asserts that are highly unlikely to occur, and would likely to be reproduced in debug mode.
#ifdef _DEBUG
#define dbgassertex(x) assertex(x)
#else
#define dbgassertex(x)
#endif

#define DEAD_PSEUDO_COUNT               0x3fffffff

//The simplest implementation of IInterface.  Can be used in situations where speed is critical, and
//there will never be a need for the equivalent of beforeDispose().
//It is generally recommended to use CInterface for general classes since derived classes may need beforeDispose().
class jlib_decl CSimpleInterface
{
    friend class CInterface;    // want to keep xxcount private outside this pair of classes
public:
    inline virtual ~CSimpleInterface() {}

    inline CSimpleInterface()           { atomic_set(&xxcount, 1); }
    inline bool IsShared(void) const    { return atomic_read(&xxcount) > 1; }
    inline int getLinkCount(void) const { return atomic_read(&xxcount); }

    inline void Link() const            { atomic_inc(&xxcount); }

    inline bool Release(void) const
    {
        if (atomic_dec_and_test(&xxcount))
        {
            delete this;
            return true;
        }
        return false;
    }

private:
    mutable atomic_t xxcount;
};

// A more general implementation of IInterface that includes a virtual function beforeDispose().
// beforeDispose() allows an a fully constructed object to be cleaned up (which means that virtual
// function calls still work (unlike a virtual destructor).
// It makes it possible to implement a cache which doesn't link count the object, but is cleared when
// the object is disposed without a critical section in Release().  (See pattern details below).
class jlib_decl CInterface : public CSimpleInterface
{
public:
    virtual void beforeDispose();

    inline bool isAlive() const         { return atomic_read(&xxcount) < DEAD_PSEUDO_COUNT; }       //only safe if Link() is called first

    inline bool Release(void) const
    {
        if (atomic_dec_and_test(&xxcount))
        {
            //Because beforeDispose could cause this object to be linked/released or call isAlive(), xxcount is set
            //to a a high mid-point positive number to avoid poss. of releasing again.
            if (atomic_cas(&xxcount, DEAD_PSEUDO_COUNT, 0))
            {
                const_cast<CInterface *>(this)->beforeDispose();
                delete this;
                return true;
            }
        }
        return false;
    }
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
class CInterfaceOf;

template <class INTERFACE>
class CReusableInterfaceOf;

// A thread safe basic implementation of IInterface that destroys the object when last Release() occurs.
template <class INTERFACE>
class CSimpleInterfaceOf : public INTERFACE
{
    friend class CInterfaceOf<INTERFACE>;    // want to keep xxcount private outside this pair of classes
    friend class CReusableInterfaceOf<INTERFACE>;
public:
    inline virtual ~CSimpleInterfaceOf() {}

    inline CSimpleInterfaceOf()           { atomic_set(&xxcount, 1); }
    inline bool IsShared(void) const    { return atomic_read(&xxcount) > 1; }
    inline int getLinkCount(void) const { return atomic_read(&xxcount); }

    inline void Link() const            { atomic_inc(&xxcount); }

    inline bool Release(void) const
    {
        if (atomic_dec_and_test(&xxcount))
        {
            delete this;
            return true;
        }
        return false;
    }

private:
    mutable atomic_t xxcount;
};

// A more general implementation of IInterface that includes a virtual function beforeDispose().
// beforeDispose() allows a fully constructed object to be cleaned up (which means that virtual
// function calls still work (unlike a virtual destructor).
// It makes it possible to implement a cache which doesn't link count the object, but is cleared when
// the object is disposed without a critical section in Release().  (See pattern details below).
template <class INTERFACE>
class CInterfaceOf : public CSimpleInterfaceOf<INTERFACE>
{
public:
    virtual void beforeDispose() {}

    inline bool isAlive() const         { return atomic_read(&this->xxcount) < DEAD_PSEUDO_COUNT; }       //only safe if Link() is called first

    inline bool Release(void) const
    {
        if (atomic_dec_and_test(&this->xxcount))
        {
            //Because beforeDispose could cause this object to be linked/released or call isAlive(), xxcount is set
            //to a a high mid-point positive number to avoid poss. of releasing again.
            if (atomic_cas(&this->xxcount, DEAD_PSEUDO_COUNT, 0))
            {
                const_cast<CInterfaceOf<INTERFACE> *>(this)->beforeDispose();
                delete this;
                return true;
            }
        }
        return false;
    }
};

// An extension of CInterfaceOf that allows objects that are being destroyed to be added to a free list instead.
// Before disposing addToFree() is called.  That function can link the object and return true which will prevent
// the object being destroyed.
template <class INTERFACE>
class CReusableInterfaceOf : public CInterfaceOf<INTERFACE>
{
public:
    //If this function returns true the object is added to a free list - if so it must be linked in the process.
    //It *must not* link and release the object if it returns false.
    virtual bool addToFreeList() const { return false; }

    inline bool Release(void) const
    {
        if (atomic_dec_and_test(&this->xxcount))
        {
            if (addToFreeList())
            {
                //It could have been added, reused and disposed between the call and this return, so must return immediately
                //and in particular cannot check/modify link count
                return true;
            }

            //NOTE: This class cannot be used for caches which don't link the pointers => use atomic_set instead of cas
            atomic_set(&this->xxcount, DEAD_PSEUDO_COUNT);
            const_cast<CReusableInterfaceOf<INTERFACE> *>(this)->beforeDispose();
            delete this;
            return true;
        }
        return false;
    }
};

//---------------------------------------------------------------------------------------------------------------------

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

    inline bool isAlive() const         { return this->xxcount < DEAD_PSEUDO_COUNT; }       //only safe if Link() is called first

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

#define IMPLEMENT_IINTERFACE_USING(x)                                                   \
    virtual void Link(void) const       { x::Link(); }                     \
    virtual bool Release(void) const    { return x::Release(); }

#define IMPLEMENT_IINTERFACE    IMPLEMENT_IINTERFACE_USING(CInterface)

#endif
