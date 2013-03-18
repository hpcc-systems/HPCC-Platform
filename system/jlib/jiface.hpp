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



#ifndef JIFACE_HPP
#define JIFACE_HPP


#include "platform.h"
#include <string.h>
#include "jscm.hpp"
#include "jatomic.hpp"

void jlib_decl RaiseAssertException(const char *assertion, const char *file, unsigned line);
void jlib_decl RaiseAssertCore(const char *assertion, const char *file, unsigned line);

#undef assert
#undef assertex

#ifdef _PREFAST_
 #pragma warning (disable : 6244)   // generates too much noise at the moment
 #define assertex(p) ((p) ? ((void) 0) : (( (void) RaiseAssertException(#p, __FILE__, __LINE__), __analysis_assume(p))))
 #define assert(p) ((p) ? ((void) 0) : (( (void) RaiseAssertCore(#p, __FILE__, __LINE__), __analysis_assume(p))))
#elif defined(_DEBUG)||defined(_TESTING)
 #define assertex(p) ((p) ? ((void) 0) : (( (void) RaiseAssertException(#p, __FILE__, __LINE__))))
 #define assert(p) ((p) ? ((void) 0) : (( (void) RaiseAssertCore(#p, __FILE__, __LINE__))))
#else
 #define assertex(p)
 #define assert(p)
#endif

#if defined(_DEBUG)||defined(_TESTING)
#define verifyex(p) ((p) ? ((void) 0) : (( (void) RaiseAssertException(#p, __FILE__, __LINE__))))
#define verify(p) ((p) ? ((void) 0) : (( (void) RaiseAssertCore(#p, __FILE__, __LINE__))))
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

//- Template variants -------------------------------------------------------------------------------------------------

template <class INTERFACE>
class CInterfaceOf;

template <class INTERFACE>
class CSimpleInterfaceOf : public INTERFACE
{
    friend class CInterfaceOf<INTERFACE>;    // want to keep xxcount private outside this pair of classes
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
// beforeDispose() allows an a fully constructed object to be cleaned up (which means that virtual
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
