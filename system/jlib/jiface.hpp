/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */



#ifndef JIFACE_HPP
#define JIFACE_HPP


#include "platform.h"
#include <string.h>
#include "jexpdef.hpp"
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

    bool Release(void) const
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
