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


#ifndef _JSCM_HPP_
#define _JSCM_HPP_

#undef interface
#define implements   public
#define extends      public

#ifdef _MSC_VER
 #define interface    struct __declspec(novtable)
#else
 #define interface    struct
#endif
#ifdef JLIB_EXPORTS
 #define jlib_decl DECL_EXPORT
 #define jlib_thrown_decl DECL_EXPORT
#else
 #define jlib_decl DECL_IMPORT
 #define jlib_thrown_decl DECL_IMPORT
#endif

#include <atomic>

interface IInterface
{
    virtual void Link() const = 0;
    virtual bool Release() const = 0;
};

template <class X> inline void Link(X * ptr)    { if (ptr) ptr->Link(); }
template <class X> inline void Release(X * ptr) { if (ptr) ptr->Release(); }

#define QUERYINTERFACE(ptr, TYPE)   (dynamic_cast<TYPE *>(ptr))

//This base class implements a shared pointer based on a link count held in the object.
//The two derived classes Owned and Linked should be used as the concrete types to construct a shared object
//from a pointer.
template <class CLASS> class Shared
{
public:
    inline Shared()                              { ptr = NULL; }
    inline Shared(CLASS * _ptr, bool owned)      { ptr = _ptr; if (!owned && _ptr) _ptr->Link(); }
    inline Shared(const Shared & other)          { ptr = other.getLink(); }
#if defined(__cplusplus) && __cplusplus >= 201100
    inline Shared(Shared && other)               { ptr = other.getClear(); }
#endif
    inline ~Shared()                             { ::Release(ptr); }
    inline Shared<CLASS> & operator = (const Shared<CLASS> & other) { this->set(other.get()); return *this;  }

    inline CLASS * operator -> () const         { return ptr; }
    inline operator CLASS *() const             { return ptr; }

    inline void clear()                         { CLASS *temp=ptr; ptr=NULL; ::Release(temp); }
    inline CLASS * get() const                  { return ptr; }
    inline CLASS * getClear()                   { CLASS * temp = ptr; ptr = NULL; return temp; }
    inline CLASS * getLink() const              { if (ptr) ptr->Link(); return ptr; }
    inline void set(CLASS * _ptr)
    {
        CLASS * temp = ptr;
        if (temp != _ptr)
        {
            ::Link(_ptr);
            ptr = _ptr;
            ::Release(temp);
        }
    }
    inline void set(const Shared<CLASS> &other) { this->set(other.get()); }
    inline void setown(CLASS * _ptr)            { CLASS * temp = ptr; ptr = _ptr; ::Release(temp); }
    inline bool setownIfNull(CLASS * _ptr)
    {
        if (!ptr)
        {
            ptr = _ptr;
            return true;
        }
        else
        {
            ::Release(_ptr);
            return false;
        }
    }
    inline void swap(Shared<CLASS> & other)     { CLASS * temp = ptr; ptr = other.ptr; other.ptr = temp; }

protected:
    inline Shared(CLASS * _ptr)                  { ptr = _ptr; } // deliberately protected

private:
    inline void setown(const Shared<CLASS> &other); // illegal - going to cause a -ve leak
    inline Shared<CLASS> & operator = (const CLASS * other);

private:
    CLASS * ptr;
};

// Similar to class Shared<X>, but thread safe versions of the functions (avoid the need for critical sections)
template <class CLASS> class AtomicShared
{
public:
    inline AtomicShared()                              { ptr.store(nullptr, std::memory_order_relaxed); }
    inline AtomicShared(CLASS * _ptr, bool owned)      { ptr.store(_ptr, std::memory_order_relaxed); if (!owned && _ptr) _ptr->Link(); }
    inline AtomicShared(const AtomicShared & other)    { ptr.store(other.getLinkNonAtomic(), std::memory_order_relaxed); }
#if defined(__cplusplus) && __cplusplus >= 201100
    inline AtomicShared(AtomicShared && other)         { ptr.store(other.getClear(), std::memory_order_relaxed); }
#endif
    inline ~AtomicShared()                             { ::Release(ptr.load(std::memory_order_relaxed)); }
    inline AtomicShared<CLASS> & operator = (const AtomicShared<CLASS> & other) { this->setown(other.getLinkNonAtomic()); return *this;  }

    inline void clear()                                { ::Release(getClear()); }
    inline CLASS * getClear()
    {
        return ptr.exchange(nullptr);
    }
    inline CLASS * getClearNonAtomic()
    {
        CLASS * result = ptr.load();
        ptr.store(nullptr);
        return result;
    }

    //The getLink() function cannot be implemented in a thread safe way - e.g. if clear is called concurrently
    //then temp will point to a freed object.  (Might be possible with support for transactional memory...)
    inline CLASS * getLinkNonAtomic() const
    {
        CLASS * temp = ptr;
        if (temp)
            temp->Link();
        return temp;
    }
    inline bool isSet() const                 { return ptr != nullptr; }
    inline void set(CLASS * _ptr)
    {
        if (ptr != _ptr)
        {
            LINK(_ptr);
            this->setown(_ptr);
        }
    }
    inline bool setownIfNull(CLASS * _ptr)
    {
        CLASS * expected = nullptr;
        if (ptr.compare_exchange_strong(expected, _ptr))
            return true;
        ::Release(_ptr);
        return false;
    }
    inline void setown(CLASS * _ptr)
    {
        CLASS * temp = ptr.exchange(_ptr);
        ::Release(temp);
    }
    inline CLASS * swap(CLASS * _ptr)
    {
        return ptr.exchange(_ptr);
    }
    //swap - this will only update this once, but other can temporarily have a null value
    inline void swap(AtomicShared<CLASS> & other)
    {
        CLASS * temp = other.getClear();
        temp = this->swap(temp);
        temp = other.swap(temp);
        ::Release(temp);
    }

protected:
    inline AtomicShared(CLASS * _ptr)                  { ptr = _ptr; } // deliberately protected

private:
    inline void setown(const AtomicShared<CLASS> &other); // illegal - going to cause a -ve leak
    inline AtomicShared<CLASS> & operator = (const CLASS * other);

private:
    std::atomic<CLASS *> ptr;
};


//An Owned Shared object takes ownership of the pointer that is passed in the constructor.
template <class CLASS> class Owned : public Shared<CLASS>
{
public:
    inline Owned()                              { }
    inline Owned(CLASS * _ptr) : Shared<CLASS>(_ptr)   { }

    inline Shared<CLASS> & operator = (const Shared<CLASS> & other) { this->set(other.get()); return *this;  }

private:
    inline Owned(const Shared<CLASS> & other); // Almost certainly a bug
    inline Owned<CLASS> & operator = (const CLASS * other);
};


//A Linked Shared object takes does not take ownership of the pointer that is passed in the constructor.
template <class CLASS> class Linked : public Shared<CLASS>
{
public:
    inline Linked()                         { }
    inline Linked(CLASS * _ptr) : Shared<CLASS>(LINK(_ptr)) { }
    inline Linked(const Shared<CLASS> & other) : Shared<CLASS>(other) { }

    inline Shared<CLASS> & operator = (const Shared<CLASS> & other) { this->set(other.get()); return *this;  }

private:
    inline Linked<CLASS> & operator = (const CLASS * other);
};

// IStringVal manages returning of arbitrary null-terminated string data between systems that may not share heap managers
interface IStringVal
{
    virtual const char * str() const = 0;
    virtual void set(const char * val) = 0;
    virtual void clear() = 0;
    virtual void setLen(const char * val, unsigned length) = 0;
    virtual unsigned length() const = 0;
};


// IDataVal manages returning of arbitrary unterminated binary data between systems that may not share heap managers
interface IDataVal
{
    virtual const void * data() const = 0;
    virtual void clear() = 0;
    virtual void setLen(const void * val, size_t length) = 0;
    virtual size_t length() const = 0;
    virtual void * reserve(size_t length) = 0;
};


// IIterator
interface IIterator : extends IInterface
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual IInterface & query() = 0;
    virtual IInterface & get() = 0;
};

template <class C>
interface IIteratorOf : public IInterface
{
public:
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual C  & query() = 0;
            C  & get() { C &c = query(); c.Link(); return c; }
};

#define ForEach(i)              for((i).first();(i).isValid();(i).next())

typedef IInterface * IInterfacePtr;
typedef Owned<IInterface> OwnedIInterface;
typedef Linked<IInterface> LinkedIInterface;

template <class X> inline X * LINK(X * ptr)     { if (ptr) ptr->Link(); return ptr; }
template <class X> inline X & OLINK(X & obj)        { obj.Link(); return obj; }
template <class X> inline X * LINK(const Shared<X> &ptr) { return ptr.getLink(); }

class StringBuffer;

// When changing this enum, be sure to update (a) the string functions, and (b) NUM value in jlog.hpp

typedef enum
{
    // Target audience: system admins
    // Purpose: Information useful for administering the platform, diagnosing errors and
    //          resolving system issues
    MSGAUD_operator    = 0x01,
    // Target audience: ECL developers
    // Purpose: Information useful for diagnosing and resolving issues (performance and faults)
    //          in the execution ECL queries
    MSGAUD_user        = 0x02,
    // Target audience: HPCC Platform developers
    // Purpose: Information related to errors relating to potential bugs or any unexpected errors that
    //          would not be resolvable by sysadmin or ECL developers. Additional information that may
    //          be useful for improving the platform.
    MSGAUD_programmer  = 0x20,
    // MSGAUD_legacy      = 0x40, REMOVED - may be reused later
    // Target audience: persons involved in accounting and security audits
    MSGAUD_audit       = 0x80,
    // MSGAUD_all is to be used for filtering or specifying which messages are to be logged
    // (A message itself should not be logged as MSGAUD_all)
    MSGAUD_all         = 0xFF
} MessageAudience;

interface jlib_thrown_decl IException : public IInterface
{
    virtual int             errorCode() const = 0;
    virtual StringBuffer &  errorMessage(StringBuffer &msg) const = 0;
    virtual MessageAudience errorAudience() const = 0;
};

#endif
