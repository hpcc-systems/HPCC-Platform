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

interface IInterface
{
    virtual void Link() const = 0;
    virtual bool Release() const = 0;
};

template <class X> inline void Link(X * ptr)    { if (ptr) ptr->Link(); }
template <class X> inline void Release(X * ptr) { if (ptr) ptr->Release(); }

#define QUERYINTERFACE(ptr, TYPE)   (dynamic_cast<TYPE *>(ptr))

//The constructors for this assume that the source pointer has already been linked
template <class CLASS> class Owned
{
public:
    inline Owned()                              { ptr = NULL; }
    inline Owned(CLASS * _ptr)                  { ptr = _ptr; }
    inline Owned(const Owned<CLASS> & other)    { ptr = other.getLink(); }
    inline ~Owned()                             { ::Release(ptr); }
    
private: 
    inline void operator = (CLASS * _ptr)                { set(_ptr);  }
    inline void operator = (const Owned<CLASS> & other) { set(other.get());  }
    inline void setown(const Owned<CLASS> &other) {  }

public:
    inline CLASS * operator -> () const         { return ptr; } 
    inline operator CLASS *() const             { return ptr; } 

    inline void clear()                         { CLASS *temp=ptr; ptr=NULL; ::Release(temp); }
    inline CLASS * get() const                  { return ptr; }
    inline CLASS * getClear()                   { CLASS * temp = ptr; ptr = NULL; return temp; }
    inline CLASS * getLink() const              { if (ptr) ptr->Link(); return ptr; }
    inline void set(CLASS * _ptr)               { CLASS * temp = ptr; ::Link(_ptr); ptr = _ptr; ::Release(temp); }
    inline void setown(CLASS * _ptr)            { CLASS * temp = ptr; ptr = _ptr; ::Release(temp); }
    inline void set(const Owned<CLASS> &other)  { set(other.get()); }
    
private:
    CLASS * ptr;
};


// Manages a scoped reference to an IInterface that HAS NOT already been linked.  
// Therefore it calls Link() in the constructor.               
template <class CLASS> class Linked : public Owned<CLASS>
{
public:
    inline Linked()                         { }
    inline Linked(CLASS * _ptr) : Owned<CLASS>(LINK(_ptr)) { }
    inline Linked(const Owned<CLASS> & other) : Owned<CLASS>(other) { }
    inline Linked(const Linked<CLASS> & other) : Owned<CLASS>(other) { }
};

//As Linked<X> but also implements assignment operator, so can be used in stl containers.
template <class CLASS> class StlLinked : public Owned<CLASS>
{
public:
    inline StlLinked() {}
    inline StlLinked(CLASS* c) : Owned<CLASS>(LINK(c)) {}
    inline StlLinked(const StlLinked<CLASS> & c) : Owned<CLASS>(c) {}
    inline void operator = (CLASS * c) { set(c); }
    inline void operator = (const StlLinked<CLASS> & other) { set(other.get()); }
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
    virtual void setLen(const void * val, unsigned length) = 0;
    virtual unsigned length() const = 0;
    virtual void * reserve(unsigned length) = 0;
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
template <class X> inline X * LINK(const Owned<X> &ptr) { return ptr.getLink(); }
template <class X> inline X * LINK(const Linked<X> &ptr)    { return ptr.getLink(); }

#endif
