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


#ifndef JITER_IPP
#define JITER_IPP

#include "jlib.hpp"
#include "jiter.hpp"

class jlib_decl CArrayIteratorBase : public CInterface
{
protected:
  IInterface *owner;
  const Array &values;
  aindex_t current;
  aindex_t start;
public:
  CArrayIteratorBase(const Array &, aindex_t start=0 , IInterface *owner=NULL);
  ~CArrayIteratorBase();

  virtual bool first();
  virtual bool next();
  virtual bool isValid();
  virtual IInterface &_query();
};

class jlib_decl CArrayIterator : public CArrayIteratorBase , implements IIterator
{
public:
  IMPLEMENT_IINTERFACE;
  CArrayIterator(const Array &a, aindex_t start = 0, IInterface *owner=NULL) : CArrayIteratorBase(a, start, owner) {}

  virtual bool first() { return CArrayIteratorBase::first(); }
  virtual bool next() { return CArrayIteratorBase::next(); }
  virtual bool isValid() { return CArrayIteratorBase::isValid(); }
  virtual IInterface & query() { return CArrayIteratorBase::_query(); };
  virtual IInterface & get() { IInterface &ret = CArrayIteratorBase::_query(); ret.Link(); return ret; };
};

template<class X, class Y> class CArrayIteratorOf : public CArrayIteratorBase, implements Y
{
public:
  IMPLEMENT_IINTERFACE;
  CArrayIteratorOf<X,Y>(const Array &a, aindex_t start = 0, IInterface *owner=NULL) : CArrayIteratorBase(a, start, owner) {}

  virtual bool first() { return CArrayIteratorBase::first(); }
  virtual bool next() { return CArrayIteratorBase::next(); }
  virtual bool isValid() { return CArrayIteratorBase::isValid(); }
  virtual X & query() { return (X &) CArrayIteratorBase::_query(); }
  virtual X & get() { X &ret = (X &) CArrayIteratorBase::_query(); ret.Link(); return ret; };
};

class jlib_decl COwnedArrayIterator : public CArrayIterator
{
public:
    COwnedArrayIterator(Array *_values, aindex_t _start = 0);
    ~COwnedArrayIterator();
};

class jlib_decl CNullIterator : public CInterface, public IIterator
{
public:
    IMPLEMENT_IINTERFACE

    virtual bool first() { return false; }
    virtual bool next()  { return false; }
    virtual bool isValid() { return false; }
    virtual IInterface & query() { IInterface * i = 0; return *i; }
    virtual IInterface & get()   { IInterface * i = 0; return *i; }
};

template<class X, class Y> class jlib_decl CNullIteratorOf : public CInterface, public Y
{
public:
    IMPLEMENT_IINTERFACE

    virtual bool first() { return false; }
    virtual bool next()  { return false; }
    virtual bool isValid() { return false; }
    virtual X & query() { X * i = 0; return *i; }
    virtual X & get()   { X * i = 0; return *i; }
};
template<class X, class Y> class CCompoundIteratorOf : public CInterfaceOf<Y>
{
public:
    CCompoundIteratorOf(Y * _left, Y * _right) : left(_left), right(_right)
    {
        doneLeft = true;
    }
    virtual bool first() 
    { 
        doneLeft = false;
        if (left->first())
            return true;
        doneLeft = true;
        return right->first();
    }
    virtual bool next()  
    {
        if (!doneLeft)
        {
            if (left->first())
                return true;
            doneLeft = true;
        }
        return right->next();
    }
    virtual bool isValid() { return doneLeft ? right->isValid() : left->isValid(); }
    virtual X & query() { return doneLeft ? right->query() : left->query();}
    
protected:
    Linked<Y> left;
    Linked<Y> right;
    bool doneLeft;
};


#endif
