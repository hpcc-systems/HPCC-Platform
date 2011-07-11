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

#endif
