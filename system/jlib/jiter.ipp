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


#ifndef JITER_IPP
#define JITER_IPP

#include "jlib.hpp"
#include "jiter.hpp"

template<class ELEMENT, class ITER> class CArrayIteratorOf : public CInterfaceOf<ITER>
{
public:
    CArrayIteratorOf<ELEMENT,ITER>(const IArray & _values, aindex_t _start = 0, IInterface * _owner=NULL)
    : owner(LINK(_owner)), values(_values), start(_start)
    {
        current = start;
    }
    ~CArrayIteratorOf<ELEMENT,ITER>()
    {
        ::Release(owner);
    }

    virtual bool first()
    {
        current = start;
        return values.isItem(current);
    }
    virtual bool next()
    {
        current++;
        return values.isItem(current);
    }
    virtual bool isValid() { return values.isItem(current); }
    virtual ELEMENT & query() { return static_cast<ELEMENT &>(values.item(current)); }
    virtual ELEMENT & get() { return OLINK(static_cast<ELEMENT &>(values.item(current))); };

protected:
  IInterface * owner;
  const IArray &values;
  aindex_t current;
  aindex_t start;
};

class jlib_decl CNullIterator : public CInterfaceOf<IIterator>
{
public:
    virtual bool first() { return false; }
    virtual bool next()  { return false; }
    virtual bool isValid() { return false; }
    virtual IInterface & query() { IInterface * i = 0; return *i; }
    virtual IInterface & get()   { IInterface * i = 0; return *i; }
};

template<class X, class Y> class jlib_decl CNullIteratorOf : public CInterfaceOf<Y>
{
public:
    virtual bool first() { return false; }
    virtual bool next()  { return false; }
    virtual bool isValid() { return false; }
    virtual X & query() { X * i = 0; return *i; }
    virtual X & get()   { X * i = 0; return *i; }
};

template<class X, class Y> class CCompoundIteratorOf : public CInterfaceOf<X>
{
public:
    CCompoundIteratorOf(X * _left, X * _right) : left(_left), right(_right)
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
            if (left->next())
                return true;
            doneLeft = true;
            return right->first();
        }
        return right->next();
    }
    virtual bool isValid() { return doneLeft ? right->isValid() : left->isValid(); }
    virtual Y & query() { return doneLeft ? right->query() : left->query();}

protected:
    Linked<X> left;
    Linked<X> right;
    bool doneLeft;
};


#endif
