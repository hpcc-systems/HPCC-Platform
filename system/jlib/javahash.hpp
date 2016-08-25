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



#ifndef JAVAHASH_HPP
#define JAVAHASH_HPP

#include "jiface.hpp"
#include "jlib.hpp"
#include "jobserve.hpp"
#include "jiter.hpp"
#include "jsuperhash.hpp"

interface jlib_decl IJavaHashable : extends IObservable
{
 public:
    virtual unsigned    getHash() const = 0;
    virtual bool      equals(const IJavaHashable & other) const = 0;
};

interface ICompare;

template <class ELEMENT>
class JavaHashTableOf
    : public SuperHashTableOf<ELEMENT, ELEMENT>, implements IObserver
{
    typedef JavaHashTableOf<ELEMENT> _SELF;
private:
    virtual void onAdd(void *next) override;
    virtual void onRemove(void *) override;
    virtual unsigned getHashFromElement(const void * et) const override
    { 
        return static_cast<const ELEMENT *>(et)->getHash(); 
    }
    virtual unsigned getHashFromFindParam(const void * et) const override
    { 
        return static_cast<const ELEMENT *>(et)->getHash(); 
    }
    virtual const void * getFindParam(const void * et) const override { return et; }
    virtual bool matchesFindParam(const void * et, const void * key, unsigned fphash __attribute__((unused))) const override
    { 
        return static_cast<const ELEMENT *>(et)->equals(*static_cast<const ELEMENT *>(key)); 
    }

  public:
    JavaHashTableOf(bool _keep)
        : SuperHashTableOf<ELEMENT, ELEMENT>(), keep(_keep) {}
    JavaHashTableOf(unsigned initsize, bool _keep)
        : SuperHashTableOf<ELEMENT, ELEMENT>(initsize), keep(_keep) {}
    ~JavaHashTableOf() { _SELF::_releaseAll(); }

    IMPLEMENT_IINTERFACE

    bool addOwn(ELEMENT &);
    bool replaceOwn(ELEMENT &);
    ELEMENT * findLink(const ELEMENT &) const;
    virtual bool onNotify(INotification & notify);

    ELEMENT * findCompare(ICompare *icmp,void * (ELEMENT::*getPtr)() const, unsigned hash, const void *val) const;

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(ELEMENT, ELEMENT)

  private:
    bool keep;
};

template <class ELEMENT>
class JavaHashIteratorOf : public SuperHashIteratorOf<ELEMENT>
{
    typedef JavaHashIteratorOf<ELEMENT> _SELF;  
  public:
    JavaHashIteratorOf(JavaHashTableOf<ELEMENT> & _table, bool linkTable=true)
        : SuperHashIteratorOf<ELEMENT>(_table, linkTable) {}

    inline ELEMENT & get() { ELEMENT & et = _SELF::query(); et.Link(); return et; }
};

#endif
