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



#ifndef JAVAHASH_HPP
#define JAVAHASH_HPP

#include "jexpdef.hpp"
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
    void onAdd(void *next);
    void onRemove(void *);
    inline unsigned getHashFromElement(const void * et) const
    { 
        return static_cast<const ELEMENT *>(et)->getHash(); 
    }
    inline unsigned getHashFromFindParam(const void * et) const
    { 
        return static_cast<const ELEMENT *>(et)->getHash(); 
    }
    inline const void * getFindParam(const void * et) const { return et; }
    inline bool matchesFindParam(const void * et, const void * key, unsigned fphash) const
    { 
        return static_cast<const ELEMENT *>(et)->equals(*static_cast<const ELEMENT *>(key)); 
    }

  public:
    JavaHashTableOf(bool _keep = true)
        : SuperHashTableOf<ELEMENT, ELEMENT>(), keep(_keep) {}
    ~JavaHashTableOf() { _SELF::releaseAll(); }

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
