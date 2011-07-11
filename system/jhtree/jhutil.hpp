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

#ifndef JHUTIL_HPP
#define JHUTIL_HPP

#include "jlib.hpp"
#include "jqueue.tpp"
#include "jhtree.hpp"

// TABLE should be SuperHashTable derivitive to contain MAPPING's
// MAPPING should be something that constructs with (KEY, ENTRY) and impl. query returning ref. to ENTRY
template <class KEY, class ENTRY, class MAPPING, class TABLE>
class CMRUCacheOf : public CInterface, public IInterface
{
protected:
    class DTABLE : public TABLE
    {
        CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE> &owner;
    public:
        DTABLE(CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE> &_owner) : owner(_owner) { }
        virtual void onAdd(void *mapping) { owner.elementAdded((MAPPING *)mapping); TABLE::onAdd(mapping); }
        virtual void onRemove(void *mapping) { owner.elementRemoved((MAPPING *)mapping); TABLE::onRemove(mapping); }
    } table;
    QueueOf<MAPPING, false> mruList;

    void clear(int count)
    {
        loop
        {
            MAPPING *tail = mruList.dequeueTail();
            if (!tail)
                break;
            table.removeExact(tail);
            if ((-1 != count) && (0 == --count))
                break;
        }
    }
public:
    typedef SuperHashIteratorOf<MAPPING> CMRUIterator;

    IMPLEMENT_IINTERFACE;

    CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE>() : table(*this) { }
    void add(KEY key, ENTRY &entry, bool promoteIfAlreadyPresent=true)
    {
        if (full())
            makeSpace();

        MAPPING *mapping = table.find(key);
        if (mapping)
        {
            if (promoteIfAlreadyPresent)
                promote(mapping);
        }
        else
        {
            mapping = new MAPPING(key, entry); // owns entry
            table.replace(*mapping);
            mruList.enqueueHead(mapping);
        }
    }
    ENTRY *query(KEY key, bool doPromote=true)
    {
        MAPPING *mapping = table.find(key);
        if (!mapping) return NULL;

        if (doPromote)
            promote(mapping);
        return &mapping->query(); // MAPPING must impl. query()
    }
    ENTRY *get(KEY key, bool doPromote=true)
    {
        return LINK(query(key, doPromote));
    }
    bool remove(MAPPING *_mapping)
    {   
        Linked<MAPPING> mapping = _mapping;
        if (!table.removeExact(_mapping))
            return false;
        mruList.dequeue(mapping);
        return true;
    }
    bool remove(KEY key)
    {
        Linked<MAPPING> mapping = table.find(key);
        if (!mapping)
            return false;
        table.removeExact(mapping);
        mruList.dequeue(mapping);
        return true;
    }
    void kill() { clear(-1); }
    void promote(MAPPING *mapping)
    {
        mruList.dequeue(mapping); // will still be linked in table
        mruList.enqueueHead(mapping);
    }
    CMRUIterator *getIterator()
    {
        return new SuperHashIteratorOf<MAPPING>(table);
    }
    virtual void makeSpace() { }
    virtual bool full() { return false; }
    virtual void elementAdded(MAPPING *mapping) { }
    virtual void elementRemoved(MAPPING *mapping) { }
};

template <class KEY, class ENTRY, class MAPPING, class TABLE>
class CMRUCacheMaxCountOf : public CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE>
{
    typedef CMRUCacheMaxCountOf<KEY, ENTRY, MAPPING, TABLE> SELF;
    unsigned cacheMax;
    unsigned cacheOverflow; // # to clear if full
public:
    CMRUCacheMaxCountOf(unsigned _cacheMax) : cacheMax(_cacheMax) { cacheOverflow = 1; }
    void setCacheLimit(unsigned _cacheMax)
    {
        if (SELF::table.count() > _cacheMax)
            clear(_cacheMax - SELF::table.count());
        cacheMax = _cacheMax;
    }
    virtual void makeSpace()
    {
        SELF::clear(cacheOverflow);
    }
    virtual bool full()
    {
        return ((unsigned)-1 != cacheMax) && (SELF::table.count() > cacheMax + cacheOverflow);
    }
};

#endif
