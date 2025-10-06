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

#ifndef JHUTIL_HPP
#define JHUTIL_HPP

#include "jlib.hpp"
#include "jqueue.hpp"
#include "jhtree.hpp"

//This interface can be used to record the mapping items that are being removed when making more space.
//If the cache is checked within a critical section this allows the removed items to be released outside the critsec
interface IRemovedMappingCallback
{
    virtual void noteRemoval(void * mapping) = 0;
};

// TABLE should be SuperHashTable derivative to contain MAPPING's
// MAPPING should be something that constructs with (KEY, ENTRY) and impl. query returning ref. to ENTRY
template <class KEY, class ENTRY, class MAPPING, class TABLE>
class CMRUCacheOf : public CInterface//, public IInterface
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
    DListOf<MAPPING> mruList;

    void clear(int count, IRemovedMappingCallback * callback)
    {
        for (;;)
        {
            MAPPING *tail = mruList.dequeueTail();
            if (!tail)
                break;
            if (callback)
                callback->noteRemoval(tail);
            table.removeExact(tail);
            if ((-1 != count) && (0 == --count))
                break;
        }
    }
public:
    typedef SuperHashIteratorOf<MAPPING> CMRUIterator;

    CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE>() : table(*this) { }
    void replace(const KEY & key, ENTRY &entry)
    {
        if (full())
            makeSpace(nullptr);

        MAPPING * mapping = new MAPPING(key, entry); // owns entry
        table.replace(*mapping);
        mruList.enqueueHead(mapping);
    }
    void replace(const KEY & key, ENTRY &entry, IRemovedMappingCallback * callback)
    {
        if (full())
            makeSpace(callback);

        MAPPING * mapping = new MAPPING(key, entry); // owns entry
        table.replace(*mapping);
        mruList.enqueueHead(mapping);
    }
    MAPPING * replace(const KEY & key, IRemovedMappingCallback * callback)
    {
        if (full())
            makeSpace(callback);

        MAPPING * mapping = new MAPPING(key); // owns entry
        table.replace(*mapping);
        mruList.enqueueHead(mapping);
        return mapping;
    }
    unsigned getKeyHash(const KEY & key) const
    {
        return table.getHashFromFindParam(&key);
    }
    ENTRY *query(KEY key, bool doPromote=true)
    {
        MAPPING *mapping = table.find(key);
        if (!mapping) return NULL;

        if (doPromote)
            promote(mapping);
        return &mapping->query(); // MAPPING must impl. query()
    }
    ENTRY *query(unsigned hashcode, KEY * key, bool doPromote=true)
    {
        MAPPING *mapping = table.find(hashcode, *key);
        if (!mapping) return NULL;

        if (doPromote)
            promote(mapping);
        return &mapping->query(); // MAPPING must impl. query()
    }
    MAPPING *queryMapping(unsigned hashcode, KEY * key, bool doPromote=true)
    {
        MAPPING *mapping = table.find(hashcode, *key);
        if (!mapping) return NULL;

        if (doPromote)
            promote(mapping);
        return mapping;
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
    void kill() { clear(-1, nullptr); }
    void promote(MAPPING *mapping)
    {
        mruList.moveToHead(mapping);
    }
    CMRUIterator *getIterator()
    {
        return new SuperHashIteratorOf<MAPPING>(table);
    }
    virtual void makeSpace(IRemovedMappingCallback *) { }
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
    unsigned setCacheLimit(unsigned _cacheMax)
    {
        if (SELF::table.count() > _cacheMax)
            this->clear(_cacheMax - SELF::table.count(), nullptr);
        unsigned oldCacheMax = cacheMax;
        cacheMax = _cacheMax;
        return oldCacheMax;
    }
    virtual void makeSpace(IRemovedMappingCallback * callback)
    {
        SELF::clear(cacheOverflow, callback);
    }
    virtual bool full()
    {
        return ((unsigned)-1 != cacheMax) && (SELF::table.count() > cacheMax + cacheOverflow);
    }
};

#endif
