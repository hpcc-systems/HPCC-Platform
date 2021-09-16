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
#include "jqueue.tpp"
#include "jhtree.hpp"

//Implementation of a queue using a doubly linked list.  Should possibly move to jlib if it would be generally useful.
//Currently assumes next and prev fields in the element can be used to maintain the list
template <class ELEMENT>
class DListOf
{
    typedef DListOf<ELEMENT> SELF;
    ELEMENT * pHead = nullptr;
    ELEMENT * pTail = nullptr;
    unsigned numEntries = 0;

public:
    void enqueueHead(ELEMENT * element)
    {
        assertex(!element->next && !element->prev);
        if (pHead)
        {
            pHead->prev = element;
            element->next = pHead;
            pHead = element;
        }
        else
        {
            pHead = pTail = element;
        }
        numEntries++;
    }
    void enqueue(ELEMENT * element)
    {
        assertex(!element->next && !element->prev);
        if (pTail)
        {
            pTail->next = element;
            element->prev = pTail;
            pTail = element;
        }
        else
        {
            pHead = pTail = element;
        }
        numEntries++;
    }
    ELEMENT *head() const { return pHead; }
    ELEMENT *tail() const { return pTail; }
    void remove(ELEMENT *element)
    {
        ELEMENT * next = element->next;
        ELEMENT * prev = element->prev;
        assertex(prev || next || element == pHead);
        if (element == pHead)
            pHead = next;
        if (element == pTail)
            pTail = prev;
        if (next)
            next->prev = prev;
        if (prev)
            prev->next = next;
        element->next = nullptr;
        element->prev = nullptr;
        numEntries--;
    }
    ELEMENT *dequeue()
    {
        if (!pHead)
            return nullptr;
        ELEMENT * element = pHead;
        ELEMENT * next = element->next;
        pHead = next;
        if (element == pTail)
            pTail = nullptr;
        if (next)
            next->prev = nullptr;
        element->next = nullptr;
        numEntries--;
        return element;
    }
    ELEMENT *dequeueTail()
    {
        if (!pTail)
            return nullptr;
        ELEMENT * element = pTail;
        ELEMENT * prev = element->prev;
        pTail = prev;
        if (element == pHead)
            pHead = nullptr;
        if (prev)
            prev->next = nullptr;
        element->prev = nullptr;
        numEntries--;
        return element;
    }
    void dequeue(ELEMENT *element)
    {
        remove(element);
    }
    inline unsigned ordinality() const { return numEntries; }
    //Check that the linked list is self-consistent.  For debugging potential issues.
    void validate() const
    {
        ELEMENT * prev = nullptr;
        ELEMENT * cur = pHead;
        unsigned count = 0;
        while (cur)
        {
            assertex(cur->prev == prev);
            prev = cur;
            cur = cur->next;
            count++;
        }
        assertex(prev == pTail);
        assertex(count == numEntries);
    }
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

    void clear(int count)
    {
        for (;;)
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

    CMRUCacheOf<KEY, ENTRY, MAPPING, TABLE>() : table(*this) { }
    void replace(KEY key, ENTRY &entry)
    {
        if (full())
            makeSpace();

        MAPPING * mapping = new MAPPING(key, entry); // owns entry
        table.replace(*mapping);
        mruList.enqueueHead(mapping);
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
        if (mruList.head() != mapping)
        {
            mruList.dequeue(mapping); // will still be linked in table
            mruList.enqueueHead(mapping);
        }
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
    unsigned setCacheLimit(unsigned _cacheMax)
    {
        if (SELF::table.count() > _cacheMax)
            this->clear(_cacheMax - SELF::table.count());
        unsigned oldCacheMax = cacheMax;
        cacheMax = _cacheMax;
        return oldCacheMax;
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
