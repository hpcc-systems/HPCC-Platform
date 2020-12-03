/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef UDPIPMAP_INCL
#define UDPIPMAP_INCL

#include "jsocket.hpp"
#include "udplib.hpp"
#include <functional>
#include <iterator>
#include <algorithm>


template<class T> class IpMapOf
{
private:
    class list
    {
    public:
        list(const ServerIdentifier &_ip, const list *_next, std::function<T *(const ServerIdentifier &)> tfunc) : ip(_ip), next(_next)
        {
            entry = tfunc(ip);
        }
        ~list()
        {
            delete entry;
        }
        const ServerIdentifier ip;
        const list *next;
        T *entry;
    };

    class myIterator
    {
    private:
        const list * value = nullptr;
        int hash = 0;
        const std::atomic<const list *> *table = nullptr;

    public:
        typedef T                     value_type;
        typedef std::ptrdiff_t        difference_type;
        typedef T*                    pointer;
        typedef T&                    reference;
        typedef std::input_iterator_tag iterator_category;

        explicit myIterator(const list *_value, int _hash, const std::atomic<const list *> *_table)
        : value(_value), hash(_hash), table(_table)
        {
        }
        reference operator*() const { return *value->entry; }
        bool operator==(const myIterator& other) const { return value == other.value && hash==other.hash; }
        bool operator!=(const myIterator& other) const { return !(*this == other); }
        myIterator operator++(int)
        {
            myIterator ret = *this;
            ++(*this);
            return ret;
        }
        myIterator& operator++()
        {
            value = value->next;
            while (!value)
            {
                hash += 1;
                if (hash==256)
                    break;
                value = table[hash].load(std::memory_order_acquire);
            }
            return *this;
        }
    };

public:
    IpMapOf<T>(std::function<T *(const ServerIdentifier &)> _tfunc) : tfunc(_tfunc)
    {
    }
    T &lookup(const ServerIdentifier &) const;
    inline T &operator[](const ServerIdentifier &ip) const { return lookup(ip); }
    myIterator begin()
    {
        // Take care as it's possible for firstHash to be updated on another thread as we are running
        unsigned lfirstHash = firstHash;
        if (lfirstHash==256)
            return end();
        else
            return myIterator(table[lfirstHash].load(std::memory_order_acquire), lfirstHash, table);
    }
    myIterator end()   { return myIterator(nullptr, 256, nullptr); }

private:
    const std::function<T *(const ServerIdentifier &)> tfunc;
    mutable std::atomic<const list *> table[256] = {};
    mutable CriticalSection lock;
    mutable std::atomic<unsigned> firstHash = { 256 };
};

template<class T> T &IpMapOf<T>::lookup(const ServerIdentifier &ip) const
{
   unsigned hash = ip.fasthash() & 0xff;
   for (;;)
   {
       const list *head = table[hash].load(std::memory_order_acquire);
       const list *finger = head;
       while (finger)
       {
           if (finger->ip == ip)
               return *finger->entry;
           finger = finger->next;
       }
       // If we get here, we need to add a new entry. This should be rare, so ok to lock
       // Note that we only lock out other additions, not other lookups
       // I could have a lock per table-entry if I thought it was worthwhile
       CriticalBlock b(lock);
       if (table[hash].load(std::memory_order_acquire) != head)
           continue;  // NOTE - an alternative implementation would be to rescan the list inside the critsec, but this is cleaner
       finger = new list(ip, head, tfunc);
       table[hash].store(finger, std::memory_order_release);
       if (hash <= firstHash)
           firstHash = hash;
       return *finger->entry;
   }
}

#endif
