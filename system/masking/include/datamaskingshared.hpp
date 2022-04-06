/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.
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

#pragma once

#include "jexcept.hpp"
#include "jiface.hpp"
#include "datamasking.h"
#include <functional>
#include <list>
#include <map>
#include <set>
#include <string>

namespace DataMasking
{
    /**
     * @brief Doubly dereferences an iterator to obtain a value reference.
     *
     * Intended to extract a reference to a shareable object from an STL collection of shareable
     * objects. STL is not required, so long as a double dereference of a value of `iterator_t`
     * type yields a value of `value_t` type.
     *
     * @tparam value_t
     * @tparam iterator_t
     * @param it
     * @return value_t&
     */
    template <typename value_t, typename iterator_t>
    inline value_t& extractShared(iterator_t it)
    {
        return *(*it);
    }

    /**
     * @brief Dereference the secnd part of a pair referred to by an iterator.
     *
     * Intended to extract a reference to a shareable object from an STL map where the shareable
     * object is the mapped value (instead of the key).
     *
     * @tparam value_t
     * @tparam iterator_t
     * @param it
     * @return value_t&
     */
    template <typename value_t, typename iterator_t>
    inline value_t& extractMappedShared(iterator_t it)
    {
        return *(it->second);
    }

    /**
     * @brief Dereference an iterator to obtain the character buffer of an STL string..
     *
     * Intended to extract character pointers from an STL collection of STL strings. A functor is
     * used in place of a function because the pointer returned by `string::c_str()` cannot be
     * directly returned as a reference.
     *
     * @tparam iterator_t
     */
    template <typename iterator_t>
    struct StdStringExtractor
    {
        const char* value;
        const char*& operator () (iterator_t it)
        {
            value = it->c_str();
            return value;
        }
    };

    template <typename iterator_t>
    struct StdStringKeyExtractor
    {
        const char* value;
        const char*& operator () (iterator_t it)
        {
            value = it->first.c_str();
            return value;
        }
    };

    /**
     * @brief Implementation of `IIteratorOfScalar<>` that is always empty.
     *
     * @tparam value_t
     */
    template <typename value_t>
    class EmptyScalarIteratorOf : public CInterfaceOf<IIteratorOfScalar<value_t> >
    {
    public:
        virtual bool first() override { return false; }
        virtual bool next() override { return false; }
        virtual bool isValid() override { return false; }
        virtual const value_t& query() override { throw makeStringException(-1, "invalid query of EmptyScalarIteratorOf"); }
    };

    /**
     * @brief Implementation of `IIteratorOfScalar<>` that iterates the values between two STL-
     *        style iterators.
     *
     * Uses a required extraction function to obtain values of type `value_t` from iterators of
     * type `src_iterator_t`. The templated iterator type allows use with many STL collections.
     * The extraction function decouples the STL collection's value type from the requested
     * value type.
     *
     * Uses an optional filter function to skip undesirable values.
     *
     * @tparam value_t
     * @tparam src_iterator_t
     */
    template <typename value_t, typename src_iterator_t>
    class TIteratorOfScalar : public CInterfaceOf<IIteratorOfScalar<value_t> >
    {
    public:
        using value_extractor_t = std::function<value_t&(src_iterator_t)>;
        using value_filter_t = std::function<bool(src_iterator_t)>;
        bool first() override
        {
            cur = begin;
            skipFiltered();
            return isValid();
        }
        bool next() override
        {
            if (isValid())
            {
                ++cur;
                skipFiltered();
                return isValid();
            }
            return false;
        }
        bool isValid() override
        {
            return (cur != end);
        }
        const value_t& query() override
        {
            if (!isValid())
                throw makeStringException(-1, "invalid TIteratorOf iterator query");
            return extractor(cur);
        }
    protected:
        src_iterator_t    begin;
        src_iterator_t    cur;
        src_iterator_t    end;
        value_extractor_t extractor;
        value_filter_t    filter = nullptr;
    public:
        TIteratorOfScalar(src_iterator_t _begin, src_iterator_t _end, value_extractor_t _extractor)
            : begin(_begin)
            , cur(_end)
            , end(_end)
            , extractor(_extractor)
        {
            if (!extractor)
                throw makeStringException(-1, "missing TIteratorOf value extractor");
        }
        TIteratorOfScalar(src_iterator_t _begin, src_iterator_t _end, value_extractor_t _extractor, value_filter_t _filter)
            : begin(_begin)
            , cur(_end)
            , end(_end)
            , extractor(_extractor)
            , filter(_filter)
        {
            if (!extractor)
                throw makeStringException(-1, "missing TIteratorOf value extractor");
        }
    protected:
        TIteratorOfScalar()
        {
        }
        void skipFiltered()
        {
            if (filter)
            {
                while (isValid() && !filter(cur))
                    ++cur;
            }
        }
    };

    /**
     * @brief Extension of `TIteratorOfScalar<>` that caches shareable value references extracted
     *        from the underlying STL container.
     *
     * Uses a required extraction function to obtain shareable value references of type `value_t`
     * from iterators of type `src_iterator_t`. Extracted valued are cached in an internal STL
     * list which is iterated by the base class methods.
     *
     * Uses an optional filter function to limit which values are cached. For example, a value
     * type iterator may cache only those instances that match with a given context.
     *
     * @tparam value_t
     * @tparam src_iterator_t
     */
    template <typename value_t, typename src_iterator_t>
    class TIteratorOfShared : public TIteratorOfScalar<value_t, typename std::list<Linked<value_t> >::const_iterator>
    {
    public:
        using cached_t = std::list<Linked<value_t> >;
        using cached_iterator_t = typename cached_t::const_iterator;
        using src_value_filter_t = std::function<bool(src_iterator_t)>;
        using src_value_extractor_t = std::function<value_t&(src_iterator_t)>;
    protected:
        std::list<Linked<value_t> > cache;
    public:
        TIteratorOfShared(src_iterator_t _begin, src_iterator_t _end, src_value_extractor_t _extractor)
        {
            init(_begin, _end, _extractor, nullptr);
        }
        TIteratorOfShared(src_iterator_t _begin, src_iterator_t _end, src_value_extractor_t _extractor, src_value_filter_t _filter)
        {
            init(_begin, _end, _extractor, _filter);
        }
    private:
        void init(src_iterator_t _begin, src_iterator_t _end, src_value_extractor_t _extractor, src_value_filter_t _filter)
        {
            if (!_extractor)
                throw makeStringException(-1, "missing TIteratorOfShared value extractor");
            if (_filter)
            {
                for (src_iterator_t it = _begin; it != _end; ++it)
                {
                    if (_filter(it))
                        cache.emplace_back(&_extractor(it));
                }
            }
            else
            {
                for (src_iterator_t it = _begin; it != _end; ++it)
                    cache.emplace_back(&_extractor(it));
            }
            this->begin = cache.begin();
            this->cur = cache.end();
            this->end = cache.end();
            this->extractor = extractShared<value_t, typename std::list<Linked<value_t> >::const_iterator>;
        }
    };

    /**
     * @brief Extension of `TIeratorOfScalar<>` that extracts and caches text from a range of
     *        iterators.
     *
     * There are scenarios where the overhead of copying text values is unavoidable, such as the
     * iteration of a mutable collection. This extracts and copies the text values from mutable
     * collections, and may also be used to persist values beyond the next call to `next()`.
     *
     * The creator of an instance is responsible for syncronizing access to the mutable data.
     *
     * @tparam src_iterator_t
     */
    template <typename src_iterator_t>
    class TCachedTextIterator : public TIteratorOfScalar<const char*, std::list<std::string>::const_iterator>
    {
    protected:
        std::list<std::string> cache;
    public:
        using src_value_extractor_t = std::function<const char*&(src_iterator_t)>;
        TCachedTextIterator(src_iterator_t begin, src_iterator_t end, src_value_extractor_t extractor)
        {
            for (src_iterator_t it = begin; it != end; ++it)
                cache.emplace_back(extractor(it));
            this->begin = cache.begin();
            this->cur = cache.end();
            this->end = cache.end();
            this->extractor = StdStringExtractor<std::list<std::string>::const_iterator>();
        }
    };

} // namespace DataMasking
