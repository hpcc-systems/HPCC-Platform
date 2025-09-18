/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#include "eventfilter.h"
#include "jregexp.hpp"
#include "jutil.hpp"
#include <set>
#include <unordered_set>

class CEventFilter : public CInterfaceOf<IEventFilter>
{
protected:
    class StringMatchHelper
    {
    private:
        std::set<std::string> patterns;

    public:
        bool matches(const char* value) const
        {
            for (const std::string& pattern : patterns)
            {
                if (WildMatch(value, pattern.c_str(), false))
                    return true;
            }
            return false;
        }

        bool acceptToken(const char* token)
        {
            patterns.insert(token);
            return true;
        }
    };

    struct FilterTerm : public CInterface
    {
        virtual void observe(const CEvent& event) const {}
        virtual bool matches(const CEvent& event, const CEventAttribute& attribute) const { return true; }
        virtual bool accept(const char* values)
        {
            StringArray tokens;
            tokens.appendList(values, ",", true);
            ForEachItemIn(i, tokens)
            {
                if (!acceptToken(tokens.item(i)))
                    return false;
            }
            return true;
        }
        virtual bool acceptToken(const char* token) = 0;
    };

    struct StringFilterTerm : public FilterTerm
    {
        StringMatchHelper helper;

        bool matches(const CEvent&, const CEventAttribute& attribute) const override
        {
            return helper.matches(attribute.queryTextValue());
        }

        bool acceptToken(const char* token) override
        {
            return helper.acceptToken(token);
        }
    };

    struct BoolFilterTerm : public FilterTerm
    {
        bool accepted[2]{false, false};

        bool matches(const CEvent&, const CEventAttribute& attribute) const override
        {
            return accepted[attribute.queryBooleanValue()];
        }

        bool acceptToken(const char* token) override
        {
            bool value = strToBool(token);
            accepted[value] = true;
            return true;
        }
    };

    struct UnsignedFilterTerm : public FilterTerm
    {
        std::set<std::pair<__uint64, __uint64>> accepted;

        bool matches(const CEvent&, const CEventAttribute& attribute) const override
        {
            __uint64 value = attribute.queryNumericValue();
            for (const std::pair<__uint64, __uint64>& range : accepted)
            {
                if (range.first <= value && value <= range.second)
                    return true;
            }
            return false;
        }

        bool acceptToken(const char* token) override
        {
            char* next = nullptr;
            const char* delim = strchr(token, '-');
            if (!delim)
            {
                __uint64 first = strtoull(token, &next, 0);
                if (*next == '\0')
                    return acceptRange(first, first);
            }
            else if (delim == token)
            {
                __uint64 last = strtoull(delim + 1, &next, 0);
                if (*next == '\0')
                    return acceptRange(0, last);
            }
            else
            {
                __uint64 first = strtoull(token, &next, 0);
                while (isspace(*next))
                    next++;
                if (*next == '-')
                {
                    if (!*(next + 1))
                        return acceptRange(first, UINT64_MAX);
                    __uint64 last = strtoull(next + 1, &next, 0);
                    if (*next == '\0')
                        return acceptRange(first, last);
                }
            }
            return false;
        }

        bool acceptRange(__uint64 first, __uint64 last)
        {
            if (first <= last)
                return accepted.insert(std::make_pair(first, last)).second;
            return false;
        }
    };

    struct FileIdFilterTerm : public UnsignedFilterTerm
    {
        virtual void observe(const CEvent& event) const override
        {
            if (MetaFileInformation == event.queryType())
            {
                if (helper.matches(event.queryTextValue(EvAttrPath)))
                    matchedPaths.insert(event.queryNumericValue(EvAttrFileId));
            }
        }

        bool matches(const CEvent& event, const CEventAttribute& attribute) const override
        {
            return matchedPaths.count(attribute.queryNumericValue()) || UnsignedFilterTerm::matches(event, attribute);
        }

        bool acceptToken(const char* token) override
        {
            if (UnsignedFilterTerm::acceptToken(token))
                return true;
            // Token not a number or range; assume it is a path pattern
            return helper.acceptToken(token);
        }

        StringMatchHelper helper;
        mutable std::set<__uint64> matchedPaths;
    };

    struct TimestampFilterTerm : public UnsignedFilterTerm
    {
        bool acceptToken(const char* token) override
        {
            // Digits and hyphen only is presumed to be a nanosecond timestamp range.
            std::string tmp(token);
            if (tmp.find_first_not_of("0123456789-") == std::string::npos)
                return UnsignedFilterTerm::acceptToken(token);

            // All else is presumed to be a date/time string or a date/time range.
            __uint64 first = 0, last = UINT64_MAX;
            CDateTime dt;
            const char* delim = nullptr;
            if (*token == '-') // range starting at 0
            {
                dt.setString(token + 1, &delim);
                if (delim && *delim) // bad content
                    return false;
                last = dt.getTimeStampNs();
            }
            else // single value or range not starting at 0
            {
                dt.setString(token, &delim);
                first = dt.getTimeStampNs();
                if (!delim) // single value
                    last = first;
                else
                {
                    skipSpaces(delim);
                    if ('-' == *delim) // a range...
                    {
                        token = delim + 1;
                        if (*token) // with bounded end
                        {
                            skipSpaces(token);
                            delim = nullptr;
                            dt.setString(token, &delim);
                            if (delim && *delim) // bad content
                                return false;
                            last = dt.getTimeStampNs();
                        }
                        else // with unbounded end
                            last = first;
                    }
                    else // bad content
                        return false;
                }
            }
            return acceptRange(first, last);
        }

        void skipSpaces(const char*& token)
        {
            while (isspace(*token))
                token++;
        }
    };

public: // IEventVisitationLink
    IMPLEMENT_IEVENTVISITATIONLINK;

    virtual bool visitEvent(CEvent& event) override
    {
        // Allow all filter terms the opportunity to see the event before it may be filtered.
        // Observation allows, for example, the relationship between file ID and path to be
        // established for enhanced filtering of index events.
        for (unsigned termIdx = EvAttrNone + 1; termIdx < EvAttrMax; termIdx++)
        {
            if (terms[termIdx])
                terms[termIdx]->observe(event);
        }

        // Apply event type filters. Event filters cannot cause the remainder of the file to be
        // skipped, so non-acceptance implies coninuation.
        if (!acceptedEvents.empty() && !acceptedEvents.count(event.queryType()))
            return true;

        // Apply attribute filters. Attribute filters do not cause the remainder of the file to be
        // skipped, so non-acceptance implies coninuation.
        for (const CEventAttribute& attr : event.assignedAttributes)
        {
            const FilterTerm* term = terms[attr.queryId()];
            if (term && !term->matches(event, attr))
                return true;
        }

        // Forward the event to the next linked visitor.
        return nextLink->visitEvent(event);
    }

public: // IEventFilter
    virtual bool acceptEvent(EventType type) override
    {
        return (acceptedEvents.insert(type).second);
    }

    virtual bool acceptEvents(EventContext context) override
    {
        bool changed = false;
        for (byte type = EventNone + 1; type < EventMax; type++)
        {
            if (queryEventContext(EventType(type)) == context)
            {
                if (acceptedEvents.insert(EventType(type)).second && !changed)
                    changed = true;
            }
        }
        return changed;
    }

    virtual bool acceptEvents(const char* events) override
    {
        StringArray tokens;
        tokens.appendList(events, ",", true);
        bool changed = false;
        ForEachItemIn(i, tokens)
        {
            EventContext context = queryEventContext(tokens.item(i));
            if (context != EventCtxMax)
            {
                if (acceptEvents(context) && !changed)
                    changed = true;
                continue;
            }
            EventType type = queryEventType(tokens.item(i));
            if (type != EventNone)
            {
                if (acceptEvent(type) && !changed)
                    changed = true;
                continue;
            }
        }
        return changed;
    }

    virtual bool acceptAttribute(EventAttr id, const char* values) override
    {
        if (EvAttrFileId == id)
        {
            // Special case to join EventIndexCacheHit, EventIndexCacheMiss, EventIndexLoad, and
            // EventIndexEviction with MetaFileInformation by the common EvAttrFileId attribute. The
            // result allows the index events to be filtered by path using the file ID.
            //
            // MetaFileInformation events cannot be filtered by file ID.
            FilterTerm* term = ensureTerm<FileIdFilterTerm>(id);
            if (!terms[EvAttrPath])
                terms[EvAttrPath].set(term);
            else if (terms[EvAttrPath] != term)
                throw makeStringException(-1, "event attribute EvAttrPath has a conflicting filter term for EvAttrFileId");
            return term->accept(values);
        }
        else
        {
            switch (queryEventAttributeType(id))
            {
            case EATtraceid:
            case EATstring:
                return ensureTerm<StringFilterTerm>(id)->accept(values);
            case EATbool:
                return ensureTerm<BoolFilterTerm>(id)->accept(values);
            case EATu1:
            case EATu2:
            case EATu4:
            case EATu8:
                return ensureTerm<UnsignedFilterTerm>(id)->accept(values);
            case EATtimestamp:
                return ensureTerm<TimestampFilterTerm>(id)->accept(values);
            case EATnone:
            case EATmax:
                throw makeStringExceptionV(-1, "event attribute id %d has an invalid type %d", int(id), int(queryEventAttributeType(id)));
            default:
                throw makeStringExceptionV(-1, "event attribute id %d has an unknown type %d", int(id), int(queryEventAttributeType(id)));
            }
        }
    }

protected:
    template <typename term_type_t>
    FilterTerm* ensureTerm(EventAttr id)
    {
        if (id <= EvAttrNone || id >= EvAttrMax)
            throw makeStringExceptionV(-1, "event attribute id %d out of range", int(id));
        if (!terms[id])
            terms[id].setown(new term_type_t);
#if defined(_DEBUG)
        else if (dynamic_cast<term_type_t*>(terms[id].get()) == nullptr)
            throw makeStringExceptionV(-1, "event attribute id %d has a different type of filter term", int(id));
#endif
        return terms[id].get();
    }

protected:
    Owned<FilterTerm> terms[EvAttrMax];
    std::unordered_set<EventType> acceptedEvents;
};

IEventFilter* createEventFilter()
{
    return new CEventFilter;
}
