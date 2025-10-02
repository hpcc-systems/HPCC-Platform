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
        std::set<std::pair<std::string, FilterTermComparison>> patterns;

    public:
        bool matches(const char* value) const
        {
            bool match = false;
            for (const std::pair<std::string, FilterTermComparison>& pattern : patterns)
            {
                switch (pattern.second)
                {
                case FilterTermComparison::Default:
                case FilterTermComparison::Wildcard:
                    match = WildMatch(value, pattern.first.c_str(), false);
                    break;
                case FilterTermComparison::Equal:
                    match = strieq(value, pattern.first.c_str());
                    break;
                case FilterTermComparison::NotEqual:
                    match = !strieq(value, pattern.first.c_str());
                    break;
                case FilterTermComparison::LessThan:
                    match = stricmp(value, pattern.first.c_str()) < 0;
                    break;
                case FilterTermComparison::LessThanOrEqual:
                    match = stricmp(value, pattern.first.c_str()) <= 0;
                    break;
                case FilterTermComparison::GreaterThanOrEqual:
                    match = stricmp(value, pattern.first.c_str()) >= 0;
                    break;
                case FilterTermComparison::GreaterThan:
                    match = stricmp(value, pattern.first.c_str()) > 0;
                    break;
                default:
                    match = false;
                    break;
                }
                if (match)
                    return true;
            }
            return false;
        }

        bool acceptToken(const char* token, FilterTermComparison comp)
        {
            (void)patterns.emplace(token, comp);
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
                const char* token = tokens.item(i);
                if (isEmptyString(token))
                    continue;
                FilterTermComparison comp = CEventFilter::extractComparison(token);
                if (!acceptToken(token, comp))
                    return false;
            }
            return true;
        }
        virtual bool acceptToken(const char* token, FilterTermComparison comp) = 0;
    };

    struct StringFilterTerm : public FilterTerm
    {
        StringMatchHelper helper;

        bool matches(const CEvent&, const CEventAttribute& attribute) const override
        {
            return helper.matches(attribute.queryTextValue());
        }

        bool acceptToken(const char* token, FilterTermComparison comp) override
        {
            return helper.acceptToken(token, comp);
        }
    };

    struct BoolFilterTerm : public FilterTerm
    {
        bool accepted[2]{false, false};

        bool matches(const CEvent&, const CEventAttribute& attribute) const override
        {
            return accepted[attribute.queryBooleanValue()];
        }

        bool acceptToken(const char* token, FilterTermComparison comp) override
        {
            assertex(FilterTermComparison::Default == comp);
            bool value = strToBool(token);
            accepted[value] = true;
            return true;
        }
    };

    struct UnsignedFilterTerm : public FilterTerm
    {
        std::set<std::pair<std::pair<__uint64, __uint64>, FilterTermComparison>> accepted;

        bool matches(const CEvent&, const CEventAttribute& attribute) const override
        {
            __uint64 value = attribute.queryNumericValue();
            bool match = false;
            for (const std::pair<std::pair<__uint64, __uint64>, FilterTermComparison>& range : accepted)
            {
                switch (range.second)
                {
                case FilterTermComparison::Default:
                case FilterTermComparison::ElementOf:
                    match = (range.first.first <= value && value <= range.first.second);
                    break;
                case FilterTermComparison::NotElementOf:
                    match = (value < range.first.first || range.first.second < value);
                    break;
                case FilterTermComparison::Equal:
                    match = (range.first.first == value);
                    break;
                case FilterTermComparison::NotEqual:
                    match = (range.first.first != value);
                    break;
                case FilterTermComparison::LessThan:
                    match = (value < range.first.first);
                    break;
                case FilterTermComparison::LessThanOrEqual:
                    match = (value <= range.first.first);
                    break;
                case FilterTermComparison::GreaterThanOrEqual:
                    match = (range.first.second <= value);
                    break;
                case FilterTermComparison::GreaterThan:
                    match = (range.first.second < value);
                    break;
                default:
                    break;
                }
                if (match)
                    return true;
            }
            return false;
        }

        bool acceptToken(const char* token, FilterTermComparison comp) override
        {
            char* next = nullptr;
            const char* delim = strchr(token, '-');
            if (!delim)
            {
                __uint64 first = strtoull(token, &next, 0);
                if (*next == '\0')
                    return acceptRange(first, first, comp);
            }
            else if (delim == token)
            {
                __uint64 last = strtoull(delim + 1, &next, 0);
                if (*next == '\0')
                    return acceptRange(0, last, comp);
            }
            else
            {
                __uint64 first = strtoull(token, &next, 0);
                while (isspace(*next))
                    next++;
                if (*next == '-')
                {
                    if (!*(next + 1))
                        return acceptRange(first, UINT64_MAX, comp);
                    __uint64 last = strtoull(next + 1, &next, 0);
                    if (*next == '\0')
                        return acceptRange(first, last, comp);
                }
            }
            return false;
        }

        bool acceptRange(__uint64 first, __uint64 last, FilterTermComparison comp)
        {
            switch (comp)
            {
            case FilterTermComparison::Equal:
            case FilterTermComparison::NotEqual:
                if (first != last)
                    return false;
                break;
            default:
                break;
            }
            if (first <= last)
                return accepted.insert({{first, last}, comp}).second;
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
            switch (attribute.queryId())
            {
            case EvAttrFileId:
                return matchedPaths.count(attribute.queryNumericValue()) || UnsignedFilterTerm::matches(event, attribute);
            case EvAttrPath:
                return matchedPaths.count(event.queryNumericValue(EvAttrFileId));
            default:
                return false;
            }
        }

        bool acceptToken(const char* token, FilterTermComparison comp) override
        {
            if (UnsignedFilterTerm::acceptToken(token, comp))
                return true;
            // Token not a number or range; assume it is a path pattern
            return helper.acceptToken(token, comp);
        }

        StringMatchHelper helper;
        mutable std::set<__uint64> matchedPaths;
    };

    struct TimestampFilterTerm : public UnsignedFilterTerm
    {
        bool acceptToken(const char* token, FilterTermComparison comp) override
        {
            // Digits and hyphen only is presumed to be a nanosecond timestamp range.
            std::string tmp(token);
            if (tmp.find_first_not_of("0123456789-") == std::string::npos)
                return UnsignedFilterTerm::acceptToken(token, comp);

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
                if (isEmptyString(delim)) // single value
                    last = first;
                else
                {
                    CEventFilter::skipSpaces(delim);
                    if ('-' == *delim) // a range...
                    {
                        token = delim + 1;
                        if (*token) // with bounded end
                        {
                            CEventFilter::skipSpaces(token);
                            delim = nullptr;
                            dt.setString(token, &delim);
                            if (delim && *delim) // bad content
                                return false;
                            last = dt.getTimeStampNs();
                        }
                        else // with unbounded end
                            last = UINT64_MAX;
                    }
                    else // bad content
                        return false;
                }
            }
            return acceptRange(first, last, comp);
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
    virtual void configure(const IPropertyTree& config) override
    {
        Owned<IPropertyTreeIterator> termsIt = config.getElements("*");
        ForEach(*termsIt)
        {
            const IPTree& termNode = termsIt->query();
            const char* termName = termNode.queryName();
            if (streq(termName, "attribute"))
            {
                EventAttr id = queryEventAttribute(termNode.queryProp("@id"));
                const char* values = termNode.queryProp("@values");
                if (EvAttrNone == id && isEmptyString(values))
                    throw makeStringException(-1, "missing event attribute filter id and values");
                if (EvAttrNone == id)
                    throw makeStringExceptionV(-1, "missing event attribute filter id for values '%s'", values);
                if (isEmptyString(values))
                    throw makeStringExceptionV(-1, "missing event attribute filter values for id %s", queryEventAttributeName(id));
                if (!acceptAttribute(id, values))
                    throw makeStringExceptionV(-1, "event filter attribute %s term '%s' not accepted", queryEventAttributeName(id), values);
            }
            else if (streq(termName, "event"))
            {
                if (termNode.hasProp("@list"))
                {
                    const char* token = termNode.queryProp("@list");
                    if (!acceptEvents(token))
                        throw makeStringExceptionV(-1, "event list term '%s' not accepted", token);
                }
                else if (termNode.hasProp("@type"))
                {
                    const char* token = termNode.queryProp("@type");
                    FilterTermComparison comp = extractComparison(token);
                    EventType type = queryEventType(token);
                    if (EventNone == type)
                        throw makeStringExceptionV(-1, "event filter event term has unknown type name '%s'", token);
                    if (!IEventFilter::acceptEvent(type))
                        throw makeStringExceptionV(-1, "event type term '%s' not accepted", token);
                }
                else if (termNode.hasProp("@context"))
                {
                    const char* token = termNode.queryProp("@context");
                    FilterTermComparison comp = extractComparison(token);
                    EventContext context = queryEventContext(token);
                    if (EventCtxOther == context)
                        throw makeStringExceptionV(-1, "event filter context term has unknown context name '%s'", token);
                    if (!IEventFilter::acceptEvents(context))
                        throw makeStringExceptionV(-1, "event context term '%s' not accepted", token);
                }
            }
        }
    }

    virtual bool acceptEvent(EventType type, FilterTermComparison comp) override
    {
        switch (comp)
        {
        case FilterTermComparison::Default:
        case FilterTermComparison::Equal:
            (void)acceptedEvents.insert(type);
            break;
        case FilterTermComparison::Except:
            (void)acceptedEvents.erase(type);
            break;
        case FilterTermComparison::NotEqual:
            for (byte t = EventNone + 1; t < EventMax; t++)
            {
                if (EventType(t) != type)
                    (void)acceptedEvents.insert(EventType(t));
            }
            break;
        default:
            return false;
        }
        return true;
    }

    virtual bool acceptEvents(EventContext context, FilterTermComparison comp) override
    {
        for (byte type = EventNone + 1; type < EventMax; type++)
        {
            bool matched = (queryEventContext(EventType(type)) == context);
            switch (comp)
            {
            case FilterTermComparison::Default:
            case FilterTermComparison::ElementOf:
                if (matched)
                    (void)acceptedEvents.insert(EventType(type));
                break;
            case FilterTermComparison::Except:
                if (matched)
                    (void)acceptedEvents.erase(EventType(type));
                break;
            case FilterTermComparison::NotElementOf:
                if (!matched)
                    (void)acceptedEvents.insert(EventType(type));
                break;
            default:
                return false;
            }
        }
        return true;
    }

    virtual bool acceptEvents(const char* events) override
    {
        StringArray tokens;
        tokens.appendList(events, ",", true);
        ForEachItemIn(i, tokens)
        {
            const char* token = tokens.item(i);
            FilterTermComparison comp = extractComparison(token);
            EventContext context = queryEventContext(token);
            if (context != EventCtxMax)
            {
                if (!acceptEvents(context, comp))
                    return false;
                continue;
            }
            EventType type = queryEventType(token);
            if (EventNone == type || !acceptEvent(type, comp))
                return false;
        }
        return true;
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

    // Examines the given token string for an optional comparison designation at thestart of the
    // token. If found, the designation text is translated into the corresponding enumerated value
    // and the token is updated to advance beyond the designation.
    //
    // Given `[in]index` on input, the return value is FilterTermComparison::ElementOf and the
    // input parameter is updated to `index`.
    static FilterTermComparison extractComparison(const char*& token)
    {
        assertex(!isEmptyString(token));
        skipSpaces(token);
        if (token[0] != '[')
            return FilterTermComparison::Default;
        skipSpaces(token);
        const char* close = strchr(token + 1, ']');
        if (!close)
            throw makeStringExceptionV(-1, "invalid filter term syntax - '%s' incomplete comparison selector", token);
        StringBuffer compStr(close - token - 1, token + 1);
        compStr.trim();
        if (compStr.isEmpty())
            return FilterTermComparison::Default;
        std::map<std::string, FilterTermComparison>::const_iterator it = comparisonMap.find(compStr.str());
        if (comparisonMap.end() == it)
            throw makeStringExceptionV(-1, "invalid filter term syntax - '%s' has unknown comparison selector", token);
        token = close + 1;
        skipSpaces(token);
        return it->second;
    }

    static void skipSpaces(const char*& token)
    {
        while (isspace(*token))
            token++;
    }

protected:
    static std::map<std::string, FilterTermComparison> comparisonMap;
    Owned<FilterTerm> terms[EvAttrMax];
    std::unordered_set<EventType> acceptedEvents;
};

std::map<std::string, FilterTermComparison> CEventFilter::comparisonMap{
    // event attribute value comparisons
    {"eq", FilterTermComparison::Equal},
    {"neq", FilterTermComparison::NotEqual},
    {"lt", FilterTermComparison::LessThan},
    {"lte", FilterTermComparison::LessThanOrEqual},
    {"gte", FilterTermComparison::GreaterThanOrEqual},
    {"gt", FilterTermComparison::GreaterThan},
    {"in", FilterTermComparison::ElementOf},
    {"out", FilterTermComparison::NotElementOf},
    {"wild", FilterTermComparison::Wildcard},
    // event type comparison(s)
    {"except", FilterTermComparison::Except},
};

IEventFilter* createEventFilter()
{
    return new CEventFilter;
}

IEventFilter* createEventFilter(const IPropertyTree& config)
{
    Owned<IEventFilter> filter = createEventFilter();
    filter->configure(config);
    return filter.getClear();
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class EventFilterTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventFilterTests);
    CPPUNIT_TEST(testFilterByEventsUnfiltered);
    CPPUNIT_TEST(testFilterByEventsByContext);
    CPPUNIT_TEST(testFilterByEventsByType);
    CPPUNIT_TEST(testFilterByEventsByContextComplement);
    CPPUNIT_TEST(testFilterByEventsByTypeComplement);
    CPPUNIT_TEST(testFilterByEventsWithGoodException);
    CPPUNIT_TEST(testFilterByEventsWithIrrelevantException);
    CPPUNIT_TEST(testFilterByEventByCanceledTerms);
    CPPUNIT_TEST(testFilterByEventByBadName);
    CPPUNIT_TEST(testFilterByEventByBadComparison);
    CPPUNIT_TEST(testFilterByAttributeByFileId);
    CPPUNIT_TEST(testFilterByAttributeByPath1);
    CPPUNIT_TEST(testFilterByAttributeByPath2);
    CPPUNIT_TEST(testFilterByAttributeByPath3);
    CPPUNIT_TEST(testFileByttributeByBool1);
    CPPUNIT_TEST(testFileByttributeByBool2);
    CPPUNIT_TEST(testFileByAttributeByTimestamp1);
    CPPUNIT_TEST(testFileByAttributeByTimestamp2);
    CPPUNIT_TEST(testFileByAttributeByTimestamp3);
    CPPUNIT_TEST_SUITE_END();

public:
    void testFilterByEventsUnfiltered()
    {
        EventType foo;
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </input>
                <expect>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByEventsByContext()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list="Index"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </input>
                <expect>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="MetaFileInformation"/>
                    <event type="IndexPayload"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByEventsByType()
    {
        EventType foo;
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list="IndexLoad,IndexEviction"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </input>
                <expect>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByEventsByContextComplement()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list=" [ out ] dali"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </input>
                <expect>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByEventsByTypeComplement()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list="[neq]RecordingActive"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </input>
                <expect>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="IndexPayload"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByEventsWithGoodException()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list="index,[except]IndexPayload"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </input>
                <expect>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="MetaFileInformation"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByEventsWithIrrelevantException()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list="index,[except]DaliCommit"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </input>
                <expect>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="IndexPayload"/>
                    <event type="MetaFileInformation"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByEventByCanceledTerms()
    {
        EventType foo;
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list="[in]index"/>
                    <event list="[out]index"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </input>
                <expect>
                    <event type="IndexCacheHit"/>
                    <event type="IndexCacheMiss"/>
                    <event type="IndexLoad"/>
                    <event type="IndexEviction"/>
                    <event type="DaliChangeMode"/>
                    <event type="DaliCommit"/>
                    <event type="DaliConnect"/>
                    <event type="DaliEnsureLocal"/>
                    <event type="DaliGet"/>
                    <event type="DaliGetChildren"/>
                    <event type="DaliGetChildrenFor"/>
                    <event type="DaliGetElements"/>
                    <event type="DaliSubscribe"/>
                    <event type="MetaFileInformation"/>
                    <event type="EventRecordingActive"/>
                    <event type="IndexPayload"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByEventByBadName()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list="bogusname"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception for bad event filter term name", testEventVisitationLinks(testData, false), std::exception);
    }

    void testFilterByEventByBadComparison()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <event list="[hih]Index"/>
                </link>
                <input>
                    <event type="IndexCacheHit"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception for bad event filter term comparison", testEventVisitationLinks(testData, false), std::exception);
    }

    void testFilterByAttributeByFileId()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="fileId" values="[lte]1,3-4,*6*,[gt]7"/>
                    <event list="[neq]FileInformation"/>
                </link>
                <input>
                    <event type="FileInformation" FileId="1" Path="/path/to/file/1.txt"/>
                    <event type="FileInformation" FileId="2" Path="/path/to/file/2.txt"/>
                    <event type="FileInformation" FileId="3" Path="/path/to/file/3.txt"/>
                    <event type="FileInformation" FileId="4" Path="/path/to/file/4.txt"/>
                    <event type="FileInformation" FileId="5" Path="/path/to/file/5.txt"/>
                    <event type="FileInformation" FileId="6" Path="/path/to/file/6.txt"/>
                    <event type="FileInformation" FileId="7" Path="/path/to/file/7.txt"/>
                    <event type="FileInformation" FileId="8" Path="/path/to/file/8.txt"/>
                    <event type="IndexCacheMiss" FileId="1"/>
                    <event type="IndexCacheMiss" FileId="2"/>
                    <event type="IndexCacheMiss" FileId="3"/>
                    <event type="IndexCacheMiss" FileId="4"/>
                    <event type="IndexCacheMiss" FileId="5"/>
                    <event type="IndexCacheMiss" FileId="6"/>
                    <event type="IndexCacheMiss" FileId="7"/>
                    <event type="IndexCacheMiss" FileId="8"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1"/>
                    <event type="IndexCacheMiss" FileId="3"/>
                    <event type="IndexCacheMiss" FileId="4"/>
                    <event type="IndexCacheMiss" FileId="6"/>
                    <event type="IndexCacheMiss" FileId="8"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByAttributeByPath1()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="Path" values="[lt]/path/to/file/2.txt,[gte]/path/to/file/8.txt,/path/to/file/3.txt,*5.txt"/>
                </link>
                <input>
                    <event type="FileInformation" FileId="1" Path="/path/to/file/1.txt"/>
                    <event type="FileInformation" FileId="2" Path="/path/to/file/2.txt"/>
                    <event type="FileInformation" FileId="3" Path="/path/to/file/3.txt"/>
                    <event type="FileInformation" FileId="4" Path="/path/to/file/4.txt"/>
                    <event type="FileInformation" FileId="5" Path="/path/to/file/5.txt"/>
                    <event type="FileInformation" FileId="6" Path="/path/to/file/6.txt"/>
                    <event type="FileInformation" FileId="7" Path="/path/to/file/7.txt"/>
                    <event type="FileInformation" FileId="8" Path="/path/to/file/8.txt"/>
                </input>
                <expect>
                    <event type="FileInformation" FileId="1" Path="/path/to/file/1.txt"/>
                    <event type="FileInformation" FileId="3" Path="/path/to/file/3.txt"/>
                    <event type="FileInformation" FileId="5" Path="/path/to/file/5.txt"/>
                    <event type="FileInformation" FileId="8" Path="/path/to/file/8.txt"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByAttributeByPath2()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="Path" values="[gt]/path/to/file/6.txt,[lte]/path/to/file/3.txt"/>
                </link>
                <input>
                    <event type="FileInformation" FileId="1" Path="/path/to/file/1.txt"/>
                    <event type="FileInformation" FileId="2" Path="/path/to/file/2.txt"/>
                    <event type="FileInformation" FileId="3" Path="/path/to/file/3.txt"/>
                    <event type="FileInformation" FileId="4" Path="/path/to/file/4.txt"/>
                    <event type="FileInformation" FileId="5" Path="/path/to/file/5.txt"/>
                    <event type="FileInformation" FileId="6" Path="/path/to/file/6.txt"/>
                    <event type="FileInformation" FileId="7" Path="/path/to/file/7.txt"/>
                    <event type="FileInformation" FileId="8" Path="/path/to/file/8.txt"/>
                </input>
                <expect>
                    <event type="FileInformation" FileId="1" Path="/path/to/file/1.txt"/>
                    <event type="FileInformation" FileId="2" Path="/path/to/file/2.txt"/>
                    <event type="FileInformation" FileId="3" Path="/path/to/file/3.txt"/>
                    <event type="FileInformation" FileId="7" Path="/path/to/file/7.txt"/>
                    <event type="FileInformation" FileId="8" Path="/path/to/file/8.txt"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFilterByAttributeByPath3()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="Path" values="[neq]/path/to/file/1.txt"/>
                </link>
                <input>
                    <event type="FileInformation" FileId="1" Path="/path/to/file/1.txt"/>
                    <event type="FileInformation" FileId="2" Path="/path/to/file/2.txt"/>
                    <event type="FileInformation" FileId="3" Path="/path/to/file/3.txt"/>
                    <event type="FileInformation" FileId="4" Path="/path/to/file/4.txt"/>
                    <event type="FileInformation" FileId="5" Path="/path/to/file/5.txt"/>
                    <event type="FileInformation" FileId="6" Path="/path/to/file/6.txt"/>
                    <event type="FileInformation" FileId="7" Path="/path/to/file/7.txt"/>
                    <event type="FileInformation" FileId="8" Path="/path/to/file/8.txt"/>
                </input>
                <expect>
                    <event type="FileInformation" FileId="2" Path="/path/to/file/2.txt"/>
                    <event type="FileInformation" FileId="3" Path="/path/to/file/3.txt"/>
                    <event type="FileInformation" FileId="4" Path="/path/to/file/4.txt"/>
                    <event type="FileInformation" FileId="5" Path="/path/to/file/5.txt"/>
                    <event type="FileInformation" FileId="6" Path="/path/to/file/6.txt"/>
                    <event type="FileInformation" FileId="7" Path="/path/to/file/7.txt"/>
                    <event type="FileInformation" FileId="8" Path="/path/to/file/8.txt"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFileByttributeByBool1()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="Enabled" values="true"/>
                    <attribute id="FirstUse" values="false"/>
                </link>
                <input>
                    <event type="RecordingActive" Enabled="true"/>
                    <event type="IndexPayload" FirstUse="true"/>
                    <event type="RecordingActive" Enabled="false"/>
                    <event type="IndexPayload" FirstUse="false"/>
                </input>
                <expect>
                    <event type="RecordingActive" Enabled="true"/>
                    <event type="IndexPayload" FirstUse="false"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFileByttributeByBool2()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="Enabled" values="true,0"/>
                    <attribute id="FirstUse" values="1,false"/>
                </link>
                <input>
                    <event type="RecordingActive" Enabled="true"/>
                    <event type="IndexPayload" FirstUse="true"/>
                    <event type="RecordingActive" Enabled="false"/>
                    <event type="IndexPayload" FirstUse="false"/>
                </input>
                <expect>
                    <event type="RecordingActive" Enabled="true"/>
                    <event type="IndexPayload" FirstUse="true"/>
                    <event type="RecordingActive" Enabled="false"/>
                    <event type="IndexPayload" FirstUse="false"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFileByAttributeByTimestamp1()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="EventTimestamp" values="-2024-01-01T08:01:00"/>
                    <attribute id="EventTimestamp" values="2024-01-01T10:30:00-"/>
                    <attribute id="EventTimestamp" values="2024-01-01T09:00:00-2024-01-01T10:01:00"/>
                    <attribute id="EventTimestamp" values="2024-01-01T08:30:45"/>
                </link>
                <input>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:00:00"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:15:30"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:30:45"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:45:12"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:00:25"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:15:38"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:30:50"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:45:05"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:00:18"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:15:33"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:30:47"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:45:59"/>
                </input>
                <expect>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:00:00"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:30:45"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:00:25"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:15:38"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:30:50"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:45:05"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:00:18"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:30:47"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:45:59"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFileByAttributeByTimestamp2()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="EventTimestamp" values="[lt]2024-01-01T08:01:00"/>
                    <attribute id="EventTimestamp" values="[gt]2024-01-01T10:30:00"/>
                    <attribute id="EventTimestamp" values="2024-01-01T09:00:00-2024-01-01T10:01:00"/>
                    <attribute id="EventTimestamp" values="2024-01-01T08:30:45"/>
                </link>
                <input>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:00:00"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:15:30"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:30:45"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:45:12"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:00:25"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:15:38"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:30:50"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:45:05"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:00:18"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:15:33"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:30:47"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:45:59"/>
                </input>
                <expect>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:00:00"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:30:45"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:00:25"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:15:38"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:30:50"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:45:05"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:00:18"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:30:47"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:45:59"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }

    void testFileByAttributeByTimestamp3()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link kind="event-filter">
                    <attribute id="EventTimestamp" values="[out]2024-01-01T09:00:00-2024-01-01T10:01:00"/>
                </link>
                <input>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:00:00"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:15:30"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:30:45"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:45:12"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:00:25"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:15:38"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:30:50"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T09:45:05"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:00:18"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:15:33"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:30:47"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:45:59"/>
                </input>
                <expect>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:00:00"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:15:30"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:30:45"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T08:45:12"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:15:33"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:30:47"/>
                    <event type="RecordingActive" EventTimestamp="2024-01-01T10:45:59"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventFilterTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventFilterTests, "eventfilter");

#endif // _USE_CPPUNIT
