/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#include "txsummary.hpp"
#include "jlog.hpp"
#include "jutil.hpp"
#include <algorithm>
#include "espcontext.hpp"
#include "jmetrics.hpp"

#define MATCH_ENTRY [&](const EntryValue& e) {return strieq(e.get()->name, pathPart);}

static auto pRequestCount = hpccMetrics::createMetricAndAddToReporter<hpccMetrics::CounterMetric>("requests", "Number of Requests");

inline bool validate(const char* k)
{
    // Empty or null keys are invalid
    if(isEmptyString(k))
        return false;
    // Keys containing an empty path portion
    // are invalid
    if(strstr(k, ".."))
        return false;
    // Keys beginning or ending in the path
    // delimiter are considered equivalent to
    // containing an empty path portion and
    // therefore are invalid
    if( ('.' == k[0]) || ('.' == k[strlen(k)-1]))
        return false;
    return true;
}

CTxSummary::TxEntryBase::TxEntryBase(const char* _key, const LogLevel _logLevel, const unsigned int _group, const char* _suffix, bool _jsonQuoted)
    : logLevel(_logLevel), group(_group), suffix(_suffix), shouldJsonQuote(_jsonQuoted)
{
    // parse dot-delimited key
    // 'name' is set to the rightmost element of _key
    // 'fullname' is the entire _key

    if(isEmptyString(_key))
    {
        fullname.set(_key);
        name = nullptr;
    }
    else
    {
        fullname.set(_key);
        const char* finger = strrchr(fullname.str(), '.');
        if(finger)
            name = ++finger;
        else
            name = fullname.str();
    }
}

bool CTxSummary::TxEntryBase::shouldSerialize(const LogLevel requestedLevel, const unsigned int requestedGroup)
{
    if(logLevel > requestedLevel)
        return false;

    if(!(group & requestedGroup))
        return false;

    return true;
}

StringBuffer& CTxSummary::TxEntryStr::serialize(StringBuffer& buf, const LogLevel requestedLevel, const unsigned int requestedGroup, const unsigned int requestedStyle)
{
    if(!shouldSerialize(requestedLevel, requestedGroup) || isEmptyString(name))
        return buf;

    if(requestedStyle & TXSUMMARY_OUT_JSON)
    {
        appendJSONStringValue(buf, name, value.get(), false, shouldJsonQuote);
    }
    else if (requestedStyle & TXSUMMARY_OUT_TEXT)
    {
        buf.append(fullname);

        if (value.length())
            buf.appendf("=%s;", value.str());
        else
            buf.append(';');
    }

    return buf;
}

bool CTxSummary::TxEntryTimer::shouldSerialize(const LogLevel requestedLevel, const unsigned int requestedGroup)
{
    if(value->getLogLevel() > requestedLevel)
        return false;

    if(!(value->getGroup() & requestedGroup))
        return false;

    return true;
}

StringBuffer& CTxSummary::TxEntryTimer::serialize(StringBuffer& buf, const LogLevel requestedLevel, const unsigned int requestedGroup, const unsigned int requestedStyle)
{
    if(!shouldSerialize(requestedLevel, requestedGroup))
        return buf;

    if(requestedStyle & TXSUMMARY_OUT_JSON)
    {
        appendJSONValue(buf, name, value->getTotalMillis());
    }
    else if (requestedStyle & TXSUMMARY_OUT_TEXT)
    {
        buf.append(fullname);
        buf.appendf("=%" I64F "ums;", value->getTotalMillis());
    }

    return buf;
}

bool CTxSummary::TxEntryObject::append(CTxSummary::TxEntryBase* entry)
{
    // Do we already have a child entry with the name of 'entry'?
    auto it = std::find_if(m_children.begin(), m_children.end(), [&](const EntryValue& e){return strieq(e->name, entry->name);});
    if(it == m_children.end())
    {
        // Create a new entry
        EntryValue ev;
        ev.setown(entry);
        m_children.push_back(ev);
        return true;
    }
    else
    {
        // 'entry' has a duplicate name, so can't be inserted
        return false;
    }
}

bool CTxSummary::TxEntryObject::set(CTxSummary::TxEntryBase* entry)
{
    // Do we already have a child entry with the name of 'entry'?
    auto it = std::find_if(m_children.begin(), m_children.end(), [&](const EntryValue& e){return strieq(e->name, entry->name);});
    if(it == m_children.end())
    {
        // Create a new entry
        EntryValue ev;
        ev.setown(entry);
        m_children.push_back(ev);
    }
    else
    {
        // Replace an existing entry
        it->setown(entry);
    }

    return true;
}

// Return the leftmost part of path, as delimited by '.'
// If no delimiters, then return entire path
size_t getFirstPathPart(StringAttr& key, const char* path)
{
    const char* finger = path;
    size_t partLen = 0;

    if(isEmptyString(path))
        return 0;

    // ignore an empty path part
    if('.' == finger[0])
        finger++;
    const char* dot = strchr(finger, '.');
    if(dot)
    {
        partLen = dot - finger;
        key.set(finger, partLen);
    } else {
        key.set(finger);
        partLen = key.length();
    }

    return partLen;
}

CTxSummary::TxEntryBase* CTxSummary::TxEntryObject::queryEntry(const char* path)
{
    StringAttr pathPart;
    const char* pathRemainder = path;
    size_t pathPartLen = 0;

    pathPartLen = getFirstPathPart(pathPart, path);
    pathRemainder += pathPartLen;

    if(pathPartLen > 0)
    {
        // Does this object contain a child with the name
        // of the first part of the path?
        auto it = std::find_if(m_children.begin(), m_children.end(), MATCH_ENTRY);

        if(it != m_children.end())
        {
            // skip over the dot delimiter
            if('.' == *pathRemainder)
                pathRemainder++;

            // If there is more to the path then traverse down.
            // Otherwise the child we found is the non-object
            // leaf node fully identified by the path.
            if(pathRemainder && *pathRemainder)
                return it->get()->queryEntry(pathRemainder);
            else
                return it->get();
        } else {
            return nullptr;
        }
    } else {
        return nullptr;
    }
}

bool CTxSummary::TxEntryObject::contains(const char* path)
{
    if(queryEntry(path))
        return true;
    else
        return false;
}

CTxSummary::TxEntryBase* CTxSummary::TxEntryObject::ensurePath(const char* path)
{
    StringAttr pathPart;
    const char* pathRemainder = path;
    size_t pathPartLen = 0;

    if(!validate(path))
    {
        DBGLOG("CTxSummary malformed entry name: (%s)", path);
    }

    pathPartLen = getFirstPathPart(pathPart, path);
    pathRemainder += pathPartLen;

    if(pathPartLen > 0 && pathRemainder && *pathRemainder)
    {
        // skip over the dot delimiter
        if('.' == *pathRemainder)
            pathRemainder++;

        auto it = std::find_if(m_children.begin(), m_children.end(), MATCH_ENTRY);
        if(it != m_children.end())
        {
            // This child found has name 'pathPart' so ensure it
            // has the remainder of the path created
            return it->get()->ensurePath(pathRemainder);
        }
        else
        {
            // Create a new entry, add it to the list of children
            // and ensure it has the remainder of the path created
            EntryValue ev;
            ev.setown(new TxEntryObject(pathPart, this->queryLogLevel(), this->queryGroup()));
            m_children.push_back(ev);
            return ev->ensurePath(pathRemainder);
        }
    } else {

        // When pathRemainder is nullptr or empty, then we've
        // ensured the path exists up to the rightmost (leaf) part.
        return this;
    }
}

unsigned __int64 CTxSummary::TxEntryObject::size()
{
    unsigned __int64 size = 0;

    for(const auto& entry : m_children)
        size += entry->size();

    return size;
}

void CTxSummary::TxEntryObject::clear()
{
    m_children.clear();
}

StringBuffer& CTxSummary::TxEntryObject::serialize(StringBuffer& buf, const LogLevel requestedLevel, const unsigned int requestedGroup, const unsigned int requestedStyle)
{
    if(!shouldSerialize(requestedLevel, requestedGroup))
        return buf;

    if(requestedStyle & TXSUMMARY_OUT_JSON)
    {
        appendJSONNameOrDelimit(buf, name);
        buf.append('{');
        for( auto it=m_children.begin(); it != m_children.end(); it++)
        {
            // after the first entry is serialised, delimit for the next
            if(it!=m_children.begin())
                appendJSONNameOrDelimit(buf, nullptr);
            it->get()->serialize(buf, requestedLevel, requestedGroup, requestedStyle);
        }
        // in the event that the last child entry isn't serialized due to
        // filtering LogLevel, group or style, remove trailing delimiter: ', '
        const char* trailing = strrchr(buf.str(), ',');
        if(trailing && streq(trailing, ", "))
        {
            buf.setCharAt(buf.length()-2, '}');
            buf.trimRight();
        }
        else
            buf.append('}');

    }

    return buf;
}

CTxSummary::CTxSummary(unsigned creationTime)
: m_creationTime(creationTime ? creationTime : msTick())
{
    pRequestCount->inc(1);
}

CTxSummary::~CTxSummary()
{
    clear();
}

unsigned __int64 CTxSummary::size() const
{
    CriticalBlock block(m_sync);
    return m_entries.size();
}

void CTxSummary::clear()
{
    CriticalBlock block(m_sync);
    m_entries.clear();
}

unsigned CTxSummary::getElapsedTime() const
{
    return msTick() - m_creationTime;
}

bool CTxSummary::contains(const char* key) const
{
    CriticalBlock block(m_sync);
    return __contains(key);
}

bool CTxSummary::appendSerialized(const char* key, const char* value, const LogLevel logLevel, const unsigned int group, bool jsonQuoted, const char* suffix)
{
    if(!validate(key))
        return false;

    CriticalBlock block(m_sync);

    if (__contains(key))
        return false;

    EntryValue entry;
    entry.setown(new TxEntryStr(key, value, logLevel, group, jsonQuoted, suffix));
    m_entries.push_back(entry);

    return true;
}

bool CTxSummary::setSerialized(const char* key, const char* value, const LogLevel logLevel, const unsigned int group, bool jsonQuoted, const char* suffix)
{
    return setEntry(key, new TxEntryStr(key, value, logLevel, group, jsonQuoted, suffix));
}

// Searches for the entry named 'key' and replaces it with the new entry 'value'
// If not found, then it adds the new entry 'value' to the end of the list
bool CTxSummary::setEntry(const char* key, TxEntryBase* value)
{
    if(!validate(key))
         return false;

    bool found = false;
    EntryValue entry;
    entry.setown(value);
    for(auto entryItr = m_entries.begin(); entryItr != m_entries.end(); entryItr++)
    {
        if(strieq(key, entryItr->get()->fullname.str()))
        {
            found = true;
            auto finger = m_entries.erase(entryItr);
            m_entries.insert(finger, entry);
            break;
        }
    }
    if(!found)
    {
        // Although the new key is not an exact match for any
        // existing entry, ensure that it is not a path prefix for
        // an existing entry. See __contains for discussion.
        if(__contains(key))
            return false;
        m_entries.push_back(entry);
    }

    return true;
}

// Set a new entry named 'destKey' with it's value copied from the entry named 'sourceKey'
// Group and LogLevel of the new entry are set as passed in.
bool CTxSummary::setCopyValueOf(const char* destKey, const char* sourceKey, const LogLevel logLevel, const unsigned int group)
{
    if(!validate(destKey))
        return false;

    CriticalBlock block(m_sync);

    if(!__contains(sourceKey))
        return false;

    TxEntryBase* sourceEntry = queryEntry(sourceKey);
    Owned<TxEntryBase> newEntry = sourceEntry->cloneWithNewName(destKey);
    newEntry->setLogLevel(logLevel);
    newEntry->setGroup(group);
    return setEntry(destKey, newEntry.getLink());
}

CumulativeTimer* CTxSummary::queryTimer(const char* key, LogLevel level, const unsigned int group)
{
    if(isEmptyString(key))
        return nullptr;

    if(!validate(key))
        throw makeStringExceptionV(-1, "CTxSummary::queryTimer Key (%s) is malformed", key);

    CriticalBlock block(m_sync);

    Linked<TxEntryBase> entry = queryEntry(key);

    if(entry)
    {
        // Is the entry a Cumulative Timer?
        TxEntryTimer* timer = dynamic_cast<TxEntryTimer*>(entry.get());
        if(!timer)
        {
            throw makeStringExceptionV(-1, "CTxSummary::queryTimer Key (%s) already exists for a non-timer entry", key);
        }
        else
        {
            if(timer->queryLogLevel() == level && timer->queryGroup() == group)
                return timer->value.get();
            else
                throw makeStringExceptionV(-1, "CTxSummary::queryTimer Search for key (%s) logLevel (%d) group (%d) found mismatched timer with logLevel (%d) group (%d)", key, level, group, timer->queryLogLevel(), timer->queryGroup());
        }
    } else {
        // The CumulativeTimer does not yet exist
        // create it and add to the list of entries
        // For new timer only, initialize with level and group values
        Owned<CumulativeTimer> newTimer = new CumulativeTimer();
        newTimer->setGroup(group);
        newTimer->setLogLevel(level);
        EntryValue newEntry;
        newEntry.setown(new TxEntryTimer(key, newTimer.get()));
        m_entries.push_back(newEntry);
        return newTimer.get();
    }
}

bool CTxSummary::updateTimer(const char* name, unsigned long long delta, const LogLevel logLevel, const unsigned int group)
{
    Owned<CumulativeTimer> timer;

    CriticalBlock block(m_sync);

    timer.set(queryTimer(name, logLevel, group));

    if (!timer)
        return false;

    timer->add(delta);
    return true;
}

bool CTxSummary::tailor(IEspContext* ctx)
{
    if(m_profile)
        return m_profile->tailorSummary(ctx);

    return false;
}

void CTxSummary::setProfile(ITxSummaryProfile* profile)
{
    m_profile.set(profile);
}

void CTxSummary::serialize(StringBuffer& buffer, const LogLevel logLevel, const unsigned int group, const unsigned int requestedStyle) const
{
    CriticalBlock block(m_sync);

    if(requestedStyle & TXSUMMARY_OUT_TEXT)
    {
        // For text serialization, we create a new entry if it has
        // a profile mapping it to a new name, then serialize it.
        // Otherwise, we just serialize the existing entry.
        for(auto entry : m_entries)
        {
            StringBuffer effectiveName;
            bool mapped = false;
            if(m_profile)
            {
                mapped = m_profile->getEffectiveName(effectiveName, entry->fullname.str(), group, requestedStyle);
                if(mapped)
                {
                    if(__contains(effectiveName.str()))
                    {
                        WARNLOG("Prevented output of duplicate TxSummary entry '%s', renamed from '%s'", effectiveName.str(), entry->fullname.get());
                        continue;
                    }
                    Owned<TxEntryBase> renamed = entry->cloneWithNewName(effectiveName);
                    renamed->serialize(buffer, logLevel, group, TXSUMMARY_OUT_TEXT);
                }
            }
            if(!mapped)
                entry->serialize(buffer, logLevel, group, TXSUMMARY_OUT_TEXT);
        }
    }
    else if(requestedStyle & TXSUMMARY_OUT_JSON)
    {
        // Create a TxEntryObject as a kind of JSON 'DOM' with copies of the entries.
        // The root should be output for all Log Levels and all output styles.
        TxEntryObject root(nullptr, LogMin, TXSUMMARY_OUT_TEXT | TXSUMMARY_OUT_JSON);
        for(auto entry : m_entries)
        {
            StringBuffer effectiveName(entry->fullname.str());
            if(m_profile)
            {
                bool mapped = m_profile->getEffectiveName(effectiveName, entry->fullname.str(), group, requestedStyle);
                if(mapped && root.contains(effectiveName.str()))
                {
                    WARNLOG("Prevented output of duplicate TxSummary entry '%s', renamed from '%s'", effectiveName.str(), entry->fullname.str());
                    continue;
                }
            }
            TxEntryBase* parent = root.ensurePath(effectiveName.str());
            parent->append(entry->cloneWithNewName(effectiveName.str()));
        }
        // Serialize our purpose-built TxEntryObject
        root.serialize(buffer, logLevel, group, TXSUMMARY_OUT_JSON);
    }
}

void CTxSummary::log(const LogLevel logLevel, const unsigned int requestedGroup, const unsigned int requestedStyle)
{
    if(__contains("user") || __contains("req"))
    {
        StringBuffer summary;

        // Support simultaneous output of all styles of TxSummary
        if(requestedStyle & TXSUMMARY_OUT_TEXT)
        {
            serialize(summary, logLevel, requestedGroup, TXSUMMARY_OUT_TEXT);
            DBGLOG("TxSummary[%s]", summary.str());
        }

        if(requestedStyle & TXSUMMARY_OUT_JSON)
        {
            serialize(summary.clear(), logLevel, requestedGroup, TXSUMMARY_OUT_JSON);
            DBGLOG("%s", summary.str());
        }
    }
}

bool CTxSummary::__contains(const char* key) const
{
    // Because now a key can be a path, we can't just
    // compare the key to the fullname of all entries.
    // We need to ensure that adding the new key won't
    // duplicate or disrupt any part of an already-added
    // path.
    //
    // If either the complete key or the entry fullname
    // is a prefix of the other on the boundary of a dot
    // delimiter, then we already have an entry named 'key'
    // For example:
    //
    // entries: foo.bar = 5, foo.baz = 6
    // key  : foo = 7
    //
    // We say it contains foo because foo is the parent entry
    // holding the bar and baz values. We can't append a
    // duplicate entry foo with value 7.

    for(const auto& pair : m_entries)
    {
        const char* k = key;
        const char* e = pair->fullname.str();
        if( nullptr == k || nullptr == e)
            return false;

        bool done = false;
        while(!done)
        {
            if('\0' == *k)
            {
                done = true;
                if('\0' == *e || '.' == *e)
                    return true;
            }
            else if('\0' == *e)
            {
                done = true;
                if('\0' == *k || '.' == *k)
                    return true;
            }
            else if(toupper(*k) != toupper(*e))
            {
                done = true;
            }
            else
            {
                k++;
                e++;
            }
        }
    }
    return false;
}

CTxSummary::TxEntryBase* CTxSummary::queryEntry(const char* key)
{
    if(!validate(key))
        return nullptr;

    for(auto entry : m_entries)
    {
        if(strieq(key, entry->fullname.str()))
            return entry.get();
    }

    return nullptr;
}
