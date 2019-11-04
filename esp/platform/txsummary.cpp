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

using std::find_if;
using std::for_each;

#define VALIDATE_KEY(k) if (!(k) || !(*k)) return false
#define MATCH_KEY       [&](const Entry& entry) { return stricmp(entry.key.str(), key) == 0; }

bool operator < (const StringAttr& a, const StringAttr& b)
{
    return (stricmp(a.str(), b.str()) < 0);
}

CTxSummary::CTxSummary(unsigned creationTime)
: m_creationTime(creationTime ? creationTime : msTick())
{
}

CTxSummary::~CTxSummary()
{
    clear();
}

unsigned __int64 CTxSummary::size() const
{
    CriticalBlock block(m_sync);
    return m_entries.size() + m_timers.size();
}

void CTxSummary::clear()
{
    CriticalBlock block(m_sync);
    m_entries.clear();
    m_timers.clear();
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

bool CTxSummary::append(const char* key, const char* value, const LogLevel logLevel)
{
    VALIDATE_KEY(key);

    CriticalBlock block(m_sync);

    if (__contains(key))
        return false;

    m_entries.push_back({key, value, logLevel});
    return true;
}

bool CTxSummary::set(const char* key, const char* value, const LogLevel logLevel)
{
    VALIDATE_KEY(key);

    CriticalBlock block(m_sync);
    Entries::iterator it = find_if(m_entries.begin(), m_entries.end(), MATCH_KEY);

    if (it != m_entries.end())
    {
        it->value.set(value);
        it->logLevel = logLevel;
    }
    else
        m_entries.push_back({key, value, logLevel});

    return true;
}

CumulativeTimer* CTxSummary::queryTimer(const char* name)
{
    if (!name || !*name)
        return NULL;

    CriticalBlock block(m_sync);
    TimerValue& timer = m_timers[name];

    if (!timer)
    {
        timer.setown(new CumulativeTimer());
    }

    return timer;
}

bool CTxSummary::updateTimer(const char* name, unsigned long long delta, const LogLevel logLevel)
{
    Owned<CumulativeTimer> timer;

    timer.set(queryTimer(name));

    if (!timer)
        return false;

    timer->add(delta);
    timer->setLogLevel(logLevel);
    return true;
}

void CTxSummary::serialize(StringBuffer& buffer, const LogLevel logLevel) const
{
    CriticalBlock block(m_sync);

    for (const Entry& entry : m_entries)
    {
        if (entry.logLevel > logLevel)
            continue;

        if (entry.value.length())
            buffer.appendf("%s=%s;", entry.key.str(), entry.value.str());
        else
            buffer.appendf("%s;", entry.key.str());
    }

    for (const std::pair<TimerKey, TimerValue>& entry : m_timers)
    {
        if (entry.second->getLogLevel() <= logLevel)
            buffer.appendf("%s=%" I64F "ums;", entry.first.str(), entry.second->getTotalMillis());
    }
}

void CTxSummary::log(const LogLevel logLevel)
{
    if (__contains("user") || __contains("req"))
    {
        StringBuffer summary;
        serialize(summary, logLevel);
        DBGLOG("TxSummary[%s]", summary.str());
    }
}

bool CTxSummary::__contains(const char* key) const
{
    return find_if(m_entries.begin(), m_entries.end(), MATCH_KEY) != m_entries.end();
}
