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

#define VALIDATE_KEY(k) if (!(k) || !(*k)) return false
#define MATCH_KEY       [&](const Entry& entry) { return stricmp(entry.key.str(), key) == 0; }

CTxSummary::CTxSummary(unsigned creationTime)
: m_creationTime(creationTime ? creationTime : msTick())
{
}

CTxSummary::~CTxSummary()
{
    log();
    clear();
}

unsigned __int64 CTxSummary::size() const
{
    return m_entries.size();
}

void CTxSummary::clear()
{
    m_entries.clear();
}

unsigned CTxSummary::getElapsedTime() const
{
    return msTick() - m_creationTime;
}

bool CTxSummary::contains(const char* key) const
{
    return find_if(m_entries.begin(), m_entries.end(), MATCH_KEY) != m_entries.end();
}

bool CTxSummary::append(const char* key, const char* value)
{
    VALIDATE_KEY(key);

    if (contains(key))
        return false;

    m_entries.push_back({key, value});
    return true;
}

bool CTxSummary::set(const char* key, const char* value)
{
    VALIDATE_KEY(key);

    Entries::iterator it = find_if(m_entries.begin(), m_entries.end(), MATCH_KEY);

    if (it != m_entries.end())
        it->value.set(value);
    else
        m_entries.push_back({key, value});

    return true;
}

void CTxSummary::serialize(StringBuffer& buffer) const
{
    for (const Entry& entry : m_entries)
    {
        if (entry.value.length())
            buffer.appendf("%s=%s;", entry.key.str(), entry.value.str());
        else
            buffer.appendf("%s;", entry.key.str());
    }
}

void CTxSummary::log()
{
    if (size())
    {
        StringBuffer summary;
        serialize(summary);
        DBGLOG("TxSummary[%s]", summary.str());
    }
}
