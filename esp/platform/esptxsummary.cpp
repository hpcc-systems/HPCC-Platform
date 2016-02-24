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

#include "esptxsummary.hpp"
#include <algorithm>
#include <list>
#include <sstream>

using std::find_if;
using std::stringstream;

#define VALIDATE_KEY(k) if (!(k) || !(*k)) return false
#define MATCH_KEY       [&](const Entry& entry) { return stricmp(entry.key.str(), key) == 0; }

class CEspTxSummary : public CInterface, implements IEspTxSummary
{
private:
    struct Entry
    {
        StringBuffer key;
        StringBuffer value;
    };

    using Entries = std::list<Entry>;

    Entries   m_entries;
    unsigned  m_creationTime;

public:
    IMPLEMENT_IINTERFACE;

    CEspTxSummary(unsigned creationTime)
    : m_creationTime(creationTime ? creationTime : msTick())
    {
    }

    ~CEspTxSummary()
    {
        log();
        clear();
    }

    virtual unsigned __int64 size() const override
    {
        return m_entries.size();
    }

    virtual void clear() override
    {
        m_entries.clear();
    }

    virtual unsigned getTimestamp() const override
    {
        return msTick() - m_creationTime;
    }

    bool contains(const char* key) const
    {
        return find_if(m_entries.begin(), m_entries.end(), MATCH_KEY) != m_entries.end();
    }

    virtual bool append(const char* key, const char* value) override
    {
        VALIDATE_KEY(key);

        if (contains(key))
            return false;

        m_entries.push_back({key, value});
        return true;
    }

    virtual bool set(const char* key, const char* value) override
    {
        VALIDATE_KEY(key);

        Entries::iterator it = find_if(m_entries.begin(), m_entries.end(), MATCH_KEY);

        if (it != m_entries.end())
            it->value.set(value);
        else
            m_entries.push_back({key, value});

        return true;
    }

    virtual void serialize(StringBuffer& buffer) const override
    {
        for (auto&& entry : m_entries)
        {
            buffer.appendf((entry.value.length() > 0 ? "%s=%s;" : "%s;"), entry.key.str(), entry.value.str());
        }
    }

private:
    void log()
    {
        if (size())
        {
            StringBuffer summary;
            serialize(summary);
            DBGLOG("TxSummary[%s]", summary.str());
        }
    }

    template <class T>
    static T find_key(T begin, T end, const char* key)
    {
        return std::find_if(begin, end, [&](const Entry& entry) { return stricmp(entry.key.str(), key) == 0; });
    }
};

IEspTxSummary* createEspTxSummary(unsigned creationTime)
{
    return new CEspTxSummary(creationTime);
}
