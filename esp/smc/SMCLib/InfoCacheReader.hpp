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

#ifndef _ESPWIZ_InfoCacheReader_HPP__
#define _ESPWIZ_InfoCacheReader_HPP__

#include "jliball.hpp"
#include "bindutil.hpp"

const unsigned defaultInfoCacheForceBuildSecond = 10;
const unsigned defaultInfoCacheAutoRebuildSecond = 120;

class CInfoCache : public CSimpleInterfaceOf<IInterface>
{
protected:
    CDateTime timeCached;
public:

    CInfoCache() {};

    bool isCachedInfoValid(unsigned timeOutSeconds);
    inline StringBuffer& queryTimeCached(StringBuffer& str) { return timeCached.getString(str, true); }
};

interface IInfoCacheReader : extends IInterface
{
    virtual CInfoCache* read() = 0;
};

class CInfoCacheReaderThread : public CSimpleInterfaceOf<IThreaded>
{
    StringAttr name;
    bool stopping = false;
    bool active = true;
    bool first = true;
    bool firstBlocked = false;
    unsigned autoRebuildSeconds = defaultInfoCacheAutoRebuildSecond;
    unsigned forceRebuildSeconds = defaultInfoCacheForceBuildSecond;
    Owned<CInfoCache> infoCache;
    Owned<IInfoCacheReader> infoCacheReader;
    Semaphore sem;
    CriticalSection crit;
    CThreaded threaded;
    std::atomic<bool> waiting = {false};

public:
    CInfoCacheReaderThread(IInfoCacheReader *_infoCacheReader, const char* _name, unsigned _autoRebuildSeconds, unsigned _forceRebuildSeconds)
        : name(_name), autoRebuildSeconds(_autoRebuildSeconds), forceRebuildSeconds(_forceRebuildSeconds), threaded(_name)
    {
        infoCacheReader.setown(_infoCacheReader);
        threaded.init(this);
    };

    ~CInfoCacheReaderThread()
    {
        stopping = true;
        sem.signal();
        threaded.join();
    }

    virtual void threadmain() override;
    CInfoCache* getCachedInfo()
    {
        CLeavableCriticalBlock b(crit);
        if (first)
        {
            if (!active)
                return nullptr;
            firstBlocked = true;
            b.leave();
            sem.wait();
            b.enter();
            if (first)
                return nullptr;
        }

        //Now, activityInfoCache should always be available.
        assertex(infoCache);
        if (active && !infoCache->isCachedInfoValid(forceRebuildSeconds))
            rebuild();
        return infoCache.getLink();
    }
    void rebuild()
    {
        bool expected = true;
        if (waiting.compare_exchange_strong(expected, false))
            sem.signal();
    }
    void setActive(bool _active)
    {
        CriticalBlock b(crit);
        if (active != _active)
        {
            active = _active;
            if (!active)
            {
                // NB: first will still be true, signal and getActivityInfo() will return null
                if (firstBlocked)
                {
                    firstBlocked = false;
                    sem.signal(); // NB: first still true
                }
            }
            else
                rebuild();
        }
    }
    bool isActive() const { return active; }
};

#endif //_ESPWIZ_InfoCacheReader_HPP__

