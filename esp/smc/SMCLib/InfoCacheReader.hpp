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
#include "TpWrapper.hpp"

const unsigned defaultInfoCacheForceBuildSecond = 10;
const unsigned defaultInfoCacheAutoRebuildSecond = 120;

class TPWRAPPER_API CInfoCache : public CSimpleInterfaceOf<IInterface>
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
    virtual void buildCachedInfo() = 0;
    virtual CInfoCache *getCachedInfo() const = 0;
    virtual void setActive(bool active) = 0;
    virtual bool isActive() const = 0;
};

class CInfoCacheReader;
class TPWRAPPER_API CInfoCacheReaderThread : public CSimpleInterfaceOf<IThreaded>
{
    StringAttr name;
    bool stopping = false;
    bool active = true;
    bool first = true;
    bool firstBlocked = false;
    unsigned autoRebuildSeconds = defaultInfoCacheAutoRebuildSecond;
    unsigned forceRebuildSeconds = defaultInfoCacheForceBuildSecond;
    Owned<CInfoCache> infoCache;
    CInfoCacheReader* infoCacheReader;
    Semaphore sem;
    Semaphore firstSem;
    CriticalSection crit;
    CThreaded threaded;
    std::atomic<bool> waiting = {false};

public:
    CInfoCacheReaderThread(CInfoCacheReader* _reader, const char* _name, unsigned _autoRebuildSeconds, unsigned _forceRebuildSeconds)
        : infoCacheReader(_reader), name(_name), autoRebuildSeconds(_autoRebuildSeconds), forceRebuildSeconds(_forceRebuildSeconds), threaded(_name)
    {
        threaded.init(this);
    };

    ~CInfoCacheReaderThread()
    {
        stopping = true;
        sem.signal();
        firstSem.signal(); // in case
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
            firstSem.wait();
            b.enter();
            if (first)
                return nullptr;
        }

        //Now, activityInfoCache should always be available.
        assertex(infoCache);
        if (active && !infoCache->isCachedInfoValid(forceRebuildSeconds))
            buildCachedInfo();
        return infoCache.getLink();
    }
    void buildCachedInfo()
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
                    firstSem.signal(); // NB: first still true
                }
            }
            else
                buildCachedInfo();
        }
    }
    bool isActive() const { return active; }
};

class CInfoCacheReader : implements IInfoCacheReader, public CInterface
{
public:
    Owned<CInfoCacheReaderThread> infoCacheReaderThread;

    IMPLEMENT_IINTERFACE;

    CInfoCacheReader(const char* _name, unsigned _autoRebuildSeconds, unsigned _forceRebuildSeconds)
    {
        infoCacheReaderThread.setown(new CInfoCacheReaderThread(this, _name, _autoRebuildSeconds, _forceRebuildSeconds));
    }

    virtual CInfoCache* getCachedInfo() const override { return infoCacheReaderThread->getCachedInfo(); }
    virtual void buildCachedInfo() override { infoCacheReaderThread->buildCachedInfo(); }
    virtual void setActive(bool _active) override { infoCacheReaderThread->setActive(_active); }
    virtual bool isActive() const override { return infoCacheReaderThread->isActive(); }

    virtual CInfoCache* read() = 0;
};

#endif //_ESPWIZ_InfoCacheReader_HPP__

