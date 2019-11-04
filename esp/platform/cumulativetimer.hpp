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

#ifndef CUMULATIVETIMER_HPP
#define CUMULATIVETIMER_HPP

#include "jiface.hpp"
#include "jutil.hpp"
#include "esp.hpp"

// Utility class for tracking cumulative elapsed time. Once instantiated, an
// instance's total time can be updated multiple times. When used with the
// TxSummary, the time spent in multiple related (or repeated) function calls
// can be presented as a single milestone.
//
// See CumulativeTimer::Scope for a simplified method of timing a code block.
//
// This class is NOT thread safe. Timing concurrent code blocks with a single
// instance is not supported.
class CumulativeTimer : public CInterface
{
public:

    CumulativeTimer()
        : mNestingDepth(0)
        , mTotalTime(0)
    {
    }

    CumulativeTimer(const CumulativeTimer& t)
        : mNestingDepth(t.mNestingDepth)
        , mTotalTime(t.mTotalTime)
    {}

    ~CumulativeTimer()
    {
    }

    inline void setLogLevel(const LogLevel _logLevel) { logLevel = _logLevel; };
    inline LogLevel getLogLevel() const { return logLevel; }
    inline unsigned __int64 getTotalMillis() const { return mTotalTime; }
    inline void reset() { mTotalTime = 0; }

public:

    // Given a cumulative timer on construction, add the milliseconds elapsed
    // between construction and destruction to the timer. If multiple instances
    // are nested, only the first instance will count.
    struct Scope
    {
        Scope(CumulativeTimer* t)
            : mTimer(t), mStart(0)
        {
            if (mTimer)
                mTimer->incNesting();
            mStart = msTick();
        }

        ~Scope()
        {
            if (mTimer)
            {
                mTimer->decNesting(msTick() - mStart);
            }
        }

    private:
        unsigned __int64    mStart;
        CumulativeTimer*    mTimer;

    private:
        Scope(const Scope& t) = delete;
        Scope& operator =(const Scope&) = delete;
    };

    void add(unsigned __int64 delta)
    {
        if (mNestingDepth == 0)
            mTotalTime += delta;
    }

private:
    friend struct CumulativeTimer::Scope;

    inline void incNesting() { ++mNestingDepth; }
    inline void decNesting(unsigned __int64 delta)
    {
        if (--mNestingDepth == 0)
            mTotalTime += delta;
    }

    unsigned __int64 mNestingDepth;
    unsigned __int64 mTotalTime;
    LogLevel logLevel;

private:
    CumulativeTimer& operator =(const CumulativeTimer&) = delete;
};

#endif // CUMULATIVETIMER_HPP
