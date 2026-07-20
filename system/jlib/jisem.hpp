/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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


#ifndef __JISEM__
#define __JISEM__

#include "jiface.hpp"
#include "jsem.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"

class jlib_thrown_decl InterruptedSemaphoreException : implements IException, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    virtual int             errorCode() const { return 0; }
    virtual StringBuffer &  errorMessage(StringBuffer &msg) const { return msg.append("Semaphore interrupted"); }
    virtual MessageAudience errorAudience() const { return MSGAUD_programmer; }
};

class jlib_decl InterruptableSemaphore : public Semaphore
{
private:
    AtomicShared<IException> error;

public:
    InterruptableSemaphore(const char * name, unsigned _initialCount = 0U) : Semaphore(name, _initialCount) {}

    void interrupt(IException *_error = NULL, unsigned count=1)
    {
        if (!_error)
            _error = new InterruptedSemaphoreException;

        if (error.setownIfNull(_error))
            signal(count);
    }

    void wait()
    {
        Semaphore::wait();
        if (error.isSet())
        {
            IException * e = error.getClear();
            if (e)
                throw e;
        }
    }

    bool wait(unsigned timeout)
    {
        bool ret = Semaphore::wait(timeout);
        if (error.isSet())
        {
            IException * e = error.getClear();
            if (e)
                throw e;
        }
        return ret;
    }

    void reinit(unsigned _initialCount = 0U)
    {
        error.clear();
        Semaphore::reinit(_initialCount);
    }

};

class jlib_decl TokenBucket : public CInterface
{
    SpinLock crit{SYNC_LOCATION}; // MORE: I suspect this should be a critical section
    Semaphore tokens{SYNC_LOCATION};
    unsigned tokensAvailable;
    unsigned maxBucketSize;
    unsigned tokensPerPeriod;
    unsigned period;
    unsigned then;

    inline void tokenUsed() 
    {
        SpinBlock b(crit);
        assertex(tokensAvailable);
        tokensAvailable--;
    }

    void refill(unsigned tokensToAdd)
    {
        if (tokensAvailable + tokensToAdd > maxBucketSize)
        {
            if (maxBucketSize > tokensAvailable)
                tokensToAdd = maxBucketSize - tokensAvailable;
            else
                tokensToAdd = 0;
        }
        if (tokensToAdd)
        {
            tokensAvailable += tokensToAdd;
            tokens.signal(tokensToAdd);
        }
    }


public:
    TokenBucket(unsigned _tokensPerPeriod, unsigned _period, unsigned _maxBucketSize)
        : tokens(SYNC_LOCATION, _maxBucketSize), maxBucketSize(_maxBucketSize), tokensPerPeriod(_tokensPerPeriod), period(_period)
    {
        tokensAvailable = _maxBucketSize;
        then = msTick();
    }
    ~TokenBucket()
    {
    }

    void wait(unsigned tokensNeeded)
    {
        while (tokensNeeded)
        {
            unsigned timeout;
            {
                SpinBlock b(crit);
                unsigned now = msTick();
                unsigned elapsed = now - then;
                if (elapsed >= period)
                {
                    refill(tokensPerPeriod * (elapsed/period));
                    timeout = (elapsed % period);
                    then = now - timeout;
                }
                else
                    timeout = elapsed;
            }
            if (tokens.wait(period-timeout))
            {
                tokenUsed();
                tokensNeeded--;
            }
        }
    }

};

#endif
