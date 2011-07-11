/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


#ifndef __JISEM__
#define __JISEM__

#include "jexpdef.hpp"
#include "jsem.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"

class jlib_thrown_decl InterruptedSemaphoreException : public CInterface, implements IException
{
public:
    IMPLEMENT_IINTERFACE;
    virtual int             errorCode() const { return 0; }
    virtual StringBuffer &  errorMessage(StringBuffer &msg) const { return msg.append("Semaphore interrupted"); }
    virtual MessageAudience errorAudience() const { return MSGAUD_unknown; }
};

class jlib_decl InterruptableSemaphore : public Semaphore
{
private:
    Owned<IException> error;
    CriticalSection crit;

public:
    InterruptableSemaphore(unsigned _initialCount = 0U) : Semaphore(_initialCount) {}

    void interrupt(IException *_error = NULL)
    {
        CriticalBlock b(crit);
        if (error)
            ::Release(_error);
        else
        {
            if (!_error)
                _error = new InterruptedSemaphoreException;
            error.setown(_error);
            signal();
        }
    }

    void wait()
    {
        Semaphore::wait();
        CriticalBlock b(crit);
        if (error)
        {
            throw error.getClear();
        }
    }

    bool wait(unsigned timeout)
    {
        bool ret = Semaphore::wait(timeout);
        CriticalBlock b(crit);
        if (error)
        {
            throw error.getClear();
        }
        return ret;
    }

    void reinit(unsigned _initialCount = 0U)
    {
        CriticalBlock b(crit);
        error.clear();
        Semaphore::reinit(_initialCount);
    }

};

class jlib_decl TokenBucket : public CInterface
{
    SpinLock crit;
    Semaphore tokens;
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
    IMPLEMENT_IINTERFACE;

    TokenBucket(unsigned _tokensPerPeriod, unsigned _period, unsigned _maxBucketSize)
        : tokensPerPeriod(_tokensPerPeriod), period(_period), maxBucketSize(_maxBucketSize), tokens(_maxBucketSize)
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
