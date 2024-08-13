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


#ifndef JQueue_TINCL
#define JQueue_TINCL
#include "jmutex.hpp"

// Simple Queue Template for an expanding circular buffer queue


typedef bool (*priorityGreaterFunction)(const void * left, const void * right);

template <class BASE, bool ALLOWNULLS> 
class QueueOf
{
    typedef QueueOf<BASE, ALLOWNULLS> SELF;
    BASE **ptrs;
    unsigned headp;
    unsigned tailp;
    unsigned max;
    RelaxedAtomic<unsigned> num; // atomic so that it can be read without a critical section
    void expand()
    {
        unsigned inc;
        if (max>=0x4000)
            inc = 0x4000;
        else if (max)
            inc = max;
        else
            inc = 4;
        reserve(inc);
    }

public:
    inline QueueOf() { ptrs = NULL; headp=0; tailp=0; max=0; num = 0; }
    inline ~QueueOf() { free(ptrs); }

    inline void clear() { free(ptrs); ptrs = NULL; headp=0; tailp=0; max=0; num = 0; }
    inline void ensure(unsigned minSize) { if (max < minSize) reserve(minSize-max); }
    void reserve(unsigned n)
    {
        max += n;
        ptrs = (BASE **)realloc(ptrs,max*sizeof(BASE *));
        assertex(!max || ptrs);
        if (headp>tailp) { // wrapped
            memmove(ptrs+headp+n, ptrs+headp, (num-tailp-1)*sizeof(BASE *));
            headp += n;
        }
        else if (num==0)
        {
            headp = 0;
            tailp = max-1;
        }
    }
    inline BASE *head() const { return num?ptrs[headp]:NULL; }
    inline BASE *tail() const { return num?ptrs[tailp]:NULL; }
    inline BASE *item(unsigned idx) const { 
        if (idx>=num)
            return NULL;
        idx+=headp;
        if (idx>=max)
            idx-=max;
        return ptrs[idx]; 
    }
    inline void enqueue(BASE *e)
    {
        if (ALLOWNULLS || e)
        {
            if (unlikely(num==max))
                expand();
            tailp++;
            if (tailp==max)
                tailp=0;
            ptrs[tailp] = e;
            num.fastAdd(1); // Do not use increment which is atomic
        }
    }
    void enqueueHead(BASE *e)
    {
        if (ALLOWNULLS || e)
        {
            if (unlikely(num==max))
                expand();
            if (headp==0)
                headp=max;
            headp--;
            ptrs[headp] = e;
            num.fastAdd(1); // Do not use increment which is atomic
        }
    }
    void enqueue(BASE *e,unsigned i)
    {
        if (ALLOWNULLS || e) {
            if (i==0)
                enqueueHead(e);
            else if (i>=num)
                enqueue(e);
            else {
                if (num==max) 
                    expand();
                i += headp;
                if (i>=max)
                    i-=max;
                // do rotate slow way for simplicity
                tailp++;
                if (tailp==max)
                    tailp = 0;
                unsigned p=tailp;
                do {
                    unsigned n = (p==0)?max:p;
                    n--;
                    ptrs[p] = ptrs[n];
                    p = n;
                } while (p!=i);
                ptrs[i] = e;
                num.fastAdd(1); // Do not use increment which is atomic
            }
        }
    }
    void enqueue(BASE *e,priorityGreaterFunction pgf)
    {
        if (ALLOWNULLS || e) {
            unsigned p=tailp;
            unsigned i=num;
            while (i&&pgf(e,ptrs[p])) {
                i--;
                if (p==0)
                    p=max;
                p--;
            }
            enqueue(e,i);
        }
    }
    inline BASE *dequeue()
    {
        if (!num)
            return NULL;
        BASE *ret = ptrs[headp];
        headp++;
        if (headp==max)
            headp = 0;
        num.fastAdd(-1); // Do not use decrement which is atomic
        return ret;
    }
    BASE *dequeueTail()
    {
        if (!num)
            return NULL;
        BASE *ret = ptrs[tailp];
        if (tailp==0)
            tailp=max;
        tailp--;
        num.fastAdd(-1); // Do not use decrement which is atomic
        return ret;
    }
    BASE *dequeue(unsigned i)
    {
        if (i==0)
            return dequeue();
        if (i>=num)
            return NULL;
        if (i+1==num) 
            return dequeueTail();
        i += headp;
        if (i>=max)
            i-=max;
        BASE *ret = ptrs[i];
        // do rotate slow way for simplicity
        unsigned p=i;
        do {
            unsigned n = (p==0)?max:p;
            n--;
            ptrs[p] = ptrs[n];
            p = n;
        } while (p!=headp);
        headp++;
        if (headp==max)
            headp = 0;
        num.fastAdd(-1); // Do not use decrement which is atomic
        return ret;
    }
    void set(unsigned idx, BASE *v)
    {
        assertex(idx<num);
        idx+=headp;
        if (idx>=max)
            idx-=max;
        ptrs[idx] = v; 
    }
    BASE * query(unsigned idx)
    {
        assertex(idx<num);
        idx+=headp;
        if (idx>=max)
            idx-=max;
        return ptrs[idx];
    }
    unsigned find(BASE *e) 
    {   // simple linear search
        if (num!=0) {
            if (e==ptrs[tailp])
                return num-1;
            unsigned i=headp;
            for (;;) {
                if (ptrs[i]==e) {
                    if (i<headp)
                        i += max;
                    return i-headp;
                }
                if (i==tailp)
                    break;
                i++;
                if (i==max)
                    i = 0;
            }
        }
        return (unsigned)-1;
    }
    BASE *dequeue(BASE *e)
    {
        return dequeue(find(e));
    }
    inline unsigned ordinality() const { return num; }
};

template <class BASE, bool ALLOWNULLS> 
class SafeQueueOf : private QueueOf<BASE, ALLOWNULLS>
{
    typedef SafeQueueOf<BASE, ALLOWNULLS> SELF;
    typedef QueueOf<BASE, ALLOWNULLS> PARENT;
protected:
    mutable CriticalSection crit;
    inline void unsafeenqueue(BASE *e) { QueueOf<BASE, ALLOWNULLS>::enqueue(e); }
    inline void unsafeenqueueHead(BASE *e) { QueueOf<BASE, ALLOWNULLS>::enqueue(e); }
    inline void unsafeenqueue(BASE *e,priorityGreaterFunction p) { QueueOf<BASE, ALLOWNULLS>::enqueue(e,p); }
    inline BASE *unsafedequeue() { return QueueOf<BASE, ALLOWNULLS>::dequeue(); }
    inline BASE *unsafedequeueTail() { return QueueOf<BASE, ALLOWNULLS>::dequeueTail(); }
    inline unsigned unsafeordinality() { return QueueOf<BASE, ALLOWNULLS>::ordinality(); }

public:
    void clear() { CriticalBlock b(crit); QueueOf<BASE, ALLOWNULLS>::clear(); }
    BASE *head() const { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::head(); }
    BASE *tail() const { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::tail(); }
    BASE *item(unsigned idx) const { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::item(idx); }
    void enqueue(BASE *e) { CriticalBlock b(crit); QueueOf<BASE, ALLOWNULLS>::enqueue(e); }
    void enqueueHead(BASE *e) { CriticalBlock b(crit); QueueOf<BASE, ALLOWNULLS>::enqueueHead(e); }
    void enqueue(BASE *e,unsigned i) { CriticalBlock b(crit); QueueOf<BASE, ALLOWNULLS>::enqueue(e, i); }
    void enqueue(BASE *e,priorityGreaterFunction p) { CriticalBlock b(crit); QueueOf<BASE, ALLOWNULLS>::enqueue(e, p); }
    BASE *dequeue() { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::dequeue(); }
    BASE *dequeueTail() { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::dequeueTail(); }
    BASE *dequeue(unsigned i) { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::dequeue(i); }
    unsigned find(BASE *e) { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::find(e); }
    void dequeue(BASE *e) { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::dequeue(e); }
    inline unsigned ordinality() const { return QueueOf<BASE, ALLOWNULLS>::ordinality(); }
    void set(unsigned idx, BASE *e) { CriticalBlock b(crit); return QueueOf<BASE, ALLOWNULLS>::set(idx, e); }
    using PARENT::ensure;
};

//Sending signals once the critical section has been released generally gives better performance.
#define SIGNAL_OUTSIDE

template <class BASE, bool ALLOWNULLS> 
class SimpleInterThreadQueueOf : protected SafeQueueOf<BASE, ALLOWNULLS>
{
    typedef SimpleInterThreadQueueOf<BASE, ALLOWNULLS> SELF;
protected:
    Semaphore deqwaitsem;
    unsigned deqwaiting;
    Semaphore enqwaitsem;
    unsigned enqwaiting;
    bool stopped;
    unsigned limit;


    bool qwait(Semaphore &sem,unsigned &waiting,unsigned timeout,unsigned &start) 
    { 
        // in crit block
        unsigned remaining;
        if (timeout) {
            if (timeout==INFINITE) 
                remaining = timeout;
            else
            {
                if (!start) {
                    start = msTick();
                    remaining = timeout;
                }
                else {
                    unsigned elapsed=msTick()-start;
                    if (elapsed>=timeout)
                        return false;
                    remaining = (timeout-elapsed);
                }
            }
        }   
        else
            return false;
        waiting++;
        bool wr;
        {
            CriticalUnblock unblock(SELF::crit);
            wr = sem.wait(remaining);
        }
        if (!wr) {
            wr = sem.wait(0);               // catch race
            if (!wr) {
                waiting--;
                return false;
            }
        }
        return true;
    }

    inline bool get(BASE *&ret,bool tail)
    {
        if (ALLOWNULLS) {
            if (SafeQueueOf<BASE, ALLOWNULLS>::unsafeordinality()) {
                ret = tail?SafeQueueOf<BASE, ALLOWNULLS>::unsafedequeueTail():SafeQueueOf<BASE, ALLOWNULLS>::unsafedequeue(); 
                return true;
            }   
            return false;
        }
        ret = tail?SafeQueueOf<BASE, ALLOWNULLS>::unsafedequeueTail():SafeQueueOf<BASE, ALLOWNULLS>::unsafedequeue(); 
        return ret!=NULL;
    }

public:
    SimpleInterThreadQueueOf<BASE, ALLOWNULLS>() 
    {
        limit = 0; // no limit
        reset();
    }

    ~SimpleInterThreadQueueOf<BASE, ALLOWNULLS>() 
    {
        stop();
    }

    void reset()
    {
        enqwaiting = 0;
        deqwaiting = 0;
        stopped = false;
    }

    bool queryStopped() const { return stopped; }

    bool enqueue(BASE *e,unsigned timeout=INFINITE) 
    { 
        unsigned numToSignal = 0;
        {
            CriticalBlock b(SELF::crit);
            if (limit) {
                unsigned start=0;
                while (limit<=SafeQueueOf<BASE, ALLOWNULLS>::unsafeordinality())
                    if (stopped||!qwait(deqwaitsem,deqwaiting,timeout,start))
                        return false;
            }
            SafeQueueOf<BASE, ALLOWNULLS>::unsafeenqueue(e);
#ifdef SIGNAL_OUTSIDE
            numToSignal = enqwaiting;
            enqwaiting = 0;
#else
            if (enqwaiting) {
                enqwaitsem.signal(enqwaiting);
                enqwaiting = 0;
            }
#endif
        }
        // Signal when critical section no longer held so the reader can actually remove the item
        if (numToSignal)
            enqwaitsem.signal(numToSignal);
        return true;
    }

    bool enqueueMany(unsigned num, BASE * *e,unsigned timeout=INFINITE)
    {
        assertex(!limit);
        unsigned numToSignal = 0;
        {
            CriticalBlock b(SELF::crit);
            for (unsigned i=0; i < num; i++)
                SafeQueueOf<BASE, ALLOWNULLS>::unsafeenqueue(e[i]);

#ifdef SIGNAL_OUTSIDE
            numToSignal = enqwaiting;
            enqwaiting = 0;
#else
            if (enqwaiting) {
                enqwaitsem.signal(enqwaiting);
                enqwaiting = 0;
            }
#endif
        }
        if (numToSignal)
            enqwaitsem.signal(numToSignal);
        return true;
    }

    bool enqueueHead(BASE *e,unsigned timeout=INFINITE) 
    { 
        CriticalBlock b(SELF::crit);    
        if (limit) {
            unsigned start=0;
            while (limit<=SafeQueueOf<BASE, ALLOWNULLS>::unsafeordinality()) 
                if (stopped||!qwait(deqwaitsem,deqwaiting,timeout,start))
                    return false;
        }
        SafeQueueOf<BASE, ALLOWNULLS>::unsafeenqueueHead(e); 
        if (enqwaiting) {
            enqwaitsem.signal(enqwaiting);
            enqwaiting = 0;
        }
        return true;
    }

    bool enqueue(BASE *e,priorityGreaterFunction p,unsigned timeout=INFINITE) 
    { 
        CriticalBlock b(SELF::crit);    
        if (limit) {
            unsigned start=0;
            while (limit<=SafeQueueOf<BASE, ALLOWNULLS>::unsafeordinality())
                if (stopped||!qwait(deqwaitsem,deqwaiting,timeout,start))
                    return false;
        }
        SafeQueueOf<BASE, ALLOWNULLS>::unsafeenqueue(e,p); 
        if (enqwaiting) {
            enqwaitsem.signal(enqwaiting);
            enqwaiting = 0;
        }
        return true;
    }

    BASE *dequeue(unsigned timeout=INFINITE) 
    { 
        BASE *ret = nullptr;
        unsigned numToSignal = 0;
        {
            CriticalBlock b(SELF::crit);
            unsigned start=0;
            while (!stopped) {
                if (get(ret,false)) {
#ifdef SIGNAL_OUTSIDE
                    numToSignal = deqwaiting;
                    deqwaiting = 0;
#else
                    if (deqwaiting) {
                        deqwaitsem.signal(deqwaiting);
                        deqwaiting = 0;
                    }
#endif
                    break;
                }
                if (!qwait(enqwaitsem,enqwaiting,timeout,start))
                    break;
            }
        }
        if (numToSignal)
            deqwaitsem.signal(numToSignal);
        return ret;
    }

    BASE *dequeueTail(unsigned timeout=INFINITE) 
    { 
        CriticalBlock b(SELF::crit); 
        unsigned start=0;
        while (!stopped) {
            BASE *ret; 
            if (get(ret,true)) {
                if (deqwaiting) {
                    deqwaitsem.signal(deqwaiting);
                    deqwaiting = 0;
                }
                return ret;
            }
            if (!qwait(enqwaitsem,enqwaiting,timeout,start))
                break;
        }
        return NULL;
    }

    BASE *dequeueNow()
    {
        CriticalBlock b(SELF::crit);
        BASE * ret=NULL;
        if(get(ret,false)) {
            if(deqwaiting)
            {
                deqwaitsem.signal(deqwaiting);
                deqwaiting = 0;
            }
        }
        return ret;
    }

    bool waitMaxOrdinality(unsigned max,unsigned timeout)
    {
        CriticalBlock b(SELF::crit); 
        unsigned start=0;
        while (!stopped) {
            if (SafeQueueOf<BASE, ALLOWNULLS>::unsafeordinality()<=max)
                return true;
            if (!qwait(deqwaitsem,deqwaiting,timeout,start))
                break;
        }
        return false;
    }

    inline unsigned ordinality() const { return SafeQueueOf<BASE, ALLOWNULLS>::ordinality(); }
    unsigned setLimit(unsigned num) {  CriticalBlock b(SELF::crit); unsigned ret=limit; limit = num; return ret; }

    void stop() // stops all waiting operations
    {
        CriticalBlock b(SELF::crit); 
        do {
            stopped = true;
            if (enqwaiting) {
                enqwaitsem.signal(enqwaiting);
                enqwaiting = 0;
            }
            if (deqwaiting) {
                deqwaitsem.signal(deqwaiting);
                deqwaiting = 0;
            }
            {
                CriticalUnblock ub(SELF::crit); 
                Sleep(10);      // bit of a kludge
            }
        } while (enqwaiting||deqwaiting);
    }
};

template <class BASE, class OWNER, bool ALLOWNULLS> 
class CallbackInterThreadQueueOf : public SimpleInterThreadQueueOf<BASE, ALLOWNULLS>
{
    typedef CallbackInterThreadQueueOf<BASE, OWNER, ALLOWNULLS> SELF;
public:
    BASE * dequeueAndNotify(OWNER * owner, unsigned timeout=INFINITE) 
    { 
        CriticalBlock b(SELF::crit); 
        unsigned start=0;
        while (!SELF::stopped) {
            BASE *ret;
            if (this->get(ret,false)) {
                owner->notify(ret);
                if (SELF::deqwaiting) {
                    SELF::deqwaitsem.signal(SELF::deqwaiting);
                    SELF::deqwaiting = 0;
                }
                return ret;
            }
            if (!this->qwait(SELF::enqwaitsem,SELF::enqwaiting,timeout,start))
                break;
        }
        return NULL;
    }

    BASE *dequeueNowAndNotify(OWNER * owner)
    {
        CriticalBlock b(SELF::crit);
        BASE *ret=NULL;
        if (this->get(ret,false)) {
            owner->notify(ret);
            if(SELF::deqwaiting)
            {
                SELF::deqwaitsem.signal(SELF::deqwaiting);
                SELF::deqwaiting = 0;
            }
        }
        return ret;
    }
};

//A lighter-weight limited thread queue which does not allow timeouts.
//Linux futexes mean that semaphores now perform very well...
template <class BASE, bool ALLOWNULLS>
class ReallySimpleInterThreadQueueOf : protected SafeQueueOf<BASE, ALLOWNULLS>
{
    typedef ReallySimpleInterThreadQueueOf<BASE, ALLOWNULLS> SELF;
    typedef SafeQueueOf<BASE, ALLOWNULLS> PARENT;
protected:
    Semaphore space;
    Semaphore avail;
    unsigned limit = 0;
    std::atomic<bool> stopped{false};

public:
    ReallySimpleInterThreadQueueOf<BASE, ALLOWNULLS>()
    {
    }

    ~ReallySimpleInterThreadQueueOf<BASE, ALLOWNULLS>()
    {
    }

    void reset()
    {
        space.reinit(limit);
        avail.reinit(0);
        stopped = false;
    }

    bool enqueue(BASE *e)
    {
        space.wait();
        if (stopped)
            return false;
        PARENT::enqueue(e);
        avail.signal();
        return true;
    }

    BASE *dequeue()
    {
        avail.wait();
        if (stopped)
            return nullptr;
        BASE * result = PARENT::dequeue();
        space.signal();
        return result;
    }

    void setLimit(unsigned num)
    {
        limit = num;
        space.reinit(limit);
        PARENT::ensure(limit);
    }

    void stop(unsigned maxReaders, unsigned maxWriters) // stops all waiting operations
    {
        assertex(maxReaders && maxWriters);
        stopped = true;
        avail.signal(maxReaders);
        space.signal(maxWriters);
    }

    BASE *dequeueNow()
    {
        return PARENT::dequeue();
    }

    using PARENT::ordinality;
};

#define ForEachQueueItemIn(x,y)     unsigned numItems##x = (y).ordinality();     \
                                    for (unsigned x = 0; x< numItems##x; ++x)

#define ForEachQueueItemInRev(x,y)  unsigned x = (y).ordinality();              \
                                    while (x--) 


#endif
