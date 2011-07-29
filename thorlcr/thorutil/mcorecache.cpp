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

#include "platform.h"
#include "jlib.hpp"
#include "jthread.hpp"
#include "jdebug.hpp"
#include "jlog.hpp"

#include "mcorecache.hpp"

#ifdef _DEBUG
#define _FULL_TRACE
#endif

class CPassThroughMultiCoreCache: public CSimpleInterface, implements IMultiCoreCache
{
    IMultiCoreRowIntercept &wrapped;
    IRecordSize &recsize;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CPassThroughMultiCoreCache(IMultiCoreRowIntercept &_wrapped,IRecordSize &_recsize)
        : wrapped(_wrapped), recsize(_recsize)
    {
    }

    bool serialGet(size32_t maxsize, void * dst, size32_t &retsize)
    {
        return wrapped.parallelGet(maxsize,dst,retsize);
    }

    bool parallelGet(size32_t maxsize, void * dst, size32_t &retsize)
    {
        return wrapped.serialGet(maxsize,dst,retsize);
    }

    void start()
    {
    }

};



class CMultiCoreCache;



class CMultiCoreCache: extends CInterface, implements IMultiCoreCache
{
    IRecordSize &recsize;
    unsigned ncores;
    unsigned nthreads;          // >= ncores
    size32_t maxrecsize;

    class CMultiCoreCacheThread: public Thread
    {
        CMultiCoreCache *parent;
        unsigned idx;
        size32_t maxrecsz;
        PointerArray rows;          // maxrecsz sized rows - not freed until exit
        UnsignedArray rowsz;
        unsigned rowidx;
        MemoryBuffer buf;
        UnsignedArray varsizes;
        bool stopping;
        bool eos;

    public:
        ThreadId tid;
        Semaphore rowsready;
        Semaphore rowsfree;
        Semaphore startget;
        bool first;

        CMultiCoreCacheThread(CMultiCoreCache *_parent,unsigned _idx,size32_t _maxrecsz)
        {
            parent = _parent;
            idx = _idx;
            maxrecsz = _maxrecsz;
            rowidx = 0;
            stopping = false;
            eos = false;
            first = true;
        }

        ~CMultiCoreCacheThread()
        {
            assertex(stopping);
            while (rows.ordinality())
                free(rows.pop());
        }

        bool innerGet(size32_t _maxrecsz, void *out, size32_t &outsz)
        {
            // called indirectly by run 
            if (first) 
                first = false;
            else {
                // time to let waiting know that rows avails
#ifdef _FULL_TRACE
                PROGLOG("***Signal rowsready(a) %d",idx);
#endif
                rowsready.signal();             // tell someone we are rowsready
#ifdef _FULL_TRACE
                PROGLOG("***Wait rowsfree(a) %d",idx);
#endif
                rowsfree.wait();
#ifdef _FULL_TRACE
                PROGLOG("***Got rowsfree(a) %d",idx);
#endif
                // out should equal rows[0] probably (but do not rely on!)
                unsigned i = rowsz.ordinality();
                if (i) {
                    rows.swap(0,i);
                    rowsz.kill();
                }
            }
            // 
#ifdef _FULL_TRACE
            PROGLOG("***Wait startget(a) %d",idx);
#endif
            startget.wait();                    // block until can call
#ifdef _FULL_TRACE
            PROGLOG("***Got startget(a) %d",idx);
#endif
            bool ret = parent->wrapped.serialGet(_maxrecsz,out,outsz);  // may block
#ifdef _FULL_TRACE
            PROGLOG("***Signal startget(a) %d",idx+1);
#endif
            parent->queryNextThread(idx).startget.signal();
#ifdef _FULL_TRACE
            PROGLOG("***innerGet(ret %s) %d",ret?"true":"false",idx);
#endif
            return ret;
        }

        int run()
        {
            tid = GetCurrentThreadId();
            while (!stopping) {
                unsigned i = rowsz.ordinality();
                if (i>=rows.ordinality())
                    rows.append(malloc(maxrecsz));
                size32_t rs;
                bool got = parent->wrapped.parallelGet(maxrecsz,rows.item(i),rs);   // will block   
                rowsz.append(rs);
                if (!got&&(rowsz.ordinality()==1))
                    break;
            }
            eos = true;
            return 0;
        }       

            
        bool outerGet(size32_t _maxrecsz, void *out, size32_t &outsz)
        {
            if (rowidx>=rowsz.ordinality())  {
                rowidx = 0;

                if (eos) {
                    outsz = 0;
                    return false;
                }

#ifdef _FULL_TRACE
                PROGLOG("***Signal rowsfree(b) %d",idx);
#endif
                rowsfree.signal();

#ifdef _FULL_TRACE
                PROGLOG("***Wait rowsready(b) %d",idx);
#endif
                rowsready.wait();
#ifdef _FULL_TRACE
                PROGLOG("***Got rowsready(b) %d",idx);
#endif
                if (rowsz.ordinality()==0) {
                    outsz = 0;
                    return false;
                }
            }
            if (stopping) {
                outsz = 0;
                return false;
            }
            unsigned i = rowidx++;
            outsz = rowsz.item(i);
            if (outsz>_maxrecsz)
                outsz = _maxrecsz;
            if (outsz) 
                memcpy(out,rows.item(i),outsz);
            if (rowidx==rowsz.ordinality()) {
                parent->setInputThread(idx+1);
            }
            return outsz!=0;
        }


        void stop()
        {
#ifdef _FULL_TRACE
            PROGLOG("***stop %d",idx);
#endif
            if (!stopping) {
                stopping = true;
                rowsready.signal(10);
                rowsfree.signal(10);
                startget.signal(10);
                join();
            }
        }


    } **threads, *curthread;

    unsigned inidx;

public:
    IMultiCoreRowIntercept &wrapped;
    bool stopping;

    
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CMultiCoreCache(IMultiCoreRowIntercept &_wrapped,IRecordSize &_recsize, unsigned _ncores)
        : wrapped(_wrapped), recsize(_recsize)
    {
        ncores = _ncores;
        nthreads = ncores;
        inidx = 0;
        maxrecsize = recsize.getRecordSize(NULL);
        stopping = false;
        threads = NULL;
        curthread = NULL;
    }

    ~CMultiCoreCache() 
    {
        stopping = true;
        if (threads) {
            unsigned i;
            for (i=0;i<nthreads;i++) 
                threads[i]->stop();
            for (i=0;i<nthreads;i++) 
                delete threads[i];
            free(threads);
        }
    }

    void start()
    {
        threads = (CMultiCoreCacheThread **)malloc(sizeof(CMultiCoreCacheThread *)*nthreads);
        unsigned i;
        for (i=0;i<nthreads;i++) {
            threads[i] = new CMultiCoreCacheThread(this,i,maxrecsize);
        }
        for (i=0;i<nthreads;i++) {
            threads[i]->start();
        }
#ifdef _FULL_TRACE
        PROGLOG("***start signal rowsready 0");
#endif
        Sleep(1000);
        threads[0]->startget.signal();
        Sleep(1000);
        curthread = threads[0];
    }

    bool parallelGet(size32_t maxsize, void * dst, size32_t &retsize)
    {
        // bit of a kludge but effective with small number of threads
        // NB assumes (and asserts) that wrapped get calls on on same stack!!
        // maybe better to add opaque param to interface?
        ThreadId mytid = GetCurrentThreadId();
        unsigned i;
        for (i=0;;i++) {
            assertex(i<nthreads);
            if (threads[i]->tid==mytid)
                break;
        }
        return threads[i]->innerGet(maxsize,dst,retsize);
    }



    bool serialGet(size32_t maxsize, void * dst, size32_t &retsize)
    {
        return curthread->outerGet(maxsize,dst,retsize);
    }

    CMultiCoreCacheThread &queryNextThread(unsigned idx)
    {
        return *threads[(idx+1)%nthreads];
    }

    void setInputThread(int idx)
    {
        curthread = threads[idx%nthreads];
#ifdef _FULL_TRACE
        PROGLOG("***setInputThread thread %d",curthread);
#endif
    }
    
    

};


IMultiCoreCache *createMultiCoreCache(IMultiCoreRowIntercept &wrapped, IRecordSize &recsize)
{
    static unsigned numCPUs = 0;
    if (numCPUs==0) {
        unsigned CPUSpeed;
        getCpuInfo(numCPUs, CPUSpeed);
    }
    if (numCPUs<=1)
        return new CPassThroughMultiCoreCache(wrapped,recsize);
    return new CMultiCoreCache(wrapped,recsize,numCPUs);
}


