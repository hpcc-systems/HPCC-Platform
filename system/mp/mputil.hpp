/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef MPUTIL_HPP
#define MPUTIL_HPP

#include "jtime.hpp"
#include "mpbase.hpp"
#include "mpbuff.hpp"
#include "mpcomm.hpp"

// Template to handle a message on another thread

template <class PARENT>
class CMessageHandler: public CInterface, public IThreadFactory
{
    PARENT *parent;
    void (PARENT::*handler)(CMessageBuffer &_mb);
    IThreadPool *pool;
    bool hasexceptionhandler;
    char *name;
public:
    virtual void Link(void) const       { CInterface::Link(); }                     
    virtual bool Release(void) const    
    { 
        //Note: getLinkCount() is not thread safe
        if (pool&&(CInterface::getLinkCount()==2)) { // circular dependancy
            pool->Release();
            const_cast<CMessageHandler *>(this)->pool = NULL;
        }
        return CInterface::Release(); 
    }

    CMessageHandler(const char *_name,PARENT *_parent,void (PARENT::*_handler)(CMessageBuffer &_mb), IExceptionHandler *exceptionHandler=NULL, unsigned maxthreads=40, unsigned timeoutOnRelease=INFINITE, unsigned lowThreadsDelay=1000)
    {
        parent = _parent;
        handler = _handler;
        name = strdup(_name);
        pool = createThreadPool(name,this,exceptionHandler,maxthreads,lowThreadsDelay,0,timeoutOnRelease); // this will cause this to be linked
        hasexceptionhandler = exceptionHandler!=NULL;
    }
    ~CMessageHandler()
    {
        if (pool) {
            IThreadPool *p = pool;
            pool = NULL;
            p->Release();
        }
        free(name);
    }

    void main(CMessageBuffer &mb)
    {
        if (hasexceptionhandler)
            (parent->*handler)(mb);
        else {
            try {
                (parent->*handler)(mb);
            }
            catch (IException *e) {
                EXCLOG(e, name);
                e->Release();
            }
        }
        mb.resetBuffer();
    }

    class Chandler: public CInterface, implements IPooledThread
    {
        CMessageBuffer mb;
        CMessageHandler<PARENT> *owner;
    public:
        IMPLEMENT_IINTERFACE;
        Chandler(CMessageHandler<PARENT> *_owner)
        {
            owner = _owner;
        }
        void init(void *_mb) 
        {
            mb.transferFrom(*(CMessageBuffer *)_mb);
        }
        void main()
        {
            owner->main(mb);
        }
        bool canReuse()
        {
            return true;
        }
        bool stop()
        {
            return true; 
        }
    };
    IPooledThread *createNew()
    {
        return new Chandler(this);
    }
    void handleMessage (CMessageBuffer &mb)
    {
        StringBuffer runname(name);
        runname.append(" Message:");
        byte *b = (byte *)mb.toByteArray();
        size32_t l = mb.length();
        if (l>32)
            l = 32;
        while (l--) 
            runname.append(' ').appendhex(*(b++),true);
        pool->start(&mb,runname.str());
    }
};







#endif
