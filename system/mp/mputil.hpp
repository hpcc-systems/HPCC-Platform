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

    CMessageHandler(const char *_name,PARENT *_parent,void (PARENT::*_handler)(CMessageBuffer &_mb), IExceptionHandler *exceptionHandler=NULL, unsigned maxthreads=40, unsigned timeoutOnRelease=INFINITE)
    {
        parent = _parent;
        handler = _handler;
        name = strdup(_name);
        pool = createThreadPool(name,this,exceptionHandler,maxthreads,1000,0,timeoutOnRelease); // this will cause this to be linked
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
