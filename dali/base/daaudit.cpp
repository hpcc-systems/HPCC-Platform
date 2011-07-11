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

#define da_decl __declspec(dllexport)
#include "platform.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jtime.hpp"
#include "jregexp.hpp"
#include "jexcept.hpp"
#include "jsort.hpp"
#include "jptree.hpp"

#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"
#include "mputil.hpp"
#include "daserver.hpp"
#include "daclient.hpp"

#include "daaudit.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

LogMsgCategory const daliAuditLogCat(MSGAUD_audit, MSGCLS_information, DefaultDetail);


enum MAuditRequestKind { 
    MAR_QUERY
};


#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite

#define BUFFSIZE    (0x4000)
#define MAXLINESIZE (0x1000)
#define DATEEND     10
#define TIMEEND     19

unsigned get2dig(const char *s)
{
    if (!isdigit(*s)||!isdigit(s[1]))
        return 0;
    return (*s-'0')*10+s[1]-'0';
}

class CDaliAuditServer: public IDaliServer, public Thread
{  // Server side

    bool stopped;
    CriticalSection handlemessagesect;
    StringAttr auditdir;

    static int compfile(IInterface **v1, IInterface **v2) // for bAdd only
    {
        IFile *e1 = (IFile *)*v1;
        IFile *e2 = (IFile *)*v2;
        return strcmp(e1->queryFilename(),e2->queryFilename());
    }

    bool matchrow(const char *row,const char *match)
    {
        loop {
            char m = *match;
            if (!m) 
                break;
            match++;
            char c = *row;
            row++;
            if (c=='\n')
                return false;
            if (m==',') { // skip fields
                while (c!=',') {
                    c = *row;
                    if (c=='\n')
                        return (*match==0);
                    row++;
                }
            }
            else {
                loop {
                    if (m!=c)
                        return false;
                    m = *(match++);
                    c = *(row++);
                    if (c=='\n') 
                        return (m==0);
                    if (!m)
                        return (c==',');
                    if (m==',') {
                        if (c!=',')
                            return false;
                        break;
                    }
                }
            }
        }
        return true;
    }
public:

    void testmatch()
    {
        assertex(matchrow("Aa,Bb,Cc\n","Aa,Bb,Cc"));
        assertex(matchrow("Aa,Bb,Cc\n","Aa,,Cc"));
        assertex(matchrow("Aa,Bb,Cc\n","Aa,Bb,"));
        assertex(matchrow("Aa,Bb,Cc\n",",Bb,Cc"));
        assertex(matchrow("Aa,Bb,Cc\n",",Bb,"));
        assertex(matchrow("Aa,Bb,Cc\n",",Bb"));
        assertex(matchrow("Aa,Bb,Cc\n",",,Cc"));
        assertex(matchrow("A\n","A"));
        assertex(!matchrow("A\n","A,"));
        assertex(!matchrow("A\n",",A"));
        assertex(!matchrow("Aa,Bb,Cc\n","Aa,bBb,Cc"));
        assertex(!matchrow("Aa,Bb,Cc\n","Aa,Bbb,Cc"));
        assertex(!matchrow("Aa,Bcb,Cc\n","Aa,Bb,Cc"));
        assertex(!matchrow("Aa\n","Aa,Bb,Cc"));
        assertex(!matchrow("Aa,Bb,Ccd\n","Aa,Bb,Cc"));
    }

    unsigned scan(const CDateTime &from,const CDateTime &to,const char *match,unsigned start,unsigned maxn,MemoryBuffer &res, bool fixlocal)
    {
        if (!match)
            match = "";
        StringBuffer fromstr;
        from.getString(fromstr,fixlocal);
        fromstr.setCharAt(DATEEND,' ');
        const char *frommatch = fromstr.str();
        StringBuffer tostr;
        to.getString(tostr,fixlocal);
        tostr.setCharAt(DATEEND,' ');
        const char *tomatch = tostr.str();
        CDateTime dt;
        Owned<IFile> dir = createIFile(auditdir.get()); 
        Owned<IDirectoryIterator> files = dir->directoryFiles("DaAudit.*");
        IArrayOf<IFile> filelist;
        StringBuffer fname;
        ForEach(*files) {
            files->getName(fname.clear());
            unsigned fday = get2dig(fname.str()+11);
            unsigned fmonth = get2dig(fname.str()+8);
            unsigned fyear = get2dig(fname.str()+14);
            if (fday&&fmonth&&fyear) {
                dt.setDate(fyear+2000,fmonth,fday);
                if ((dt.compareDate(to)<=0)&&(dt.compareDate(from)>=0)) {
                    IInterface *e = &files->get();
                    bool added;
                    filelist.bAdd(e,compfile,added);
                }
            }
        }
        unsigned n = 0;
        ForEachItemIn(fi,filelist) {
            Owned<IFileIO> fio = filelist.item(fi).openShared(IFOread,IFSHfull);
            if (!fio) {
                StringBuffer fn;
                files->getName(fn);
                WARNLOG("Could not open %s",fn.str());
                continue;
            }
            bool eof=false;
            offset_t fpos = 0;
            MemoryAttr mba;
            char *buf = (char *)mba.allocate(BUFFSIZE+MAXLINESIZE+1);
            size32_t lbsize = 0;
            char *p=NULL;
            loop {
Retry:
                if (lbsize<MAXLINESIZE) {
                    if (!eof) {
                        if (lbsize&&(p!=buf)) 
                            memmove(buf,p,lbsize);
                        p = buf;
                        size32_t rd = fio->read(fpos,BUFFSIZE,buf+lbsize);
                        if (rd==0) {
                            eof = true;
                            while (lbsize&&(buf[lbsize-1]!='\n'))
                                lbsize--;                           // remove unfinished line
                            if (lbsize==0)
                                break;
                        }
                        else {
                            lbsize += rd;
                            fpos += rd;
                        }
                    }
                    else if (lbsize==0)
                        break;
                }
                if ((p[4]!='-')&&(p[13]=='-')) {
                    p+=9; // kludge while msgid prefix about
                    lbsize-=9;
                    goto Retry;
                }
                size32_t len = 0;
                while ((len<MAXLINESIZE)&&(p[len]!='\n'))
                    len++;
                if ((len>TIMEEND+1)&&(memcmp(p,frommatch,TIMEEND)>=0)&&(memcmp(p,tomatch,TIMEEND)<0)) {
                    if (!*match||matchrow(p+TIMEEND+2,match)) {
                        if (start)
                            start--;
                        else {
                            res.append(len+1,p);
                            n++;
                            if (n>=maxn)
                                return n;
                        }
                    }
                }
                len++;
                lbsize-=len;
                p+=len;
            }   
        }
        return n;
    }

public:
    IMPLEMENT_IINTERFACE;

    CDaliAuditServer(const char *_auditdir)
        : auditdir(_auditdir),Thread("CDaliAuditServer")
    {
        stopped = true;
    }

    ~CDaliAuditServer()
    {
    }

    void start()
    {
        Thread::start();
    }

    void ready()
    {
    }
    
    void suspend()
    {
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            queryCoven().cancel(RANK_ALL,MPTAG_DALI_AUDIT_REQUEST);
        }
        join();
    }

    int run()
    {
        ICoven &coven=queryCoven();
        CMessageBuffer mb;
        stopped = false;
        while (!stopped) {
            try {
                mb.clear();
                if (coven.recv(mb,RANK_ALL,MPTAG_DALI_AUDIT_REQUEST,NULL)) {
                    processMessage(mb); // not synchronous to ensure queue operations handled in correct order
                }   
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "CDaliAuditServer:run");
                e->Release();
                stopped = true;
            }
        }
        return 0;
    }


    void processMessage(CMessageBuffer &mb)
    {
        CriticalBlock block(handlemessagesect);
        ICoven &coven=queryCoven();
        int fn;
        mb.read(fn);
        unsigned ret = 0;
        try {
            switch (fn) {
            case MAR_QUERY:                     
                {
                    CDateTime from;
                    CDateTime to;
                    StringAttr match;
                    unsigned start;
                    unsigned maxn;
                    from.deserialize(mb);
                    to.deserialize(mb);
                    mb.read(match).read(start).read(maxn);
                    bool fixlocal = false;
                    if (mb.remaining()>sizeof(bool))
                        mb.read(fixlocal);
                    mb.clear().append((unsigned)0).append((unsigned)0);
                    unsigned n = scan(from,to,match,start,maxn,mb,fixlocal);
                    mb.writeDirect(sizeof(unsigned),sizeof(unsigned),&n);
                }
                break;
            }
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            EXCLOG(e, "Audit Request Server - handleMessage");
            mb.clear().append(e->errorCode()).append(s.str());
            e->Release();
        }
        coven.reply(mb); 

    }

    void nodeDown(rank_t rank)
    {
        assertex(!"TBD");
    }


};

IDaliServer *createDaliAuditServer(const char *auditdir)
{
    return new CDaliAuditServer(auditdir);
}

                        

unsigned queryAuditLogs(const CDateTime &from,const CDateTime &to, const char *match,StringAttrArray &out,unsigned start, unsigned max)
{
    CMessageBuffer mb;
    mb.append((int)MAR_QUERY);
    from.serialize(mb);
    to.serialize(mb);
    if (!match||(strcmp(match,"*")==0))
        match = "";
    bool fixlocal = true; 
    mb.append(match).append(start).append(max).append(fixlocal);
    queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_AUDIT_REQUEST);
    unsigned ret;
    mb.read(ret);
    if (ret) {
        StringAttr except;
        mb.read(except);
        throw MakeStringException(ret,"Audit Request Server Exception: %s",except.get());
    }
    unsigned n;
    mb.read(n);
    const char *base = mb.toByteArray()+mb.getPos();
    const char *p = base;
    unsigned l = mb.length()-mb.getPos();
    for (unsigned i=0;i<n;i++) {
        const char *q=p;
        while (((unsigned)(q-base)<l)&&(*q!='\n')) 
            q++;
        out.append(*new StringAttrItem(p,q-p));
        p = q+1;
    }
    return n;
}

