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

#include <platform.h>
#include "thirdparty.h"
#include <stdio.h>
#include <limits.h>
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"
#include "jstring.hpp"
#include "jmisc.hpp"
#include "jprop.hpp"
#include "jio.hpp"

#include "dadfs.hpp"
#include "danqs.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "dalienv.hpp"
#include "mpcomm.hpp"
#include "mplog.hpp"
#include "rmtfile.hpp"

#include "dfuerror.hpp"
#include "dfurepl.hpp"

#define LOGPFX "REPLICATE: "
#define SDS_TIMEOUT (60*60*1000) // 1hr
#define MAX_REPLICATING_THREADS 20

struct ReplicatePartItem;
struct ReplicateFileItem;
class CReplicateServer;


enum ReplicatePartCopyState 
{
    RPCS_exists,
    RPCS_missing,
    RPCS_tempcopied,
    RPCS_failed,
    RPCS_failedtempcopy
};



struct ReplicatePartCopyItem: extends CInterface
{
    Owned<IFile> file;
    RemoteFilename tmpname;
    ReplicatePartCopyState state;       
    ReplicatePartCopyItem *from;
    ReplicatePartItem &parent;

    ReplicatePartCopyItem(ReplicatePartItem &parent,IFile *_file,unsigned pn);

    bool startCopy(ReplicatePartCopyItem &_from)    // returns true if not done in one
    {
        from = &_from;
        return !doneCopy(0);
    }

    bool doneCopy(unsigned timeout); 

    void commit(bool rollback)
    {
        if (state==RPCS_failedtempcopy)
            rollback = true;
        else if (state!=RPCS_tempcopied)
            return;
        Owned<IFile> tmpfile;
        try {   // failures don't affect other parts
            tmpfile.setown(createIFile(tmpname));
            if (!rollback&&tmpfile.get()) {
                tmpfile->rename(pathTail(file->queryFilename()));
                return;
            }
        }
        catch (IException *e) {
            StringBuffer s(LOGPFX "Error renaming temp file ");
            tmpname.getRemotePath(s);
            s.append(" to ").append(pathTail(file->queryFilename()));
            EXCLOG(e,s.str());
            e->Release();
        }
        if (tmpfile) { // failed/rollback 
            try {   // failures don't affect other parts
                tmpfile->remove();  
            }
            catch (IException *e) {
                // suppress delete errors on rollback (already reported elsewhere)
                e->Release();
            }
        }
    }
};


struct ReplicatePartItem: extends CInterface
{
    CIArrayOf<ReplicatePartCopyItem> copies;
    ReplicateFileItem &parent;

    ReplicatePartItem(ReplicateFileItem &parent,IDistributedFilePart &part);
    unsigned startReplicate()
    {
        unsigned ret = 0;
        UnsignedArray src; // bit OTT when probably only 2 but you never know
        UnsignedArray dst; 
        ForEachItemIn(i1,copies) {  
            switch(copies.item(i1).state) {
            case RPCS_exists: 
                src.append(i1); 
                break;
            case RPCS_missing: 
                dst.append(i1); 
                break;
            }
        }
        unsigned i3=0;
        ForEachItemIn(i2,dst) {  
            ReplicatePartCopyItem &dstcopy = copies.item(dst.item(i2));
            if (i3>=src.ordinality()) {
                if (i3==0) {
                    ERRLOG(LOGPFX "Cannot find copy for %s",dstcopy.file->queryFilename());
                    dstcopy.state = RPCS_failed;
                }
                else
                    i3 = 0;
            }
            if (dstcopy.startCopy(copies.item(src.item(i3))))
                ret++;
        }
        return ret;
    }
    void waitReplicate()
    {
        loop {
            bool alldone=true;
            ForEachItemIn(i1,copies) {  
                if (!copies.item(i1).doneCopy(1000*60)) 
                    alldone = false;
            }
            if (alldone)
                break;
            Sleep(1000); // actually not really needed as doneCopy will delay (but JIC)
        }
    }

    void commit(bool rollback)
    {
        ForEachItemIn(i1,copies) {  
            copies.item(i1).commit(rollback);
        }
    }

};


struct ReplicateFileItem: extends CInterface
{
    CDfsLogicalFileName dlfn;
    Linked<IUserDescriptor> userdesc;
    Owned<IRemoteConnection> conn;
    CReplicateServer &parent;
    CDateTime filedt;                               // used to validate same file
    ReplicateFileItem(CReplicateServer &_parent);
    StringAttr uuid;
    CIArrayOf<ReplicatePartItem> parts;
    Semaphore sem;
    CriticalSection sect;
    bool stopping;

    class cThread: public Thread
    {
        ReplicateFileItem &parent;
    public:
        cThread(ReplicateFileItem &_parent)
            : Thread("ReplicateFileThread"), parent(_parent)
        {
        }
        int run()
        {
            try {
                parent.main();
            }
            catch (IException *e) {
                EXCLOG(e,LOGPFX "Replicate thread error(1)");
                e->Release();
            }
            try {
                parent.done();
            }
            catch (IException *e) {
                EXCLOG(e,LOGPFX "Replicate thread error(2)");
                e->Release();
            }
            return 1;
        }
    } thread;

    ReplicateFileItem(CReplicateServer &_parent,CDfsLogicalFileName _dlfn,IUserDescriptor *_userdesc,CDateTime _filedt)
        : thread(*this), parent(_parent), dlfn(_dlfn), userdesc(_userdesc), filedt(_filedt)
    {
        stopping = true; // set false once started
    }

    ~ReplicateFileItem()
    {
        stop();
    }

    void start()
    {
        CriticalBlock block(sect);
        stopping = false;
        thread.start();
    }

    void stop()
    {
        {
            CriticalBlock block(sect);
            if (stopping)
                return;
            stopping = true;
            sem.signal();
        }
        thread.join();
    }

    void done();

    void main()
    {
        StringBuffer tmp;
        const char *lfn = dlfn.get();
        if (dlfn.isExternal()) {
            ERRLOG(LOGPFX "Cannot replicate external file %s",lfn);
            return;
        }
        if (dlfn.isForeign()) {
            ERRLOG(LOGPFX "Cannot replicate foreign file %s",lfn);
            return;
        }
        Owned<IDistributedFile> dfile = queryDistributedFileDirectory().lookup(dlfn,userdesc);
        if (!dfile) {
            WARNLOG(LOGPFX "Cannot find file %s, perhaps deleted",lfn);
            return;
        }
        CDateTime dt;       
        dfile->getModificationTime(dt);     // check not modified while queued
        if (!filedt.equals(dt)) {
            dt.getString(filedt.getString(tmp.clear()).append(','));
            WARNLOG(LOGPFX "File %s changed (%s), ignoring replicate",lfn,tmp.str());
            return;
        }
        // see if already replicating
        Owned<IRemoteConnection> pconn = querySDS().connect("DFU/Replicating", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY,  SDS_TIMEOUT);
        if (!pconn.get()) {
            ERRLOG(LOGPFX "Connect to DFU/Replicating %s failed",lfn);
            return;
        }
        StringBuffer xpath;
        xpath.appendf("File[@name=\"%s\"]",lfn);
        if (pconn->queryRoot()->hasProp(xpath.str())) {
            // read date
            xpath.append("/@filedt");
            pconn->queryRoot()->getProp(xpath.str(),tmp.clear());
            dt.setDateString(tmp.str());
            if (filedt.equals(dt)) {
                WARNLOG(LOGPFX "Already replicating %s, ignoring",lfn);
                return;
            }
            else
                WARNLOG(LOGPFX "Already replicating %s, contining",lfn);
        }
        // now as long as SDS doesn't lock children this should be OK
        conn.setown(querySDS().connect("DFU/Replicating/File", myProcessSession(), RTM_CREATE_ADD | RTM_LOCK_READ, 5*60*1000));
        if (!conn.get()) {
            ERRLOG(LOGPFX "Create of DFU/Replicating/File %s failed",lfn);
            return;
        }
        genUUID(tmp.clear(),true);  // true for windows
        uuid.set(tmp.str());
        IPropertyTree &root = *conn->queryRoot();
        root.setProp("@name",lfn);
        StringBuffer node;
        queryMyNode()->endpoint().getIpText(node);
        root.setProp("@node",node.str());
        root.setPropInt("@mpport",queryMyNode()->endpoint().port);
        dt.setNow();
        root.setProp("@started",dt.getString(tmp.clear()).str());
        root.setProp("@filedt",filedt.getString(tmp.clear()).str());
        root.setProp("@uuid",uuid.get());
        root.setPropInt64("@session",myProcessSession());
        conn->commit();
        pconn.clear();  // don't need parent now
        Owned<IDistributedFilePartIterator> piter = dfile->getIterator();
        ForEach(*piter) {
            parts.append(*new ReplicatePartItem(*this,piter->query()));
        }
        // now free file and get to work
        dfile.clear();
        PROGLOG(LOGPFX "Starting replicate of %s",lfn);
        unsigned numtoreplicate = 0;
        ForEachItemIn(i1,parts) {
            numtoreplicate += parts.item(i1).startReplicate();
        }
        if (numtoreplicate==0) {
            PROGLOG(LOGPFX "Nothing to do for %s",lfn);
            return;
        }
        PROGLOG(LOGPFX "Started replicate of %s (%d parts left to copy)",lfn,numtoreplicate);
        ForEachItemIn(i2,parts) {
            parts.item(i2).waitReplicate();
        }
        bool abort = true;
        try {
            dfile.setown(queryDistributedFileDirectory().lookup(dlfn,userdesc));
            if (dfile) {
                CDateTime newfiledt;
                dfile->getModificationTime(newfiledt);
                if (filedt.equals(newfiledt)) 
                    abort = false;
                else {
                    newfiledt.getString(filedt.getString(tmp.clear()).append(','));
                    WARNLOG(LOGPFX "File %s changed (%s)",lfn,tmp.str());
                }
            }
            else 
                WARNLOG(LOGPFX "Cannot find file %s, perhaps deleted",lfn);
        }
        catch (IException *e) { // actually don't expect (unless maybe dali down or something)
            EXCLOG(e,LOGPFX "Replicate error(3)");
            e->Release();
        }
        PROGLOG(LOGPFX "%s parts copy for %s, start temp file %s",abort?"Aborting":"Finalizing",lfn,abort?"delete":"rename");
        ForEachItemIn(i3,parts) {
            parts.item(i3).commit(abort);
        }
        PROGLOG(LOGPFX "%s replicate of %s",abort?"Aborted":"Finished",lfn);
    }

};

ReplicatePartItem::ReplicatePartItem(ReplicateFileItem &_parent,IDistributedFilePart &part)
    : parent(_parent)
{
    for (unsigned c=0;c<part.numCopies();c++) {
        RemoteFilename rfn;
        part.getFilename(rfn,c);
        try {
            IFile *file = createIFile(rfn);
            copies.append(*new ReplicatePartCopyItem(*this,file,part.getPartIndex()+1));
        }
        catch (IException *e) { // not sure if possible but in case
            StringBuffer s(LOGPFX "Error replicating part ");
            rfn.getRemotePath(s);
            EXCLOG(e,s.str());
            e->Release();
        }
    }

}

ReplicatePartCopyItem::ReplicatePartCopyItem(ReplicatePartItem &_parent,IFile *_file,unsigned pn)
    : parent(_parent),file(_file)
{
    from = NULL;
    try {
        if (file->exists())
            state = RPCS_exists;
        else {
            state = RPCS_missing;
            StringBuffer dst;
            splitDirTail(file->queryFilename(),dst);
            addPathSepChar(dst).append("R_").append(parent.parent.uuid).append('_').append(pn).append(".tmp");
            tmpname.setRemotePath(dst.str());
        }
    }
    catch (IException *e) {
        state = RPCS_failed;
        StringBuffer s(LOGPFX "calling exists ");
        s.append(file->queryFilename());
        EXCLOG(e,s.str());
        e->Release();
    }
}

bool ReplicatePartCopyItem::doneCopy(unsigned timeout)
{
    if (state==RPCS_missing) {
        try {
            if (asyncCopyFileSection(
                parent.parent.uuid,
                from->file,
                tmpname,
                (offset_t)-1, // creates file
                0,
                (offset_t)-1, // all file
                NULL, // no progress needed (yet)
                timeout))
                    state = RPCS_tempcopied;  // done
        }
        catch (IException *e) {
            EXCLOG(e,LOGPFX "Replicate part copy error");
            e->Release();
            state = RPCS_failedtempcopy;    // tmpfile will be deleted later
        }
    }
    return state!=RPCS_missing;
}


class CReplicateServer: public CInterface, implements IThreaded, implements IReplicateServer
{
    CriticalSection runningsect;
    Semaphore runningsem;
    CIArrayOf<ReplicateFileItem> running;
    Owned<IQueueChannel> qchannel;
    bool stopping;
    Owned<CThreaded> thread;
    StringAttr qname;

public:
    IMPLEMENT_IINTERFACE;

    CReplicateServer(const char *_qname)
        : qname(_qname)
    {
        runningsem.signal(MAX_REPLICATING_THREADS);
    }

    void replicateFile(const char *lfn,IUserDescriptor *userdesc, CDateTime &filedt)
    {
        CDfsLogicalFileName dlfn;
        dlfn.set(lfn);
        while (!runningsem.wait(1000*60))
            PROGLOG(LOGPFX "too many replications active, waiting");
        CriticalBlock block(runningsect);
        ReplicateFileItem &fitem = *new ReplicateFileItem(*this,dlfn,userdesc,filedt);
        running.append(fitem);  
        fitem.start();
    }

    void done(ReplicateFileItem *item)
    {
        CriticalBlock block(runningsect);
        if (running.zap(*item)) 
            runningsem.signal();
    }
        
    void runServer()
    {
        stopping = false;
        thread.setown(new CThreaded("ReplicateServerThread"));
        thread->init(this);
    }

    void main()
    {
        Owned<INamedQueueConnection> qconn = createNamedQueueConnection(0);
        qchannel.setown(qconn->open(qname));
        MemoryBuffer mb;
        while (!stopping) {
            try {
                PROGLOG(LOGPFX "Waiting on queue %s", qname.get());
                qchannel->get(mb.clear());
                byte fn;
                if (stopping||(mb.length()==0))
                    fn = DRQ_STOP;
                else 
                    mb.read(fn);
                switch (fn) {
                case DRQ_STOP:
                    stopping = true;
                    break;
                case DRQ_REPLICATE: {
                        StringAttr lfn;
                        mb.read(lfn);
                        Owned<IUserDescriptor> userdesc = createUserDescriptor();
                        userdesc->deserialize(mb);
                        CDateTime filedt;
                        filedt.deserialize(mb);
                        replicateFile(lfn,userdesc,filedt);
                    }
                    break;
                }
            }
            catch (IException *e) {
                EXCLOG(e,LOGPFX "Server thread(1)");    // exit DFU server? (e.g. Dali down)
                return;
            }
        }
        qchannel.clear();
    }

    void stopServer()
    {
        if (!stopping&&qchannel) {
            stopping = true;
            qchannel->cancelGet();
        }
        if (thread)
            thread->join();
    }

};

void ReplicateFileItem::done()
{
    parent.done(this);
}



IReplicateServer *createReplicateServer(const char *qname)
{
    return new CReplicateServer(qname);
}





