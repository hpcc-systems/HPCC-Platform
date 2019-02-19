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

#include "jfile.hpp"
#include "jlog.hpp"
#include "jqueue.tpp"
#include "jsuperhash.hpp"

#include "thormisc.hpp"
#include "slavmain.hpp"

#include "backup.hpp"

class CStringTuple : public CSimpleInterface
{
public:
    CStringTuple(const char *_dst, const char *_src) : dst(_dst), src(_src) { }
    CStringTuple(const char *_dst, unsigned dstL, const char *_src, unsigned srcL) : dst(_dst, dstL), src(_src, srcL) { }
    StringAttr dst, src;

    const char *queryFindString() const { return src; }
};

class CThorBackupHandler : public CSimpleInterface, implements IBackup, implements IThreaded, implements ICopyFileProgress
{
    CThreaded threaded;
    bool aborted, currentAbort;
    Semaphore sem, cancelSem;
    CriticalSection crit;
    StringSuperHashTableOf<CStringTuple> lookup;
    QueueOf<CStringTuple, false> todo;
    OwnedIFileIO iFileIO;
    Owned<CStringTuple> currentItem;
    static const char *formatV;
    bool async;

    void write()
    {
        // JCS could compress output
        if (0 == todo.ordinality())
            iFileIO->setSize(0);
        else
        {
            Owned<IFileIOStream> stream = createBufferedIOStream(iFileIO);
            stream->write(3, formatV); // 3 byte format definition, incase of change later
            ForEachItemIn(i, todo)
            {
                CStringTuple *item = todo.item(i);
                writeStringToStream(*stream, item->dst);
                writeCharToStream(*stream, ' ');
                writeStringToStream(*stream, item->src);
                writeCharToStream(*stream, '\n');
            }
            offset_t pos = stream->tell();
            stream.clear();
            iFileIO->setSize(pos);
        }
    }
    void doBackup(CStringTuple *item, bool ignoreError=true)
    {
        PROGLOG("CThorBackupHandler, copying to target: %s", item->dst.get());
        StringBuffer backupTmp(item->dst);
        backupTmp.append(".__tmp");
        Owned<IFile> backupIFile = createIFile(backupTmp.str());
        OwnedIFile primaryIFile = createIFile(item->src);
        try
        {
            ensureDirectoryForFile(backupTmp.str());
            copyFile(backupIFile, primaryIFile, 0x100000, this);
            bool doit = false;
            if (primaryIFile->exists()) // possible it could have disappeared in interim
            {
                if (primaryIFile->size() == backupIFile->size()) // check if primary altered in interim OR if copy was cancelled
                {
                    CDateTime primaryDT, backupDT;
                    primaryIFile->getTime(NULL, &primaryDT, NULL);
                    primaryIFile->getTime(NULL, &backupDT, NULL);
                    if (primaryDT.equals(backupDT))
                        doit = true;
                }
            }
            
            if (doit)
            {
                OwnedIFile dstIFile = createIFile(item->dst);
                dstIFile->remove();
                backupIFile->rename(pathTail(item->dst.get()));
                PROGLOG("Backed up: '%s'", item->dst.get());
            }
            else
            {
                StringBuffer errMsg;
                if (!currentAbort)
                    LOG(MCuserWarning, thorJob, "%s", errMsg.append("Backup inconsistency detected, backup aborted: ").append(item->dst).str());
                backupIFile->remove();
            }
        }
        catch (IException *e)
        {
            StringBuffer errMsg("copying: ");
            LOG(MCuserWarning, thorJob, e, errMsg.append(item->src));
            try { backupIFile->remove(); } catch (IException *e) { EXCLOG(e); e->Release(); }
            if (!ignoreError)
                throw;
            e->Release();
        }
    }
    void _backup(const char *dst, const char *src, bool writeFile=true)
    {
        CStringTuple *item = lookup.find(dst);
        assertex(!item);
        item = new CStringTuple(dst, src);
        if (async)
        {
            todo.enqueue(item);
            lookup.add(* item);
            if (writeFile)
                write();
        }
        else
        {
            {
                CriticalBlock b(crit);
                currentItem.setown(item);
            }
            doBackup(currentItem, false);
            {
                CriticalBlock b(crit);
                currentItem.clear();
            }
        }
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorBackupHandler() : threaded("CBackupHandler")
    {
        aborted = currentAbort = false;
        async = globals->getPropBool("@replicateAsync", true);
        init();
    }
    ~CThorBackupHandler()
    {
        aborted = true;
        if (async)
        {
            sem.signal();
            threaded.join();
        }
        deinit();
    }

    void deinit()
    {
        for (;;)
        {
            CStringTuple *item = todo.dequeue();
            if (!item) break;
            item->Release();
        }
        lookup.kill();
    }

    void init()
    {
        deinit();
        StringBuffer path;
        globals->getProp("@thorPath", path);
        addPathSepChar(path);
        path.append("backup_");
        globals->getProp("@name", path);
        path.append("_");
        path.append(queryNodeGroup().rank(queryMyNode()));
        path.append(".lst");
        ensureDirectoryForFile(path.str());
        Owned<IFile> iFile = createIFile(path.str());
        iFileIO.setown(iFile->open(IFOreadwrite));
        if (!iFileIO)
        {
            PROGLOG("Failed to open/create backup file: %s", path.str());
            aborted = true;
            return;
        }
        MemoryBuffer mb;
        read(iFileIO, 0, (unsigned)iFileIO->size(), mb);
        const char *mem = mb.toByteArray();
        if (mem)
        {
            const char *endMem = mem+mb.length();
            mem += 3; // formatV header
            do
            {
                const char *sep = strchr(mem, ' ');
                const char *eol = strchr(sep+1, '\n');
                StringAttr dst(mem, sep-mem);
                StringAttr src(sep+1, eol-(sep+1));
                _backup(dst, src, false);
                mem = eol+1;
            }
            while (mem != endMem);
        }
        if (async)
        {
            threaded.init(this);
            sem.signal();
        }
    }

    virtual void threadmain() override
    {
        threaded.adjustPriority(-2);
        for (;;)
        {
            sem.wait();
            if (aborted) break;
            {
                CriticalBlock b(crit);
                currentItem.setown((CStringTuple *)todo.dequeue());
                if (!currentItem) continue;
                lookup.removeExact(currentItem.get());
                write();
                currentAbort = false;
            }
            doBackup(currentItem);
            if (currentAbort)
            {
                currentAbort = false;
                cancelSem.signal();
            }
            CriticalBlock b(crit);
            currentItem.clear();
        }
    }
    
// ICopyFileProgress
    virtual CFPmode onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize)
    {
        CriticalBlock b(crit);
        if (aborted||currentAbort)
        {
            if (currentAbort)
            {
                currentAbort = false;
                cancelSem.signal();
            }
            return CFPstop;
        }
        return CFPcontinue;
    }

// IBackup
    virtual void backup(const char *dst, const char *src)
    {
        if (aborted) return;
        {
            CriticalBlock b(crit);
            _backup(dst, src);
        }
        if (async)
            sem.signal();
    }
    virtual void cancel(const char *dst)
    {
        if (aborted) return;
        CriticalBlock b(crit);
        if (!currentAbort && currentItem && 0 == strcmp(currentItem->dst, dst))
        {
            currentAbort = true;
            CriticalUnblock b(crit); // _cancel always called in critical block
            if (cancelSem.wait(5000))
                PROGLOG("Backup: cancelled copy in progress: '%s'", dst);
            else
                PROGLOG("Backup: cancelled [timeout] copy in progress: '%s'", dst);
        }
        else if (async)
        {
            CStringTuple *item = lookup.find(dst);
            if (item)
            {
                lookup.removeExact(item);
                unsigned pos = todo.find(item);
                todo.dequeue(item);
                item->Release();
                write();
                PROGLOG("Backup: cancelled: '%s'", dst);
            }
        }
    }
};

const char *CThorBackupHandler::formatV = "01\n";


IBackup *createBackupHandler()
{
    return new CThorBackupHandler();
}

