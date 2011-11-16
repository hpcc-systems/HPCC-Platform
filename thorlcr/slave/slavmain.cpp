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

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "jprop.hpp"
#include "jiter.ipp"

#include "jhtree.hpp"
#include "mpcomm.hpp"

#include "portlist.h"
#include "rmtfile.hpp"
#include "daclient.hpp"
#include "dafdesc.hpp"

#include "slwatchdog.hpp"
#include "thbuf.hpp"
#include "thcrc.hpp"
#include "thmem.hpp"
#include "thexception.hpp"

#include "backup.hpp"
#include "slave.hpp"
#include "thormisc.hpp"
#include "thorport.hpp"
#include "thgraphslave.hpp"
#include "slave.ipp"

//---------------------------------------------------------------------------

//---------------------------------------------------------------------------

class CJobListener : public CSimpleInterface
{
    bool stopped;
    CriticalSection crit;
    OwningStringSuperHashTableOf<CJobSlave> jobs;
    CFifoFileCache querySoCache; // used to mirror master cache

    class CThreadExceptionCatcher : implements IExceptionHandler
    {
        CJobListener &jobListener;
    public:
        CThreadExceptionCatcher(CJobListener &_jobListener) : jobListener(_jobListener)
        {
            addThreadExceptionHandler(this);
        }
        ~CThreadExceptionCatcher()
        {
            removeThreadExceptionHandler(this);
        }
        virtual bool fireException(IException *e)
        {
            mptag_t mptag;
            {
                CriticalBlock b(jobListener.crit);
                if (0 == jobListener.jobs.count())
                {
                    EXCLOG(e, "No job active exception: ");
                    return true;
                }
                IThorException *te = QUERYINTERFACE(e, IThorException);
                CJobSlave *job = NULL;
                if (te && te->queryJobId())
                    job = jobListener.jobs.find(te->queryJobId());
                if (!job)
                {
                    // JCSMORE - exception fallen through to thread exception handler, from unknown job, fire up to 1st job for now.
                    job = (CJobSlave *)jobListener.jobs.next(NULL);
                }
                mptag = job->querySlaveMpTag();
            }
            CMessageBuffer msg;
            msg.append(smt_errorMsg);
            serializeThorException(e, msg);

            try
            {
                if (!queryClusterComm().sendRecv(msg, 0, mptag, LONGTIMEOUT))
                    EXCLOG(e, "Failed to send exception to master");
            }
            catch (IException *e2)
            {
                StringBuffer str("Error whilst sending exception '");
                e->errorMessage(str);
                str.append("' to master");
                EXCLOG(e2, str.str());
                e2->Release();
            }
            return true;
        }
    } excptHandler;
public:
    CJobListener() : excptHandler(*this)
    {
        stopped = true;
    }
    ~CJobListener()
    {
        stop();
    }
    void stop()
    {
        queryClusterComm().cancel(0, masterSlaveMpTag);
    }
    virtual void main()
    {
        StringBuffer soPath;
        globals->getProp("@query_so_dir", soPath);
        StringBuffer soPattern("*.");
#ifdef _WIN32
        soPattern.append("dll");
#else
        soPattern.append("so");
#endif
        if (globals->getPropBool("Debug/@dllsToSlaves",true))
            querySoCache.init(soPath.str(), DEFAULT_QUERYSO_LIMIT, soPattern);
        Owned<ISlaveWatchdog> watchdog;
        if (globals->getPropBool("@watchdogEnabled"))
            watchdog.setown(createProgressHandler());

        CMessageBuffer msg;
        stopped = false;
        bool doReply;
        rank_t sendert;
        while (!stopped && queryClusterComm().recv(msg, 0, masterSlaveMpTag, &sendert))
        {
            doReply = true;
            msgids cmd;
            try
            {
                msg.read((unsigned &)cmd);
                switch (cmd)
                {
                    case QueryInit:
                    {
                        mptag_t mptag, slaveMsgTag;
                        deserializeMPtag(msg, mptag);
                        queryClusterComm().flush(mptag);
                        deserializeMPtag(msg, slaveMsgTag);
                        queryClusterComm().flush(slaveMsgTag);
                        StringBuffer soPath;
                        StringAttr wuid, graphName, remoteSoPath;
                        msg.read(wuid);
                        msg.read(graphName);
                        msg.read(remoteSoPath);
                        bool sendSo;
                        msg.read(sendSo);
                        const SocketEndpoint &masterEp = queryClusterGroup().queryNode(0).endpoint();
                        if (sendSo)
                        {
                            size32_t size;
                            msg.read(size);
                            globals->getProp("@query_so_dir", soPath);
                            if (soPath.length())
                                addPathSepChar(soPath);
                            RemoteFilename rfn;
                            rfn.setPath(masterEp, remoteSoPath);
                            rfn.getTail(soPath);
                            Owned<IFile> iFile = createIFile(soPath.str());
                            try
                            {
                                const void *soPtr = msg.readDirect(size);
#ifdef _DEBUG
                                if (!iFile->exists())
#else
                                if (1)
#endif
                                {
                                    iFile->setCreateFlags(S_IRWXU);
                                    Owned<IFileIO> iFileIO = iFile->open(IFOwrite);
                                    iFileIO->write(0, size, soPtr);
                                }
                            }
                            catch (IException *e)
                            {
                                StringBuffer msg("Failed to save dll, cwd = ");
                                char buf[255];
                                getcwd(buf, sizeof(buf));
                                msg.append(buf).append(", path = ").append(soPath.str());
                                EXCLOG(e, msg.str());
                                e->Release();
                            }
                            assertex(globals->getPropBool("Debug/@dllsToSlaves", true));
                            querySoCache.add(soPath.str());
                        }
                        else if (globals->getPropBool("Debug/@dllsToSlaves", true))
                        {
                            // i.e. should have previously been sent.
                            globals->getProp("@query_so_dir", soPath);
                            if (soPath.length())
                                addPathSepChar(soPath);
                            RemoteFilename rfn;
                            rfn.setPath(masterEp, remoteSoPath);
                            rfn.getTail(soPath);
                            OwnedIFile iFile = createIFile(soPath.str());
                            if (!iFile->exists())
                            {
                                WARNLOG("Slave cached query dll missing: %s, will attempt to fetch from master", soPath.str());
                                StringBuffer rpath;
                                rfn.getRemotePath(rpath);
                                if (rfn.isLocal())
                                    rfn.getLocalPath(rpath.clear());
                                copyFile(soPath.str(), rpath.str());
                            }
                            querySoCache.add(soPath.str());
                        }
                        else
                            soPath.append(remoteSoPath);
                        PROGLOG("Using query: %s", soPath.str());
                        Owned<IPropertyTree> workUnitInfo = createPTree(msg);
                        Owned<CJobSlave> job = new CJobSlave(watchdog, workUnitInfo, graphName, soPath.str(), mptag, slaveMsgTag);
                        jobs.replace(*LINK(job));

                        Owned<IPropertyTree> deps = createPTree(msg);
                        job->setXGMML(deps);

                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case QueryDone:
                    {
                        StringAttr key;
                        msg.read(key);
                        PROGLOG("QueryDone, removing %s from jobs", key.get());
                        CJobSlave *job = jobs.find(key.get());
                        jobs.removeExact(job);
                        PROGLOG("QueryDone, removed %s from jobs", key.get());

                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case GraphInit:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (!job)
                            throw MakeStringException(0, "Job not found: %s", jobKey.get());
                        Owned<IPropertyTree> graphNode = createPTree(msg);
                        Owned<CSlaveGraph> subGraph = (CSlaveGraph *)job->createGraph();
                        subGraph->createFromXGMML(graphNode, NULL, NULL, NULL);
                        PROGLOG("GraphInit: %s, graphId=%"GIDPF"d", jobKey.get(), subGraph->queryGraphId());
                        subGraph->setExecuteReplyTag(subGraph->queryJob().deserializeMPTag(msg));
                        unsigned len;
                        msg.read(len);
                        MemoryBuffer initData;
                        initData.append(len, msg.readDirect(len));
                        subGraph->deserializeCreateContexts(initData);
                        graph_id gid;
                        msg.read(gid);
                        assertex(gid == subGraph->queryGraphId());
                        subGraph->init(msg);

                        job->addSubGraph(*LINK(subGraph));
                        job->addDependencies(job->queryXGMML(), false);

                        subGraph->execute(0, NULL, true, true);

                        msg.clear();
                        msg.append(false);

                        break;
                    }
                    case GraphEnd:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            graph_id gid;
                            msg.read(gid);
                            msg.clear();
                            msg.append(false);
                            Owned<CSlaveGraph> graph = (CSlaveGraph *)job->getGraph(gid);
                            if (graph)
                            {
                                graph->getDone(msg);
                                graph->join(); // graph will wind-up.
                            }
                            else
                            {
                                msg.clear();
                                msg.append(false);
                            }
                        }
                        else
                        {
                            msg.clear();
                            msg.append(false);
                        }
                        break;
                    }
                    case GraphAbort:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        PROGLOG("GraphAbort: %s", jobKey.get());
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            graph_id gid;
                            msg.read(gid);
                            Owned<CGraphBase> graph = job->getGraph(gid);
                            if (graph)
                            {
                                Owned<IThorException> e = MakeThorException(0, "GraphAbort");
                                e->setGraphId(gid);
                                graph->abort(e);
                            }
                        }
                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case Shutdown:
                    {
                        doReply = false;
                        stopped = true;
                        break;
                    }
                    default:
                        throwUnexpected();
                }
            }
            catch (IException *e)
            {
                EXCLOG(e, NULL);
                if (doReply && TAG_NULL != msg.getReplyTag())
                {
                    doReply = false;
                    msg.clear();
                    msg.append(true);
                    serializeThorException(e, msg);
                    queryClusterComm().reply(msg);
                }
                e->Release();
            }
            if (doReply && msg.getReplyTag()!=TAG_NULL)
                queryClusterComm().reply(msg);
        }
    }

friend class CThreadExceptionCatcher;
};

//////////////////////////


class CStringAttr : public StringAttr, public CSimpleInterface
{
public:
    CStringAttr(const char *str) : StringAttr(str) { }
    const char *queryFindString() const { return get(); }
};
class CFileInProgressHandler : public CSimpleInterface, implements IFileInProgressHandler
{
    CriticalSection crit;
    StringSuperHashTableOf<CStringAttr> lookup;
    QueueOf<CStringAttr, false> fipList;
    OwnedIFileIO iFileIO;
    static const char *formatV;

    void write()
    {
        if (0 == fipList.ordinality())
            iFileIO->setSize(0);
        else
        {
            Owned<IFileIOStream> stream = createBufferedIOStream(iFileIO);
            stream->write(3, formatV); // 3 byte format definition, incase of change later
            ForEachItemIn(i, fipList)
            {
                writeStringToStream(*stream, fipList.item(i)->get());
                writeCharToStream(*stream, '\n');
            }
            offset_t pos = stream->tell();
            stream.clear();
            iFileIO->setSize(pos);
        }
    }
    void doDelete(const char *fip)
    {
        OwnedIFile iFile = createIFile(fip);
        try
        {
            iFile->remove();
        }
        catch (IException *e)
        {
            StringBuffer errStr("FileInProgressHandler, failed to remove: ");
            EXCLOG(e, errStr.append(fip).str());
            e->Release();
        }
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFileInProgressHandler()
    {
        init();
    }
    ~CFileInProgressHandler()
    {
        deinit();
    }
    void deinit()
    {
        loop
        {
            CStringAttr *item = fipList.dequeue();
            if (!item) break;
            doDelete(item->get());
            item->Release();
        }
        lookup.kill();
    }
    void init()
    {
        StringBuffer path;
        globals->getProp("@thorPath", path);
        addPathSepChar(path);
        path.append("fiplist_");
        globals->getProp("@name", path);
        path.append("_");
        path.append(queryClusterGroup().rank(queryMyNode()));
        path.append(".lst");
        ensureDirectoryForFile(path.str());
        Owned<IFile> iFile = createIFile(path.str());
        iFileIO.setown(iFile->open(IFOreadwrite));
        if (!iFileIO)
        {
            PROGLOG("Failed to open/create backup file: %s", path.str());
            return;
        }
        MemoryBuffer mb;
        read(iFileIO, 0, (size32_t)iFileIO->size(), mb);
        const char *mem = mb.toByteArray();
        if (mem)
        {
            const char *endMem = mem+mb.length();
            mem += 3; // formatV header
            do
            {
                const char *eol = strchr(mem, '\n');
                StringAttr fip(mem, eol-mem);
                doDelete(fip);
                mem = eol+1;
            }
            while (mem != endMem);
        }
        write();
    }
    
// IFileInProgressHandler
    virtual void add(const char *fip)
    {
        CriticalBlock b(crit);
        CStringAttr *item = lookup.find(fip);
        assertex(!item);
        item = new CStringAttr(fip);
        fipList.enqueue(item);
        lookup.add(* item);
        write();
    }
    virtual void remove(const char *fip)
    {
        CriticalBlock b(crit);
        CStringAttr *item = lookup.find(fip);
        if (item)
        {
            lookup.removeExact(item);
            unsigned pos = fipList.find(item);
            fipList.dequeue(item);
            item->Release();
            write();
        }
    }
};
const char *CFileInProgressHandler::formatV = "01\n";


class CThorResourceSlave : public CThorResourceBase
{
    Owned<IThorFileCache> fileCache;
    Owned<IBackup> backupHandler;
    Owned<IFileInProgressHandler> fipHandler;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorResourceSlave()
    {
        backupHandler.setown(createBackupHandler(queryBaseDirectory()));
        fileCache.setown(createFileCache(globals->getPropInt("@fileCacheLimit", 1800)));
        fipHandler.setown(new CFileInProgressHandler());
    }
    ~CThorResourceSlave()
    {
        fileCache.clear();
        backupHandler.clear();
        fipHandler.clear();
    }

// IThorResource
    virtual IThorFileCache &queryFileCache() { return *fileCache.get(); }
    virtual IBackup &queryBackup() { return *backupHandler.get(); }
    virtual IFileInProgressHandler &queryFileInProgressHandler() { return *fipHandler.get(); }
};

void slaveMain()
{
    CJobListener jobListener;
    CThorResourceSlave slaveResource;
    setIThorResource(slaveResource);

#ifdef __linux__
    bool useMirrorMount = globals->getPropBool("Debug/@useMirrorMount", false);
    if (useMirrorMount && queryClusterGroup().ordinality() > 2)
    {
        unsigned slaves = queryClusterGroup().ordinality()-1;
        rank_t next = queryClusterGroup().rank()%slaves;  // note 0 = master
        const IpAddress &ip = queryClusterGroup().queryNode(next+1).endpoint();
        StringBuffer ipStr;
        ip.getIpText(ipStr);
        PROGLOG("Redirecting local mount to %s", ipStr.str());
        const char * overrideReplicateDirectory = globals->queryProp("@thorReplicateDirectory");
        StringBuffer repdir;
        if (getConfigurationDirectory(globals->queryPropTree("Directories"),"mirror","thor",globals->queryProp("@name"),repdir))
            overrideReplicateDirectory = repdir.str();
        else
            overrideReplicateDirectory = "/d$";
        setLocalMountRedirect(ip, overrideReplicateDirectory, "/mnt/mirror");
    }
#endif

    jobListener.main();
}

void abortSlave()
{
    if (clusterInitialized())
        queryClusterComm().cancel(0, masterSlaveMpTag);
}

