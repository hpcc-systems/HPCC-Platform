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

// todo look at IRemoteFileServer stop

#include <vector>

#include "platform.h"
#include "limits.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"
#include "jsecrets.hpp"

#include "securesocket.hpp"
#include "portlist.h"
#include "jsocket.hpp"
#include "jencrypt.hpp"
#include "jlzw.hpp"
#include "jset.hpp"
#include "jhtree.hpp"

#include "dadfs.hpp"

#include "remoteerr.hpp"
#include <atomic>
#include <string>
#include <unordered_map>

#include "rtldynfield.hpp"
#include "rtlds_imp.hpp"
#include "rtlread_imp.hpp"
#include "rtlrecord.hpp"
#include "eclhelper_dyn.hpp"

#include "rtlcommon.hpp"
#include "rtlformat.hpp"

#include "jflz.hpp"
#include "digisign.hpp"

#include "dafdesc.hpp"

#include "thorcommon.hpp"
#include "csvsplitter.hpp"
#include "thorxmlread.hpp"

#include "dafscommon.hpp"
#include "rmtfile.hpp"
#include "rmtclient_impl.hpp"
#include "dafsserver.hpp"

#include "ftslavelib.hpp"
#include "filecopy.hpp"


using namespace cryptohelper;


#define SOCKET_CACHE_MAX 500


#define TREECOPYTIMEOUT   (60*60*1000)     // 1Hr (I guess could take longer for big file but at least will stagger)
#define TREECOPYPOLLTIME  (60*1000*5)      // for tracing that delayed
#define TREECOPYPRUNETIME (24*60*60*1000)  // 1 day

static const unsigned __int64 defaultFileStreamChooseNLimit = I64C(0x7fffffffffffffff); // constant should be move to common place (see eclhelper.hpp)
static const unsigned __int64 defaultFileStreamSkipN = 0;
static const unsigned defaultDaFSReplyLimitKB = 1024; // 1MB
enum OutputFormat:byte { outFmt_Binary, outFmt_Xml, outFmt_Json };


///////////////////////////


static unsigned maxConnectTime = 0;
static unsigned maxReceiveTime = 0;

#ifndef _CONTAINERIZED
//Security and default port attributes
static class _securitySettingsServer
{
public:
    DAFSConnectCfg  connectMethod;
    unsigned short  daFileSrvPort;
    unsigned short  daFileSrvSSLPort;
    const char *    certificate;
    const char *    privateKey;
    const char *    passPhrase;

    void init()
    {
        queryDafsSecSettings(&connectMethod, &daFileSrvPort, &daFileSrvSSLPort, &certificate, &privateKey, &passPhrase);
    }

    const IPropertyTree * getSecureConfig()
    {
        //Later: return a synced tree...
        return createSecureSocketConfig(certificate, privateKey, passPhrase, false);
    }

} securitySettings;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    // initialize after other dependent (in jlib) objects are initialized.
    securitySettings.init();
    return true;
}

#endif

static CriticalSection              secureContextCrit;
static Owned<ISecureSocketContext>  secureContextServer;

#ifdef _USE_OPENSSL
static ISecureSocket *createSecureSocket(ISocket *sock, bool disableClientCertVerification)
{
    {
        CriticalBlock b(secureContextCrit);
        if (!secureContextServer)
        {
#ifdef _CONTAINERIZED
            /* Connections are expected from 3rd parties via TLS,
            * we do not expect them to provide a valid certificate for verification.
            * Currently the server (this dafilesrv), will use either the "public" certificate issuer,
            * unless it's visibility is "cluster" (meaning internal only)
            */

            const char *certScope = strsame("cluster", getComponentConfigSP()->queryProp("service/@visibility")) ? "local" : "public";
            Owned<const ISyncedPropertyTree> info = getIssuerTlsSyncedConfig(certScope, nullptr, disableClientCertVerification);
            if (!info || !info->isValid())
                throw makeStringException(-1, "createSecureSocket() : missing MTLS configuration");
            secureContextServer.setown(createSecureSocketContextSynced(info, ServerSocket));
#else
            Owned<IPropertyTree> cert = getComponentConfigSP()->getPropTree("cert");
            if (cert)
            {
                Owned<ISyncedPropertyTree> certSyncedWrapper = createSyncedPropertyTree(cert);
                secureContextServer.setown(createSecureSocketContextSynced(certSyncedWrapper, ServerSocket));
            }
            else
                secureContextServer.setown(createSecureSocketContextEx2(securitySettings.getSecureConfig(), ServerSocket));
#endif
        }
    }
    int loglevel = SSLogNormal;
#ifdef _DEBUG
    loglevel = SSLogMax;
#endif
    return secureContextServer->createSecureSocket(sock, loglevel);
}
#else
static ISecureSocket *createSecureSocket(ISocket *sock, bool disableClientCertVerification)
{
    throwUnexpected();
}
#endif

static void reportFailedSecureAccepts(const char *context, IException *exception, unsigned &numFailedConn, unsigned &timeLastLog)
{
    numFailedConn++;
    unsigned timeNow = msTick();
    if ((timeNow - timeLastLog) >= 60000)
    {
        StringBuffer msg("CRemoteFileServer ");
        if (context)
            msg.append("(").append(context).append(") ");
        msg.appendf("[failure count : %u]", numFailedConn);
        EXCLOG(exception, msg.str());
        timeLastLog = timeNow;
    }
}

struct sRFTM
{
    CTimeMon *timemon;
    sRFTM(unsigned limit) {  timemon = limit ? new CTimeMon(limit) : NULL; }
    ~sRFTM() { delete timemon; }
};


const char *remoteServerVersionString() { return DAFILESRV_VERSIONSTRING; }


#define CLIENT_TIMEOUT      (1000*60*60*12)     // long timeout in case zombies
#define CLIENT_INACTIVEWARNING_TIMEOUT (1000*60*60*12) // time between logging inactive clients
#define SERVER_TIMEOUT      (1000*60*5)         // timeout when waiting for dafilesrv to reply after command
                                                // (increased when waiting for large block)
#define RFCText(cmd) #cmd

const char *RFCStrings[] =
{
    RFCText(RFCopenIO),
    RFCText(RFCcloseIO),
    RFCText(RFCread),
    RFCText(RFCwrite),
    RFCText(RFCsize),
    RFCText(RFCexists),
    RFCText(RFCremove),
    RFCText(RFCrename),
    RFCText(RFCgetver),
    RFCText(RFCisfile),
    RFCText(RFCisdirectory),
    RFCText(RFCisreadonly),
    RFCText(RFCsetreadonly),
    RFCText(RFCgettime),
    RFCText(RFCsettime),
    RFCText(RFCcreatedir),
    RFCText(RFCgetdir),
    RFCText(RFCstop),
    RFCText(RFCexec),
    RFCText(RFCdummy1),
    RFCText(RFCredeploy),
    RFCText(RFCgetcrc),
    RFCText(RFCmove),
    RFCText(RFCsetsize),
    RFCText(RFCextractblobelements),
    RFCText(RFCcopy),
    RFCText(RFCappend),
    RFCText(RFCmonitordir),
    RFCText(RFCsettrace),
    RFCText(RFCgetinfo),
    RFCText(RFCfirewall),
    RFCText(RFCunlock),
    RFCText(RFCunlockreply),
    RFCText(RFCinvalid),
    RFCText(RFCcopysection),
    RFCText(RFCtreecopy),
    RFCText(RFCtreecopytmp),
    RFCText(RFCsetthrottle), // legacy version
    RFCText(RFCsetthrottle2),
    RFCText(RFCsetfileperms),
    RFCText(RFCreadfilteredindex),
    RFCText(RFCreadfilteredcount),
    RFCText(RFCreadfilteredblob),
    RFCText(RFCStreamRead),
    RFCText(RFCStreamReadTestSocket),
    RFCText(RFCStreamGeneral),
    RFCText(RFCStreamReadJSON),
    RFCText(RFCFtSlaveCmd),
    RFCText(RFCmaxnormal),
};

static const char *getRFCText(RemoteFileCommandType cmd)
{
    if (cmd==RFCStreamReadJSON)
        return "RFCStreamReadJSON";
    else
    {
        unsigned elems = sizeof(RFCStrings) / sizeof(RFCStrings[0]);
        if (cmd >= elems)
            return "RFCunknown";
        return RFCStrings[cmd];
    }
}

static const char *getRFSERRText(unsigned err)
{
    switch (err)
    {
        case RFSERR_InvalidCommand:
            return "RFSERR_InvalidCommand";
        case RFSERR_NullFileIOHandle:
            return "RFSERR_NullFileIOHandle";
        case RFSERR_InvalidFileIOHandle:
            return "RFSERR_InvalidFileIOHandle";
        case RFSERR_TimeoutFileIOHandle:
            return "RFSERR_TimeoutFileIOHandle";
        case RFSERR_OpenFailed:
            return "RFSERR_OpenFailed";
        case RFSERR_ReadFailed:
            return "RFSERR_ReadFailed";
        case RFSERR_WriteFailed:
            return "RFSERR_WriteFailed";
        case RFSERR_RenameFailed:
            return "RFSERR_RenameFailed";
        case RFSERR_ExistsFailed:
            return "RFSERR_ExistsFailed";
        case RFSERR_RemoveFailed:
            return "RFSERR_RemoveFailed";
        case RFSERR_CloseFailed:
            return "RFSERR_CloseFailed";
        case RFSERR_IsFileFailed:
            return "RFSERR_IsFileFailed";
        case RFSERR_IsDirectoryFailed:
            return "RFSERR_IsDirectoryFailed";
        case RFSERR_IsReadOnlyFailed:
            return "RFSERR_IsReadOnlyFailed";
        case RFSERR_SetReadOnlyFailed:
            return "RFSERR_SetReadOnlyFailed";
        case RFSERR_GetTimeFailed:
            return "RFSERR_GetTimeFailed";
        case RFSERR_SetTimeFailed:
            return "RFSERR_SetTimeFailed";
        case RFSERR_CreateDirFailed:
            return "RFSERR_CreateDirFailed";
        case RFSERR_GetDirFailed:
            return "RFSERR_GetDirFailed";
        case RFSERR_GetCrcFailed:
            return "RFSERR_GetCrcFailed";
        case RFSERR_MoveFailed:
            return "RFSERR_MoveFailed";
        case RFSERR_ExtractBlobElementsFailed:
            return "RFSERR_ExtractBlobElementsFailed";
        case RFSERR_CopyFailed:
            return "RFSERR_CopyFailed";
        case RFSERR_AppendFailed:
            return "RFSERR_AppendFailed";
        case RFSERR_AuthenticateFailed:
            return "RFSERR_AuthenticateFailed";
        case RFSERR_CopySectionFailed:
            return "RFSERR_CopySectionFailed";
        case RFSERR_TreeCopyFailed:
            return "RFSERR_TreeCopyFailed";
        case RAERR_InvalidUsernamePassword:
            return "RAERR_InvalidUsernamePassword";
        case RFSERR_MasterSeemsToHaveDied:
            return "RFSERR_MasterSeemsToHaveDied";
        case RFSERR_TimeoutWaitSlave:
            return "RFSERR_TimeoutWaitSlave";
        case RFSERR_TimeoutWaitConnect:
            return "RFSERR_TimeoutWaitConnect";
        case RFSERR_TimeoutWaitMaster:
            return "RFSERR_TimeoutWaitMaster";
        case RFSERR_NoConnectSlave:
            return "RFSERR_NoConnectSlave";
        case RFSERR_NoConnectSlaveXY:
            return "RFSERR_NoConnectSlaveXY";
        case RFSERR_VersionMismatch:
            return "RFSERR_VersionMismatch";
        case RFSERR_SetThrottleFailed:
            return "RFSERR_SetThrottleFailed";
        case RFSERR_MaxQueueRequests:
            return "RFSERR_MaxQueueRequests";
        case RFSERR_KeyIndexFailed:
            return "RFSERR_MaxQueueRequests";
        case RFSERR_StreamReadFailed:
            return "RFSERR_StreamReadFailed";
        case RFSERR_InternalError:
            return "Internal Error";
    }
    return "RFSERR_Unknown";
}

#define ThrottleText(throttleClass) #throttleClass
const char *ThrottleStrings[] =
{
    ThrottleText(ThrottleStd),
    ThrottleText(ThrottleSlow),
};

// very high upper limits that configure can't exceed
#define THROTTLE_MAX_LIMIT 1000000
#define THROTTLE_MAX_DELAYMS 3600000
#define THROTTLE_MAX_CPUTHRESHOLD 100
#define THROTTLE_MAX_QUEUELIMIT 10000000

static const char *getThrottleClassText(ThrottleClass throttleClass) { return ThrottleStrings[throttleClass]; }

//---------------------------------------------------------------------------


// TreeCopy

#define TREECOPY_CACHE_SIZE 50

struct CTreeCopyItem: public CInterface
{
    StringAttr net;
    StringAttr mask;
    offset_t sz;                // original size
    CDateTime dt;               // original date
    RemoteFilenameArray loc;    // locations for file - 0 is original
    Owned<IBitSet> busy;
    unsigned lastused;

    CTreeCopyItem(RemoteFilename &orig, const char *_net, const char *_mask, offset_t _sz, CDateTime &_dt)
        : net(_net), mask(_mask)
    {
        loc.append(orig);
        dt.set(_dt);
        sz = _sz;
        busy.setown(createThreadSafeBitSet());
        lastused = msTick();
    }
    bool equals(const RemoteFilename &orig, const char *_net, const char *_mask, offset_t _sz, CDateTime &_dt)
    {
        if (!orig.equals(loc.item(0)))
            return false;
        if (strcmp(_net,net)!=0)
            return false;
        if (strcmp(_mask,mask)!=0)
            return false;
        if (sz!=_sz)
            return false;
        return (dt.equals(_dt,false));
    }
};

static CIArrayOf<CTreeCopyItem>  treeCopyArray;
static CriticalSection           treeCopyCrit;
static unsigned                  treeCopyWaiting=0;
static Semaphore                 treeCopySem;

/////////////////////////


//====================================================================================================


class CAsyncCommandManager
{
    class CAsyncJob: public CInterface
    {
        class cThread: public Thread
        {
            CAsyncJob *parent;
        public:
            cThread(CAsyncJob *_parent)
                : Thread("CAsyncJob")
            {
                parent = _parent;
            }
            int run()
            {
                int ret = -1;
                try {
                    ret = parent->run();
                    parent->setDone();
                }
                catch (IException *e)
                {
                    parent->setException(e);
                }
                parent->signal();
                return ret;
            }
        } *thread;
        StringAttr uuid;
        CAsyncCommandManager &parent;
    public:
        CAsyncJob(CAsyncCommandManager &_parent, const char *_uuid)
            : uuid(_uuid), parent(_parent)
        {
            thread = new cThread(this);
            hash = hashc((const byte *)uuid.get(),uuid.length(),~0U);
        }
        ~CAsyncJob()
        {
            thread->join();
            thread->Release();
        }
        static void destroy(CAsyncJob *j)
        {
            j->Release();
        }
        void signal()
        {
            parent.signal();
        }
        void start()
        {
            parent.wait();
            //These are async jobs - so do not preserve the active thread context because it will go out of scope
            thread->start(false);
        }
        void join()
        {
            thread->join();
        }
        static unsigned getHash(const char *key)
        {
            return hashcz((const byte *)key,~0U);
        }
        static CAsyncJob* create(const char *key) { assertex(!"CAsyncJob::create not implemented"); return NULL; }
        unsigned hash;
        bool eq(const char *key)
        {
            return stricmp(key,uuid.get())==0;
        }
        virtual int run()=0;
        virtual void setException(IException *e)=0;
        virtual void setDone()=0;
    };

    class CAsyncCopySection: public CAsyncJob
    {
        Owned<IFile> src;
        RemoteFilename dst;
        offset_t toOfs;
        offset_t fromOfs;
        offset_t size;
        CFPmode mode; // not yet supported
        CriticalSection sect;
        offset_t done;
        offset_t total;
        Semaphore finished;
        AsyncCommandStatus status;
        Owned<IException> exc;
    public:
        CAsyncCopySection(CAsyncCommandManager &parent, const char *_uuid, const char *fromFile, const char *toFile, offset_t _toOfs, offset_t _fromOfs, offset_t _size)
            : CAsyncJob(parent, _uuid)
        {
            status = ACScontinue;
            src.setown(createIFile(fromFile));
            dst.setRemotePath(toFile);
            toOfs = _toOfs;
            fromOfs = _fromOfs;
            size = _size;
            mode = CFPcontinue;
            done = 0;
            total = (offset_t)-1;
        }
        AsyncCommandStatus poll(offset_t &_done, offset_t &_total,unsigned timeout)
        {
            if (timeout&&finished.wait(timeout))
                finished.signal();      // may need to call again
            CriticalBlock block(sect);
            if (exc)
                throw exc.getClear();
            _done = done;
            _total = total;
            return status;
        }
        int run()
        {
            class cProgress: implements ICopyFileProgress
            {
                CriticalSection &sect;
                CFPmode &mode;
                offset_t &done;
                offset_t &total;
            public:
                cProgress(CriticalSection &_sect,offset_t &_done,offset_t &_total,CFPmode &_mode)
                    : sect(_sect), mode(_mode), done(_done), total(_total)
                {
                }
                CFPmode onProgress(offset_t sizeDone, offset_t totalSize)
                {
                    CriticalBlock block(sect);
                    done = sizeDone;
                    total = totalSize;
                    return mode;
                }
            } progress(sect,total,done,mode);
            src->copySection(dst,toOfs, fromOfs, size, &progress);  // exceptions will be handled by base class
            return 0;
        }
        void setException(IException *e)
        {
            EXCLOG(e,"CAsyncCommandManager::CAsyncJob");
            CriticalBlock block(sect);
            if (exc.get())
                e->Release();
            else
                exc.setown(e);
            status = ACSerror;
        }
        void setDone()
        {
            CriticalBlock block(sect);
            finished.signal();
            status = ACSdone;
        }
    };

    CMinHashTable<CAsyncJob> jobtable;
    CriticalSection sect;
    Semaphore threadsem;
    unsigned limit;

public:
    CAsyncCommandManager(unsigned _limit) : limit(_limit)
    {
        if (limit) // 0 == unbound
            threadsem.signal(limit); // max number of async jobs
    }
    void join()
    {
        CriticalBlock block(sect);
        unsigned i;
        CAsyncJob *j=jobtable.first(i);
        while (j) {
            j->join();
            j=jobtable.next(i);
        }
    }

    void signal()
    {
        if (limit)
            threadsem.signal();
    }

    void wait()
    {
        if (limit)
            threadsem.wait();
    }

    AsyncCommandStatus copySection(const char *uuid, const char *fromFile, const char *toFile, offset_t toOfs, offset_t fromOfs, offset_t size, offset_t &done, offset_t &total, unsigned timeout)
    {
        // return 0 if continuing, 1 if done
        CAsyncCopySection * job;
        Linked<CAsyncJob> cjob;
        {
            CriticalBlock block(sect);
            cjob.set(jobtable.find(uuid,false));
            if (cjob) {
                job = QUERYINTERFACE(cjob.get(),CAsyncCopySection);
                if (!job) {
                    throw MakeStringException(-1,"Async job ID mismatch");
                }
            }
            else {
                job = new CAsyncCopySection(*this, uuid, fromFile, toFile, toOfs, fromOfs, size);
                cjob.setown(job);
                jobtable.add(cjob.getLink());
                cjob->start();
            }
        }
        AsyncCommandStatus ret = ACSerror;
        Owned<IException> rete;
        try {
            ret = job->poll(done,total,timeout);
        }
        catch (IException * e) {
            rete.setown(e);
        }
        if ((ret!=ACScontinue)||rete.get()) {
            job->join();
            CriticalBlock block(sect);
            jobtable.remove(job);
            if (rete.get())
                throw rete.getClear();
        }
        return ret;
    }
};


//====================================================================================================



inline void appendErr(MemoryBuffer &reply, unsigned e)
{
    reply.append(e).append(getRFSERRText(e));
}

#define MAPCOMMAND(c,p) case c: { this->p(msg, reply) ; break; }
#define MAPCOMMANDCLIENT(c,p,client) case c: { this->p(msg, reply, client); break; }
#define MAPCOMMANDCLIENTTHROTTLE(c,p,client,throttler) case c: { this->p(msg, reply, client, throttler); break; }
#define MAPCOMMANDSTATS(c,p,stats) case c: { this->p(msg, reply, stats); break; }
#define MAPCOMMANDCLIENTSTATS(c,p,client,stats) case c: { this->p(msg, reply, client, stats); break; }

static unsigned ClientCount = 0;
static unsigned MaxClientCount = 0;
static CriticalSection ClientCountSect;

#define DEFAULT_THROTTLOG_LOG_INTERVAL_SECS 60 // log total throttled delay period


class CClientStats : public CInterface
{
public:
    CClientStats(const char *_client) : client(_client), count(0), bRead(0), bWritten(0) { }
    const char *queryFindString() const { return client; }
    inline void addRead(unsigned __int64 len)
    {
        bRead += len;
    }
    inline void addWrite(unsigned __int64 len)
    {
        bWritten += len;
    }
    void getStatus(StringBuffer & info) const
    {
        info.appendf("Client %s - %" I64F "d requests handled, bytes read = %" I64F "d, bytes written = % " I64F "d",
            client.get(), count, bRead.load(), bWritten.load()).newline();
    }

    StringAttr client;
    unsigned __int64 count;
    std::atomic<unsigned __int64> bRead;
    std::atomic<unsigned __int64> bWritten;
};

class CClientStatsTable : public OwningStringSuperHashTableOf<CClientStats>
{
    typedef OwningStringSuperHashTableOf<CClientStats> PARENT;
    CriticalSection crit;
    unsigned cmdStats[RFCmax];

    static int compareElement(void* const *ll, void* const *rr)
    {
        const CClientStats *l = (const CClientStats *) *ll;
        const CClientStats *r = (const CClientStats *) *rr;
        if (l->count == r->count)
            return 0;
        else if (l->count<r->count)
            return 1;
        else
            return -1;
    }
public:
    CClientStatsTable()
    {
        memset(&cmdStats[0], 0, sizeof(cmdStats));
    }
    ~CClientStatsTable()
    {
        _releaseAll();
    }
    CClientStats *getClientReference(RemoteFileCommandType cmd, const char *client)
    {
        CriticalBlock b(crit);
        CClientStats *stats = PARENT::find(client);
        if (!stats)
        {
            stats = new CClientStats(client);
            PARENT::replace(*stats);
        }
        if (cmd<RFCmax) // i.e. ignore duff command (which will be traced), but still record client connected
            cmdStats[cmd]++;
        ++stats->count;
        return LINK(stats);
    }
    StringBuffer &getInfo(StringBuffer &info, unsigned level=1)
    {
        CriticalBlock b(crit);
        unsigned __int64 totalCmds = 0;
        for (unsigned c=0; c<RFCmax; c++)
            totalCmds += cmdStats[c];
        unsigned totalClients = PARENT::ordinality();
        info.appendf("Commands processed = %" I64F "u, unique clients = %u", totalCmds, totalClients);
        if (totalCmds)
        {
            info.append("Command stats:").newline();
            for (unsigned c=0; c<RFCmax; c++)
            {
                unsigned __int64 count = cmdStats[c];
                if (count)
                    info.append(getRFCText(c)).append(": ").append(count).newline();
            }
        }
        if (totalClients)
        {
            SuperHashIteratorOf<CClientStats> iter(*this);
            PointerArrayOf<CClientStats> elements;
            ForEach(iter)
            {
                CClientStats &elem = iter.query();
                elements.append(&elem);
            }
            elements.sort(&compareElement);
            if (level < 10)
            {
                // list up to 10 clients ordered by # of commands processed
                unsigned max=elements.ordinality();
                if (max>10)
                    max = 10; // cap
                info.append("Top 10 clients:").newline();
                for (unsigned e=0; e<max; e++)
                {
                    const CClientStats &element = *elements.item(e);
                    element.getStatus(info);
                }
            }
            else // list all
            {
                info.append("All clients:").newline();
                ForEachItemIn(e, elements)
                {
                    const CClientStats &element = *elements.item(e);
                    element.getStatus(info);
                }
            }
        }
        return info;
    }
    void reset()
    {
        CriticalBlock b(crit);
        memset(&cmdStats[0], 0, sizeof(cmdStats));
        kill();
    }
};

interface IRemoteReadActivity;
interface IRemoteWriteActivity;
interface IRemoteFetchActivity;
interface IRemoteActivity : extends IInterface
{
    virtual unsigned __int64 queryProcessed() const = 0;
    virtual IOutputMetaData *queryOutputMeta() const = 0;
    virtual StringBuffer &getInfoStr(StringBuffer &out) const = 0;
    virtual void serializeCursor(MemoryBuffer &tgt) const = 0;
    virtual void restoreCursor(MemoryBuffer &src) = 0;
    virtual bool isGrouped() const = 0;
    virtual void flushStatistics(CClientStats &stats) = 0;
    virtual IRemoteReadActivity *queryIsReadActivity() { return nullptr; }
    virtual IRemoteWriteActivity *queryIsWriteActivity() { return nullptr; }
    virtual IRemoteFetchActivity *queryIsFetchActivity() { return nullptr; }
};


interface IRemoteReadActivity : extends IRemoteActivity
{
    virtual const void *nextRow(MemoryBufferBuilder &outBuilder, size32_t &sz) = 0;
    virtual bool requiresPostProject() const = 0;
    virtual void seek(offset_t pos) = 0;
};

interface IRemoteFetchActivity : extends IRemoteReadActivity
{
    virtual void loadFetchBatch(IPropertyTree *fetch) = 0;
};

interface IRemoteWriteActivity : extends IRemoteActivity
{
    virtual void write(size32_t sz, const void *row) = 0;
};

class CRemoteRequest : public CSimpleInterfaceOf<IInterface>
{
    int cursorHandle;
    OutputFormat format;
    unsigned __int64 replyLimit = defaultDaFSReplyLimitKB * 1024;
    Linked<IRemoteActivity> activity;
    Linked<ICompressor> compressor;
    Linked<IExpander> expander;
    MemoryBuffer expandMb;
    Owned<IXmlWriterExt> responseWriter; // for xml or json response

    OwnedSpanLifetime requestSpan;
    std::string requestTraceParent;

    bool handleFull(MemoryBuffer &inMb, size32_t inPos, MemoryBuffer &compressMb, ICompressor *compressor, size32_t replyLimit, size32_t &totalSz)
    {
        size32_t sz = inMb.length()-inPos;
        if (sz < replyLimit)
            return false;

        if (!compressor)
            return true;

        // consumes data from inMb into compressor
        totalSz += sz;
        const void *data = inMb.bytes()+inPos;
        assertex(compressor->write(data, sz) == sz);
        inMb.setLength(inPos);
        return compressMb.capacity() > replyLimit;
    }
    void processRead(IPropertyTree *requestTree, MemoryBuffer &responseMb)
    {
        IRemoteReadActivity *readActivity = activity->queryIsReadActivity();
        assertex(readActivity);

        IRemoteFetchActivity *fetchActivity = activity->queryIsFetchActivity();
        if (fetchActivity)
        {
            IPropertyTree *fetchBranch = requestTree->queryPropTree("fetch");
            if (fetchBranch)
                fetchActivity->loadFetchBatch(fetchBranch);
        }

        MemoryBuffer compressMb;

        IOutputMetaData *outMeta = readActivity->queryOutputMeta();
        bool eoi=false;

        bool grouped = readActivity->isGrouped();
        MemoryBuffer resultBuffer;
        MemoryBufferBuilder outBuilder(resultBuffer, outMeta->getMinRecordSize());
        if (outFmt_Binary == format)
        {
            if (compressor)
            {
                compressMb.setEndian(__BIG_ENDIAN);
                compressMb.append(responseMb);
            }

            DelayedMarker<size32_t> dataLenMarker(compressor ? compressMb : responseMb); // uncompressed data size

            if (compressor)
            {
                size32_t initialSz = replyLimit >= 0x10000 ? 0x10000 : replyLimit;
                compressor->open(compressMb, initialSz);
            }

            outBuilder.setBuffer(responseMb); // write direct to responseMb buffer for efficiency
            unsigned __int64 numProcessedStart = readActivity->queryProcessed();
            size32_t totalDataSz = 0;
            size32_t dataStartPos = responseMb.length();

            if (grouped)
            {
                bool pastFirstRow = numProcessedStart>0;
                do
                {
                    size32_t eogPos = 0;
                    if (pastFirstRow)
                    {
                        /* this is for last row output, which might have been returned in the previous request
                         * The eog marker may change as a result of the next row (see writeDirect() call below);
                         */
                        eogPos = responseMb.length();
                        responseMb.append(false);
                    }
                    size32_t rowSz;
                    const void *row = readActivity->nextRow(outBuilder, rowSz);
                    if (!row)
                    {
                        if (!pastFirstRow)
                        {
                            eoi = true;
                            break;
                        }
                        else
                        {
                            bool eog = true;
                            responseMb.writeDirect(eogPos, sizeof(eog), &eog);
                            row = readActivity->nextRow(outBuilder, rowSz);
                            if (!row)
                            {
                                eoi = true;
                                break;
                            }
                        }
                    }
                    pastFirstRow = true;
                }
                while (!handleFull(responseMb, dataStartPos, compressMb, compressor, replyLimit, totalDataSz));
            }
            else
            {
                do
                {
                    size32_t rowSz;
                    const void *row = readActivity->nextRow(outBuilder, rowSz);
                    if (!row)
                    {
                        eoi = true;
                        break;
                    }
                }
                while (!handleFull(responseMb, dataStartPos, compressMb, compressor, replyLimit, totalDataSz));
            }

            // Consume any trailing data remaining
            if (compressor)
            {
                size32_t sz = responseMb.length()-dataStartPos;
                if (sz)
                {
                    // consumes data built up in responseMb buffer into compressor
                    totalDataSz += sz;
                    const void *data = responseMb.bytes()+dataStartPos;
                    assertex(compressor->write(data, sz) == sz);
                    responseMb.setLength(dataStartPos);
                }
            }

            // finalize responseMb
            dataLenMarker.write(compressor ? totalDataSz : responseMb.length()-dataStartPos);
            DelayedSizeMarker cursorLenMarker(responseMb); // cursor length
            if (!eoi || fetchActivity) // if fetching, then still want cursor for continuation
                readActivity->serializeCursor(responseMb);
            cursorLenMarker.write();
            if (compressor)
            {
                // consume cursor into compressor
                size32_t sz = responseMb.length()-dataStartPos;
                const void *data = responseMb.bytes()+dataStartPos;
                assertex(compressor->write(data, sz) == sz);
                compressor->close();
                // now ready to swap compressed output into responseMb
                responseMb.swapWith(compressMb);
            }
        }
        else
        {
            responseWriter->outputBeginArray("Row");
            if (grouped)
            {
                bool pastFirstRow = readActivity->queryProcessed()>0;
                bool first = true;
                do
                {
                    size32_t rowSz;
                    const void *row = readActivity->nextRow(outBuilder, rowSz);
                    if (!row)
                    {
                        if (!pastFirstRow)
                        {
                            eoi = true;
                            break;
                        }
                        else
                        {
                            row = readActivity->nextRow(outBuilder, rowSz);
                            if (!row)
                            {
                                eoi = true;
                                break;
                            }
                            if (first) // possible if eog was 1st row on next packet
                                responseWriter->outputBeginNested("Row", false);
                            responseWriter->outputBool(true, "dfs:Eog"); // field name cannot clash with an ecl field name
                        }
                    }
                    if (pastFirstRow)
                        responseWriter->outputEndNested("Row"); // close last row

                    responseWriter->outputBeginNested("Row", false);
                    outMeta->toXML((const byte *)row, *responseWriter);
                    resultBuffer.clear();
                    pastFirstRow = true;
                    first = false;
                }
                while (responseWriter->length() < replyLimit);
                if (pastFirstRow)
                    responseWriter->outputEndNested("Row"); // close last row
            }
            else
            {
                do
                {
                    size32_t rowSz;
                    const void *row = readActivity->nextRow(outBuilder, rowSz);
                    if (!row)
                    {
                        eoi = true;
                        break;
                    }
                    responseWriter->outputBeginNested("Row", false);
                    outMeta->toXML((const byte *)row, *responseWriter);
                    responseWriter->outputEndNested("Row");
                    resultBuffer.clear();
                }
                while (responseWriter->length() < replyLimit);
            }
            responseWriter->outputEndArray("Row");
            if (!eoi)
            {
                MemoryBuffer cursorMb;
                cursorMb.setEndian(__BIG_ENDIAN);
                readActivity->serializeCursor(cursorMb);
                StringBuffer cursorBinStr;
                JBASE64_Encode(cursorMb.toByteArray(), cursorMb.length(), cursorBinStr, true);
                responseWriter->outputString(cursorBinStr.length(), cursorBinStr.str(), "cursorBin");
            }
        }
    }
    void processWrite(IPropertyTree *requestTree, MemoryBuffer &rowDataMb, MemoryBuffer &responseMb)
    {
        IRemoteWriteActivity *writeActivity = activity->queryIsWriteActivity();
        assertex(writeActivity);

        /* row data is in serialized disk format already, and do not need to look at individual rows
         * so simply dump to disk
         */
        size32_t rowDataSz;
        rowDataMb.read(rowDataSz);
        const void *rowData;
        if (expander)
        {
            rowDataSz = expander->init(rowDataMb.readDirect(rowDataSz));
            expandMb.clear().reserve(rowDataSz);
            expander->expand(expandMb.bufferBase());
            rowData = expandMb.bufferBase();
        }
        else
            rowData = rowDataMb.readDirect(rowDataSz);
        writeActivity->write(rowDataSz, rowData);
    }

public:
    CRemoteRequest(int _cursorHandle, OutputFormat _format, ICompressor *_compressor, IExpander *_expander, IRemoteActivity *_activity)
        : cursorHandle(_cursorHandle), format(_format), activity(_activity), compressor(_compressor), expander(_expander)
    {
        if (outFmt_Binary != format)
        {
            responseWriter.setown(createIXmlWriterExt(0, 0, nullptr, outFmt_Xml == format ? WTStandard : WTJSONObject));
            responseWriter->outputBeginNested("Response", true);
            if (outFmt_Xml == format)
                responseWriter->outputCString("urn:hpcc:dfs", "@xmlns:dfs");
            responseWriter->outputUInt(cursorHandle, sizeof(cursorHandle), "handle");
        }
    }

    ~CRemoteRequest()
    {
        if (requestSpan != nullptr)
        {
            requestSpan->setSpanStatusSuccess(true);
        }
    }

    OutputFormat queryFormat() const { return format; }
    unsigned __int64 queryReplyLimit() const { return replyLimit; }
    IRemoteActivity *queryActivity() const { return activity; }
    ICompressor *queryCompressor() const { return compressor; }

    void process(IPropertyTree *requestTree, MemoryBuffer &restMb, MemoryBuffer &responseMb, CClientStats &stats)
    {
        bool traceParentChanged = false;
        const char* fullTraceContext = requestTree->queryProp("_trace/traceparent");
        if (fullTraceContext != nullptr)
        {
            // We only want to compare the trace-id & span-id, so ignore the last sampling group after the '-'
            const char* lastHyphen = strchr(fullTraceContext, '-');
            if (lastHyphen != nullptr)
            {
                size_t lastHyphenIdx = lastHyphen - fullTraceContext;
                traceParentChanged = strncmp(fullTraceContext, requestTraceParent.c_str(), lastHyphenIdx) != 0;
            }
        }

        if (traceParentChanged)
        {
            // Check to see if we have an existing span that needs to be marked successful before close
            if (requestSpan != nullptr)
            {
                requestSpan->setSpanStatusSuccess(true);
            }

            Owned<IProperties> traceHeaders = createProperties();
            traceHeaders->setProp("traceparent", fullTraceContext);

            const char* requestSpanName = nullptr;
            if (activity->queryIsReadActivity())
                requestSpanName = "ReadRequest";
            else
                requestSpanName = "WriteRequest";

            requestSpan.setown(queryTraceManager().createServerSpan(requestSpanName, traceHeaders));
            requestTraceParent = fullTraceContext;
        }

        ActiveSpanScope activeSpan(requestSpan.query());

        if (requestTree->hasProp("replyLimit"))
            replyLimit = requestTree->getPropInt64("replyLimit", defaultDaFSReplyLimitKB) * 1024;

        if (outFmt_Binary == format)
            responseMb.append(cursorHandle);
        else // outFmt_Xml || outFmt_Json
            responseWriter->outputUInt(cursorHandle, sizeof(cursorHandle), "handle");

        if (requestTree->hasProp("cursorBin")) // use handle if one provided
        {
            MemoryBuffer cursorMb;
            cursorMb.setEndian(__BIG_ENDIAN);
            JBASE64_Decode(requestTree->queryProp("cursorBin"), cursorMb);
            activity->restoreCursor(cursorMb);
        }

        if (activity->queryIsReadActivity())
            processRead(requestTree, responseMb);
        else if (activity->queryIsWriteActivity())
            processWrite(requestTree, restMb, responseMb);
        activity->flushStatistics(stats);

        if (outFmt_Binary != format)
        {
            responseWriter->outputEndNested("Response");
            responseWriter->finalize();
            PROGLOG("Response: %s", responseWriter->str());
            responseMb.append(responseWriter->length(), responseWriter->str());
        }
    }
};

enum OpenFileFlag { of_null=0x0, of_key=0x01 };
struct OpenFileInfo
{
    OpenFileInfo() { }
    OpenFileInfo(int _handle, IFileIO *_fileIO, StringAttrItem *_filename) : fileIO(_fileIO), filename(_filename), handle(_handle) { }
    OpenFileInfo(int _handle, CRemoteRequest *_remoteRequest, StringAttrItem *_filename)
        : remoteRequest(_remoteRequest), filename(_filename), handle(_handle) { }
    Linked<IFileIO> fileIO;
    Linked<CRemoteRequest> remoteRequest;
    Linked<StringAttrItem> filename; // for debug
    int handle = 0;
    unsigned flags = 0;
};



static IOutputMetaData *getTypeInfoOutputMetaData(IPropertyTree &actNode, const char *typePropName, bool grouped)
{
    IPropertyTree *json = actNode.queryPropTree(typePropName);
    if (json)
        return createTypeInfoOutputMetaData(*json, grouped);
    else
    {
        StringBuffer binTypePropName(typePropName);
        const char *jsonBin = actNode.queryProp(binTypePropName.append("Bin"));
        if (!jsonBin)
            return nullptr;
        MemoryBuffer mb;
        JBASE64_Decode(jsonBin, mb);
        return createTypeInfoOutputMetaData(mb, grouped);
    }
}


class CRemoteDiskBaseActivity : public CSimpleInterfaceOf<IRemoteReadActivity>, implements IVirtualFieldCallback
{
    typedef CSimpleInterfaceOf<IRemoteReadActivity> PARENT;
protected:
    StringAttr fileName; // physical filename
    Linked<IOutputMetaData> inMeta, outMeta;
    unsigned __int64 processed = 0;
    bool outputGrouped = false;
    bool opened = false;
    bool eofSeen = false;
    const RtlRecord *record = nullptr;
    RowFilter filter;
    RtlDynRow *filterRow = nullptr;
    // virtual field values
    StringAttr logicalFilename;
    unsigned numInputFields = 0;

    inline bool fieldFilterMatch(const void * buffer)
    {
        if (filterRow)
        {
            filterRow->setRow(buffer, filter.getNumFieldsRequired());
            return filter.matches(*filterRow);
        }
        else
            return true;
    }
public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CRemoteDiskBaseActivity(IPropertyTree &config, IFileDescriptor *fileDesc)
    {
        fileName.set(config.queryProp("fileName"));
        if (isEmptyString(fileName))
            throw createDafsException(DAFSERR_cmdstream_protocol_failure, "CRemoteDiskBaseActivity: fileName missing");
        logicalFilename.set(config.queryProp("virtualFields/logicalFilename"));
    }
    ~CRemoteDiskBaseActivity()
    {
        delete filterRow;
    }
    void setupInputMeta(const IPropertyTree &config, IOutputMetaData *_inMeta)
    {
        inMeta.setown(_inMeta);
        record = &inMeta->queryRecordAccessor(true);
        numInputFields = record->getNumFields();

        if (config.hasProp("keyFilter"))
        {
            filterRow = new RtlDynRow(*record);
            Owned<IPropertyTreeIterator> filterIter = config.getElements("keyFilter");
            ForEach(*filterIter)
                filter.addFilter(*record, filterIter->query().queryProp(nullptr));
        }
    }
// IRemoteReadActivity impl.
    virtual unsigned __int64 queryProcessed() const override
    {
        return processed;
    }
    virtual IOutputMetaData *queryOutputMeta() const override
    {
        return outMeta;
    }
    virtual bool isGrouped() const override
    {
        return outputGrouped;
    }
    virtual void serializeCursor(MemoryBuffer &tgt) const override
    {
        // we need to serialize something, because the lack of a cursor is used to signify end of stream
        // NB: the cursor is opaque and only to be consumed by dafilesrv. When used it is simply passed back.
        tgt.append("UNSUPPORTED");
    }
    virtual void restoreCursor(MemoryBuffer &src) override
    {
        throw makeStringExceptionV(0, "restoreCursor not supported in: %s", typeid(*this).name());
        throwUnimplemented();
    }
    virtual void flushStatistics(CClientStats &stats) override
    {
        throwUnexpected();
    }
    virtual IRemoteReadActivity *queryIsReadActivity() override
    {
        return this;
    }
    virtual bool requiresPostProject() const override
    {
        return false;
    }
    virtual void seek(offset_t pos) override
    {
        throwUnexpected();
    }
//interface IVirtualFieldCallback
    virtual const char * queryLogicalFilename(const void * row) override
    {
        return logicalFilename.str();
    }
    virtual unsigned __int64 getFilePosition(const void * row) override
    {
        throwUnexpected();
    }
    virtual unsigned __int64 getLocalFilePosition(const void * row) override
    {
        throwUnexpected();
    }
    virtual const byte * lookupBlob(unsigned __int64 id) override
    {
        throwUnexpected();
    }
};


class CRemoteStreamReadBaseActivity : public CRemoteDiskBaseActivity, implements IFileSerialStreamCallback
{
    typedef CRemoteDiskBaseActivity PARENT;

protected:
    Owned<IBufferedSerialInputStream> inputStream;
    Owned<IFileIO> iFileIO;
    unsigned __int64 chooseN = 0;
    unsigned __int64 startPos = 0;
    bool compressed = false;
    bool cursorDirty = false;
    bool fetching = false;
    unsigned __int64 bytesRead = 0;
    // virtual field values
    unsigned partNum = 0;
    offset_t baseFpos = 0;

    virtual bool refreshCursor()
    {
        if (inputStream->tell() != startPos)
        {
            inputStream->reset(startPos, UnknownOffset);
            return true;
        }
        return false;
    }
    void setFetchPos(offset_t fpos)
    {
        startPos = fpos;
        cursorDirty = true;
        fetching = true;
    }
    bool checkOpen() // NB: returns true if this call opened file
    {
        if (opened)
        {
            if (!cursorDirty)
                return false;
            refreshCursor();
            eofSeen = false;
            cursorDirty = false;
            return false;
        }
        cursorDirty = false;
        OwnedIFile iFile = createIFile(fileName);
        assertex(iFile);
        iFileIO.setown(createCompressedFileReader(iFile));
        if (iFileIO)
        {
            if (!compressed)
            {
                WARNLOG("meta info did not mark file '%s' as compressed, but detected file as compressed", fileName.get());
                compressed = true;
            }
        }
        else
        {
            iFileIO.setown(iFile->open(IFOread));
            if (!iFileIO)
                throw createDafsExceptionV(DAFSERR_cmdstream_protocol_failure, "Failed to open: '%s'", fileName.get());
            if (compressed)
            {
                WARNLOG("meta info marked file '%s' as compressed, but detected file as uncompressed", fileName.get());
                compressed = false;
            }
        }
        inputStream.setown(createFileSerialStream(iFileIO, startPos, (offset_t)-1, (size32_t)-1, this));

        opened = true;
        eofSeen = false;
        return true;
    }
    void close()
    {
        iFileIO.clear();
        opened = false;
        eofSeen = true;
    }
// IFileSerialStreamCallback impl.
    virtual void process(offset_t ofs, size32_t sz, const void *buf) override
    {
        bytesRead += sz;
    }
public:
    CRemoteStreamReadBaseActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc)
    {
        compressed = config.getPropBool("compressed");
        chooseN = config.getPropInt64("chooseN", defaultFileStreamChooseNLimit);

        partNum = config.getPropInt("virtualFields/partNum");
        baseFpos = (offset_t)config.getPropInt64("virtualFields/baseFpos");
    }
    virtual void flushStatistics(CClientStats &stats) override
    {
        // NB: will be called by same thread that is reading.
        stats.addRead(bytesRead);
        bytesRead = 0;
    }
// IRemoteReadActivity impl.
    virtual void seek(offset_t pos) override
    {
        setFetchPos(pos);
        checkOpen();
    }
// IVirtualFieldCallback impl.
    virtual unsigned __int64 getFilePosition(const void * row) override
    {
        return inputStream->tell() + baseFpos;
    }
    virtual unsigned __int64 getLocalFilePosition(const void * row) override
    {
        return makeLocalFposOffset(partNum, inputStream->tell());
    }
};


class CRemoteDiskReadActivity : public CRemoteStreamReadBaseActivity
{
    typedef CRemoteStreamReadBaseActivity PARENT;

    CThorContiguousRowBuffer prefetchBuffer;
    Owned<ISourceRowPrefetcher> prefetcher;
    bool inputGrouped = false;
    bool eogPending = false;
    bool someInGroup = false;
    Owned<const IDynamicTransform> translator;

    virtual bool refreshCursor() override
    {
        if (prefetchBuffer.tell() != startPos)
        {
            inputStream->reset(startPos, UnknownOffset);
            prefetchBuffer.clearStream();
            prefetchBuffer.setStream(inputStream);
            return true;
        }
        return false;
    }
    bool checkOpen()
    {
        if (!PARENT::checkOpen()) // returns true if it opened file
            return false;
        prefetchBuffer.setStream(inputStream);
        prefetcher.setown(inMeta->createDiskPrefetcher());
        return true;
    }
public:
    CRemoteDiskReadActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc), prefetchBuffer(nullptr)
    {
        inputGrouped = config.getPropBool("inputGrouped", false);
        setupInputMeta(config, getTypeInfoOutputMetaData(config, "input", inputGrouped));

        outputGrouped = config.getPropBool("outputGrouped", false);
        if (!inputGrouped && outputGrouped)
            outputGrouped = false; // perhaps should fire error
        outMeta.setown(getTypeInfoOutputMetaData(config, "output", outputGrouped));
        if (!outMeta)
            outMeta.set(inMeta);
        translator.setown(createRecordTranslator(outMeta->queryRecordAccessor(true), *record));
    }
// IRemoteReadActivity impl.
    virtual const void *nextRow(MemoryBufferBuilder &outBuilder, size32_t &retSz) override
    {
        if (eogPending || eofSeen)
        {
            eogPending = false;
            someInGroup = false;
            retSz = 0;
            return nullptr;
        }
        checkOpen();
        while (!eofSeen && (processed < chooseN))
        {
            while (!prefetchBuffer.eos())
            {
                prefetcher->readAhead(prefetchBuffer);
                size32_t inputRowSz = prefetchBuffer.queryRowSize();
                bool eog = false;
                if (inputGrouped)
                {
                    prefetchBuffer.skip(sizeof(eog));
                    if (outputGrouped)
                    {
                        byte b = *(prefetchBuffer.queryRow()+inputRowSz);
                        memcpy(&eog, prefetchBuffer.queryRow()+inputRowSz, sizeof(eog));
                    }
                }
                const byte *next = prefetchBuffer.queryRow();
                size32_t rowSz; // use local var instead of reference param for efficiency
                if (fieldFilterMatch(next))
                    rowSz = translator->translate(outBuilder, *this, next);
                else
                    rowSz = 0;
                prefetchBuffer.finishedRow();
                const void *ret = outBuilder.getSelf();
                outBuilder.finishRow(rowSz);

                if (rowSz)
                {
                    processed++;
                    eogPending = eog;
                    someInGroup = true;
                    retSz = rowSz;
                    return ret;
                }
                else if (eog)
                {
                    eogPending = false;
                    if (someInGroup)
                    {
                        someInGroup = false;
                        return nullptr;
                    }
                }
            }
            break;
        }
        if (!fetching) // when in fetch mode, don't assume file won't be used again (likely will be another batch)
        {
            eofSeen = true;
            close();
        }
        retSz = 0;
        return nullptr;
    }
    virtual void seek(offset_t pos) override
    {
        setFetchPos(pos);
        checkOpen();
    }
    virtual void serializeCursor(MemoryBuffer &tgt) const override
    {
        tgt.append(prefetchBuffer.tell());
        tgt.append(processed);
        tgt.append(someInGroup);
        tgt.append(eogPending);
    }
    virtual void restoreCursor(MemoryBuffer &src) override
    {
        cursorDirty = true;
        src.read(startPos);
        src.read(processed);
        src.read(someInGroup);
        src.read(eogPending);
    }
    virtual StringBuffer &getInfoStr(StringBuffer &out) const override
    {
        return out.appendf("diskread[%s]", fileName.get());
    }
//interface IVirtualFieldCallback
    virtual unsigned __int64 getFilePosition(const void * row) override
    {
        return prefetchBuffer.tell() + baseFpos;
    }
};


class CRemoteExternalFormatReadActivity : public CRemoteStreamReadBaseActivity
{
    typedef CRemoteStreamReadBaseActivity PARENT;

protected:
    Owned<const IDynamicFieldValueFetcher> fieldFetcher;
    Owned<const IDynamicTransform> translator;
    bool postProject = false;

public:
    CRemoteExternalFormatReadActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc)
    {
        setupInputMeta(config, getTypeInfoOutputMetaData(config, "input", false));
        outMeta.setown(getTypeInfoOutputMetaData(config, "output", false));
        const RtlRecord *outRecord = record;
        if (filterRow)
        {
            if (outMeta)
                postProject = true;
            outMeta.set(inMeta);
        }
        else
        {
            if (outMeta)
                outRecord = &outMeta->queryRecordAccessor(true);
            else
                outMeta.set(inMeta);
        }
        translator.setown(createRecordTranslatorViaCallback(*outRecord, *record, type_utf8));
    }
    virtual bool requiresPostProject() const override
    {
        return postProject;
    }
};


class CNullNestedRowIterator : public CSimpleInterfaceOf<IDynamicRowIterator>
{
public:
    virtual bool first() override { return false; }
    virtual bool next() override { return false; }
    virtual bool isValid() override { return false; }
    virtual IDynamicFieldValueFetcher &query() override
    {
        throwUnexpected();
    }
} nullNestedRowIterator;

class CRemoteCsvReadActivity : public CRemoteExternalFormatReadActivity
{
    typedef CRemoteExternalFormatReadActivity PARENT;

    StringBuffer csvQuote, csvSeparate, csvTerminate, csvEscape;
    unsigned __int64 headerLines = 0;
    unsigned __int64 maxRowSize = 0;
    bool preserveWhitespace = false;
    CSVSplitter csvSplitter;

    class CFieldFetcher : public CSimpleInterfaceOf<IDynamicFieldValueFetcher>
    {
        CSVSplitter &csvSplitter;
        unsigned numInputFields;
    public:
        CFieldFetcher(CSVSplitter &_csvSplitter, unsigned _numInputFields) : csvSplitter(_csvSplitter), numInputFields(_numInputFields)
        {
        }
        virtual const byte *queryValue(unsigned fieldNum, size_t &sz) const override
        {
            dbgassertex(fieldNum < numInputFields);
            sz = csvSplitter.queryLengths()[fieldNum];
            return csvSplitter.queryData()[fieldNum];
        }
        virtual IDynamicRowIterator *getNestedIterator(unsigned fieldNum) const override
        {
            return LINK(&nullNestedRowIterator);
        }
        virtual size_t getSize(unsigned fieldNum) const override { throwUnexpected(); }
        virtual size32_t getRecordSize() const override { throwUnexpected(); }
    };

    bool checkOpen()
    {
        if (!PARENT::checkOpen())
            return false;
        csvSplitter.init(numInputFields, maxRowSize, csvQuote, csvSeparate, csvTerminate, csvEscape, preserveWhitespace);

        if (headerLines && !fetching) // NB: would be harmless if skipped headers on open if fetching, but it's a waste of time
        {
            do
            {
                size32_t lineLength = csvSplitter.splitLine(inputStream, maxRowSize);
                if (0 == lineLength)
                    break;
                inputStream->skip(lineLength);
            }
            while (--headerLines);
        }

        if (!fieldFetcher)
            fieldFetcher.setown(new CFieldFetcher(csvSplitter, numInputFields));

        return true;
    }
    const unsigned defaultMaxCsvRowSize = 10; // MB
public:
    CRemoteCsvReadActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc)
    {
        maxRowSize = config.getPropInt64("ActivityOptions/maxRowSize", defaultMaxCsvRowSize) * 1024 * 1024;
        preserveWhitespace = config.getPropBool("ActivityOptions/preserveWhitespace");

        if (!config.getProp("ActivityOptions/csvQuote", csvQuote))
        {
            if (!fileDesc->queryProperties().getProp("@csvQuote", csvQuote))
                csvQuote.append("\"");
        }
        if (!config.getProp("ActivityOptions/csvSeparate", csvSeparate))
        {
            if (!fileDesc->queryProperties().getProp("@csvSeparate", csvSeparate))
                csvSeparate.append("\\,");
        }
        if (!config.getProp("ActivityOptions/csvTerminate", csvTerminate))
        {
            if (!fileDesc->queryProperties().getProp("@csvTerminate", csvTerminate))
                csvTerminate.append("\\n,\\r\\n");
        }
        if (!config.getProp("ActivityOptions/csvEscape", csvEscape))
            fileDesc->queryProperties().getProp("@csvEscape", csvEscape);

        headerLines = config.getPropInt64("ActivityOptions/headerLines"); // really this should be a published attribute too
    }
    virtual StringBuffer &getInfoStr(StringBuffer &out) const override
    {
        return out.appendf("csvread[%s]", fileName.get());
    }
// IRemoteReadActivity impl.
    virtual const void *nextRow(MemoryBufferBuilder &outBuilder, size32_t &retSz) override
    {
        if (eofSeen)
        {
            retSz = 0;
            return nullptr;
        }
        checkOpen();
        while (!eofSeen && (processed < chooseN))
        {
            size32_t lineLength = csvSplitter.splitLine(inputStream, maxRowSize);
            if (!lineLength)
                break;

            retSz = translator->translate(outBuilder, *this, *fieldFetcher);
            dbgassertex(retSz);
            const void *ret = outBuilder.getSelf();
            if (fieldFilterMatch(ret))
            {
                outBuilder.finishRow(retSz);
                ++processed;
                if (!fetching) // harmless, but pointless to skip if fetching
                    inputStream->skip(lineLength);
                return ret;
            }
            else
                outBuilder.removeBytes(retSz);
            if (!fetching) // harmless, but pointless to skip if fetching
                inputStream->skip(lineLength);
        }
        if (!fetching) // when in fetch mode, don't assume file won't be used again (likely will be another batch)
        {
            eofSeen = true;
            close();
        }
        retSz = 0;
        return nullptr;
    }
    virtual void seek(offset_t pos) override
    {
        setFetchPos(pos);
        checkOpen();
    }
};


class CRemoteMarkupReadActivity : public CRemoteExternalFormatReadActivity, implements IXMLSelect
{
    typedef CRemoteExternalFormatReadActivity PARENT;

    ThorActivityKind kind;
    IXmlToRowTransformer *xmlTransformer = nullptr;
    Linked<IColumnProvider> lastMatch;
    Owned<IXMLParse> xmlParser;

    bool noRoot = false;
    bool useXmlContents = false;

    // JCSMORE - it would be good if these were cached/reused (can I assume anything using fetcher is single threaded?)
    class CFieldFetcher : public CSimpleInterfaceOf<IDynamicFieldValueFetcher>
    {
        unsigned numInputFields;
        const RtlRecord &recInfo;
        Linked<IColumnProvider> currentMatch;
        const char **compoundXPaths = nullptr;

        const char *queryCompoundXPath(unsigned fieldNum) const
        {
            if (compoundXPaths && compoundXPaths[fieldNum])
                return compoundXPaths[fieldNum];
            else
                return recInfo.queryXPath(fieldNum);
        }
    public:
        CFieldFetcher(const RtlRecord &_recInfo, IColumnProvider *_currentMatch) : recInfo(_recInfo), currentMatch(_currentMatch)
        {
            numInputFields = recInfo.getNumFields();

            // JCSMORE - should this be done (optionally) when RtlRecord is created?
            for (unsigned fieldNum=0; fieldNum<numInputFields; fieldNum++)
            {
                if (recInfo.queryType(fieldNum)->queryChildType())
                {
                    const char *xpath = recInfo.queryXPath(fieldNum);
                    dbgassertex(xpath);
                    const char *ptr = xpath;
                    char *expandedXPath = nullptr;
                    char *expandedXPathPtr = nullptr;
                    while (true)
                    {
                        if (*ptr == xpathCompoundSeparatorChar)
                        {
                            if (!compoundXPaths)
                            {
                                compoundXPaths = new const char *[numInputFields];
                                memset(compoundXPaths, 0, sizeof(const char *)*numInputFields);
                            }

                            size_t sz = strlen(xpath)+1;
                            expandedXPath = new char[sz];
                            expandedXPathPtr = expandedXPath;
                            if (ptr == xpath) // if leading char, just skip
                                ++ptr;
                            else
                            {
                                size32_t len = ptr-xpath;
                                memcpy(expandedXPath, xpath, len);
                                expandedXPathPtr = expandedXPath + len;
                                *expandedXPathPtr++ = '/';
                                ++ptr;
                            }
                            while (*ptr)
                            {
                                if (*ptr == xpathCompoundSeparatorChar)
                                {
                                    *expandedXPathPtr++ = '/';
                                    ++ptr;
                                }
                                else
                                    *expandedXPathPtr++ = *ptr++;
                            }
                        }
                        else
                            ptr++;
                        if ('\0' == *ptr)
                        {
                            if (expandedXPath)
                            {
                                *expandedXPathPtr = '\0';
                                compoundXPaths[fieldNum] = expandedXPath;
                            }
                            break;
                        }
                    }
                }
            }
        }
        ~CFieldFetcher()
        {
            if (compoundXPaths)
            {
                for (unsigned fieldNum=0; fieldNum<numInputFields; fieldNum++)
                    delete [] compoundXPaths[fieldNum];
                delete [] compoundXPaths;
            }
        }
        void setCurrentMatch(IColumnProvider *_currentMatch)
        {
            currentMatch.set(_currentMatch);
        }
    // IDynamicFieldValueFetcher impl.
        virtual const byte *queryValue(unsigned fieldNum, size_t &sz) const override
        {
            dbgassertex(fieldNum < numInputFields);
            dbgassertex(currentMatch);

            size32_t rawSz;
            const char *ret = currentMatch->readRaw(recInfo.queryXPath(fieldNum), rawSz);
            sz = rawSz;
            return (const byte *)ret;
        }
        virtual IDynamicRowIterator *getNestedIterator(unsigned fieldNum) const override
        {
            dbgassertex(fieldNum < numInputFields);
            dbgassertex(currentMatch);

            const RtlRecord *nested = recInfo.queryNested(fieldNum);
            if (!nested)
                return nullptr;

            class CIterator : public CSimpleInterfaceOf<IDynamicRowIterator>
            {
                XmlChildIterator xmlIter;
                Linked<IDynamicFieldValueFetcher> curFieldValueFetcher;
                Linked<IColumnProvider> parentMatch;
                const RtlRecord &nestedRecInfo;
            public:
                CIterator(const RtlRecord &_nestedRecInfo, IColumnProvider *_parentMatch, const char *xpath) : parentMatch(_parentMatch), nestedRecInfo(_nestedRecInfo)
                {
                    xmlIter.initOwn(parentMatch->getChildIterator(xpath));
                }
                virtual bool first() override
                {
                    IColumnProvider *child = xmlIter.first();
                    if (!child)
                    {
                        curFieldValueFetcher.clear();
                        return false;
                    }
                    curFieldValueFetcher.setown(new CFieldFetcher(nestedRecInfo, child));

                    return true;
                }
                virtual bool next() override
                {
                    IColumnProvider *child = xmlIter.next();
                    if (!child)
                    {
                        curFieldValueFetcher.clear();
                        return false;
                    }
                    curFieldValueFetcher.setown(new CFieldFetcher(nestedRecInfo, child));
                    return true;
                }
                virtual bool isValid() override
                {
                    return nullptr != curFieldValueFetcher.get();
                }
                virtual IDynamicFieldValueFetcher &query() override
                {
                    assertex(curFieldValueFetcher);
                    return *curFieldValueFetcher;
                }
            };
            // JCSMORE - it would be good if these were cached/reused (can I assume anything using parent fetcher is single threaded?)
            return new CIterator(*nested, currentMatch, queryCompoundXPath(fieldNum));
        }
        virtual size_t getSize(unsigned fieldNum) const override { throwUnexpected(); }
        virtual size32_t getRecordSize() const override { throwUnexpected(); }
    };

    bool checkOpen()
    {
        if (!PARENT::checkOpen())
            return false;

        class CSimpleStream : public CSimpleInterfaceOf<ISimpleReadStream>
        {
            Linked<IBufferedSerialInputStream> stream;
        public:
            CSimpleStream(IBufferedSerialInputStream *_stream) : stream(_stream)
            {
            }
        // ISimpleReadStream impl.
            virtual size32_t read(size32_t max_len, void * data) override
            {
                size32_t got;
                const void *res = stream->peek(max_len, got);
                if (got)
                {
                    if (got>max_len)
                        got = max_len;
                    memcpy(data, res, got);
                    stream->skip(got);
                }
                return got;
            }
        };
        Owned<ISimpleReadStream> simpleStream = new CSimpleStream(inputStream);
        if (kind==TAKjsonread)
            xmlParser.setown(createJSONParse(*simpleStream, xpath, *this, noRoot?ptr_noRoot:ptr_none, useXmlContents));
        else
            xmlParser.setown(createXMLParse(*simpleStream, xpath, *this, noRoot?ptr_noRoot:ptr_none, useXmlContents));

        if (!fieldFetcher)
            fieldFetcher.setown(new CFieldFetcher(*record, nullptr));

        return true;
    }
protected:
    StringBuffer xpath;
    StringBuffer customRowTag;
public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CRemoteMarkupReadActivity(IPropertyTree &config, IFileDescriptor *fileDesc, ThorActivityKind _kind) : PARENT(config, fileDesc), kind(_kind)
    {
        config.getProp("ActivityOptions/rowTag", customRowTag);
        noRoot = config.getPropBool("noRoot");
    }
    IColumnProvider *queryMatch() const { return lastMatch; }
    virtual StringBuffer &getInfoStr(StringBuffer &out) const override
    {
        return out.appendf("%s[%s]", getActivityText(kind), fileName.get());
    }
// IRemoteReadActivity impl.
    virtual const void *nextRow(MemoryBufferBuilder &outBuilder, size32_t &retSz) override
    {
        if (eofSeen)
        {
            retSz = 0;
            return nullptr;
        }
        checkOpen();

        while (xmlParser->next())
        {
            if (lastMatch)
            {
                ((CFieldFetcher *)fieldFetcher.get())->setCurrentMatch(lastMatch);
                retSz = translator->translate(outBuilder, *this, *fieldFetcher);
                dbgassertex(retSz);

                lastMatch.clear();
                const void *ret = outBuilder.getSelf();
                if (fieldFilterMatch(ret))
                {
                    outBuilder.finishRow(retSz);
                    ++processed;
                    return ret;
                }
                else
                    outBuilder.removeBytes(retSz);
            }
        }
        if (!fetching) // when in fetch mode, don't assume file won't be used again (likely will be another batch)
        {
            eofSeen = true;
            close();
        }
        retSz = 0;
        return nullptr;
    }
    virtual void seek(offset_t pos) override
    {
        setFetchPos(pos);
        checkOpen();
    }
// IXMLSelect impl.
    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        lastMatch.set(&entry);
    }
};


class CRemoteXmlReadActivity : public CRemoteMarkupReadActivity
{
    typedef CRemoteMarkupReadActivity PARENT;
public:
    CRemoteXmlReadActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc, TAKxmlread)
    {
        if (customRowTag.isEmpty()) // no override
            fileDesc->queryProperties().getProp("@rowTag", xpath);
        else
        {
            xpath.set("/Dataset/");
            xpath.append(customRowTag);
        }
    }
};


class CRemoteJsonReadActivity : public CRemoteMarkupReadActivity
{
    typedef CRemoteMarkupReadActivity PARENT;
public:
    CRemoteJsonReadActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc, TAKjsonread)
    {
        if (customRowTag.isEmpty()) // no override
            fileDesc->queryProperties().getProp("@rowTag", xpath);
        else
        {
            xpath.set("/");
            xpath.append(customRowTag);
        }
    }
};

/* A IRemoteReadActivity that projects to output format
 * Created if input activity requires filtering, but it must 1st translate from external format to the actual format
 * NB: processor, grouped and cursor are same as input.
 */
class CRemoteCompoundReadProjectActivity : public CSimpleInterfaceOf<IRemoteReadActivity>
{
    Linked<IRemoteReadActivity> input;
    Owned<IOutputMetaData> outMeta;
    Owned<const IDynamicTransform> translator;
    UnexpectedVirtualFieldCallback fieldCallback;
    MemoryBuffer inputRowMb;
    MemoryBufferBuilder *inputRowBuilder;
public:
    CRemoteCompoundReadProjectActivity(IPropertyTree &config, IRemoteReadActivity *_input) : input(_input)
    {
        IOutputMetaData *inMeta = input->queryOutputMeta();
        outMeta.setown(getTypeInfoOutputMetaData(config, "output", false));
        dbgassertex(outMeta);
        const RtlRecord &inRecord = inMeta->queryRecordAccessor(true);
        const RtlRecord &outRecord = outMeta->queryRecordAccessor(true);
        translator.setown(createRecordTranslator(outRecord, inRecord));
        inputRowBuilder = new MemoryBufferBuilder(inputRowMb, inMeta->getMinRecordSize());
    }
    ~CRemoteCompoundReadProjectActivity()
    {
        delete inputRowBuilder;
    }
    virtual StringBuffer &getInfoStr(StringBuffer &out) const override
    {
        return input->getInfoStr(out).append(" - CompoundProject");
    }
// IRemoteReadActivity impl.
    virtual unsigned __int64 queryProcessed() const override
    {
        return input->queryProcessed();
    }
    virtual IOutputMetaData *queryOutputMeta() const override
    {
        return outMeta;
    }
    virtual bool isGrouped() const override
    {
        return input->isGrouped();
    }
    virtual void serializeCursor(MemoryBuffer &tgt) const override
    {
        input->serializeCursor(tgt);
    }
    virtual void restoreCursor(MemoryBuffer &src) override
    {
        input->restoreCursor(src);
    }
    virtual void flushStatistics(CClientStats &stats) override
    {
        input->flushStatistics(stats);
    }
    virtual IRemoteReadActivity *queryIsReadActivity() override
    {
        return this;
    }
    virtual const void *nextRow(MemoryBufferBuilder &outBuilder, size32_t &retSz) override
    {
        size32_t rowSz;
        const void *row = input->nextRow(*inputRowBuilder, rowSz);
        if (!row)
        {
            retSz = 0;
            return nullptr;
        }
        retSz = translator->translate(outBuilder, fieldCallback, (const byte *)row);

        const void *ret = outBuilder.getSelf();
        outBuilder.finishRow(retSz);
        return ret;
    }
    virtual bool requiresPostProject() const override
    {
        return false;
    }
    virtual void seek(offset_t pos) override
    {
        throwUnexpected();
    }
};


class CRemoteCompoundBatchFPosFetchActivity : public CSimpleInterfaceOf<IRemoteFetchActivity>
{
    Linked<IRemoteReadActivity> input;
    std::vector<offset_t> fposs;
    unsigned fetchItemIndex = 0;
    StringAttr fileName;

    bool handleNextFetch()
    {
        if (fetchItemIndex == fposs.size()) // end
            return false;
        offset_t fpos = fposs[fetchItemIndex++];
        input->seek(fpos);
        return true;
    }
public:
    CRemoteCompoundBatchFPosFetchActivity(IPropertyTree &requestTree, IRemoteReadActivity *_input) : input(_input)
    {
        fileName.set(requestTree.queryProp("node/fileName"));
    }
// IRemoteReadActivity impl.
    virtual StringBuffer &getInfoStr(StringBuffer &out) const override
    {
        return input->getInfoStr(out).append(" - CompoundBatchFPosFetch");
    }
    virtual const void *nextRow(MemoryBufferBuilder &outBuilder, size32_t &retSz) override
    {
        while (handleNextFetch())
        {
            const void *ret = input->nextRow(outBuilder, retSz);
            if (ret)
                return ret;
        }
        retSz = 0;
        return nullptr;
    }
    virtual unsigned __int64 queryProcessed() const override
    {
        return input->queryProcessed();
    }
    virtual IOutputMetaData *queryOutputMeta() const override
    {
        return input->queryOutputMeta();
    }
    virtual bool isGrouped() const override
    {
        return input->isGrouped();
    }
    virtual bool requiresPostProject() const override
    {
        return input->requiresPostProject();
    }
    virtual void flushStatistics(CClientStats &stats) override
    {
        input->flushStatistics(stats);
    }
    virtual IRemoteReadActivity *queryIsReadActivity() override
    {
        return this;
    }
    virtual void seek(offset_t pos) override
    {
        throwUnexpected();
    }
    virtual void loadFetchBatch(IPropertyTree *fetch) override
    {
        if (isGrouped())
            throw createDafsExceptionV(DAFSERR_cmdstream_protocol_failure, "Output cannot be grouped when fetching. fileName=%s", fileName.get());

        // remove consumed fpos'
        fposs.erase(fposs.begin(), fposs.begin() + fetchItemIndex);
        fetchItemIndex = 0;

        Owned<IPropertyTreeIterator> iter = fetch->getElements("fpos");
        ForEach (*iter)
            fposs.push_back(iter->query().getPropInt64(nullptr));
    }
    virtual void serializeCursor(MemoryBuffer &tgt) const override
    {
        tgt.append((unsigned)(fposs.size()-fetchItemIndex));
        for (unsigned i=fetchItemIndex; i<fposs.size(); i++)
            tgt.append(fposs[i]);
        input->serializeCursor(tgt);
    }
    virtual void restoreCursor(MemoryBuffer &src) override
    {
        fposs.clear();
        unsigned num;
        src.read(num);
        while (num--)
        {
            offset_t fpos;
            src.read(fpos);
            fposs.push_back(fpos);
        }
        fetchItemIndex = 0;
        input->restoreCursor(src);
    }
    virtual IRemoteFetchActivity *queryIsFetchActivity() override { return this; }
};

class CRemoteIndexBaseActivity : public CRemoteDiskBaseActivity
{
    typedef CRemoteDiskBaseActivity PARENT;

protected:
    bool isTlk = false;
    unsigned fileCrc = 0;
    Owned<IKeyIndex> keyIndex;
    Owned<IKeyManager> keyManager;
    RowFilter keyFilter;

    void checkOpen()
    {
        if (opened)
            return;
        Owned<IFile> indexFile = createIFile(fileName);
        CDateTime modTime;
        indexFile->getTime(nullptr, &modTime, nullptr);
        time_t modTimeTT = modTime.getSimple();
        CRC32 crc32(fileCrc);
        crc32.tally(sizeof(time_t), &modTimeTT);
        unsigned crc = crc32.get();

        keyIndex.setown(createKeyIndex(fileName, crc, isTlk, 0));
        keyManager.setown(createLocalKeyManager(*record, keyIndex, nullptr, true, false));
        keyFilter.createSegmentMonitors(keyManager);
        keyManager->finishSegmentMonitors();
        keyManager->reset();

        opened = true;
    }
    void close()
    {
        keyManager.clear();
        keyIndex.clear();
        opened = false;
        eofSeen = true;
    }
public:
    CRemoteIndexBaseActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc)
    {
        setupInputMeta(config, getTypeInfoOutputMetaData(config, "input", false));
        filter.splitIntoKeyFilter(*record, keyFilter);

        isTlk = config.getPropBool("isTlk");
        fileCrc = config.getPropInt("crc");
    }
    virtual void flushStatistics(CClientStats &stats) override
    {
        // TBD, IKeyCursor should probably have a getStatistic(StatisticKind kind) implementation
    }
};

class CRemoteIndexReadActivity : public CRemoteIndexBaseActivity
{
    typedef CRemoteIndexBaseActivity PARENT;

    Owned<const IDynamicTransform> translator;
    unsigned __int64 chooseN = 0;
    bool cleanupBlobs = false;
public:
    CRemoteIndexReadActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc)
    {
        chooseN = config.getPropInt64("chooseN", defaultFileStreamChooseNLimit);
        outMeta.setown(getTypeInfoOutputMetaData(config, "output", false));
        if (outMeta)
            translator.setown(createRecordTranslator(outMeta->queryRecordAccessor(true), *record));
        else
            outMeta.set(inMeta);
    }
// IRemoteReadActivity impl.
    virtual const void *nextRow(MemoryBufferBuilder &outBuilder, size32_t &retSz) override
    {
        if (eofSeen)
        {
            retSz = 0;
            return nullptr;
        }
        checkOpen();
        if (!eofSeen)
        {
            if (processed < chooseN)
            {
                while (keyManager->lookup(true))
                {
                    const byte *keyRow = keyManager->queryKeyBuffer();
                    if (fieldFilterMatch(keyRow))
                    {
                        if (translator)
                            retSz = translator->translate(outBuilder, *this, keyRow);
                        else
                        {
                            retSz = keyManager->queryRowSize();
                            outBuilder.ensureCapacity(retSz, nullptr);
                            memcpy(outBuilder.getSelf(), keyRow, retSz);
                        }
                        dbgassertex(retSz);

                        if (cleanupBlobs)
                        {
                            keyManager->releaseBlobs();
                            cleanupBlobs = false;
                        }

                        const void *ret = outBuilder.getSelf();
                        outBuilder.finishRow(retSz);
                        ++processed;
                        return ret;
                    }
                }
                retSz = 0;
            }
            eofSeen = true;
        }
        close();
        return nullptr;
    }
    virtual void serializeCursor(MemoryBuffer &tgt) const override
    {
        keyManager->serializeCursorPos(tgt);
        tgt.append(processed);

/* JCSMORE (see HPCC-19640), serialize seek/scan data to client
        tgt.append(keyManager->querySeeks());
        tgt.append(keyManager->queryScans());
*/
    }
    virtual void restoreCursor(MemoryBuffer &src) override
    {
        checkOpen();
        eofSeen = false;
        keyManager->deserializeCursorPos(src);
        src.read(processed);
    }
    virtual StringBuffer &getInfoStr(StringBuffer &out) const override
    {
        return out.appendf("indexread[%s]", fileName.get());
    }

    virtual const byte * lookupBlob(unsigned __int64 id) override
    {
        size32_t dummy;
        cleanupBlobs = true;
        return (byte *) keyManager->loadBlob(id, dummy, nullptr);
    }
};


class CRemoteWriteBaseActivity : public CSimpleInterfaceOf<IRemoteWriteActivity>
{
protected:
    StringAttr fileName; // physical filename
    Linked<IOutputMetaData> meta;
    unsigned __int64 processed = 0;
    unsigned __int64 bytesWritten = 0;
    bool opened = false;
    bool eofSeen = false;

    Owned<IFileIO> iFileIO;
    bool grouped = false;

    void close()
    {
        if (iFileIO)
        {
            iFileIO->close();
            iFileIO.clear();
        }
        opened = false;
        eofSeen = true;
    }
public:
    CRemoteWriteBaseActivity(IPropertyTree &config, IFileDescriptor *fileDesc)
    {
        fileName.set(config.queryProp("fileName"));
        if (isEmptyString(fileName))
            throw createDafsException(DAFSERR_cmdstream_protocol_failure, "CRemoteWriteBaseActivity: fileName missing");
        grouped = config.getPropBool("inputGrouped");
        meta.setown(getTypeInfoOutputMetaData(config, "input", grouped));
    }
    ~CRemoteWriteBaseActivity()
    {
    }
// IRemoteWriteActivity impl.
    virtual unsigned __int64 queryProcessed() const override
    {
        return processed;
    }
    virtual IOutputMetaData *queryOutputMeta() const override
    {
        return meta;
    }
    virtual bool isGrouped() const override
    {
        return grouped;
    }
    virtual void serializeCursor(MemoryBuffer &tgt) const override
    {
        // we need to serialize something, because the lack of a cursor is used to signify end of stream
        // NB: the cursor is opaque and only to be consumed by dafilesrv. When used it is simply passed back.
        tgt.append("UNSUPPORTED");
    }
    virtual void restoreCursor(MemoryBuffer &src) override
    {
        throw makeStringExceptionV(0, "restoreCursor not supported in: %s", typeid(*this).name());
    }
    virtual StringBuffer &getInfoStr(StringBuffer &out) const override
    {
        return out.appendf("diskwrite[%s]", fileName.get());
    }
    virtual void write(size32_t sz, const void *rowData) override
    {
        throwUnexpected(); // method should be implemented in derived classes.
    }
    virtual void flushStatistics(CClientStats &stats) override
    {
        // NB: will be called by same thread that is writing.
        stats.addWrite(bytesWritten);
        bytesWritten = 0;
    }
    virtual IRemoteWriteActivity *queryIsWriteActivity()
    {
        return this;
    }
};


class CRemoteDiskWriteActivity : public CRemoteWriteBaseActivity
{
    typedef CRemoteWriteBaseActivity PARENT;

    unsigned compressionFormat = COMPRESS_METHOD_NONE;
    bool eogPending = false;
    bool someInGroup = false;
    size32_t recordSize = 0;
    Owned<IFileIOStream> iFileIOStream;
    bool append = false;

    void checkOpen()
    {
        if (opened)
            return;

        if (!recursiveCreateDirectoryForFile(fileName))
            throw createDafsExceptionV(DAFSERR_cmdstream_openfailure, "Failed to create dirtory for file: '%s'", fileName.get());
        OwnedIFile iFile = createIFile(fileName);
        assertex(iFile);

        /* NB: if concurrent writers were supported, then would need mutex here, during open/create
         * multiple activities, each with there own handle would be possible, with mutex during write.
         * Would need mutex per physical filename active.
         */
        if (compressionFormat)
            iFileIO.setown(createCompressedFileWriter(iFile, recordSize, append, true, nullptr, compressionFormat));
        else
        {
            iFileIO.setown(iFile->open(append ? IFOwrite : IFOcreate));
            if (!iFileIO)
                throw createDafsExceptionV(DAFSERR_cmdstream_openfailure, "Failed to open: '%s' for write", fileName.get());
        }

        iFileIOStream.setown(createIOStream(iFileIO));
        if (append)
            iFileIOStream->seek(0, IFSend);
        opened = true;
        eofSeen = false;
    }
public:
    CRemoteDiskWriteActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc)
    {
        const char *compressed = config.queryProp("compressed"); // the compression format for the serialized rows in the transport
        if (!isEmptyString(compressed))
        {
            // boolean or format allowed
            if (strieq("true", compressed))
                compressionFormat = translateToCompMethod(nullptr); // gets default
            else if (strieq("false", compressed))
                compressionFormat = COMPRESS_METHOD_NONE;
            else
                compressionFormat = translateToCompMethod(compressed);
        }
        append = config.getPropBool("append");
    }
    virtual void write(size32_t sz, const void *rowData) override
    {
        checkOpen();
        iFileIOStream->write(sz, rowData);
        bytesWritten += sz;
    }
    virtual void serializeCursor(MemoryBuffer &tgt) const override
    {
        tgt.append(iFileIOStream->tell());
    }
    virtual void restoreCursor(MemoryBuffer &src) override
    {
        offset_t pos;
        src.read(pos);
        checkOpen();
        iFileIOStream->seek(pos, IFSbegin);
    }
};


// create a { unsigned8 } output meta for the count
static const RtlIntTypeInfo indexCountFieldType(type_unsigned|type_int, 8);
static const RtlFieldStrInfo indexCountField("count", nullptr, &indexCountFieldType);
static const RtlFieldInfo * const indexCountFields[2] = { &indexCountField, nullptr };
static const RtlRecordTypeInfo indexCountRecord(type_record, 2, indexCountFields);

class CRemoteIndexCountActivity : public CRemoteIndexBaseActivity
{
    typedef CRemoteIndexBaseActivity PARENT;

    unsigned __int64 rowLimit = 0;

public:
    CRemoteIndexCountActivity(IPropertyTree &config, IFileDescriptor *fileDesc) : PARENT(config, fileDesc)
    {
        rowLimit = config.getPropInt64("chooseN");

        outMeta.setown(new CDynamicOutputMetaData(indexCountRecord));
    }
// IRemoteReadActivity impl.
    virtual const void *nextRow(MemoryBufferBuilder &outBuilder, size32_t &retSz) override
    {
        if (eofSeen)
        {
            retSz = 0;
            return nullptr;
        }
        checkOpen();
        unsigned __int64 count = 0;
        if (!eofSeen)
        {
            if (rowLimit)
                count = keyManager->checkCount(rowLimit);
            else
                count = keyManager->getCount();
        }
        void *tgt = outBuilder.ensureCapacity(sizeof(count), "count");
        const void *ret = outBuilder.getSelf();
        memcpy(tgt, &count, sizeof(count));
        outBuilder.finishRow(sizeof(count));
        close();
        return ret;
    }
    virtual StringBuffer &getInfoStr(StringBuffer &out) const override
    {
        return out.appendf("indexcount[%s]", fileName.get());
    }
};


void checkExpiryTime(IPropertyTree &metaInfo)
{
    const char *expiryTime = metaInfo.queryProp("expiryTime");
    if (isEmptyString(expiryTime))
        throw createDafsException(DAFSERR_cmdstream_invalidexpiry, "createRemoteActivity: invalid expiry specification");

    CDateTime expiryTimeDt;
    try
    {
        expiryTimeDt.setString(expiryTime);
    }
    catch (IException *e)
    {
        e->Release();
        throw createDafsException(DAFSERR_cmdstream_invalidexpiry, "createRemoteActivity: invalid expiry specification");
    }
    CDateTime nowDt;
    nowDt.setNow();
    if (nowDt >= expiryTimeDt)
        throw createDafsException(DAFSERR_cmdstream_authexpired, "createRemoteActivity: authorization expired");
}

IFileDescriptor *verifyMetaInfo(IPropertyTree &actNode, bool authorizedOnly, const IPropertyTree *keyPairInfo)
{
    if (!authorizedOnly) // if configured false, allows unencrypted meta info
    {
        if (actNode.hasProp("fileName"))
            return nullptr;
    }
    StringBuffer metaInfoB64;
    actNode.getProp("metaInfo", metaInfoB64);
    if (0 == metaInfoB64.length())
        throw createDafsException(DAFSERR_cmdstream_protocol_failure, "createRemoteActivity: missing metaInfo");

    MemoryBuffer compressedMetaInfoMb;
    JBASE64_Decode(metaInfoB64.str(), compressedMetaInfoMb);
    MemoryBuffer decompressedMetaInfoMb;
    fastLZDecompressToBuffer(decompressedMetaInfoMb, compressedMetaInfoMb);
    Owned<IPropertyTree> metaInfoEnvelope = createPTree(decompressedMetaInfoMb);

    Owned<IPropertyTree> metaInfo;
#if defined(_USE_OPENSSL)
    MemoryBuffer metaInfoBlob;
    metaInfoEnvelope->getPropBin("metaInfoBlob", metaInfoBlob);

    bool isSigned = metaInfoBlob.length() != 0;
    if (authorizedOnly && !isSigned)
    {
        const char *fileGroup = metaInfoEnvelope->queryProp("group");
        if (!fileGroup)
            fileGroup = "(undefined)";
        throw createDafsExceptionV(DAFSERR_cmd_unauthorized, "createRemoteActivity: unauthorized (file group=%s)", fileGroup);
    }

    if (isSigned)
    {
        metaInfo.setown(createPTree(metaInfoBlob));
        const char *keyPairName = metaInfo->queryProp("keyPairName");
        dbgassertex(keyPairName); // because it's signed cannot be missing
        const char *fileGroup = metaInfo->queryProp("group");
        if (!fileGroup) // conceivably an older esp service constructed this metainfo without this field
            fileGroup = "(undefined)";
        StringBuffer metaInfoSignature;
        if (!metaInfoEnvelope->getProp("signature", metaInfoSignature)) // should not be possible (metaInfoBlob and signature are set at same time)
            throw createDafsExceptionV(DAFSERR_cmd_unauthorized, "createRemoteActivity: missing signature (file group=%s, keyPairName=%s)", fileGroup, keyPairName);

#ifdef _CONTAINERIZED
        /* This public key that is sent with request will be verified as being issued by same CA
         * and used to verify the meta info signature.
         */
        const char *certificate = metaInfoEnvelope->queryProp("certificate");
        if (isEmptyString(certificate))
            throw createDafsExceptionV(DAFSERR_cmd_unauthorized, "createRemoteActivity: missing certificate (file group=%s, keyPairName=%s)", fileGroup, keyPairName);
        Owned<CLoadedKey> publicKey = loadPublicKeyFromCertMemory(certificate);
#else
        VStringBuffer keyPairPath("KeyPair[@name=\"%s\"]", keyPairName);
        IPropertyTree *keyPair = keyPairInfo->queryPropTree(keyPairPath);
        if (!keyPair)
            throw createDafsExceptionV(DAFSERR_cmd_unauthorized, "createRemoteActivity: missing key pair definition (file group=%s, keyPairName=%s)", fileGroup, keyPairName);
        const char *publicKeyFName = keyPair->queryProp("@publicKey");
        if (isEmptyString(publicKeyFName))
            throw createDafsExceptionV(DAFSERR_cmd_unauthorized, "createRemoteActivity: missing public key definition (file group=%s, keyPairName=%s)", fileGroup, keyPairName);
        Owned<CLoadedKey> publicKey = loadPublicKeyFromFile(publicKeyFName); // NB: if cared could cache loaded keys
#endif
        if (!digiVerify(metaInfoSignature, metaInfoBlob.length(), metaInfoBlob.bytes(), *publicKey))
            throw createDafsExceptionV(DAFSERR_cmd_unauthorized, "createRemoteActivity: signature verification failed (file group=%s, keyPairName=%s)", fileGroup, keyPairName);
        checkExpiryTime(*metaInfo);
    }
    else
#endif
        metaInfo.set(metaInfoEnvelope);

    assertex(actNode.hasProp("filePart"));
    unsigned partNum = actNode.getPropInt("filePart");
    assertex(partNum);
    unsigned partCopy = actNode.getPropInt("filePartCopy", 1);

    Owned<IFileDescriptor> fileDesc;
    unsigned metaInfoVersion = metaInfo->getPropInt("version");
    switch (metaInfoVersion)
    {
        case 0:
            // implies unsigned direct request from engines (on unsecure port)
            // fall through
        case 1: // legacy
        {
            IPropertyTree *fileInfo = metaInfo->queryPropTree("FileInfo");
            assertex(fileInfo);

            VStringBuffer xpath("Part[%u]/Copy[%u]/@filePath", partNum, partCopy);
            StringBuffer partFileName;
            fileInfo->getProp(xpath, partFileName);
            if (!partFileName.length())
                throw createDafsException(DAFSERR_cmdstream_protocol_failure, "createRemoteActivity: invalid file info");

            actNode.setProp("fileName", partFileName.str());
            break;
        }
        case 2: // serialized compact IFileDescriptor
        case 3: // same as v2 with additional 'group' meta info within metaInfo
        {
            IPropertyTree *fileInfo = metaInfo->queryPropTree("FileInfo");

            fileDesc.setown(deserializeFileDescriptorTree(fileInfo));

            RemoteFilename rfn;
            fileDesc->getFilename(partNum-1, partCopy-1, rfn);

            StringBuffer path;
            rfn.getLocalPath(path);

            actNode.setProp("fileName", path.str());
            break;
        }
        default:
            throw createDafsExceptionV(DAFSERR_cmdstream_protocol_failure, "createRemoteActivity: unsupported meta info version %u", metaInfoVersion);
    }

    verifyex(actNode.removeProp("metaInfo")); // no longer needed

    return fileDesc.getClear();
}

template<class ActivityClass> IRemoteReadActivity *createConditionalProjectingActivity(IPropertyTree &actNode, IFileDescriptor *fileDesc)
{
    Owned<IRemoteReadActivity> activity = new ActivityClass(actNode, fileDesc);
    if (activity->requiresPostProject())
        return new CRemoteCompoundReadProjectActivity(actNode, activity);
    else
        return activity.getClear();
}

IRemoteActivity *createRemoteActivity(IPropertyTree &actNode, bool authorizedOnly, const IPropertyTree *keyPairInfo)
{
    Owned<IFileDescriptor> fileDesc = verifyMetaInfo(actNode, authorizedOnly, keyPairInfo);

    const char *partFileName = actNode.queryProp("fileName");
    const char *kindStr = actNode.queryProp("kind");
    ThorActivityKind kind = TAKnone;
    if (kindStr)
    {
        if (strieq("diskread", kindStr))
            kind = TAKdiskread;
        if (strieq("csvread", kindStr))
            kind = TAKcsvread;
        else if (strieq("xmlread", kindStr))
            kind = TAKxmlread;
        else if (strieq("jsonread", kindStr))
            kind = TAKjsonread;
        else if (strieq("indexread", kindStr))
            kind = TAKindexread;
        else if (strieq("indexcount", kindStr))
            kind = TAKindexcount;
        else if (strieq("diskwrite", kindStr))
            kind = TAKdiskwrite;
        else if (strieq("indexwrite", kindStr))
            kind = TAKindexwrite;
        // else - auto-detect
    }

    Owned<IRemoteActivity> activity;
    switch (kind)
    {
        case TAKdiskread:
        {
            activity.setown(new CRemoteDiskReadActivity(actNode, fileDesc));
            break;
        }
        case TAKcsvread:
        {
            activity.setown(createConditionalProjectingActivity<CRemoteCsvReadActivity>(actNode, fileDesc));
            break;
        }
        case TAKxmlread:
        {
            activity.setown(createConditionalProjectingActivity<CRemoteXmlReadActivity>(actNode, fileDesc));
            break;
        }
        case TAKjsonread:
        {
            activity.setown(createConditionalProjectingActivity<CRemoteJsonReadActivity>(actNode, fileDesc));
            break;
        }
        case TAKindexread:
        {
            activity.setown(new CRemoteIndexReadActivity(actNode, fileDesc));
            break;
        }
        case TAKindexcount:
        {
            activity.setown(new CRemoteIndexCountActivity(actNode, fileDesc));
            break;
        }
        case TAKdiskwrite:
        {
            activity.setown(new CRemoteDiskWriteActivity(actNode, fileDesc));
            break;
        }
        default: // in absense of type, read is assumed and file format is auto-detected.
        {
            const char *action = actNode.queryProp("action");
            if (isIndexFile(partFileName))
            {
                if (!isEmptyString(action))
                {
                    if (streq("count", action))
                        activity.setown(new CRemoteIndexCountActivity(actNode, fileDesc));
                    else
                        throw createDafsExceptionV(DAFSERR_cmdstream_protocol_failure, "Unknown action '%s' on index '%s'", action, partFileName);
                }
                else
                    activity.setown(new CRemoteIndexReadActivity(actNode, fileDesc));
            }
            else
            {
                if (!isEmptyString(action))
                {
                    if (streq("count", action))
                        throw createDafsException(DAFSERR_cmdstream_protocol_failure, "Remote Disk Counts currently unsupported");
                    else
                        throw createDafsExceptionV(DAFSERR_cmdstream_protocol_failure, "Unknown action '%s' on flat file '%s'", action, partFileName);
                }
                else
                {
                    const char *kind = queryFileKind(fileDesc);
                    if (isEmptyString(kind) || (streq("flat", kind)))
                        activity.setown(new CRemoteDiskReadActivity(actNode, fileDesc));
                    else if (streq("csv", kind))
                        activity.setown(createConditionalProjectingActivity<CRemoteCsvReadActivity>(actNode, fileDesc));
                    else if (streq("xml", kind))
                        activity.setown(createConditionalProjectingActivity<CRemoteXmlReadActivity>(actNode, fileDesc));
                    else if (streq("json", kind))
                        activity.setown(createConditionalProjectingActivity<CRemoteJsonReadActivity>(actNode, fileDesc));
                    else
                        throw createDafsExceptionV(DAFSERR_cmdstream_protocol_failure, "Unknown file kind '%s'", kind);
                }
            }
            break;
        }
    }
    return activity.getClear();
}

IRemoteActivity *createOutputActivity(IPropertyTree &requestTree, bool authorizedOnly, const IPropertyTree *keyPairInfo)
{
    IPropertyTree *actNode = requestTree.queryPropTree("node");
    assertex(actNode);
    Owned<IRemoteActivity> activity = createRemoteActivity(*actNode, authorizedOnly, keyPairInfo);
    if (requestTree.hasProp("fetch"))
    {
        IRemoteReadActivity *readActivity = activity->queryIsReadActivity();
        if (!readActivity)
            throw createDafsExceptionV(DAFSERR_cmdstream_protocol_failure, "fpos fetching specified in non reading activity");
        return new CRemoteCompoundBatchFPosFetchActivity(requestTree, readActivity);
    }
    else
        return activity.getClear();
}

#define MAX_KEYDATA_SZ 0x10000

enum class FeatureSupport
{
    none     = 0x0,
    stream   = 0x1,
    directIO = 0x2,
    spray    = 0x4,
    all      = stream|directIO|spray
};
BITMASK_ENUM(FeatureSupport);

enum class CommandRetFlags
{
    none          = 0x0,
    testSocket    = 0x1,
    replyHandled  = 0x2
};
BITMASK_ENUM(CommandRetFlags);

class CRemoteFileServer : implements IRemoteFileServer, public CInterface
{
    class CThrottler;
    class CRemoteClientHandler : implements ISocketSelectNotify, public CInterface
    {
        bool calledByRowService;
        byte *msgWritePtr = nullptr;
    public:
        CRemoteFileServer *parent;
        Owned<ISocket> socket;
        StringAttr peerName;
        MemoryBuffer msg;
        size32_t left;
        bool gotSize = false;
        StructArrayOf<OpenFileInfo> openFiles;
        Owned<IDirectoryIterator> opendir;
        unsigned            lasttick, lastInactiveTick;
        std::atomic<unsigned> &globallasttick;
        unsigned            previdx;        // for debug


        IMPLEMENT_IINTERFACE;

        CRemoteClientHandler(CRemoteFileServer *_parent,ISocket *_socket,std::atomic<unsigned> &_globallasttick, bool _calledByRowService)
            : calledByRowService(_calledByRowService), socket(_socket), globallasttick(_globallasttick)
        {
            previdx = (unsigned)-1;
            StringBuffer peerBuf;
            char name[256];
            name[0] = 0;
            int port = socket->peer_name(name,sizeof(name)-1);
            if (port>=0)
            {
                peerBuf.append(name);
                if (port)
                    peerBuf.append(':').append(port);
                peerName.set(peerBuf);
            }
            else
            {
                /* There's a possibility the socket closed before got here, in which case, peer name is unavailable
                 * May potentially be unavailable for other reasons also.
                 * Must be set, as used in client stats HT.
                 * If socket closed, the handler will start up but notice closed and quit
                 */
                peerName.set("UNKNOWN PEER NAME");
            }
            {
                CriticalBlock block(ClientCountSect);
                if (++ClientCount>MaxClientCount)
                    MaxClientCount = ClientCount;
                if (TF_TRACE_CLIENT_CONN)
                {
                    StringBuffer s;
                    s.appendf("Connecting(%p) [%d,%d] to ",this,ClientCount,MaxClientCount);
                    s.append(peerName);
                    PROGLOG("%s", s.str());
                }
            }
            parent = _parent;
            left = 0;
            msg.setEndian(__BIG_ENDIAN);
            touch();
        }
        ~CRemoteClientHandler()
        {
            {
                CriticalBlock block(ClientCountSect);
                ClientCount--;
                if (TF_TRACE_CLIENT_CONN) {
                    PROGLOG("Disconnecting(%p) [%d,%d] ",this,ClientCount,MaxClientCount);
                }
            }
            ISocket *sock = socket.getClear();
            try {
                sock->Release();
            }
            catch (IException *e) {
                EXCLOG(e,"~CRemoteClientHandler");
                e->Release();
            }
        }
        bool isRowServiceClient() const { return calledByRowService; }
        bool notifySelected(ISocket *sock, unsigned selected)
        {
            if (TF_TRACE_FULL)
                PROGLOG("notifySelected(%p)",this);
            if (sock!=socket)
                WARNLOG("notifySelected - invalid socket passed");
            touch();
            try
            {
                while (true)
                {
                    if (!gotSize)
                    {
                        // left represents amount we have read of leading size32_t (normally expect to be read in 1 go)
                        if (0 == msg.length()) // 1st time
                            msgWritePtr = (byte *)msg.reserveTruncate(sizeof(size32_t));
                        size32_t szRead;
                        sock->read(msgWritePtr, 0, sizeof(size32_t)-left, szRead, WAIT_FOREVER, false);

                        left += szRead;
                        msgWritePtr += szRead;
                        if (left == sizeof(size32_t)) // if not, we exit, and rely on next notifySelected
                        {
                            gotSize = true;
                            msg.read(left);
                            msg.clear();
                            try
                            {
                                msgWritePtr = (byte *)msg.reserveTruncate(left);
                            }
                            catch (IException *e)
                            {
                                EXCLOG(e,"notifySelected(1)");
                                e->Release();
                                left = 0;
                                // if too big then suggest corrupted packet, try to consume
                                // JCSMORE this seems a bit pointless, and it used to only read last 'avail',
                                // which is not necessarily everything that was sent
                                char fbuf[1024];
                                while (true)
                                {
                                    try
                                    {
                                        size32_t szRead;
                                        sock->read(fbuf, 0, 1024, szRead, WAIT_FOREVER, true);
                                    }
                                    catch (IException *e)
                                    {
                                        EXCLOG(e,"notifySelected(2)");
                                        e->Release();
                                        break;
                                    }
                                }
                            }
                            if (0 == left)
                            {
                                gotSize = false;
                                msg.clear();
                                parent->onCloseSocket(this, 5);
                                return true;
                            }
                        }
                        else
                            break; // wait for rest via subsequent notifySelected's
                    }
                    bool gc = false;
                    if (gotSize) // left represents length of message remaining to receive
                    {
                        size32_t szRead;
                        gc = readtmsAllowClose(sock, msgWritePtr, 0, left, szRead, WAIT_FOREVER);
                        msgWritePtr += szRead;
                        left -= szRead;
                        if (0 == left) // NB: only ever here if original size was >0
                        {
                            gotSize = false; // reset for next packet
                            parent->handleCompleteMessage(this, msg); // consumes msg
                            if (gc)
                                THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
                        }
                        else
                        {
                            if (gc)
                                THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
                            break; // wait for rest via subsequent notifySelected's
                        }
                    }
                    else if (gc)
                        THROWJSOCKEXCEPTION(JSOCKERR_graceful_close);
                    // to be here, implies handled full message, loop around to see if more on the wire.
                    // will break out if nothing/partial.
                }
            }
            catch (IJSOCK_Exception *e)
            {
                if (JSOCKERR_graceful_close == e->errorCode())
                {
                    if (gotSize)
                        WARNLOG("notifySelected: Closing mid packet, %u remaining", left);
                }
                else
                    EXCLOG(e, "notifySelected(3)");
                e->Release();
                parent->onCloseSocket(this, 5);
                left = 0;
                gotSize = false;
                msg.clear();
            }
            return true;
        }

        void logPrevHandle()
        {
            if (previdx<openFiles.ordinality())
            {
                const OpenFileInfo &fileInfo = openFiles.item(previdx);
                PROGLOG("Previous handle(%d): %s", fileInfo.handle, fileInfo.filename->text.get());
            }
        }

        bool throttleCommand(MemoryBuffer &msg)
        {
            RemoteFileCommandType cmd = RFCunknown;
            Owned<IException> e;
            try
            {
                msg.read(cmd);
                parent->throttleCommand(cmd, msg, this);
                return true;
            }
            catch (IException *_e)
            {
                e.setown(_e);
            }
            /* processCommand() will handle most exception and replies,
             * but if throttleCommand fails before it gets that far, this will handle
             */
            MemoryBuffer reply;
            initSendBuffer(reply);
            unsigned err = (cmd == RFCopenIO) ? RFSERR_OpenFailed : 0;
            parent->formatException(reply, e, cmd, false, err, this);
            sendDaFsBuffer(socket, reply);
            return false;
        }

        void processCommand(RemoteFileCommandType cmd, MemoryBuffer &msg, CThrottler *throttler)
        {
            MemoryBuffer reply;
            CommandRetFlags cmdFlags = parent->processCommand(cmd, msg, initSendBuffer(reply), this, throttler);

            // some commands (i.e. RFCFtSlaveCmd), reply early, so should not reply again here.
            if (!hasMask(cmdFlags, CommandRetFlags::replyHandled))
                sendDaFsBuffer(socket, reply, hasMask(cmdFlags, CommandRetFlags::testSocket));
        }

        bool timedOut()
        {
            return (msTick()-lasttick)>CLIENT_TIMEOUT;
        }

        bool inactiveTimedOut()
        {
            unsigned ms = msTick();
            if ((ms-lastInactiveTick)>CLIENT_INACTIVEWARNING_TIMEOUT)
            {
                lastInactiveTick = ms;
                return true;
            }
            return false;
        }

        void touch()
        {
            lastInactiveTick = lasttick = msTick();
            globallasttick = lasttick;
        }

        const char *queryPeerName()
        {
            return peerName;
        }

        bool getInfo(StringBuffer &str)
        {
            str.append("client(");
            const char *name = queryPeerName();
            bool ok;
            if (name)
            {
                ok = true;
                str.append(name);
            }
            else
                ok = false;
            unsigned ms = msTick();
            str.appendf("): last touch %d ms ago (%d, %d)",ms-lasttick,lasttick,ms);
            ForEachItemIn(i, openFiles)
            {
                const OpenFileInfo &fileInfo = openFiles.item(i);
                str.appendf("\n  %d: ", fileInfo.handle);
                str.append(fileInfo.filename->text.get());
            }
            return ok;
        }
    };

    class CThrottleQueueItem : public CSimpleInterface
    {
    public:
        RemoteFileCommandType cmd;
        Linked<CRemoteClientHandler> client;
        MemoryBuffer msg;
        CCycleTimer timer;
        CThrottleQueueItem(RemoteFileCommandType _cmd, MemoryBuffer &_msg, CRemoteClientHandler *_client) : cmd(_cmd), client(_client)
        {
            msg.swapWith(_msg);
        }
    };

    class CThrottler
    {
        Semaphore sem;
        CriticalSection crit, configureCrit;
        StringAttr title;
        unsigned limit, delayMs, cpuThreshold, queueLimit;
        unsigned disabledLimit;
        unsigned __int64 totalThrottleDelay;
        CCycleTimer totalThrottleDelayTimer;
        QueueOf<CThrottleQueueItem, false> queue;
        unsigned statsIntervalSecs;

    public:
        CThrottler(const char *_title) : title(_title)
        {
            totalThrottleDelay = 0;
            limit = 0;
            delayMs = DEFAULT_STDCMD_THROTTLEDELAYMS;
            cpuThreshold = DEFAULT_STDCMD_THROTTLECPULIMIT;
            disabledLimit = 0;
            queueLimit = DEFAULT_STDCMD_THROTTLEQUEUELIMIT;
            statsIntervalSecs = DEFAULT_STDCMD_THROTTLECPULIMIT;
        }
        ~CThrottler()
        {
            for (;;)
            {
                Owned<CThrottleQueueItem> item = queue.dequeue();
                if (!item)
                    break;
            }
        }
        unsigned queryLimit() const { return limit; }
        unsigned queryDelayMs() const { return delayMs; };;
        unsigned queryCpuThreshold() const { return cpuThreshold; }
        unsigned queryQueueLimit() const { return queueLimit; }
        StringBuffer &getInfoSummary(StringBuffer &info)
        {
            info.appendf("Throttler(%s) - limit=%u, delayMs=%u, cpuThreshold=%u, queueLimit=%u", title.get(), limit, delayMs, cpuThreshold, queueLimit).newline();
            unsigned elapsedSecs = totalThrottleDelayTimer.elapsedMs()/1000;
            time_t simple;
            time(&simple);
            simple -= elapsedSecs;

            CDateTime dt;
            dt.set(simple);
            StringBuffer dateStr;
            dt.getTimeString(dateStr, true);
            info.appendf("Throttler(%s): statistics since %s", title.get(), dateStr.str()).newline();
            info.appendf("Total delay of %0.2f seconds", ((double)totalThrottleDelay)/1000).newline();
            info.appendf("Requests currently queued: %u", queue.ordinality());
            return info;
        }
        void getInfo(StringBuffer &info)
        {
            CriticalBlock b(crit);
            getInfoSummary(info).newline();
        }
        void configure(unsigned _limit, unsigned _delayMs, unsigned _cpuThreshold, unsigned _queueLimit)
        {
            if (_limit > THROTTLE_MAX_LIMIT || _delayMs > THROTTLE_MAX_DELAYMS || _cpuThreshold > THROTTLE_MAX_CPUTHRESHOLD || _queueLimit > THROTTLE_MAX_QUEUELIMIT)
                throw MakeStringException(0, "Throttler(%s), rejecting configure command: limit=%u (max permitted=%u), delayMs=%u (max permitted=%u), cpuThreshold=%u (max permitted=%u), queueLimit=%u (max permitted=%u)",
                                              title.str(), _limit, THROTTLE_MAX_LIMIT, _delayMs, THROTTLE_MAX_DELAYMS, _cpuThreshold,
                                              THROTTLE_MAX_CPUTHRESHOLD, _queueLimit, THROTTLE_MAX_QUEUELIMIT);
            CriticalBlock b(configureCrit);
            int delta = 0;
            if (_limit)
            {
                if (disabledLimit) // if transitioning from disabled to some throttling
                {
                    assertex(0 == limit);
                    delta = _limit - disabledLimit; // + or -
                    disabledLimit = 0;
                }
                else
                    delta = _limit - limit; // + or -
            }
            else if (0 == disabledLimit)
            {
                PROGLOG("Throttler(%s): disabled, previous limit: %u", title.get(), limit);
                /* disabling - set limit immediately to let all new transaction through.
                 * NB: the semaphore signals are not consumed in this case, because transactions could be waiting on it.
                 * Instead the existing 'limit' is kept in 'disabledLimit', so that if/when throttling is
                 * re-enabled, it is used as a basis for increasing or consuming the semaphore signal count.
                 */
                disabledLimit = limit;
                limit = 0;
            }
            if (delta > 0)
            {
                PROGLOG("Throttler(%s): Increasing limit from %u to %u", title.get(), limit, _limit);
                sem.signal(delta);
                limit = _limit;
                // NB: If throttling was off, this doesn't effect transactions in progress, i.e. will only throttle new transactions coming in.
            }
            else if (delta < 0)
            {
                PROGLOG("Throttler(%s): Reducing limit from %u to %u", title.get(), limit, _limit);
                // NB: This is not expected to take long
                CCycleTimer timer;
                while (delta < 0)
                {
                    if (sem.wait(1000))
                        ++delta;
                    else
                        PROGLOG("Throttler(%s): Waited %0.2f seconds so far for up to a maximum of %u (previous limit) transactions to complete, %u completed", title.get(), ((double)timer.elapsedMs())/1000, limit, -delta);
                }
                limit = _limit;
                // NB: doesn't include transactions in progress, i.e. will only throttle new transactions coming in.
            }
            if (_delayMs != delayMs)
            {
                PROGLOG("Throttler(%s): New delayMs=%u, previous: %u", title.get(), _delayMs, delayMs);
                delayMs = _delayMs;
            }
            if (_cpuThreshold != cpuThreshold)
            {
                PROGLOG("Throttler(%s): New cpuThreshold=%u, previous: %u", title.get(), _cpuThreshold, cpuThreshold);
                cpuThreshold = _cpuThreshold;
            }
            if (((unsigned)-1) != _queueLimit && _queueLimit != queueLimit)
            {
                PROGLOG("Throttler(%s): New queueLimit=%u%s, previous: %u", title.get(), _queueLimit, 0==_queueLimit?"(disabled)":"", queueLimit);
                queueLimit = _queueLimit;
            }
        }
        void setStatsInterval(unsigned _statsIntervalSecs)
        {
            if (_statsIntervalSecs != statsIntervalSecs)
            {
                PROGLOG("Throttler(%s): New statsIntervalSecs=%u, previous: %u", title.get(), _statsIntervalSecs, statsIntervalSecs);
                statsIntervalSecs = _statsIntervalSecs;
            }
        }
        void take(RemoteFileCommandType cmd) // cmd for info. only
        {
            for (;;)
            {
                if (sem.wait(delayMs))
                    return;
                PROGLOG("Throttler(%s): transaction delayed [cmd=%s]", title.get(), getRFCText(cmd));
            }
        }
        void release()
        {
            sem.signal();
        }
        StringBuffer &getStats(StringBuffer &stats, bool reset)
        {
            CriticalBlock b(crit);
            getInfoSummary(stats);
            if (reset)
            {
                totalThrottleDelayTimer.reset();
                totalThrottleDelay = 0;
            }
            return stats;
        }
        void addCommand(RemoteFileCommandType cmd, MemoryBuffer &msg, CRemoteClientHandler *client)
        {
            CCycleTimer timer;
            Owned<IException> exception;
            bool hadSem = true;
            if (!sem.wait(delayMs))
            {
                CriticalBlock b(crit);
                if (!sem.wait(0)) // check hasn't become available
                {
                    unsigned cpu = getLatestCPUUsage();
                    if (getLatestCPUUsage()<cpuThreshold)
                    {
                        /* Allow to proceed, despite hitting throttle limit because CPU < threshold
                         * NB: The overall number of threads is still capped by the thread pool.
                         */
                        unsigned ms = timer.elapsedMs();
                        totalThrottleDelay += ms;
                        PROGLOG("Throttler(%s): transaction delayed [cmd=%s] for : %u milliseconds, proceeding as cpu(%u)<throttleCPULimit(%u)", title.get(), getRFCText(cmd), ms, cpu, cpuThreshold);
                        hadSem = false;
                    }
                    else
                    {
                        if (queueLimit && queue.ordinality()>=queueLimit)
                            throw MakeStringException(0, "Throttler(%s), the maxiumum number of items are queued (%u), rejecting new command[%s]", title.str(), queue.ordinality(), getRFCText(cmd));
                        queue.enqueue(new CThrottleQueueItem(cmd, msg, client)); // NB: takes over ownership of 'client' from running thread
                        PROGLOG("Throttler(%s): transaction delayed [cmd=%s], queuing (%u queueud), [client=%p, sock=%u]", title.get(), getRFCText(cmd), queue.ordinality(), client, client->socket->OShandle());
                        return;
                    }
                }
            }

            /* Guarantee that sem is released.
             * Should normally release on clean exit when queue is empty.
             */
            struct ReleaseSem
            {
                Semaphore *sem;
                ReleaseSem(Semaphore *_sem) { sem = _sem; }
                ~ReleaseSem() { if (sem) sem->signal(); }
            } releaseSem(hadSem?&sem:NULL);

            /* Whilst holding on this throttle slot (i.e. before signalling semaphore back), process
             * queued items. NB: other threads that are finishing will do also.
             * Queued items are processed 1st, then the current request, then anything that was queued when handling current request
             * Throttle slot (semaphore) is only given back when no more to do.
             */
            Linked<CRemoteClientHandler> currentClient;
            MemoryBuffer currentMsg;
            unsigned ms;
            for (;;)
            {
                RemoteFileCommandType currentCmd;
                {
                    CriticalBlock b(crit);
                    Owned<CThrottleQueueItem> item = queue.dequeue();
                    if (item)
                    {
                        currentCmd = item->cmd;
                        currentClient.setown(item->client.getClear());
                        currentMsg.swapWith(item->msg);
                        ms = item->timer.elapsedMs();
                    }
                    else
                    {
                        if (NULL == client) // previously handled and queue empty
                        {
                            /* Commands are only queued if semaphore is exhaused (checked inside crit)
                             * so only signal the semaphore inside the crit, after checking if there are no queued items
                             */
                            if (hadSem)
                            {
                                releaseSem.sem = NULL;
                                sem.signal();
                            }
                            break;
                        }
                        currentCmd = cmd;
                        currentClient.set(client); // process current request after dealing with queue
                        currentMsg.swapWith(msg);
                        ms = timer.elapsedMs();
                        client = NULL;
                    }
                }
                if (ms >= 1000)
                {
                    if (ms>delayMs)
                        PROGLOG("Throttler(%s): transaction delayed [cmd=%s] for : %u seconds", title.get(), getRFCText(currentCmd), ms/1000);
                }
                {
                    CriticalBlock b(crit);
                    totalThrottleDelay += ms;
                }
                try
                {
                    currentClient->processCommand(currentCmd, currentMsg, this);
                }
                catch (IException *e)
                {
                    EXCLOG(e, "addCommand: processCommand failed");
                    e->Release();
                }
            }
        }
    };

    // temporarily release a throttler slot
    class CThrottleReleaseBlock
    {
        CThrottler &throttler;
        RemoteFileCommandType cmd;
    public:
        CThrottleReleaseBlock(CThrottler &_throttler, RemoteFileCommandType _cmd) : throttler(_throttler), cmd(_cmd)
        {
            throttler.release();
        }
        ~CThrottleReleaseBlock()
        {
            throttler.take(cmd);
        }
    };

    int                 lasthandle;
    CriticalSection     sect;
    Owned<ISocket>      acceptsock;
    Owned<ISocket>      securesock;
    Owned<ISocket>      rowServiceSock;
    Linked<IPropertyTree> componentConfig;
    FeatureSupport featureSupport = FeatureSupport::all; // NB: will be overridden in run()
#ifdef _WIN32
    unsigned retryOpenMs = 0;
#endif
    bool rowServiceOnStdPort = true; // should row service commands be processed on std. service port
    bool rowServiceSSL = false;

    Owned<ISocketSelectHandler> selecthandler;
    Owned<IThreadPool>  threads;    // for commands
    bool stopping;
    unsigned clientcounttick;   // is only touched/read by checkTimeout() that is not contended itself.
    unsigned closedclients;   // is only touched/read by checkTimeout() that is not contended itself.
    CAsyncCommandManager asyncCommandManager;
    CThrottler stdCmdThrottler, slowCmdThrottler;
    CClientStatsTable clientStatsTable;
    std::atomic<unsigned> globallasttick;
    unsigned targetActiveThreads;
    Linked<IPropertyTree> keyPairInfo;
    enum class StreamCmd:byte { NEWSTREAM, CONTINUE, CLOSE, VERSION };
    std::unordered_map<std::string, StreamCmd> streamCmdMap = {
        {"newstream", StreamCmd::NEWSTREAM}, {"continue", StreamCmd::CONTINUE},
        {"close", StreamCmd::CLOSE}, {"version", StreamCmd::VERSION}
    };

    class CHandleTracer
    {
        CTimeMon timer;
        CriticalSection crit;
        Owned<IFile> stdIOIFile;
        std::vector<Owned<IFileIO>> reservedHandles;
        unsigned handlesToReserve = 3; // need a few for pipe process to succeed

        void reserveHandles()
        {
            if (stdIOIFile)
            {
                for (unsigned r=0; r<handlesToReserve; r++)
                {
                    IFileIO *iFileIO = stdIOIFile->open(IFOread);
                    if (iFileIO)
                        reservedHandles.push_back(iFileIO);
                }
            }
        }
        void releaseHandles()
        {
            reservedHandles.clear();
        }
    public:
        CHandleTracer()
        {
            /* Reserve handles, so that when we run out, we hope to release them
             * and thereby have enough to use when reading current state.
             */
            stdIOIFile.setown(createIFile("stdout:"));
            timer.reset(0);
            reserveHandles();
        }
        void traceIfReady()
        {
            CriticalBlock b(crit);
            if (timer.timedout())
            {
                DBGLOG("Open handles:");
                releaseHandles();
                /* NB: can't guarantee that handles will be available after releaseHandles(), if other threads have allocated them.
                 * If printLsOf fails, mark timer to retry again on next event in shorter time period.
                 */
                if (!printLsOf())
                {
                    DBGLOG("Failed to run lsof");
                    timer.reset(1000); // next attempt in >=1 second
                }
                else
                    timer.reset(60*1000); // next trace in >=1 minute
                reserveHandles();
            }
        }
    } handleTracer;

    int getNextHandle()
    {
        // called in sect critical block
        for (;;) {
            if (lasthandle==INT_MAX)
                lasthandle = 1;
            else
                lasthandle++;
            unsigned idx1;
            unsigned idx2;
            if (!findHandle(lasthandle,idx1,idx2))
                return lasthandle;
        }
    }

    bool findHandle(int handle,unsigned &clientidx,unsigned &handleidx)
    {
        // called in sect critical block
        clientidx = (unsigned)-1;
        handleidx = (unsigned)-1;
        ForEachItemIn(i,clients) {
            CRemoteClientHandler &client = clients.item(i);
            ForEachItemIn(j, client.openFiles)
            {
                if (client.openFiles.item(j).handle==handle)
                {
                    handleidx = j;
                    clientidx = i;
                    return true;
                }
            }
        }
        return false;
    }

    unsigned readKeyData(IKeyManager *keyManager, unsigned maxRecs, MemoryBuffer &reply, bool &maxHit)
    {
        DelayedSizeMarker keyDataSzReturned(reply);
        unsigned numRecs = 0;
        maxHit = false;
        unsigned pos = reply.length();
        while (keyManager->lookup(true))
        {
            unsigned size = keyManager->queryRowSize();
            const byte *result = keyManager->queryKeyBuffer();
            reply.append(size);
            reply.append(size, result);
            ++numRecs;
            if (maxRecs && (0 == --maxRecs))
            {
                maxHit = true;
                break;
            }
            if (reply.length()-pos >= MAX_KEYDATA_SZ)
            {
                maxHit = true;
                break;
            }
        }
        keyDataSzReturned.write();
        return numRecs;
    }

    class cCommandProcessor: public CInterface, implements IPooledThread
    {
        Owned<CRemoteClientHandler> client;
        MemoryBuffer msg;

    public:
        IMPLEMENT_IINTERFACE;

        struct cCommandProcessorParams
        {
            cCommandProcessorParams() { msg.setEndian(__BIG_ENDIAN); }
            CRemoteClientHandler *client;
            MemoryBuffer msg;
        };

        virtual void init(void *_params) override
        {
            cCommandProcessorParams &params = *(cCommandProcessorParams *)_params;
            client.set(params.client);
            msg.swapWith(params.msg);
        }

        virtual void threadmain() override
        {
            // idea is that initially we process commands inline then pass over to select handler
            try
            {
                client->throttleCommand(msg);
            }
            catch (IException *e)
            {
                // suppress some errors
                EXCLOG(e,"cCommandProcessor::threadmain");
                e->Release();
            }
            try
            {
                client.clear();
            }
            catch (IException *e)
            {
                // suppress some more errors clearing client
                EXCLOG(e,"cCommandProcessor::threadmain(2)");
                e->Release();
            }
        }
        virtual bool stop() override
        {
            return true;
        }
        virtual bool canReuse() const override
        {
            return false; // want to free owned socket
        }
    };

    IArrayOf<CRemoteClientHandler> clients;

#ifndef _CONTAINERIZED
    void validateSSLSetup()
    {
        if (!securitySettings.certificate)
            throw createDafsException(DAFSERR_serverinit_failed, "SSL Certificate information not found in environment.conf");
        if (!checkFileExists(securitySettings.certificate))
            throw createDafsException(DAFSERR_serverinit_failed, "SSL Certificate File not found in environment.conf");
        if (!securitySettings.privateKey)
            throw createDafsException(DAFSERR_serverinit_failed, "SSL Key information not found in environment.conf");
        if (!checkFileExists(securitySettings.privateKey))
            throw createDafsException(DAFSERR_serverinit_failed, "SSL Key File not found in environment.conf");
    }
#endif
public:

    IMPLEMENT_IINTERFACE

    CRemoteFileServer(unsigned maxThreads, unsigned maxThreadsDelayMs, unsigned maxAsyncCopy, IPropertyTree *_keyPairInfo)
        : asyncCommandManager(maxAsyncCopy), stdCmdThrottler("stdCmdThrottler"), slowCmdThrottler("slowCmdThrottler"), keyPairInfo(_keyPairInfo)
    {
        lasthandle = 0;
        selecthandler.setown(createSocketSelectHandler(NULL));

        stdCmdThrottler.configure(DEFAULT_STDCMD_PARALLELREQUESTLIMIT, DEFAULT_STDCMD_THROTTLEDELAYMS, DEFAULT_STDCMD_THROTTLECPULIMIT, DEFAULT_STDCMD_THROTTLEQUEUELIMIT);
        slowCmdThrottler.configure(DEFAULT_SLOWCMD_PARALLELREQUESTLIMIT, DEFAULT_SLOWCMD_THROTTLEDELAYMS, DEFAULT_SLOWCMD_THROTTLECPULIMIT, DEFAULT_SLOWCMD_THROTTLEQUEUELIMIT);

        unsigned targetMinThreads=maxThreads*20/100; // 20%
        if (0 == targetMinThreads) targetMinThreads = 1;
        targetActiveThreads=maxThreads*80/100; // 80%
        if (0 == targetActiveThreads) targetActiveThreads = 1;

        class CCommandFactory : public CSimpleInterfaceOf<IThreadFactory>
        {
            CRemoteFileServer &parent;
        public:
            CCommandFactory(CRemoteFileServer &_parent) : parent(_parent) { }
            virtual IPooledThread *createNew()
            {
                return parent.createCommandProcessor();
            }
        };
        Owned<IThreadFactory> factory = new CCommandFactory(*this); // NB: pool links factory, so takes ownership
        threads.setown(createThreadPool("CRemoteFileServerPool", factory, false, nullptr, maxThreads, maxThreadsDelayMs,
#ifdef __64BIT__
            0, // Unlimited stack size
#else
            0x10000,
#endif
        INFINITE,targetMinThreads));
        threads->setStartDelayTracing(60); // trace amount delayed every minute.

        PROGLOG("CRemoteFileServer: maxThreads = %u, maxThreadsDelayMs = %u, maxAsyncCopy = %u", maxThreads, maxThreadsDelayMs, maxAsyncCopy);

        stopping = false;
        clientcounttick = msTick();
        closedclients = 0;
        globallasttick = msTick();
    }

    ~CRemoteFileServer()
    {
#ifdef _DEBUG
        PROGLOG("Exiting CRemoteFileServer");
#endif
        asyncCommandManager.join();
        clients.kill();
#ifdef _DEBUG
        PROGLOG("Exited CRemoteFileServer");
#endif
    }
    bool lookupFileIOHandle(int handle, OpenFileInfo &fileInfo, unsigned newFlags=0)
    {
        if (handle<=0)
            return false;
        CriticalBlock block(sect);
        unsigned clientidx;
        unsigned handleidx;
        if (!findHandle(handle,clientidx,handleidx))
            return false;
        CRemoteClientHandler &client = clients.item(clientidx);
        OpenFileInfo &openFileInfo = client.openFiles.element(handleidx); // NB: links members
        openFileInfo.flags |= newFlags;
        fileInfo = openFileInfo;
        client.previdx = handleidx;
        return true;
    }

    //MORE: The file handles should timeout after a while, and accessing an old (invalid handle)
    // should throw a different exception
    bool checkFileIOHandle(int handle, Owned<IFileIO> & fileio, bool del=false)
    {
        fileio.clear();
        if (handle<=0)
            return false;
        CriticalBlock block(sect);
        unsigned clientidx;
        unsigned handleidx;
        if (findHandle(handle,clientidx,handleidx))
        {
            CRemoteClientHandler &client = clients.item(clientidx);
            const OpenFileInfo &fileInfo = client.openFiles.item(handleidx);
            if (del)
            {
                if (fileInfo.flags & of_key)
                    clearKeyStoreCacheEntry(fileInfo.fileIO);
                client.openFiles.remove(handleidx);
                client.previdx = (unsigned)-1;
            }
            else
            {
               fileio.set(client.openFiles.item(handleidx).fileIO);
               client.previdx = handleidx;
            }
            return true;
        }
        return false;
    }

    void checkFileIOHandle(MemoryBuffer &reply, int handle, Owned<IFileIO> & fileio, bool del=false)
    {
        if (!checkFileIOHandle(handle, fileio, del))
            throw createDafsException(RFSERR_InvalidFileIOHandle, nullptr);
    }

    void onCloseSocket(CRemoteClientHandler *client, int which)
    {
        if (!client)
            return;
        CriticalBlock block(sect);
#ifdef _DEBUG
        StringBuffer s(client->queryPeerName());
        PROGLOG("onCloseSocket(%d) %s",which,s.str());
#endif
        if (client->socket)
        {
            try
            {
                /* JCSMORE - shouldn't this really be dependent on whether selecthandled=true
                 * It has not been added to the selecthandler
                 * Harmless, but wasteful if so.
                 */
                selecthandler->remove(client->socket);
            }
            catch (IException *e) {
                EXCLOG(e,"CRemoteFileServer::onCloseSocket.1");
                e->Release();
            }
        }
        try {
            clients.zap(*client);
        }
        catch (IException *e) {
            EXCLOG(e,"CRemoteFileServer::onCloseSocket.2");
            e->Release();
        }
    }


    bool cmdOpenFileIO(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
    {
        Owned<StringAttrItem> name = new StringAttrItem;
        byte mode;
        byte share;
        msg.read(name->text).read(mode).read(share);
        // also try to recv extra byte
        byte extra = 0;
        unsigned short sMode = IFUnone;
        unsigned short cFlags = IFUnone;
        if (msg.remaining() >= sizeof(byte))
        {
            msg.read(extra);
            // and then try to recv extra sMode, cFlags (always sent together)
            if (msg.remaining() >= (sizeof(sMode) + sizeof(cFlags)))
                msg.read(sMode).read(cFlags);
        }
        IFEflags extraFlags = (IFEflags)extra;
        // none => nocache for remote (hint)
        // can change this default setting with:
        //  bare-metal legacy - conf file setting: allow_pgcache_flush=false
        //  bare-metal - environment.xml dafilesrv expert setting: disableIFileMask=0x1 (IFEnocache)
        //  containerized - values.yaml dafilesrv expert setting: disableIFileMask: 0x1 (IFEnocache)
        if (extraFlags == IFEnone)
            extraFlags = IFEnocache;
        Owned<IFile> file = createIFile(name->text);
        switch ((compatIFSHmode)share) {
        case compatIFSHnone:
            if (mode != IFOread)
                file->setCreateFlags(S_IRUSR|S_IWUSR);
            file->setShareMode(IFSHnone);
            break;
        case compatIFSHread:
            file->setShareMode(IFSHread);
            break;
        case compatIFSHwrite:
            file->setShareMode(IFSHfull);
            break;
        case compatIFSHexec:
            if (mode != IFOread)
                file->setCreateFlags(S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
            break;
        case compatIFSHall:
            if (mode != IFOread)
                file->setCreateFlags(S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH); // bit excessive
            file->setShareMode(IFSHfull);
            break;
        }
        // use sMode, cFlags if sent
        if (sMode != IFUnone && cFlags != IFUnone)
        {
            file->setCreateFlags(cFlags);
            file->setShareMode((IFSHmode)sMode);
        }
        if (TF_TRACE_PRE_IO)
            PROGLOG("before open file '%s', (%d,%d,%d,%d,0%o)",name->text.get(),(int)mode,(int)share,extraFlags,sMode,cFlags);

        Owned<IFileIO> fileio;
#ifndef _WIN32
        fileio.setown(file->open((IFOmode)mode,extraFlags));
#else
        // This is an attempt to deal this issue raised after MS update KB5025229 was applied.
        // retry to open if file in use by another process (MS process suspected to be holding file for a short time)
        unsigned retries = 0;
        CCycleTimer timer;
        if (0 == retryOpenMs) // disabled
            fileio.setown(file->open((IFOmode)mode,extraFlags));
        else
        {
            while (true)
            {
                unsigned elapsedMs = 0;
                try
                {
                    fileio.setown(file->open((IFOmode)mode,extraFlags));
                    break;
                }
                catch (IOSException *e)
                {
                    // abort unless "The process cannot access the file because it is being used by another process."
                    if (ERROR_SHARING_VIOLATION != e->errorCode())
                        throw;

                    elapsedMs = timer.elapsedMs();
                    if (elapsedMs >= retryOpenMs)
                    {
                        StringBuffer msg;
                        e->errorMessage(msg);
                        msg.appendf(" - retries = %u", retries);
                        e->Release();
                        throw makeOsException(ERROR_SHARING_VIOLATION, msg.str());
                    }
                }
                unsigned delayMs = 10 + (getRandom() % 90); // 10-100 ms
                unsigned remainingMs = retryOpenMs-elapsedMs;
                if (delayMs > remainingMs)
                    delayMs = remainingMs;
                MilliSleep(delayMs);
                ++retries;
            }
        }
#endif
        int handle;
        if (fileio)
        {
            CriticalBlock block(sect);
            handle = getNextHandle();
            client.previdx = client.openFiles.ordinality();
            client.openFiles.append(OpenFileInfo(handle, fileio, name));
        }
        else
            handle = 0;
        reply.append(RFEnoerror);
        reply.append(handle);
        if (TF_TRACE)
        {
#ifndef _WIN32
            PROGLOG("open file '%s', (%d,%d) handle = %d",name->text.get(),(int)mode,(int)share,handle);
#else
            PROGLOG("open file '%s', (%d,%d) handle = %d, retries = %u, time(ms) = %u",name->text.get(),(int)mode,(int)share,handle,retries,timer.elapsedMs());
#endif
        }
        return true;
    }

    bool cmdCloseFileIO(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        int handle;
        msg.read(handle);
        Owned<IFileIO> fileio;
        checkFileIOHandle(reply, handle, fileio, true);
        if (TF_TRACE)
            PROGLOG("close file, handle = %d",handle);
        reply.append(RFEnoerror);
        return true;
    }

    void cmdRead(MemoryBuffer & msg, MemoryBuffer & reply, CClientStats &stats)
    {
        int handle;
        __int64 pos;
        size32_t len;
        msg.read(handle).read(pos).read(len);
        Owned<IFileIO> fileio;
        checkFileIOHandle(reply, handle, fileio);

        //arrange it so we read directly into the reply buffer...
        unsigned posOfErr = reply.length();
        reply.append((unsigned)RFEnoerror);
        size32_t numRead;
        unsigned posOfLength = reply.length();
        if (TF_TRACE_PRE_IO)
            PROGLOG("before read file, handle = %d, toread = %d",handle,len);
        reply.reserve(sizeof(numRead));
        void *data = reply.reserve(len);
        numRead = fileio->read(pos,len,data);
        stats.addRead(len);
        if (TF_TRACE)
            PROGLOG("read file, handle = %d, pos = %" I64F "d, toread = %d, read = %d",handle,pos,len,numRead);
        reply.setLength(posOfLength + sizeof(numRead) + numRead);
        reply.writeEndianDirect(posOfLength,sizeof(numRead),&numRead);
    }

    void cmdSize(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        int handle;
        msg.read(handle);
        Owned<IFileIO> fileio;
        checkFileIOHandle(reply, handle, fileio);
        __int64 size = fileio->size();
        reply.append((unsigned)RFEnoerror).append(size);
        if (TF_TRACE)
            PROGLOG("size file, handle = %d, size = %" I64F "d",handle,size);
    }

    void cmdSetSize(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        int handle;
        offset_t size;
        msg.read(handle).read(size);
        if (TF_TRACE)
            PROGLOG("set size file, handle = %d, size = %" I64F "d",handle,size);
        Owned<IFileIO> fileio;
        checkFileIOHandle(reply, handle, fileio);
        fileio->setSize(size);
        reply.append((unsigned)RFEnoerror);
    }

    void cmdWrite(MemoryBuffer & msg, MemoryBuffer & reply, CClientStats &stats)
    {
        int handle;
        __int64 pos;
        size32_t len;
        msg.read(handle).read(pos).read(len);
        Owned<IFileIO> fileio;
        checkFileIOHandle(reply, handle, fileio);
        const byte *data = (const byte *)msg.readDirect(len);
        if (TF_TRACE_PRE_IO)
            PROGLOG("before write file, handle = %d, towrite = %d",handle,len);
        size32_t numWritten = fileio->write(pos,len,data);
        stats.addWrite(numWritten);
        if (TF_TRACE)
            PROGLOG("write file, handle = %d, towrite = %d, written = %d",handle,len,numWritten);
        reply.append((unsigned)RFEnoerror).append(numWritten);
    }

    void cmdExists(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("exists, '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        bool e = file->exists();
        reply.append((unsigned)RFEnoerror).append(e);
    }

    void cmdRemove(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("remove, '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        bool e = file->remove();
        reply.append((unsigned)RFEnoerror).append(e);
    }

    void cmdGetVer(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        if (TF_TRACE)
            PROGLOG("getVer");
        /* weird backward compatibility convention,
         * newer clients will send another unsigned to denote
         * and result in the numeric DAFILESRV_VERSION being returned
         * Ancient clients will get back the string form only (SERVER_VERSTRING)
         */
        if (msg.getPos()+sizeof(unsigned)>msg.length())
            reply.append((unsigned)RFEnoerror);
        else
            reply.append((unsigned)DAFILESRV_VERSION+0x10000);
        reply.append(DAFILESRV_VERSIONSTRING);
    }

    void cmdRename(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        StringAttr fromname;
        msg.read(fromname);
        StringAttr toname;
        msg.read(toname);
        if (TF_TRACE)
            PROGLOG("rename, '%s' to '%s'",fromname.get(),toname.get());
        Owned<IFile> file=createIFile(fromname);
        file->rename(toname);
        reply.append((unsigned)RFEnoerror);
    }

    void cmdMove(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        StringAttr fromname;
        msg.read(fromname);
        StringAttr toname;
        msg.read(toname);
        if (TF_TRACE)
            PROGLOG("move, '%s' to '%s'",fromname.get(),toname.get());
        Owned<IFile> file=createIFile(fromname);
        file->move(toname);
        reply.append((unsigned)RFEnoerror);
    }

    void cmdCopy(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
    {
        StringAttr fromname;
        msg.read(fromname);
        StringAttr toname;
        msg.read(toname);
        if (TF_TRACE)
            PROGLOG("copy, '%s' to '%s'",fromname.get(),toname.get());
        copyFile(toname, fromname);
        reply.append((unsigned)RFEnoerror);
    }

    void cmdAppend(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client, CClientStats &stats)
    {
        int handle;
        __int64 pos;
        __int64 len;
        StringAttr srcname;
        msg.read(handle).read(srcname).read(pos).read(len);
        Owned<IFileIO> fileio;
        checkFileIOHandle(reply, handle, fileio);

        Owned<IFile> file = createIFile(srcname.get());
        __int64 written = fileio->appendFile(file,pos,len);
        stats.addWrite(written);
        if (TF_TRACE)
            PROGLOG("append file, handle = %d, file=%s, pos = %" I64F "d len = %" I64F "d written = %" I64F "d",handle,srcname.get(),pos,len,written);
        reply.append((unsigned)RFEnoerror).append(written);
    }

    void cmdIsFile(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isFile, '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        unsigned ret = (unsigned)file->isFile();
        reply.append((unsigned)RFEnoerror).append(ret);
    }

    void cmdIsDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isDir, '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        unsigned ret = (unsigned)file->isDirectory();
        reply.append((unsigned)RFEnoerror).append(ret);
    }

    void cmdIsReadOnly(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isReadOnly, '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        unsigned ret = (unsigned)file->isReadOnly();
        reply.append((unsigned)RFEnoerror).append(ret);
    }

    void cmdSetReadOnly(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        bool set;
        msg.read(name).read(set);

        if (TF_TRACE)
            PROGLOG("setReadOnly, '%s' %d",name.get(),(int)set);
        Owned<IFile> file=createIFile(name);
        file->setReadOnly(set);
        reply.append((unsigned)RFEnoerror);
    }

    void cmdSetFilePerms(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        unsigned fPerms;
        msg.read(name).read(fPerms);
        if (TF_TRACE)
            PROGLOG("setFilePerms, '%s' 0%o",name.get(),fPerms);
        Owned<IFile> file=createIFile(name);
        file->setFilePermissions(fPerms);
        reply.append((unsigned)RFEnoerror);
    }

    void cmdGetTime(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("getTime, '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        CDateTime createTime;
        CDateTime modifiedTime;
        CDateTime accessedTime;
        bool ret = file->getTime(&createTime,&modifiedTime,&accessedTime);
        reply.append((unsigned)RFEnoerror).append(ret);
        if (ret)
        {
            createTime.serialize(reply);
            modifiedTime.serialize(reply);
            accessedTime.serialize(reply);
        }
    }

    void cmdSetTime(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        bool creategot;
        CDateTime createTime;
        bool modifiedgot;
        CDateTime modifiedTime;
        bool accessedgot;
        CDateTime accessedTime;
        msg.read(name);
        msg.read(creategot);
        if (creategot)
            createTime.deserialize(msg);
        msg.read(modifiedgot);
        if (modifiedgot)
            modifiedTime.deserialize(msg);
        msg.read(accessedgot);
        if (accessedgot)
            accessedTime.deserialize(msg);

        if (TF_TRACE)
            PROGLOG("setTime, '%s'",name.get());
        Owned<IFile> file=createIFile(name);

        bool ret = file->setTime(creategot?&createTime:NULL,modifiedgot?&modifiedTime:NULL,accessedgot?&accessedTime:NULL);
        reply.append((unsigned)RFEnoerror).append(ret);
    }

    void cmdCreateDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("CreateDir, '%s'",name.get());
        Owned<IFile> dir=createIFile(name);
        bool ret = dir->createDirectory();
        reply.append((unsigned)RFEnoerror).append(ret);
    }

    void cmdGetDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        StringAttr mask;
        bool includedir;
        bool sub;
        byte stream = 0;
        msg.read(name).read(mask).read(includedir).read(sub);
        if (msg.remaining()>=sizeof(byte))
        {
            msg.read(stream);
            if (stream==1)
                client.opendir.clear();
        }
        if (TF_TRACE)
            PROGLOG("GetDir, '%s', '%s', stream='%u'",name.get(),mask.get(),stream);
        if (!stream && !containsFileWildcard(mask))
        {
            // if no streaming, and mask contains no wildcard, it is much more efficient to get the info without a directory iterator!
            StringBuffer fullFilename(name);
            addPathSepChar(fullFilename).append(mask);
            Owned<IFile> iFile = createIFile(fullFilename);

            // NB: This must preserve same serialization format as CRemoteDirectoryIterator::serialize produces for <=1 file.
            reply.append((unsigned)RFEnoerror);
            if (!iFile->exists())
                reply.append((byte)0);
            else
            {
                byte b=1;
                reply.append(b);
                bool isDir = fileBool::foundYes == iFile->isDirectory();
                reply.append(isDir);
                reply.append(isDir ? 0 : iFile->size());
                CDateTime dt;
                iFile->getTime(nullptr, &dt, nullptr);
                dt.serialize(reply);
                reply.append(mask);
                b = 0;
                reply.append(b);
            }
        }
        else
        {
            Owned<IFile> dir=createIFile(name);

            Owned<IDirectoryIterator> iter;
            if (stream>1)
                iter.set(client.opendir);
            else
            {
                iter.setown(dir->directoryFiles(mask.length()?mask.get():NULL,sub,includedir));
                if (stream != 0)
                    client.opendir.set(iter);
            }
            if (!iter)
                throw createDafsException(RFSERR_GetDirFailed, nullptr);
            reply.append((unsigned)RFEnoerror);
            if (serializeRemoteDirectoryIterator(reply,iter,stream?0x100000:0,stream<2))
            {
                if (stream != 0)
                    client.opendir.clear();
            }
            else
            {
                bool cont=true;
                reply.append(cont);
            }
        }
    }

    void cmdMonitorDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        StringAttr mask;
        bool includedir;
        bool sub;
        unsigned checkinterval;
        unsigned timeout;
        __int64 cancelid; // not yet used
        msg.read(name).read(mask).read(includedir).read(sub).read(checkinterval).read(timeout).read(cancelid);
        byte isprev;
        msg.read(isprev);
        Owned<IDirectoryIterator> prev;
        if (isprev==1)
        {
            SocketEndpoint ep;
            prev.setown(createRemoteDirectorIterator(ep, name, msg));
        }
        if (TF_TRACE)
            PROGLOG("MonitorDir, '%s' '%s'",name.get(),mask.get());
        Owned<IFile> dir=createIFile(name);
        Owned<IDirectoryDifferenceIterator> iter=dir->monitorDirectory(prev,mask.length()?mask.get():NULL,sub,includedir,checkinterval,timeout);
        reply.append((unsigned)RFEnoerror);
        byte state = (iter.get()==NULL)?0:1;
        reply.append(state);
        if (state==1)
            serializeRemoteDirectoryDiff(reply, iter);
    }

    void cmdCopySection(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr uuid;
        StringAttr fromFile;
        StringAttr toFile;
        offset_t toOfs;
        offset_t fromOfs;
        offset_t size;
        offset_t sizeDone=0;
        offset_t totalSize=(offset_t)-1;
        unsigned timeout;
        msg.read(uuid).read(fromFile).read(toFile).read(toOfs).read(fromOfs).read(size).read(timeout);
        AsyncCommandStatus status = asyncCommandManager.copySection(uuid,fromFile,toFile,toOfs,fromOfs,size,sizeDone,totalSize,timeout);
        reply.append((unsigned)RFEnoerror).append((unsigned)status).append(sizeDone).append(totalSize);
    }

    static void treeCopyFile(RemoteFilename &srcfn, RemoteFilename &dstfn, const char *net, const char *mask, IpAddress &ip, bool usetmp, CThrottler *throttler, CFflags copyFlags=CFnone)
    {
        unsigned start = msTick();
        Owned<IFile> dstfile = createIFile(dstfn);
        // the following is really to check the dest node is up and working (otherwise not much point in continuing!)
        if (dstfile->exists())
            PROGLOG("TREECOPY overwriting '%s'",dstfile->queryFilename());
        Owned<IFile> srcfile = createIFile(srcfn);
        unsigned lastmin = 0;
        if (!srcfn.queryIP().ipequals(dstfn.queryIP())) {
            CriticalBlock block(treeCopyCrit);
            for (;;) {
                CDateTime dt;
                offset_t sz;
                try {
                    sz = srcfile->size();
                    if (sz==(offset_t)-1) {
                        if (TF_TRACE_TREE_COPY)
                            PROGLOG("TREECOPY source not found '%s'",srcfile->queryFilename());
                        break;
                    }
                    srcfile->getTime(NULL,&dt,NULL);
                }
                catch (IException *e) {
                    EXCLOG(e,"treeCopyFile(1)");
                    e->Release();
                    break;
                }
                Linked<CTreeCopyItem> tc;
                unsigned now = msTick();
                ForEachItemInRev(i1,treeCopyArray) {
                    CTreeCopyItem &item = treeCopyArray.item(i1);
                    // prune old entries (not strictly needed buf I think better)
                    if (now-item.lastused>TREECOPYPRUNETIME)
                        treeCopyArray.remove(i1);
                    else if (!tc.get()&&item.equals(srcfn,net,mask,sz,dt)) {
                        tc.set(&item);
                        item.lastused = now;
                    }
                }
                if (!tc.get()) {
                    if (treeCopyArray.ordinality()>=TREECOPY_CACHE_SIZE)
                        treeCopyArray.remove(0);
                    tc.setown(new CTreeCopyItem(srcfn,net,mask,sz,dt));
                    treeCopyArray.append(*tc.getLink());
                }
                ForEachItemInRev(cand,tc->loc) { // rev to choose copied locations first (maybe optional?)
                    if (!tc->busy->testSet(cand)) {
                        // check file accessible and matches
                        if (!cand&&dstfn.equals(tc->loc.item(cand)))  // hmm trying to overwrite existing, better humor
                            continue;
                        bool ok = true;
                        Owned<IFile> rmtfile = createIFile(tc->loc.item(cand));
                        if (cand) { // only need to check if remote
                            try {
                                if (rmtfile->size()!=sz)
                                    ok = false;
                                else {
                                    CDateTime fdt;
                                    rmtfile->getTime(NULL,&fdt,NULL);
                                    ok = fdt.equals(dt);
                                }
                            }
                            catch (IException *e) {
                                EXCLOG(e,"treeCopyFile(2)");
                                e->Release();
                                ok = false;
                            }
                        }
                        if (ok) { // if not ok leave 'busy'
                            // finally lets try and copy!
                            try {
                                if (TF_TRACE_TREE_COPY)
                                    PROGLOG("TREECOPY(started) %s to %s",rmtfile->queryFilename(),dstfile->queryFilename());
                                {
                                    CriticalUnblock unblock(treeCopyCrit); // note we have tc linked
                                    rmtfile->copyTo(dstfile,DEFAULT_COPY_BLKSIZE,NULL,usetmp,copyFlags);
                                }
                                if (TF_TRACE_TREE_COPY)
                                    PROGLOG("TREECOPY(done) %s to %s",rmtfile->queryFilename(),dstfile->queryFilename());
                                tc->busy->set(cand,false);
                                if (treeCopyWaiting)
                                    treeCopySem.signal((treeCopyWaiting>1)?2:1);
                                // add to known locations
                                tc->busy->set(tc->loc.ordinality(),false); // prob already is clear
                                tc->loc.append(dstfn);
                                ip.ipset(tc->loc.item(cand).queryIP());
                                return;
                            }
                            catch (IException *e) {
                                if (cand==0) {
                                    tc->busy->set(0,false); // don't leave busy
                                    if (treeCopyWaiting)
                                        treeCopySem.signal();
                                    throw;      // what more can we do!
                                }
                                EXCLOG(e,"treeCopyFile(3)");
                                e->Release();
                            }
                        }
                    }
                }
                // all locations busy
                if (msTick()-start>TREECOPYTIMEOUT) {
                    WARNLOG("Treecopy %s wait timed out", srcfile->queryFilename());
                    break;
                }
                treeCopyWaiting++; // note this isn't precise - just indication
                {
                    CriticalUnblock unblock(treeCopyCrit);
                    if (throttler)
                    {
                        CThrottleReleaseBlock block(*throttler, RFCtreecopy);
                        treeCopySem.wait(TREECOPYPOLLTIME);
                    }
                    else
                        treeCopySem.wait(TREECOPYPOLLTIME);
                }
                treeCopyWaiting--;
                if ((msTick()-start)/10*1000!=lastmin) {
                    lastmin = (msTick()-start)/10*1000;
                    PROGLOG("treeCopyFile delayed: %s to %s",srcfile->queryFilename(),dstfile->queryFilename());
                }
            }
        }
        else if (TF_TRACE_TREE_COPY)
            PROGLOG("TREECOPY source on same node as destination");
        if (TF_TRACE_TREE_COPY)
            PROGLOG("TREECOPY(started,fallback) %s to %s",srcfile->queryFilename(),dstfile->queryFilename());
        try {
            GetHostIp(ip);
            srcfile->copyTo(dstfile,DEFAULT_COPY_BLKSIZE,NULL,usetmp,copyFlags);
        }
        catch (IException *e) {
            EXCLOG(e,"TREECOPY(done,fallback)");
            throw;
        }
        if (TF_TRACE_TREE_COPY)
            PROGLOG("TREECOPY(done,fallback) %s to %s",srcfile->queryFilename(),dstfile->queryFilename());
    }

    void cmdTreeCopy(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client, CThrottler *throttler, bool usetmp=false)
    {
        RemoteFilename src;
        src.deserialize(msg);
        RemoteFilename dst;
        dst.deserialize(msg);
        StringAttr net;
        StringAttr mask;
        msg.read(net).read(mask);
        IpAddress ip;
        treeCopyFile(src,dst,net,mask,ip,usetmp,throttler);
        unsigned status = 0;
        reply.append((unsigned)RFEnoerror).append((unsigned)status);
        ip.ipserialize(reply);
    }

    void cmdTreeCopyTmp(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client, CThrottler *throttler)
    {
        cmdTreeCopy(msg, reply, client, throttler, true);
    }

    void cmdGetCRC(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("getCRC, '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        unsigned ret = file->getCRC();
        reply.append((unsigned)RFEnoerror).append(ret);
    }

    void cmdStop(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        PROGLOG("Abort request received");
        stopping = true;
        if (acceptsock)
            acceptsock->cancel_accept();
        if (securesock)
            securesock->cancel_accept();
        if (rowServiceSock)
            rowServiceSock->cancel_accept();
        reply.append((unsigned)RFEnoerror);
    }

    void cmdExec(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr cmdLine;
        msg.read(cmdLine);
        // NB: legacy remoteExec used to simply pass error code and buffer back to caller.
        VStringBuffer errMsg("Remote command execution no longer supported. Trying to execute cmdline=%s", cmdLine.get());
        WARNLOG("%s", errMsg.str());
        size32_t outSz = errMsg.length()+1; // reply with null terminated string
        // reply with error code -1
        reply.append((unsigned)-1).append((unsigned)0).append(outSz).append(outSz, errMsg.str());
    }

    void cmdSetTrace(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        byte flags;
        msg.read(flags);
        int retcode=-1;
        if (flags!=255)   // escape
        {
            retcode = traceFlags;
            traceFlags = flags;
        }
        reply.append(retcode);
    }

    void cmdGetInfo(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        unsigned level=1;
        if (msg.remaining() >= sizeof(unsigned))
            msg.read(level);
        StringBuffer retstr;
        getInfo(retstr, level);
        reply.append(RFEnoerror).append(retstr.str());
    }

    void cmdFirewall(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        // TBD
        StringBuffer retstr;
        getInfo(retstr);
        reply.append(RFEnoerror).append(retstr.str());
    }

    void cmdExtractBlobElements(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr prefix;
        StringAttr filename;
        msg.read(prefix).read(filename);
        RemoteFilename rfn;
        rfn.setLocalPath(filename);
        ExtractedBlobArray extracted;
        extractBlobElements(prefix, rfn, extracted);
        unsigned n = extracted.ordinality();
        reply.append((unsigned)RFEnoerror).append(n);
        for (unsigned i=0;i<n;i++)
            extracted.item(i).serialize(reply);
    }

    void cmdStreamGeneral(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client, CClientStats &stats)
    {
        size32_t jsonSz;
        msg.read(jsonSz);
        Owned<IPropertyTree> requestTree = createPTreeFromJSONString(jsonSz, (const char *)msg.readDirect(jsonSz));
        cmdStreamCommon(requestTree, msg, reply, client, stats);
    }

    /* Notes on protocol:
     *
     * A JSON request with these top-level fields:
     * "format" - the format of the reply. Supported formats = "binary", "xml", "json"
     * "handle" - the handle of for a file session that was previously open (for continuation)
     * "commCompression" - compression format of the communication protocol. Supports "LZ4", "LZW", "FLZ" (TBD: "ZLIB")
     * "replyLimit" - Number of K to limit each reply size to. (default 1024)
     * "node" - contains all 'activity' properties below:
     *
     * For a secured dafilesrv (streaming protocol), requests will only be accepted if the meta blob ("metaInfo") has a matching signature.
     * The request must specify "filePart" (1 based) to denote the partition # being read from or written to.
     *
     * "filePartCopy" (1 based) defaults to 1
     *
     * "kind" - supported kinds = "diskread", "diskwrite", "indexread", "indexcount" (TBD: "diskcount", "indexwrite", "disklookup")
     * NB: disk vs index will be auto detected if "kind" is absent.
     *
     * "action" - supported actions = "count" (used if "kind" is auto-detected to specify count should be performed instead of read)
     *
     * "keyFilter" - filter the results by this expression (See: HPCC-18474 for more details).
     *
     * "chooseN" - maximum # of results to return
     *
     * "compressed" - specifies whether input file is compressed. NB: not relevant to "index" types. Default = false. Auto-detected.
     *
     * "input" - specifies layout on disk of the file being read.
     *
     * "output" - where relavant, specifies the output format to be returned
     *
     * "fileName" is only used for unsecured non signed connections (normally forbidden), and specifies the fully qualified path to a physical file.
     *
     */
    void cmdStreamCommon(IPropertyTree *requestTree, MemoryBuffer &rest, MemoryBuffer &reply, CRemoteClientHandler &client, CClientStats &stats)
    {
        /* Example JSON request:
         *
         * {
         *  "format" : "binary",
         *  "command": "newstream"
         *  "replyLimit" : "64",
         *  "commCompression" : "LZ4",
         *  "node" : {
         *   "metaInfo" : "",
         *   "filePart" : 2,
         *   "filePartCopy" : 1,
         *   "kind" : "diskread",
         *   "fileName": "examplefilename",
         *   "keyFilter" : "f1='1    '",
         *   "chooseN" : 5,
         *   "compressed" : "false"
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   },
         *   "output" : {
         *    "f2" : "string",
         *    "f1" : "real"
         *   }
         *  }
         * }
         * OR
         * {
         *  "format" : "binary",
         *  "command": "newstream"
         *  "replyLimit" : "64",
         *  "commCompression" : "LZ4",
         *  "node" : {
         *   "kind" : "diskread",
         *   "fileName": "examplefilename",
         *   "keyFilter" : "f1='1    '",
         *   "chooseN" : 5,
         *   "compressed" : "false"
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   },
         *   "output" : {
         *    "f2" : "string",
         *    "f1" : "real"
         *   }
         *  }
         * }
         * OR
         * {
         *  "format" : "xml",
         *  "command": "newstream"
         *  "replyLimit" : "64",
         *  "node" : {
         *   "kind" : "diskread",
         *   "fileName": "examplefilename",
         *   "keyFilter" : "f1='1    '",
         *   "chooseN" : 5,
         *   "compressed" : "false"
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   },
         *   "output" : {
         *    "f2" : "string",
         *    "f1" : "real"
         *   }
         *  }
         * }
         * OR
         * {
         *  "format" : "xml",
         *  "command": "newstream"
         *  "node" : {
         *   "kind" : "indexread",
         *   "fileName": "examplefilename",
         *   "keyFilter" : "f1='1    '",
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   },
         *   "output" : {
         *    "f2" : "string",
         *    "f1" : "real"
         *   }
         *  }
         * OR
         * {
         *  "format" : "xml",
         *  "command": "newstream"
         *  "node" : {
         *   "kind" : "xmlread",
         *   "fileName": "examplefilename",
         *   "keyFilter" : "f1='1    '",
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   },
         *   "output" : {
         *    "f2" : "string",
         *    "f1" : "real"
         *   }
         *   "ActivityOptions" : { // usually not required, options here may override file meta info.
         *    "rowTag" : "/Dataset/OtherRow"
         *   }
         *  }
         * OR
         * {
         *  "format" : "xml",
         *  "command": "newstream"
         *  "node" : {
         *   "kind" : "csvread",
         *   "fileName": "examplefilename",
         *   "keyFilter" : "f1='1    '",
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   },
         *   "output" : {
         *    "f2" : "string",
         *    "f1" : "real"
         *   }
         *   "ActivityOptions" : { // usually not required, options here may override file meta info.
         *    "csvQuote" : "\"",
         *    "csvSeparate" : ","
         *    "csvTerminate" : "\\n,\\r\\n",
         *   }
         *  }
         * OR
         * {
         *  "format" : "xml",
         *  "command": "newstream"
         *  "node" : {
         *   "action" : "count",            // if present performs count with/without filter and returns count
         *   "fileName": "examplefilename", // can be either index or flat file
         *   "keyFilter" : "f1='1    '",
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   },
         *  }
         * }
         * OR
         * {
         *  "format" : "binary",
         *  "command": "newstream"
         *  "replyLimit" : "64",
         *  "commCompression" : "LZ4",
         *  "node" : {
         *   "kind" : "diskwrite",
         *   "fileName": "examplefilename",
         *   "compressed" : "false" (or "LZ4", "FLZ", "LZW")
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   }
         *  }
         * }
         * OR
         * {
         *  "format" : "binary",
         *  "command": "newstream"
         *  "replyLimit" : "64",
         *  "node" : {
         *   "kind" : "indexwrite",
         *   "fileName": "examplefilename",
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   }
         *  }
         * }
         *
         * Fetch fpos stream example:
         * {
         *  "format" : "binary",
         *  "command": "newstream"
         *  "replyLimit" : "64",
         *  "fetch" : {
         *   "fpos" : "30",
         *   "fpos" : "90"
         *  },
         *  "node" : {
         *   "kind" : "diskread",
         *   "fileName": "examplefilename",
         *   "input" : {
         *    "f1" : "string5",
         *    "f2" : "string5"
         *   }
         *  }
         * }
         *
         * fetch continuation:
         * {
         *  "format" : "binary",
         *  "command": "continue"
         *  "handle" : "1234",
         *  "replyLimit" : "64",
         *  "fetch" : {
         *   "fpos" : "120",
         *   "fpos" : "135",
         *   "fpos" : "150"
         *  }
         *
         * Close an open file:
         * {
         *  "format" : "binary",
         *  "command": "close"
         *  "handle" : "1234",
         * }
         */

        int cursorHandle = requestTree->getPropInt("handle");
        OutputFormat outputFormat = outFmt_Xml;
        Owned<ICompressor> compressor;
        Owned<IExpander> expander;
        Owned<CRemoteRequest> remoteRequest;
        Owned<IRemoteActivity> outputActivity;
        OpenFileInfo fileInfo;

        StreamCmd cmd;
        const char *qCommand = requestTree->queryProp("command");
        if (!qCommand)
        {
            // legacy handling
            // If cursor sent - meant continuation of existing stream, handle should correspond to existing request
            // No cursor - meant request would contain info to build a new stream
            if (cursorHandle)
                cmd = StreamCmd::CONTINUE;
            else
                cmd = StreamCmd::NEWSTREAM;
        }
        else
        {
            auto it = streamCmdMap.find(qCommand);
            if (it == streamCmdMap.end())
                throw makeStringExceptionV(0, "Unrecognised stream command: %s", qCommand);
            cmd = it->second;
        }

        const char *outputFmtStr = requestTree->queryProp("format");
        if (nullptr == outputFmtStr)
            outputFormat = outFmt_Xml; // default
        else if (strieq("binary", outputFmtStr))
            outputFormat = outFmt_Binary;
        else if (strieq("xml", outputFmtStr))
            outputFormat = outFmt_Xml;
        else if (strieq("json", outputFmtStr))
            outputFormat = outFmt_Json;
        else
            throw MakeStringException(0, "Unrecognised output format: %s", outputFmtStr);

        switch (cmd)
        {
            case StreamCmd::NEWSTREAM:
            {
                if (cursorHandle)
                    throw createDafsException(DAFSERR_cmdstream_protocol_failure, "unexpected cursor handle supplied to 'newstream' command");

                /* pre-version 2.4, "outputCompression" denoted data was compressed in communication protocol and only applied to reply row data
                * Since 2.5 "commCompression" replaces "outputCompression", and applies to both incoming row data (write) and outgoing row data (read).
                * But "outputCompression" is checked for backward compatibility.
                */
                if (requestTree->hasProp("outputCompression") || requestTree->hasProp("commCompression"))
                {
                    const char *commCompressionType = requestTree->queryProp("commCompression");
                    if (isEmptyString(commCompressionType))
                        commCompressionType = requestTree->queryProp("outputCompression");

                    if (isEmptyString(commCompressionType))
                    {
                        compressor.setown(queryDefaultCompressHandler()->getCompressor());
                        expander.setown(queryDefaultCompressHandler()->getExpander());
                    }
                    else if (outFmt_Binary == outputFormat)
                    {
                        compressor.setown(getCompressor(commCompressionType));
                        expander.setown(getExpander(commCompressionType));
                        if (!compressor)
                            WARNLOG("Unknown compressor type specified: %s", commCompressionType);
                    }
                    else
                        WARNLOG("Communication protocol compression not supported for format: %s", outputFmtStr);
                }

#ifdef _CONTAINERIZED
                bool authorizedOnly = hasMask(featureSupport, FeatureSupport::stream) && !hasMask(featureSupport, FeatureSupport::directIO);
#else
                /* NB: In bare-metal, unless client call is on dedicated service, allow non-authorized requests through, e.g. from engines talking to unsecured port
                * In a locked down secure setup, this service will be configured on a dedicated port, and the std. insecure dafilesrv will be unreachable.
                */
                bool authorizedOnly = rowServiceSock && client.isRowServiceClient();
#endif

                // In future this may be passed the request and build a chain of activities and return sink.
                outputActivity.setown(createOutputActivity(*requestTree, authorizedOnly, keyPairInfo));

                {
                    CriticalBlock block(sect);
                    cursorHandle = getNextHandle();
                }
                remoteRequest.setown(new CRemoteRequest(cursorHandle, outputFormat, compressor, expander, outputActivity));

                StringBuffer requestStr("jsonrequest:");
                outputActivity->getInfoStr(requestStr);
                Owned<StringAttrItem> name = new StringAttrItem(requestStr);

                CriticalBlock block(sect);
                client.previdx = client.openFiles.ordinality();
                client.openFiles.append(OpenFileInfo(cursorHandle, remoteRequest, name));

                remoteRequest->process(requestTree, rest, reply, stats);
                return;
            }
            case StreamCmd::CONTINUE:
            {
                if (0 == cursorHandle)
                    throw createDafsException(DAFSERR_cmdstream_protocol_failure, "cursor handle not supplied to 'continue' command");

                if (lookupFileIOHandle(cursorHandle, fileInfo)) // known handle, continuation
                {
                    remoteRequest.set(fileInfo.remoteRequest);
                    outputFormat = fileInfo.remoteRequest->queryFormat();

                    remoteRequest->process(requestTree, rest, reply, stats);
                    return;
                }

                cursorHandle = 0; // challenge response ..
                break;
            }
            case StreamCmd::CLOSE:
            {
                OwnedActiveSpanScope closeSpan;
                const char* traceParent = requestTree->queryProp("_trace/traceparent");
                if (traceParent != nullptr)
                {
                    Owned<IProperties> traceHeaders = createProperties();
                    traceHeaders->setProp("traceparent", traceParent);

                    closeSpan.setown(queryTraceManager().createServerSpan("CloseRequest", traceHeaders));
                }

                if (0 == cursorHandle)
                {
                    IDAFS_Exception* exception = createDafsException(DAFSERR_cmdstream_protocol_failure, "cursor handle not supplied to 'close' command");
                    if (closeSpan)
                        closeSpan->recordException(exception);
                    throw exception;
                }

                Owned<IFileIO> dummy;
                checkFileIOHandle(cursorHandle, dummy, true);
                break;
            }
            case StreamCmd::VERSION:
            {
                // handled in final response, see below
                break;
            }
            default: // should not get here, see streamCmdMap lookup before switch statement
                throwUnexpected();
        }

        Owned<IXmlWriterExt> responseWriter;
        if (outFmt_Binary == outputFormat)
            reply.append(cursorHandle);
        else // outFmt_Xml || outFmt_Json
        {
            responseWriter.setown(createIXmlWriterExt(0, 0, nullptr, outFmt_Xml == outputFormat ? WTStandard : WTJSONObject));
            responseWriter->outputBeginNested("Response", true);
            if (outFmt_Xml == outputFormat)
                responseWriter->outputCString("urn:hpcc:dfs", "@xmlns:dfs");
            responseWriter->outputUInt(cursorHandle, sizeof(cursorHandle), "handle");
        }
        switch (cmd)
        {
            case StreamCmd::VERSION:
            {
                OwnedActiveSpanScope versionSpan;
                const char* traceParent = requestTree->queryProp("_trace/traceparent");
                if (traceParent != nullptr)
                {
                    Owned<IProperties> traceHeaders = createProperties();
                    traceHeaders->setProp("traceparent", traceParent);

                    versionSpan.setown(queryTraceManager().createServerSpan("VersionRequest", traceHeaders));
                }

                if (outFmt_Binary == outputFormat)
                    reply.append(DAFILESRV_VERSIONSTRING);
                else
                    responseWriter->outputString(strlen(DAFILESRV_VERSIONSTRING), DAFILESRV_VERSIONSTRING, "version");
                break;
            }
            default:
                break;
        }
        if (outFmt_Binary != outputFormat)
        {
            responseWriter->outputEndNested("Response");
            responseWriter->finalize();
            reply.append(responseWriter->length(), responseWriter->str());
        }
    }

    void cmdStreamReadCommon(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client, CClientStats &stats)
    {
        size32_t jsonSz = msg.remaining();
        Owned<IPropertyTree> requestTree = createPTreeFromJSONString(jsonSz, (const char *)msg.readDirect(jsonSz));
        cmdStreamCommon(requestTree, msg, reply, client, stats);
    }


    // NB: JSON header to message, for some requests (e.g. write), there will be trailing raw data (e.g. row data)

    void cmdStreamReadStd(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client, CClientStats &stats)
    {
        reply.append(RFEnoerror); // gets patched if there is a follow on error
        cmdStreamReadCommon(msg, reply, client, stats);
    }

    void cmdStreamReadJSON(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client, CClientStats &stats)
    {
        /* NB: exactly the same handling as cmdStreamReadStd(RFCStreamRead) for now,
         * may want to differentiate later
         * i.e. return format is { len[unsigned4-bigendian], errorcode[unsigned4-bigendian], result } - where result format depends on request output type.
         * errorcode = 0 means no error
         */
        reply.append(RFEnoerror); // gets patched if there is a follow on error
        cmdStreamReadCommon(msg, reply, client, stats);
    }

    void cmdStreamReadTestSocket(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client, CClientStats &stats)
    {
        reply.append('J');
        cmdStreamReadCommon(msg, reply, client, stats);
    }

    // legacy version
    void cmdSetThrottle(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        unsigned limit, delayMs, cpuThreshold;
        msg.read(limit);
        msg.read(delayMs);
        msg.read(cpuThreshold);
        stdCmdThrottler.configure(limit, delayMs, cpuThreshold, (unsigned)-1);
        reply.append((unsigned)RFEnoerror);
    }

    void cmdSetThrottle2(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        unsigned throttleClass, limit, delayMs, cpuThreshold, queueLimit;
        msg.read(throttleClass);
        msg.read(limit);
        msg.read(delayMs);
        msg.read(cpuThreshold);
        msg.read(queueLimit);
        setThrottle((ThrottleClass)throttleClass, limit, delayMs, cpuThreshold, queueLimit);
        reply.append((unsigned)RFEnoerror);
    }

    void cmdFtSlaveCmd(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
    {
        byte action;
        msg.read(action);

        MemoryBuffer results;
        results.setEndian(__BIG_ENDIAN);
        Owned<IException> exception;
        bool ok=false;
        try
        {
            // NB: will run continuously and write progress back to client.socket
            ok = processFtCommand(action, client.socket, msg, results);
        }
        catch (IException *e)
        {
            EXCLOG(e);
            exception.setown(e);
        }
        msg.clear().append(true).append(ok);
        serializeException(exception, msg);
        msg.append(results);
        catchWriteBuffer(client.socket, msg);

        LOG(MCdebugProgress, "Results sent from slave: %s", client.peerName.str());
    }

    void formatException(MemoryBuffer &reply, IException *e, RemoteFileCommandType cmd, bool testSocketFlag, unsigned _dfsErrorCode, CRemoteClientHandler *client)
    {
        unsigned dfsErrorCode = _dfsErrorCode;
        if (!dfsErrorCode)
        {
            if (e)
                dfsErrorCode = (QUERYINTERFACE(e, IDAFS_Exception)) ? e->errorCode() : RFSERR_InternalError;
            else
                dfsErrorCode = RFSERR_InternalError;
        }
        VStringBuffer errMsg("ERROR: cmd=%s, error=%s", getRFCText(cmd), getRFSERRText(dfsErrorCode));
        if (e)
        {
            errMsg.appendf(" (%u, ", e->errorCode());
            unsigned len = errMsg.length();
            e->errorMessage(errMsg);
            if (len == errMsg.length())
                errMsg.setLength(len-2); // strip off ", " if no message in exception
            errMsg.append(")");
        }
        if (testSocketFlag)
            reply.append('-');
        else
            reply.append(dfsErrorCode);
        reply.append(errMsg.str());

        if (client && cmd!=RFCunlock)
        {
            const char *peer = client->queryPeerName();
            if (peer)
            {
                VStringBuffer err("%s. Client: %s", errMsg.str(), peer);
                PROGLOG("%s", err.str());
            }
            client->logPrevHandle();
        }
    }

    void throttleCommand(RemoteFileCommandType cmd, MemoryBuffer &msg, CRemoteClientHandler *client)
    {
        switch (cmd)
        {
            case RFCexec:
            case RFCgetcrc:
            case RFCcopy:
            case RFCappend:
            case RFCtreecopy:
            case RFCtreecopytmp:
            case RFCremove:
            case RFCcopysection:
            case RFCFtSlaveCmd:
                slowCmdThrottler.addCommand(cmd, msg, client);
                return;
            case RFCcloseIO:
            case RFCopenIO:
            case RFCread:
            case RFCsize:
            case RFCwrite:
            case RFCexists:
            case RFCrename:
            case RFCgetver:
            case RFCisfile:
            case RFCisdirectory:
            case RFCisreadonly:
            case RFCsetreadonly:
            case RFCsetfileperms:
            case RFCreadfilteredindex:
            case RFCreadfilteredindexcount:
            case RFCreadfilteredindexblob:
            case RFCgettime:
            case RFCsettime:
            case RFCcreatedir:
            case RFCgetdir:
            case RFCmonitordir:
            case RFCstop:
            case RFCextractblobelements:
            case RFCredeploy:
            case RFCmove:
            case RFCsetsize:
            case RFCsettrace:
            case RFCgetinfo:
            case RFCfirewall:
            case RFCStreamRead:
            case RFCStreamReadTestSocket:
            case RFCStreamReadJSON:
                stdCmdThrottler.addCommand(cmd, msg, client);
                return;
            // NB: The following commands are still bound by the the thread pool
            case RFCsetthrottle: // legacy version
            case RFCsetthrottle2:
            default:
            {
                client->processCommand(cmd, msg, NULL);
                break;
            }
        }
    }

    void checkAuthorizedStreamCommand(CRemoteClientHandler &client)
    {
        if (!rowServiceOnStdPort && !client.isRowServiceClient())
            throw createDafsException(DAFSERR_cmd_unauthorized, "Unauthorized command");
    }

    CommandRetFlags processCommand(RemoteFileCommandType cmd, MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler *client, CThrottler *throttler)
    {
        Owned<CClientStats> stats = clientStatsTable.getClientReference(cmd, client->queryPeerName());
        CommandRetFlags retFlags = CommandRetFlags::none;
        unsigned posOfErr = reply.length();
        try
        {
            FeatureSupport featureSupportCheck = featureSupport;

            /* isRowServiceClient only set for bare-metal clients
             * If set, only support streaming commands
             */
            if (client->isRowServiceClient())
                featureSupportCheck = FeatureSupport::stream;

            switch (cmd)
            {
                case RFCStreamGeneral:
                case RFCStreamRead:
                case RFCStreamReadJSON:
                    if (!hasMask(featureSupportCheck, FeatureSupport::stream))
                        throw createDafsException(DAFSERR_cmd_unauthorized, "Unauthorized command");
                    break;
                case RFCFtSlaveCmd:
                    if (!hasMask(featureSupportCheck, FeatureSupport::spray))
                        throw createDafsException(DAFSERR_cmd_unauthorized, "Unauthorized command");
                    break;
                default: // all other commands are considered 'directIO'
                    if (!hasMask(featureSupportCheck, FeatureSupport::directIO))
                        throw createDafsException(DAFSERR_cmd_unauthorized, "Unauthorized command");
                    break;
            }

            switch (cmd)
            {
                MAPCOMMANDSTATS(RFCread, cmdRead, *stats);
                MAPCOMMANDSTATS(RFCwrite, cmdWrite, *stats);
                MAPCOMMANDCLIENTSTATS(RFCappend, cmdAppend, *client, *stats);
                MAPCOMMAND(RFCcloseIO, cmdCloseFileIO);
                MAPCOMMANDCLIENT(RFCopenIO, cmdOpenFileIO, *client);
                MAPCOMMAND(RFCsize, cmdSize);
                MAPCOMMANDCLIENT(RFCexists, cmdExists, *client);
                MAPCOMMANDCLIENT(RFCremove, cmdRemove, *client);
                MAPCOMMANDCLIENT(RFCrename, cmdRename, *client);
                MAPCOMMAND(RFCgetver, cmdGetVer);
                MAPCOMMANDCLIENT(RFCisfile, cmdIsFile, *client);
                MAPCOMMANDCLIENT(RFCisdirectory, cmdIsDir, *client);
                MAPCOMMANDCLIENT(RFCisreadonly, cmdIsReadOnly, *client);
                MAPCOMMANDCLIENT(RFCsetreadonly, cmdSetReadOnly, *client);
                MAPCOMMANDCLIENT(RFCsetfileperms, cmdSetFilePerms, *client);
                MAPCOMMANDCLIENT(RFCgettime, cmdGetTime, *client);
                MAPCOMMANDCLIENT(RFCsettime, cmdSetTime, *client);
                MAPCOMMANDCLIENT(RFCcreatedir, cmdCreateDir, *client);
                MAPCOMMANDCLIENT(RFCgetdir, cmdGetDir, *client);
                MAPCOMMANDCLIENT(RFCmonitordir, cmdMonitorDir, *client);
                MAPCOMMAND(RFCstop, cmdStop);
                MAPCOMMANDCLIENT(RFCexec, cmdExec, *client);
                MAPCOMMANDCLIENT(RFCextractblobelements, cmdExtractBlobElements, *client);
                MAPCOMMANDCLIENT(RFCgetcrc, cmdGetCRC, *client);
                MAPCOMMANDCLIENT(RFCmove, cmdMove, *client);
                MAPCOMMANDCLIENT(RFCcopy, cmdCopy, *client);
                MAPCOMMAND(RFCsetsize, cmdSetSize);
                MAPCOMMAND(RFCsettrace, cmdSetTrace);
                MAPCOMMAND(RFCgetinfo, cmdGetInfo);
                MAPCOMMAND(RFCfirewall, cmdFirewall);
                MAPCOMMANDCLIENT(RFCcopysection, cmdCopySection, *client);
                MAPCOMMANDCLIENTTHROTTLE(RFCtreecopy, cmdTreeCopy, *client, &slowCmdThrottler);
                MAPCOMMANDCLIENTTHROTTLE(RFCtreecopytmp, cmdTreeCopyTmp, *client, &slowCmdThrottler);
                MAPCOMMAND(RFCsetthrottle, cmdSetThrottle); // legacy version
                MAPCOMMAND(RFCsetthrottle2, cmdSetThrottle2);
                case RFCStreamReadTestSocket:
                {
                    retFlags |= CommandRetFlags::testSocket;
                    checkAuthorizedStreamCommand(*client);
                    cmdStreamReadTestSocket(msg, reply, *client, *stats);
                    break;
                }
                // row service commands
                case RFCStreamGeneral:
                {
                    checkAuthorizedStreamCommand(*client);
                    reply.append(RFEnoerror); // gets patched if there is a follow on error
                    cmdStreamGeneral(msg, reply, *client, *stats);
                    break;
                }
                case RFCStreamRead:
                {
                    checkAuthorizedStreamCommand(*client);
                    cmdStreamReadStd(msg, reply, *client, *stats);
                    break;
                }
                case RFCStreamReadJSON:
                {
                    checkAuthorizedStreamCommand(*client);
                    cmdStreamReadJSON(msg, reply, *client, *stats);
                    break;
                }
                case RFCFtSlaveCmd:
                {
                    reply.append((unsigned)RFEnoerror);
                    sendDaFsBuffer(client->socket, reply, false);
                    // NB: command replied to.
                    // This command uses/takes over use of the socket for progress updates
                    retFlags |= CommandRetFlags::replyHandled;
                    cmdFtSlaveCmd(msg, reply, *client);
                    break;
                }
            default:
                formatException(reply, nullptr, cmd, false, RFSERR_InvalidCommand, client);
                break;
            }
        }
        catch (IException *e)
        {
            checkOutOfHandles(e);
            reply.setWritePos(posOfErr);
            formatException(reply, e, cmd, hasMask(retFlags, CommandRetFlags::testSocket), 0, client);
            e->Release();
        }
        return retFlags;
    }

    IPooledThread *createCommandProcessor()
    {
        return new cCommandProcessor();
    }

    void checkOutOfHandles(IException *exception)
    {
        if (EMFILE == exception->errorCode())
            handleTracer.traceIfReady();
    }

    virtual void run(IPropertyTree *componentConfig, DAFSConnectCfg _connectMethod, const SocketEndpoint &listenep, unsigned sslPort, unsigned listenQueueLimit, const SocketEndpoint *rowServiceEp, bool _rowServiceSSL, bool _rowServiceOnStdPort) override
    {
        SocketEndpoint sslep(listenep);
#ifndef _CONTAINERIZED
        if (sslPort)
            sslep.port = sslPort;
        else
            sslep.port = securitySettings.daFileSrvSSLPort;
#endif

        Owned<ISocket> acceptSock, secureSock, rowServiceSock;
        if (_connectMethod != SSLOnly)
        {
            if (listenep.port == 0)
                throw createDafsException(DAFSERR_serverinit_failed, "dafilesrv port not specified");

            if (listenep.isNull())
                acceptSock.setown(ISocket::create(listenep.port, listenQueueLimit));
            else
            {
                StringBuffer ips;
                listenep.getHostText(ips);
                acceptSock.setown(ISocket::create_ip(listenep.port, ips.str(), listenQueueLimit));
            }
        }

        if (_connectMethod != SSLNone)
        {
            if (sslep.port == 0)
                throw createDafsException(DAFSERR_serverinit_failed, "Secure dafilesrv port not specified");
#ifndef _CONTAINERIZED
            if (_connectMethod == UnsecureFirst)
            {
                // don't fail, but warn - this allows for fast SSL client rejections
                if (!securitySettings.certificate)
                    WARNLOG("SSL Certificate information not found in environment.conf, cannot accept SSL connections");
                else if ( !checkFileExists(securitySettings.certificate) )
                {
                    WARNLOG("SSL Certificate File not found in environment.conf, cannot accept SSL connections");
                    securitySettings.certificate = nullptr;
                }
                if (!securitySettings.privateKey)
                    WARNLOG("SSL Key information not found in environment.conf, cannot accept SSL connections");
                else if ( !checkFileExists(securitySettings.privateKey) )
                {
                    WARNLOG("SSL Key File not found in environment.conf, cannot accept SSL connections");
                    securitySettings.privateKey = nullptr;
                }
            }
            else if (!isContainerized() && getComponentConfigSP()->hasProp("cert"))
            {
                // validated when context is created in createSecureSocket
            }
            else // using environment.conf HPCCCertificateFile etc.
                validateSSLSetup();
#endif

            if (sslep.isNull())
                secureSock.setown(ISocket::create(sslep.port, listenQueueLimit));
            else
            {
                StringBuffer ips;
                sslep.getHostText(ips);
                secureSock.setown(ISocket::create_ip(sslep.port, ips.str(), listenQueueLimit));
            }
        }

        if (rowServiceEp)
        {
            rowServiceSSL = _rowServiceSSL;
            rowServiceOnStdPort = _rowServiceOnStdPort;

            if (rowServiceEp->isNull())
                rowServiceSock.setown(ISocket::create(rowServiceEp->port, listenQueueLimit));
            else
            {
                StringBuffer ips;
                rowServiceEp->getHostText(ips);
                rowServiceSock.setown(ISocket::create_ip(rowServiceEp->port, ips.str(), listenQueueLimit));
            }

#ifndef _CONTAINERIZED
# ifdef _USE_OPENSSL
            if (rowServiceSSL)
                validateSSLSetup();
# else
            rowServiceSSL = false;
# endif
#endif
        }

        run(componentConfig, _connectMethod, acceptSock.getClear(), secureSock.getClear(), rowServiceSock.getClear());
    }

    virtual void run(IPropertyTree *_componentConfig, DAFSConnectCfg _connectMethod, ISocket *_acceptSock, ISocket *_secureSock, ISocket *_rowServiceSock) override
    {
        acceptsock.setown(_acceptSock);
        securesock.setown(_secureSock);
        rowServiceSock.setown(_rowServiceSock);
        componentConfig.set(_componentConfig);
#ifdef _CONTAINERIZED
        if (componentConfig) // will be null in some scenarios (test cases)
        {
            // In K8s the application type determines what features this dafilesrv supports
            const char *appType = componentConfig->queryProp("@application");
            if (strsame("stream", appType))
                featureSupport = FeatureSupport::stream;
            else if (strsame("spray", appType))
                featureSupport = FeatureSupport::spray;
            else if (strsame("directio", appType))
                featureSupport = FeatureSupport::directIO|FeatureSupport::stream;
        }
#endif
        if (_connectMethod != SSLOnly)
        {
            if (!acceptsock)
                throw createDafsException(DAFSERR_serverinit_failed, "Invalid non-secure socket");
        }

        if (_connectMethod != SSLNone)
        {
            if (!securesock)
                throw createDafsException(DAFSERR_serverinit_failed, "Invalid secure socket");
        }

#ifdef _WIN32
        if (componentConfig)
        {
            constexpr unsigned defaultRetryOpenMs = 5000;
            retryOpenMs = componentConfig->getPropInt("@retryOpenMs", defaultRetryOpenMs);
        }
#endif

        selecthandler->start();

        unsigned timeLastLog = 0;
        unsigned numFailedConn = 0;
        Owned<IException> exception;
        for (;;)
        {
            Owned<ISocket> sock;
            Owned<ISocket> sockSSL;
            Owned<ISocket> acceptedRSSock;
            bool sockavail = false;
            bool securesockavail = false;
            bool rowServiceSockAvail = false;
            if (_connectMethod == SSLNone && (nullptr == rowServiceSock.get()))
                sockavail = acceptsock->wait_read(1000*60*1)!=0;
            else if (_connectMethod == SSLOnly && (nullptr == rowServiceSock.get()))
                securesockavail = securesock->wait_read(1000*60*1)!=0;
            else
            {
                UnsignedArray readSocks;
                UnsignedArray waitingSocks;
                if (acceptsock)
                    readSocks.append(acceptsock->OShandle());
                if (securesock)
                    readSocks.append(securesock->OShandle());
                if (rowServiceSock)
                    readSocks.append(rowServiceSock->OShandle());
                int numReady = wait_read_multiple(readSocks, 1000*60*1, waitingSocks);
                if (numReady > 0)
                {
                    for (int idx = 0; idx < numReady; idx++)
                    {
                        unsigned waitingSock = waitingSocks.item(idx);
                        if (acceptsock && (waitingSock == acceptsock->OShandle()))
                            sockavail = true;
                        else if (securesock && (waitingSock == securesock->OShandle()))
                            securesockavail = true;
                        else if (rowServiceSock && (waitingSock == rowServiceSock->OShandle()))
                            rowServiceSockAvail = true;
                    }
                }
            }
#if 0
            if (!sockavail && !securesockavail && !rowServiceSockAvail)
            {
                JSocketStatistics stats;
                getSocketStatistics(stats);
                StringBuffer s;
                getSocketStatisticsString(stats,s);
                PROGLOG( "Socket statistics : \n%s\n",s.str());
            }
#endif

            if (stopping)
                break;

            if (sockavail || securesockavail || rowServiceSockAvail)
            {
                if (sockavail)
                {
                    try
                    {
                        sock.setown(acceptsock->accept(true));
                        if (!sock||stopping)
                            break;
                    }
                    catch (IException *e)
                    {
                        exception.setown(e);
                    }
                    if (exception)
                    {
                        EXCLOG(exception, "CRemoteFileServer");
                        checkOutOfHandles(exception);
                        exception.clear();
                        sockavail = false;
                    }
                }

                if (securesockavail)
                {
                    Owned<ISecureSocket> ssock;
                    try
                    {
                        sockSSL.setown(securesock->accept(true));
                        if (!sockSSL||stopping)
                            break;

#ifndef _CONTAINERIZED
                        if ( (_connectMethod == UnsecureFirst) && (!securitySettings.certificate || !securitySettings.privateKey) )
                        {
                            // for client secure_connect() to fail quickly ...
                            cleanupDaFsSocket(sockSSL);
                            sockSSL.clear();
                            securesockavail = false;
                        }
                        else
#endif
                        {
                            // NB: if this is a dedicated stream service (e.g. in containerized mode)
                            // disabled cert verification, because stream requests will authenticate via signed opaque blob
                            bool disableClientCertVerification = (featureSupport == FeatureSupport::stream);

                            ssock.setown(createSecureSocket(sockSSL.getLink(), disableClientCertVerification));
                            int status = ssock->secure_accept();
                            if (status < 0)
                            {
                                if (status == PORT_CHECK_SSL_ACCEPT_ERROR)
                                    throw createDafsException(DAFSERR_serveraccept_fail_portcheck, "secure connection failure - port check");
                                else
                                    throw createDafsException(DAFSERR_serveraccept_failed, "Failure to establish secure connection");
                            }
                            sockSSL.setown(ssock.getLink());
                        }
                    }
                    catch (IException *e)
                    {
                        exception.setown(e);
                    }
                    if (exception)
                    {
                        if (exception->errorCode() != DAFSERR_serveraccept_fail_portcheck)
                            EXCLOG(exception, "CRemoteFileServer (secure)");
                        else
                            reportFailedSecureAccepts("secure", exception, numFailedConn, timeLastLog);
                        cleanupDaFsSocket(sockSSL);
                        sockSSL.clear();
                        cleanupDaFsSocket(ssock);
                        ssock.clear();
                        checkOutOfHandles(exception);
                        exception.clear();
                        securesockavail = false;
                    }
                }

                if (rowServiceSockAvail)
                {
                    Owned<ISecureSocket> ssock;
                    try
                    {
                        acceptedRSSock.setown(rowServiceSock->accept(true));
                        if (!acceptedRSSock||stopping)
                            break;

                        if (rowServiceSSL) // NB: will be disabled if !_USE_OPENSLL
                        {
                            // disabled cert verification, because stream requests will authenticate via signed opaque blob
                            ssock.setown(createSecureSocket(acceptedRSSock.getClear(), true));
                            int status = ssock->secure_accept();
                            if (status < 0)
                            {
                                if (status == PORT_CHECK_SSL_ACCEPT_ERROR)
                                    throw createDafsException(DAFSERR_serveraccept_fail_portcheck, "secure connection failure - port check");
                                else
                                    throw createDafsException(DAFSERR_serveraccept_failed, "Failure to establish SSL row service connection");
                            }
                            acceptedRSSock.setown(ssock.getLink());
                        }
                    }
                    catch (IException *e)
                    {
                        exception.setown(e);
                    }
                    if (exception)
                    {
                        if (exception->errorCode() != DAFSERR_serveraccept_fail_portcheck)
                            EXCLOG(exception, "CRemoteFileServer (row service)");
                        else
                            reportFailedSecureAccepts("row service", exception, numFailedConn, timeLastLog);
                        cleanupDaFsSocket(acceptedRSSock);
                        acceptedRSSock.clear();
                        cleanupDaFsSocket(ssock);
                        ssock.clear();
                        checkOutOfHandles(exception);
                        exception.clear();
                        rowServiceSockAvail = false;
                    }
                }

#ifdef _DEBUG
                SocketEndpoint eps;
                StringBuffer peerURL;
#endif
                if (sockavail)
                {
#ifdef _DEBUG
                    sock->getPeerEndpoint(eps);
                    eps.getEndpointHostText(peerURL);
                    PROGLOG("Server accepting from %s", peerURL.str());
#endif
                    addClient(sock.getClear(), false, false);
                }

                if (securesockavail)
                {
#ifdef _DEBUG
                    sockSSL->getPeerEndpoint(eps);
                    eps.getEndpointHostText(peerURL.clear());
                    PROGLOG("Server accepting SECURE from %s", peerURL.str());
#endif
                    addClient(sockSSL.getClear(), true, false);
                }

                if (!isContainerized() && rowServiceSockAvail) // in contaierized each service is on a single dedicated port, the below 2 cases are for BM only
                {
#ifdef _DEBUG
                    acceptedRSSock->getPeerEndpoint(eps);
                    eps.getEndpointHostText(peerURL.clear());
                    PROGLOG("Server accepting row service socket from %s", peerURL.str());
#endif
                    addClient(acceptedRSSock.getClear(), rowServiceSSL, true);
                }
            }
            else
                checkTimeout();
        }
        if (TF_TRACE_CLIENT_STATS)
            PROGLOG("CRemoteFileServer:run exiting");
        selecthandler->stop(true);
    }

    void processUnauthenticatedCommand(RemoteFileCommandType cmd, ISocket *socket, MemoryBuffer &msg)
    {
        // these are unauthenticated commands
        if (cmd != RFCgetver)
            cmd = RFCinvalid;
        MemoryBuffer reply;
        CommandRetFlags cmdFlags = processCommand(cmd, msg, initSendBuffer(reply), NULL, NULL);

        // some commands (i.e. RFCFtSlaveCmd), reply early, so should not reply again here.
        if (!hasMask(cmdFlags, CommandRetFlags::replyHandled))
            sendDaFsBuffer(socket, reply, hasMask(cmdFlags, CommandRetFlags::testSocket));
    }

    void addClient(ISocket *sock, bool secure, bool rowService) // rowService used to distinguish client calls
    {
        Owned<CRemoteClientHandler> client = new CRemoteClientHandler(this, sock, globallasttick, rowService);
        {
            CriticalBlock block(sect);
            clients.append(*client.getLink());
        }
        // JCSMORE - perhaps cap # added here... ?
        selecthandler->add(sock, SELECTMODE_READ, client);
    }

    void stop()
    {
        // stop accept loop
        if (TF_TRACE_CLIENT_STATS)
            PROGLOG("CRemoteFileServer::stop");
        if (acceptsock)
            acceptsock->cancel_accept();
        if (securesock)
            securesock->cancel_accept();
        threads->stopAll();
        threads->joinAll(true,60*1000);
    }

    bool handleCompleteMessage(CRemoteClientHandler *client, MemoryBuffer &msg)
    {
        if (TF_TRACE_FULL)
            PROGLOG("notify CRemoteClientHandler(%p), msg length=%u", client, msg.length());
        cCommandProcessor::cCommandProcessorParams params;
        params.client = client; // NB: IPooledThread::init will link 'client' (called before this function exits)
        params.msg.swapWith(msg);

        /* NB: if it hits the thread pool limit, it will start throttling (introducing delays),
         * whilst it is blocked/delaying here, the accept loop will not be listening for new
         * connections.
         */
        threads->start(&params);

        return false;
    }

    void checkTimeout()
    {
        if (msTick() - clientcounttick > 1000 * 60 * 60)
        {
            {
                CriticalBlock block(ClientCountSect);
                if (TF_TRACE_CLIENT_STATS && (ClientCount || MaxClientCount))
                    PROGLOG("Client count = %d, max = %d", ClientCount, MaxClientCount);
                MaxClientCount = ClientCount;
            }
            if (closedclients)
            {
                if (TF_TRACE_CLIENT_STATS)
                    PROGLOG("Closed client count = %d", closedclients);
                closedclients = 0;
            }
            clientcounttick = msTick();
        }
        CriticalBlock block(sect);
        ForEachItemInRev(i, clients)
        {
            CRemoteClientHandler &client = clients.item(i);
            if (client.timedOut())
            {
                StringBuffer s;
                bool ok = client.getInfo(s); // will spot duff sockets
                if (ok && (client.openFiles.ordinality() != 0))
                {
                    if (TF_TRACE_CLIENT_CONN && client.inactiveTimedOut())
                        WARNLOG("Inactive %s", s.str());
                }
                else
                {
#ifndef _DEBUG
                    if (TF_TRACE_CLIENT_CONN)
#endif
                        PROGLOG("Timing out %s", s.str());
                    closedclients++;
                    onCloseSocket(&client, 4); // removes owned handles
                }
            }
        }
    }

    void getInfo(StringBuffer &info, unsigned level=1)
    {
        {
            CriticalBlock block(ClientCountSect);
            info.append(DAFILESRV_VERSIONSTRING).append('\n');
            info.appendf("Client count = %d\n",ClientCount);
            info.appendf("Max client count = %d",MaxClientCount);
        }
        CriticalBlock block(sect);
        ForEachItemIn(i,clients)
        {
            info.newline().append(i).append(": ");
            clients.item(i).getInfo(info);
        }
        info.newline().appendf("Running threads: %u", threadRunningCount());
        info.newline();
        stdCmdThrottler.getInfo(info);
        info.newline();
        slowCmdThrottler.getInfo(info);
        clientStatsTable.getInfo(info, level);
    }

    unsigned threadRunningCount()
    {
        if (!threads)
            return 0;
        return threads->runningCount();
    }

    unsigned idleTime()
    {
        unsigned t = globallasttick;
        return msTick()-t;
    }

    void setThrottle(ThrottleClass throttleClass, unsigned limit, unsigned delayMs, unsigned cpuThreshold, unsigned queueLimit)
    {
        switch (throttleClass)
        {
            case ThrottleStd:
                stdCmdThrottler.configure(limit, delayMs, cpuThreshold, queueLimit);
                break;
            case ThrottleSlow:
                slowCmdThrottler.configure(limit, delayMs, cpuThreshold, queueLimit);
                break;
            default:
            {
                StringBuffer availableClasses("{ ");
                for (unsigned c=0; c<ThrottleClassMax; c++)
                {
                    availableClasses.append(c).append(" = ").append(getThrottleClassText((ThrottleClass)c));
                    if (c+1<ThrottleClassMax)
                        availableClasses.append(", ");
                }
                availableClasses.append(" }");
                throw MakeStringException(0, "Unknown throttle class: %u, available classes are: %s", (unsigned)throttleClass, availableClasses.str());
            }
        }
    }

    StringBuffer &getStats(StringBuffer &stats, bool reset)
    {
        CriticalBlock block(sect);
        stdCmdThrottler.getStats(stats, reset).newline();
        slowCmdThrottler.getStats(stats, reset);
        if (reset)
            clientStatsTable.reset();
        return stats;
    }
};


IRemoteFileServer * createRemoteFileServer(unsigned maxThreads, unsigned maxThreadsDelayMs, unsigned maxAsyncCopy, IPropertyTree *keyPairInfo)
{
    return new CRemoteFileServer(maxThreads, maxThreadsDelayMs, maxAsyncCopy, keyPairInfo);
}

int setDaliServerTrace(byte flags)
{
    byte ret = traceFlags;
    traceFlags = flags;
    return ret;
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "rmtfile.hpp"

/* MP_START_PORT -> MP_END_PORT is the MP reserved dynamic port range, and is used here for convenience.
 * MP_START_PORT is used as starting point to find an available port for the temporary dafilesrv service in these unittests.
 * All (MP) components using this range always check and find an unused port.
 */
static unsigned serverPort = MP_START_PORT;
static StringBuffer basePath;
static Owned<CSimpleInterface> serverThread;


class RemoteFileSlowTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(RemoteFileSlowTest);
        CPPUNIT_TEST(testRemoteFilename);
        CPPUNIT_TEST(testStartServer);
        CPPUNIT_TEST(testBasicFunctionality);
        CPPUNIT_TEST(testCopy);
        CPPUNIT_TEST(testOther);
        CPPUNIT_TEST(testConfiguration);
        CPPUNIT_TEST(testDirectoryMonitoring);
        CPPUNIT_TEST(testFinish);
    CPPUNIT_TEST_SUITE_END();

    size32_t testLen = 1024;

protected:
    void testRemoteFilename()
    {
        const char *rfns = "//1.2.3.4/dir1/file1|//1.2.3.4:7100/dir1/file1,"
                           "//1.2.3.4:7100/dir1/file1|//1.2.3.4:7100/dir1/file1,"
                           "//1.2.3.4/c$/dir1/file1|//1.2.3.4:7100/c$/dir1/file1,"
                           "//1.2.3.4:7100/c$/dir1/file1|//1.2.3.4:7100/c$/dir1/file1,"
                           "//1.2.3.4:7100/d$/dir1/file1|//1.2.3.4:7100/d$/dir1/file1";
        StringArray tests;
        tests.appendList(rfns, ",");

        ForEachItemIn(i, tests)
        {
            StringArray inOut;
            const char *pair = tests.item(i);
            inOut.appendList(pair, "|");
            const char *rfn = inOut.item(0);
            const char *expected = inOut.item(1);
            Owned<IFile> iFile = createIFile(rfn);
            const char *res = iFile->queryFilename();
            if (!streq(expected, res))
            {
                StringBuffer errMsg("testRemoteFilename MISMATCH");
                errMsg.newline().append("Expected: ").append(expected);
                errMsg.newline().append("Got: ").append(res);
                PROGLOG("%s", errMsg.str());
                CPPUNIT_ASSERT_MESSAGE(errMsg.str(), 0);
            }
            else
                PROGLOG("MATCH: %s", res);
        }
    }
    void testStartServer()
    {
        Owned<ISocket> socket;

        unsigned endPort = MP_END_PORT;
        while (1)
        {
            try
            {
                socket.setown(ISocket::create(serverPort));
                break;
            }
            catch (IJSOCK_Exception *e)
            {
                if (e->errorCode() != JSOCKERR_port_in_use)
                {
                    StringBuffer eStr;
                    e->errorMessage(eStr);
                    e->Release();
                    CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
                }
                else if (serverPort == endPort)
                {
                    e->Release();
                    CPPUNIT_ASSERT_MESSAGE("Could not find a free port to use for remote file server", 0);
                }
            }
            ++serverPort;
        }

        basePath.append("//");
        SocketEndpoint ep(serverPort);
        ep.getEndpointHostText(basePath);

        char cpath[_MAX_DIR];
        if (!GetCurrentDirectory(_MAX_DIR, cpath))
            CPPUNIT_ASSERT_MESSAGE("Current directory path too big", 0);
        else
            basePath.append(cpath);
        addPathSepChar(basePath);

        PROGLOG("basePath = %s", basePath.str());

        class CServerThread : public CSimpleInterface, implements IThreaded
        {
            CThreaded threaded;
            Owned<CRemoteFileServer> server;
            Linked<ISocket> socket;
        public:
            CServerThread(CRemoteFileServer *_server, ISocket *_socket) : threaded("CServerThread"), server(_server), socket(_socket)
            {
                threaded.init(this, false);
            }
            ~CServerThread()
            {
                threaded.join();
            }
        // IThreaded
            virtual void threadmain() override
            {
                DAFSConnectCfg sslCfg = SSLNone;
                server->run(nullptr, sslCfg, socket, nullptr, nullptr);
            }
        };
        Owned<IRemoteFileServer> server = createRemoteFileServer();
        serverThread.setown(new CServerThread(QUERYINTERFACE(server.getClear(), CRemoteFileServer), socket.getClear()));
    }
    void testBasicFunctionality()
    {
        VStringBuffer filePath("%s%s", basePath.str(), "file1");

        // create file
        Owned<IFile> iFile = createIFile(filePath);
        CPPUNIT_ASSERT(iFile);
        Owned<IFileIO> iFileIO = iFile->open(IFOcreate);
        CPPUNIT_ASSERT(iFileIO);

        // write out 1k of random data and crc
        MemoryBuffer mb;
        char *buf = (char *)mb.reserveTruncate(testLen);
        for (unsigned b=0; b<1024; b++)
            buf[b] = getRandom()%256;
        CRC32 crc;
        crc.tally(testLen, buf);
        unsigned writeCrc = crc.get();

        size32_t sz = iFileIO->write(0, testLen, buf);
        CPPUNIT_ASSERT(sz == testLen);

        // close file
        iFileIO.clear();

        // validate remote crc
        CPPUNIT_ASSERT(writeCrc == iFile->getCRC());

        // exists
        CPPUNIT_ASSERT(iFile->exists());

        // validate size
        CPPUNIT_ASSERT(iFile->size() == testLen);

        // read back and validate read data's crc against written
        iFileIO.setown(iFile->open(IFOread));
        CPPUNIT_ASSERT(iFileIO);
        sz = iFileIO->read(0, testLen, buf);
        iFileIO.clear();
        CPPUNIT_ASSERT(sz == testLen);
        crc.reset();
        crc.tally(testLen, buf);
        CPPUNIT_ASSERT(writeCrc == crc.get());
    }
    void testCopy()
    {
        VStringBuffer filePath("%s%s", basePath.str(), "file1");
        Owned<IFile> iFile = createIFile(filePath);

        // test file copy
        VStringBuffer filePathCopy("%s%s", basePath.str(), "file1copy");
        Owned<IFile> iFile1Copy = createIFile(filePathCopy);
        iFile->copyTo(iFile1Copy);

        // read back copy and validate read data's crc against written
        Owned<IFileIO> iFileIO = iFile1Copy->open(IFOreadwrite); // open read/write for appendFile in next step.
        CPPUNIT_ASSERT(iFileIO);
        MemoryBuffer mb;
        char *buf = (char *)mb.reserveTruncate(testLen);
        size32_t sz = iFileIO->read(0, testLen, buf);
        CPPUNIT_ASSERT(sz == testLen);
        CRC32 crc;
        crc.tally(testLen, buf);
        CPPUNIT_ASSERT(iFile->getCRC() == crc.get());

        // check appendFile functionality. NB after this "file1copy" should be 2*testLen
        CPPUNIT_ASSERT(testLen == iFileIO->appendFile(iFile));
        iFileIO.clear();

        // validate new size
        CPPUNIT_ASSERT(iFile1Copy->size() == 2 * testLen);

        // setSize test, truncate copy to original size
        iFileIO.setown(iFile1Copy->open(IFOreadwrite));
        iFileIO->setSize(testLen);

        // validate new size
        CPPUNIT_ASSERT(iFile1Copy->size() == testLen);
    }
    void testOther()
    {
        VStringBuffer filePath("%s%s", basePath.str(), "file1");
        Owned<IFile> iFile = createIFile(filePath);
        // rename
        iFile->rename("file2");

        // create a directory
        VStringBuffer subDirPath("%s%s", basePath.str(), "subdir1");
        Owned<IFile> subDirIFile = createIFile(subDirPath);
        subDirIFile->createDirectory();

        // check isDirectory result
        CPPUNIT_ASSERT(subDirIFile->isDirectory()==fileBool::foundYes);

        // move previous created and renamed file into new sub-directory
        // ensure not present before move
        VStringBuffer subDirFilePath("%s/%s", subDirPath.str(), "file2");
        Owned<IFile> iFile2 = createIFile(subDirFilePath);
        iFile2->remove();
        iFile->move(subDirFilePath);

        // open sub-directory file2 explicitly
        RemoteFilename rfn;
        rfn.setRemotePath(subDirPath.str());
        Owned<IFile> dir = createIFile(rfn);
        Owned<IDirectoryIterator> diriter = dir->directoryFiles("file2");
        if (!diriter->first())
        {
            CPPUNIT_ASSERT_MESSAGE("Error, file2 diriter->first() is null", 0);
        }

        Linked<IFile> iFile3 = &diriter->query();
        diriter.clear();
        dir.clear();

        OwnedIFileIO iFile3IO = iFile3->openShared(IFOread, IFSHfull);
        if (!iFile3IO)
        {
            CPPUNIT_ASSERT_MESSAGE("Error, file2 openShared() failed", 0);
        }
        iFile3IO->close();

        // count sub-directory files with a wildcard
        unsigned count=0;
        Owned<IDirectoryIterator> iter = subDirIFile->directoryFiles("*2");
        ForEach(*iter)
            ++count;
        CPPUNIT_ASSERT(1 == count);

        // check isFile result
        CPPUNIT_ASSERT(iFile2->isFile()==fileBool::foundYes);

        // validate isReadOnly before after setting
        CPPUNIT_ASSERT(iFile2->isReadOnly()==fileBool::foundNo);
        iFile2->setReadOnly(true);
        CPPUNIT_ASSERT(iFile2->isReadOnly()==fileBool::foundYes);

        // get/set Time and validate result
        CDateTime createTime, modifiedTime, accessedTime;
        CPPUNIT_ASSERT(subDirIFile->getTime(&createTime, &modifiedTime, &accessedTime));
        CDateTime newModifiedTime = modifiedTime;
        newModifiedTime.adjustTime(-86400); // -1 day
        CPPUNIT_ASSERT(subDirIFile->setTime(&createTime, &newModifiedTime, &accessedTime));
        CPPUNIT_ASSERT(subDirIFile->getTime(&createTime, &modifiedTime, &accessedTime));
        CPPUNIT_ASSERT(modifiedTime == newModifiedTime);

        // test set file permissions
        try
        {
            iFile2->setFilePermissions(0777);
        }
        catch (...)
        {
            CPPUNIT_ASSERT_MESSAGE("iFile2->setFilePermissions() exception", 0);
        }
    }
    void testConfiguration()
    {
        SocketEndpoint ep(serverPort); // test trace open connections
        CPPUNIT_ASSERT(setDafileSvrTraceFlags(ep, 0x08));

        StringBuffer infoStr;
        CPPUNIT_ASSERT(RFEnoerror == getDafileSvrInfo(ep, 10, infoStr));

        CPPUNIT_ASSERT(RFEnoerror == setDafileSvrThrottleLimit(ep, ThrottleStd, DEFAULT_STDCMD_PARALLELREQUESTLIMIT+1, DEFAULT_STDCMD_THROTTLEDELAYMS+1, DEFAULT_STDCMD_THROTTLECPULIMIT+1, DEFAULT_STDCMD_THROTTLEQUEUELIMIT+1));
    }
    void testDirectoryMonitoring()
    {
        VStringBuffer subDirPath("%s%s", basePath.str(), "subdir1");
        Owned<IFile> subDirIFile = createIFile(subDirPath);
        subDirIFile->createDirectory();

        VStringBuffer filePath("%s/%s", subDirPath.str(), "file1");
        class CDelayedFileCreate : implements IThreaded
        {
            CThreaded threaded;
            StringAttr filePath;
            Semaphore doneSem;
        public:
            CDelayedFileCreate(const char *_filePath) : threaded("CDelayedFileCreate"), filePath(_filePath)
            {
                threaded.init(this, false);
            }
            ~CDelayedFileCreate()
            {
                stop();
            }
            void stop()
            {
                doneSem.signal();
                threaded.join();
            }
            // IThreaded impl.
            virtual void threadmain() override
            {
                MilliSleep(1000); // give monitorDirectory a chance to be monitoring

                // create file
                Owned<IFile> iFile = createIFile(filePath);
                CPPUNIT_ASSERT(iFile);
                Owned<IFileIO> iFileIO = iFile->open(IFOcreate);
                CPPUNIT_ASSERT(iFileIO);
                iFileIO.clear();

                doneSem.wait(60 * 1000);

                CPPUNIT_ASSERT(iFile->remove());
            }
        } delayedFileCreate(filePath);
        Owned<IDirectoryDifferenceIterator> iter = subDirIFile->monitorDirectory(nullptr, nullptr, false, false, 2000, 60 * 1000);
        ForEach(*iter)
        {
            StringBuffer fname;
            iter->getName(fname);
            PROGLOG("fname = %s", fname.str());
        }
        delayedFileCreate.stop();
    }
    void testFinish()
    {
        // clearup
        VStringBuffer filePathCopy("%s%s", basePath.str(), "file1copy");
        Owned<IFile> iFile1Copy = createIFile(filePathCopy);
        CPPUNIT_ASSERT(iFile1Copy->remove());

        VStringBuffer subDirPath("%s%s", basePath.str(), "subdir1");
        VStringBuffer subDirFilePath("%s/%s", subDirPath.str(), "file2");
        Owned<IFile> iFile2 = createIFile(subDirFilePath);
        CPPUNIT_ASSERT(iFile2->remove());

        Owned<IFile> subDirIFile = createIFile(subDirPath);
        CPPUNIT_ASSERT(subDirIFile->remove());

        SocketEndpoint ep(serverPort);
        Owned<ISocket> sock = ISocket::connect_timeout(ep, 60 * 1000);
        CPPUNIT_ASSERT(RFEnoerror == stopRemoteServer(sock));

        serverThread.clear();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( RemoteFileSlowTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RemoteFileSlowTest, "RemoteFileSlowTests" );


#endif // _USE_CPPUNIT
