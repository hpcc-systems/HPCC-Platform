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

#include <limits.h>
#include <stdlib.h>
#include <future>
#include <vector>
#include <iterator>
#include "jcontainerized.hpp"
#include "jexcept.hpp"
#include "jiter.ipp"
#include "jlzw.hpp"
#include "jprop.hpp"
#include "jsocket.hpp"
#include "jset.hpp"
#include "jsort.hpp"
#include "portlist.h"
#include "jhtree.hpp"
#include "mputil.hpp"
#include "dllserver.hpp"
#include "dautils.hpp"
#include "danqs.hpp"
#include "daclient.hpp"
#include "daaudit.hpp"
#include "wujobq.hpp"
#include "thorport.hpp"
#include "commonext.hpp"
#include "thorxmlread.hpp"
#include "thorplugin.hpp"
#include "thormisc.hpp"
#include "thgraphmaster.ipp"
#include "thdemonserver.hpp"
#include "rtlds_imp.hpp"
#include "eclhelper.hpp"
#include "thexception.hpp"
#include "thactivitymaster.ipp"
#include "thmem.hpp"
#include "thcompressutil.hpp"

using roxiemem::OwnedRoxieString;

static CriticalSection *jobManagerCrit;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    jobManagerCrit = new CriticalSection;
    return true;
}
MODULE_EXIT()
{
    delete jobManagerCrit;
}

unsigned uniqGraphId = 1;

#define FATAL_TIMEOUT 60
class CFatalHandler : public CTimeoutTrigger, implements IFatalHandler
{
public:
    IMPLEMENT_IINTERFACE;

    CFatalHandler(unsigned timeout) : CTimeoutTrigger(timeout, "EXCEPTION")
    {
        start();
    }
    virtual bool action() override
    {
        StringBuffer s("FAILED TO RECOVER FROM EXCEPTION, STOPPING THOR");
        FLLOG(MCoperatorWarning, exception, s.str());
        Owned<IJobManager> jobManager = getJobManager();
        if (jobManager)
        {
            jobManager->fatal(exception);
            jobManager.clear();
        }
        return true;
    }
// IFatalHandler
    virtual void inform(IException *e) override
    {
        CTimeoutTrigger::inform(e);
    }
    virtual void clear() override
    {
        CTimeoutTrigger::clear();
    }
};

/////

CSlaveMessageHandler::CSlaveMessageHandler(CJobMaster &_job, mptag_t _mptag) : threaded("CSlaveMessageHandler"), job(_job), mptag(_mptag)
{
    stopped = false;
    threaded.init(this, false);
    childGraphInitTimeout = job.getOptUInt(THOROPT_CHILD_GRAPH_INIT_TIMEOUT, 5*60) * 1000; // default 5 minutes
}

CSlaveMessageHandler::~CSlaveMessageHandler()
{
    stop();
}

void CSlaveMessageHandler::stop()
{
    if (!stopped)
    {
        stopped = true;
        job.queryJobChannel(0).queryJobComm().cancel(0, mptag);
        threaded.join();
    }
}

void CSlaveMessageHandler::threadmain()
{
    try
    {
        for (;;)
        {
            rank_t sender;
            CMessageBuffer msg;
            if (stopped || !job.queryJobChannel(0).queryJobComm().recv(msg, RANK_ALL, mptag, &sender))
                break;
            SlaveMsgTypes msgType;
            readUnderlyingType(msg, msgType);
            switch (msgType)
            {
                case smt_errorMsg:
                {
                    unsigned slave;
                    msg.read(slave);
                    Owned<IThorException> e = deserializeThorException(msg);
                    if (slave==0) // slave # unknown
                    {
                        if (job.queryChannelsPerSlave()>1) // we don't know which slave, but we know node (sender)
                            e->setSlave(-sender); // -ve number used by reporting to flag that it is a known node vs slave #
                        else
                            e->setSlave(sender);
                    }
                    else
                        e->setSlave(slave);
                    Owned<CGraphBase> graph = job.queryJobChannel(0).getGraph(e->queryGraphId());
                    if (graph)
                    {
                        activity_id id = e->queryActivityId();
                        if (id)
                        {
                            CGraphElementBase *elem = graph->queryElement(id);
                            CActivityBase *act = elem->queryActivity();
                            if (act)
                                act->fireException(e);
                            else
                                graph->fireException(e);
                        }
                        else
                            graph->fireException(e);
                    }
                    else
                        job.fireException(e);
                    if (msg.getReplyTag() <= TAG_REPLY_BASE)
                    {
                        msg.clear();
                        job.queryJobChannel(0).queryJobComm().reply(msg);
                    }
                    break;
                }
                case smt_dataReq:
                {
                    graph_id gid;
                    activity_id id;
                    unsigned slave;
                    msg.read(slave);
                    msg.read(gid);
                    msg.read(id);
                    msg.clear();
                    Owned<CGraphBase> graph = job.queryJobChannel(0).getGraph(gid);
                    if (graph)
                    {
                        CMasterGraphElement *e = (CMasterGraphElement *)graph->queryElement(id);
                        e->queryActivity()->getInitializationData(slave, msg);
                    }
                    job.queryJobChannel(0).queryJobComm().reply(msg);
                    break;
                }
                case smt_initGraphReq:
                {
                    graph_id gid;
                    msg.read(gid);
                    Owned<CMasterGraph> graph = (CMasterGraph *)job.queryJobChannel(0).getGraph(gid);
                    assertex(graph);
                    {
                        CriticalBlock b(graph->queryCreateLock());
                        Owned<IThorActivityIterator> iter = graph->getIterator();
                        // onCreate all
                        ForEach (*iter)
                        {
                            CMasterGraphElement &element = (CMasterGraphElement &)iter->query();
                            element.onCreate();
                        }
                    }
                    msg.clear();
                    graph->serializeCreateContexts(msg);
                    job.queryJobChannel(0).queryJobComm().reply(msg);
                    break;
                }
                case smt_initActDataReq:
                {
                    graph_id gid;
                    msg.read(gid);
                    unsigned slave;
                    msg.read(slave);
                    Owned<CMasterGraph> graph = (CMasterGraph *)job.queryJobChannel(0).getGraph(gid);
                    assertex(graph);
                    CGraphElementArray toSerialize;
                    CriticalBlock b(graph->queryCreateLock());
                    size32_t parentExtractSz;
                    msg.read(parentExtractSz);
                    const byte *parentExtract = NULL;
                    if (parentExtractSz)
                    {
                        parentExtract = msg.readDirect(parentExtractSz);
                        StringBuffer msg("Graph(");
                        msg.append(graph->queryGraphId()).append(") - initializing master graph with parentExtract ").append(parentExtractSz).append(" bytes");
                        DBGLOG("%s", msg.str());
                        parentExtract = graph->setParentCtx(parentExtractSz, parentExtract);
                    }
                    Owned<IException> exception;
                    for (;;)
                    {
                        activity_id id;
                        msg.read(id);
                        if (!id)
                            break;
                        CMasterGraphElement *element = (CMasterGraphElement *)graph->queryElement(id);
                        assertex(element);
                        try
                        {
                            element->reset();
                            size32_t startCtxLen;
                            msg.read(startCtxLen);
                            element->doCreateActivity(parentExtractSz, parentExtract, startCtxLen ? &msg : nullptr);
                            if (element->queryActivity())
                                element->preStart(parentExtractSz, parentExtract);
                        }
                        catch (IException *e)
                        {
                            IWARNLOG(e);
                            exception.setown(e);
                            break;
                        }
                        CActivityBase *activity = element->queryActivity();
                        if (activity)
                            element->sentActInitData->set(slave, 0); // clear to permit serializeActivityInitData to resend
                        toSerialize.append(*LINK(element));
                    }
                    msg.clear();
                    mptag_t replyTag = job.queryJobChannel(0).queryMPServer().createReplyTag();
                    msg.append(replyTag); // second reply
                    if (exception)
                    {
                        msg.append(true);
                        serializeException(exception, msg);
                    }
                    else
                    {
                        msg.append(false);
                        CGraphElementArrayIterator iter(toSerialize);
                        graph->serializeActivityInitData(slave, msg, iter);
                    }
                    job.queryJobChannel(0).queryJobComm().reply(msg);
                    if (!job.queryJobChannel(0).queryJobComm().recv(msg, slave+1, replyTag, NULL, childGraphInitTimeout))
                        throwUnexpected();
                    if (exception)
                        throw exception.getClear();
                    bool error;
                    msg.read(error);
                    if (error)
                    {
                        Owned<IThorException> e = deserializeThorException(msg);
                        e->setSlave(slave);
                        StringBuffer tmpStr("Slave ");
                        job.queryJobGroup().queryNode(slave).endpoint().getEndpointHostText(tmpStr);
                        GraphPrintLog(graph, e, "%s", tmpStr.append(": slave initialization error").str());
                        throw e.getClear();
                    }
                    break;
                }
                case smt_getPhysicalName:
                {
                    DBGLOG("getPhysicalName called from node %d", sender-1);
                    StringAttr logicalName;
                    unsigned partNo;
                    bool create;
                    msg.read(logicalName);
                    msg.read(partNo);
                    msg.read(create);
                    msg.clear();
                    StringBuffer phys;
                    if (create && !job.queryCreatedFile(logicalName)) // not sure who would do this ever??
                        queryThorFileManager().getPublishPhysicalName(job, logicalName, partNo, phys); 
                    else
                        queryThorFileManager().getPhysicalName(job, logicalName, partNo, phys);
                    msg.append(phys);
                    job.queryJobChannel(0).queryJobComm().reply(msg);
                    break;
                }
                case smt_getFileOffset:
                {
                    DBGLOG("getFileOffset called from node %d", sender-1);
                    StringAttr logicalName;
                    unsigned partNo;
                    msg.read(logicalName);
                    msg.read(partNo);
                    msg.clear();
                    offset_t offset = queryThorFileManager().getFileOffset(job, logicalName, partNo);
                    msg.append(offset);
                    job.queryJobChannel(0).queryJobComm().reply(msg);
                    break;
                }
                case smt_actMsg:
                {
                    DBGLOG("smt_actMsg called from node %d", sender-1);
                    graph_id gid;
                    msg.read(gid);
                    activity_id id;
                    msg.read(id);
                    Owned<CMasterGraph> graph = (CMasterGraph *)job.queryJobChannel(0).getGraph(gid);
                    assertex(graph);
                    CMasterGraphElement *container = (CMasterGraphElement *)graph->queryElement(id);
                    assertex(container);
                    CMasterActivity *activity = (CMasterActivity *)container->queryActivity();
                    assertex(activity);
                    activity->handleSlaveMessage(msg); // don't block
                    break;
                }
                case smt_getresult:
                {
                    unsigned slave;
                    msg.read(slave);
                    DBGLOG("smt_getresult called from slave %d", slave);
                    graph_id gid;
                    msg.read(gid);
                    activity_id ownerId;
                    msg.read(ownerId);
                    unsigned resultId;
                    msg.read(resultId);
                    CJobChannel &jobChannel = job.queryJobChannel(0);
                    mptag_t replyTag = jobChannel.deserializeMPTag(msg);
                    Owned<IThorResult> result = jobChannel.getOwnedResult(gid, ownerId, resultId);
                    Owned<IRowStream> resultStream = result->getRowStream();
                    sendInChunks(job.queryJobChannel(0).queryJobComm(), slave+1, replyTag, resultStream, result->queryRowInterfaces());
                    break;
                }
            }
        }
    }
    catch (IException *e)
    {
        Owned<IThorException> te = ThorWrapException(e, "CSlaveMessageHandler::threadmain");
        e->Release();
        job.fireException(te);
    }
}

//////////////////////

CMasterActivity::CMasterActivity(CGraphElementBase *_container, const StatisticsMapping &statsMapping)
    : CActivityBase(_container, statsMapping), threaded("CMasterActivity", this), statsCollection(statsMapping)
{
    notedWarnings = createThreadSafeBitSet();
    mpTag = TAG_NULL;
    data = new MemoryBuffer[container.queryJob().querySlaves()];
    asyncStart = false;
    if (container.isSink())
        edgeStatsVector.push_back(new CThorEdgeCollection);
    else
    {
        for (unsigned o=0; o<container.getOutputs(); o++)
            edgeStatsVector.push_back(new CThorEdgeCollection);
    }
}

CMasterActivity::~CMasterActivity()
{
    if (asyncStart)
        threaded.join();
    notedWarnings->Release();
    queryJob().freeMPTag(mpTag);
    delete [] data;
}

IDistributedFile *CMasterActivity::queryReadFile(unsigned f)
{
    if (f>=readFiles.size())
        return nullptr;
    return readFiles[f];
}

unsigned CMasterActivity::queryReadFileId(const char *normalizedFileName)
{
    auto it = readFilesMap.find(normalizedFileName);
    if (it != readFilesMap.end())
        return (int)(it->second);
    return NotFound;
}

IDistributedFile *CMasterActivity::findReadFile(const char *normalizedFileName)
{
    auto it = readFilesMap.find(normalizedFileName);
    if (it != readFilesMap.end())
        return LINK(readFiles[it->second]);
    return nullptr;
}

IDistributedFile *CMasterActivity::lookupReadFile(const char *lfnName, AccessMode mode, bool jobTemp, bool temp, bool opt, bool statsForMultipleFiles, const StatisticsMapping & statsMapping, unsigned * fileStatsStartEntry)
{
    StringBuffer normalizedFileName;
    queryThorFileManager().addScope(container.queryJob(), lfnName, normalizedFileName, jobTemp|temp);
    Owned<IDistributedFile> file;
    unsigned fileId = queryReadFileId(normalizedFileName.str());
    if (fileId==NotFound)
    {
        file.setown(queryThorFileManager().lookup(container.queryJob(), lfnName, mode, jobTemp|temp, opt, true, container.activityIsCodeSigned()));
        if (file)
        {
            fileId = readFiles.size();
            readFiles.push_back(LINK(file));
            readFilesMap[normalizedFileName.str()] = fileId;
            if (!temp) // NB: Temps not listed in workunit
                queryThorFileManager().noteFileRead(container.queryJob(), file);
            if (!jobTemp && !temp && fileStatsStartEntry!=nullptr)
            {
                unsigned fileStatsSizeExtend = 0; // how many entries need to be added to fileStats vector
                IDistributedSuperFile *super = file->querySuperFile();
                bool isSuper = super!=nullptr;
                unsigned numSubs = isSuper ? super->numSubFiles(true) : 0;
                if (statsForMultipleFiles)
                {
                    // fileStats[] will need to track stats from multiple files.  To do this, fileStatsTable[] vector
                    // will be used to map from file id to the starting point of the stats in fileStats.
                    // e.g.
                    // Given that file id for 'child::super' is 0
                    //            file id for 'child::f1' is 1
                    //            file id for 'child::f2' is 2
                    //       and child::super has 2 subfiles
                    //
                    // Then the following table will be generated (through multiple calls to this function)
                    // fileStatsTable[0] = 0;
                    // fileStatsTable[1] = 2;
                    // fileStatsTable[2] = 3;
                    //
                    // fileStats[0] = stats for 'child::super' - stats for child::super and subfile 1
                    // fileStats[1] = stats for 'child::super' - stats for child::super and subfile 2
                    // fileStats[2] = stats for 'child::f1' - stats for child::f1
                    // fileStats[3] = stats for 'child::f2' - stats for child::f2
                    *fileStatsStartEntry = (unsigned) fileStats.size(); // index of the first stats entry for this file
                    fileStatsTable.push_back(*fileStatsStartEntry);
                    assertex(fileId==fileStatsTable.size()-1);
                    fileStatsSizeExtend = isSuper?numSubs:1;
                }
                else  // Use entire fileStats to track stats for a superfile (fileStats is not used if not a superfile)
                {
                    assertex(fileStats.size() == 0);
                    *fileStatsStartEntry = 0;
                    // Super files will use fileStats to track stats.  Non-super file stats tracked in graph stats
                    fileStatsSizeExtend = isSuper?numSubs:0;
                }
                for (unsigned i=0; i<fileStatsSizeExtend; i++)
                    fileStats.push_back(new CThorStatsCollection(statsMapping));
            }
        }
    }
    else
    {
        file.setown(LINK(queryReadFile(fileId)));
        if (!jobTemp && !temp && !fileStatsTable.empty())
            *fileStatsStartEntry = fileStatsTable[fileId];
    }
    return file.getClear();
}

MemoryBuffer &CMasterActivity::queryInitializationData(unsigned slave) const
{ // NB: not intended to be called by multiple threads.
    return data[slave].reset();
}

MemoryBuffer &CMasterActivity::getInitializationData(unsigned slave, MemoryBuffer &dst) const
{
    return dst.append(data[slave]);
}

void CMasterActivity::threadmain()
{
    try
    {
        process();
    }
    catch (IException *e)
    {
        Owned<IException> e2;
        if (QUERYINTERFACE(e, ISEH_Exception))
            e2.setown(MakeThorFatal(e, TE_SEH, "(SEH)"));
        else
            e2.setown(MakeActivityException(this, e, "Master exception"));
        e->Release();
        ActPrintLog(e2, "In CMasterActivity::threadmain");
        fireException(e2);
    }
    catch (CATCHALL)
    {
        Owned<IException> e = MakeThorFatal(NULL, TE_MasterProcessError, "FATAL: Unknown master process exception kind=%s, id=%" ACTPF "d", activityKindStr(container.getKind()), container.queryId());
        ActPrintLog(e, "In CMasterActivity::threadmain");
        fireException(e);
    }
}

void CMasterActivity::init()
{
    // Files are added to readFiles during initialization,
    // If this is a CQ query act., then it will be repeatedly re-initialized if it depends on master context,
    // e.g. due to a variable filename.
    // Therefore, do not clear readFiles on reinitialization, to avoid repeatedly [expensively] looking them up.
    bool inCQ = container.queryOwner().queryOwner() && !container.queryOwner().isGlobal();
    if (!inCQ)
    {
        readFiles.clear();
        readFilesMap.clear();
        fileStatsTable.clear();
        fileStats.clear();
    }
}

void CMasterActivity::startProcess(bool async)
{
    if (async)
    {
        asyncStart = true;
        threaded.start(true);
    }
    else
        threadmain();
}

bool CMasterActivity::wait(unsigned timeout)
{
    if (!asyncStart)
        return true;
    return threaded.join(timeout);
}

void CMasterActivity::kill()
{
    CActivityBase::kill();
    readFiles.clear();
    readFilesMap.clear();
}

bool CMasterActivity::fireException(IException *_e)
{
    IThorException *e = QUERYINTERFACE(_e, IThorException);
    if (!e) return false;
    switch (e->errorCode())
    {
        case TE_LargeBufferWarning:
        case TE_MoxieIndarOverflow:
        case TE_BuildIndexFewExcess:
        case TE_FetchMisaligned:
        case TE_FetchOutOfRange:
        case TE_CouldNotCreateLookAhead:
        case TE_SpillAdded:
        case TE_ReadPartialFromPipe:
        case TE_LargeAggregateTable:
        case TE_RemoteReadFailure:
        {
            if (!notedWarnings->testSet(e->errorCode()))
                CActivityBase::fireException(e);
            return true;
        }
    }
    return container.queryOwner().fireException(e);
}

void CMasterActivity::reset()
{
    asyncStart = false;
    CActivityBase::reset();
}

void CMasterActivity::deserializeStats(unsigned node, MemoryBuffer &mb)
{
    CriticalBlock b(progressCrit); // don't think needed
    statsCollection.deserialize(node, mb);
    rowcount_t count;
    for (auto &collection: edgeStatsVector)
    {
        mb.read(count);
        collection->set(node, count);
    }
}

void CMasterActivity::getActivityStats(IStatisticGatherer & stats)
{
    statsCollection.getStats(stats);
}

void CMasterActivity::getEdgeStats(IStatisticGatherer & stats, unsigned idx)
{
    CriticalBlock b(progressCrit);
    if (idx < edgeStatsVector.size())
        edgeStatsVector[idx]->getStats(stats);
}

void CMasterActivity::done()
{
    CActivityBase::done();
    if (readFiles.size())
    {
        IArrayOf<IDistributedFile> tmpFiles;
        for (IDistributedFile * file: readFiles)
        {
            IDistributedSuperFile *super = file->querySuperFile();
            if (super)
            {
                getSuperFileSubs(super, tmpFiles, true);
                tmpFiles.append(*LINK(file));
            }
            else
                tmpFiles.append(*LINK(file));
        }
        ForEachItemIn(s, tmpFiles)
        {
            IDistributedFile &file = tmpFiles.item(s);
            file.setAccessed();
        }
    }
}

// calcFileReadCostStats calculates and returns the read costs for all files read by the activity
// In addition, if updateFileProps==true, it updates the file attributes with @readCost and @numDiskReads
// Note: should be called once per activity with "updateFileProps==true" to avoid double counting
cost_type CMasterActivity::calcFileReadCostStats(bool updateFileProps)
{
    // 1) Returns readCost 2) if updateFilePros==true, updates file attributes with @readCost and @numDiskReads
    auto updateReadCosts = [updateFileProps](bool useJhtreeCacheStats, IDistributedFile *file, CThorStatsCollection &stats)
    {
        StringBuffer clusterName;
        file->getClusterName(0, clusterName);
        cost_type curReadCost = 0;
        stat_type curDiskReads = stats.getStatisticSum(StNumDiskReads);
        if(useJhtreeCacheStats)
        {
            stat_type numActualReads = stats.getStatisticSum(StNumNodeDiskFetches)
                                    + stats.getStatisticSum(StNumLeafDiskFetches)
                                    + stats.getStatisticSum(StNumBlobDiskFetches);
            curReadCost = calcFileAccessCost(clusterName, 0, numActualReads);
        }
        else
            curReadCost = calcFileAccessCost(clusterName, 0, curDiskReads);

        if (updateFileProps)
            updateCostAndNumReads(file, curDiskReads);
        return curReadCost;
    };
    cost_type readCost = 0;
    ThorActivityKind actKind = container.getKind();
    bool bIndexReadActivity = isIndexReadActivity(actKind);
    if (fileStats.size()>0)
    {
        unsigned fileIndex = 0;
        diskAccessCost = 0;
        for (unsigned i=0; i<readFiles.size(); i++)
        {
            IDistributedFile *file = queryReadFile(i);
            bool useJhtreeCache = false;
            // Determine if jhtree cache stats should be used to calculate file access cost:
            // * Any activities that reads an index should use jhtree cache stats to calculate cost
            // * However, if it is a keyed join activity, then only use jhtree cache for cost calc if it is the first file
            //   (because any other file will be a regular data file)
            if (bIndexReadActivity && (!(TAKkeyedjoin == actKind) || (0 == i)))
                useJhtreeCache = true;
            if (file)
            {
                IDistributedSuperFile *super = file->querySuperFile();
                if (super)
                {
                    unsigned numSubFiles = super->numSubFiles(true);
                    for (unsigned i=0; i<numSubFiles; i++)
                    {
                        IDistributedFile &subFile = super->querySubFile(i, true);
                        readCost += updateReadCosts(useJhtreeCache, &subFile, *fileStats[fileIndex]);
                        fileIndex++;
                    }
                }
                else
                {
                    readCost += updateReadCosts(useJhtreeCache, file, *fileStats[fileIndex]);
                    fileIndex++;
                }
            }
        }
    }
    else
    {
        IDistributedFile *file = queryReadFile(0);
        if (file)
        {
            // note: use jhtree cache stats to calculate file access cost if it is an index activity
            readCost = updateReadCosts(bIndexReadActivity, file, statsCollection);
        }
    }
    return readCost;
}

//////////////////////
// CMasterGraphElement impl.
//

CMasterGraphElement::CMasterGraphElement(CGraphBase &_owner, IPropertyTree &_xgmml) : CGraphElementBase(_owner, _xgmml, nullptr)
{
}

bool CMasterGraphElement::checkUpdate()
{
    if (!onlyUpdateIfChanged)
        return false;
    if (!globals->getPropBool("@updateEnabled", true) || 0 != queryJob().getWorkUnitValueInt("disableUpdate", 0))
        return false;

    bool doCheckUpdate = false;
    OwnedRoxieString filename;
    unsigned eclCRC;
    unsigned __int64 totalCRC;
    bool temporary = false;
    switch (getKind())
    {
        case TAKindexwrite:
        {
            IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *)queryHelper();
            doCheckUpdate = 0 != (helper->getFlags() & TIWupdate);
            filename.setown(helper->getFileName());
            helper->getUpdateCRCs(eclCRC, totalCRC);
            break;
        }
        case TAKdiskwrite:
        case TAKcsvwrite:
        case TAKxmlwrite:
        case TAKjsonwrite:
        case TAKspillwrite:
        {
            IHThorDiskWriteArg *helper = (IHThorDiskWriteArg *)queryHelper();
            doCheckUpdate = 0 != (helper->getFlags() & TDWupdate);
            filename.setown(helper->getFileName());
            helper->getUpdateCRCs(eclCRC, totalCRC);
            if (TAKdiskwrite == getKind())
                temporary = 0 != (helper->getFlags() & (TDXtemporary|TDXjobtemp));
            else if (TAKspillwrite == getKind())
                temporary = true;
            break;
        }
        default:
            break;
    }

    if (doCheckUpdate)
    {
        StringAttr lfn;
        Owned<IDistributedFile> file = queryThorFileManager().lookup(queryJob(), filename, AccessMode::readMeta, temporary, true, false, defaultPrivilegedUser);
        if (file)
        {
            IPropertyTree &props = file->queryAttributes();
            if ((eclCRC == (unsigned)props.getPropInt("@eclCRC")) && (totalCRC == (unsigned __int64)props.getPropInt64("@totalCRC")))
            {
                // so this needs pruning
                Owned<IThorException> e = MakeActivityWarning(this, ENGINEERR_FILE_UPTODATE, "output file = '%s' - is up to date - it will not be rebuilt", file->queryLogicalName());
                queryOwner().fireException(e);
                return true;
            }
        }
    }
    return false;
}

void CMasterGraphElement::initActivity()
{
    CriticalBlock b(crit);
    if (!initialized)
    {
        initialized = true;
        ((CMasterActivity *)queryActivity())->init();
    }
}

void CMasterGraphElement::reset()
{
    CGraphElementBase::reset();
    if (activity && activity->needReInit())
        initialized = false;
}

void CMasterGraphElement::doCreateActivity(size32_t parentExtractSz, const byte *parentExtract, MemoryBuffer *startCtx)
{
    createActivity();
    onStart(parentExtractSz, parentExtract, startCtx);
    initActivity();
}

void CMasterGraphElement::slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
{
    ((CMasterActivity *)queryActivity())->slaveDone(slaveIdx, mb);
}


//////

///////
class CBarrierMaster : public CInterface, implements IBarrier
{
    mptag_t tag;
    Linked<ICommunicator> comm;
    bool receiving;
public:
    IMPLEMENT_IINTERFACE;

    CBarrierMaster(ICommunicator &_comm, mptag_t _tag) : comm(&_comm), tag(_tag)
    {
        receiving = false;
    }
    virtual mptag_t queryTag() const override { return tag; }
    virtual bool wait(bool exception, unsigned timeout) override
    {
        Owned<IException> e;
        CTimeMon tm(timeout);
        unsigned s=comm->queryGroup().ordinality()-1;
        bool aborted = false;
        CMessageBuffer msg;
        Owned<IBitSet> raisedSet = createThreadSafeBitSet();
        unsigned remaining = timeout;
        while (s--)
        {
            rank_t sender;
            msg.clear();
            if (INFINITE != timeout && tm.timedout(&remaining))
            {
                if (exception)
                    throw createBarrierAbortException();
                else
                    return false;
            }
            {
                BooleanOnOff onOff(receiving);
                if (!comm->recv(msg, RANK_ALL, tag, &sender, remaining))
                    break;
            }
            msg.read(aborted);
            bool hasExcept;
            msg.read(hasExcept);
            if (hasExcept && !e.get())
                e.setown(deserializeException(msg));
            sender = sender - 1; // 0 = master
            if (raisedSet->testSet(sender, true) && !aborted)
                IWARNLOG("CBarrierMaster, raise barrier message on tag %d, already received from slave %d", tag, sender);
            if (aborted) break;
        }
        msg.clear();
        msg.append(aborted);
        if (e)
        {
            msg.append(true);
            serializeException(e, msg);
        }
        else
            msg.append(false);
        if (INFINITE != timeout && tm.timedout(&remaining))
        {
            if (exception)
                throw createBarrierAbortException();
            else
                return false;
        }
        if (!comm->send(msg, RANK_ALL_OTHER, tag, INFINITE != timeout ? remaining : LONGTIMEOUT))
            throw MakeStringException(0, "CBarrierMaster::wait - Timeout sending to slaves");
        if (aborted)
        {
            if (!exception)
                return false;
            if (e)
                throw e.getClear();
            else
                throw createBarrierAbortException();
        }
        return true;
    }
    virtual void cancel(IException *e) override
    {
        if (receiving)
            comm->cancel(RANK_ALL, tag);
        CMessageBuffer msg;
        msg.append(true);
        if (e)
        {
            msg.append(true);
            serializeException(e, msg);
        }
        else
            msg.append(false);
        if (!comm->send(msg, RANK_ALL_OTHER, tag, LONGTIMEOUT))
            throw MakeStringException(0, "CBarrierMaster::cancel - Timeout sending to slaves");
    }
};

/////////////


class CMasterGraphTempHandler : public CGraphTempHandler
{
public:
    CMasterGraphTempHandler(CJobBase &job, bool errorOnMissing) : CGraphTempHandler(job, errorOnMissing) { }

    virtual bool removeTemp(const char *name)
    {
        queryThorFileManager().clearCacheEntry(name);
        return true;
    }
    virtual CFileUsageEntry * registerFile(const char *name, graph_id graphId, unsigned usageCount, bool temp, WUFileKind fileKind, StringArray *clusters)
    {
        if (!temp || job.queryUseCheckpoints())
        {
            Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
            wu->addFile(name, clusters, usageCount, fileKind, job.queryGraphName());
            return nullptr;
        }
        else
            return CGraphTempHandler::registerFile(name, graphId, usageCount, temp, fileKind, clusters);
    }
    virtual void deregisterFile(const char *name, bool kept) // NB: only called for temp files
    {
        if (kept || job.queryUseCheckpoints())
        {
            Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
            wu->releaseFile(name);
        }
        else
            CGraphTempHandler::deregisterFile(name);
    }
    virtual void clearTemps()
    {
        try
        {
            if (!job.queryPausing()) // temps of completed workunit will have been preserved and want to keep
            {
                Owned<IWorkUnit> lwu = &job.queryWorkUnit().lock();
                lwu->deleteTempFiles(job.queryGraphName(), false, false);
            }
        }
        catch (IException *e)
        {
            IERRLOG(e, "Problem deleting temp files");
            e->Release();
        }
        CGraphTempHandler::clearTemps();
    }
};

static const char * getResultText(StringBuffer & s, const char * stepname, unsigned sequence)
{
    switch ((int)sequence)
    {
    case -1: return s.append("STORED('").append(stepname).append("')");
    case -2: return s.append("PERSIST('").append(stepname).append("')");
    case -3: return s.append("global('").append(stepname).append("')");
    default:
        if (stepname)
            return s.append(stepname);
        return s.append('#').append(sequence);
    }
}

class CThorCodeContextMaster : public CThorCodeContextBase
{
    Linked<IConstWorkUnit> workunit;
    Owned<IDistributedFileTransaction> superfiletransaction;

    IConstWUResult * getResult(const char * name, unsigned sequence)
    {
        return getWorkUnitResult(workunit, name, sequence);
    }
    #define PROTECTED_GETRESULT(STEPNAME, SEQUENCE, KIND, KINDTEXT, ACTION) \
        DBGLOG("getResult%s(%s,%d)", KIND, STEPNAME?STEPNAME:"", SEQUENCE); \
        Owned<IConstWUResult> r = getResultForGet(STEPNAME, SEQUENCE); \
        try \
        { \
            ACTION \
        } \
        catch (IException * e) { \
            StringBuffer s, text; e->errorMessage(text); e->Release(); \
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "result %s in workunit contains an invalid " KINDTEXT " value [%s]", getResultText(s, STEPNAME, SEQUENCE), text.str()); \
        } \
        catch (CATCHALL)    { StringBuffer s; throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "value %s in workunit contains an invalid " KINDTEXT " value", getResultText(s, STEPNAME, SEQUENCE)); }

public:
    CThorCodeContextMaster(CJobChannel &jobChannel, IConstWorkUnit &_workunit, ILoadedDllEntry &querySo, IUserDescriptor &userDesc) : CThorCodeContextBase(jobChannel, querySo, userDesc), workunit(&_workunit)
    {
    }

// ICodeContext
    virtual unsigned getGraphLoopCounter() const override { return 0; }
    virtual IDebuggableContext *queryDebugContext() const override { return nullptr; }
    virtual void setResultBool(const char *name, unsigned sequence, bool result) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultBool(result);   
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultBool");
    }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void *result) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultData(result, len);  
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultData");
    }
    virtual void setResultDecimal(const char * name, unsigned sequence, int len, int precision, bool isSigned, const void *val) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultDecimal(val, len);
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultDecimal");
    }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 result, unsigned size) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultInt(result);
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultInt");
    }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void *result) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultRaw(len, result, ResultFormatRaw);  
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultData");
    }
    virtual void setResultReal(const char *name, unsigned sequence, double result) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultReal(result);   
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultReal");
    }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void *result, ISetToXmlTransformer *) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultIsAll(isAll);
            r->setResultRaw(len, result, ResultFormatRaw);  
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultData");
    }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char *result) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultString(result, len);    
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultString");
    }
    virtual void setResultUnicode(const char * name, unsigned sequence, int len, UChar const * result) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultUnicode(result, len);
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultUnicode");
    }
    virtual void setResultVarString(const char * name, unsigned sequence, const char *result) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultString(result, strlen(result)); 
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultVarString");
    }
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 result, unsigned size) override
    {
        WorkunitUpdate w(&workunit->lock());
        Owned<IWUResult> r = updateWorkUnitResult(w, name, sequence);
        if (r)
        {
            r->setResultUInt(result);
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultUInt");
    }
    virtual void setResultVarUnicode(const char * stepname, unsigned sequence, UChar const *val) override
    {
        setResultUnicode(stepname, sequence, rtlUnicodeStrlen(val), val);
    }
    virtual bool getResultBool(const char * stepname, unsigned sequence) override
    { 
        PROTECTED_GETRESULT(stepname, sequence, "Bool", "bool",
            return r->getResultBool();
        );
    }
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * stepname, unsigned sequence) override
    {
        PROTECTED_GETRESULT(stepname, sequence, "Data", "data",
            SCMStringBuffer result;
            r->getResultString(result, false);
            tlen = result.length();
            tgt = (char *)result.s.detach();
        );
    }
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence) override
    {
        PROTECTED_GETRESULT(stepname, sequence, "Decimal", "decimal",
            r->getResultDecimal(tgt, tlen, precision, isSigned);
        );
    }
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override
    {
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Raw", "raw",
            Variable2IDataVal result(&tlen, &tgt);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
        );
    }
    virtual void getResultSet(bool & isAll, unsigned & tlen, void * & tgt, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override
    {
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Raw", "raw",
            Variable2IDataVal result(&tlen, &tgt);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            isAll = r->getResultIsAll();
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
        );
    }
    virtual __int64 getResultInt(const char * name, unsigned sequence) override
    { 
        PROTECTED_GETRESULT(name, sequence, "Int", "integer",
            return r->getResultInt();
        );
    }
    virtual double getResultReal(const char * name, unsigned sequence) override
    {
        PROTECTED_GETRESULT(name, sequence, "Real", "real",
            return r->getResultReal();
        );
    }
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * stepname, unsigned sequence) override
    {
        PROTECTED_GETRESULT(stepname, sequence, "String", "string",
            SCMStringBuffer result;
            r->getResultString(result, false);
            tlen = result.length();
            tgt = (char *)result.s.detach();
        );
    }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * stepname, unsigned sequence) override
    {
        PROTECTED_GETRESULT(stepname, sequence, "String", "string",
            SCMStringBuffer result;
            r->getResultString(result, false);
            rtlStrToStr(tlen, tgt, result.length(), result.s.str());
        );
    }
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * stepname, unsigned sequence) override
    {
        PROTECTED_GETRESULT(stepname, sequence, "Unicode", "unicode",
            MemoryBuffer result;
            r->getResultUnicode(MemoryBuffer2IDataVal(result));
            tlen = result.length()/2;
            tgt = (UChar *)malloc(tlen*2);
            memcpy_iflen(tgt, result.toByteArray(), tlen*2);
        );
    }
    virtual char * getResultVarString(const char * stepname, unsigned sequence) override
    { 
        PROTECTED_GETRESULT(stepname, sequence, "VarString", "string",
            SCMStringBuffer result;
            r->getResultString(result, false);
            return result.s.detach();
        );
    }
    virtual UChar * getResultVarUnicode(const char * stepname, unsigned sequence) override
    {
        PROTECTED_GETRESULT(stepname, sequence, "VarUnicode", "unicode",
            MemoryBuffer result;
            r->getResultUnicode(MemoryBuffer2IDataVal(result));
            result.append((UChar)0);
            return (UChar *)result.detach();
        );
    }
    virtual unsigned getResultHash(const char * name, unsigned sequence) override
    { 
        PROTECTED_GETRESULT(name, sequence, "Hash", "hash",
            return r->getResultHash();
        );
    }
    virtual unsigned getExternalResultHash(const char * wuid, const char * stepname, unsigned sequence) override
    {
        try
        {
            DBGLOG("getExternalResultRaw %s", stepname);

            Owned<IConstWUResult> r = getExternalResult(wuid, stepname, sequence);
            return r->getResultHash();
        }
        catch (CATCHALL)
        {
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "Failed to retrieve external data hash %s from workunit %s", stepname, wuid);
        }
    }
    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * stepname, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override
    {
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Rowset", "rowset",
            MemoryBuffer datasetBuffer;
            MemoryBuffer2IDataVal result(datasetBuffer);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
            Owned<IOutputRowDeserializer> deserializer = _rowAllocator->createDiskDeserializer(this);
            rtlDataset2RowsetX(tcount, tgt, _rowAllocator, deserializer, datasetBuffer.length(), datasetBuffer.toByteArray(), isGrouped);
        );
    }
    virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override
    {
        tcount = 0;
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Dictionary", "dictionary",
            MemoryBuffer datasetBuffer;
            MemoryBuffer2IDataVal result(datasetBuffer);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
            Owned<IOutputRowDeserializer> deserializer = _rowAllocator->createDiskDeserializer(this);
            rtlDeserializeDictionary(tcount, tgt, _rowAllocator, deserializer, datasetBuffer.length(), datasetBuffer.toByteArray());
        );
    }
    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override
    {
        tgt = NULL;
        try
        {
            DBGLOG("getExternalResultRaw %s", stepname);

            Variable2IDataVal result(&tlen, &tgt);
            Owned<IConstWUResult> r = getExternalResult(wuid, stepname, sequence);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
        }
        catch (CATCHALL)
        {
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "Failed to retrieve external data value %s from workunit %s", stepname, wuid);
        }
    }
    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source) override
    {
        addWuExceptionEx(text, code, severity, MSGAUD_programmer, source);
    }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) override
    {
        DBGLOG("%s", text);
        try
        {
            Owned<IWorkUnit> w = updateWorkUnit();
            unsigned activity = 0;
            addExceptionToWorkunit(w, SeverityError, "user", code, text, filename, lineno, column, activity);
        }
        catch (IException *E)
        {
            StringBuffer m;
            E->errorMessage(m);
            IERRLOG("Unable to record exception in workunit: %s", m.str());
            E->Release();
        }
        catch (...)
        {
            IERRLOG("Unable to record exception in workunit: unknown exception");
        }
        if (isAbort)
            throw makeStringException(MSGAUD_user, code, text);
    }
    virtual unsigned __int64 getFileOffset(const char *logicalName) override { assertex(false); return 0; }
    virtual unsigned getNodes() override { return jobChannel.queryJob().querySlaves(); }
    virtual unsigned getNodeNum() override { throw MakeThorException(0, "Unsupported. getNodeNum() called in master"); return (unsigned)-1; }
    virtual char *getFilePart(const char *logicalName, bool create=false) override { assertex(false); return NULL; }
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash) override
    {
        StringBuffer fullname('~');
        expandLogicalName(fullname, name);
        Owned<IDistributedFile> iDfsFile = queryThorFileManager().lookup(jobChannel.queryJob(), fullname, AccessMode::readMeta, false, true, false, defaultPrivilegedUser); // NB: do not update accessed
        if (iDfsFile.get())
        {
            // NB: if the file or any of the subfiles are compressed, then we cannot rely on the checksum
            // use the hash of the modified time instead (which is what roxie and hthor do for all files).
            bool useModifiedTime = false;
            IDistributedSuperFile * super = iDfsFile->querySuperFile();
            if (super)
            {
                Owned<IDistributedFileIterator> iter = super->getSubFileIterator(true);
                ForEach(*iter)
                {
                    if (iter->query().isCompressed())
                    {
                        useModifiedTime = true;
                        break;
                    }
                }
            }
            else if (iDfsFile->isCompressed())
                useModifiedTime = true;

            unsigned checkSum = 0;
            if (!useModifiedTime && iDfsFile->getFileCheckSum(checkSum))
                hash ^= checkSum;
            else
                hash = crcLogicalFileTime(iDfsFile, hash, fullname);
        }
        return hash;
    }
    virtual IDistributedFileTransaction *querySuperFileTransaction() override
    {
        if (!superfiletransaction.get())
            superfiletransaction.setown(createDistributedFileTransaction(userDesc, this));
        return superfiletransaction.get();
    }
    virtual char *getJobName() override
    {
        return strdup(workunit->queryJobName());
    }
    virtual char *getClusterName() override
    {
        return strdup(workunit->queryClusterName());
    }
    virtual char *getGroupName() override
    {
        StringBuffer out;
        if (globals)
            globals->getProp("@nodeGroup",out); 
        return out.detach();
    }
// ICodeContextExt impl.
    virtual IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence) override
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IConstWorkUnit> externalWU = factory->openWorkUnit(wuid);
        if (!externalWU)
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "workunit %s not found, retrieving value %s", wuid, name);
        externalWU->remoteCheckAccess(userDesc, false);
        return getWorkUnitResult(externalWU, name, sequence);
    }
    virtual IConstWUResult *getResultForGet(const char *name, unsigned sequence) override
    {
        Owned<IConstWUResult> r = getResult(name, sequence);
        if (!r || (r->getResultStatus() == ResultStatusUndefined))
        {
            StringBuffer s;
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "value %s in workunit is undefined", getResultText(s,name,sequence));
        }
        return r.getClear();
    }
    virtual IWorkUnit *updateWorkUnit() const override
    {
        return &workunit->lock();
    }
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned audience, const char * source) override
    {
        LOG(mapToLogMsgCategory((ErrorSeverity)severity, (MessageAudience)audience), "%s", text);
        try
        {
            Owned<IWorkUnit> w = updateWorkUnit();
            Owned<IWUException> we = w->createException();
            we->setSeverity((ErrorSeverity)severity);
            we->setExceptionMessage(text);
            we->setExceptionSource(source);
            if (code)
                we->setExceptionCode(code);
        }
        catch (IException *E)
        {
            StringBuffer m;
            E->errorMessage(m);
            IERRLOG("Unable to record exception in workunit: %s", m.str());
            E->Release();
        }
        catch (...)
        {
            IERRLOG("Unable to record exception in workunit: unknown exception");
        }
    }
};

class CThorCodeContextMasterSharedMem : public CThorCodeContextMaster
{
    IThorAllocator *sharedAllocator;
public:
    CThorCodeContextMasterSharedMem(CJobChannel &jobChannel, IThorAllocator *_sharedAllocator, IConstWorkUnit &_workunit, ILoadedDllEntry &querySo, IUserDescriptor &userDesc)
        : CThorCodeContextMaster(jobChannel, _workunit, querySo, userDesc)
    {
        sharedAllocator = _sharedAllocator;
    }
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
    {
        return sharedAllocator->getRowAllocator(meta, activityId);
    }
};


/////////////

//
// CJobMaster
//

void loadPlugin(SafePluginMap *pluginMap, const char *_path, const char *name)
{
    StringBuffer path(_path);
    path.append(name);

    pluginMap->addPlugin(path.str(), name);
}

CJobMaster::CJobMaster(IConstWorkUnit &_workunit, const char *graphName, ILoadedDllEntry *querySo, bool _sendSo, const SocketEndpoint &_agentEp)
    : CJobBase(querySo, graphName), workunit(&_workunit), sendSo(_sendSo), agentEp(_agentEp)
{
    SCMStringBuffer _token, _scope;
    workunit->getScope(_scope);
    workunit->getWorkunitDistributedAccessToken(_token);
    wuid.set(workunit->queryWuid());
    user.set(workunit->queryUser());
    token.append(_token.str());
    scope.append(_scope.str());
    numChannels = 1;
    init();

    Owned<IProperties> traceHeaders = extractTraceDebugOptions(workunit);
    OwnedActiveSpanScope requestSpan = queryTraceManager().createServerSpan("run_graph", traceHeaders);
    ContextSpanScope spanScope(*logctx, requestSpan);
    requestSpan->setSpanAttribute("hpcc.wuid", workunit->queryWuid());
    requestSpan->setSpanAttribute("hpcc.graph", graphName);

    resumed = WUActionResume == workunit->getAction();
    fatalHandler.setown(new CFatalHandler(globals->getPropInt("@fatal_timeout", FATAL_TIMEOUT)));
    querySent = spillsSaved = false;

    StringBuffer pluginsDir;
    globals->getProp("@pluginsPath", pluginsDir);
    if (pluginsDir.length())
        addPathSepChar(pluginsDir);
    Owned<IConstWUPluginIterator> pluginIter = &workunit->getPlugins();
    ForEach(*pluginIter)
    {
        IConstWUPlugin &plugin = pluginIter->query();
        SCMStringBuffer name;
        plugin.getPluginName(name);
        loadPlugin(pluginMap, pluginsDir.str(), name.str());
    }

    applyMemorySettings("manager");
    sharedAllocator.setown(::createThorAllocator(queryMemoryMB, 0, 1, memorySpillAtPercentage, *logctx, crcChecking, usePackedAllocator));
    Owned<IMPServer> mpServer = getMPServer();
    CJobChannel *channel = addChannel(mpServer);
    channel->reservePortKind(TPORT_mp); 
    channel->reservePortKind(TPORT_watchdog);
    channel->reservePortKind(TPORT_debug);

    slavemptag = allocateMPTag();
    slaveMsgHandler.setown(new CSlaveMessageHandler(*this, slavemptag));
    tmpHandler.setown(createTempHandler(true));
    xgmml.set(graphXGMML);

    StringBuffer tempDir(globals->queryProp("@thorTempDirectory"));
#ifdef _CONTAINERIZED
    // multiple thor jobs can be running on same node, sharing same local disk for temp storage.
    // make unique by adding wuid+graphName+worker-num
    VStringBuffer uniqueSubDir("%s_%s_0", workunit->queryWuid(), graphName); // 0 denotes master (workers = 1..N)
    SetTempDir(tempDir, uniqueSubDir, "thtmp", false); // no point in clearing as dir. is unique per job+graph
#else
    // in bare-metal where only 1 Thor instance of each name, re-use same dir, in order to guarantee it will be cleared on startup
    VStringBuffer uniqueSubDir("%s_0", globals->queryProp("@name")); // 0 denotes master (workers = 1..N)
    SetTempDir(tempDir, uniqueSubDir, "thtmp", true);
#endif
}

void CJobMaster::endJob()
{
    if (jobEnded)
        return;

    slaveMsgHandler.clear();
    freeMPTag(slavemptag);
    tmpHandler.clear();
    PARENT::endJob();
}

CJobChannel *CJobMaster::addChannel(IMPServer *mpServer)
{
    CJobChannel *channel = new CJobMasterChannel(*this, mpServer, jobChannels.ordinality());
    jobChannels.append(*channel);
    return channel;
}


static IException *createBCastException(unsigned slave, const char *errorMsg)
{
    // think this should always be fatal, could check link down here, or in general and flag as _shutdown.
    StringBuffer msg("General failure communicating to slave");
    if (slave)
        msg.append("(").append(slave).append(") ");
    else
        msg.append("s ");
    Owned<IThorException> e = MakeThorException(0, "%s", msg.append(" [").append(errorMsg).append("]").str());
    e->setAction(tea_shutdown);
    return e.getClear();
}

mptag_t CJobMaster::allocateMPTag()
{
    mptag_t tag = allocateClusterMPTag();
    queryJobChannel(0).queryJobComm().flush(tag);
    LOG(MCthorDetailedDebugInfo, "allocateMPTag: tag = %d", (int)tag);
    return tag;
}

void CJobMaster::freeMPTag(mptag_t tag)
{
    if (TAG_NULL != tag)
    {
        freeClusterMPTag(tag);
        LOG(MCthorDetailedDebugInfo, "freeMPTag: tag = %d", (int)tag);
        queryJobChannel(0).queryJobComm().flush(tag);
    }
}

void CJobMaster::broadcast(ICommunicator &comm, CMessageBuffer &msg, mptag_t mptag, unsigned timeout, const char *errorMsg, CReplyCancelHandler *msgHandler, bool sendOnly, bool aborting)
{
    unsigned groupSizeExcludingMaster = comm.queryGroup().ordinality() - 1;

    mptag_t replyTag = TAG_NULL;
    if (!sendOnly)
    {
        replyTag = queryJobChannel(0).queryMPServer().createReplyTag();
        msg.setReplyTag(replyTag);
    }
    auto f = [&](unsigned r) -> bool
    {
        if (!aborting || comm.verifyConnection(r, (unsigned)-1, false))
        {
            if (!comm.send(msg, r, mptag, timeout))
                throw createBCastException(r, errorMsg);
            return true;
        }
        return false;
    };
    std::vector<std::future<bool>> sendResultsFuture;
    Owned<IException> sendExcept;
    for (unsigned dstrank=1; dstrank<=groupSizeExcludingMaster; ++dstrank)
        sendResultsFuture.push_back(std::async(f, dstrank));
    unsigned numberMsgSent = 0;
    for (auto & sendResultFuture: sendResultsFuture)
    {
        try
        {
            if (sendResultFuture.get())
                ++numberMsgSent;
        }
        catch (IException *e)
        {
            sendExcept.setownIfNull(e);
        }
    }
    if (sendExcept)
    {
        IWARNLOG(sendExcept, "broadcastSendAsync");
        abort(sendExcept);
        throw sendExcept.getClear();
    }

    if (sendOnly) return;
    unsigned respondents = 0;
    Owned<IBitSet> bitSet = createThreadSafeBitSet();
    for (;;)
    {
        rank_t sender;
        CMessageBuffer msg;
        bool r = msgHandler ? msgHandler->recv(comm, msg, RANK_ALL, replyTag, &sender, LONGTIMEOUT)
                            : comm.recv(msg, RANK_ALL, replyTag, &sender, LONGTIMEOUT);
        if (!r)
        {
            StringBuffer tmpStr;
            if (errorMsg)
                tmpStr.append(": ").append(errorMsg).append(" - ");
            tmpStr.append("Timeout receiving from slaves - no reply from: [");
            unsigned s = bitSet->scan(0, false);
            assertex(s<querySlaves()); // must be at least one
            tmpStr.append(s+1);
            for (;;)
            {
                s = bitSet->scan(s+1, false);
                if (s>=querySlaves())
                    break;
                tmpStr.append(",").append(s+1);
            }
            tmpStr.append("]");
            Owned<IException> e = MakeThorFatal(NULL, 0, " %s", tmpStr.str());
            IWARNLOG(e);
            throw e.getClear();
        }
        bool error;
        msg.read(error);
        if (error)
        {
            Owned<IThorException> e = deserializeThorException(msg);
            e->setSlave(sender);
            throw e.getClear();
        }
        ++respondents;
        bitSet->set((unsigned)sender-1);
        if (respondents == numberMsgSent)
            break;
    }
}

IPropertyTree *CJobMaster::prepareWorkUnitInfo()
{
    Owned<IPropertyTree> workUnitInfo = createPTree("workUnitInfo");
    workUnitInfo->setProp("wuid", wuid);
    workUnitInfo->setProp("user", user);
    workUnitInfo->setProp("token", token);
    workUnitInfo->setProp("scope", scope);

    Owned<IConstWUPluginIterator> pluginIter = &queryWorkUnit().getPlugins();
    IPropertyTree *plugins = NULL;
    ForEach(*pluginIter)
    {
        IConstWUPlugin &thisplugin = pluginIter->query();
        if (!plugins)
            plugins = workUnitInfo->addPropTree("plugins", createPTree());
        SCMStringBuffer name;
        thisplugin.getPluginName(name);
        IPropertyTree *plugin = plugins->addPropTree("plugin", createPTree());
        plugin->setProp("@name", name.str());
    }
    IPropertyTree *debug = workUnitInfo->addPropTree("Debug", createPTree(ipt_caseInsensitive));
    SCMStringBuffer debugStr, valueStr;
    Owned<IStringIterator> debugIter = &queryWorkUnit().getDebugValues();
    ForEach (*debugIter)
    {
        debugIter->str(debugStr);
        queryWorkUnit().getDebugValue(debugStr.str(), valueStr);
        debug->setProp(debugStr.str(), valueStr.str());
    }
    return workUnitInfo.getClear();
}

void CJobMaster::sendQuery()
{
    CriticalBlock b(sendQueryCrit);
    if (querySent) return;
    CMessageBuffer tmp;
    tmp.append(slavemptag);
    tmp.append(queryWuid());
    tmp.append(graphName);
    const char *soName = queryDllEntry().queryName();
    DBGLOG("Query dll: %s", soName);
    tmp.append(soName);
    if (getExpertOptBool("saveQueryDlls"))
    {
        tmp.append(sendSo);
        if (sendSo)
        {
            CTimeMon atimer;
            OwnedIFile iFile = createIFile(soName);
            OwnedIFileIO iFileIO = iFile->open(IFOread);
            size32_t sz = (size32_t)iFileIO->size();
            tmp.append(sz);
            read(iFileIO, 0, sz, tmp);
            DBGLOG("Loading query for serialization to slaves took %d ms", atimer.elapsed());
        }
        queryJobManager().addCachedSo(soName);
    }
    Owned<IPropertyTree> deps = createPTree(queryXGMML()->queryName());
    Owned<IPropertyTreeIterator> edgeIter = queryXGMML()->getElements("edge"); // JCSMORE trim to those actually needed
    ForEach (*edgeIter)
    {
        IPropertyTree &edge = edgeIter->query();
        deps->addPropTree("edge", LINK(&edge));
    }
    Owned<IPropertyTree> workUnitInfo = prepareWorkUnitInfo();
    workUnitInfo->serialize(tmp);
    deps->serialize(tmp);

    CMessageBuffer msg;
    msg.append(QueryInit);
    compressToBuffer(msg, tmp.length(), tmp.toByteArray());

    CTimeMon queryToSlavesTimer;
    querySent = true;
    broadcast(queryNodeComm(), msg, managerWorkerMpTag, LONGTIMEOUT, "sendQuery");
    DBGLOG("Serialization of query init info (%d bytes) to slaves took %d ms", msg.length(), queryToSlavesTimer.elapsed());
}

void CJobMaster::jobDone()
{
    if (!querySent) return;

    if (isContainerized())
    {
        if (hasMask(jobInfoCaptureBehaviour, JobInfoCaptureBehaviour::always) ||
        (aborted && hasMask(jobInfoCaptureBehaviour, JobInfoCaptureBehaviour::onFailure)))
        {
            captureJobInfo(queryWorkUnit(), JobInfoCaptureType::logs);
        }
    }
    // else - BM does not setup a postmortem log handler, and all logging is typically local and to persistent storage

    CMessageBuffer msg;
    msg.append(QueryDone);
    msg.append(queryKey());
    broadcast(queryNodeComm(), msg, managerWorkerMpTag, LONGTIMEOUT, "jobDone");
}

void CJobMaster::saveSpills()
{
    CriticalBlock b(spillCrit);
    if (spillsSaved)
        return;
    spillsSaved = true;

    PROGLOG("Paused, saving spills..");
    assertex(!queryUseCheckpoints()); // JCSMORE - checkpoints probably need revisiting

    unsigned numSavedSpills = 0;
    // stash away spills ready for resume, make them owned by workunit in event of abort/delete
    IGraphTempHandler *tempHandler = queryTempHandler(false);
    if (tempHandler)
    {
        Owned<IFileUsageIterator> iter = tempHandler->getIterator();
        ForEach(*iter)
        {
            CFileUsageEntry &entry = iter->query();
            StringAttr tmpName = entry.queryName();
            if (WUGraphComplete == workunit->queryNodeState(queryGraphName(), entry.queryGraphId()))
            {
                IArrayOf<IGroup> groups;
                StringArray clusters;
                fillClusterArray(*this, tmpName, clusters, groups);
                Owned<IFileDescriptor> fileDesc = queryThorFileManager().create(*this, tmpName, clusters, groups, true, TDXtemporary|TDWnoreplicate);
                fileDesc->queryProperties().setPropBool("@pausefile", true); // JCSMORE - mark to keep, may be able to distinguish via other means
                fileDesc->queryProperties().setProp("@kind", "flat");

                IPropertyTree &props = fileDesc->queryProperties();
                props.setPropBool("@owned", true);
                bool blockCompressed=true; // JCSMORE, should come from helper really
                if (blockCompressed)
                    props.setPropBool("@blockCompressed", true);

                Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fileDesc);

                // NB: This is renaming/moving from temp path

                StringBuffer newName;
                queryThorFileManager().addScope(*this, tmpName, newName, true, true);
                verifyex(file->renamePhysicalPartFiles(newName.str(), NULL, NULL, queryBaseDirectory(grp_unknown)));

                file->attach(newName,userDesc);

                Owned<IWorkUnit> wu = &queryWorkUnit().lock();
                wu->addFile(newName, &clusters, entry.queryUsage(), entry.queryKind(), queryGraphName());
                ++numSavedSpills;
            }
        }
    }
    PROGLOG("Paused, %d spill(s) saved.", numSavedSpills);
}

void CJobMaster::captureJobInfo(IConstWorkUnit &wu, JobInfoCaptureType flags)
{
    StringBuffer dir;
    if (!getConfigurationDirectory(globals->queryPropTree("Directories"), "debug", "thor", globals->queryProp("@name"), dir))
    {
        if (!isContainerized())
        {
            appendCurrentDirectory(dir, false);
            addPathSepChar(dir);
            dir.append("debuginfo"); // use ./debuginfo in non-containerized mode
        }
        else
        {
            IWARNLOG("Failed to get debug directory");
            return;
        }
    }
    addPathSepChar(dir);
    dir.append(queryWuid());
    addPathSepChar(dir);
    CDateTime now;
    timestamp_type nowTime = getTimeStampNowValue();
    now.setTimeStamp(nowTime);
    unsigned year, month, day, hour, minute, second, nano;
    now.getDate(year, month, day);
    now.getTime(hour, minute, second, nano);
    unsigned hundredths = ((unsigned __int64)nano) * 100 / 1000000000;
    VStringBuffer dateStr("%04u%02u%02u-%02u%02u%02u.%02u", year, month, day, hour, minute, second, hundredths);
    dir.append(dateStr);

    auto managerCaptureFunc = [this, &dir, &flags]()
    {
        StringBuffer instanceDir(dir);
        std::vector<std::string> capturedFiles;
        if (isContainerized())
        {
            addInstanceContextPaths(instanceDir);
            if (hasMask(flags, JobInfoCaptureType::logs))
            {
                StringBuffer logFilename(instanceDir);
                addPathSepChar(logFilename);
                logFilename.append("thormanager.log");
                copyPostMortemLogging(logFilename, hasMask(jobInfoCaptureBehaviour, JobInfoCaptureBehaviour::clearLogs));
                capturedFiles.push_back(logFilename.str());
            }
        }
        if (hasMask(flags, JobInfoCaptureType::stacks))
        {
            std::vector<std::string> result = captureDebugInfo(instanceDir, "thormanager", nullptr);
            capturedFiles.insert(capturedFiles.end(), result.begin(), result.end());
        }
        return capturedFiles;
    };
    std::future<std::vector<std::string>> managerResultsFuture = std::async(std::launch::async, managerCaptureFunc);

    std::vector<std::string> capturedFiles;
    auto responseFunc = [&capturedFiles](unsigned worker, MemoryBuffer &mb)
    {
        bool res;
        mb.read(res);
        if (!res)
        {
            Owned<IException> e = deserializeException(mb);
            VStringBuffer msg("Failed to get debug info from worker %u", worker);
            IWARNLOG(e, msg);
        }
        StringAttr file;
        while (true)
        {
            mb.read(file);
            if (file.isEmpty())
                break;
            capturedFiles.push_back(file.get());
        }
    };
    VStringBuffer cmd("<debuginfo dir=\"%s\" flags=\"%u\"/>", dir.str(), (byte)flags);
    issueWorkerDebugCmd(cmd, 0, responseFunc);
    std::vector<std::string> managerResults = managerResultsFuture.get();
    capturedFiles.insert(capturedFiles.end(), managerResults.begin(), managerResults.end());

    VStringBuffer description("debuginfo-%s", dateStr.str());
    if (isContainerized())
    {
        VStringBuffer archiveFilename("debuginfo-%s.tar.gz", dateStr.str());
        VStringBuffer tarCmd("cd %s && tar -czf %s --exclude=%s --remove-files *", dir.str(), archiveFilename.str(), archiveFilename.str());
        if (0 != system(tarCmd))
        {
            OWARNLOG("Failed to create tarball of debuginfo");
            return;
        }
        Owned<IWorkUnit> lw = &wu.lock();
        Owned<IWUQuery> query = lw->updateQuery();
        VStringBuffer archiveFilePath("%s/%s", dir.str(), archiveFilename.str());
        query->addAssociatedFile(FileTypePostMortem, archiveFilePath, "localhost", description, 0, 0, 0);
    }
    else
    {
        Owned<IWorkUnit> lw = &wu.lock();
        Owned<IWUQuery> query = lw->updateQuery();
        for (auto &file: capturedFiles)
        {
            RemoteFilename rfn;
            rfn.setRemotePath(file.c_str());
            StringBuffer localPath;
            rfn.getLocalPath(localPath);
            StringBuffer host;
            rfn.queryEndpoint().getEndpointHostText(host);
            query->addAssociatedFile(FileTypeLog, localPath, host, description, 0, 0, 0);
        }
    }
}

bool CJobMaster::go()
{
    // detected abort conditions or pause state changes.
    class CWorkunitStateChangeHandler : public CInterface, implements IWorkUnitSubscriber
    {
        CJobMaster &job;
        IConstWorkUnit &wu;
        Owned<IWorkUnitWatcher> watcher;
        CriticalSection crit;
    public:
        IMPLEMENT_IINTERFACE;

        CWorkunitStateChangeHandler(CJobMaster &_job, IConstWorkUnit &_wu) : job(_job), wu(_wu)
        {
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            watcher.setown(factory->getWatcher(this, (WUSubscribeOptions) (SubscribeOptionAction | SubscribeOptionAbort), wu.queryWuid()));
        }
        ~CWorkunitStateChangeHandler() { stop(); }
        void stop()
        {
            Owned<IWorkUnitWatcher> _watcher;
            {
                CriticalBlock b(crit);
                if (!watcher)
                    return;
                _watcher.setown(watcher.getClear());
            }
            _watcher->unsubscribe();
        }
        virtual void notify(WUSubscribeOptions flags, unsigned valueLen, const void *valueData) override
        {
            CriticalBlock b(crit);
            if (!watcher)
                return;
            if (flags & SubscribeOptionAbort)
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                if (factory->isAborting(wu.queryWuid()))
                {
                    DBGLOG("ABORT detected from user");

                    unsigned code = TE_WorkUnitAborting; // default
                    if (job.getOptBool("dumpInfoOnUserAbort", false))
                        code = TE_WorkUnitAbortingDumpInfo;
                    else if ((1 == valueLen) && ('2' == *((const char *)valueData)))
                    {
                        /* NB: Standard abort mechanism will trigger abort subscriber with "1"
                         * If "2" is signalled, it means - Abort with dump info.
                         */
                        code = TE_WorkUnitAbortingDumpInfo;
                    }
                    Owned <IException> e = MakeThorException(code, "User signalled abort");
                    job.fireException(e);
                }
                else
                    PROGLOG("CWorkunitStateChangeHandler [SubscribeOptionAbort] notifier called, workunit was not aborting");
            }
            if (flags & SubscribeOptionAction)
            {
                job.markWuDirty();
                wu.forceReload();
                WUAction action = wu.getAction();
                if ((WUActionPause==action) || (WUActionPauseNow==action))
                {
                    // pause after current subgraph
                    bool abort = (action==WUActionPauseNow); // abort current subgraph
                    PROGLOG("Pausing job%s", abort?" [now]":"");
                    job.pause(abort);
                }
                else if (action==WUActionGenerateDebugInfo)
                    job.captureJobInfo(wu, JobInfoCaptureType::logs|JobInfoCaptureType::stacks);
            }
        }
    } workunitStateChangeHandler(*this, *workunit);
    class CQueryTimeoutHandler : public CTimeoutTrigger
    {
        CJobMaster &job;
    public:
        CQueryTimeoutHandler(CJobMaster &_job, unsigned timeout) : CTimeoutTrigger(timeout, "QUERY"), job(_job)
        {
            start();
            inform(MakeThorException(TE_QueryTimeoutError, "Query took greater than %d seconds", timeout));
        }
        virtual bool action() override
        {
            job.fireException(exception);
            return true;
        }
    };
    class CAgentSessionWatcher : implements IThreaded
    {
        CJobMaster &job;
        SessionId agentSessionID = -1;
        unsigned periodMs = 0;
        CThreaded threaded;
        std::atomic<bool> stopped{true};
        Semaphore sem;
    public:
        CAgentSessionWatcher(CJobMaster &_job, unsigned _periodMs) : job(_job), periodMs(_periodMs), threaded("AgentSessionWatcher", this)
        {
            if (queryDaliServerVersion().compare("2.1")>=0)
            {
                agentSessionID = job.queryWorkUnit().getAgentSession();
                if (agentSessionID <= 0)
                    throw MakeThorException(0, "Unexpected: job has an invalid agent sesssion id: %" I64F "d", agentSessionID);
                stopped = false;
                threaded.start(true);
            }
        }
        ~CAgentSessionWatcher()
        {
            stop();
        }
        void stop()
        {
            if (stopped)
                return;
            stopped = true;
            sem.signal();
            threaded.join();
        }
        virtual void threadmain() override
        {
            while (!stopped)
            {
                if (sem.wait(periodMs))
                    break; // signaled by dtor/abort
                if (querySessionManager().sessionStopped(agentSessionID, 0))
                {
                    DBGLOG("Agent session stopped");
                    job.fireException(MakeThorException(0, "Agent session stopped"));
                    break;
                }
            }
        }
    } agentSessionWatcher(*this, 60000); // check every minute
    Owned<CTimeoutTrigger> qtHandler;
    int guillotineTimeout = workunit->getDebugValueInt("maxRunTime", 0);
    if (guillotineTimeout > 0)
        qtHandler.setown(new CQueryTimeoutHandler(*this, guillotineTimeout));
    else if (guillotineTimeout < 0)
    {
        Owned<IException> e = MakeStringException(0, "Ignoring negative maxRunTime: %d", guillotineTimeout);
        reportExceptionToWorkunit(*workunit, e);
    }
    if (WUActionPause == workunit->getAction() || WUActionPauseNow == workunit->getAction())
        throw MakeStringException(0, "Job paused at start, exiting");

    bool allDone = true;
    unsigned concurrentSubGraphs = (unsigned)getWorkUnitValueInt("concurrentSubGraphs", globals->getPropInt("@concurrentSubGraphs", 1));
    try
    {
        startJob();
        workunit->setGraphState(queryGraphName(), getWfid(), WUGraphRunning);
        Owned<IThorGraphIterator> iter = queryJobChannel(0).getSubGraphs();
        CICopyArrayOf<CMasterGraph> toRun;
        ForEach(*iter)
        {
            CMasterGraph &graph = (CMasterGraph &)iter->query();
            if ((queryResumed() || queryUseCheckpoints()) && WUGraphComplete == workunit->queryNodeState(queryGraphName(), graph.queryGraphId()))
                graph.setCompleteEx();
            else
                toRun.append(graph);
        }
        ForEachItemInRev(g, toRun)
        {
            if (aborted) break;
            CMasterGraph &graph = toRun.item(g);
            if (graph.isSink())
                graph.execute(0, NULL, true, concurrentSubGraphs>1);
            if (queryPausing()) break;
        }
        queryJobChannel(0).wait();
        workunitStateChangeHandler.stop();
        agentSessionWatcher.stop();
        ForEachItemIn(tr, toRun)
        {
            CMasterGraph &graph = toRun.item(tr);
            if (!graph.isComplete())
            {
                allDone = false;
                break;
            }
        }
    }
    catch (IException *e)
    {
        Owned<IThorException> te = ThorWrapException(e, "Error running sub graphs");
        e->Release();
        fireException(te);
    }
    catch (CATCHALL) { Owned<IException> e = MakeThorException(0, "Unknown exception running sub graphs"); fireException(e); }
    workunit->setGraphState(queryGraphName(), getWfid(), aborted?WUGraphFailed:(allDone?WUGraphComplete:(pausing?WUGraphPaused:WUGraphComplete)));

    if (queryPausing())
        saveSpills();

    Owned<IException> jobDoneException;
    try { jobDone(); }
    catch (IException *e)
    {
        jobDoneException.setown(ThorWrapException(e, "Error in jobDone"));
        e->Release();
        IWARNLOG(jobDoneException);
    }
    queryTempHandler()->clearTemps();
    slaveMsgHandler->stop();
    if (jobDoneException.get())
        throw LINK(jobDoneException);
    return allDone;
}

void CJobMaster::pause(bool doAbort)
{
    pausing = true;
    if (doAbort)
    {
        // reply will trigger DAMP_THOR_REPLY_PAUSED to agent, unless all graphs are already complete.
        queryJobManager().replyException(*this, NULL);

        // abort current graph asynchronously.
        // After spill files have been saved, trigger timeout handler in case abort doesn't succeed.
        Owned<IException> e = MakeThorException(0, "Unable to recover from pausenow");
        class CAbortThread : implements IThreaded
        {
            CJobMaster &owner;
            CThreaded threaded;
            Linked<IException> exception;
        public:
            CAbortThread(CJobMaster &_owner, IException *_exception) : owner(_owner), exception(_exception), threaded("SaveSpillThread", this)
            {
                threaded.start(true);
            }
            ~CAbortThread()
            {
                threaded.join();
            }
        // IThreaded
            virtual void threadmain() override
            {
                owner.abort(exception);
            }
        } abortThread(*this, e);
        saveSpills();
        fatalHandler->inform(e.getClear());
    }
}

void CJobMaster::issueWorkerDebugCmd(const char *rawText, unsigned workerNum, std::function<void(unsigned, MemoryBuffer &mb)> responseFunc)
{
    mptag_t replyTag = createReplyTag();
    ICommunicator &comm = queryNodeComm();
    CMessageBuffer mbuf;
    mbuf.append(DebugRequest);
    mbuf.append(queryKey());
    serializeMPtag(mbuf, replyTag);
    mbuf.append(rawText);
    rank_t rank = workerNum ? workerNum : RANK_ALL_OTHER; // 0 == all workers
    if (!comm.send(mbuf, rank, managerWorkerMpTag, MP_ASYNC_SEND))
    {
        DBGLOG("Failed to send debug info to slave");
        throwUnexpected();
    }

    rank = workerNum ? workerNum : RANK_ALL;
    unsigned numToRecv = workerNum ? 1 : queryNodes();
    while (numToRecv)
    {
        rank_t sender;
        mbuf.clear();
        unsigned recvTimeoutCount = 0;
        while (!comm.recv(mbuf, rank, replyTag, &sender, SHORTTIMEOUT))
        {
            if (queryAborted())
                return;
            ++recvTimeoutCount;
            if (recvTimeoutCount == 10)
                throw makeStringExceptionV(0, "Timedout waiting for debugcmd response from worker %u", workerNum);
            IWARNLOG("Waiting for debugcmd response from worker %u", workerNum);
        }
        while (mbuf.remaining())
            responseFunc(sender, mbuf);

        numToRecv--;
    }
}

bool CJobMaster::queryCreatedFile(const char *file)
{
    StringBuffer scopedName;
    queryThorFileManager().addScope(*this, file, scopedName, false);
    return (NotFound != createdFiles.find(scopedName.str()));
}

void CJobMaster::addCreatedFile(const char *file)
{
    StringBuffer scopedName;
    queryThorFileManager().addScope(*this, file, scopedName, false);
    createdFiles.append(scopedName.str());
}

__int64 CJobMaster::getWorkUnitValueInt(const char *prop, __int64 defVal) const
{
    return queryWorkUnit().getDebugValueInt64(prop, defVal);
}

bool CJobMaster::getWorkUnitValueBool(const char *prop, bool defVal) const
{
    return queryWorkUnit().getDebugValueBool(prop, defVal);
}

double CJobMaster::getWorkUnitValueReal(const char *prop, double defVal) const
{
    return queryWorkUnit().getDebugValueReal(prop, defVal);
}

StringBuffer &CJobMaster::getWorkUnitValue(const char *prop, StringBuffer &str) const
{
    SCMStringBuffer scmStr;
    queryWorkUnit().getDebugValue(prop, scmStr);
    return str.append(scmStr.str());
}

IGraphTempHandler *CJobMaster::createTempHandler(bool errorOnMissing)
{
    return new CMasterGraphTempHandler(*this, errorOnMissing);
}

bool CJobMaster::fireException(IException *e)
{
    IThorException *te = QUERYINTERFACE(e, IThorException);
    ThorExceptionAction action;
    if (!te) action = tea_null;
    else action = te->queryAction();
    if (QUERYINTERFACE(e, IMP_Exception) && MPERR_link_closed==e->errorCode())
        action = tea_shutdown;
    else if (QUERYINTERFACE(e, ISEH_Exception))
        action = tea_shutdown;
    CriticalBlock b(exceptCrit);
    switch (action)
    {
        case tea_warning:
        {
            LOG(MCprogress, e);
            reportExceptionToWorkunitCheckIgnore(*workunit, e);
            break;
        }
        default:
        {
            LOG(MCprogress, e);
            queryJobManager().replyException(*this, e); 
            fatalHandler->inform(LINK(e));
            try { abort(e); }
            catch (IException *e)
            {
                Owned<IThorException> te = ThorWrapException(e, "Error aborting job, will cause thor restart");
                e->Release();
                reportExceptionToWorkunit(*workunit, te);
                action = tea_shutdown;
            }
            if (tea_shutdown == action)
                queryJobManager().stop();
        }
    }
    return true;
}

IFatalHandler *CJobMaster::clearFatalHandler()
{
    return fatalHandler.getClear();
}

// CJobMasterChannel

CJobMasterChannel::CJobMasterChannel(CJobBase &job, IMPServer *mpServer, unsigned channel) : CJobChannel(job, mpServer, channel)
{
    codeCtx.setown(new CThorCodeContextMaster(*this, job.queryWorkUnit(), job.queryDllEntry(), *job.queryUserDescriptor()));
    sharedMemCodeCtx.setown(new CThorCodeContextMasterSharedMem(*this, job.querySharedAllocator(), job.queryWorkUnit(), job.queryDllEntry(), *job.queryUserDescriptor()));
}

CGraphBase *CJobMasterChannel::createGraph()
{
    return new CMasterGraph(*this);
}

IBarrier *CJobMasterChannel::createBarrier(mptag_t tag)
{
    return new CBarrierMaster(*jobComm, tag);
}


///////////////////

class CCollatedResult : implements IThorResult, public CSimpleInterface
{
    CMasterGraph &graph;
    CActivityBase &activity;
    IThorRowInterfaces *rowIf;
    unsigned id;
    CriticalSection crit;
    PointerArrayOf<CThorExpandingRowArray> results;
    Owned<IThorResult> result;
    unsigned spillPriority;
    activity_id ownerId;

    void ensure()
    {
        CriticalBlock b(crit);
        if (result)
            return;
        mptag_t replyTag = graph.queryMPServer().createReplyTag();
        CMessageBuffer msg;
        msg.append(GraphGetResult);
        msg.append(activity.queryJob().queryKey());
        msg.append(graph.queryGraphId());
        msg.append(ownerId);
        msg.append(id);
        msg.append(replyTag);
        ((CJobMaster &)graph.queryJob()).broadcast(queryNodeComm(), msg, managerWorkerMpTag, LONGTIMEOUT, "CCollectResult", NULL, true);

        unsigned numSlaves = graph.queryJob().querySlaves();
        for (unsigned n=0; n<numSlaves; n++)
            results.item(n)->kill();
        rank_t sender;
        MemoryBuffer mb;
        Owned<IBufferedSerialInputStream> stream = createMemoryBufferSerialStream(mb);
        CThorStreamDeserializerSource rowSource(stream);
        unsigned todo = numSlaves;

        for (;;)
        {
            for (;;)
            {
                if (activity.queryAbortSoon())
                    return;
                msg.clear();
                if (activity.receiveMsg(msg, RANK_ALL, replyTag, &sender, 60*1000))
                    break;
                ActPrintLog(&activity, "WARNING: tag %d timedout, retrying", (unsigned)replyTag);
            }
            sender = sender - 1; // 0 = master
            if (!msg.length())
            {
                --todo;
                if (0 == todo)
                    break; // done
            }
            else
            {
                bool error;
                msg.read(error);
                if (error)
                {
                    Owned<IThorException> e = deserializeThorException(msg);
                    e->setSlave(sender);
                    throw e.getClear();
                }
                ThorExpand(msg, mb.clear());

                CThorExpandingRowArray *slaveResults = results.item(sender);
                while (!rowSource.eos())
                {
                    RtlDynamicRowBuilder rowBuilder(rowIf->queryRowAllocator());
                    size32_t sz = rowIf->queryRowDeserializer()->deserialize(rowBuilder, rowSource);
                    slaveResults->append(rowBuilder.finalizeRowClear(sz));
                }
            }
        }
        Owned<IThorResult> _result = ::createResult(activity, rowIf, thorgraphresult_nul, spillPriority);
        Owned<IRowWriter> resultWriter = _result->getWriter();
        for (unsigned s=0; s<numSlaves; s++)
        {
            CThorExpandingRowArray *slaveResult = results.item(s);
            ForEachItemIn(r, *slaveResult)
            {
                const void *row = slaveResult->query(r);
                LinkThorRow(row);
                resultWriter->putRow(row);
            }
        }
        result.setown(_result.getClear());
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCollatedResult(CMasterGraph &_graph, CActivityBase &_activity, IThorRowInterfaces *_rowIf, unsigned _id, activity_id _ownerId, unsigned _spillPriority)
        : graph(_graph), activity(_activity), rowIf(_rowIf), id(_id), ownerId(_ownerId), spillPriority(_spillPriority)
    {
        for (unsigned n=0; n<graph.queryJob().querySlaves(); n++)
            results.append(new CThorExpandingRowArray(activity, rowIf));
    }
    ~CCollatedResult()
    {
        ForEachItemIn(l, results)
        {
            CThorExpandingRowArray *result = results.item(l);
            delete result;
        }
    }
    void setId(unsigned _id)
    {
        id = _id;
    }

// IThorResult
    virtual IRowWriter *getWriter() { throwUnexpected(); }
    virtual void setResultStream(IRowWriterMultiReader *stream, rowcount_t count)
    {
        throwUnexpected();
    }
    virtual IRowStream *getRowStream()
    {
        ensure();
        return result->getRowStream();
    }
    virtual IThorRowInterfaces *queryRowInterfaces()
    {
        return rowIf;
    }
    virtual CActivityBase *queryActivity()
    {
        return &activity;
    }
    virtual bool isDistributed() const { return false; }
    virtual void serialize(MemoryBuffer &mb)
    {
        ensure();
        result->serialize(mb);
    }
    virtual void getLinkedResult(unsigned & count, const byte * * & ret) override
    {
        ensure();
        result->getLinkedResult(count, ret);
    }
    virtual const void * getLinkedRowResult()
    {
        ensure();
        return result->getLinkedRowResult();
    }
};

///////////////////

//
// CMasterGraph impl.
//

CMasterGraph::CMasterGraph(CJobChannel &jobChannel) : CGraphBase(jobChannel), graphStats(graphStatistics)
{
    jobM = (CJobMaster *)&jobChannel.queryJob();
    mpTag = queryJob().allocateMPTag();
    startBarrierTag = queryJob().allocateMPTag();
    waitBarrierTag = queryJob().allocateMPTag();
    startBarrier = jobChannel.createBarrier(startBarrierTag);
    waitBarrier = jobChannel.createBarrier(waitBarrierTag);
}


CMasterGraph::~CMasterGraph()
{
    queryJob().freeMPTag(mpTag);
    queryJob().freeMPTag(startBarrierTag);
    queryJob().freeMPTag(waitBarrierTag);
    if (TAG_NULL != doneBarrierTag)
        queryJob().freeMPTag(doneBarrierTag);
    if (TAG_NULL != executeReplyTag)
        queryJob().freeMPTag(executeReplyTag);
}

void CMasterGraph::init()
{
    CGraphBase::init();
    if (queryOwner() && isGlobal())
    {
        doneBarrierTag = queryJob().allocateMPTag();
        doneBarrier = jobChannel.createBarrier(doneBarrierTag);
    }
}

bool CMasterGraph::fireException(IException *e)
{
    IThorException *te = QUERYINTERFACE(e, IThorException);
    ThorExceptionAction action;
    if (!te) action = tea_null;
    else action = te->queryAction();
    if (QUERYINTERFACE(e, IMP_Exception) && MPERR_link_closed==e->errorCode())
        action = tea_shutdown;
    else if (QUERYINTERFACE(e, ISEH_Exception))
        action = tea_shutdown;
    CriticalBlock b(exceptCrit);
    switch (action)
    {
        case tea_warning:
        {
            LOG(MCprogress, e);
            reportExceptionToWorkunitCheckIgnore(job.queryWorkUnit(), e);
            break;
        }
        default:
        {
            LOG(MCprogress, e);
            if (NULL != fatalHandler)
                fatalHandler->inform(LINK(e));
            if (owner)
                owner->fireException(e);
            else
                queryJobChannel().fireException(e);
        }
    }
    return true;
}

void CMasterGraph::reset()
{
    CGraphBase::reset();
    bcastMsgHandler.reset();
    activityInitMsgHandler.reset();
    executeReplyMsgHandler.reset();
}

void CMasterGraph::abort(IException *e)
{
    if (aborted) return;
    if (initialized) // if aborted before initialized, there will be no slave activity to collect
        getFinalProgress(true);
    bool _graphDone = graphDone; // aborting master activities can trigger master graphDone, but want to fire GraphAbort to slaves if graphDone=false at start.
    bool dumpInfo = TE_WorkUnitAbortingDumpInfo == e->errorCode() || job.getOptBool("dumpInfoOnAbort");
    if (dumpInfo)
    {
        StringBuffer dumpInfoCmd;
        checkAndDumpAbortInfo(queryJob().getOpt("dumpInfoCmd", dumpInfoCmd));
    }
    try { CGraphBase::abort(e); }
    catch (IException *e)
    {
        GraphPrintLog(e, "Aborting master graph");
        e->Release();
    }
    bcastMsgHandler.cancel(0);
    activityInitMsgHandler.cancel(RANK_ALL);
    executeReplyMsgHandler.cancel(RANK_ALL);
    if (started && !_graphDone)
    {
        try
        {
            CMessageBuffer msg;
            msg.append(GraphAbort);
            msg.append(job.queryKey());
            msg.append(dumpInfo);
            msg.append(queryGraphId());
            jobM->broadcast(queryNodeComm(), msg, managerWorkerMpTag, LONGTIMEOUT, "abort");
        }
        catch (IException *e)
        {
            GraphPrintLog(e, "Aborting slave graph");
            if (abortException)
            {
                e->Release();
                throw LINK(abortException);
            }
            throw;
        }
    }
    if (!queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryJobManager().queryDeMonServer()->endGraph(this, !queryAborted());
    }
}

bool CMasterGraph::serializeActivityInitData(unsigned slave, MemoryBuffer &mb, IThorActivityIterator &iter)
{
    CriticalBlock b(createdCrit);
    DelayedSizeMarker sizeMark1(mb);
    ForEach (iter)
    {
        CMasterGraphElement &element = (CMasterGraphElement &)iter.query();
        if (!element.sentActInitData->testSet(slave))
        {
            CMasterActivity *activity = (CMasterActivity *)element.queryActivity();
            if (activity)
            {
                mb.append(element.queryId());
                DelayedSizeMarker sizeMark2(mb);
                activity->serializeSlaveData(mb, slave);
                sizeMark2.write();
            }
        }
    }
    if (0 == sizeMark1.size())
        return false;
    mb.append((activity_id)0); // terminator
    sizeMark1.write();
    return true;
}

void CMasterGraph::readActivityInitData(MemoryBuffer &mb, unsigned slave)
{
    for (;;)
    {
        activity_id id;
        mb.read(id);
        if (0 == id)
            break;
        size32_t dataLen;
        mb.read(dataLen);

        CGraphElementBase *element = queryElement(id);
        MemoryBuffer &mbDst = element->queryActivity()->queryInitializationData(slave);
        mbDst.append(dataLen, mb.readDirect(dataLen));
    }
}

bool CMasterGraph::prepare(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async)
{
    if (!CGraphBase::prepare(parentExtractSz, parentExtract, checkDependencies, shortCircuit, async)) return false;
    if (aborted) return false;
    return true;
}

void CMasterGraph::execute(size32_t _parentExtractSz, const byte *parentExtract, bool checkDependencies, bool async)
{
    if (isComplete())
        return;
    CThorPerfTracer perf;
    double perfinterval = 0.0;
    if (!queryOwner()) // owning graph sends query+child graphs
    {
        perfinterval = job.getOptReal("perfInterval");
        if (perfinterval)
            perf.start(job.queryWuid(), graphId, perfinterval);

        jobM->sendQuery(); // if not previously sent
    }
    CGraphBase::execute(parentExtractSz, parentExtract, checkDependencies, async);
    if (perfinterval)
        perf.stop();
}

void CMasterGraph::start()
{
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach (*iter)
        iter->query().queryActivity()->startProcess();
}

void CMasterGraph::sendActivityInitData()
{
    CMessageBuffer msg;
    mptag_t replyTag = queryJobChannel().queryMPServer().createReplyTag();
    msg.setReplyTag(replyTag);
    unsigned pos = msg.length();
    unsigned w=0;
    unsigned sentTo = 0;
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    for (; w<queryJob().querySlaves(); w++)
    {
        unsigned needActInit = 0;
        ForEach(*iter)
        {
            CGraphElementBase &element = iter->query();
            CActivityBase *activity = element.queryActivity();
            if (activity && activity->needReInit())
                element.sentActInitData->set(w, false); // force act init to be resent
            if (!element.sentActInitData->test(w)) // has it been sent
                ++needActInit;
        }
        if (needActInit)
        {
            try
            {
                msg.rewrite(pos);
                serializeActivityInitData(w, msg, *iter);
            }
            catch (IException *e)
            {
                GraphPrintLog(e);
                throw;
            }
            if (!queryJobChannel().queryJobComm().send(msg, w+1, mpTag, LONGTIMEOUT))
            {
                StringBuffer epStr;
                throw MakeStringException(0, "Timeout sending to slave %s", job.querySlaveGroup().queryNode(w).endpoint().getEndpointHostText(epStr).str());
            }
            ++sentTo;
        }
    }
    if (sentTo)
    {
        assertex(sentTo == queryJob().querySlaves());
        w=0;
        Owned<IException> e;
        // now get back initialization data from graph tag
        for (; w<queryJob().querySlaves(); w++)
        {
            rank_t sender;
            msg.clear();
            if (!activityInitMsgHandler.recv(queryJobChannel().queryJobComm(), msg, w+1, replyTag, &sender, LONGTIMEOUT))
                throw MakeGraphException(this, 0, "Timeout receiving from slaves after graph sent");

            bool error;
            msg.read(error);
            if (error)
            {
                Owned<IThorException> se = deserializeThorException(msg);
                se->setSlave(sender);
                if (!e.get())
                {
                    StringBuffer tmpStr("Slave ");
                    queryJob().queryJobGroup().queryNode(sender).endpoint().getEndpointHostText(tmpStr);
                    GraphPrintLog(se, "%s", tmpStr.append(": slave initialization error").str());
                    e.setown(se.getClear());
                }
                continue; // to read other slave responses.
            }
            readActivityInitData(msg, w);
        }
        if (e.get())
            throw LINK(e);
    }
}

void CMasterGraph::serializeGraphInit(MemoryBuffer &mb)
{
    mb.append(graphId);
    serializeMPtag(mb, mpTag);
    mb.append((int)startBarrierTag);
    mb.append((int)waitBarrierTag);
    mb.append((int)doneBarrierTag);
    sourceActDependents->serialize(mb);
    mb.append(queryChildGraphCount());
    Owned<IThorGraphIterator> childIter = getChildGraphIterator();
    ForEach (*childIter)
    {
        CMasterGraph &childGraph = (CMasterGraph &)childIter->query();
        childGraph.serializeGraphInit(mb);
    }
}

// IThorChildGraph impl.
IEclGraphResults *CMasterGraph::evaluate(unsigned _parentExtractSz, const byte *parentExtract)
{
    throw MakeGraphException(this, 0, "Thor master does not support the execution of child queries");
}

void CMasterGraph::executeSubGraph(size32_t parentExtractSz, const byte *parentExtract)
{
    if (job.queryResumed()) // skip complete subgraph if resuming. NB: temp spill have been tucked away for this purpose when paused.
    {
        if (!queryOwner())
        {
            if (WUGraphComplete == job.queryWorkUnit().queryNodeState(job.queryGraphName(), graphId))
                setCompleteEx();
        }
    }
    if (isComplete())
        return;
    if (!sentGlobalInit)
    {
        sentGlobalInit = true;
        if (!queryOwner())
            sendGraph();
        else
        {
            if (isGlobal())
            {
                CMessageBuffer msg;
                serializeCreateContexts(msg);
                try
                {
                    jobM->broadcast(queryJobChannel().queryJobComm(), msg, mpTag, LONGTIMEOUT, "serializeCreateContexts", &bcastMsgHandler);
                }
                catch (IException *e)
                {
                    GraphPrintLog(e, "Aborting graph create(2)");
                    if (abortException)
                    {
                        e->Release();
                        throw LINK(abortException);
                    }
                    throw;
                }
            }
        }
    }
    fatalHandler.clear();
    fatalHandler.setown(new CFatalHandler(globals->getPropInt("@fatal_timeout", FATAL_TIMEOUT)));
    CGraphBase::executeSubGraph(parentExtractSz, parentExtract);
    if (TAG_NULL != executeReplyTag)
    {
        rank_t sender;
        unsigned s=0;
        for (; s<queryJob().querySlaves(); s++)
        {
            CMessageBuffer msg;
            if (!executeReplyMsgHandler.recv(queryJobChannel().queryJobComm(), msg, RANK_ALL, executeReplyTag, &sender))
                break;
            bool error;
            msg.read(error);
            if (error)
            {
                Owned<IThorException> exception = deserializeThorException(msg);
                exception->setSlave(sender);
                GraphPrintLog(exception, "slave execute reply exception");
                throw exception.getClear();
            }
        }
        if (fatalHandler)
            fatalHandler->clear();
    }
    fatalHandler.clear();
#ifndef _CONTAINERIZED
    Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
    queryJobManager().updateWorkUnitLog(*wu);
#endif
}

void CMasterGraph::sendGraph()
{
    CTimeMon atimer;
    CMessageBuffer msg;
    msg.append(GraphInit);
    msg.append(job.queryKey());
    if (TAG_NULL == executeReplyTag)
        executeReplyTag = jobM->allocateMPTag();
    serializeMPtag(msg, executeReplyTag);
    serializeCreateContexts(msg);
    serializeGraphInit(msg);
    // slave graph data
    try
    {
        jobM->broadcast(queryNodeComm(), msg, managerWorkerMpTag, LONGTIMEOUT, "sendGraph", &bcastMsgHandler);
    }
    catch (IException *e)
    {
        GraphPrintLog(e, "Aborting sendGraph");
        if (abortException)
        {
            e->Release();
            throw LINK(abortException);
        }
        throw;
    }
    GraphPrintLog("sendGraph took %d ms", atimer.elapsed());
}

bool CMasterGraph::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    GraphPrintLog("Processing graph");
    if (!queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryJobManager().queryDeMonServer()->startGraph(this);
    }
    if (syncInitData())
    {
        sendActivityInitData(); // has to be done at least once
        // NB: At this point, on the slaves, the graphs will start
    }
    CGraphBase::preStart(parentExtractSz, parentExtract);
    if (isGlobal())
    {
        if (!startBarrier->wait(true)) // ensure all graphs are at start point at same time, as some may request data from each other.
            return false;
    }
    if (!queryOwner())
        job.queryWorkUnit().setNodeState(job.queryGraphName(), graphId, WUGraphRunning);
    return true;
}

void CMasterGraph::handleSlaveDone(unsigned node, MemoryBuffer &mb)
{
    graph_id gid;
    mb.read(gid);
    assertex(gid == queryGraphId());
    unsigned count;
    mb.read(count);
    while (count--)
    {
        activity_id activityId;
        mb.read(activityId);
        CMasterGraphElement *act = (CMasterGraphElement *)queryElement(activityId);
        unsigned len;
        mb.read(len);
        const void *d = mb.readDirect(len);
        MemoryBuffer sdMb;
        sdMb.setBuffer(len, (void *)d);
        act->slaveDone(node, sdMb);
    }
}

void CMasterGraph::getFinalProgress(bool aborting)
{
    CMessageBuffer msg;
    mptag_t replyTag = queryJobChannel().queryMPServer().createReplyTag();
    msg.setReplyTag(replyTag);
    msg.append((unsigned)GraphEnd);
    msg.append(job.queryKey());
    msg.append(queryGraphId());
    // If aborted, some slaves may have disconnnected/aborted so don't wait so long
    unsigned timeOutPeriod = aborting ? SHORTTIMEOUT : LONGTIMEOUT;
    jobM->broadcast(queryNodeComm(), msg, managerWorkerMpTag, timeOutPeriod, "graphEnd", NULL, true, aborting);

    Owned<IBitSet> respondedBitSet = createBitSet();
    unsigned n=queryJob().queryNodes();
    while (n--)
    {
        rank_t sender;
        if (!queryNodeComm().recv(msg, RANK_ALL, replyTag, &sender, timeOutPeriod))
        {
            StringBuffer slaveList;
            n=queryJob().queryNodes();
            unsigned s = 0;
            for (;;)
            {
                unsigned ns = respondedBitSet->scan(s, true); // scan for next respondent

                // list all non-respondents up to this found respondent (or rest of slaves)
                while (s<ns && s<n) // NB: if scan find nothing else set, ns = (unsigned)-1
                {
                    if (slaveList.length())
                        slaveList.append(",");
                    ++s; // inc. 1st as slaveList is list of slaves that are 1 based.
                    slaveList.append(s);
                }
                ++s;
                if (s>=n)
                    break;
            }
            if (aborting)
            {
                IWARNLOG("Timeout receiving final progress from slaves - these slaves failed to respond: %s", slaveList.str());
                return;
            }
            else
                throw MakeGraphException(this, 0, "Timeout receiving final progress from slaves - these slaves failed to respond: %s", slaveList.str());
        }
        bool error;
        msg.read(error);
        if (error)
        {
            Owned<IThorException> e = deserializeThorException(msg);
            e->setSlave(sender);
            throw e.getClear();
        }
        respondedBitSet->set(((unsigned)sender)-1);
        if (0 == msg.remaining())
            continue;

        unsigned channelsPerSlave = globals->getPropInt("@channelsPerSlave", 1); // JCSMORE - should move somewhere common
        for (unsigned sc=0; sc<channelsPerSlave; sc++)
        {
            unsigned slave;
            msg.read(slave);
            if (RANK_NULL == slave) // worker no longer had graph registered
                continue;
            handleSlaveDone(slave, msg);
            if (!queryOwner())
            {
                if (globals->getPropBool("@watchdogProgressEnabled"))
                {
                    try
                    {
                        size32_t progressLen;
                        msg.read(progressLen);
                        if (progressLen)
                        {
                            MemoryBuffer progressData;
                            progressData.setBuffer(progressLen, (void *)msg.readDirect(progressLen));
                            queryJobManager().queryDeMonServer()->takeHeartBeat(progressData);
                        }
                    }
                    catch (IException *e)
                    {
                        GraphPrintLog(e, "Failure whilst deserializing stats/progress");
                        e->Release();
                    }
                }
            }
        }
    }
}

void CMasterGraph::done()
{
    if (started)
    {
        if (!aborted && (!queryOwner() || isGlobal()))
            getFinalProgress(); // waiting for slave graph to finish and send final progress update + extra act end info.
    }

    CGraphBase::done();
    if (started && !queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryJobManager().queryDeMonServer()->endGraph(this, true);
    }
}

void CMasterGraph::setComplete(bool tf)
{
    CGraphBase::setComplete(tf);
    if (tf && !queryOwner())
    {
        job.queryWorkUnit().setNodeState(job.queryGraphName(), graphId, graphDone?WUGraphComplete:WUGraphFailed);
    }
}

bool CMasterGraph::deserializeStats(unsigned node, MemoryBuffer &mb)
{
    CriticalBlock b(createdCrit);

    graphStats.deserialize(node, mb);
    unsigned count;
    mb.read(count);
    if (count)
        setProgressUpdated();
    while (count--)
    {
        activity_id activityId;
        mb.read(activityId);
        CMasterActivity *activity = NULL;
        CMasterGraphElement *element = (CMasterGraphElement *)queryElement(activityId);
        if (element)
        {
            activity = (CMasterActivity *)element->queryActivity();
            if (!activity)
            {
                CGraphBase *parentGraph = element->queryOwner().queryOwner(); // i.e. am I in a childgraph
                if (!parentGraph)
                {
                    GraphPrintLog("Activity id=%" ACTPF "d not created in master and not a child query activity", activityId);
                    return false; // don't know if or how this could happen, but all bets off with packet if did.
                }
                Owned<IException> e;
                try
                {
                    element->createActivity();
                    activity = (CMasterActivity *)element->queryActivity();
                }
                catch (IException *_e)
                {
                    e.setown(_e);
                    GraphPrintLog(_e, "In deserializeStats");
                }
                if (!activity || e.get())
                {
                    GraphPrintLog("Activity id=%" ACTPF "d failed to created child query activity ready for progress", activityId);
                    return false;
                }
            }
            if (activity)
                activity->deserializeStats(node, mb);
        }
        else
        {
            GraphPrintLog("Failed to find activity, during progress deserialization, id=%" ACTPF "d", activityId);
            return false; // don't know if or how this could happen, but all bets off with packet if did.
        }
    }
    unsigned subs;
    mb.read(subs);
    while (subs--)
    {
        graph_id subId;
        mb.read(subId);
        Owned<CMasterGraph> graph = (CMasterGraph *)job.queryJobChannel(0).getGraph(subId);
        if (NULL == graph.get())
            return false;
        if (!graph->deserializeStats(node, mb))
            return false;
    }
    return true;
}

void CMasterGraph::getStats(IStatisticGatherer &stats)
{
    stats.addStatistic(StNumSlaves, queryClusterWidth());
    // graph specific stats
    graphStats.getStats(stats);

    Owned<IThorActivityIterator> iter;
    if (queryOwner() && !isGlobal())
        iter.setown(getIterator()); // Local child graphs still send progress, but aren't connected in master
    else
        iter.setown(getConnectedIterator());
    ForEach (*iter)
    {
        CMasterGraphElement &container = (CMasterGraphElement &)iter->query();
        CMasterActivity *activity = (CMasterActivity *)container.queryActivity();
        if (activity) // may not be created (if within child query)
        {
            activity_id id = container.queryId();

            unsigned outputs = container.getOutputs();
            unsigned oid = 0;
            for (; oid < outputs; oid++)
            {
                StatsEdgeScope edgeScope(stats, id, oid);
                activity->getEdgeStats(stats, oid); // for subgraph, may recursively call reportGraph
            }

            StatsActivityScope scope(stats, id);
            activity->getActivityStats(stats);
        }
    }
    cost_type costDiskAccess = getDiskAccessCost();
    if (costDiskAccess)
        stats.addStatistic(StCostFileAccess, costDiskAccess);
}

cost_type CMasterGraph::getDiskAccessCost()
{
    cost_type totalDiskAccessCost = 0;
    Owned<IThorGraphIterator> iterChildGraph = getChildGraphIterator();
    ForEach(*iterChildGraph)
    {
        CGraphBase &graph = iterChildGraph->query();
        totalDiskAccessCost += graph.getDiskAccessCost();
    }

    Owned<IThorActivityIterator> iter;
    if (queryOwner() && !isGlobal())
        iter.setown(getIterator()); // Local child graphs still send progress, but aren't connected in master
    else
        iter.setown(getConnectedIterator());
    ForEach (*iter)
    {
        CMasterGraphElement &container = (CMasterGraphElement &)iter->query();
        CMasterActivity *activity = (CMasterActivity *)container.queryActivity();
        if (activity) // may not be created (if within child query)
            totalDiskAccessCost += activity->getDiskAccessCost();
    }
    return totalDiskAccessCost;
}

IThorResult *CMasterGraph::createResult(CActivityBase &activity, unsigned id, IThorGraphResults *results, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority)
{
    Owned<CCollatedResult> result = new CCollatedResult(*this, activity, rowIf, id, results->queryOwnerId(), spillPriority);
    results->setResult(id, result);
    return result;
}

IThorResult *CMasterGraph::createResult(CActivityBase &activity, unsigned id, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority)
{
    Owned<CCollatedResult> result = new CCollatedResult(*this, activity, rowIf, id, localResults->queryOwnerId(), spillPriority);
    localResults->setResult(id, result);
    return result;
}

IThorResult *CMasterGraph::createGraphLoopResult(CActivityBase &activity, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority)
{
    Owned<CCollatedResult> result = new CCollatedResult(*this, activity, rowIf, 0, localResults->queryOwnerId(), spillPriority);
    unsigned id = graphLoopResults->addResult(result);
    result->setId(id);
    return result;
}


///////////////////////////////////////////////////

const StatisticsMapping CThorEdgeCollection::edgeStatsMapping({StNumRowsProcessed, StNumStarts, StNumStops});

void CThorEdgeCollection::set(unsigned node, unsigned __int64 value)
{
    unsigned __int64 numRows = value & THORDATALINK_COUNT_MASK;
    byte stop = (value & THORDATALINK_STOPPED) ? 1 : 0;
    byte start = stop || (value & THORDATALINK_STARTED) ? 1 : 0; // start implied if stopped, keeping previous semantics
    setStatistic(node, StNumRowsProcessed, numRows);
    setStatistic(node, StNumStops, stop);
    setStatistic(node, StNumStarts, start);
}

///////////////////////////////////////////////////

CJobMaster *createThorGraph(const char *graphName, IConstWorkUnit &workunit, ILoadedDllEntry *querySo, bool sendSo, const SocketEndpoint &agentEp)
{
    Owned<CJobMaster> jobMaster = new CJobMaster(workunit, graphName, querySo, sendSo, agentEp);
    IPropertyTree *graphXGMML = jobMaster->queryGraphXGMML();
    Owned<IPropertyTreeIterator> iter = graphXGMML->getElements("node");
    ForEach(*iter)
        jobMaster->addSubGraph(iter->query());
    jobMaster->addDependencies(graphXGMML);
    return LINK(jobMaster);
}

static IJobManager *jobManager = NULL;
void setJobManager(IJobManager *_jobManager)
{
    CriticalBlock b(*jobManagerCrit);
    jobManager = _jobManager;
}
IJobManager *getJobManager()
{
    CriticalBlock b(*jobManagerCrit);
    return LINK(jobManager);
}
IJobManager &queryJobManager()
{
    CriticalBlock b(*jobManagerCrit);
    assertex(jobManager);
    return *jobManager;
}

