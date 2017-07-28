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
#include <algorithm>
#include "hthor.ipp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jsocket.hpp"
#include "jprop.hpp"
#include "jdebug.hpp"
#include "jlzw.hpp"
#include "jisem.hpp"
#include "roxiedebug.hpp"
#include "roxierow.hpp"
#include "roxiemem.hpp"
#include "eclhelper.hpp"
#include "workunit.hpp"
#include "jfile.hpp"
#include "keybuild.hpp"

#include "hrpc.hpp"
#include "hrpcsock.hpp"

#include "dafdesc.hpp"
#include "dautils.hpp"
#include "dasess.hpp"
#include "dadfs.hpp"
#include "thorfile.hpp"
#include "thorsort.hpp"
#include "thorparse.ipp"
#include "thorxmlwrite.hpp"
#include "jsmartsock.hpp"
#include "thorstep.hpp"
#include "eclagent.ipp"
#include "roxierowbuff.hpp"
#include "ftbase.ipp"

#define EMPTY_LOOP_LIMIT 1000

static unsigned const hthorReadBufferSize = 0x10000;
static offset_t const defaultHThorDiskWriteSizeLimit = I64C(10*1024*1024*1024); //10 GB, per Nigel
static size32_t const spillStreamBufferSize = 0x10000;
static unsigned const hthorPipeWaitTimeout = 100; //100ms - fairly arbitrary choice

using roxiemem::IRowManager;
using roxiemem::OwnedRoxieRow;
using roxiemem::OwnedRoxieString;
using roxiemem::OwnedConstRoxieRow;

IRowManager * theRowManager;

void setHThorRowManager(IRowManager * manager)
{
    theRowManager = manager;
}

IRowManager * queryRowManager()
{
    return theRowManager;
}

void throwOOMException(size_t size, char const * label)
{
    throw MakeStringException(0, "Out of Memory in hthor: trying to allocate %" I64F "u bytes for %s", (unsigned __int64) size, label);
}

void * checked_malloc(size_t size, char const * label)
{
    void * ret = malloc(size);
    if(!ret)
        throwOOMException(size, label);
    return ret;
}

void * checked_calloc(size_t size, size_t num, char const * label)
{
    void * ret = calloc(size, num);
    if(!ret)
        throwOOMException(size*num, label);
    return ret;
}

inline bool checkIsCompressed(unsigned int flags, size32_t fixedSize, bool grouped)
{
    return ((flags & TDWnewcompress) || ((flags & TDXcompress) && (fixedSize+(grouped?1:0) >= MIN_ROWCOMPRESS_RECSIZE)));
}

//=====================================================================================================

//=====================================================================================================

CRowBuffer::CRowBuffer(IRecordSize * _recsize, bool _grouped) : recsize(_recsize), grouped(_grouped)
{
    fixsize = recsize->getFixedSize();
    count = 0;
    index = 0;
}

void CRowBuffer::insert(const void * next)
{
    buff.append(next);
    count++;
}

bool CRowBuffer::pull(IHThorInput * input, unsigned __int64 rowLimit)
{
    while(true)
    {
        OwnedConstRoxieRow next(input->nextRow());
        if(!next)
        {
            next.setown(input->nextRow());
            if(!next)
                break;
            if(grouped)
                buff.append(NULL);
        }
        insert(next.getClear());
        if(count > rowLimit)
            return false;
    }
    return true;
}

void CRowBuffer::clear()
{
    buff.clear();
    index = 0;
    count = 0;
}

const void * CRowBuffer::next()
{
    if(buff.isItem(index))
        return buff.itemClear(index++);
    else
        return NULL;
}

//=====================================================================================================

CHThorActivityBase::CHThorActivityBase(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _help, ThorActivityKind _kind) : agent(_agent), help(_help),  outputMeta(help.queryOutputMeta()), kind(_kind), activityId(_activityId), subgraphId(_subgraphId)
{
    input = NULL;
    processed = 0;
    rowAllocator = NULL;    
}

void CHThorActivityBase::setInput(unsigned index, IHThorInput *_input)
{
    assertex(index == 0);
    input = _input;
}

IHThorInput *CHThorActivityBase::queryOutput(unsigned index)
{
    agent.fail(255, "internal logic error: CHThorActivityBase::queryOutput");
    // never returns....
    return NULL;
}

void CHThorActivityBase::ready()
{
    if (input)
        input->ready();
    if (needsAllocator())       
        createRowAllocator();   
    initialProcessed = processed;
}

CHThorActivityBase::~CHThorActivityBase()
{
    ::Release(rowAllocator);
}
void CHThorActivityBase::createRowAllocator()
{
    if (!rowAllocator) 
        rowAllocator = agent.queryCodeContext()->getRowAllocator(outputMeta.queryOriginal(), activityId);
}

__int64 CHThorActivityBase::getCount()
{
    throw MakeStringException(2, "Internal error: CHThorActivityBase::getCount");
    return 0;
}

void CHThorActivityBase::execute()
{
    agent.fail(255, "internal logic error: CHThorActivityBase::execute");
}

void CHThorActivityBase::extractResult(unsigned & len, void * & ret)
{
    agent.fail(255, "internal logic error: CHThorActivityBase::extractResult");
}

void CHThorActivityBase::stop()
{
    if (input)
        input->stop();
}

void CHThorActivityBase::resetEOF()
{
    if (input)
        input->resetEOF();
}

void CHThorActivityBase::updateProgress(IStatisticGatherer &progress) const
{
    updateProgressForOther(progress, activityId, subgraphId);
    if (input)
        input->updateProgress(progress);
}

void CHThorActivityBase::updateProgressForOther(IStatisticGatherer &progress, unsigned otherActivity, unsigned otherSubgraph) const
{
    updateProgressForOther(progress, otherActivity, otherSubgraph, 0, processed);
}

void CHThorActivityBase::updateProgressForOther(IStatisticGatherer &progress, unsigned otherActivity, unsigned otherSubgraph, unsigned whichOutput, unsigned __int64 numProcessed) const
{
    StatsEdgeScope scope(progress, otherActivity, whichOutput);
    progress.addStatistic(StNumRowsProcessed, numProcessed);
    progress.addStatistic(StNumStarts, 1);  // wrong for an activity in a subquery
    progress.addStatistic(StNumStops, 1);
    progress.addStatistic(StNumSlaves, 1);  // MORE: A bit pointless for an hthor graph
}

ILocalEclGraphResults * CHThorActivityBase::resolveLocalQuery(__int64 graphId)
{
    return static_cast<ILocalEclGraphResults *>(agent.queryCodeContext()->resolveLocalQuery(graphId));
}

IException * CHThorActivityBase::makeWrappedException(IException * e) const
{
    if(dynamic_cast<IHThorException *>(e) ||  dynamic_cast<IUserException *>(e))
        return e;
    else
        return makeHThorException(kind, activityId, subgraphId, e);
}

IException * CHThorActivityBase::makeWrappedException(IException * e, char const * extra) const
{
    if(dynamic_cast<IHThorException *>(e) ||  dynamic_cast<IUserException *>(e))
        return e;
    else
        return makeHThorException(kind, activityId, subgraphId, e, extra);
}

bool CHThorActivityBase::isPassThrough()
{
    return false;
}

//=====================================================================================================

CHThorSimpleActivityBase::CHThorSimpleActivityBase(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _help, ThorActivityKind _kind) : CHThorActivityBase(_agent, _activityId, _subgraphId, _help, _kind)
{
}

IHThorInput * CHThorSimpleActivityBase::queryOutput(unsigned index) 
{ 
    assertex(index == 0);
    return this; 
}

bool CHThorSimpleActivityBase::isGrouped()
{ 
    return input ? input->isGrouped() : outputMeta.isGrouped();
}

IOutputMetaData * CHThorSimpleActivityBase::queryOutputMeta() const
{
    return outputMeta;
}

//=====================================================================================================

class CHThorClusterWriteHandler : public ClusterWriteHandler
{
    IAgentContext &agent;
public:
    CHThorClusterWriteHandler(char const * _logicalName, char const * _activityType, IAgentContext &_agent) 
        : agent(_agent), ClusterWriteHandler(_logicalName, _activityType)
    {
    }

private:
    virtual void getTempFilename(StringAttr & out) const
    {
        StringBuffer buff;
        agent.getTempfileBase(buff).appendf(".cluster_write_%p.%" I64F "d_%u", this, (__int64)GetCurrentThreadId(), GetCurrentProcessId());
        out.set(buff.str());
    }
};

ClusterWriteHandler *createClusterWriteHandler(IAgentContext &agent, IHThorIndexWriteArg *iwHelper, IHThorDiskWriteArg *dwHelper, const char * lfn, StringAttr &fn, bool extend)
{
    Owned<CHThorClusterWriteHandler> clusterHandler;
    unsigned clusterIdx = 0;
    while(true)
    {
        OwnedRoxieString cluster(iwHelper ? iwHelper->getCluster(clusterIdx++) : dwHelper->getCluster(clusterIdx++));
        if(!cluster)
            break;
        if(!clusterHandler)
        {
            if(extend)
                throw MakeStringException(0, "Cannot combine EXTEND and CLUSTER flags on disk write of file %s", lfn);
            clusterHandler.setown(new CHThorClusterWriteHandler(lfn, "OUTPUT", agent));
        }
        clusterHandler->addCluster(cluster);
    }
    if(clusterHandler)
    {
        clusterHandler->getLocalPhysicalFilename(fn);
    }
    else if (!agent.queryResolveFilesLocally())
    {
        StringBuffer filenameText;
        bool wasDFS;
        makeSinglePhysicalPartName(lfn, filenameText, true, wasDFS);
        fn.set(filenameText.str());
    }
    else
    {
        fn.set(lfn);
    }
    StringBuffer dir;
    splitFilename(fn, &dir, &dir, NULL, NULL);
    recursiveCreateDirectory(dir.str());
    return clusterHandler.getClear();
}

//=====================================================================================================

CHThorDiskWriteActivity::CHThorDiskWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskWriteArg &_arg, ThorActivityKind _kind) : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    incomplete = false;
}

CHThorDiskWriteActivity::~CHThorDiskWriteActivity()
{
    diskout.clear();
    if(incomplete)
    {
        PROGLOG("Disk write incomplete, deleting physical file: %s", filename.get());
        diskout.clear();
        outSeq.clear();
        file->remove();
    }
}

void CHThorDiskWriteActivity::ready()       
{ 
    CHThorActivityBase::ready(); 
    grouped = (helper.getFlags() & TDXgrouped) != 0;
    extend = ((helper.getFlags() & TDWextend) != 0);
    overwrite = ((helper.getFlags() & TDWoverwrite) != 0);
    resolve();
    uncompressedBytesWritten = 0;
    numRecords = 0;
    sizeLimit = agent.queryWorkUnit()->getDebugValueInt64("hthorDiskWriteSizeLimit", defaultHThorDiskWriteSizeLimit);
    rowIf.setown(createRowInterfaces(input->queryOutputMeta(), activityId, 0, agent.queryCodeContext()));
    open();
}

void CHThorDiskWriteActivity::execute()
{
    // Loop thru the results
    numRecords = 0;
    while (next()) 
        numRecords++;
    finishOutput();
}

void CHThorDiskWriteActivity::stop()
{
    outSeq->flush();
    if(blockcompressed)
        uncompressedBytesWritten = outSeq->getPosition();
    updateWorkUnitResult(numRecords);
    close();
    if((helper.getFlags() & (TDXtemporary | TDXjobtemp) ) == 0 && !agent.queryResolveFilesLocally())
        publish();
    incomplete = false;
    if(clusterHandler)
        clusterHandler->finish(file);
    CHThorActivityBase::stop();
}

void CHThorDiskWriteActivity::resolve()
{
    OwnedRoxieString rawname = helper.getFileName();
    mangleHelperFileName(mangledHelperFileName, rawname, agent.queryWuid(), helper.getFlags());
    assertex(mangledHelperFileName.str());
    if((helper.getFlags() & (TDXtemporary | TDXjobtemp)) == 0)
    {
        Owned<ILocalOrDistributedFile> f = agent.resolveLFN(mangledHelperFileName.str(),"Cannot write, invalid logical name",true,false,true,&lfn);
        if (f)
        {
            if (f->queryDistributedFile())
            {
                // An already existing dali file
                if(extend)
                    agent.logFileAccess(f->queryDistributedFile(), "HThor", "EXTENDED");
                else if(overwrite) {
                    PrintLog("Removing %s from DFS", lfn.str());
                    agent.logFileAccess(f->queryDistributedFile(), "HThor", "DELETED");
                    if (!agent.queryResolveFilesLocally())
                        f->queryDistributedFile()->detach();
                    else
                    {
                        Owned<IFile> file = createIFile(lfn);
                        if (file->exists())
                            file->remove();
                    }
                }
                else
                    throw MakeStringException(99, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", lfn.str());
            }
            else if (f->exists() || f->isExternal() || agent.queryResolveFilesLocally())
            {
                // special/local/external file
                if (f->numParts()!=1)
                    throw MakeStringException(99, "Cannot write %s, external file has multiple parts)", lfn.str());
                RemoteFilename rfn;
                f->getPartFilename(rfn,0);
                StringBuffer full;
                if (rfn.isLocal())
                    rfn.getLocalPath(full);
                else
                    rfn.getRemotePath(full);
                filename.set(full);
                if (isSpecialPath(filename))
                {
                    PROGLOG("Writing to query %s", filename.get());
                    return;
                }
                if (stdIoHandle(filename)>=0) {
                    PROGLOG("Writing to %s", filename.get());
                    return;
                }
                Owned<IFile> file = createIFile(filename);
                if (file->exists())
                {
                    if (!overwrite) 
                        throw MakeStringException(99, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", full.str());
                    file->remove();
                }

                //Ensure target folder exists
                if (!recursiveCreateDirectoryForFile(filename.get()))
                {
                    throw MakeStringException(99, "Cannot create file folder for %s", filename.str());
                }

                PROGLOG("Writing to file %s", filename.get());
            }
            f.clear();
        }
        if (filename.isEmpty())  // wasn't local or special (i.e. DFS file)
        {
            CDfsLogicalFileName dfsLogicalName;
            dfsLogicalName.allowOsPath(agent.queryResolveFilesLocally());
            if (!dfsLogicalName.setValidate(lfn.str()))
            {
                throw MakeStringException(99, "Could not resolve DFS Logical file %s", lfn.str());
            }

            clusterHandler.setown(createClusterWriteHandler(agent, NULL, &helper, dfsLogicalName.get(), filename, extend));
        }
    }
    else
    {
        StringBuffer mangledName;
        mangleLocalTempFilename(mangledName, mangledHelperFileName.str());
        filename.set(agent.noteTemporaryFile(mangledName.str()));
        PROGLOG("DISKWRITE: using temporary filename %s", filename.get());
    }
}

void CHThorDiskWriteActivity::open()
{
    // Open an output file...
    file.setown(createIFile(filename));
    serializedOutputMeta.set(input->queryOutputMeta()->querySerializedDiskMeta());//returns outputMeta if serialization not needed

    Linked<IRecordSize> groupedMeta = input->queryOutputMeta()->querySerializedDiskMeta();
    if(grouped)
        groupedMeta.setown(createDeltaRecordSize(groupedMeta, +1));
    blockcompressed = checkIsCompressed(helper.getFlags(), serializedOutputMeta.getFixedSize(), grouped);//TDWnewcompress for new compression, else check for row compression
    void *ekey;
    size32_t ekeylen;
    helper.getEncryptKey(ekeylen,ekey);
    encrypted = false;
    Owned<ICompressor> ecomp;
    if (ekeylen!=0) {
        ecomp.setown(createAESCompressor256(ekeylen,ekey));
        memset(ekey,0,ekeylen);
        rtlFree(ekey);
        encrypted = true;
        blockcompressed = true;
    }
    Owned<IFileIO> io;
    if(blockcompressed)
        io.setown(createCompressedFileWriter(file, groupedMeta->getFixedSize(), extend, true, ecomp));
    else
        io.setown(file->open(extend ? IFOwrite : IFOcreate));
    if(!io)
        throw MakeStringException(errno, "Failed to create%s file %s for writing", (encrypted ? " encrypted" : (blockcompressed ? " compressed" : "")), filename.get());
    incomplete = true;
    
    diskout.setown(createBufferedIOStream(io));
    if(extend)
        diskout->seek(0, IFSend);

    unsigned rwFlags = rw_autoflush;
    if(grouped)
        rwFlags |= rw_grouped;
    if(!agent.queryWorkUnit()->getDebugValueBool("skipFileFormatCrcCheck", false) && !(helper.getFlags() & TDRnocrccheck))
        rwFlags |= rw_crc;
    IExtRowWriter * writer = createRowWriter(diskout, rowIf, rwFlags);
    outSeq.setown(writer);

}


const void * CHThorDiskWriteActivity::getNext()
{   // through operation (writes and returns row)
    // needs a one row lookahead to preserve group
    if (!nextrow.get()) 
    {
        nextrow.setown(input->nextRow());
        if (!nextrow.get())
        {
            nextrow.setown(input->nextRow());
            if (nextrow.get()&&grouped)  // only write eog if not at eof
                outSeq->putRow(NULL);
            return NULL;
        }
    }
    outSeq->putRow(nextrow.getLink());
    checkSizeLimit();
    return nextrow.getClear();
}

bool CHThorDiskWriteActivity::next()
{
    if (!nextrow.get())
    {
        OwnedConstRoxieRow row(input->nextRow());
        if (!row.get()) 
        {
            row.setown(input->nextRow());
            if (!row.get())
                return false; // we are done        
            if (grouped)
                outSeq->putRow(NULL);
        }
        outSeq->putRow(row.getClear());
    }
    else
        outSeq->putRow(nextrow.getClear());
    checkSizeLimit();
    return true;
}


void CHThorDiskWriteActivity::finishOutput()
{
}


void CHThorDiskWriteActivity::close()
{
    diskout.clear();
    outSeq.clear();
    if(clusterHandler)
        clusterHandler->copyPhysical(file, agent.queryWorkUnit()->getDebugValueBool("__output_cluster_no_copy_physical", false));
}

void CHThorDiskWriteActivity::publish()
{
    StringBuffer dir,base;
    offset_t fileSize = file->size();
    if(clusterHandler)
        clusterHandler->splitPhysicalFilename(dir, base);
    else
        splitFilename(filename, &dir, &dir, &base, &base);

    Owned<IFileDescriptor> desc = createFileDescriptor();
    desc->setDefaultDir(dir.str());

    Owned<IPropertyTree> attrs;
    if(clusterHandler) 
        attrs.setown(createPTree("Part")); // clusterHandler is going to set attributes
    else 
    {
        // add cluster
        StringBuffer mygroupname;
        Owned<IGroup> mygrp = agent.getHThorGroup(mygroupname);
        ClusterPartDiskMapSpec partmap; // will get this from group at some point
        desc->setNumParts(1);
        desc->setPartMask(base.str());
        desc->addCluster(mygroupname.str(),mygrp, partmap);
        attrs.set(&desc->queryPart(0)->queryProperties());
    }
    //properties of the first file part.
    if(blockcompressed)
    {
        attrs->setPropInt64("@size", uncompressedBytesWritten);
        attrs->setPropInt64("@compressedSize", fileSize);
    }
    else
        attrs->setPropInt64("@size", fileSize);
    CDateTime createTime, modifiedTime, accessedTime;
    file->getTime(&createTime, &modifiedTime, &accessedTime);
    // round file time down to nearest sec. Nanosec accurancy is not preserved elsewhere and can lead to mismatch later.
    unsigned hour, min, sec, nanosec;
    modifiedTime.getTime(hour, min, sec, nanosec);
    modifiedTime.setTime(hour, min, sec, 0);
    StringBuffer timestr;
    modifiedTime.getString(timestr);
    if(timestr.length())
        attrs->setProp("@modified", timestr.str());
    if(clusterHandler)
        clusterHandler->setDescriptorParts(desc, base.str(), attrs);

    // properties of the logical file
    IPropertyTree & properties = desc->queryProperties();
    properties.setPropInt64("@size", (blockcompressed) ? uncompressedBytesWritten : fileSize);
    if (encrypted)
        properties.setPropBool("@encrypted", true);
    if (blockcompressed)
    {
        properties.setPropInt64("@compressedSize", fileSize);
        properties.setPropBool("@blockCompressed", true);
    }
    if (helper.getFlags() & TDWpersist)
        properties.setPropBool("@persistent", true);
    if (grouped)
        properties.setPropBool("@grouped", true);
    properties.setPropInt64("@recordCount", numRecords);
    properties.setProp("@owner", agent.queryWorkUnit()->queryUser());
    if (helper.getFlags() & (TDWowned|TDXjobtemp|TDXtemporary))
        properties.setPropBool("@owned", true);
    if (helper.getFlags() & TDWresult)
        properties.setPropBool("@result", true);

    properties.setProp("@workunit", agent.queryWorkUnit()->queryWuid());
    properties.setProp("@job", agent.queryWorkUnit()->queryJobName());
    setFormat(desc);

    if (helper.getFlags() & TDWexpires)
        setExpiryTime(properties, helper.getExpiryDays());
    if (helper.getFlags() & TDWupdate)
    {
        unsigned eclCRC;
        unsigned __int64 totalCRC;
        helper.getUpdateCRCs(eclCRC, totalCRC);
        properties.setPropInt("@eclCRC", eclCRC);
        properties.setPropInt64("@totalCRC", totalCRC);
    }
    properties.setPropInt("@formatCrc", helper.getFormatCrc());

    StringBuffer lfn;
    expandLogicalFilename(lfn, mangledHelperFileName.str(), agent.queryWorkUnit(), agent.queryResolveFilesLocally(), false);
    CDfsLogicalFileName logicalName;
    if (agent.queryResolveFilesLocally())
        logicalName.allowOsPath(true);
    if (!logicalName.setValidate(lfn.str()))
        throw MakeStringException(99, "Cannot publish %s, invalid logical name", lfn.str());
    if (!logicalName.isExternal()) // no need to publish externals
    {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(desc);
        if(file->getModificationTime(modifiedTime))
            file->setAccessedTime(modifiedTime);
        file->attach(logicalName.get(), agent.queryCodeContext()->queryUserDescriptor());
        agent.logFileAccess(file, "HThor", "CREATED");
    }
}

void CHThorDiskWriteActivity::updateWorkUnitResult(unsigned __int64 reccount)
{
    if(lfn.length()) //this is required as long as temp files don't get a name which can be stored in the WU and automatically deleted by the WU
    {
        WorkunitUpdate wu = agent.updateWorkUnit();
        StringArray clusters;
        if (clusterHandler)
            clusterHandler->getClusters(clusters);
        else
            clusters.append(wu->queryClusterName());
        unsigned flags = helper.getFlags();
        if (!agent.queryResolveFilesLocally())
        {
            WUFileKind fileKind;
            if (TDXtemporary & flags)
                fileKind = WUFileTemporary;
            else if(TDXjobtemp & flags)
                fileKind = WUFileJobOwned;
            else if(TDWowned & flags)
                fileKind = WUFileOwned;
            else
                fileKind = WUFileStandard;
            wu->addFile(lfn.str(), &clusters, helper.getTempUsageCount(), fileKind, NULL);
        }
        else if ((TDXtemporary | TDXjobtemp) & flags)          
            agent.noteTemporaryFilespec(filename);//note for later deletion
        if (!(flags & TDXtemporary) && helper.getSequence() >= 0)
        {
            Owned<IWUResult> result = wu->updateResultBySequence(helper.getSequence());
            if (result)
            {
                result->setResultTotalRowCount(reccount); 
                result->setResultStatus(ResultStatusCalculated);
                if (helper.getFlags() & TDWresult)
                    result->setResultFilename(lfn.str());
                else
                    result->setResultLogicalName(lfn.str());
            }
        }
    }
}

void CHThorDiskWriteActivity::setFormat(IFileDescriptor * desc)
{
    if ((serializedOutputMeta.isFixedSize()) && !isOutputTransformed())
        desc->queryProperties().setPropInt("@recordSize", serializedOutputMeta.getFixedSize() + (grouped ? 1 : 0));

    const char *recordECL = helper.queryRecordECL();
    if (recordECL && *recordECL)
        desc->queryProperties().setProp("ECL", recordECL);
    desc->queryProperties().setProp("@kind", "flat");
}

void CHThorDiskWriteActivity::checkSizeLimit()
{
    if(sizeLimit && outSeq && (outSeq->getPosition() > sizeLimit))
    {
        StringBuffer msg;
        msg.append("Exceeded disk write size limit of ").append(sizeLimit).append(" while writing file ").append(mangledHelperFileName.str());
        throw MakeStringExceptionDirect(0, msg.str());
    }
}

//=====================================================================================================

CHThorSpillActivity::CHThorSpillActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSpillArg &_arg, ThorActivityKind _kind) : CHThorDiskWriteActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

void CHThorSpillActivity::setInput(unsigned index, IHThorInput *_input)
{
    CHThorActivityBase::setInput(index, _input);

}

void CHThorSpillActivity::ready()
{
    CHThorDiskWriteActivity::ready();
}

void CHThorSpillActivity::execute()
{
    UNIMPLEMENTED;
}

const void *CHThorSpillActivity::nextRow()
{
    const void *nextrec = getNext();
    if (nextrec)
    {
        numRecords++;
        processed++;
    }
    return nextrec;
}

void CHThorSpillActivity::stop()
{
    for (;;)
    {
        OwnedConstRoxieRow nextrec(nextRow());
        if (!nextrec) 
        {
            nextrec.setown(nextRow());
            if (!nextrec)
                break;
        }   
    }
    finishOutput();
    CHThorDiskWriteActivity::stop();
}

//=====================================================================================================


CHThorCsvWriteActivity::CHThorCsvWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCsvWriteArg &_arg, ThorActivityKind _kind) : CHThorDiskWriteActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    csvOutput.init(helper.queryCsvParameters(),agent.queryWorkUnit()->getDebugValueBool("oldCSVoutputFormat", false));
}

void CHThorCsvWriteActivity::execute()
{
    OwnedRoxieString header(helper.queryCsvParameters()->getHeader());
    if (header) {
        csvOutput.beginLine();
        csvOutput.writeHeaderLn(strlen(header), header);
        diskout->write(csvOutput.length(), csvOutput.str());
    }

    // Loop thru the results
    numRecords = 0;
    for (;;)
    {
        OwnedConstRoxieRow nextrec(input->nextRow());
        if (!nextrec)
        {
            nextrec.setown(input->nextRow());
            if (!nextrec)
                break;
        }

        try
        {
            csvOutput.beginLine();
            helper.writeRow((const byte *)nextrec.get(), &csvOutput);
            csvOutput.endLine();
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
        diskout->write(csvOutput.length(), csvOutput.str());
        numRecords++;
    }

    OwnedRoxieString footer(helper.queryCsvParameters()->getFooter());
    if (footer) {
        csvOutput.beginLine();
        csvOutput.writeHeaderLn(strlen(footer), footer);
        diskout->write(csvOutput.length(), csvOutput.str());
    }
}

void CHThorCsvWriteActivity::setFormat(IFileDescriptor * desc)
{
    // MORE - should call parent's setFormat too?
    ICsvParameters * csvInfo = helper.queryCsvParameters();
    OwnedRoxieString rs(csvInfo->getSeparator(0));
    StringBuffer separator;
    const char *s = rs;
    while (s && *s)
    {
        if (',' == *s)
            separator.append("\\,");
        else
            separator.append(*s);
        ++s;
    }
    desc->queryProperties().setProp("@csvSeparate", separator.str());
    desc->queryProperties().setProp("@csvQuote", rs.setown(csvInfo->getQuote(0)));
    desc->queryProperties().setProp("@csvTerminate", rs.setown(csvInfo->getTerminator(0)));
    desc->queryProperties().setProp("@csvEscape", rs.setown(csvInfo->getEscape(0)));
    desc->queryProperties().setProp("@format","utf8n");
    desc->queryProperties().setProp("@kind", "csv");
    const char *recordECL = helper.queryRecordECL();
    if (recordECL && *recordECL)
        desc->queryProperties().setProp("ECL", recordECL);
}

//=====================================================================================================

CHThorXmlWriteActivity::CHThorXmlWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorXmlWriteArg &_arg, ThorActivityKind _kind) : CHThorDiskWriteActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), headerLength(0), footerLength(0)
{
    OwnedRoxieString xmlpath(helper.getXmlIteratorPath());
    if (!xmlpath)
        rowTag.append(DEFAULTXMLROWTAG);
    else
    {
        const char *path = xmlpath;
        if (*path == '/') path++;
        if (strchr(path, '/')) UNIMPLEMENTED;               // more what do we do with /mydata/row
        rowTag.append(path);
    }
}

void CHThorXmlWriteActivity::execute()
{
    // Loop thru the results
    numRecords = 0;
    StringBuffer header;
    OwnedRoxieString suppliedHeader(helper.getHeader());
    if (kind==TAKjsonwrite)
        buildJsonHeader(header, suppliedHeader, rowTag);
    else if (suppliedHeader)
        header.set(suppliedHeader);
    else
        header.append(DEFAULTXMLHEADER).newline();

    headerLength = header.length();
    diskout->write(headerLength, header.str());

    Owned<IXmlWriterExt> writer = createIXmlWriterExt(helper.getXmlFlags(), 0, NULL, (kind==TAKjsonwrite) ? WTJSON : WTStandard);
    writer->outputBeginArray(rowTag); //need to set up the array
    writer->clear(); //but not output it

    for (;;)
    {
        OwnedConstRoxieRow nextrec(input->nextRow());
        if (!nextrec)
        {
            nextrec.setown(input->nextRow());
            if (!nextrec)
                break;
        }

        try
        {
            writer->clear().outputBeginNested(rowTag, false);
            helper.toXML((const byte *)nextrec.get(), *writer);
            writer->outputEndNested(rowTag);
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }

        diskout->write(writer->length(), writer->str());
        numRecords++;
    }

    OwnedRoxieString suppliedFooter(helper.getFooter());
    StringBuffer footer;
    if (kind==TAKjsonwrite)
        buildJsonFooter(footer.newline(), suppliedFooter, rowTag);
    else if (suppliedFooter)
        footer.append(suppliedFooter);
    else
        footer.append(DEFAULTXMLFOOTER).newline();

    footerLength=footer.length();
    diskout->write(footerLength, footer);
}

void CHThorXmlWriteActivity::setFormat(IFileDescriptor * desc)
{
    desc->queryProperties().setProp("@format","utf8n");
    desc->queryProperties().setProp("@rowTag",rowTag.str());
    desc->queryProperties().setProp("@kind", (kind==TAKjsonwrite) ? "json" : "xml");

    desc->queryProperties().setPropInt(FPheaderLength, headerLength);
    desc->queryProperties().setPropInt(FPfooterLength, footerLength);

    const char *recordECL = helper.queryRecordECL();
    if (recordECL && *recordECL)
        desc->queryProperties().setProp("ECL", recordECL);
}

//=====================================================================================================

void throwPipeProcessError(unsigned err, char const * preposition, char const * program, IPipeProcess * pipe)
{
    StringBuffer msg;
    msg.append("Error piping ").append(preposition).append(" (").append(program).append("): process failed with code ").append(err);
    if(pipe->hasError())
    {
        try
        {
            char error[512];
            size32_t sz = pipe->readError(sizeof(error), error);
            if(sz && sz!=(size32_t)-1)
                msg.append(", stderr: '").append(sz, error).append("'");
        }
        catch (IException *e)
        {
            EXCLOG(e, "Error reading pipe stderr");
            e->Release();
        }
    }
    throw MakeStringExceptionDirect(2, msg.str());
}

//=====================================================================================================

CHThorIndexWriteActivity::CHThorIndexWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexWriteArg &_arg, ThorActivityKind _kind) : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    incomplete = false;
    StringBuffer lfn;
    OwnedRoxieString fname(helper.getFileName());
    expandLogicalFilename(lfn, fname, agent.queryWorkUnit(), agent.queryResolveFilesLocally(), false);
    if (!agent.queryResolveFilesLocally())
    {
        Owned<IDistributedFile> f = queryDistributedFileDirectory().lookup(lfn, agent.queryCodeContext()->queryUserDescriptor(), true);

        if (f)
        {
            if (TIWoverwrite & helper.getFlags()) 
            {
                PrintLog("Removing %s from DFS", lfn.str());
                agent.logFileAccess(f, "HThor", "DELETED");
                f->detach();
            }
            else // not quite sure about raising exceptions in constructors
                throw MakeStringException(99, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", lfn.str());
        }
    }
    clusterHandler.setown(createClusterWriteHandler(agent, &helper, NULL, lfn, filename, false));
    sizeLimit = agent.queryWorkUnit()->getDebugValueInt64("hthorDiskWriteSizeLimit", defaultHThorDiskWriteSizeLimit);
}

CHThorIndexWriteActivity::~CHThorIndexWriteActivity()
{
    if(incomplete)
    {
        PROGLOG("Index write incomplete, deleting physical file: %s", filename.get());
        file->remove();
    }
}

void CHThorIndexWriteActivity::execute()
{
    size32_t maxDiskRecordSize;
    if (helper.queryDiskRecordSize()->isVariableSize())
    {
        if (helper.getFlags() & TIWmaxlength)
            maxDiskRecordSize = helper.getMaxKeySize();
        else
            maxDiskRecordSize = KEYBUILD_MAXLENGTH; // Current default behaviour, could be improved in the future
    }
    else
        maxDiskRecordSize = helper.queryDiskRecordSize()->getFixedSize();

    if (maxDiskRecordSize > KEYBUILD_MAXLENGTH)
        throw MakeStringException(99, "Index maximum record length (%d) exceeds 32K internal limit", maxDiskRecordSize);

    OwnedMalloc<char> rowBuffer(maxDiskRecordSize, true);

    // Loop thru the results
    unsigned __int64 reccount = 0;
    unsigned int fileCrc = -1;
    file.setown(createIFile(filename.get()));
    {
        OwnedIFileIO io;
        try
        {
            io.setown(file->open(IFOcreate));
        }
        catch(IException * e)
        {
            e->Release();
            clearKeyStoreCache(false);
            io.setown(file->open(IFOcreate));
        }
        incomplete = true;
        Owned<IFileIOStream> out = createIOStream(io);
        bool isVariable = helper.queryDiskRecordSize()->isVariableSize();
        unsigned flags = COL_PREFIX | HTREE_FULLSORT_KEY;
        if (helper.getFlags() & TIWrowcompress)
            flags |= HTREE_COMPRESSED_KEY|HTREE_QUICK_COMPRESSED_KEY;
        else if (!(helper.getFlags() & TIWnolzwcompress))
            flags |= HTREE_COMPRESSED_KEY;
        if (isVariable)
            flags |= HTREE_VARSIZE;
        Owned<IPropertyTree> metadata;
        buildUserMetadata(metadata);
        buildLayoutMetadata(metadata);
        unsigned nodeSize = metadata ? metadata->getPropInt("_nodeSize", NODESIZE) : NODESIZE;
        size32_t keyMaxSize = helper.queryDiskRecordSize()->getRecordSize(NULL);
        Owned<IKeyBuilder> builder = createKeyBuilder(out, flags, keyMaxSize, nodeSize, helper.getKeyedSize(), 0);
        class BcWrapper : implements IBlobCreator
        {
            IKeyBuilder *builder;
        public:
            BcWrapper(IKeyBuilder *_builder) : builder(_builder) {}
            virtual unsigned __int64 createBlob(size32_t size, const void * ptr)
            {
                return builder->createBlob(size, (const char *) ptr);
            }
        } bc(builder);
        for (;;)
        {
            OwnedConstRoxieRow nextrec(input->nextRow());
            if (!nextrec)
            {
                nextrec.setown(input->nextRow());
                if (!nextrec)
                    break;
            }
            try
            {
                unsigned __int64 fpos;
                RtlStaticRowBuilder rowBuilder(rowBuffer, maxDiskRecordSize);
                size32_t thisSize = helper.transform(rowBuilder, nextrec, &bc, fpos);
                builder->processKeyData(rowBuffer, fpos, thisSize);
            }
            catch(IException * e)
            {
                throw makeWrappedException(e);
            }
            if(sizeLimit && (out->tell() > sizeLimit))
            {
                StringBuffer msg;
                OwnedRoxieString fname(helper.getFileName());
                msg.append("Exceeded disk write size limit of ").append(sizeLimit).append(" while writing index ").append(fname);
                throw MakeStringExceptionDirect(0, msg.str());
            }
            reccount++;
        }
        if(metadata)
            builder->finish(metadata,&fileCrc);
        else
            builder->finish(&fileCrc);
        out->flush();
        out.clear();
    }

    if(clusterHandler)
        clusterHandler->copyPhysical(file, agent.queryWorkUnit()->getDebugValueBool("__output_cluster_no_copy_physical", false));

    // Now publish to name services
    StringBuffer dir,base;
    offset_t indexFileSize = file->size();
    if(clusterHandler)
        clusterHandler->splitPhysicalFilename(dir, base);
    else
        splitFilename(filename, &dir, &dir, &base, &base);

    Owned<IFileDescriptor> desc = createFileDescriptor();
    desc->setDefaultDir(dir.str());

    //properties of the first file part.
    Owned<IPropertyTree> attrs;
    if(clusterHandler) 
        attrs.setown(createPTree("Part"));  // clusterHandler is going to set attributes
    else 
    {
        // add cluster
        StringBuffer mygroupname;
        Owned<IGroup> mygrp = NULL;
        if (!agent.queryResolveFilesLocally())
            mygrp.setown(agent.getHThorGroup(mygroupname));
        ClusterPartDiskMapSpec partmap; // will get this from group at some point
        desc->setNumParts(1);
        desc->setPartMask(base.str());
        desc->addCluster(mygroupname.str(),mygrp, partmap);
        attrs.set(&desc->queryPart(0)->queryProperties());
    }
    attrs->setPropInt64("@size", indexFileSize);

    CDateTime createTime, modifiedTime, accessedTime;
    file->getTime(&createTime, &modifiedTime, &accessedTime);
    // round file time down to nearest sec. Nanosec accurancy is not preserved elsewhere and can lead to mismatch later.
    unsigned hour, min, sec, nanosec;
    modifiedTime.getTime(hour, min, sec, nanosec);
    modifiedTime.setTime(hour, min, sec, 0);
    StringBuffer timestr;
    modifiedTime.getString(timestr);
    if(timestr.length())
        attrs->setProp("@modified", timestr.str());

    if(clusterHandler)
        clusterHandler->setDescriptorParts(desc, base.str(), attrs);

    // properties of the logical file
    IPropertyTree & properties = desc->queryProperties();
    properties.setProp("@kind", "key");
    properties.setPropInt64("@size", indexFileSize);
    properties.setPropInt64("@recordCount", reccount);
    properties.setProp("@owner", agent.queryWorkUnit()->queryUser());
    properties.setProp("@workunit", agent.queryWorkUnit()->queryWuid());
    properties.setProp("@job", agent.queryWorkUnit()->queryJobName());
#if 0
    IRecordSize * irecsize = helper.queryDiskRecordSize();
    if(irecsize && (irecsize->isFixedSize()))
        properties.setPropInt("@recordSize", irecsize->getFixedSize());
#endif
    char const * rececl = helper.queryRecordECL();
    if(rececl && *rececl)
        properties.setProp("ECL", rececl);
    

    if (helper.getFlags() & TIWexpires)
        setExpiryTime(properties, helper.getExpiryDays());
    if (helper.getFlags() & TIWupdate)
    {
        unsigned eclCRC;
        unsigned __int64 totalCRC;
        helper.getUpdateCRCs(eclCRC, totalCRC);
        properties.setPropInt("@eclCRC", eclCRC);
        properties.setPropInt64("@totalCRC", totalCRC);
    }

    properties.setPropInt("@fileCrc", fileCrc);
    properties.setPropInt("@formatCrc", helper.getFormatCrc());
    void * layoutMetaBuff;
    size32_t layoutMetaSize;
    if(helper.getIndexLayout(layoutMetaSize, layoutMetaBuff))
    {
        properties.setPropBin("_record_layout", layoutMetaSize, layoutMetaBuff);
        rtlFree(layoutMetaBuff);
    }
    StringBuffer lfn;
    Owned<IDistributedFile> dfile = NULL;
    if (!agent.queryResolveFilesLocally())
    {
        dfile.setown(queryDistributedFileDirectory().createNew(desc));
        OwnedRoxieString fname(helper.getFileName());
        expandLogicalFilename(lfn, fname, agent.queryWorkUnit(), agent.queryResolveFilesLocally(), false);
        dfile->attach(lfn.str(),agent.queryCodeContext()->queryUserDescriptor());
        agent.logFileAccess(dfile, "HThor", "CREATED");
    }
    else
        lfn = filename;

    incomplete = false;

    if(clusterHandler)
        clusterHandler->finish(file);

    // and update wu info
    if (helper.getSequence() >= 0)
    {
        WorkunitUpdate wu = agent.updateWorkUnit();
        Owned<IWUResult> result = wu->updateResultBySequence(helper.getSequence());
        if (result)
        {
            result->setResultTotalRowCount(reccount); 
            result->setResultStatus(ResultStatusCalculated);
            result->setResultLogicalName(lfn.str());
        }
    }
}

void CHThorIndexWriteActivity::buildUserMetadata(Owned<IPropertyTree> & metadata)
{
    size32_t nameLen;
    char * nameBuff;
    size32_t valueLen;
    char * valueBuff;
    unsigned idx = 0;
    while(helper.getIndexMeta(nameLen, nameBuff, valueLen, valueBuff, idx++))
    {
        StringBuffer name(nameLen, nameBuff);
        StringBuffer value(valueLen, valueBuff);
        if(*nameBuff == '_' && strcmp(name, "_nodeSize") != 0)
        {
            OwnedRoxieString fname(helper.getFileName());
            throw MakeStringException(0, "Invalid name %s in user metadata for index %s (names beginning with underscore are reserved)", name.str(), fname.get());
        }
        if(!validateXMLTag(name.str()))
        {
            OwnedRoxieString fname(helper.getFileName());
            throw MakeStringException(0, "Invalid name %s in user metadata for index %s (not legal XML element name)", name.str(), fname.get());
        }
        if(!metadata) metadata.setown(createPTree("metadata"));
        metadata->setProp(name.str(), value.str());
    }
}

void CHThorIndexWriteActivity::buildLayoutMetadata(Owned<IPropertyTree> & metadata)
{
    if(!metadata) metadata.setown(createPTree("metadata"));
    metadata->setProp("_record_ECL", helper.queryRecordECL());

    void * layoutMetaBuff;
    size32_t layoutMetaSize;
    if(helper.getIndexLayout(layoutMetaSize, layoutMetaBuff))
    {
        metadata->setPropBin("_record_layout", layoutMetaSize, layoutMetaBuff);
        rtlFree(layoutMetaBuff);
    }
}

//=====================================================================================================

class CHThorPipeReadActivity : public CHThorSimpleActivityBase
{
    IHThorPipeReadArg &helper;
    Owned<IPipeProcess> pipe;
    StringAttr pipeCommand;
    Owned<IOutputRowDeserializer> rowDeserializer;
    Owned<IReadRowStream> readTransformer;
    bool groupSignalled;
public:
    CHThorPipeReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorPipeReadArg &_arg, ThorActivityKind _kind)
        : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
    {
        groupSignalled = true;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void ready()
    {
        groupSignalled = true; // i.e. don't start with a NULL row
        CHThorSimpleActivityBase::ready();
        rowDeserializer.setown(rowAllocator->createDiskDeserializer(agent.queryCodeContext()));
        OwnedRoxieString xmlIteratorPath(helper.getXmlIteratorPath());
        readTransformer.setown(createReadRowStream(rowAllocator, rowDeserializer, helper.queryXmlTransformer(), helper.queryCsvTransformer(), xmlIteratorPath, helper.getPipeFlags()));
        OwnedRoxieString pipeProgram(helper.getPipeProgram());
        openPipe(pipeProgram);
    }

    virtual void stop()
    {
        //Need to close the output (or read it in its entirety), otherwise we might wait forever for the
        //program to finish
        if (pipe)
            pipe->closeOutput();
        pipe.clear();
        readTransformer->setStream(NULL);
        CHThorSimpleActivityBase::stop();
    }

    virtual const void *nextRow()
    {
        while (!waitForPipe())
        {
            if (!pipe)
                return NULL;
            if (helper.getPipeFlags() & TPFgroupeachrow)
            {
                if (!groupSignalled)
                {
                    groupSignalled = true;
                    return NULL;
                }
            }
        }
        const void *ret = readTransformer->next();
        assertex(ret != NULL); // if ret can ever be NULL then we need to recode this logic
        processed++;
        groupSignalled = false;
        return ret;
    }

protected:
    bool waitForPipe()
    {
        if (!pipe)
            return false;  // done
        if (!readTransformer->eos())
            return true;
        verifyPipe();
        return false;
    }

    void openPipe(char const * cmd)
    {
        pipeCommand.setown(cmd);
        pipe.setown(createPipeProcess(agent.queryAllowedPipePrograms()));
        if(!pipe->run(NULL, cmd, ".", false, true, true, 0x10000))
            throw MakeStringException(2, "Could not run pipe process %s", cmd);
        Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
        readTransformer->setStream(pipeReader.get());
    }

    void verifyPipe()
    {
        if (pipe)
        {
            unsigned err = pipe->wait();
            if(err && !(helper.getPipeFlags() & TPFnofail))
                throwPipeProcessError(err, "from", pipeCommand.get(), pipe);
            pipe.clear();
        }
    }
};

//=====================================================================================================

// Through pipe code - taken from Roxie implementation

interface IPipeRecordPullerCallback : extends IExceptionHandler
{
    virtual void processRow(const void *row) = 0;
    virtual void processDone() = 0;
    virtual const void *nextInput() = 0;
};

class CPipeRecordPullerThread : public Thread
{
protected:
    IPipeRecordPullerCallback *helper;
    bool eog;

public:
    CPipeRecordPullerThread() : Thread("PipeRecordPullerThread")
    {
        helper = NULL;
        eog = false;
    }

    void setInput(IPipeRecordPullerCallback *_helper)
    {
        helper = _helper;
    }

    virtual int run()
    {
        try
        {
            for (;;)
            {
                const void * row = helper->nextInput();
                if (row)
                {
                    eog = false;
                    helper->processRow(row);
                }
                else if (!eog)
                {
                    eog = true;
                }
                else
                {
                    break;
                }
            }
            helper->processDone();
        }
        catch (IException *e)
        {
            helper->fireException(e);
        }
        catch (...)
        {
            helper->fireException(MakeStringException(2, "Unexpected exception caught in PipeRecordPullerThread::run"));
        }
        return 0;
    }
};

class CHThorPipeThroughActivity : public CHThorSimpleActivityBase, implements IPipeRecordPullerCallback
{
    IHThorPipeThroughArg &helper;
    CPipeRecordPullerThread puller;
    Owned<IPipeProcess> pipe;
    StringAttr pipeCommand;
    InterruptableSemaphore pipeVerified;
    InterruptableSemaphore pipeOpened;
    CachedOutputMetaData inputMeta;
    Owned<IOutputRowSerializer> rowSerializer;
    Owned<IOutputRowDeserializer> rowDeserializer;
    Owned<IPipeWriteXformHelper> writeTransformer;
    Owned<IReadRowStream> readTransformer;
    bool firstRead;
    bool recreate;
    bool inputExhausted;
    bool groupSignalled;

public:
    CHThorPipeThroughActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorPipeThroughArg &_arg, ThorActivityKind _kind)
        : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
    {
        recreate = helper.recreateEachRow();
        groupSignalled = true;
        firstRead = false;
        inputExhausted = false;
        puller.setInput(this);
    }

    virtual void ready()
    {
        CHThorSimpleActivityBase::ready();
        // From the create() in roxie

        inputMeta.set(input->queryOutputMeta());
        rowSerializer.setown(inputMeta.createDiskSerializer(agent.queryCodeContext(), activityId));
        rowDeserializer.setown(rowAllocator->createDiskDeserializer(agent.queryCodeContext()));
        writeTransformer.setown(createPipeWriteXformHelper(helper.getPipeFlags(), helper.queryXmlOutput(), helper.queryCsvOutput(), rowSerializer));

        // From the start() in roxie
        firstRead = true;
        inputExhausted = false;
        groupSignalled = true; // i.e. don't start with a NULL row
        pipeVerified.reinit();
        pipeOpened.reinit();
        writeTransformer->ready();

        if (!readTransformer)
        {
            OwnedRoxieString xmlIterator(helper.getXmlIteratorPath());
            readTransformer.setown(createReadRowStream(rowAllocator, rowDeserializer, helper.queryXmlTransformer(), helper.queryCsvTransformer(), xmlIterator, helper.getPipeFlags()));
        }
        if(!recreate)
        {
            OwnedRoxieString pipeProgram(helper.getPipeProgram());
            openPipe(pipeProgram);
        }
        puller.start();
    }

    void stop()
    {
        //Need to close the output (or read it in its entirety), otherwise we might wait forever for the
        //program to finish
        if (pipe)
            pipe->closeOutput();
        pipeVerified.interrupt(NULL);
        pipeOpened.interrupt(NULL);
        puller.join();
        CHThorSimpleActivityBase::stop();
        pipe.clear();
        readTransformer->setStream(NULL);
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextRow()
    {
        while (!waitForPipe())
        {
            if (!pipe)
                return NULL;
            if (helper.getPipeFlags() & TPFgroupeachrow)
            {
                if (!groupSignalled)
                {
                    groupSignalled = true;
                    return NULL;
                }
            }
        }
        const void *ret = readTransformer->next();
        assertex(ret != NULL); // if ret can ever be NULL then we need to recode this logic
        processed++;
        groupSignalled = false;
        return ret;
    }

    virtual bool isGrouped()
    {
        return outputMeta.isGrouped();
    }

    virtual void processRow(const void *row)
    {
        // called from puller thread
        if(recreate)
            openPipe(helper.getNameFromRow(row));
        writeTransformer->writeTranslatedText(row, pipe);
        ReleaseRoxieRow(row);
        if(recreate)
        {
            closePipe();
            pipeVerified.wait();
        }
    }

    virtual void processDone()
    {
        // called from puller thread
        if(recreate)
        {
            inputExhausted = true;
            pipeOpened.signal();
        }
        else
        {
            closePipe();
            pipeVerified.wait();
        }
    }

    virtual const void *nextInput()
    {
        return input->nextRow();
    }

    virtual bool fireException(IException *e)
    {
        inputExhausted = true;
        pipeOpened.interrupt(LINK(e));
        pipeVerified.interrupt(e);
        return true;
    }

private:
    bool waitForPipe()
    {
        if (firstRead)
        {
            pipeOpened.wait();
            firstRead = false;
        }
        if (!pipe)
            return false;  // done
        if (!readTransformer->eos())
            return true;
        verifyPipe();
        if (recreate && !inputExhausted)
            pipeOpened.wait();
        return false;
    }

    void openPipe(char const * cmd)
    {
        pipeCommand.setown(cmd);
        pipe.setown(createPipeProcess(agent.queryAllowedPipePrograms()));
        if(!pipe->run(NULL, cmd, ".", true, true, true, 0x10000))
            throw MakeStringException(2, "Could not run pipe process %s", cmd);
        writeTransformer->writeHeader(pipe);
        Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
        readTransformer->setStream(pipeReader.get());
        pipeOpened.signal();
    }

    void closePipe()
    {
        writeTransformer->writeFooter(pipe);
        pipe->closeInput();
    }

    void verifyPipe()
    {
        if (pipe)
        {
            unsigned err = pipe->wait();
            if(err && !(helper.getPipeFlags() & TPFnofail))
                throwPipeProcessError(err, "through", pipeCommand.get(), pipe);
            pipe.clear();
            pipeVerified.signal();
        }
    }
};

class CHThorPipeWriteActivity : public CHThorActivityBase
{
    IHThorPipeWriteArg &helper;
    Owned<IPipeProcess> pipe;
    StringAttr pipeCommand;
    CachedOutputMetaData inputMeta;
    Owned<IOutputRowSerializer> rowSerializer;
    Owned<IPipeWriteXformHelper> writeTransformer;
    bool firstRead;
    bool recreate;
    bool inputExhausted;
public:
    IMPLEMENT_SINKACTIVITY;

    CHThorPipeWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorPipeWriteArg &_arg, ThorActivityKind _kind)
        : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
    {
        recreate = helper.recreateEachRow();
        firstRead = false;
        inputExhausted = false;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void ready()
    {
        CHThorActivityBase::ready();
        inputMeta.set(input->queryOutputMeta());
        rowSerializer.setown(inputMeta.createDiskSerializer(agent.queryCodeContext(), activityId));
        writeTransformer.setown(createPipeWriteXformHelper(helper.getPipeFlags(), helper.queryXmlOutput(), helper.queryCsvOutput(), rowSerializer));

        firstRead = true;
        inputExhausted = false;
        writeTransformer->ready();
        if(!recreate)
        {
            OwnedRoxieString pipeProgram(helper.getPipeProgram());
            openPipe(pipeProgram);
        }
    }

    virtual void execute()
    {
        for (;;)
        {
            const void *row = input->nextRow();
            if (!row)
            {
                row = input->nextRow();
                if (!row)
                    break;
            }
            processed++;
            if(recreate)
                openPipe(helper.getNameFromRow(row));
            writeTransformer->writeTranslatedText(row, pipe);
            ReleaseRoxieRow(row);
            if(recreate)
                closePipe();
        }
        closePipe();
        if (helper.getSequence() >= 0)
        {
            WorkunitUpdate wu = agent.updateWorkUnit();
            Owned<IWUResult> result = wu->updateResultBySequence(helper.getSequence());
            if (result)
            {
                result->setResultTotalRowCount(processed);
                result->setResultStatus(ResultStatusCalculated);
            }
        }
    }

private:
    void openPipe(char const * cmd)
    {
        pipeCommand.setown(cmd);
        pipe.setown(createPipeProcess(agent.queryAllowedPipePrograms()));
        if(!pipe->run(NULL, cmd, ".", true, false, true, 0x10000))
            throw MakeStringException(2, "Could not run pipe process %s", cmd);
        writeTransformer->writeHeader(pipe);
    }

    void closePipe()
    {
        writeTransformer->writeFooter(pipe);
        pipe->closeInput();
        unsigned err = pipe->wait();
        if(err && !(helper.getPipeFlags() & TPFnofail))
            throwPipeProcessError(err, "to", pipeCommand.get(), pipe);
        pipe.clear();
    }
};

//=====================================================================================================

CHThorIterateActivity::CHThorIterateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIterateArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorIterateActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    right.clear();
    left.clear();
}

void CHThorIterateActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    if (!defaultRecord)
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t thisSize = helper.createDefault(rowBuilder);
        defaultRecord.setown(rowBuilder.finalizeRowClear(thisSize));
    }
    counter = 0;
}

const void *CHThorIterateActivity::nextRow()
{
    for (;;)
    {
        right.setown(input->nextRow());
        if(!right)
        {
            bool skippedGroup = (!left) && (counter > 0); //we have just skipped entire group, but shouldn't output a double null
            left.clear();
            counter = 0;
            if(skippedGroup) continue;
            return NULL;
        }
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned outSize = helper.transform(rowBuilder, left ? left : defaultRecord, right, ++counter);
            if (outSize)
            {
                left.setown(rowBuilder.finalizeRowClear(outSize));  
                processed++;
                return left.getLink();
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=====================================================================================================

CHThorProcessActivity::CHThorProcessActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorProcessArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

CHThorProcessActivity::~CHThorProcessActivity()
{
}


void CHThorProcessActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    rightRowAllocator.setown(agent.queryCodeContext()->getRowAllocator( helper.queryRightRecordSize(), activityId));

    RtlDynamicRowBuilder rowBuilder(rightRowAllocator);
    size32_t thisSize = helper.createInitialRight(rowBuilder);
    initialRight.setown(rowBuilder.finalizeRowClear(thisSize));

    curRight.set(initialRight);
    counter = 0;
}

const void *CHThorProcessActivity::nextRow()
{
    try
    {
        for (;;)
        {
            OwnedConstRoxieRow next(input->nextRow());
            if (!next)
            {
                bool eog = (curRight != initialRight);          // processed any records?
                counter = 0;
                curRight.set(initialRight);
                if (eog)
                    return NULL;
                next.setown(input->nextRow());
                if (!next)
                    return NULL;
            }

            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            RtlDynamicRowBuilder rightRowBuilder(rightRowAllocator);
            size32_t outSize = helper.transform(rowBuilder, rightRowBuilder, next, curRight, ++counter);

            if (outSize)
            {
                size32_t rightSize = rightRowAllocator->queryOutputMeta()->getRecordSize(rightRowBuilder.getSelf());    // yuk
                curRight.setown(rightRowBuilder.finalizeRowClear(rightSize));
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

//=====================================================================================================

CHThorNormalizeActivity::CHThorNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    IRecordSize* recSize = outputMeta;
    if (recSize == NULL)
        throw MakeStringException(2, "Unexpected null pointer from helper.queryOutputMeta()");
}

CHThorNormalizeActivity::~CHThorNormalizeActivity()
{
}

void CHThorNormalizeActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    numThisRow = 0;
    curRow = 0;
    numProcessedLastGroup = processed;
}

const void *CHThorNormalizeActivity::nextRow()
{
    for (;;)
    {
        while (curRow == numThisRow)
        {
            inbuff.setown(input->nextRow());
            if (!inbuff && (processed == numProcessedLastGroup))
                inbuff.setown(input->nextRow());
            if (!inbuff)
            {
                numProcessedLastGroup = processed;
                return NULL;
            }

            curRow = 0;
            numThisRow = helper.numExpandedRows(inbuff);
        }


        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            memsize_t thisSize = helper.transform(rowBuilder, inbuff, ++curRow);
            if(thisSize != 0)
            {
                processed++;
                return rowBuilder.finalizeRowClear(thisSize);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}


//=====================================================================================================

CHThorNormalizeChildActivity::CHThorNormalizeChildActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeChildArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

CHThorNormalizeChildActivity::~CHThorNormalizeChildActivity()
{
}

bool CHThorNormalizeChildActivity::advanceInput()
{
    for (;;)
    {
        inbuff.setown(input->nextRow());
        if (!inbuff && (processed == numProcessedLastGroup))
            inbuff.setown(input->nextRow());
        if (!inbuff)
        {
            numProcessedLastGroup = processed;
            return false;
        }

        curChildRow = cursor->first(inbuff);
        if (curChildRow)
        {
            curRow = 0;
            return true;
        }
    }
}

void CHThorNormalizeChildActivity::stop()
{
    inbuff.clear();
    CHThorSimpleActivityBase::stop();
}

void CHThorNormalizeChildActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    curRow = 0;
    numProcessedLastGroup = processed;
    cursor = helper.queryIterator();
    curChildRow = NULL;
}

const void *CHThorNormalizeChildActivity::nextRow()
{
    for (;;)
    {
        if (!inbuff)
        {
            if (!advanceInput())
                return NULL;
        }

        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transform(rowBuilder, inbuff, curChildRow, ++curRow);
            curChildRow = cursor->next();
            if (!curChildRow)
                inbuff.clear();
            if (outSize != 0)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=================================================================================
bool CHThorNormalizeLinkedChildActivity::advanceInput()
{
    for (;;)
    {
        curParent.setown(input->nextRow());
        if (!curParent && (processed == numProcessedLastGroup))
            curParent.setown(input->nextRow());
        if (!curParent)
        {
            numProcessedLastGroup = processed;
            return false;
        }

        curChild.set(helper.first(curParent));
        if (curChild)
            return true;
    }
}

CHThorNormalizeLinkedChildActivity::CHThorNormalizeLinkedChildActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeLinkedChildArg &_arg, ThorActivityKind _kind) 
    : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}
CHThorNormalizeLinkedChildActivity::~CHThorNormalizeLinkedChildActivity()
{
}

void CHThorNormalizeLinkedChildActivity::ready()
{
    numProcessedLastGroup = 0;
    CHThorSimpleActivityBase::ready();
}

void CHThorNormalizeLinkedChildActivity::stop()
{
    curParent.clear();
    curChild.clear();
    CHThorSimpleActivityBase::stop(); 
}

const void * CHThorNormalizeLinkedChildActivity::nextRow()
{
    for (;;)
    {
        if (!curParent)
        {
            if (!advanceInput())
                return NULL;
        }
        try
        {
            const void *ret = curChild.getClear();
            curChild.set(helper.next());
            if (!curChild)
                curParent.clear();
            if (ret)
            {
                processed++;
                return ret;
            }
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }
}

//=====================================================================================================
CHThorProjectActivity::CHThorProjectActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorProjectArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

CHThorProjectActivity::~CHThorProjectActivity()
{
}

void CHThorProjectActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    numProcessedLastGroup = processed;
}


const void * CHThorProjectActivity::nextRow()
{
    for (;;)
    {
        OwnedConstRoxieRow in(input->nextRow());
        if (!in)
        {
            if (numProcessedLastGroup == processed)
                in.setown(input->nextRow());
            if (!in)
            {
                numProcessedLastGroup = processed;
                return NULL;
            }
        }

        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transform(rowBuilder, in);
            if (outSize)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=====================================================================================================
CHThorPrefetchProjectActivity::CHThorPrefetchProjectActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorPrefetchProjectArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorPrefetchProjectActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    recordCount = 0;
    numProcessedLastGroup = processed;
    eof = !helper.canMatchAny();
    child = helper.queryChild();
}

const void * CHThorPrefetchProjectActivity::nextRow()
{
    if (eof)
        return NULL;
    for (;;)
    {
        try
        {
            OwnedConstRoxieRow row(input->nextRow());
            if (!row)
            {
                if (numProcessedLastGroup == processed)
                    row.setown(input->nextRow());
                if (!row)
                {
                    numProcessedLastGroup = processed;
                    return NULL;
                }
            }

            ++recordCount;
            rtlRowBuilder extract;
            if (helper.preTransform(extract,row,recordCount))
            {
                Owned<IEclGraphResults> results;
                if (child)
                {
                    results.setown(child->evaluate(extract.size(), extract.getbytes()));
                }
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size32_t outSize = helper.transform(rowBuilder, row, results, recordCount);
                if (outSize)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=====================================================================================================
CHThorFilterProjectActivity::CHThorFilterProjectActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorFilterProjectArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

CHThorFilterProjectActivity::~CHThorFilterProjectActivity()
{
}

void CHThorFilterProjectActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    recordCount = 0;
    numProcessedLastGroup = processed;
    eof = !helper.canMatchAny();
}


const void * CHThorFilterProjectActivity::nextRow()
{
    if (eof)
        return NULL;
    for (;;)
    {
        OwnedConstRoxieRow in = input->nextRow();
        if (!in)
        {
            recordCount = 0;
            if (numProcessedLastGroup == processed)
                in.setown(input->nextRow());
            if (!in)
            {
                numProcessedLastGroup = processed;
                return NULL;
            }
        }

        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transform(rowBuilder, in, ++recordCount);
            if (outSize)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);    
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}
//=====================================================================================================

CHThorCountProjectActivity::CHThorCountProjectActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCountProjectArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

CHThorCountProjectActivity::~CHThorCountProjectActivity()
{
}

void CHThorCountProjectActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    recordCount = 0;
    numProcessedLastGroup = processed;
}


const void * CHThorCountProjectActivity::nextRow()
{
    for (;;)
    {
        OwnedConstRoxieRow in = input->nextRow();
        if (!in)
        {
            recordCount = 0;
            if (numProcessedLastGroup == processed)
                in.setown(input->nextRow());
            if (!in)
            {
                numProcessedLastGroup = processed;
                return NULL;
            }
        }

        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transform(rowBuilder, in, ++recordCount);
            if (outSize)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=====================================================================================================

CHThorRollupActivity::CHThorRollupActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorRollupArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

CHThorRollupActivity::~CHThorRollupActivity()
{
}

void CHThorRollupActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    left.setown(input->nextRow());
    prev.set(left);
}

void CHThorRollupActivity::stop()
{
    left.clear();
    prev.clear();
    right.clear();
    CHThorSimpleActivityBase::stop();
}

const void *CHThorRollupActivity::nextRow()
{
    for (;;)
    {
        right.setown(input->nextRow());
        if(!prev || !right || !helper.matches(prev,right))
        {
            const void * ret = left.getClear();
            if(ret)
            {
                processed++;
            }
            left.setown(right.getClear());
            prev.set(left);
            return ret;
        }
        try
        {
            //MORE: could optimise by reusing buffer, but would have to make sure to call destructor on previous contents before overwriting
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            if(unsigned outSize = helper.transform(rowBuilder, left, right))
            {
                left.setown(rowBuilder.finalizeRowClear(outSize));
            }
            if (helper.getFlags() & RFrolledismatchleft)
                prev.set(left);
            else
                prev.set(right);
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=====================================================================================================

CHThorGroupDedupActivity::CHThorGroupDedupActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorGroupDedupActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    numToKeep = helper.numToKeep();
    numKept = 0;
}

//=====================================================================================================

CHThorGroupDedupKeepLeftActivity::CHThorGroupDedupKeepLeftActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &_arg, ThorActivityKind _kind) : CHThorGroupDedupActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

void CHThorGroupDedupKeepLeftActivity::ready()
{
    CHThorGroupDedupActivity::ready();
    prev.clear();
}

void CHThorGroupDedupKeepLeftActivity::stop()
{
    prev.clear();
    CHThorSimpleActivityBase::stop();
}

const void *CHThorGroupDedupKeepLeftActivity::nextRow()
{
    OwnedConstRoxieRow next;
    for (;;)
    {
        next.setown(input->nextRow());
        if (!prev || !next || !helper.matches(prev,next))
        {
            numKept = 0;
            break;
        }

        if (numKept < numToKeep-1)
        {
            numKept++;
            break;
        }
    }

    const void * ret = next.getClear();
    prev.set(ret);
    if(ret)
        processed++;
    return ret;
}

const void * CHThorGroupDedupKeepLeftActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    OwnedConstRoxieRow next;
    for (;;)
    {
        next.setown(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
        if (!prev || !next || !helper.matches(prev,next))
        {
            numKept = 0;
            break;
        }

        if (numKept < numToKeep-1)
        {
            numKept++;
            break;
        }
    }

    const void * ret = next.getClear();
    prev.set(ret);
    if(ret)
        processed++;
    return ret;
}

void CHThorGroupDedupKeepLeftActivity::setInput(unsigned index, IHThorInput *_input)
{
    CHThorGroupDedupActivity::setInput(index, _input);
    if (input)
        inputStepping = input->querySteppingMeta();
}

IInputSteppingMeta * CHThorGroupDedupKeepLeftActivity::querySteppingMeta()
{
    return inputStepping;
}

bool CHThorGroupDedupKeepLeftActivity::gatherConjunctions(ISteppedConjunctionCollector & collector)
{
    return input->gatherConjunctions(collector);
}

void CHThorGroupDedupKeepLeftActivity::resetEOF()
{
    input->resetEOF();
}


//=====================================================================================================

CHThorGroupDedupKeepRightActivity::CHThorGroupDedupKeepRightActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &_arg, ThorActivityKind _kind) : CHThorGroupDedupActivity(_agent, _activityId, _subgraphId, _arg, _kind), compareBest(nullptr)
{
}

void CHThorGroupDedupKeepRightActivity::ready()
{
    CHThorGroupDedupActivity::ready();
    assertex(numToKeep==1);
    firstDone = false;
    if (helper.keepBest())
        compareBest = helper.queryCompareBest();
}

void CHThorGroupDedupKeepRightActivity::stop()
{
    kept.clear();
    CHThorGroupDedupActivity::stop();
}

const void *CHThorGroupDedupKeepRightActivity::nextRow()
{
    if (!firstDone)
    {
        firstDone = true;
        kept.setown(input->nextRow());
    }

    OwnedConstRoxieRow next;
    for (;;)
    {
        next.setown(input->nextRow());
        if (!kept || !next || !helper.matches(kept,next))
        {
            numKept = 0;
            break;
        }

        if (compareBest)
        {
            if (compareBest->docompare(kept,next) > 0)
                kept.setown(next.getClear());
        }
        else
        {
            if (numKept < numToKeep-1)
            {
                numKept++;
                break;
            }

            kept.setown(next.getClear());
        }
    }

    const void * ret = kept.getClear();
    kept.setown(next.getClear());
    if(ret)
        processed++;
    return ret;
}

//=====================================================================================================

CHThorGroupDedupAllActivity::CHThorGroupDedupAllActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorGroupDedupAllActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    keepLeft = helper.keepLeft();
    primaryCompare = helper.queryComparePrimary();
    assertex(helper.numToKeep() == 1);
    firstDone = false;
    survivorIndex = 0;
}

void CHThorGroupDedupAllActivity::stop()
{
    survivors.clear();
    CHThorSimpleActivityBase::stop();
}

bool CHThorGroupDedupAllActivity::calcNextDedupAll()
{
    survivors.clear();
    survivorIndex = 0;

    OwnedRowArray group;
    const void * next;
    while((next = input->nextRow()) != NULL)
        group.append(next);
    if(group.ordinality() == 0)
        return false;

    unsigned max = group.ordinality();
    if (primaryCompare)
    {
        //hard, if not impossible, to hit this code once optimisations in place
        MemoryAttr indexbuff(max*sizeof(void *));
        void ** temp = (void **)indexbuff.bufferBase();
        void ** rows = (void * *)group.getArray();
        msortvecstableinplace(rows, max, *primaryCompare, temp);
        unsigned first = 0;
        for (unsigned idx = 1; idx < max; idx++)
        {
            if (primaryCompare->docompare(rows[first], rows[idx]) != 0)
            {
                dedupRange(first, idx, group);
                first = idx;
            }
        }
        dedupRange(first, max, group);

        for(unsigned idx2=0; idx2<max; ++idx2)
        {
            void * cur = rows[idx2];
            if(cur)
            {
                LinkRoxieRow(cur);
                survivors.append(cur);
            }
        }
    }
    else
    {
        dedupRange(0, max, group);
        for(unsigned idx=0; idx<max; ++idx)
        {
            const void * cur = group.itemClear(idx);
            if(cur)
                survivors.append(cur);
        }
    }

    return true;
}

void CHThorGroupDedupAllActivity::dedupRange(unsigned first, unsigned last, OwnedRowArray & group)
{
    for (unsigned idxL = first; idxL < last; idxL++)
    {
        const void * left = group.item(idxL);
        if (left)
        {
            for (unsigned idxR = first; idxR < last; idxR++)
            {
                const void * right = group.item(idxR);
                if ((idxL != idxR) && right)
                {
                    if (helper.matches(left, right))
                    {
                        if (keepLeft)
                        {
                            group.replace(NULL, idxR);
                        }
                        else
                        {
                            group.replace(NULL, idxL);
                            break;
                        }
                    }
                }
            }
        }
    }
}

const void *CHThorGroupDedupAllActivity::nextRow()
{
    if (!firstDone)
    {
        firstDone = true;
        calcNextDedupAll();
    }

    if(survivors.isItem(survivorIndex))
    {
        processed++;
        return survivors.itemClear(survivorIndex++);
    }
    calcNextDedupAll();
    return NULL;
}

//=====================================================================================================
bool HashDedupTable::insert(const void * row)
{
    unsigned hash = helper.queryHash()->hash(row);
    RtlDynamicRowBuilder keyRowBuilder(keyRowAllocator, true);
    size32_t thisKeySize = helper.recordToKey(keyRowBuilder, row);
    OwnedConstRoxieRow keyRow = keyRowBuilder.finalizeRowClear(thisKeySize);
    if (find(hash, keyRow.get()))
        return false;
    addNew(new HashDedupElement(hash, keyRow.getClear()), hash);
    return true;
}
bool HashDedupTable::insertBest(const void * nextrow)
{
    unsigned hash = helper.queryHash()->hash(nextrow);
    const void *et = find(hash, nextrow);
    if (et)
    {
        const HashDedupElement *element = reinterpret_cast<const HashDedupElement *>(et);
        const void * row = element->queryRow();
        if (queryBestCompare->docompare(row,nextrow) <= 0)
            return false;
        removeExact( const_cast<void *>(et));
        // drop-through to add new row
    }
    LinkRoxieRow(nextrow);
    addNew(new HashDedupElement(hash, nextrow), hash);
    return true;
}

CHThorHashDedupActivity::CHThorHashDedupActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorHashDedupArg & _arg, ThorActivityKind _kind)
: CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), table(_arg, activityId), hashTableFilled(false), hashDedupTableIter(table)
{
    keepBest = helper.keepBest();
}

void CHThorHashDedupActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    table.setRowAllocator(agent.queryCodeContext()->getRowAllocator(helper.queryKeySize(), activityId));
}

void CHThorHashDedupActivity::stop()
{
    table.kill();
    CHThorSimpleActivityBase::stop();
}

const void * CHThorHashDedupActivity::nextRow()
{
    if (keepBest)
    {
        // Populate hash table with best rows
        if (!hashTableFilled)
        {
            OwnedConstRoxieRow next(input->nextRow());
            while(next)
            {
                table.insertBest(next);
                next.setown(input->nextRow());
            }
            hashTableFilled = true;
            hashDedupTableIter.first();
        }

        // Iterate through hash table returning rows
        if (hashDedupTableIter.isValid())
        {
            HashDedupElement &el = hashDedupTableIter.query();

            OwnedConstRoxieRow row(el.getRow());
            hashDedupTableIter.next();
            return row.getClear();
        }
        table.kill();
        hashTableFilled = false;
        return NULL;
    }
    else
    {
        while(true)
        {
            OwnedConstRoxieRow next(input->nextRow());
            if(!next)
            {
                table.kill();
                return NULL;
            }
            if(table.insert(next))
                return next.getClear();
        }
    }
}

//=====================================================================================================

CHThorSteppableActivityBase::CHThorSteppableActivityBase(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _help, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _help, _kind)
{
    inputStepping = NULL;
    stepCompare = NULL;
}

void CHThorSteppableActivityBase::setInput(unsigned index, IHThorInput *_input)
{
    CHThorSimpleActivityBase::setInput(index, _input);
    if (input && index == 0)
    {
        inputStepping = input->querySteppingMeta();
        if (inputStepping)
            stepCompare = inputStepping->queryCompare();
    }
}

IInputSteppingMeta * CHThorSteppableActivityBase::querySteppingMeta()
{
    return inputStepping;
}

//=====================================================================================================

CHThorFilterActivity::CHThorFilterActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorFilterArg &_arg, ThorActivityKind _kind) : CHThorSteppableActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorFilterActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    anyThisGroup = false;
    eof = !helper.canMatchAny();
}

const void * CHThorFilterActivity::nextRow()
{
    if (eof)
        return NULL;

    for (;;)
    {
        OwnedConstRoxieRow ret(input->nextRow());
        if (!ret)
        {
            //stop returning two NULLs in a row.
            if (anyThisGroup)
            {
                anyThisGroup = false;
                return NULL;
            }
            ret.setown(input->nextRow());
            if (!ret)
                return NULL;                // eof...
        }

        if (helper.isValid(ret))
        {
            anyThisGroup = true;
            processed++;
            return ret.getClear();
        }
    }
}

const void * CHThorFilterActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    if (eof)
        return NULL;

    OwnedConstRoxieRow ret(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
    if (!ret)
        return NULL;

    if (helper.isValid(ret))
    {
        anyThisGroup = true;
        processed++;
        return ret.getClear();
    }

    return ungroupedNextRow();
}

bool CHThorFilterActivity::gatherConjunctions(ISteppedConjunctionCollector & collector) 
{ 
    return input->gatherConjunctions(collector); 
}

void CHThorFilterActivity::resetEOF() 
{ 
    //Sometimes the smart stepping code returns a premature eof indicator (two nulls) and will
    //therefore call resetEOF so the activity can reset its eof without resetting the activity itself.
    //Note that resetEOF only needs to be implemented by activities that implement gatherConjunctions()
    //and that cache eof.
    eof = false;
    anyThisGroup = false;
    input->resetEOF(); 
}

//=====================================================================================================

CHThorFilterGroupActivity::CHThorFilterGroupActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorFilterGroupArg &_arg, ThorActivityKind _kind) : CHThorSteppableActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorFilterGroupActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    eof = !helper.canMatchAny();
    nextIndex = 0;
}

void CHThorFilterGroupActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    pending.clear();
}

const void * CHThorFilterGroupActivity::nextRow()
{
    for (;;)
    {
        if (eof)
            return NULL;

        if (pending.ordinality())
        {
            if (pending.isItem(nextIndex))
            {
                processed++;
                return pending.itemClear(nextIndex++);
            }
            nextIndex = 0;
            pending.clear();
            return NULL;
        }

        const void * ret = input->nextRow();
        while (ret)
        {
            pending.append(ret);
            ret = input->nextRow();
        }

        unsigned num = pending.ordinality();
        if (num != 0)
        {
            if (!helper.isValid(num, (const void * *)pending.getArray()))
                pending.clear();        // read next group
        }
        else
            eof = true;
    }
}

const void * CHThorFilterGroupActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    if (eof)
        return NULL;

    if (pending.ordinality())
    {
        while (pending.isItem(nextIndex))
        {
            OwnedConstRoxieRow ret(pending.itemClear(nextIndex++));
            if (stepCompare->docompare(ret, seek, numFields) >= 0)
            {
                processed++;
                return ret.getClear();
            }
        }
        nextIndex = 0;
        pending.clear();
    }

    const void * ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
    while (ret)
    {
        pending.append(ret);
        ret = input->nextRow();
    }

    unsigned num = pending.ordinality();
    if (num != 0)
    {
        if (!helper.isValid(num, (const void * *)pending.getArray()))
            pending.clear();        // read next group
    }
    else
        eof = true;

    return ungroupedNextRow();
}


//=====================================================================================================

CHThorLimitActivity::CHThorLimitActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &_arg, ThorActivityKind _kind) : CHThorSteppableActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorLimitActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    rowLimit = helper.getRowLimit();
    numGot = 0;
}

const void * CHThorLimitActivity::nextRow()
{
    OwnedConstRoxieRow ret(input->nextRow());
    if (ret)
    {
        if (++numGot > rowLimit)
        {
            if ( agent.queryCodeContext()->queryDebugContext())
                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            helper.onLimitExceeded();
            return NULL;
        }
        processed++;
    }
    
    return ret.getClear();
}

const void * CHThorLimitActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    OwnedConstRoxieRow ret(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
    if (ret)
    {
        if (++numGot > rowLimit)
        {
            if ( agent.queryCodeContext()->queryDebugContext())
                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            helper.onLimitExceeded();
            return NULL;
        }
        processed++;
    }
    
    return ret.getClear();
}

//=====================================================================================================

CHThorSkipLimitActivity::CHThorSkipLimitActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorSkipLimitActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    rowLimit = helper.getRowLimit();
}

void CHThorSkipLimitActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    buffer.clear();
}

const void * CHThorSkipLimitActivity::nextRow()
{
    if(!buffer)
    {
        buffer.setown(new CRowBuffer(input->queryOutputMeta(), true));
        if(!buffer->pull(input, rowLimit))
        {
            if ( agent.queryCodeContext()->queryDebugContext())
                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            onLimitExceeded();
        }
    }
    const void * next = buffer->next();
    if(next)
        processed++;
    return next;
}

//=====================================================================================================

CHThorCatchActivity::CHThorCatchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCatchArg &_arg, ThorActivityKind _kind) : CHThorSteppableActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

const void * CHThorCatchActivity::nextRow()
{
    try
    {
        OwnedConstRoxieRow ret(input->nextRow());
        if (ret)
            processed++;
        return ret.getClear();
    }
    catch (IException *E)
    {
        E->Release();
        helper.onExceptionCaught();
    }
    catch (...)
    {
        helper.onExceptionCaught();
    }
    throwUnexpected(); // onExceptionCaught should have thrown something
}

const void * CHThorCatchActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    try
    {
        OwnedConstRoxieRow ret(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
        if (ret)
            processed++;
        return ret.getClear();
    }
    catch (IException *E)
    {
        E->Release();
        helper.onExceptionCaught();
    }
    catch (...)
    {
        helper.onExceptionCaught();
    }
    throwUnexpected(); // onExceptionCaught should have thrown something
}

//=====================================================================================================

CHThorSkipCatchActivity::CHThorSkipCatchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCatchArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorSkipCatchActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    buffer.clear();
}

void CHThorSkipCatchActivity::onException(IException *E)
{
    buffer->clear();
    if (kind == TAKcreaterowcatch)
    {
        createRowAllocator();
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t newSize = helper.transformOnExceptionCaught(rowBuilder, E);
        if (newSize)
            buffer->insert(rowBuilder.finalizeRowClear(newSize));
    }
    E->Release();
}


const void * CHThorSkipCatchActivity::nextRow()
{
    if(!buffer)
    {
        buffer.setown(new CRowBuffer(input->queryOutputMeta(), true));
        try
        {
            buffer->pull(input, (unsigned __int64) -1);
        }
        catch (IException *E)
        {
            onException(E);
        }
        catch (...)
        {
            onException(MakeStringException(2, "Unknown exception caught"));
        }
    }
    const void * next = buffer->next();
    if(next)
        processed++;
    return next;
}

//=====================================================================================================

CHThorOnFailLimitActivity::CHThorOnFailLimitActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &_arg, ThorActivityKind _kind) : CHThorSkipLimitActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{
    transformExtra = static_cast<IHThorLimitTransformExtra *>(helper.selectInterface(TAIlimittransformextra_1));
    assertex(transformExtra);
}

void CHThorOnFailLimitActivity::onLimitExceeded() 
{ 
    buffer->clear(); 

    RtlDynamicRowBuilder rowBuilder(rowAllocator);
    size32_t newSize = transformExtra->transformOnLimitExceeded(rowBuilder);
    if (newSize)
        buffer->insert(rowBuilder.finalizeRowClear(newSize));
}

//=====================================================================================================

CHThorIfActivity::CHThorIfActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIfArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    inputTrue = NULL;
    inputFalse = NULL;
    selectedInput = NULL;
}


void CHThorIfActivity::stop()
{
    if (selectedInput)
        selectedInput->stop();
    CHThorSimpleActivityBase::stop();
}


void CHThorIfActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    selectedInput = helper.getCondition() ? inputTrue : inputFalse;
    if (selectedInput)
        selectedInput->ready();
}


void CHThorIfActivity::setInput(unsigned index, IHThorInput *_input)
{
    if (index==0)
        inputTrue = _input;
    else if (index == 1)
        inputFalse = _input;
    else
        CHThorActivityBase::setInput(index, _input);
}

const void * CHThorIfActivity::nextRow()
{
    if (!selectedInput)
        return NULL;

    const void *ret = selectedInput->nextRow();
    if (ret)
        processed++;
    return ret;
}

//=====================================================================================================

CHThorCaseActivity::CHThorCaseActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCaseArg &_arg, ThorActivityKind _kind) : CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorCaseActivity::ready()
{
    //Evaluate the condition here to avoid calling ready() on the unused branch?
    initialProcessed = processed;
    selectedInput = NULL;
    unsigned whichBranch = helper.getBranch();
    if (whichBranch >= inputs.ordinality())
        whichBranch = inputs.ordinality()-1;
    selectedInput = inputs.item(whichBranch);
    selectedInput->ready();
}

void CHThorCaseActivity::stop()
{
    if (selectedInput)
        selectedInput->stop();
}

const void *CHThorCaseActivity::nextRow()
{
    if (!selectedInput)
        return NULL;

    const void *ret = selectedInput->nextRow();
    if (ret)
        processed++;
    return ret;
}


//=====================================================================================================

CHThorSampleActivity::CHThorSampleActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSampleArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorSampleActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    numSamples = helper.getProportion();
    whichSample = helper.getSampleNumber();
    numToSkip = (whichSample ? whichSample-1 : 0);
    anyThisGroup = false;
}

const void * CHThorSampleActivity::nextRow()
{
    for (;;)
    {
        OwnedConstRoxieRow ret(input->nextRow());
        if (!ret)
        {
            //this does work with groups - may or may not be useful...
            //reset the sample for each group.... probably best.
            numToSkip = (whichSample ? whichSample-1 : 0);
            if (anyThisGroup)
            {
                anyThisGroup = false;
                return NULL;
            }
            ret.setown(input->nextRow());
            if (!ret)
                return NULL;                // eof...
        }

        if (numToSkip == 0)
        {
            anyThisGroup = true;
            numToSkip = numSamples-1;
            processed++;
            return ret.getClear();
        }
        numToSkip--;
    }
}

//=====================================================================================================

CHThorAggregateActivity::CHThorAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorAggregateArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorAggregateActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    eof = false;
}

const void * CHThorAggregateActivity::nextRow()
{
    if (eof)
        return NULL;
    const void * next = input->nextRow();
    if (!next && input->isGrouped())
    {
        eof = true;
        return NULL;
    }
    
    RtlDynamicRowBuilder rowBuilder(rowAllocator);
    helper.clearAggregate(rowBuilder);
    
    if (next)
    {
        helper.processFirst(rowBuilder, next);
        ReleaseRoxieRow(next);
        
        bool abortEarly = (kind == TAKexistsaggregate) && !input->isGrouped();
        if (!abortEarly)
        {
            for (;;)
            {
                next = input->nextRow();
                if (!next)
                    break;

                helper.processNext(rowBuilder, next);
                ReleaseRoxieRow(next);
            }
        }
    }
    
    if (!input->isGrouped())        // either read all, or aborted early
        eof = true;
    
    processed++;
    size32_t finalSize = outputMeta.getRecordSize(rowBuilder.getSelf());
    return rowBuilder.finalizeRowClear(finalSize);
}

//=====================================================================================================

CHThorHashAggregateActivity::CHThorHashAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorHashAggregateArg &_arg, ThorActivityKind _kind, bool _isGroupedAggregate)
: CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg),
  isGroupedAggregate(_isGroupedAggregate),
  aggregated(_arg, _arg)
{
}

void CHThorHashAggregateActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    eof = false;
    gathered = false;
}

void CHThorHashAggregateActivity::stop()
{
    aggregated.reset();
    CHThorSimpleActivityBase::stop();
}


const void * CHThorHashAggregateActivity::nextRow()
{
    if (eof)
        return NULL;

    if (!gathered)
    {
        bool eog = true;
        aggregated.start(rowAllocator);
        for (;;)
        {
            OwnedConstRoxieRow next(input->nextRow());
            if (!next)
            {
                if (isGroupedAggregate)
                {
                    if (eog)
                        eof = true;
                    break;
                }
                next.setown(input->nextRow());
                if (!next)
                    break;
            }
            eog = false;
            try
            {
                aggregated.addRow(next);
            }
            catch(IException * e)
            {
                throw makeWrappedException(e);
            }
        }
        gathered = true;
    }

    Owned<AggregateRowBuilder> next = aggregated.nextResult();
    if (next)
    {
        processed++;
        return next->finalizeRowClear();
    }

    if (!isGroupedAggregate)
        eof = true;

    aggregated.reset();
    gathered = false;
    return NULL;
}

//=====================================================================================================

CHThorSelectNActivity::CHThorSelectNActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSelectNArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

const void * CHThorSelectNActivity::defaultRow()
{
    if (!rowAllocator)                  
        createRowAllocator();            //We delay as often not needed...
    RtlDynamicRowBuilder rowBuilder(rowAllocator);
    size32_t thisSize = helper.createDefault(rowBuilder);
    return rowBuilder.finalizeRowClear(thisSize);
}

void CHThorSelectNActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    finished = false;
}

const void * CHThorSelectNActivity::nextRow()
{
    if (finished)
        return NULL;

    finished = true;
    unsigned __int64 index = helper.getRowToSelect();
    while (--index)
    {
        OwnedConstRoxieRow next(input->nextRow());
        if (!next)
            next.setown(input->nextRow());
        if (!next)
        {
            processed++;
            return defaultRow();
        }
    }

    OwnedConstRoxieRow next(input->nextRow());
    if (!next)
        next.setown(input->nextRow());
    if (!next)
        next.setown(defaultRow());

    processed++;
    return next.getClear();
}

//=====================================================================================================

CHThorFirstNActivity::CHThorFirstNActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorFirstNArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    grouped = outputMeta.isGrouped();
}

void CHThorFirstNActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    skip = helper.numToSkip();
    limit = helper.getLimit();
    doneThisGroup = 0;
    finished = (limit == 0);
    if (limit + skip >= limit)
        limit += skip;
}

const void * CHThorFirstNActivity::nextRow()
{
    if (finished)
        return NULL;

    OwnedConstRoxieRow ret;
    for (;;)
    {
        ret.setown(input->nextRow());
        if (!ret)
        {
            if (grouped)
            {
                if (doneThisGroup > skip)
                {
                    doneThisGroup = 0;
                    return NULL;
                }
                doneThisGroup = 0;
            }

            ret.setown(input->nextRow());
            if (!ret)
            {
                finished = true;
                return NULL;
            }

        }
        doneThisGroup++;
        if (doneThisGroup > skip)
            break;
    }

    if (doneThisGroup <= limit)
    {
        processed++;
        return ret.getClear();
    }

    if (grouped)
    {
        ret.setown(input->nextRow());
        while (ret)
            ret.setown(input->nextRow());
        doneThisGroup = 0;
    }
    else
        finished = true;
    return NULL;
}

//=====================================================================================================

CHThorChooseSetsActivity::CHThorChooseSetsActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    numSets = helper.getNumSets();
    setCounts = new unsigned[numSets];
}

CHThorChooseSetsActivity::~CHThorChooseSetsActivity()
{
    delete [] setCounts;
}

void CHThorChooseSetsActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    finished = false;
    memset(setCounts, 0, sizeof(unsigned)*numSets);
    helper.setCounts(setCounts);
}

const void * CHThorChooseSetsActivity::nextRow()
{
    if (finished)
        return NULL;

    for (;;)
    {
        OwnedConstRoxieRow ret(input->nextRow());
        if (!ret)
        {
            ret.setown(input->nextRow());
            if (!ret)
                return NULL;
        }
        processed++;
        switch (helper.getRecordAction(ret))
        {
        case 2:
            finished = true;
            return ret.getClear();
        case 1:
            return ret.getClear();
        }
    }
}

//=====================================================================================================

CHThorChooseSetsExActivity::CHThorChooseSetsExActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsExArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    numSets = helper.getNumSets();
    setCounts = new unsigned[numSets];
    memset(setCounts, 0, sizeof(unsigned)*numSets);
    limits = (count_t *)checked_calloc(sizeof(count_t), numSets, "choose sets ex");
    helper.getLimits(limits);
}

CHThorChooseSetsExActivity::~CHThorChooseSetsExActivity()
{
    delete [] setCounts;
    free(limits);
}

void CHThorChooseSetsExActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    finished = false;
    curIndex = 0;
    memset(setCounts, 0, sizeof(unsigned)*numSets);
}

void CHThorChooseSetsExActivity::stop()
{
    gathered.clear();
    CHThorSimpleActivityBase::stop();
}

const void * CHThorChooseSetsExActivity::nextRow()
{
    if (gathered.ordinality() == 0)
    {
        curIndex = 0;
        const void * next = input->nextRow();
        while(next)
        {
            gathered.append(next);
            next = input->nextRow();
        }
        if(gathered.ordinality() == 0)
        {
            finished = true;
            return NULL;
        }

        ForEachItemIn(idx1, gathered)
        {
            unsigned category = helper.getCategory(gathered.item(idx1));
            if (category)
                setCounts[category-1]++;
        }
        calculateSelection();
    }

    while (gathered.isItem(curIndex))
    {
        OwnedConstRoxieRow row(gathered.itemClear(curIndex++));
        if (includeRow(row))
        {
            processed++;
            return row.getClear();
        }
    }

    gathered.clear();
    return NULL;
}


//=====================================================================================================

CHThorChooseSetsLastActivity::CHThorChooseSetsLastActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsExArg &_arg, ThorActivityKind _kind) : CHThorChooseSetsExActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{ 
    numToSkip = (unsigned *)checked_calloc(sizeof(unsigned), numSets, "choose sets last");
}

CHThorChooseSetsLastActivity::~CHThorChooseSetsLastActivity() 
{ 
    free(numToSkip); 
}

void CHThorChooseSetsLastActivity::ready()
{
    CHThorChooseSetsExActivity::ready();
    memset(numToSkip, 0, sizeof(unsigned) * numSets);
}

void CHThorChooseSetsLastActivity::calculateSelection()
{
    for (unsigned idx=0; idx < numSets; idx++)
    {
        if (setCounts[idx] < limits[idx])
            numToSkip[idx] = 0;
        else
            numToSkip[idx] = (unsigned)(setCounts[idx] - limits[idx]);
    }
}


bool CHThorChooseSetsLastActivity::includeRow(const void * row)
{
    unsigned category = helper.getCategory(row);
    if (category)
    {
        if (numToSkip[category-1] == 0)
            return true;
        numToSkip[category-1]--;
    }
    return false;       
}


//=====================================================================================================

CHThorChooseSetsEnthActivity::CHThorChooseSetsEnthActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsExArg &_arg, ThorActivityKind _kind) : CHThorChooseSetsExActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{ 
    counter = (unsigned __int64 *)checked_calloc(sizeof(unsigned __int64), numSets, "choose sets enth");
}

CHThorChooseSetsEnthActivity::~CHThorChooseSetsEnthActivity() 
{ 
    free(counter); 
}

void CHThorChooseSetsEnthActivity::ready()
{
    CHThorChooseSetsExActivity::ready();
    memset(counter, 0, sizeof(unsigned __int64) * numSets);
}

void CHThorChooseSetsEnthActivity::calculateSelection()
{
}


bool CHThorChooseSetsEnthActivity::includeRow(const void * row)
{
    unsigned category = helper.getCategory(row);
    if (category)
    {
        counter[category-1] += limits[category-1];
        if(counter[category-1] >= setCounts[category-1])
        {
            counter[category-1] -= setCounts[category-1];
            return true;
        }       
    }
    return false;       
}


//=====================================================================================================

CHThorDegroupActivity::CHThorDegroupActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDegroupArg &_arg, ThorActivityKind _kind) : CHThorSteppableActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

const void * CHThorDegroupActivity::nextRow()
{
    const void * ret = input->ungroupedNextRow();
    if (ret)
        processed++;
    return ret;
}

const void * CHThorDegroupActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    const void * ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
    if (ret)
        processed++;
    return ret;
}


bool CHThorDegroupActivity::isGrouped()
{
    return false;
}

//=====================================================================================================

CHThorGroupActivity::CHThorGroupActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorGroupArg &_arg, ThorActivityKind _kind) : CHThorSteppableActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

bool CHThorGroupActivity::isGrouped()
{
    return true;
}

void CHThorGroupActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    next.clear();
    endPending = false;
    firstDone = false;
}

void CHThorGroupActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    next.clear();
}

const void *CHThorGroupActivity::nextRow()
{
    if (!firstDone)
    {
        firstDone = true;
        next.setown(input->nextRow());
    }

    if (endPending)
    {
        endPending = false;
        return NULL;
    }

    OwnedConstRoxieRow prev(next.getClear());
    next.setown(input->nextRow());
    if (!next)  // skip incoming groups. (should it sub-group??)
        next.setown(input->nextRow());

    if (next)
    {
        assertex(prev);  // If this fails, you have an initial empty group. That is not legal.
        if (!helper.isSameGroup(prev, next))
            endPending = true;
    }
    if (prev)
        processed++;
    return prev.getClear();
}

const void * CHThorGroupActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    if (firstDone)
    {
        if (next)
        {
            if (stepCompare->docompare(next, seek, numFields) >= 0)
                return nextRow();
        }
    }
    next.setown(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
    firstDone = true;
    return nextRow();
}

//=====================================================================================================

CHThorGroupSortActivity::CHThorGroupSortActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSortArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    gotSorted = false;
}

void CHThorGroupSortActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    if(!sorter)
        createSorter();
}

void CHThorGroupSortActivity::stop()
{
    if(sorter)
    {
        if(sorterIsConst)
            sorter->killSorted();
        else
            sorter.clear();
    }
    gotSorted = false;
    diskReader.clear();
    CHThorSimpleActivityBase::stop();
}

const void *CHThorGroupSortActivity::nextRow()
{
    if(!gotSorted)
        getSorted();

    if(diskReader)
    {
        const void *row = diskReader->nextRow();
        if (row)
            return row;
        diskReader.clear();
    }
    else
    {
        const void * ret = sorter->getNextSorted();
        if(ret)
        {
            processed++;
            return ret;
        }
    }
    sorter->killSorted();
    gotSorted = false;
    return NULL;
}

void CHThorGroupSortActivity::createSorter()
{
    IHThorAlgorithm * algo = static_cast<IHThorAlgorithm *>(helper.selectInterface(TAIalgorithm_1));
    if(!algo)
    {
        sorter.setown(new CHeapSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
        return;
    }
    unsigned flags = algo->getAlgorithmFlags();
    sorterIsConst = ((flags & TAFconstant) != 0);
    OwnedRoxieString algoname(algo->getAlgorithm());
    if(!algoname)
    {
        if((flags & TAFunstable) != 0)
            sorter.setown(new CQuickSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
        else
            sorter.setown(new CHeapSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
        return;
    }
    if(stricmp(algoname, "quicksort") == 0)
    {
        if((flags & TAFstable) != 0)
            sorter.setown(new CStableQuickSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep, this));
        else
            sorter.setown(new CQuickSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
    }
    else if(stricmp(algoname, "parquicksort") == 0)
    {
        if((flags & TAFstable) != 0)
            sorter.setown(new CParallelStableQuickSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep, this));
        else
            sorter.setown(new CParallelQuickSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
    }
    else if(stricmp(algoname, "mergesort") == 0)
    {
        if((flags & TAFparallel) != 0)
            sorter.setown(new CParallelStableMergeSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep, this));
        else
            sorter.setown(new CStableMergeSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep, this));
    }
    else if(stricmp(algoname, "parmergesort") == 0)
        sorter.setown(new CParallelStableMergeSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep, this));
    else if(stricmp(algoname, "heapsort") == 0)
        sorter.setown(new CHeapSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
    else if(stricmp(algoname, "insertionsort") == 0)
    {
        if((flags & TAFstable) != 0)
            sorter.setown(new CStableInsertionSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
        else
            sorter.setown(new CInsertionSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
    }
    else
    {
        StringBuffer sb;
        sb.appendf("Ignoring unsupported sort order algorithm '%s', using default", algoname.get());
        agent.addWuException(sb.str(),WRN_UnsupportedAlgorithm,SeverityWarning,"hthor");
        if((flags & TAFunstable) != 0)
            sorter.setown(new CQuickSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
        else
            sorter.setown(new CHeapSorter(helper.queryCompare(), queryRowManager(), InitialSortElements, CommitStep));
    }
    sorter->setActivityId(activityId);
}

void CHThorGroupSortActivity::getSorted()
{
    diskMerger.clear();
    diskReader.clear();
    queryRowManager()->addRowBuffer(this);//register for OOM callbacks
    const void * next;
    while((next = input->nextRow()) != NULL)
    {
        if (!sorter->addRow(next))
        {
            {
                //Unlikely that this code will ever be executed but added for comfort
                roxiemem::RoxieOutputRowArrayLock block(sorter->getRowArray());
                sorter->flushRows();
                sortAndSpillRows();
                //Ensure new rows are written to the head of the array.  It needs to be a separate call because
                //performSort() cannot shift active row pointer since it can be called from any thread
                sorter->flushRows();
            }
            if (!sorter->addRow(next))
            {
                ReleaseRoxieRow(next);
                throw MakeStringException(0, "Insufficient memory to append sort row");
            }
        }
    }
    queryRowManager()->removeRowBuffer(this);//unregister for OOM callbacks
    sorter->flushRows();

    if(diskMerger)
    {
        sortAndSpillRows();
        sorter->killSorted();
        ICompare *compare = helper.queryCompare();
        diskReader.setown(diskMerger->merge(compare));
    }
    else
    {
        sorter->performSort();
    }
    gotSorted = true;
}

//interface roxiemem::IBufferedRowCallback
unsigned CHThorGroupSortActivity::getSpillCost() const
{
    return 10;
}


unsigned CHThorGroupSortActivity::getActivityId() const
{
    return activityId;
}

bool CHThorGroupSortActivity::freeBufferedRows(bool critical)
{
    roxiemem::RoxieOutputRowArrayLock block(sorter->getRowArray());
    return sortAndSpillRows();
}



bool CHThorGroupSortActivity::sortAndSpillRows()
{
    if (0 == sorter->numCommitted())
        return false;
    if(!diskMerger)
    {
        StringBuffer fbase;
        agent.getTempfileBase(fbase).appendf(".spill_sort_%p", this);
        PROGLOG("SORT: spilling to disk, filename base %s", fbase.str());
        class CHThorRowLinkCounter : implements IRowLinkCounter, public CSimpleInterface
        {
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
            virtual void releaseRow(const void *row)
            {
                ReleaseRoxieRow(row);
            }
            virtual void linkRow(const void *row)
            {
                LinkRoxieRow(row);
            }
        };
        Owned<IRowLinkCounter> linker = new CHThorRowLinkCounter();
        Owned<IRowInterfaces> rowInterfaces = createRowInterfaces(input->queryOutputMeta(), activityId, 0, agent.queryCodeContext());
        diskMerger.setown(createDiskMerger(rowInterfaces, linker, fbase.str()));
    }
    sorter->performSort();
    sorter->spillSortedToDisk(diskMerger);
    return true;
}

// Base for Quick sort and both Insertion sorts

void CSimpleSorterBase::spillSortedToDisk(IDiskMerger * merger)
{
    Owned<IRowWriter> out = merger->createWriteBlock();
    for (;;)
    {
        const void *row = getNextSorted();
        if (!row)
            break;
        out->putRow(row);
    }
    finger = 0;
    out->flush();
    rowsToSort.noteSpilled(rowsToSort.numCommitted());
}

// Quick sort

void CQuickSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        qsortvec((void * *)rows, numRows, *compare);
        finger = 0;
    }
}

// Quick sort

void CParallelQuickSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        parqsortvec((void * *)rows, numRows, *compare);
        finger = 0;
    }
}

// StableQuick sort

bool CStableSorter::addRow(const void * next)
{
    roxiemem::rowidx_t nextRowCapacity = rowsToSort.rowCapacity() + 1;//increment capacity for the row we are about to add
    if (nextRowCapacity > indexCapacity)
    {
        void *** newIndex = (void ***)rowManager->allocate(nextRowCapacity * sizeof(void*), activityId);//could force an OOM callback
        if (newIndex)
        {
            roxiemem::RoxieOutputRowArrayLock block(getRowArray());//could force an OOM callback after index is freed but before index,indexCapacity is updated
            ReleaseRoxieRow(index);
            index = newIndex;
            indexCapacity = RoxieRowCapacity(index) / sizeof(void*);
        }
        else
        {
            killSorted();
            ReleaseRoxieRow(next);
            throw MakeStringException(0, "Insufficient memory to allocate StableQuickSorter index");
        }
    }
    return CSimpleSorterBase::addRow(next);
}

void CStableSorter::spillSortedToDisk(IDiskMerger * merger)
{
    CSimpleSorterBase::spillSortedToDisk(merger);
    ReleaseRoxieRow(index);
    index = NULL;
    indexCapacity = 0;
}

void CStableSorter::killSorted()
{
    CSimpleSorterBase::killSorted();
    ReleaseRoxieRow(index);
    index = NULL;
    indexCapacity = 0;
}

// StableQuick sort

void CStableQuickSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        qsortvecstableinplace((void * *)rows, numRows, *compare, (void * *)index);
        finger = 0;
    }
}

void CParallelStableQuickSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        parqsortvecstableinplace((void * *)rows, numRows, *compare, (void * *)index);
        finger = 0;
    }
}

// StableMerge sort

void CStableMergeSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        msortvecstableinplace((void * *)rows, numRows, *compare, (void * *)index);
        finger = 0;
    }
}

void CParallelStableMergeSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        parmsortvecstableinplace((void * *)rows, numRows, *compare, (void * *)index);
        finger = 0;
    }
}

// Heap sort

void CHeapSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        heapsize = numRows;
        for (unsigned i = 0; i < numRows; i++)
        {
            heap.append(i);
            heap_push_up(i, heap.getArray(), rows, compare);
        }
    }
}

void CHeapSorter::spillSortedToDisk(IDiskMerger * merger)
{
    CSimpleSorterBase::spillSortedToDisk(merger);
    heap.kill();
    heapsize = 0;
}

const void * CHeapSorter::getNextSorted()
{
    if(heapsize)
    {
        size32_t numRows = rowsToSort.numCommitted();
        if (numRows)
        {
            const void * * rows = rowsToSort.getBlock(numRows);
            unsigned top = heap.item(0);
            --heapsize;
            heap.replace(heap.item(heapsize), 0);
            heap_push_down(0, heapsize, heap.getArray(), rows, compare);
            const void * row = rows[top];
            rows[top] = NULL;
            return row;
        }
    }
    return NULL;
}

void CHeapSorter::killSorted()
{
    CSimpleSorterBase::killSorted();
    heap.kill();
    heapsize = 0;
}

// Insertion sorts

void CInsertionSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        for (unsigned i = 0; i < numRows; i++)
        {
            binary_vec_insert(rowsToSort.query(i), rows, i, *compare);
        }
        finger = 0;
    }
}

void CStableInsertionSorter::performSort()
{
    size32_t numRows = rowsToSort.numCommitted();
    if (numRows)
    {
        const void * * rows = rowsToSort.getBlock(numRows);
        for (unsigned i = 0; i < numRows; i++)
        {
            binary_vec_insert_stable(rowsToSort.query(i), rows, i, *compare);
        }
        finger = 0;
    }
}

//=====================================================================================================

CHThorGroupedActivity::CHThorGroupedActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorGroupedArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorGroupedActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    firstDone = false;
    nextRowIndex = 0;
}

void CHThorGroupedActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    next[0].clear();
    next[1].clear();
    next[2].clear();
}

const void *CHThorGroupedActivity::nextRow()
{
    if (!firstDone)
    {
        next[0].setown(input->nextRow());
        next[1].setown(input->nextRow());
        nextRowIndex = 0;
    }

    unsigned nextToCompare = (nextRowIndex + 1) % 3;
    unsigned nextToFill  = (nextRowIndex + 2) % 3;
    next[nextToFill].setown(input->nextRow());

    OwnedConstRoxieRow ret(next[nextRowIndex].getClear());
    if (ret)
    {
        if (next[nextToCompare]) 
        {
            if (!helper.isSameGroup(ret, next[nextToCompare]))
                throw MakeStringException(100, "GROUPED(%u), expected a group break between adjacent rows (rows %" I64F "d, %" I64F "d) ", activityId, processed+1, processed+2);
        }
        else if (next[nextToFill])
        {
            if (helper.isSameGroup(ret, next[nextToFill]))
                throw MakeStringException(100, "GROUPED(%u), unexpected group break found between rows %" I64F "d and %" I64F "d)", activityId, processed+1, processed+2);
        }
        processed++;
    }
    nextRowIndex = nextToCompare;
    return ret.getClear();
}

//=====================================================================================================

CHThorSortedActivity::CHThorSortedActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSortedArg &_arg, ThorActivityKind _kind) : CHThorSteppableActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    //MORE: Should probably have a inter group and intra group sort functions
    compare = helper.queryCompare();
}

void CHThorSortedActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    firstDone = false;
}

void CHThorSortedActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    next.clear();
}

const void *CHThorSortedActivity::nextRow()
{
    if (!firstDone)
    {
        firstDone = true;
        next.setown(input->nextRow());
    }

    OwnedConstRoxieRow prev(next.getClear());
    next.setown(input->nextRow());
    if (prev && next)
        if (compare->docompare(prev, next) > 0)
            throw MakeStringException(100, "SORTED(%u) detected incorrectly sorted rows  (row %" I64F "d,  %" I64F "d))", activityId, processed+1, processed+2);
    if (prev)
        processed++;
    return prev.getClear();
}

const void * CHThorSortedActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    if (next)
    {
        if (stepCompare->docompare(next, seek, numFields) >= 0)
            return nextRow();
    }

    firstDone = true;
    next.setown(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
    return nextRow();
}


//=====================================================================================================

CHThorTraceActivity::CHThorTraceActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorTraceArg &_arg, ThorActivityKind _kind)
: CHThorSteppableActivityBase(_agent, _activityId, _subgraphId, _arg, _kind),
  helper(_arg),  keepLimit(0), skip(0), sample(0), traceEnabled(false)
{
}

void CHThorTraceActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    traceEnabled = agent.queryWorkUnit()->getDebugValueBool("traceEnabled", false);
    if (traceEnabled && helper.canMatchAny())
    {
        keepLimit = helper.getKeepLimit();
        if (keepLimit==(unsigned) -1)
            keepLimit = agent.queryWorkUnit()->getDebugValueInt("traceLimit", 10);
        skip = helper.getSkip();
        sample = helper.getSample();
        if (sample)
            sample--;
        name.setown(helper.getName());
        if (!name)
            name.set("Row");
    }
    else
        keepLimit = 0;
}

void CHThorTraceActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    name.clear();
}

const void *CHThorTraceActivity::nextRow()
{
    OwnedConstRoxieRow ret(input->nextRow());
    if (!ret)
        return NULL;
    onTrace(ret);
    processed++;
    return ret.getClear();
}

const void * CHThorTraceActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    OwnedConstRoxieRow ret(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
    if (ret)
    {
        onTrace(ret);
        processed++;
    }
    return ret.getClear();
}

void CHThorTraceActivity::onTrace(const void *row)
{
    if (keepLimit && helper.isValid(row))
    {
        if (skip)
            skip--;
        else if (sample)
            sample--;
        else
        {
            CommonXmlWriter xmlwrite(XWFnoindent);
            outputMeta.toXML((const byte *) row, xmlwrite);
            DBGLOG("TRACE: <%s>%s<%s>", name.get(), xmlwrite.str(), name.get());
            keepLimit--;
            sample = helper.getSample();
            if (sample)
                sample--;
        }
    }
}

//=====================================================================================================

void getLimitType(unsigned flags, bool & limitFail, bool & limitOnFail)
{
    if((flags & JFmatchAbortLimitSkips) != 0)
    {
        limitFail = false;
        limitOnFail = false;
    }
    else
    {
        limitOnFail = ((flags & JFonfail) != 0);
        limitFail = !limitOnFail;
    }
}

CHThorJoinActivity::CHThorJoinActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorJoinArg &_arg, ThorActivityKind _kind) 
                : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), outBuilder(NULL)
{
}

void CHThorJoinActivity::ready()
{
    CHThorActivityBase::ready();
    input1->ready();
    bool isStable = (helper.getJoinFlags() & JFunstable) == 0;
    RoxieSortAlgorithm sortAlgorithm = isStable ? stableSpillingQuickSortAlgorithm : spillingQuickSortAlgorithm;
    StringBuffer tempBase;
    agent.getTempfileBase(tempBase);
    if (helper.isLeftAlreadySorted())
        sortedLeftInput.setown(createDegroupedInputReader(&input->queryStream()));
    else
        sortedLeftInput.setown(createSortedInputReader(&input->queryStream(), createSortAlgorithm(sortAlgorithm, helper.queryCompareLeft(), *queryRowManager(), input->queryOutputMeta(), agent.queryCodeContext(), tempBase, activityId)));
    ICompare *compareRight = helper.queryCompareRight();
    if (helper.isRightAlreadySorted())
        groupedSortedRightInput.setown(createGroupedInputReader(&input1->queryStream(), compareRight));
    else
        groupedSortedRightInput.setown(createSortedGroupedInputReader(&input1->queryStream(), compareRight, createSortAlgorithm(sortAlgorithm, compareRight, *queryRowManager(), input1->queryOutputMeta(), agent.queryCodeContext(), tempBase, activityId)));
    outBuilder.setAllocator(rowAllocator);
    leftOuterJoin = (helper.getJoinFlags() & JFleftouter) != 0;
    rightOuterJoin = (helper.getJoinFlags() & JFrightouter) != 0;
    exclude = (helper.getJoinFlags() & JFexclude) != 0;
    getLimitType(helper.getJoinFlags(), limitFail, limitOnFail);
    if (rightOuterJoin && !defaultLeft)
        createDefaultLeft();    
    if ((leftOuterJoin || limitOnFail) && !defaultRight)
        createDefaultRight();   
    betweenjoin = ((helper.getJoinFlags() & JFslidingmatch) != 0);
    assertex(!(betweenjoin && rightOuterJoin));

    keepLimit = helper.getKeepLimit();
    if (keepLimit == 0)
        keepLimit = (unsigned)-1;
    atmostLimit = helper.getJoinLimit();
    if(atmostLimit == 0)
        atmostLimit = (unsigned)-1;
    else
        assertex(!rightOuterJoin && !betweenjoin);
    abortLimit = helper.getMatchAbortLimit();
    if (abortLimit == 0) 
        abortLimit = (unsigned)-1;

    assertex((helper.getJoinFlags() & (JFfirst | JFfirstleft | JFfirstright)) == 0); // no longer supported

    if(betweenjoin)
    {
        collate = helper.queryCompareLeftRightLower();
        collateupper = helper.queryCompareLeftRightUpper();
    }
    else
    {
        collate = collateupper = helper.queryCompareLeftRight();
    }

    rightIndex = 0;
    joinCounter = 0;
    failingLimit.clear();
    state = JSfill;
    if ((helper.getJoinFlags() & JFlimitedprefixjoin) && helper.getJoinLimit()) 
    {   //Limited Match Join (s[1..n])
        limitedhelper.setown(createRHLimitedCompareHelper());
        limitedhelper->init( helper.getJoinLimit(), groupedSortedRightInput, collate, helper.queryPrefixCompare() );
    }
}

void CHThorJoinActivity::stop()
{
    outBuilder.clear();
    right.clear();
    left.clear();
    pendingRight.clear();
    sortedLeftInput.clear();
    groupedSortedRightInput.clear();
    CHThorActivityBase::stop();
    input1->stop();
}

void CHThorJoinActivity::setInput(unsigned index, IHThorInput *_input)
{
    if (index==1)
        input1 = _input;
    else
        CHThorActivityBase::setInput(index, _input);
}

void CHThorJoinActivity::createDefaultLeft()
{
    if (!defaultLeft)
    {
        if (!defaultLeftAllocator)
            defaultLeftAllocator.setown(agent.queryCodeContext()->getRowAllocator(input->queryOutputMeta(), activityId));

        RtlDynamicRowBuilder rowBuilder(defaultLeftAllocator);
        size32_t thisSize = helper.createDefaultLeft(rowBuilder);
        defaultLeft.setown(rowBuilder.finalizeRowClear(thisSize));
    }
}

void CHThorJoinActivity::createDefaultRight()
{
    if (!defaultRight)
    {
        if (!defaultRightAllocator)
            defaultRightAllocator.setown(agent.queryCodeContext()->getRowAllocator(input1->queryOutputMeta(), activityId));

        RtlDynamicRowBuilder rowBuilder(defaultRightAllocator);
        size32_t thisSize = helper.createDefaultRight(rowBuilder);
        defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
    }
}


void CHThorJoinActivity::fillLeft()
{
    matchedLeft = false;
    left.setown(sortedLeftInput->nextRow()); // NOTE: already degrouped
    if(betweenjoin && left && pendingRight && (collate->docompare(left, pendingRight) >= 0))
        fillRight();
    if (limitedhelper && 0==rightIndex)
    {
        rightIndex = 0;
        joinCounter = 0;
        right.clear();
        matchedRight.kill();
        if (left)
        {
            limitedhelper->getGroup(right,left);
            ForEachItemIn(idx, right)
                matchedRight.append(false);
        }
    }
}

void CHThorJoinActivity::fillRight()
{
    if (limitedhelper)
        return;
    failingLimit.clear();
    if(betweenjoin && left)
    {
        aindex_t start = 0;
        while(right.isItem(start) && (collateupper->docompare(left, right.item(start)) > 0))
            start++;
        if(start>0)
            right.clearPart(0, start);
    }
    else
        right.clear();
    rightIndex = 0;
    joinCounter = 0;
    unsigned groupCount = 0;
    while(true)
    {
        OwnedConstRoxieRow next;
        if(pendingRight)
        {
            next.setown(pendingRight.getClear());
        }
        else
        {
            next.setown(groupedSortedRightInput->nextRow());
        }
        if(!rightOuterJoin && next && (!left || (collateupper->docompare(left, next) > 0))) // if right is less than left, and not right outer, can skip group
        {
            while(next) 
                next.setown(groupedSortedRightInput->nextRow());
            continue;
        }
        while(next)
        {
            if(groupCount==abortLimit)
            {
                if(limitFail)
                    failLimit();
                if ( agent.queryCodeContext()->queryDebugContext())
                    agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                if(limitOnFail)
                {
                    assertex(!failingLimit);
                    try
                    {
                        failLimit();
                    }
                    catch(IException * except)
                    {
                        failingLimit.setown(except);
                    }
                    assertex(failingLimit);
                }
                right.append(next.getClear());
                do
                {
                    next.setown(groupedSortedRightInput->nextRow());
                } while(next);
                break;
            }
            else if(groupCount==atmostLimit)
            {
                right.clear();
                groupCount = 0;
                while(next) 
                {
                    next.setown(groupedSortedRightInput->nextRow());
                }
            }
            else
            {
                right.append(next.getClear());
                groupCount++;
            }
            next.setown(groupedSortedRightInput->nextRow());
            
        }
        // normally only want to read one right group, but if is between join and next right group is in window for left, need to continue
        if(betweenjoin && left)
        {
            pendingRight.setown(groupedSortedRightInput->nextRow());
            if(!pendingRight || (collate->docompare(left, pendingRight) < 0))
                break;
        }
        else
            break;
    }

    matchedRight.kill();
    ForEachItemIn(idx, right)
        matchedRight.append(false);
}

const void * CHThorJoinActivity::joinRecords(const void * curLeft, const void * curRight, unsigned counter, unsigned flags)
{
    try
    {
        outBuilder.ensureRow();
        size32_t thisSize = helper.transform(outBuilder, curLeft, curRight, counter, flags);
        if(thisSize)
            return outBuilder.finalizeRowClear(thisSize);
        else
            return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

const void * CHThorJoinActivity::groupDenormalizeRecords(const void * curLeft, ConstPointerArray & rows, unsigned flags)
{
    try
    {
        outBuilder.ensureRow();
        unsigned numRows = rows.ordinality();
        const void * rhs = numRows ? rows.item(0) : defaultRight.get();
        if (numRows>0)
            flags |= JTFmatchedright;
        memsize_t thisSize = helper.transform(outBuilder, curLeft, rhs, numRows, (const void * *)rows.getArray(), flags);
        if(thisSize)
            return outBuilder.finalizeRowClear(thisSize);
        else
            return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

const void * CHThorJoinActivity::joinException(const void * curLeft, IException * except)
{
    try
    {
        outBuilder.ensureRow();
        size32_t thisSize = helper.onFailTransform(outBuilder, curLeft, defaultRight, except, JTFmatchedleft);
        if(thisSize)
            return outBuilder.finalizeRowClear(thisSize);
        else
            return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

void CHThorJoinActivity::failLimit()
{
    helper.onMatchAbortLimitExceeded();
    CommonXmlWriter xmlwrite(0);
    if (input->queryOutputMeta() && input->queryOutputMeta()->hasXML())
    {
        input->queryOutputMeta()->toXML((byte *)left.get(), xmlwrite);
    }
    throw MakeStringException(0, "More than %d match candidates in join for row %s", abortLimit, xmlwrite.str());
}

const void *CHThorJoinActivity::nextRow()
{
    for (;;)
    {
        switch (state)
        {
        case JSfill:
            fillLeft();
            state = JSfillright;
            break;

        case JSfillright:
            fillRight();
            state = JScollate;
            break;

        case JSfillleft:
            fillLeft();
            state = JScollate;
            break;

        case JScollate:
            if (right.ordinality() == 0)
            {
                if (!left)
                    return NULL;
                state = JSleftonly;
            }
            else
            {
                if (!left)
                    state = JSrightonly;
                else
                {
                    int diff;
                    if(betweenjoin)
                        diff = ((collate->docompare(left, right.item(0)) < 0) ? -1 : ((collateupper->docompare(left, right.item(right.ordinality()-1)) > 0) ? +1 : 0));
                    else
                        diff = collate->docompare(left, right.item(0));
                    bool limitExceeded =  right.ordinality()>abortLimit;
                    if (diff == 0)
                    {
                        if (limitExceeded)
                        {
                            const void * ret = NULL;
                            if(failingLimit)
                            {
                                if ( agent.queryCodeContext()->queryDebugContext())
                                    agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                                ret = joinException(left, failingLimit);
                            }
                            left.clear();
                            state = JSfillleft;
                            ForEachItemIn(idx, right)
                                matchedRight.replace(true, idx);
                            if(ret)
                            {
                                processed++;
                                return ret;
                            }
                        }
                        else
                        {
                            state = JScompare;
                            joinLimit = keepLimit;
                        }
                    }
                    else if (diff < 0)
                        state = JSleftonly;
                    else if (limitExceeded)
                    {
                        // MORE - Roxie code seems to think there should be a destroyRowset(right) here....
                        state = JSfillright;
                    }
                    else
                        state = JSrightonly;
                }
            }
            break;

        case JSrightonly:
            if (rightOuterJoin)
            {
                switch (kind)
                {
                case TAKjoin:
                    {
                        while (right.isItem(rightIndex))
                        {
                            if (!matchedRight.item(rightIndex))
                            {
                                const void * rhs = right.item(rightIndex++);
                                const void * ret = joinRecords(defaultLeft, rhs, 0, JTFmatchedright);
                                if (ret)
                                {
                                    processed++;
                                    return ret;
                                }
                            }
                            else
                                rightIndex++;
                        }
                        break;
                    }
                //Probably excessive to implement the following, but possibly useful
                case TAKdenormalize:
                    {
                        OwnedConstRoxieRow newLeft(defaultLeft.getLink());
                        unsigned rowSize = 0;
                        unsigned leftCount = 0;
                        while (right.isItem(rightIndex))
                        {
                            if (!matchedRight.item(rightIndex))
                            {
                                const void * rhs = right.item(rightIndex);
                                try
                                {
                                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                                    size32_t thisSize = helper.transform(rowBuilder, newLeft, rhs, ++leftCount, JTFmatchedright);
                                    if (thisSize)
                                    {
                                        rowSize = thisSize;
                                        newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                                    }
                                }
                                catch(IException * e)
                                {
                                    throw makeWrappedException(e);
                                }
                            }
                            rightIndex++;
                        }
                        state = JSfillright;
                        if (rowSize)
                        {
                            processed++;
                            return newLeft.getClear();
                        }
                        break;
                    }
                case TAKdenormalizegroup:
                    {
                        filteredRight.kill();
                        while (right.isItem(rightIndex))
                        {
                            if (!matchedRight.item(rightIndex))
                                filteredRight.append(right.item(rightIndex));
                            rightIndex++;
                        }
                        state = JSfillright;
                        if (filteredRight.ordinality())
                        {
                            const void * ret = groupDenormalizeRecords(defaultLeft, filteredRight, 0);
                            filteredRight.kill();

                            if (ret)
                            {
                                processed++;
                                return ret;
                            }
                        }
                        break;
                    }
                default:
                    throwUnexpected();
                }
            }
            state = JSfillright;
            break;
            
        case JSleftonly:
        {
            const void * ret = NULL;
            if (!matchedLeft && leftOuterJoin)
            {
                switch (kind)
                {
                case TAKjoin:
                    ret = joinRecords(left, defaultRight, 0, JTFmatchedleft);
                    break;
                case TAKdenormalize:
                    ret = left.getClear();
                    break;
                case TAKdenormalizegroup:
                    filteredRight.kill();
                    ret = groupDenormalizeRecords(left, filteredRight, JTFmatchedleft);
                    break;
                default:
                    throwUnexpected();
                }
            }
            left.clear();
            state = JSfillleft;
            if (ret)
            {
                processed++;
                return ret;
            }
            break;
        }

        case JScompare:
            if (joinLimit != 0)
            {
                switch (kind)
                {
                case TAKjoin:
                    {
                        while (right.isItem(rightIndex))
                        {
                            const void * rhs = right.item(rightIndex++);
                            if (helper.match(left, rhs))
                            {
                                matchedRight.replace(true, rightIndex-1);
                                matchedLeft = true;
                                if (!exclude)
                                {
                                    const void *ret = joinRecords(left, rhs, ++joinCounter, JTFmatchedleft|JTFmatchedright);
                                    if (ret)
                                    {
                                        processed++;
                                        joinLimit--;
                                        return ret;
                                    }
                                }
                            }
                        }
                        break;
                    }
                case TAKdenormalize:
                    {
                        OwnedConstRoxieRow newLeft;
                        newLeft.set(left);
                        unsigned rowSize = 0;
                        unsigned leftCount = 0;
                        while (right.isItem(rightIndex) && joinLimit)
                        {
                            const void * rhs = right.item(rightIndex++);
                            if (helper.match(left, rhs))
                            {
                                matchedRight.replace(true, rightIndex-1);
                                matchedLeft = true;
                                if (!exclude)
                                {
                                    try
                                    {
                                        RtlDynamicRowBuilder rowBuilder(rowAllocator);
                                        unsigned thisSize = helper.transform(rowBuilder, newLeft, rhs, ++leftCount, JTFmatchedleft|JTFmatchedright);
                                        if (thisSize)
                                        {
                                            rowSize = thisSize;
                                            newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                                            joinLimit--;
                                        }
                                    }
                                    catch(IException * e)
                                    {
                                        throw makeWrappedException(e);
                                    }
                                }
                            }
                        }
                        state = JSleftonly;
                        rightIndex = 0;
                        if (rowSize)
                        {
                            processed++;
                            return newLeft.getClear();
                        }
                        break;
                    }
                case TAKdenormalizegroup:
                    {
                        filteredRight.kill();
                        while (right.isItem(rightIndex))
                        {
                            const void * rhs = right.item(rightIndex++);
                            if (helper.match(left, rhs))
                            {
                                matchedRight.replace(true, rightIndex-1);
                                filteredRight.append(rhs);
                                matchedLeft = true;
                                if (filteredRight.ordinality()==joinLimit)
                                    break;
                            }
                        }
                        state = JSleftonly;
                        rightIndex = 0;

                        if (!exclude && filteredRight.ordinality())
                        {
                            const void * ret = groupDenormalizeRecords(left, filteredRight, JTFmatchedleft);
                            filteredRight.kill();

                            if (ret)
                            {
                                processed++;
                                return ret;
                            }
                        }
                        break;
                    }
                default:
                    throwUnexpected();
                }
            }
            state = JSleftonly;
            rightIndex = 0;
            joinCounter = 0;
            break;
        }
    }
}

bool CHThorJoinActivity::isGrouped()
{
    return false;
}

//=====================================================================================================

CHThorSelfJoinActivity::CHThorSelfJoinActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorJoinArg &_arg, ThorActivityKind _kind) 
        : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), outBuilder(NULL)
{
    dualCacheInput = NULL;
}

void CHThorSelfJoinActivity::ready()
{
    CHThorActivityBase::ready();
    outBuilder.setAllocator(rowAllocator);
    ICompare *compareLeft = helper.queryCompareLeft();
    if (helper.isLeftAlreadySorted())
        groupedInput.setown(createGroupedInputReader(&input->queryStream(), compareLeft));
    else
    {
        bool isStable = (helper.getJoinFlags() & JFunstable) == 0;
        RoxieSortAlgorithm sortAlgorithm = isStable ? stableSpillingQuickSortAlgorithm : spillingQuickSortAlgorithm;
        StringBuffer tempBase;
        agent.getTempfileBase(tempBase);
        groupedInput.setown(createSortedGroupedInputReader(&input->queryStream(), compareLeft, createSortAlgorithm(sortAlgorithm, compareLeft, *queryRowManager(), input->queryOutputMeta(), agent.queryCodeContext(), tempBase, activityId)));
    }
    leftOuterJoin = (helper.getJoinFlags() & JFleftouter) != 0;
    rightOuterJoin = (helper.getJoinFlags() & JFrightouter) != 0;
    exclude = (helper.getJoinFlags() & JFexclude) != 0;
    getLimitType(helper.getJoinFlags(), limitFail, limitOnFail);
    if (rightOuterJoin && !defaultLeft)
    {
        if (!defaultAllocator)                          
            defaultAllocator.setown(agent.queryCodeContext()->getRowAllocator(input->queryOutputMeta(), activityId));

        RtlDynamicRowBuilder rowBuilder(defaultAllocator);
        size32_t thisSize = helper.createDefaultLeft(rowBuilder);
        defaultLeft.setown(rowBuilder.finalizeRowClear(thisSize));
    }
    if ((leftOuterJoin || limitOnFail) && !defaultRight)
    {
        if (!defaultAllocator)                          
            defaultAllocator.setown(agent.queryCodeContext()->getRowAllocator(input->queryOutputMeta(), activityId));

        RtlDynamicRowBuilder rowBuilder(defaultAllocator);
        size32_t thisSize = helper.createDefaultRight(rowBuilder);
        defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
    }

    if((helper.getJoinFlags() & JFslidingmatch) != 0)
        throw MakeStringException(99, "Sliding self join not supported");
    keepLimit = helper.getKeepLimit();
    if(keepLimit == 0)
        keepLimit = (unsigned)-1;
    atmostLimit = helper.getJoinLimit();
    if(atmostLimit == 0)
        atmostLimit = (unsigned)-1;
    else
        assertex(!rightOuterJoin);
    abortLimit = helper.getMatchAbortLimit();
    if (abortLimit == 0) 
        abortLimit = (unsigned)-1;

    assertex((helper.getJoinFlags() & (JFfirst | JFfirstleft | JFfirstright)) == 0); // no longer supported

    collate = helper.queryCompareLeftRight();

    eof = false;
    doneFirstFill = false;
    failingLimit.clear();
    if ((helper.getJoinFlags() & JFlimitedprefixjoin) && helper.getJoinLimit()) 
    {   //Limited Match Join (s[1..n])
        dualcache.setown(new CRHDualCache());
        dualcache->init(groupedInput);
        dualCacheInput = dualcache->queryOut1();
        failingOuterAtmost = false;
        matchedLeft = false;
        leftIndex = 0;
        rightOuterIndex = 0;

        limitedhelper.setown(createRHLimitedCompareHelper());
        limitedhelper->init( helper.getJoinLimit(), dualcache->queryOut2(), collate, helper.queryPrefixCompare() );
    }
    joinCounter = 0;
}

void CHThorSelfJoinActivity::stop()
{
    outBuilder.clear();
    group.clear();
    groupedInput.clear();
    CHThorActivityBase::stop();
}

bool CHThorSelfJoinActivity::fillGroup()
{
    group.clear();
    matchedLeft = false;
    matchedRight.kill();
    failingOuterAtmost = false;
    OwnedConstRoxieRow next;
    unsigned groupCount = 0;
    next.setown(groupedInput->nextRow());
    while(next)
    {
        if(groupCount==abortLimit)
        {
            if(limitFail)
                failLimit(next);
            if ( agent.queryCodeContext()->queryDebugContext())
                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            if(limitOnFail)
            {
                assertex(!failingLimit);
                try
                {
                    failLimit(next);
                }
                catch(IException * except)
                {
                    failingLimit.setown(except);
                }
                assertex(failingLimit);
                group.append(next.getClear());
                groupCount++;
                break;
            }
            group.clear();
            groupCount = 0;
            while(next) 
                next.setown(groupedInput->nextRow());
        }
        else if(groupCount==atmostLimit)
        {
            if(leftOuterJoin)
            {
                group.append(next.getClear());
                groupCount++;
                failingOuterAtmost = true;
                break;
            }
            else
            {
                group.clear();
                groupCount = 0;
                while(next) 
                    next.setown(groupedInput->nextRow());
            }
        }
        else
        {
            group.append(next.getClear());
            groupCount++;
        }
        next.setown(groupedInput->nextRow());
    }
    if(group.ordinality()==0)
    {
        eof = true;
        return false;
    }
    leftIndex = 0;
    rightIndex = 0;
    joinCounter = 0;
    rightOuterIndex = 0;
    joinLimit = keepLimit;
    ForEachItemIn(idx, group)
        matchedRight.append(false);
    return true;
}

const void * CHThorSelfJoinActivity::nextRow()
{
    if (limitedhelper)  {
        while(!eof) //limited match join
        {
            if (!group.isItem(rightIndex))
            {
                lhs.setown(dualCacheInput->nextRow());
                if (lhs)
                {
                    rightIndex = 0;
                    joinCounter = 0;
                    group.clear();
                    limitedhelper->getGroup(group,lhs);
                }
                else 
                    eof = true;
            }

            if (group.isItem(rightIndex))
            {
                const void * rhs = group.item(rightIndex++);
                if(helper.match(lhs, rhs))
                {
                    const void * ret = joinRecords(lhs, rhs, ++joinCounter, JTFmatchedleft|JTFmatchedright, NULL);
                    if(ret)
                    {
                        processed++;
                        return ret;
                    }
                }
            }
        }
        return NULL;
    }

    if(!doneFirstFill)
    {
        fillGroup();
        doneFirstFill = true;
    }
    while(!eof)
    {
        if(failingOuterAtmost)
            while(group.isItem(leftIndex))
            {
                const void * ret = joinRecords(group.item(leftIndex++), defaultRight, 0, JTFmatchedleft, NULL);
                if(ret)
                {
                    processed++;
                    return ret;
                }
            }
        if((joinLimit == 0) || !group.isItem(rightIndex))
        {
            if(leftOuterJoin && !matchedLeft && !failingLimit)
            {
                const void * ret = joinRecords(group.item(leftIndex), defaultRight, 0, JTFmatchedleft, NULL);
                if(ret)
                {
                    matchedLeft = true;
                    processed++;
                    return ret;
                }
            }
            leftIndex++;
            matchedLeft = false;
            rightIndex = 0;
            joinCounter = 0;
            joinLimit = keepLimit;
        }
        if(!group.isItem(leftIndex))
        {
            if(failingLimit || failingOuterAtmost)
            {
                OwnedConstRoxieRow lhs(groupedInput->nextRow());  // dualCache never active here
                while(lhs)
                {
                    const void * ret = joinRecords(lhs, defaultRight, 0, JTFmatchedleft, failingLimit);
                    if(ret)
                    {
                        processed++;
                        return ret;
                    }
                    lhs.setown(groupedInput->nextRow());
                }
                failingLimit.clear();
            }
            if(rightOuterJoin && !failingLimit)
                while(group.isItem(rightOuterIndex))
                    if(!matchedRight.item(rightOuterIndex++))
                    {
                        const void * ret = joinRecords(defaultLeft, group.item(rightOuterIndex-1), 0, JTFmatchedright, NULL);
                        if(ret)
                        {
                            processed++;
                            return ret;
                        }
                    }
            if(!fillGroup())
                return NULL;
            continue;
        }
        const void * lhs = group.item(leftIndex);
        if(failingLimit)
        {
            leftIndex++;
            const void * ret = joinRecords(lhs, defaultRight, 0, JTFmatchedleft, failingLimit);
            if(ret)
            {
                processed++;
                return ret;
            }
        }
        else
        {
            const void * rhs = group.item(rightIndex++);
            if(helper.match(lhs, rhs))
            {
                matchedLeft = true;
                matchedRight.replace(true, rightIndex-1);
                if(!exclude)
                {
                    const void * ret = joinRecords(lhs, rhs, ++joinCounter, JTFmatchedleft|JTFmatchedright, NULL);
                    if(ret)
                    {
                        processed++;
                        joinLimit--;
                        return ret;
                    }
                }
            }
        }
    }
    return NULL;
}

const void * CHThorSelfJoinActivity::joinRecords(const void * curLeft, const void * curRight, unsigned counter, unsigned flags, IException * except)
{
    outBuilder.ensureRow();
    try
    {
            size32_t thisSize = (except ? helper.onFailTransform(outBuilder, curLeft, curRight, except, flags) : helper.transform(outBuilder, curLeft, curRight, counter, flags));
            if(thisSize){
                return outBuilder.finalizeRowClear(thisSize);   
            }
            else
                return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

void CHThorSelfJoinActivity::failLimit(const void * next)
{
    helper.onMatchAbortLimitExceeded();
    CommonXmlWriter xmlwrite(0);
    if (input->queryOutputMeta() && input->queryOutputMeta()->hasXML())
    {
        input->queryOutputMeta()->toXML((byte *) next, xmlwrite);
    }
    throw MakeStringException(0, "More than %d match candidates in self-join for row %s", abortLimit, xmlwrite.str());
}

bool CHThorSelfJoinActivity::isGrouped()
{
    return false;
}

//=====================================================================================================

CHThorLookupJoinActivity::LookupTable::LookupTable(unsigned _size, ICompare * _leftRightCompare, ICompare * _rightCompare, IHash * _leftHash, IHash * _rightHash, bool _dedupOnAdd)
    : leftRightCompare(_leftRightCompare), rightCompare(_rightCompare), leftHash(_leftHash), rightHash(_rightHash), dedupOnAdd(_dedupOnAdd)
{
    unsigned minsize = (4*_size)/3;
    size = 2;
    while((minsize >>= 1) > 0)
        size <<= 1;
    mask = size - 1;
    table = new OwnedConstRoxieRow[size];
    findex = BadIndex;
}

CHThorLookupJoinActivity::LookupTable::~LookupTable()
{
    delete [] table;
}

bool CHThorLookupJoinActivity::LookupTable::add(const void * _right)
{
    OwnedConstRoxieRow right(_right);
    findex = BadIndex;
    unsigned start = rightHash->hash(right) & mask;
    unsigned index = start;
    while(table[index])
    {
        if(dedupOnAdd && (rightCompare->docompare(table[index], right) == 0))
            return false;
        index++;
        if(index==size)
            index = 0;
        if(index==start)
            return false; //table is full, should never happen
    }
    table[index].setown(right.getClear());
    return true;
}

const void * CHThorLookupJoinActivity::LookupTable::find(const void * left) const
{
    fstart = leftHash->hash(left) & mask;
    findex = fstart;
    return doFind(left);
}

const void * CHThorLookupJoinActivity::LookupTable::findNext(const void * left) const
{
    if(findex == BadIndex)
        return NULL;
    advance();
    return doFind(left);
}

void CHThorLookupJoinActivity::LookupTable::advance() const
{
    findex++;
    if(findex==size)
        findex = 0;
    if(findex==fstart)
        throw MakeStringException(0, "Internal error hthor lookup join activity (hash table full on lookup)");
}

const void * CHThorLookupJoinActivity::LookupTable::doFind(const void * left) const
{
    while(table[findex])
    {
        if(leftRightCompare->docompare(left, table[findex]) == 0)
            return table[findex];
        advance();
    }
    findex = BadIndex;
    return NULL;
}

CHThorLookupJoinActivity::CHThorLookupJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorHashJoinArg &_arg, ThorActivityKind _kind) : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), table(0), outBuilder(NULL)
{
}

void CHThorLookupJoinActivity::ready()
{
    CHThorActivityBase::ready();
    input1->ready();
    outBuilder.setAllocator(rowAllocator);

    leftOuterJoin = (helper.getJoinFlags() & JFleftouter) != 0;
    assertex((helper.getJoinFlags() & JFrightouter) == 0);
    exclude = (helper.getJoinFlags() & JFexclude) != 0;
    many = (helper.getJoinFlags() & JFmanylookup) != 0;
    dedupRHS = (helper.getJoinFlags() & (JFmanylookup | JFmatchrequired | JFtransformMaySkip)) == 0; // optimisation: can implicitly dedup RHS unless is many lookup, or match required, or transform may skip
    if((helper.getJoinFlags() & (JFfirst | JFfirstleft | JFfirstright | JFslidingmatch)) != 0)
        throwUnexpected();  // compiler should have rejected

    keepLimit = helper.getKeepLimit();
    if(keepLimit==0)
        keepLimit = static_cast<unsigned>(-1);
    atmostLimit = helper.getJoinLimit();
    limitLimit = helper.getMatchAbortLimit();
    hasGroupLimit = ((atmostLimit > 0) || (limitLimit > 0));
    if(atmostLimit==0)
        atmostLimit = static_cast<unsigned>(-1);
    if(limitLimit==0)
        limitLimit = static_cast<unsigned>(-1);
    isSmartJoin = (helper.getJoinFlags() & JFsmart) != 0;
    getLimitType(helper.getJoinFlags(), limitFail, limitOnFail);

    if((leftOuterJoin || limitOnFail) && !defaultRight)
        createDefaultRight();   
    eog = false;
    matchedGroup = false;
    joinCounter = 0;
}

void CHThorLookupJoinActivity::stop()
{
    outBuilder.clear();
    left.clear();
    table.clear();
    CHThorActivityBase::stop();
    input1->stop();
}

void CHThorLookupJoinActivity::createDefaultRight()
{
    if (!defaultRight)
    {
        if (!defaultRightAllocator)
            defaultRightAllocator.setown(agent.queryCodeContext()->getRowAllocator(input1->queryOutputMeta(), activityId));

        RtlDynamicRowBuilder rowBuilder(defaultRightAllocator);
        size32_t thisSize = helper.createDefaultRight(rowBuilder);
        defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
    }
}

void CHThorLookupJoinActivity::loadRight()
{
    OwnedRowArray rightset;
    const void * next;
    while(true)
    {
        next = input1->nextRow();
        if(!next)
            next = input1->nextRow();
        if(!next)
            break;
        rightset.append(next);
    }

    unsigned rightord = rightset.ordinality();
    table.setown(new LookupTable(rightord, helper.queryCompareLeftRight(), helper.queryCompareRight(), helper.queryHashLeft(), helper.queryHashRight(), dedupRHS));

    unsigned i;
    for(i=0; i<rightord; i++)
        table->add(rightset.itemClear(i));
};

void CHThorLookupJoinActivity::setInput(unsigned index, IHThorInput * _input)
{
    if (index==1)
        input1 = _input;
    else
        CHThorActivityBase::setInput(index, _input);
}

//following are all copied from CHThorJoinActivity - should common up.
const void * CHThorLookupJoinActivity::joinRecords(const void * left, const void * right, unsigned counter, unsigned flags)
{
    try
    {
        outBuilder.ensureRow();
        size32_t thisSize = helper.transform(outBuilder, left, right, counter, flags);
        if(thisSize)
            return outBuilder.finalizeRowClear(thisSize);
        else
            return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

const void * CHThorLookupJoinActivity::joinException(const void * left, IException * except)
{
    try
    {
        outBuilder.ensureRow();
        memsize_t thisSize = helper.onFailTransform(outBuilder, left, defaultRight, except, JTFmatchedleft);
        if(thisSize)
            return outBuilder.finalizeRowClear(thisSize);
        else
            return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

const void * CHThorLookupJoinActivity::groupDenormalizeRecords(const void * left, ConstPointerArray & rows, unsigned flags)
{
    try
    {
        outBuilder.ensureRow();
        unsigned numRows = rows.ordinality();
        const void * right = numRows ? rows.item(0) : defaultRight.get();
        if (numRows>0)
            flags |= JTFmatchedright;
        memsize_t thisSize = helper.transform(outBuilder, left, right, numRows, (const void * *)rows.getArray(), flags);
        if(thisSize)
            return outBuilder.finalizeRowClear(thisSize);
        else
            return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

const void * CHThorLookupJoinActivity::nextRow()
{
    if(!table)
        loadRight();
    switch (kind)
    {
    case TAKlookupjoin:
    case TAKsmartjoin:
        return nextRowJoin();
    case TAKlookupdenormalize:
    case TAKlookupdenormalizegroup:
    case TAKsmartdenormalize:
    case TAKsmartdenormalizegroup:
        return nextRowDenormalize();
    }
    throwUnexpected();
}

const void * CHThorLookupJoinActivity::nextRowJoin()
{
    while(true)
    {
        const void * right = NULL;
        if(!left)
        {
            left.setown(input->nextRow());
            keepCount = keepLimit;
            if(!left)
            {
                if (isSmartJoin)
                    left.setown(input->nextRow());

                if(!left)
                {
                    if(matchedGroup || eog)
                    {
                        matchedGroup = false;
                        eog = true;
                        return NULL;
                    }
                    eog = true;
                    continue;
                }
            }
            eog = false;
            gotMatch = false;
            right = getRightFirst();
        }
        else
            right = getRightNext();
        const void * ret = NULL;
        if(failingLimit)
        {
            ret = joinException(left, failingLimit);
        }
        else
        {
            while(right)
            {
                if(helper.match(left, right))
                {
                    gotMatch = true;
                    if(exclude)
                        break;
                    ret = joinRecords(left, right, ++joinCounter, JTFmatchedleft|JTFmatchedright);
                    if(ret)
                    {
                        processed++;
                        break;
                    }
                }
                right = getRightNext();
                ret = NULL;
            }
            if(leftOuterJoin && !gotMatch)
            {
                ret = joinRecords(left, defaultRight, 0, JTFmatchedleft);
                gotMatch = true;
            }
        }
        if(ret)
        {
            matchedGroup = true;
            processed++;
            if(!many || (--keepCount == 0) || failingLimit)
            {
                left.clear();
                joinCounter = 0;
                failingLimit.clear();
            }
            return ret;
        }
        left.clear();
        joinCounter = 0;
    }
}

const void * CHThorLookupJoinActivity::nextRowDenormalize()
{
    while(true)
    {
        left.setown(input->nextRow());
        if(!left)
        {
            if (!matchedGroup || isSmartJoin)
                left.setown(input->nextRow());

            if (!left)
            {
                matchedGroup = false;
                return NULL;
            }
        }
        gotMatch = false;

        const void * right = getRightFirst();
        const void * ret = NULL;
        if (failingLimit)
            ret = joinException(left, failingLimit);
        else if (kind == TAKlookupdenormalize || kind == TAKsmartdenormalize)
        {
            OwnedConstRoxieRow newLeft(left.getLink());
            unsigned rowSize = 0;
            unsigned leftCount = 0;
            keepCount = keepLimit;
            while (right)
            {
                if (helper.match(left, right))
                {
                    gotMatch = true;
                    if (exclude)
                        break;

                    try
                    {
                        RtlDynamicRowBuilder rowBuilder(rowAllocator);
                        unsigned thisSize = helper.transform(rowBuilder, newLeft, right, ++leftCount, JTFmatchedleft|JTFmatchedright);
                        if (thisSize)
                        {
                            rowSize = thisSize;
                            newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                        }
                    }
                    catch(IException * e)
                    {
                        throw makeWrappedException(e);
                    }
                    if(!many || (--keepCount == 0))
                        break;
                }
                right = getRightNext();
            }
            //Is this rowSize test correct??  Is there any situation where it shouldn't just return newLeft?
            if (rowSize)
                ret = newLeft.getClear();
            else if (leftOuterJoin && !gotMatch)
                ret = left.getClear();
        }
        else
        {
            filteredRight.kill();
            keepCount = keepLimit;
            while (right)
            {
                if (helper.match(left, right))
                {
                    gotMatch = true;
                    if(exclude)
                        break;
                    filteredRight.append(right);
                    if(!many || (--keepCount == 0))
                        break;
                }
                right = getRightNext();
            }

            if((filteredRight.ordinality() > 0) || (leftOuterJoin && !gotMatch))
                ret = groupDenormalizeRecords(left, filteredRight, JTFmatchedleft);
            filteredRight.kill();
        }
        left.clear();
        failingLimit.clear();
        if(ret)
        {
            matchedGroup = true;
            processed++;
            return ret;
        }
    }
}


bool CHThorLookupJoinActivity::isGrouped()
{
    return input ? input->isGrouped() : false;
}

const void * CHThorLookupJoinActivity::fillRightGroup()
{
    rightGroup.kill();
    for(const void * right = table->find(left); right; right = table->findNext(left))
    {
        rightGroup.append(right);
        if(rightGroup.ordinality() > limitLimit)
        {
            if(limitFail)
                failLimit();
            if ( agent.queryCodeContext()->queryDebugContext())
                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            gotMatch = true;
            if(limitOnFail)
            {
                assertex(!failingLimit);
                try
                {
                    failLimit();
                }
                catch(IException * e)
                {
                    failingLimit.setown(e);
                }
                assertex(failingLimit);
            }
            else
            {
                rightGroup.kill();
            }
            break;
        }
        if(rightGroup.ordinality() > atmostLimit)
        {
            rightGroup.kill();
            break;
        }
    }
    rightGroupIndex = 0;
    return readRightGroup();
}

void CHThorLookupJoinActivity::failLimit()
{
    helper.onMatchAbortLimitExceeded();
    CommonXmlWriter xmlwrite(0);
    if(input->queryOutputMeta() && input->queryOutputMeta()->hasXML())
    {
        input->queryOutputMeta()->toXML(static_cast<const unsigned char *>(left.get()), xmlwrite);
    }
    throw MakeStringException(0, "More than %u match candidates in join for row %s", limitLimit, xmlwrite.str());
}

unsigned const CHThorLookupJoinActivity::LookupTable::BadIndex(static_cast<unsigned>(-1));

//=====================================================================================================

CHThorAllJoinActivity::CHThorAllJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorAllJoinArg &_arg, ThorActivityKind _kind) : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), outBuilder(NULL)
{
}

void CHThorAllJoinActivity::ready()
{
    CHThorActivityBase::ready();
    input1->ready();
    outBuilder.setAllocator(rowAllocator);

    leftOuterJoin = (helper.getJoinFlags() & JFleftouter) != 0;
    exclude = (helper.getJoinFlags() & JFexclude) != 0;
    if(leftOuterJoin && !defaultRight)
        createDefaultRight();   
    if((helper.getJoinFlags() & (JFrightouter | JFfirst | JFfirstleft | JFfirstright)) != 0)
        throwUnexpected();

    keepLimit = helper.getKeepLimit();
    if(keepLimit==0)
        keepLimit = (unsigned)-1;

    started = false;
    countForLeft = keepLimit;
    matchedLeft = false;
    matchedGroup = false;
    eog = false;
    eos = false;
}

void CHThorAllJoinActivity::stop()
{
    outBuilder.clear();
    left.clear();
    rightset.clear();
    matchedRight.kill();
    CHThorActivityBase::stop();
    input1->stop();
}

void CHThorAllJoinActivity::createDefaultRight()
{
    if (!defaultRight)
    {
        if (!defaultRightAllocator)
            defaultRightAllocator.setown(agent.queryCodeContext()->getRowAllocator(input1->queryOutputMeta(), activityId));

        RtlDynamicRowBuilder rowBuilder(defaultRightAllocator);
        size32_t thisSize = helper.createDefaultRight(rowBuilder);
        defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
    }
}

void CHThorAllJoinActivity::loadRight()
{
    const void * next;
    while(true)
    {
        next = input1->nextRow();
        if(!next)
            next = input1->nextRow();
        if(!next)
            break;
        rightset.append(next);
        matchedRight.append(false);
    }
    rightIndex = 0;
    joinCounter = 0;
    rightOrdinality = rightset.ordinality();
}

const void * CHThorAllJoinActivity::joinRecords(const void * left, const void * right, unsigned counter, unsigned flags)
{
    try
    {
        outBuilder.ensureRow();
        memsize_t thisSize = helper.transform(outBuilder, left, right, counter, flags);
        if(thisSize)
            return outBuilder.finalizeRowClear(thisSize);
        else
            return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

const void * CHThorAllJoinActivity::groupDenormalizeRecords(const void * curLeft, ConstPointerArray & rows, unsigned flags)
{
    try
    {
        outBuilder.ensureRow();
        unsigned numRows = rows.ordinality();
        const void * right = numRows ? rows.item(0) : defaultRight.get();
        if (numRows>0)
            flags |= JTFmatchedright;
        memsize_t thisSize = helper.transform(outBuilder, curLeft, right, numRows, (const void * *)rows.getArray(), flags);
        if(thisSize)
            return outBuilder.finalizeRowClear(thisSize);
        else
            return NULL;
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

void CHThorAllJoinActivity::setInput(unsigned index, IHThorInput * _input)
{
    if (index==1)
        input1 = _input;
    else
    {
        CHThorActivityBase::setInput(index, _input);
        leftIsGrouped = true; // input->isGrouped() is unreliable and it is just as good to always behave as if input is grouped
    }
}

const void * CHThorAllJoinActivity::nextRow()
{
    if(!started)
    {
        started = true;
        left.setown(input->nextRow());
        matchedLeft = false;
        countForLeft = keepLimit;
        if(!left)
        {
            eos = true;
            return NULL;
        }
        loadRight();
    }

    const void * ret;   
    const void * right;
    if(eos)
        return NULL;

    while(true)
    {
        ret = NULL;

        if((rightIndex == rightOrdinality) || (countForLeft==0))
        {
            if(leftOuterJoin && left && !matchedLeft)
            {
                switch(kind)
                {
                case TAKalljoin:
                    ret = joinRecords(left, defaultRight, 0, JTFmatchedleft);
                    break;
                case TAKalldenormalize:
                    ret = left.getClear();
                    break;
                case TAKalldenormalizegroup:
                    filteredRight.kill();
                    ret = groupDenormalizeRecords(left, filteredRight, JTFmatchedleft);
                    break;
                default:
                    throwUnexpected();
                }
            }
            rightIndex = 0;
            joinCounter = 0;
            left.clear();
            if(ret)
            {
                matchedGroup = true;
                processed++;
                return ret;
            }
        }

        if(!left)
        {
            left.setown(input->nextRow());
            matchedLeft = false;
            countForLeft = keepLimit;
        }
        if(!left)
        {
            if(eog)
            {
                eos = true;
                matchedGroup = false;
                return NULL;
            }
            eog = true;
            if(matchedGroup && leftIsGrouped)
            {
                matchedGroup = false;
                return NULL;
            }
            matchedGroup = false;
            continue;
        }

        eog = false;
        switch(kind)
        {
        case TAKalljoin:
            while(rightIndex < rightOrdinality)
            {
                right = rightset.item(rightIndex);
                if(helper.match(left, right))
                {
                    matchedLeft = true;
                    matchedRight.replace(true, rightIndex);
                    if(!exclude)
                        ret = joinRecords(left, right, ++joinCounter, JTFmatchedleft|JTFmatchedright);
                }
                rightIndex++;
                if(ret)
                {
                    countForLeft--;
                    matchedGroup = true;
                    processed++;
                    return ret;
                }
            }
        case TAKalldenormalize:
            {
                OwnedConstRoxieRow newLeft;
                newLeft.set(left);
                unsigned rowSize = 0;
                unsigned leftCount = 0;
                while((rightIndex < rightOrdinality) && countForLeft)
                {
                    right = rightset.item(rightIndex);
                    if(helper.match(left, right))
                    {
                        matchedLeft = true;
                        matchedRight.replace(true, rightIndex);
                        if(!exclude)
                        {
                            try
                            {
                                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                                unsigned thisSize = helper.transform(rowBuilder, newLeft, right, ++leftCount, JTFmatchedleft|JTFmatchedright);
                                if(thisSize)
                                {
                                    rowSize = thisSize;
                                    newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                                    --countForLeft;
                                }
                            }
                            catch(IException * e)
                            {
                                throw makeWrappedException(e);
                            }
                        }
                    }
                    rightIndex++;
                }
                if(rowSize)
                {
                    processed++;
                    return newLeft.getClear();
                }
            }
            break;
        case TAKalldenormalizegroup:
            filteredRight.kill();
            while((rightIndex < rightOrdinality) && countForLeft)
            {
                right = rightset.item(rightIndex);
                if(helper.match(left, right))
                {
                    matchedLeft = true;
                    matchedRight.replace(true, rightIndex);
                    filteredRight.append(right);
                    --countForLeft;
                }
                ++rightIndex;
            }
            if(!exclude && filteredRight.ordinality())
            {
                const void * ret = groupDenormalizeRecords(left, filteredRight, JTFmatchedleft);
                filteredRight.kill();
                if(ret)
                {
                    processed++;
                    return ret;
                }
            }
            break;
        default:
            throwUnexpected();
        }
    }
}

bool CHThorAllJoinActivity::isGrouped()
{
    return input ? input->isGrouped() : false;
}

//=====================================================================================================
//=====================================================================================================

CHThorWorkUnitWriteActivity::CHThorWorkUnitWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorWorkUnitWriteArg &_arg, ThorActivityKind _kind)
 : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorWorkUnitWriteActivity::execute()
{
    unsigned flags = helper.getFlags();
    grouped = (POFgrouped & flags) != 0;
    // In absense of OPT_OUTPUTLIMIT check pre 5.2 legacy name OPT_OUTPUTLIMIT_LEGACY
    size32_t outputLimit = agent.queryWorkUnit()->getDebugValueInt(OPT_OUTPUTLIMIT, agent.queryWorkUnit()->getDebugValueInt(OPT_OUTPUTLIMIT_LEGACY, defaultDaliResultLimit));
    if (flags & POFmaxsize)
        outputLimit = helper.getMaxSize();
    if (outputLimit>defaultDaliResultOutputMax)
        throw MakeStringException(0, "Dali result outputs are restricted to a maximum of %d MB, the current limit is %d MB. A huge dali result usually indicates the ECL needs altering.", defaultDaliResultOutputMax, defaultDaliResultLimit);
    assertex(outputLimit<=0x1000); // 32bit limit because MemoryBuffer/CMessageBuffers involved etc.
    outputLimit *= 0x100000;
    MemoryBuffer rowdata;
    __int64 rows = 0;
    IRecordSize * inputMeta = input->queryOutputMeta();
    if (0 != (POFextend & helper.getFlags()))
    {
        WorkunitUpdate w = agent.updateWorkUnit();
        Owned<IWUResult> result = updateWorkUnitResult(w, helper.queryName(), helper.getSequence());
        rows = result->getResultRowCount();
    }
    __int64 initialRows = rows;

    Owned<IOutputRowSerializer> rowSerializer;
    if (input->queryOutputMeta()->getMetaFlags() & MDFneedserializedisk)
        rowSerializer.setown( input->queryOutputMeta()->createDiskSerializer(agent.queryCodeContext(), activityId) );

    int seq = helper.getSequence();
    bool toStdout = (seq >= 0) && agent.queryWriteResultsToStdout();
    Owned<SimpleOutputWriter> writer;
    if (toStdout)
        writer.setown(new SimpleOutputWriter);

    if (agent.queryOutputFmt() == ofXML  && seq >= 0)
    {
        StringBuffer sb;
        const char *name = helper.queryName();
        if (name && *name)
            sb.appendf("<Dataset name='%s'>\n", name);
        else
            sb.appendf("<Dataset name='Result %d'>\n", seq+1);
        agent.queryOutputSerializer()->fwrite(seq, (const void*)sb.str(), 1, sb.length());
    }
    for (;;)
    {
        if ((unsigned __int64)rows >= agent.queryStopAfter())
            break;
        OwnedConstRoxieRow nextrec(input->nextRow());
        if (grouped && (rows != initialRows))
            rowdata.append(nextrec == NULL);
        if (!nextrec)
        {
            nextrec.setown(input->nextRow());
            if (!nextrec)
                break;
        }
        size32_t thisSize = inputMeta->getRecordSize(nextrec);
        if(outputLimit && ((rowdata.length() + thisSize) > outputLimit))
        {
            StringBuffer errMsg("Dataset too large to output to workunit (limit "); 
            errMsg.append(outputLimit/0x100000).append(" megabytes), in result (");
            const char *name = helper.queryName();
            if (name)
                errMsg.append("name=").append(name);
            else
                errMsg.append("sequence=").append(helper.getSequence());
            errMsg.append(")");
            throw MakeStringExceptionDirect(0, errMsg.str());
         }
        if (rowSerializer)
        {
            CThorDemoRowSerializer serializerTarget(rowdata);
            rowSerializer->serialize(serializerTarget, (const byte *) nextrec.get() );
        }
        else
            rowdata.append(thisSize, nextrec);
        if (toStdout && seq >= 0)
        {
            if (agent.queryOutputFmt() == ofSTD)
            {
                helper.serializeXml((byte *) nextrec.get(), *writer);
                writer->newline();
                agent.queryOutputSerializer()->fwrite(seq, (const void*)writer->str(), 1, writer->length());
                writer->clear();
            }
            else if (agent.queryOutputFmt() == ofXML)
            {
                CommonXmlWriter xmlwrite(0,1);
                xmlwrite.outputBeginNested(DEFAULTXMLROWTAG, false);
                helper.serializeXml((byte *) nextrec.get(), xmlwrite);
                xmlwrite.outputEndNested(DEFAULTXMLROWTAG);
                agent.queryOutputSerializer()->fwrite(seq, (const void*)xmlwrite.str(), 1, xmlwrite.length());
            }
        }
        rows++;
    }
    WorkunitUpdate w = agent.updateWorkUnit();
    Owned<IWUResult> result = updateWorkUnitResult(w, helper.queryName(), helper.getSequence());
    if (0 != (POFextend & helper.getFlags()))
        result->addResultRaw(rowdata.length(), rowdata.toByteArray(), ResultFormatRaw);
    else
        result->setResultRaw(rowdata.length(), rowdata.toByteArray(), ResultFormatRaw);
    result->setResultStatus(ResultStatusCalculated);
    result->setResultRowCount(rows);
    result->setResultTotalRowCount(rows); // Is this right??
    if (toStdout && seq >= 0)
    {
        if (agent.queryOutputFmt() == ofXML)
        {
            StringBuffer sb;
            sb.appendf(DEFAULTXMLFOOTER).newline();
            agent.queryOutputSerializer()->fwrite(seq, (const void*)sb.str(), 1, sb.length());
        }
        else if (agent.queryOutputFmt() != ofSTD)
            agent.outputFormattedResult(helper.queryName(), seq, false);

        if (!(POFextend & helper.getFlags()))//POextend results will never get closed, so wont flush until serializer dtor
            agent.queryOutputSerializer()->close(seq, false);
    }
}

//=====================================================================================================

CHThorDictionaryWorkUnitWriteActivity::CHThorDictionaryWorkUnitWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDictionaryWorkUnitWriteArg &_arg, ThorActivityKind _kind)
 : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorDictionaryWorkUnitWriteActivity::execute()
{
    int sequence = helper.getSequence();
    const char *storedName = helper.queryName();
    assertex(storedName && *storedName);
    assertex(sequence < 0);

    RtlLinkedDictionaryBuilder builder(rowAllocator, helper.queryHashLookupInfo());
    for (;;)
    {
        const void *row = input->nextRow();
        if (!row)
        {
            row = input->nextRow();
            if (!row)
                break;
        }
        builder.appendOwn(row);
        processed++;
    }
    unsigned __int64 usedCount = rtlDictionaryCount(builder.getcount(), builder.queryrows());

    // In absense of OPT_OUTPUTLIMIT check pre 5.2 legacy name OPT_OUTPUTLIMIT_LEGACY
    size32_t outputLimit = agent.queryWorkUnit()->getDebugValueInt(OPT_OUTPUTLIMIT, agent.queryWorkUnit()->getDebugValueInt(OPT_OUTPUTLIMIT_LEGACY, defaultDaliResultLimit)) * 0x100000;
    MemoryBuffer rowdata;
    CThorDemoRowSerializer out(rowdata);
    Owned<IOutputRowSerializer> serializer = input->queryOutputMeta()->createDiskSerializer(agent.queryCodeContext(), activityId);
    rtlSerializeDictionary(out, serializer, builder.getcount(), builder.queryrows());
    if(outputLimit && (rowdata.length()  > outputLimit))
    {
        StringBuffer errMsg("Dictionary too large to output to workunit (limit ");
        errMsg.append(outputLimit/0x100000).append(" megabytes), in result (");
        const char *name = helper.queryName();
        if (name)
            errMsg.append("name=").append(name);
        else
            errMsg.append("sequence=").append(helper.getSequence());
        errMsg.append(")");
        throw MakeStringExceptionDirect(0, errMsg.str());
    }

    WorkunitUpdate w = agent.updateWorkUnit();
    Owned<IWUResult> result = updateWorkUnitResult(w, helper.queryName(), helper.getSequence());
    result->setResultRaw(rowdata.length(), rowdata.toByteArray(), ResultFormatRaw);
    result->setResultStatus(ResultStatusCalculated);
    result->setResultRowCount(usedCount);
    result->setResultTotalRowCount(usedCount); // Is this right??
}

//=====================================================================================================


CHThorRemoteResultActivity::CHThorRemoteResultActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorRemoteResultArg &_arg, ThorActivityKind _kind)
 : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorRemoteResultActivity::execute()
{
    OwnedConstRoxieRow result(input->nextRow());
    helper.sendResult(result);
}


//=====================================================================================================

CHThorInlineTableActivity::CHThorInlineTableActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorInlineTableArg &_arg, ThorActivityKind _kind) :
                 CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorInlineTableActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    curRow = 0;
    numRows = helper.numRows();
}


const void *CHThorInlineTableActivity::nextRow()
{
    // Filtering empty rows, returns the next valid row
    while (curRow < numRows)
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t size = helper.getRow(rowBuilder, curRow++);
        if (size)
        {
            processed++;
            return rowBuilder.finalizeRowClear(size);
        }
    }
    return NULL;
}

//=====================================================================================================

CHThorNullActivity::CHThorNullActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

const void *CHThorNullActivity::nextRow()
{
    return NULL;
}

//=====================================================================================================

CHThorActionActivity::CHThorActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorActionArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorActionActivity::execute()
{
    helper.action();
}

const void *CHThorActionActivity::nextRow()
{
    return NULL;
}
//=====================================================================================================

CHThorSideEffectActivity::CHThorSideEffectActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSideEffectArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

const void *CHThorSideEffectActivity::nextRow()
{
    try
    {
        helper.action();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }

    return NULL;
}

//=====================================================================================================

CHThorDummyActivity::CHThorDummyActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

void CHThorDummyActivity::execute()
{
}

const void *CHThorDummyActivity::nextRow()
{
    return input ? input->nextRow() : NULL;
}

//=====================================================================================================

CHThorWhenActionActivity::CHThorWhenActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind, EclGraphElement * _graphElement)
                         : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), graphElement(_graphElement)
{
}

void CHThorWhenActionActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    graphElement->executeDependentActions(agent, NULL, WhenBeforeId);
    graphElement->executeDependentActions(agent, NULL, WhenParallelId);
}

void CHThorWhenActionActivity::execute()
{
    graphElement->executeDependentActions(agent, NULL, 1);
}

const void * CHThorWhenActionActivity::nextRow()
{
    return input->nextRow();
}

void CHThorWhenActionActivity::stop()
{
    graphElement->executeDependentActions(agent, NULL, WhenSuccessId);
    CHThorSimpleActivityBase::stop();
}

//=====================================================================================================

CHThorMultiInputActivity::CHThorMultiInputActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

void CHThorMultiInputActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    ForEachItemIn(idx, inputs)
        inputs.item(idx)->ready();
}

void CHThorMultiInputActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    ForEachItemIn(idx, inputs)
        inputs.item(idx)->stop();
}

void CHThorMultiInputActivity::resetEOF()
{
    CHThorSimpleActivityBase::resetEOF();
    ForEachItemIn(idx, inputs)
        inputs.item(idx)->resetEOF();
}

void CHThorMultiInputActivity::setInput(unsigned index, IHThorInput *_input)
{
    if (index==inputs.length())
    {
        inputs.append(_input);
    }
    else
    {
        while (!inputs.isItem(index))
            inputs.append(NULL);
        inputs.replace(_input, index);
    }
}

void CHThorMultiInputActivity::updateProgress(IStatisticGatherer &progress) const
{
    CHThorSimpleActivityBase::updateProgress(progress);
    ForEachItemIn(idx, inputs)
    {
        IHThorInput *i = inputs.item(idx);
        if (i)
            i->updateProgress(progress);
    }
}   

//=====================================================================================================

CHThorConcatActivity::CHThorConcatActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorFunnelArg &_arg, ThorActivityKind _kind) : CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorConcatActivity::ready()
{
    grouped = helper.queryOutputMeta()->isGrouped();
    inputIdx = 0;
    curInput = inputs.item(inputIdx);
    eogSeen = false;
    anyThisGroup = false;
    CHThorMultiInputActivity::ready();
}

const void *CHThorConcatActivity::nextRow()
{
    if (!curInput)
        return NULL;  // eof
    const void * next = curInput->nextRow();
    if (next)
    {
        anyThisGroup = true;
        eogSeen = false;
        processed++;
        return next;
    }
    else if (!eogSeen)
    {
        eogSeen = true;
        if (grouped)
        {
            if (anyThisGroup)
            {
                anyThisGroup = false;
                return NULL;
            }
            else
                return nextRow();
        }
        else
            return nextRow();
    }
    else if (inputIdx < inputs.length()-1)
    {
        inputIdx++;
        curInput = inputs.item(inputIdx);
        eogSeen = false;
        anyThisGroup = false;
        return nextRow();
    }
    else
    {
        curInput = NULL;
        return NULL;
    }
}


//=====================================================================================================

CHThorNonEmptyActivity::CHThorNonEmptyActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNonEmptyArg &_arg, ThorActivityKind _kind) : CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorNonEmptyActivity::ready()
{
    grouped = helper.queryOutputMeta()->isGrouped();
    selectedInput = NULL;
    CHThorMultiInputActivity::ready();
}

const void *CHThorNonEmptyActivity::nextRow()
{
    if (!selectedInput)
    {
        ForEachItemIn(i, inputs)
        {
            IHThorInput * cur = inputs.item(i);
            const void * next = cur->nextRow();
            if (next)
            {
                selectedInput = cur;
                processed++;
                return next;
            }
        }
        return NULL;
    }
    const void * next = selectedInput->nextRow();
    if (next)
        processed++;
    return next;
}


//=====================================================================================================

CHThorRegroupActivity::CHThorRegroupActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorRegroupArg &_arg, ThorActivityKind _kind) : CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorRegroupActivity::ready()
{
    inputIndex = 0;
    eof = false;
    numProcessedLastGroup = processed;
    CHThorMultiInputActivity::ready();
}


const void * CHThorRegroupActivity::nextFromInputs()
{
    unsigned initialInput = inputIndex;
    while (inputs.isItem(inputIndex))
    {
        OwnedConstRoxieRow next(inputs.item(inputIndex)->nextRow());
        if (next)
        {
            if ((inputIndex != initialInput) && (inputIndex != initialInput+1))
            {
                throw MakeStringException(100, "Mismatched groups supplied to regroup %u", activityId);
            }
            return next.getClear();
        }
        inputIndex++;
    }

    if ((initialInput != 0) && (initialInput+1 != inputs.ordinality()))
        throw MakeStringException(100, "Mismatched groups supplied to Regroup Activity(%u)", activityId);

    inputIndex = 0;
    return NULL;
}

const void * CHThorRegroupActivity::nextRow()
{
    if (eof)
        return NULL;

    const void * ret = nextFromInputs();
    if (ret)
    {
        processed++;
        return ret;
    }

    if (numProcessedLastGroup != processed)
    {
        numProcessedLastGroup = processed;
        return NULL;
    }

    eof = true;
    return NULL;
}

//=====================================================================================================

CHThorRollupGroupActivity::CHThorRollupGroupActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorRollupGroupArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorRollupGroupActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    eof = false;
}


const void * CHThorRollupGroupActivity::nextRow()
{
    if (eof)
        return NULL;

    for (;;)
    {
        OwnedRowArray group;

        for (;;)
        {
            const void * in = input->nextRow();
            if (!in)
                break;
            group.append(in);
        }

        if (group.ordinality() == 0)
        {
            eof = true;
            return NULL;
        }

        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transform(rowBuilder, group.ordinality(), (const void * *)group.getArray());
            if (outSize)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=====================================================================================================

CHThorCombineActivity::CHThorCombineActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCombineArg &_arg, ThorActivityKind _kind) : CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorCombineActivity::ready()
{
    numProcessedLastGroup = processed;
    CHThorMultiInputActivity::ready();
}

void CHThorCombineActivity::nextInputs(OwnedRowArray & out)
{
    ForEachItemIn(i, inputs)
    {
        const void * next = inputs.item(i)->nextRow();
        if (next)
            out.append(next);
    }
}


const void *CHThorCombineActivity::nextRow()
{
    for (;;)
    {
        OwnedRowArray group;
        nextInputs(group);
        if ((group.ordinality() == 0) && (numProcessedLastGroup == processed))
            nextInputs(group);
        if (group.ordinality() == 0)
        {
            numProcessedLastGroup = processed;
            return NULL;
        }
        else if (group.ordinality() != inputs.ordinality())
        {
            throw MakeStringException(101, "Mismatched group input for Combine Activity(%u)", activityId);
        }

        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transform(rowBuilder, group.ordinality(), (const void * *)group.getArray());
            if (outSize)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=====================================================================================================

CHThorCombineGroupActivity::CHThorCombineGroupActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCombineGroupArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorCombineGroupActivity::ready()
{
    numProcessedLastGroup = processed;
    CHThorSimpleActivityBase::ready();
    input1->ready();
}

void CHThorCombineGroupActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    input1->stop();
}

void CHThorCombineGroupActivity::setInput(unsigned index, IHThorInput *_input)
{
    if (index==1)
        input1 = _input;
    else
        CHThorSimpleActivityBase::setInput(index, _input);
}


const void *CHThorCombineGroupActivity::nextRow()
{
    for (;;)
    {
        OwnedConstRoxieRow left(input->nextRow());
        if (!left && (numProcessedLastGroup == processed))
            left.setown(input->nextRow());

        if (!left)
        {
            if (numProcessedLastGroup == processed)
            {
                OwnedConstRoxieRow nextRight(input1->nextRow());
                if (nextRight)
                    throw MakeStringException(101, "Missing LEFT record for Combine group Activity(%u)", activityId);
            }
            else
                numProcessedLastGroup = processed;
            return NULL;
        }

        OwnedRowArray group;
        for (;;)
        {
            const void * in = input1->nextRow();
            if (!in)
                break;
            group.append(in);
        }

        if (group.ordinality() == 0)
        {
            throw MakeStringException(101, "Missing RIGHT group for Combine Group Activity(%u)", activityId);
        }

        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transform(rowBuilder, left, group.ordinality(), (const void * *)group.getArray());
            if (outSize)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }
}

//=====================================================================================================

CHThorApplyActivity::CHThorApplyActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorApplyArg &_arg, ThorActivityKind _kind) : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorApplyActivity::execute()
{
    try
    {
        helper.start();
        for (;;)
        {
            OwnedConstRoxieRow next(input->nextRow());
            if (!next)
            {
                next.setown(input->nextRow());
                if (!next)
                    break;
            }
            helper.apply(next);
        }
        helper.end();
    }
    catch (IException *e)
    {
        throw makeWrappedException(e);
    }
}

//=====================================================================================================

CHThorDistributionActivity::CHThorDistributionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDistributionArg &_arg, ThorActivityKind _kind)
 : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}


void CHThorDistributionActivity::execute()
{
    MemoryAttr ma;
    IDistributionTable * * accumulator = (IDistributionTable * *)ma.allocate(helper.queryInternalRecordSize()->getMinRecordSize());
    helper.clearAggregate(accumulator); 

    OwnedConstRoxieRow nextrec(input->nextRow());
    for (;;)
    {
        if (!nextrec)
        {
            nextrec.setown(input->nextRow());
            if (!nextrec)
                break;
        }
        helper.process(accumulator, nextrec);
        nextrec.setown(input->nextRow());
    }
    StringBuffer result;
    result.append("<XML>");
    helper.gatherResult(accumulator, result);
    result.append("</XML>");
    helper.sendResult(result.length(), result.str());
    helper.destruct(accumulator);
}

//---------------------------------------------------------------------------

CHThorWorkunitReadActivity::CHThorWorkunitReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorWorkunitReadArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    first = true;
    bufferStream.setown(createMemoryBufferSerialStream(resultBuffer));
    deserializer.setStream(bufferStream);
}

CHThorWorkunitReadActivity::~CHThorWorkunitReadActivity()
{
}

void CHThorWorkunitReadActivity::ready()
{
    CHThorSimpleActivityBase::ready();

    rowDeserializer.setown(rowAllocator->createDiskDeserializer(agent.queryCodeContext()));

    if(first)
    {
        checkForDiskRead();
        first = false;
    }
    if(diskread)
    {
        diskread->ready();
        return;
    }

    grouped = outputMeta.isGrouped();
    unsigned lenData;
    void * tempData;
    OwnedRoxieString fromWuid(helper.getWUID());
    ICsvToRowTransformer * csvTransformer = helper.queryCsvTransformer();
    IXmlToRowTransformer * xmlTransformer = helper.queryXmlTransformer();
    if (fromWuid)
        agent.queryCodeContext()->getExternalResultRaw(lenData, tempData, fromWuid, helper.queryName(), helper.querySequence(), xmlTransformer, csvTransformer);
    else
        agent.queryCodeContext()->getResultRaw(lenData, tempData, helper.queryName(), helper.querySequence(), xmlTransformer, csvTransformer);
    resultBuffer.setBuffer(lenData, tempData, true);
    eogPending = false;
}

void CHThorWorkunitReadActivity::checkForDiskRead()
{
    StringBuffer diskFilename;
    OwnedRoxieString fromWuid(helper.getWUID());
    if (agent.getWorkunitResultFilename(diskFilename, fromWuid, helper.queryName(), helper.querySequence()))
    {
        diskreadHelper.setown(createWorkUnitReadArg(diskFilename.str(), &helper));
        try
        {
            diskreadHelper->onCreate(agent.queryCodeContext(), NULL, NULL);
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
        diskread.setown(new CHThorDiskReadActivity(agent, activityId, subgraphId, *diskreadHelper, TAKdiskread));
    }
}

void CHThorWorkunitReadActivity::stop()
{
    if(diskread)
        diskread->stop();
    resultBuffer.resetBuffer();
    CHThorSimpleActivityBase::stop();
}


const void *CHThorWorkunitReadActivity::nextRow()
{
    if(diskread)
    {
        const void * ret = diskread->nextRow();
        processed = diskread->queryProcessed();
        return ret;
    }
    if (deserializer.eos()) 
        return NULL;                    

    if (eogPending)
    {
        eogPending = false;
        return NULL;
    }
    RtlDynamicRowBuilder rowBuilder(rowAllocator);
    size32_t newSize = rowDeserializer->deserialize(rowBuilder, deserializer);
    
    if (grouped)
        deserializer.read(sizeof(bool), &eogPending);

    processed++;
    return rowBuilder.finalizeRowClear(newSize);                
}

//=====================================================================================================

CHThorParseActivity::CHThorParseActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorParseArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    //DebugBreak();
    anyThisGroup = false;
    curSearchTextLen = 0;
    curSearchText = NULL;

    algorithm = createThorParser(agent.queryCodeContext(), helper);
    parser = algorithm->createParser(agent.queryCodeContext(), activityId, helper.queryHelper(), &helper);
    rowIter = parser->queryResultIter();
}

CHThorParseActivity::~CHThorParseActivity()
{
    if (curSearchText && helper.searchTextNeedsFree())
        rtlFree(curSearchText);
    parser->Release();
    algorithm->Release();
}

void CHThorParseActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    anyThisGroup = false;
    parser->reset();
}

void CHThorParseActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    if (curSearchText && helper.searchTextNeedsFree())
        rtlFree(curSearchText);
    curSearchText = NULL;
    in.clear();
}

bool CHThorParseActivity::processRecord(const void * in)
{
    if (curSearchText && helper.searchTextNeedsFree())
        rtlFree(curSearchText);

    curSearchTextLen = 0;
    curSearchText = NULL;
    helper.getSearchText(curSearchTextLen, curSearchText, in);

    return parser->performMatch(*this, in, curSearchTextLen, curSearchText);
}


unsigned CHThorParseActivity::onMatch(ARowBuilder & self, const void * curRecord, IMatchedResults * results, IMatchWalker * walker)
{
    try
    {
        return helper.transform(self, curRecord, results, walker);
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}


const void * CHThorParseActivity::nextRow()
{
    for (;;)
    {
        if (rowIter->isValid())
        {
            anyThisGroup = true;
            OwnedConstRoxieRow out = rowIter->getRow();
            rowIter->next();
            processed++;
            return out.getClear();
        }

        in.setown(input->nextRow());
        if (!in)
        {
            if (anyThisGroup)
            {
                anyThisGroup = false;
                return NULL;
            }
            in.setown(input->nextRow());
            if (!in)
                return NULL;
        }

        processRecord(in);
        rowIter->first();
    }
}

//=====================================================================================================

CHThorEnthActivity::CHThorEnthActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorEnthArg & _arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), outBuilder(NULL)
{
}

void CHThorEnthActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    outBuilder.setAllocator(rowAllocator);
    numerator = helper.getProportionNumerator();
    denominator = helper.getProportionDenominator();
    started = false;
}

void CHThorEnthActivity::stop()
{
    outBuilder.clear();
}

void CHThorEnthActivity::start()
{
    if(denominator == 0) denominator = 1;
    counter = (helper.getSampleNumber()-1) * greatestCommonDivisor(numerator, denominator);
    if (counter >= denominator)
        counter %= denominator;
    started = true;
}

const void * CHThorEnthActivity::nextRow()
{
    if(!started)
        start();
    OwnedConstRoxieRow ret;
    for (;;)
    {
        ret.setown(input->nextRow());
        if(!ret) //end of group
            ret.setown(input->nextRow());
        if(!ret) //eof
            return NULL;
        if (wanted())
        {
            processed++;
            return ret.getClear();
        }
    }
}

//=====================================================================================================

CHThorTopNActivity::CHThorTopNActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorTopNArg & _arg, ThorActivityKind _kind)
    : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), compare(*helper.queryCompare())
{
    hasBest = helper.hasBest();
    grouped = outputMeta.isGrouped();
    curIndex = 0;
    sortedCount = 0;
    limit = 0;
    sorted = NULL;
}

CHThorTopNActivity::~CHThorTopNActivity()
{
    roxiemem::ReleaseRoxieRowRange(sorted, curIndex, sortedCount);
    free(sorted);
}

void CHThorTopNActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    limit = helper.getLimit();
    assertex(limit == (size_t)limit);
    sorted = (const void * *)checked_calloc((size_t)(limit+1), sizeof(void *), "topn");
    sortedCount = 0;
    curIndex = 0;
    eof = false;
    eoi = false;
}

void CHThorTopNActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    roxiemem::ReleaseRoxieRowRange(sorted, curIndex, sortedCount);
    free(sorted);
    sorted = NULL;
    curIndex = 0;
    sortedCount = 0;
}

const void * CHThorTopNActivity::nextRow()
{
    if(eof)
        return NULL;
    if(curIndex >= sortedCount)
    {
        bool eog = sortedCount != 0;
        getSorted();
        if(sortedCount == 0)
        {
            eof = true;
            return NULL;
        }
        if (eog)
            return NULL;
    }
    processed++;
    return sorted[curIndex++];
}

bool CHThorTopNActivity::abortEarly()
{
    if (hasBest && (sortedCount == limit))
    {
        int compare = helper.compareBest(sorted[sortedCount-1]);
        if (compare == 0)
        {
            if (grouped)
            {
                //MORE: This would be more efficient if we had a away of skipping to the end of the incomming group.
                OwnedConstRoxieRow next;
                do
                {
                    next.setown(input->nextRow());
                } while(next);
            }
            else
                eoi = true;
            return true;
        }

        //This only checks the lowest element - we could check all elements inserted, but it would increase the number of compares
        if (compare < 0)
            throw MakeStringException(0, "TOPN: row found that exceeds the best value");
    }
    return false;
}


void CHThorTopNActivity::getSorted()
{
    curIndex = 0;
    sortedCount = 0;

    if (eoi)
        return;

    OwnedConstRoxieRow next(input->nextRow());
    while(next)
    {
        if(sortedCount < limit)
        {
            binary_vec_insert_stable(next.getClear(), sorted, sortedCount, compare);
            sortedCount++;
            if (abortEarly())
                return;
        }
        else
        {
            // do not bother with insertion sort if we know next will fall off the end
            if(limit && compare.docompare(sorted[sortedCount-1], next) > 0)
            {
                binary_vec_insert_stable(next.getClear(), sorted, sortedCount, compare);
                ReleaseRoxieRow(sorted[sortedCount]);
                if (abortEarly())
                    return;
            }
        }
        next.setown(input->nextRow());
    }
}

//=====================================================================================================

CHThorXmlParseActivity::CHThorXmlParseActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorXmlParseArg & _arg, ThorActivityKind _kind)
    : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    srchStrNeedsFree = helper.searchTextNeedsFree();
    srchStr = NULL;
}

CHThorXmlParseActivity::~CHThorXmlParseActivity()
{
    if(srchStrNeedsFree) rtlFree(srchStr);
}

void CHThorXmlParseActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    numProcessedLastGroup = processed;
}

void CHThorXmlParseActivity::stop()
{
    CHThorSimpleActivityBase::stop();
    if(srchStrNeedsFree) rtlFree(srchStr);
    srchStr = NULL;
    in.clear();
}


const void * CHThorXmlParseActivity::nextRow()
{
    for (;;)
    {
        if(xmlParser)
        {
            for (;;)
            {
                bool gotNext = false;
                try
                {
                    gotNext = xmlParser->next();
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
                if(!gotNext)
                {
                    if(srchStrNeedsFree)
                    {
                        rtlFree(srchStr);
                        srchStr = NULL;
                    }
                    xmlParser.clear();
                    break;
                }
                if(lastMatch)
                {
                    try
                    {
                        RtlDynamicRowBuilder rowBuilder(rowAllocator);
                        unsigned sizeGot = helper.transform(rowBuilder, in, lastMatch);
                        lastMatch.clear();
                        if (sizeGot)
                        {
                            processed++;
                            return rowBuilder.finalizeRowClear(sizeGot);
                        }
                    }
                    catch(IException * e)
                    {
                        throw makeWrappedException(e);
                    }
                }
            }
        }
        in.setown(input->nextRow());
        if(!in)
        {
            if(numProcessedLastGroup == processed)
                in.setown(input->nextRow());
            if(!in)
            {
                numProcessedLastGroup = processed;
                return NULL;
            }
        }
        size32_t srchLen;
        helper.getSearchText(srchLen, srchStr, in);
        OwnedRoxieString xmlIteratorPath(helper.getXmlIteratorPath());
        xmlParser.setown(createXMLParse(srchStr, srchLen, xmlIteratorPath, *this, ptr_noRoot, helper.requiresContents()));
    }
}

//=====================================================================================================

class CHThorMergeActivity : public CHThorMultiInputActivity
{
protected:
    IHThorMergeArg &helper;
    CHThorStreamMerger merger;

public:
    CHThorMergeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorMergeArg &_arg, ThorActivityKind _kind) : CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
    {
        merger.init(helper.queryCompare(), helper.dedup(), NULL);       // can mass null for range because merger.nextGE() never called
    }

    ~CHThorMergeActivity()
    {
        merger.cleanup();
    }

    virtual void ready()
    {
        CHThorMultiInputActivity::ready();
        merger.initInputs(inputs.length(), inputs.getArray());
    }

    virtual void stop() 
    {
        merger.done();
        CHThorMultiInputActivity::stop(); 
    }

    virtual const void * nextRow()
    {
        const void * ret = merger.nextRow();
        if (ret)
            processed++;
        return ret;
    }
};

//=====================================================================================================
//Web Service Call base
CHThorWSCBaseActivity::CHThorWSCBaseActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorWebServiceCallArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    callHelper = &_arg;
    init();
}

CHThorWSCBaseActivity::CHThorWSCBaseActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorWebServiceCallActionArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    callHelper = NULL;
    init();
}

void CHThorWSCBaseActivity::stop()
{
    WSChelper.clear();//doesn't return until helper threads terminate
    CHThorSimpleActivityBase::stop();
}

void CHThorWSCBaseActivity::init()
{
    // Build authentication token
    StringBuffer uidpair;
    IUserDescriptor *userDesc = agent.queryCodeContext()->queryUserDescriptor();
    if (userDesc)//NULL if standalone
    {
        userDesc->getUserName(uidpair);
        uidpair.append(":");
        userDesc->getPassword(uidpair);
        JBASE64_Encode(uidpair.str(), uidpair.length(), authToken);
    }
    soapTraceLevel = agent.queryWorkUnit()->getDebugValueInt("soapTraceLevel", 1);
}

//---------------------------------------------------------------------------

CHThorWSCRowCallActivity::CHThorWSCRowCallActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorWebServiceCallArg &_arg, ThorActivityKind _kind) : CHThorWSCBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

const void *CHThorWSCRowCallActivity::nextRow()
{
    try
    {
        assertex(WSChelper);
        OwnedConstRoxieRow ret = WSChelper->getRow();
        if (!ret)
            return NULL;
        ++processed;
        return ret.getClear();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

//---------------------------------------------------------------------------

const void *CHThorHttpRowCallActivity::nextRow()
{
    try
    {
        if (WSChelper == NULL)
        {
            WSChelper.setown(createHttpCallHelper(this, rowAllocator, authToken.str(), SCrow, NULL, queryDummyContextLogger(),NULL));
            WSChelper->start();
        }
        return CHThorWSCRowCallActivity::nextRow();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

//---------------------------------------------------------------------------

const void *CHThorSoapRowCallActivity::nextRow()
{
    try
    {
        if (WSChelper == NULL)
        {
            WSChelper.setown(createSoapCallHelper(this, rowAllocator, authToken.str(), SCrow, NULL, queryDummyContextLogger(),NULL));
            WSChelper->start();
        }
        return CHThorWSCRowCallActivity::nextRow();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------

CHThorSoapRowActionActivity::CHThorSoapRowActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapActionArg &_arg, ThorActivityKind _kind) : CHThorWSCBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

void CHThorSoapRowActionActivity::execute()
{
    try
    {
        WSChelper.setown(createSoapCallHelper(this, NULL, authToken.str(), SCrow, NULL, queryDummyContextLogger(),NULL));
        WSChelper->start();
        WSChelper->waitUntilDone();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
    IException *e = WSChelper->getError();
    if(e)
        throw makeWrappedException(e);
}

//---------------------------------------------------------------------------

CHThorSoapDatasetCallActivity::CHThorSoapDatasetCallActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapCallArg &_arg, ThorActivityKind _kind) : CHThorWSCBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{
}


const void * CHThorSoapDatasetCallActivity::nextRow()
{
    try
    {
        if (WSChelper == NULL)
        {
            WSChelper.setown(createSoapCallHelper(this, rowAllocator, authToken.str(), SCdataset, NULL, queryDummyContextLogger(),NULL));
            WSChelper->start();
        }
        OwnedConstRoxieRow ret = WSChelper->getRow();
        if (!ret)
            return NULL;
        ++processed;
        return ret.getClear();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

const void * CHThorSoapDatasetCallActivity::getNextRow()
{
    CriticalBlock b(crit);

    const void *nextrec = input->nextRow();
    if (!nextrec)
    {
        nextrec = input->nextRow();
    }

    return nextrec;
};

//---------------------------------------------------------------------------

CHThorSoapDatasetActionActivity::CHThorSoapDatasetActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapActionArg &_arg, ThorActivityKind _kind) : CHThorWSCBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

void CHThorSoapDatasetActionActivity::execute()
{
    try
    {
        WSChelper.setown(createSoapCallHelper(this, NULL, authToken.str(), SCdataset, NULL, queryDummyContextLogger(),NULL));
        WSChelper->start();
        WSChelper->waitUntilDone();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
    IException *e = WSChelper->getError();
    if(e)
        throw makeWrappedException(e);
}

const void * CHThorSoapDatasetActionActivity::getNextRow()
{
    CriticalBlock b(crit);

    const void *nextrec = input->nextRow();
    if (!nextrec)
    {
        nextrec = input->nextRow();
    }
    if (nextrec)
    {
        processed++;
    }

    return nextrec;
};

//=====================================================================================================

CHThorResultActivity::CHThorResultActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind)
 : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

void CHThorResultActivity::extractResult(unsigned & retSize, void * & ret)
{
    unsigned len = rowdata.length();
    retSize = len;
    if (len)
    {
        void * temp = rtlMalloc(len);
        memcpy(temp, rowdata.toByteArray(), len);
        ret = temp;
    }
    else
        ret = NULL;
}

//=====================================================================================================

CHThorDatasetResultActivity::CHThorDatasetResultActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDatasetResultArg &_arg, ThorActivityKind _kind)
 : CHThorResultActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorDatasetResultActivity::execute()
{
    rowdata.clear();
    IRecordSize * inputMeta = input->queryOutputMeta();
    for (;;)
    {
        OwnedConstRoxieRow nextrec(input->nextRow());
        if (!nextrec)
        {
            nextrec.setown(input->nextRow());
            if (!nextrec)
                break;
        }
        rowdata.append(inputMeta->getRecordSize(nextrec), nextrec);
    }
}


//=====================================================================================================

CHThorRowResultActivity::CHThorRowResultActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorRowResultArg &_arg, ThorActivityKind _kind)
 : CHThorResultActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorRowResultActivity::execute()
{
    OwnedConstRoxieRow nextrec(input->nextRow());
    assertex(nextrec);
    IRecordSize * inputMeta = input->queryOutputMeta();
    unsigned length = inputMeta->getRecordSize(nextrec);
    rowdata.clear().append(length, nextrec);
}

//=====================================================================================================

CHThorChildIteratorActivity::CHThorChildIteratorActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildIteratorArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}


const void *CHThorChildIteratorActivity::nextRow()
{
    if (eof)
        return NULL;

    bool ok;
    if (!started)
    {
        ok = helper.first();
        started = true;
    }
    else
        ok = helper.next();

    try
    {
        while(ok)
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transform(rowBuilder);
            if(outSize)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
            ok = helper.next();
        }
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }

    eof = true;
    return NULL;
}

void CHThorChildIteratorActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    started = false;
    eof = false;
}

//=====================================================================================================

CHThorLinkedRawIteratorActivity::CHThorLinkedRawIteratorActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLinkedRawIteratorArg &_arg, ThorActivityKind _kind) 
    : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

const void *CHThorLinkedRawIteratorActivity::nextRow()
{
    const void *ret =helper.next();
    if (ret)
    {
        LinkRoxieRow(ret);
        processed++;
    }
    return ret;
}
//=====================================================================================================

//=====================================================================================================
//== New implementations - none are currently used, created or tested =================================
//=====================================================================================================

CHThorChildNormalizeActivity::CHThorChildNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildNormalizeArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}


const void *CHThorChildNormalizeActivity::nextRow()
{
    if (eof)
        return NULL;

    bool ok;
    if (!started)
    {
        ok = helper.first();
        started = true;
    }
    else
        ok = helper.next();

    try
    {
        if (ok)
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            do {
                unsigned thisSize = helper.transform(rowBuilder);
                if (thisSize)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(thisSize);
                }
                ok = helper.next();
            }
            while (ok);
        }
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }

    eof = true;
    return NULL;
}

void CHThorChildNormalizeActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    started = false;
    eof = false;
}

//=====================================================================================================

CHThorChildAggregateActivity::CHThorChildAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildAggregateArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}


const void *CHThorChildAggregateActivity::nextRow()
{
    if (eof)
        return NULL;

    eof = true;
    processed++;
    try
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        helper.clearAggregate(rowBuilder);
        helper.processRows(rowBuilder);
        size32_t finalSize = outputMeta.getRecordSize(rowBuilder.getSelf());
        return rowBuilder.finalizeRowClear(finalSize);
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

void CHThorChildAggregateActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    eof = false;
}

//=====================================================================================================

CHThorChildGroupAggregateActivity::CHThorChildGroupAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildGroupAggregateArg &_arg, ThorActivityKind _kind) 
  : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), 
    helper(_arg), 
    aggregated(_arg, _arg)
{
}

void CHThorChildGroupAggregateActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    eof = false;
    gathered = false;
    aggregated.start(rowAllocator);
}

void CHThorChildGroupAggregateActivity::stop()
{
    aggregated.reset();
    CHThorSimpleActivityBase::stop();
}


void CHThorChildGroupAggregateActivity::processRow(const void * next)
{
    aggregated.addRow(next);
}
        

const void * CHThorChildGroupAggregateActivity::nextRow()
{
    if (eof)
        return NULL;

    if (!gathered)
    {
        helper.processRows(this);
        gathered = true;
    }

    Owned<AggregateRowBuilder> next = aggregated.nextResult();
    if (next)
    {
        processed++;
        return next->finalizeRowClear();
    }
    eof = true;
    return NULL;
}


//=====================================================================================================

CHThorChildThroughNormalizeActivity::CHThorChildThroughNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildThroughNormalizeArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), outBuilder(NULL)
{
}

void CHThorChildThroughNormalizeActivity::stop()
{
    outBuilder.clear();
    lastInput.clear();
    CHThorSimpleActivityBase::stop();
}

void CHThorChildThroughNormalizeActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    outBuilder.setAllocator(rowAllocator);
    numProcessedLastGroup = processed;
    ok = false;
}


const void *CHThorChildThroughNormalizeActivity::nextRow()
{
    try
    {
        for (;;)
        {
            if (ok)
                ok = helper.next();

            while (!ok)
            {
                lastInput.setown(input->nextRow());
                if (!lastInput)
                {
                    if (numProcessedLastGroup != processed)
                    {
                        numProcessedLastGroup = processed;
                        return NULL;
                    }
                    lastInput.setown(input->nextRow());
                    if (!lastInput)
                        return NULL;
                }

                ok = helper.first(lastInput);
            }
            
            outBuilder.ensureRow();
            do 
            {
                size32_t thisSize = helper.transform(outBuilder);
                if (thisSize)
                {
                    processed++;
                    return outBuilder.finalizeRowClear(thisSize);
                }
                ok = helper.next();
            } while (ok);
        }
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

//=====================================================================================================

CHThorDiskReadBaseActivity::CHThorDiskReadBaseActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskReadBaseArg &_arg, ThorActivityKind _kind) : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    helper.setCallback(this);
}

CHThorDiskReadBaseActivity::~CHThorDiskReadBaseActivity()
{
    close();
}

void CHThorDiskReadBaseActivity::ready()        
{ 
    CHThorActivityBase::ready(); 

    grouped = false;
    recordsize = 0;
    fixedDiskRecordSize = 0;
    eofseen = false;
    opened = false;
    compressed = false;
    rowcompressed = false;
    blockcompressed = false;
    persistent = false;
    localOffset = 0;
    offsetOfPart = 0;
    partNum = (unsigned)-1;

    resolve();
}

void CHThorDiskReadBaseActivity::stop()
{
    close();
    CHThorActivityBase::stop();
}

void CHThorDiskReadBaseActivity::resolve()
{
    OwnedRoxieString fileName(helper.getFileName());
    mangleHelperFileName(mangledHelperFileName, fileName, agent.queryWuid(), helper.getFlags());
    if (helper.getFlags() & (TDXtemporary | TDXjobtemp))
    {
        StringBuffer mangledFilename;
        mangleLocalTempFilename(mangledFilename, mangledHelperFileName.str());
        tempFileName.set(agent.queryTemporaryFile(mangledFilename.str()));
        logicalFileName.set(tempFileName);
        gatherInfo(NULL);
    }
    else
    {
        ldFile.setown(agent.resolveLFN(mangledHelperFileName.str(), "Read", 0 != (helper.getFlags() & TDRoptional)));
        if ( mangledHelperFileName.charAt(0) == '~')
            logicalFileName.set(mangledHelperFileName.str()+1);
        else
            logicalFileName.set(mangledHelperFileName.str());

        if (ldFile)
        {
            Owned<IFileDescriptor> fdesc;
            fdesc.setown(ldFile->getFileDescriptor());
            gatherInfo(fdesc);
            IDistributedFile *dFile = ldFile->queryDistributedFile();
            if (dFile)  //only makes sense for distributed (non local) files
            {
                persistent = dFile->queryAttributes().getPropBool("@persistent");
                dfsParts.setown(dFile->getIterator());
                if (helper.getFlags() & TDRfilenamecallback)
                {
                    IDistributedSuperFile *super = dFile->querySuperFile();
                    if (super)
                    {
                        unsigned numsubs = super->numSubFiles(true);
                        unsigned s=0;
                        for (; s<numsubs; s++)
                        {
                            IDistributedFile &subfile = super->querySubFile(s, true);
                            subfileLogicalFilenames.append(subfile.queryLogicalName());
                        }
                        assertex(fdesc);
                        superfile.set(fdesc->querySuperFileDescriptor());
                        if (!superfile && numsubs>0)
                            logicalFileName.set(subfileLogicalFilenames.item(0));

                    }
                }
                if((helper.getFlags() & (TDXtemporary | TDXjobtemp)) == 0)
                    agent.logFileAccess(dFile, "HThor", "READ");
                if(!agent.queryWorkUnit()->getDebugValueBool("skipFileFormatCrcCheck", false) && !(helper.getFlags() & TDRnocrccheck))
                    verifyRecordFormatCrc();
            }
        }
        if (!ldFile)
        {
            StringBuffer buff;
            buff.appendf("Input file '%s' was missing but declared optional", mangledHelperFileName.str());
            WARNLOG("%s", buff.str());
            agent.addWuException(buff.str(), WRN_SkipMissingOptFile, SeverityInformation, "hthor");
        }
    }
}

void CHThorDiskReadBaseActivity::gatherInfo(IFileDescriptor * fileDesc)
{
    if(fileDesc)
    {
        if (!agent.queryResolveFilesLocally())
        {
            grouped = fileDesc->isGrouped();
            if(grouped != ((helper.getFlags() & TDXgrouped) != 0))
            {
                StringBuffer msg;
                msg.append("DFS and code generated group info. differs: DFS(").append(grouped ? "grouped" : "ungrouped").append("), CodeGen(").append(grouped ? "ungrouped" : "grouped").append("), using DFS info");
                WARNLOG("%s", msg.str());
                agent.addWuException(msg.str(), WRN_MismatchGroupInfo, SeverityError, "hthor");
            }
        }
        else
            grouped = ((helper.getFlags() & TDXgrouped) != 0);
    }
    else
    {
        grouped = ((helper.getFlags() & TDXgrouped) != 0);
    }

    diskMeta.set(helper.queryDiskRecordSize()->querySerializedDiskMeta());
    if (grouped)
        diskMeta.setown(new CSuffixedOutputMeta(+1, diskMeta));
    if (outputMeta.isFixedSize())
    {
        recordsize = outputMeta.getFixedSize();
        if (grouped)
            recordsize++;
    }
    else
        recordsize = 0;
    calcFixedDiskRecordSize();

    if(fileDesc)
    {
        compressed = fileDesc->isCompressed(&blockcompressed); //try new decompression, fall back to old unless marked as block
        if(fixedDiskRecordSize)
        {
            size32_t dfsSize = fileDesc->queryProperties().getPropInt("@recordSize");
            if(!((dfsSize == 0) || (dfsSize == fixedDiskRecordSize) || (grouped && (dfsSize+1 == fixedDiskRecordSize)))) //third option for backwards compatibility, as hthor used to publish @recordSize not including the grouping byte
                throw MakeStringException(0, "Published record size %d for file %s does not match coded record size %d", dfsSize, mangledHelperFileName.str(), fixedDiskRecordSize);
            if (!compressed && (((helper.getFlags() & TDXcompress) != 0) && (fixedDiskRecordSize >= MIN_ROWCOMPRESS_RECSIZE)))
            {
                StringBuffer msg;
                msg.append("Ignoring compression attribute on file ").append(mangledHelperFileName.str()).append(", which is not published as compressed");
                WARNLOG("%s", msg.str());
                agent.addWuException(msg.str(), WRN_MismatchCompressInfo, SeverityWarning, "hthor");
                compressed = true;
            }
        }
    }
    else
    {
        compressed = checkIsCompressed(helper.getFlags(), fixedDiskRecordSize, false);//grouped=FALSE because fixedDiskRecordSize already includes grouped
    }
    void *k;
    size32_t kl;
    helper.getEncryptKey(kl,k);
    encryptionkey.setOwn(kl,k);

    if (encryptionkey.length()!=0) 
    {
        blockcompressed = true;
        compressed = true;
    }
}

void CHThorDiskReadBaseActivity::close()
{
    closepart();
    tempFileName.clear();
    dfsParts.clear();
    if(ldFile)
    {
        IDistributedFile * dFile = ldFile->queryDistributedFile();
        if(dFile)
            dFile->setAccessed();
        ldFile.clear();
    }
}

unsigned __int64 CHThorDiskReadBaseActivity::getFilePosition(const void * row)
{
    return localOffset + offsetOfPart;
}

unsigned __int64 CHThorDiskReadBaseActivity::getLocalFilePosition(const void * row)
{
    return makeLocalFposOffset(partNum-1, localOffset);
}

void CHThorDiskReadBaseActivity::closepart()
{
    inputstream.clear();
    inputfileio.clear();
    inputfile.clear();
}

bool CHThorDiskReadBaseActivity::openNext()
{
    offsetOfPart += localOffset;
    localOffset = 0;
    saveOpenExc.clear();

    if (dfsParts||ldFile)
    {
        // open next part of a multipart, if there is one
        while ((dfsParts&&dfsParts->isValid())||
              (!dfsParts&&(partNum<ldFile->numParts())))
        {
            IDistributedFilePart * curPart = dfsParts?&dfsParts->query():NULL;
            unsigned numCopies = curPart?curPart->numCopies():ldFile->numPartCopies(partNum);
            //MORE: Order of copies should be optimized at this point....
            StringBuffer file, filelist;
            closepart();
            if (dfsParts && superfile && curPart)
            {
                unsigned subfile;
                unsigned lnum;
                if (superfile->mapSubPart(partNum, subfile, lnum))
                    logicalFileName.set(subfileLogicalFilenames.item(subfile));
            }
            for (unsigned copy=0; copy < numCopies; copy++)
            {
                RemoteFilename rfilename;
                if (curPart)
                    curPart->getFilename(rfilename,copy);
                else
                    ldFile->getPartFilename(rfilename,partNum,copy);
                rfilename.getPath(file.clear());
                filelist.append('\n').append(file);
                try
                {
                    inputfile.setown(createIFile(rfilename));   
                    if(compressed)
                    {
                        Owned<IExpander> eexp;
                        if (encryptionkey.length()!=0) 
                            eexp.setown(createAESExpander256((size32_t)encryptionkey.length(),encryptionkey.bufferBase()));
                        inputfileio.setown(createCompressedFileReader(inputfile,eexp));
                        if(!inputfileio && !blockcompressed) //fall back to old decompression, unless dfs marked as new
                        {
                            inputfileio.setown(inputfile->open(IFOread));
                            if(inputfileio)
                                rowcompressed = true;
                        }
                    }
                    else
                        inputfileio.setown(inputfile->open(IFOread));
                    if (inputfileio)
                        break;
                }
                catch (IException *E)
                {
                    if (saveOpenExc.get())
                        E->Release();
                    else
                        saveOpenExc.setown(E);
                }
                closepart();
            }

            if (dfsParts)
                dfsParts->next();
            partNum++;
            if (checkOpenedFile(file.str(), filelist.str()))
            {
                opened = true;
                return true;
            }
        }
        return false;
    }
    else if (!tempFileName.isEmpty())
    {
        StringBuffer file(tempFileName.get());
        tempFileName.clear();
        closepart();
        try
        {
            inputfile.setown(createIFile(file.str()));
            if(compressed)
            {
                Owned<IExpander> eexp;
                if (encryptionkey.length()) 
                    eexp.setown(createAESExpander256((size32_t) encryptionkey.length(),encryptionkey.bufferBase()));
                inputfileio.setown(createCompressedFileReader(inputfile,eexp));
                if(!inputfileio && !blockcompressed) //fall back to old decompression, unless dfs marked as new
                {
                    inputfileio.setown(inputfile->open(IFOread));
                    if(inputfileio)
                        rowcompressed = true;
                }
            }
            else
                inputfileio.setown(inputfile->open(IFOread));
        }
        catch (IException *E)
        {
            closepart();
            StringBuffer msg;
            WARNLOG("%s", E->errorMessage(msg).str());
            if (saveOpenExc.get())
                E->Release();
            else
                saveOpenExc.setown(E);
        }
        partNum++;
        if (checkOpenedFile(file.str(), NULL))
        {
            opened = true;
            return true;
        }
    }
    return false;
}

bool CHThorDiskReadBaseActivity::checkOpenedFile(char const * filename, char const * filenamelist)
{
    unsigned __int64 filesize = 0;
    if (!inputfileio) 
    {
        if (!(helper.getFlags() & TDRoptional))
        {
            StringBuffer s;
            if(filenamelist) {
                if (saveOpenExc.get())
                {
                    if (strstr(mangledHelperFileName.str(),"::>")!=NULL) // if a 'special' filename just use saved exception 
                        saveOpenExc->errorMessage(s);
                    else 
                    {
                        s.append("Could not open logical file ").append(mangledHelperFileName.str()).append(" in any of these locations:").append(filenamelist).append(" (");
                        saveOpenExc->errorMessage(s).append(")");
                    }
                }
                else
                    s.append("Could not open logical file ").append(mangledHelperFileName.str()).append(" in any of these locations:").append(filenamelist).append(" (").append((unsigned)GetLastError()).append(")");
            }
            else
                s.append("Could not open local physical file ").append(filename).append(" (").append((unsigned)GetLastError()).append(")");
            agent.fail(1, s.str());
        }
    }
    else
        filesize = inputfileio->size();
    saveOpenExc.clear();
    if (filesize)
    {
        if (!compressed && fixedDiskRecordSize && (filesize % fixedDiskRecordSize) != 0)
        {
            StringBuffer s;
            s.append("File ").append(filename).append(" size is ").append(filesize).append(" which is not a multiple of ").append(fixedDiskRecordSize);
            agent.fail(1, s.str());
        }

        unsigned readBufferSize = queryReadBufferSize();
        inputstream.setown(createFileSerialStream(inputfileio, 0, filesize, readBufferSize));

        StringBuffer report("Reading file ");
        report.append(inputfile->queryFilename());
        agent.reportProgress(report.str());
    }

    return (filesize != 0);
}

void CHThorDiskReadBaseActivity::open()
{
    assertex(!opened);
    partNum = 0;
    if (dfsParts)
        eofseen = !dfsParts->first() || !openNext();
    else if (ldFile||tempFileName.length())
        eofseen = !openNext();
    else
        eofseen = true;
    opened = true;
}

//=====================================================================================================

CHThorBinaryDiskReadBase::CHThorBinaryDiskReadBase(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskReadBaseArg &_arg, IHThorCompoundBaseArg & _segHelper, ThorActivityKind _kind)
: CHThorDiskReadBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind), segHelper(_segHelper), prefetchBuffer(NULL), recInfo(outputMeta.queryOriginal()->queryRecordAccessor(true)),rowInfo(recInfo)
{
}

void CHThorBinaryDiskReadBase::calcFixedDiskRecordSize()
{
    fixedDiskRecordSize = diskMeta->getFixedSize();
}

void CHThorBinaryDiskReadBase::append(IKeySegmentMonitor *segment)
{
    if (segment->isWild())
        segment->Release();
    else
    {
        segMonitors.append(*segment);
        if (segment->numFieldsRequired() > numFieldsRequired)
            numFieldsRequired = segment->numFieldsRequired();
    }
}

void CHThorBinaryDiskReadBase::setMergeBarrier(unsigned barrierOffset)
{
    // nothing to do - we don't merge...
}

unsigned CHThorBinaryDiskReadBase::ordinality() const
{
    return segMonitors.length();
}

IKeySegmentMonitor *CHThorBinaryDiskReadBase::item(unsigned idx) const
{
    if (segMonitors.isItem(idx))
        return &segMonitors.item(idx);
    else
        return NULL;
}

void CHThorBinaryDiskReadBase::ready()      
{ 
    CHThorDiskReadBaseActivity::ready(); 
    if (!diskMeta)
        diskMeta.set(outputMeta);
    segMonitors.kill();
    numFieldsRequired = 0;
    segHelper.createSegmentMonitors(this);
    prefetcher.setown(diskMeta->createDiskPrefetcher(agent.queryCodeContext(), activityId));
    deserializer.setown(diskMeta->createDiskDeserializer(agent.queryCodeContext(), activityId));
}

bool CHThorBinaryDiskReadBase::openNext()
{
    if (CHThorDiskReadBaseActivity::openNext())
    {
        if(rowcompressed && fixedDiskRecordSize)
        {
            throwUnexpected();
            //MORE: What happens here
            PROGLOG("Disk read falling back to legacy decompression routine");
            //in.setown(createRowCompReadSeq(*inputfileiostream, 0, fixedDiskRecordSize));
        }

        //Only one of these will actually be used.
        prefetchBuffer.setStream(inputstream);
        deserializeSource.setStream(inputstream);
        return true;
    }
    return false;
}

void CHThorBinaryDiskReadBase::closepart()
{
    prefetchBuffer.clearStream();
    deserializeSource.clearStream();
    CHThorDiskReadBaseActivity::closepart();
}

unsigned CHThorBinaryDiskReadBase::queryReadBufferSize()
{
    return hthorReadBufferSize;
}

void CHThorBinaryDiskReadBase::open()
{
    if (!segHelper.canMatchAny())
    {
        eofseen = true;
        opened = true;
    }
    else
        CHThorDiskReadBaseActivity::open();
}


//=====================================================================================================

CHThorDiskReadActivity::CHThorDiskReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskReadArg &_arg, ThorActivityKind _kind) : CHThorBinaryDiskReadBase(_agent, _activityId, _subgraphId, _arg, _arg, _kind), helper(_arg), outBuilder(NULL)
{
    needTransform = false;
    eogPending = 0;
    lastGroupProcessed = 0;
}

void CHThorDiskReadActivity::ready()        
{ 
    PARENT::ready(); 
    outBuilder.setAllocator(rowAllocator);
    eogPending = false;
    lastGroupProcessed = processed;
    needTransform = helper.needTransform() || segMonitors.length();
    limit = helper.getRowLimit();
    if (helper.getFlags() & TDRlimitskips)
        limit = (unsigned __int64) -1;
    stopAfter = helper.getChooseNLimit();
}


void CHThorDiskReadActivity::stop()
{ 
    outBuilder.clear();
    PARENT::stop(); 
}


const void *CHThorDiskReadActivity::nextRow()
{
    if (!opened) open();
    if (eogPending && (lastGroupProcessed != processed))
    {
        eogPending = false;
        lastGroupProcessed = processed;
        return NULL;
    }

    try
    {
        if (needTransform || grouped)
        {
            while (!eofseen && ((stopAfter == 0) || ((processed - initialProcessed) < stopAfter)))
            {
                queryUpdateProgress();
                while (!prefetchBuffer.eos())
                {
                    queryUpdateProgress();

                    prefetcher->readAhead(prefetchBuffer);
                    const byte * next = prefetchBuffer.queryRow();
                    size32_t sizeRead = prefetchBuffer.queryRowSize();
                    size32_t thisSize;
                    if (segMonitorsMatch(next))
                    {
                        thisSize = helper.transform(outBuilder.ensureRow(), next);
                    }
                    else
                        thisSize = 0;

                    bool eog = grouped && next[sizeRead-1];
                    prefetchBuffer.finishedRow();

                    localOffset += sizeRead;
                    if (thisSize)
                    {
                        if (grouped)
                            eogPending = eog;
                        if ((processed - initialProcessed) >=limit)
                        {
                            outBuilder.clear();
                            if ( agent.queryCodeContext()->queryDebugContext())
                                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                            helper.onLimitExceeded();
                            return NULL;
                        }
                        processed++;
                        return outBuilder.finalizeRowClear(thisSize);
                    }
                    if (eog && (lastGroupProcessed != processed))
                    {
                        lastGroupProcessed = processed;
                        return NULL;
                    }
                }
                eofseen = !openNext();
            }
        }
        else
        {
            assertex(outputMeta == diskMeta);
            while(!eofseen && ((stopAfter == 0) || (processed - initialProcessed) < stopAfter)) 
            {
                queryUpdateProgress();

                if (!inputstream->eos())
                {
                    size32_t sizeRead = deserializer->deserialize(outBuilder.ensureRow(), deserializeSource);
                    //In this case size read from disk == size created in memory
                    localOffset += sizeRead;
                    if ((processed - initialProcessed)>=limit)
                    {
                        outBuilder.clear();
                        if ( agent.queryCodeContext()->queryDebugContext())
                            agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                        helper.onLimitExceeded();
                        return NULL;
                    }
                    processed++;
                    return outBuilder.finalizeRowClear(sizeRead);
                }
                eofseen = !openNext();
            }
        }
        close();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
    return NULL;
}

//=====================================================================================================

CHThorDiskNormalizeActivity::CHThorDiskNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskNormalizeArg &_arg, ThorActivityKind _kind) : CHThorBinaryDiskReadBase(_agent, _activityId, _subgraphId, _arg, _arg, _kind), helper(_arg), outBuilder(NULL)
{
}

void CHThorDiskNormalizeActivity::stop()        
{ 
    outBuilder.clear();
    PARENT::stop(); 
}

void CHThorDiskNormalizeActivity::ready()       
{ 
    PARENT::ready(); 
    outBuilder.setAllocator(rowAllocator);
    limit = helper.getRowLimit();
    if (helper.getFlags() & TDRlimitskips)
        limit = (unsigned __int64) -1;
    stopAfter = helper.getChooseNLimit();
    lastSizeRead = 0;
    expanding = false;
}

void CHThorDiskNormalizeActivity::gatherInfo(IFileDescriptor * fd)
{
    PARENT::gatherInfo(fd);
    assertex(!grouped);
}

const void *CHThorDiskNormalizeActivity::nextRow()
{
    if (!opened) open();
    for (;;)
    {
        if (eofseen || (stopAfter && (processed - initialProcessed) >= stopAfter)) 
            break;

        for (;;)
        {
            if (expanding)
            {
                for (;;)
                {
                    expanding = helper.next();
                    if (!expanding)
                        break;

                    const void * ret = createNextRow();
                    if (ret)
                        return ret;
                }
            }

            localOffset += lastSizeRead;
            prefetchBuffer.finishedRow();

            if (inputstream->eos())
            {
                lastSizeRead = 0;
                break;
            }

            prefetcher->readAhead(prefetchBuffer);
            const byte * next = prefetchBuffer.queryRow();
            lastSizeRead = prefetchBuffer.queryRowSize();

            queryUpdateProgress();
            if (segMonitorsMatch(next))
            {
                try
                {
                    expanding = helper.first(next);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
                if (expanding)
                {
                    const void * ret = createNextRow();
                    if (ret)
                        return ret;
                }
            }
        }
        eofseen = !openNext();
    }
    close();
    return NULL;
}


const void * CHThorDiskNormalizeActivity::createNextRow()
{
    try
    {
        size32_t thisSize = helper.transform(outBuilder.ensureRow());
        if (thisSize == 0)
            return NULL;

        if ((processed - initialProcessed) >=limit)
        {
            outBuilder.clear();
            if ( agent.queryCodeContext()->queryDebugContext())
                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            helper.onLimitExceeded();
            return NULL;
        }
        processed++;
        return outBuilder.finalizeRowClear(thisSize);
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

//=====================================================================================================

CHThorDiskAggregateActivity::CHThorDiskAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskAggregateArg &_arg, ThorActivityKind _kind) : CHThorBinaryDiskReadBase(_agent, _activityId, _subgraphId, _arg, _arg, _kind), helper(_arg), outBuilder(NULL)
{
}

void CHThorDiskAggregateActivity::stop()        
{ 
    outBuilder.clear();
    PARENT::stop(); 
}

void CHThorDiskAggregateActivity::ready()       
{ 
    PARENT::ready(); 
    outBuilder.setAllocator(rowAllocator);
    finished = false;
}

void CHThorDiskAggregateActivity::gatherInfo(IFileDescriptor * fd)
{
    PARENT::gatherInfo(fd);
    assertex(!grouped);
}


const void *CHThorDiskAggregateActivity::nextRow()
{
    if (finished) return NULL;
    try
    {
        if (!opened) open();
        outBuilder.ensureRow();
        helper.clearAggregate(outBuilder);
        while (!eofseen)
        {
            while (!inputstream->eos())
            {
                queryUpdateProgress();

                prefetcher->readAhead(prefetchBuffer);
                const byte * next = prefetchBuffer.queryRow();
                size32_t sizeRead = prefetchBuffer.queryRowSize();
                if (segMonitorsMatch(next))
                    helper.processRow(outBuilder, next);
                prefetchBuffer.finishedRow();
                localOffset += sizeRead;
            }
            eofseen = !openNext();
        }
        close();

        processed++;
        finished = true;
        unsigned retSize = outputMeta.getRecordSize(outBuilder.getSelf());
        return outBuilder.finalizeRowClear(retSize);
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

//=====================================================================================================

CHThorDiskCountActivity::CHThorDiskCountActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskCountArg &_arg, ThorActivityKind _kind) : CHThorBinaryDiskReadBase(_agent, _activityId, _subgraphId, _arg, _arg, _kind), helper(_arg)
{
    finished = true;
}

CHThorDiskCountActivity::~CHThorDiskCountActivity()
{
}

void CHThorDiskCountActivity::ready()       
{ 
    PARENT::ready(); 
    finished = false;
    stopAfter = helper.getChooseNLimit();
}

void CHThorDiskCountActivity::gatherInfo(IFileDescriptor * fd)
{
    PARENT::gatherInfo(fd);
    assertex(!grouped);
}


const void *CHThorDiskCountActivity::nextRow()
{
    if (finished) return NULL;

    unsigned __int64 totalCount = 0;
    if ((segMonitors.ordinality() == 0) && !helper.hasFilter() && (fixedDiskRecordSize != 0) && !(helper.getFlags() & (TDXtemporary | TDXjobtemp))
        && !((helper.getFlags() & TDXcompress) && agent.queryResolveFilesLocally()) )
    {
        resolve();
        if (segHelper.canMatchAny() && ldFile)
        {
            unsigned __int64 size = ldFile->getFileSize();
            if (size % fixedDiskRecordSize)
                throw MakeStringException(0, "Physical file %s has size %" I64F "d which is not a multiple of record size %d", ldFile->queryLogicalName(), size, fixedDiskRecordSize);
            totalCount = size / fixedDiskRecordSize;
        }
    }
    else
    {
        if (!opened) open();

        for (;;)
        {
            if (eofseen) 
                break;
            while (!inputstream->eos())
            {
                queryUpdateProgress();

                prefetcher->readAhead(prefetchBuffer);
                const byte * next = prefetchBuffer.queryRow();
                size32_t sizeRead = prefetchBuffer.queryRowSize();
                if (segMonitorsMatch(next))
                    totalCount += helper.numValid(next);
                prefetchBuffer.finishedRow();
                localOffset += sizeRead;
                if (totalCount > stopAfter)
                    break;
            }
            if (totalCount > stopAfter)
                break;
            eofseen = !openNext();
        }
        close();
    }

    if (totalCount > stopAfter)
        totalCount = stopAfter;
    finished = true;
    processed++;

    size32_t outSize = outputMeta.getFixedSize();
    void * ret = rowAllocator->createRow();
    if (outSize == 1)
    {
        assertex(stopAfter == 1);
        *(byte *)ret = (byte)totalCount;
    }
    else
    {
        assertex(outSize == sizeof(unsigned __int64));
        *(unsigned __int64 *)ret = totalCount;
    }
    return rowAllocator->finalizeRow(outSize, ret, outSize);
}

//=====================================================================================================

CHThorDiskGroupAggregateActivity::CHThorDiskGroupAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskGroupAggregateArg &_arg, ThorActivityKind _kind) 
  : CHThorBinaryDiskReadBase(_agent, _activityId, _subgraphId, _arg, _arg, _kind), 
    helper(_arg), 
    aggregated(_arg, _arg)
{
}

void CHThorDiskGroupAggregateActivity::ready()      
{ 
    PARENT::ready(); 
    eof = false;
    gathered = false;
}

void CHThorDiskGroupAggregateActivity::gatherInfo(IFileDescriptor * fd)
{
    PARENT::gatherInfo(fd);
    assertex(!grouped);
    aggregated.start(rowAllocator);
}

void CHThorDiskGroupAggregateActivity::processRow(const void * next)
{
    aggregated.addRow(next);
}


const void *CHThorDiskGroupAggregateActivity::nextRow()
{
    if (eof)
        return NULL;

    try
    {
        if (!gathered)
        {
            if (!opened) open();
            while (!eofseen)
            {
                while (!inputstream->eos())
                {
                    queryUpdateProgress();

                    prefetcher->readAhead(prefetchBuffer);
                    const byte * next = prefetchBuffer.queryRow();
                    size32_t sizeRead = prefetchBuffer.queryRowSize();

                        //helper.processRows(sizeRead, next, this);
                    helper.processRow(next, this);

                    prefetchBuffer.finishedRow();
                    localOffset += sizeRead;
                }
                eofseen = !openNext();
            }
            close();
            gathered = true;
        }
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }

    Owned<AggregateRowBuilder> next = aggregated.nextResult();
    if (next)
    {
        processed++;
        return next->finalizeRowClear();
    }
    eof = true;
    return NULL;
}

//=====================================================================================================

CHThorCsvReadActivity::CHThorCsvReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCsvReadArg &_arg, ThorActivityKind _kind) : CHThorDiskReadBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    maxRowSize = agent.queryWorkUnit()->getDebugValueInt(OPT_MAXCSVROWSIZE, defaultMaxCsvRowSize) * 1024 * 1024;
}

CHThorCsvReadActivity::~CHThorCsvReadActivity()
{
}

void CHThorCsvReadActivity::ready()
{
    PARENT::ready();
}

void CHThorCsvReadActivity::stop()
{
    csvSplitter.reset();
    PARENT::stop();
}

void CHThorCsvReadActivity::gatherInfo(IFileDescriptor * fd)
{
    PARENT::gatherInfo(fd);

    ICsvParameters * csvInfo = helper.queryCsvParameters();

    headerLines = csvInfo->queryHeaderLen();
    maxDiskSize = csvInfo->queryMaxSize();
    limit = helper.getRowLimit();
    if (helper.getFlags() & TDRlimitskips)
        limit = (unsigned __int64) -1;
    stopAfter = helper.getChooseNLimit();

    const char * quotes = NULL;
    const char * separators = NULL;
    const char * terminators = NULL;
    const char * escapes = NULL;
    IDistributedFile * dFile = ldFile?ldFile->queryDistributedFile():NULL;
    if (dFile)  //only makes sense for distributed (non local) files
    {
        IPropertyTree & options = dFile->queryAttributes();
        quotes = options.queryProp("@csvQuote");
        separators = options.queryProp("@csvSeparate");
        terminators = options.queryProp("@csvTerminate");
        escapes = options.queryProp("@csvEscape");
    }
    csvSplitter.init(helper.getMaxColumns(), csvInfo, quotes, separators, terminators, escapes);
}

void CHThorCsvReadActivity::calcFixedDiskRecordSize()
{
    fixedDiskRecordSize = 0;
}

const void *CHThorCsvReadActivity::nextRow()
{
    while (!stopAfter || (processed - initialProcessed) < stopAfter)
    {
        checkOpenNext();
        if (eofseen)
            break;
        if (!inputstream->eos())
        {
            size32_t rowSize = 4096; // MORE - make configurable
            size32_t thisLineLength;
            for (;;)
            {
                size32_t avail;
                const void *peek = inputstream->peek(rowSize, avail);
                thisLineLength = csvSplitter.splitLine(avail, (const byte *)peek);
                if (thisLineLength < rowSize || avail < rowSize)
                    break;
                if (rowSize == maxRowSize)
                {
                    OwnedRoxieString fileName(helper.getFileName());
                    throw MakeStringException(99, "File %s contained a line of length greater than %d bytes.", fileName.get(), rowSize);
                }
                if (rowSize >= maxRowSize/2)
                    rowSize = maxRowSize;
                else
                    rowSize += rowSize;
            }

            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned thisSize;
            try
            {
                thisSize = helper.transform(rowBuilder, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData());
            }
            catch(IException * e)
            {
                throw makeWrappedException(e);
            }
            inputstream->skip(thisLineLength);
            localOffset += thisLineLength;
            if (thisSize)
            {
                OwnedConstRoxieRow ret = rowBuilder.finalizeRowClear(thisSize);
                if ((processed - initialProcessed) >= limit)
                {
                    if ( agent.queryCodeContext()->queryDebugContext())
                        agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                    helper.onLimitExceeded();
                    return NULL;
                }
                processed++;
                return ret.getClear();
            }
        }
    }
    close();
    return NULL;
}


bool CHThorCsvReadActivity::openNext()
{
    if (CHThorDiskReadBaseActivity::openNext())
    {
        unsigned lines = headerLines;
        while (lines-- && !inputstream->eos())
        {
            size32_t numAvailable;
            const void * next = inputstream->peek(maxDiskSize, numAvailable);
            inputstream->skip(csvSplitter.splitLine(numAvailable, (const byte *)next));
        }
        // only skip header in the first file - since spray doesn't duplicate the header.
        headerLines = 0;        
        return true;
    }
    return false;
}

void CHThorCsvReadActivity::checkOpenNext()
{
    agent.reportProgress(NULL);
    if (!opened)
    {
        agent.reportProgress(NULL);
        if (!helper.canMatchAny())
        {
            eofseen = true;
            opened = true;
        }
        else
            open();
    }

    for (;;)
    {
        if (eofseen || !inputstream->eos())
            return;

        eofseen = !openNext();
    }
}

//=====================================================================================================

CHThorXmlReadActivity::CHThorXmlReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorXmlReadArg &_arg, ThorActivityKind _kind) : CHThorDiskReadBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorXmlReadActivity::ready()
{
    CHThorDiskReadBaseActivity::ready();
    rowTransformer.set(helper.queryTransformer());
    localOffset = 0;
    limit = helper.getRowLimit();
    if (helper.getFlags() & TDRlimitskips)
        limit = (unsigned __int64) -1;
    stopAfter = helper.getChooseNLimit();
}

void CHThorXmlReadActivity::stop()
{
    xmlParser.clear();
    CHThorDiskReadBaseActivity::stop();
}

void CHThorXmlReadActivity::gatherInfo(IFileDescriptor * fd)
{
    PARENT::gatherInfo(fd);
}

void CHThorXmlReadActivity::calcFixedDiskRecordSize()
{
    fixedDiskRecordSize = 0;
}

const void *CHThorXmlReadActivity::nextRow()
{
    if(!opened) open();
    while (!eofseen && (!stopAfter  || (processed - initialProcessed) < stopAfter))
    {
        agent.reportProgress(NULL);
        //call to next() will callback on the IXmlSelect interface
        bool gotNext = false;
        try
        {
            gotNext = xmlParser->next();
        }
        catch(IException * e)
        {
            throw makeWrappedException(e, inputfile->queryFilename());
        }
        if(!gotNext)
            eofseen = !openNext();
        else if (lastMatch)
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned sizeGot;
            try
            {
                sizeGot = rowTransformer->transform(rowBuilder, lastMatch, this);
            }
            catch(IException * e)
            {
                throw makeWrappedException(e);
            }
            lastMatch.clear();
            localOffset = 0;
            if (sizeGot)
            {
                OwnedConstRoxieRow ret = rowBuilder.finalizeRowClear(sizeGot);
                if ((processed - initialProcessed) >= limit)
                {
                    if ( agent.queryCodeContext()->queryDebugContext())
                        agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                    helper.onLimitExceeded();
                    return NULL;
                }
                processed++;
                return ret.getClear();
            }
        }
    }
    return NULL;
}


bool CHThorXmlReadActivity::openNext()
{
    if (inputfileio)
        offsetOfPart += inputfileio->size();
    localOffset = 0;
    if (CHThorDiskReadBaseActivity::openNext())
    {
        unsigned readBufferSize = queryReadBufferSize();
        OwnedIFileIOStream inputfileiostream;
        if(readBufferSize)
            inputfileiostream.setown(createBufferedIOStream(inputfileio, readBufferSize));
        else
            inputfileiostream.setown(createIOStream(inputfileio));

        OwnedRoxieString xmlIterator(helper.getXmlIteratorPath());
        if (kind==TAKjsonread)
            xmlParser.setown(createJSONParse(*inputfileiostream, xmlIterator, *this, (0 != (TDRxmlnoroot & helper.getFlags()))?ptr_noRoot:ptr_none, (helper.getFlags() & TDRusexmlcontents) != 0));
        else
            xmlParser.setown(createXMLParse(*inputfileiostream, xmlIterator, *this, (0 != (TDRxmlnoroot & helper.getFlags()))?ptr_noRoot:ptr_none, (helper.getFlags() & TDRusexmlcontents) != 0));
        return true;
    }
    return false;
}

void CHThorXmlReadActivity::closepart()
{
    xmlParser.clear();
    CHThorDiskReadBaseActivity::closepart();
}

//---------------------------------------------------------------------------

CHThorLocalResultReadActivity::CHThorLocalResultReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultReadArg &_arg, ThorActivityKind _kind, __int64 graphId) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    physicalRecordSize = outputMeta;
    grouped = outputMeta.isGrouped();
    graph = resolveLocalQuery(graphId);
    result = NULL;
}

void CHThorLocalResultReadActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    result = graph->queryResult(helper.querySequence());
    curRow = 0;
}


const void *CHThorLocalResultReadActivity::nextRow()
{
    const void * next = result->queryRow(curRow++);
    if (next)
    {
        processed++;
        LinkRoxieRow(next);
        return next;
    }
    return NULL;
}

//=====================================================================================================

CHThorLocalResultWriteActivity::CHThorLocalResultWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultWriteArg &_arg, ThorActivityKind _kind, __int64 graphId)
 : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    graph = resolveLocalQuery(graphId);
}

void CHThorLocalResultWriteActivity::execute()
{
    IHThorGraphResult * result = graph->createResult(helper.querySequence(), LINK(rowAllocator));
    for (;;)
    {
        const void *nextrec = input->nextRow();
        if (!nextrec)
        {
            nextrec = input->nextRow();
            if (!nextrec)
                break;
            result->addRowOwn(NULL);
        }
        result->addRowOwn(nextrec);
    }
}

//=====================================================================================================

CHThorDictionaryResultWriteActivity::CHThorDictionaryResultWriteActivity (IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDictionaryResultWriteArg &_arg, ThorActivityKind _kind, __int64 graphId)
 : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    graph = resolveLocalQuery(graphId);
}

void CHThorDictionaryResultWriteActivity::execute()
{
    RtlLinkedDictionaryBuilder builder(rowAllocator, helper.queryHashLookupInfo());
    for (;;)
    {
        const void *row = input->nextRow();
        if (!row)
        {
            row = input->nextRow();
            if (!row)
                break;
        }
        builder.appendOwn(row);
    }
    IHThorGraphResult * result = graph->createResult(helper.querySequence(), LINK(rowAllocator));
    size32_t dictSize = builder.getcount();
    byte ** dictRows = builder.queryrows();
    for (size32_t row = 0; row < dictSize; row++)
    {
        byte *thisRow = dictRows[row];
        if (thisRow)
            LinkRoxieRow(thisRow);
        result->addRowOwn(thisRow);
    }
}

//=====================================================================================================

CHThorLocalResultSpillActivity::CHThorLocalResultSpillActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultSpillArg &_arg, ThorActivityKind _kind, __int64 graphId)
 : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    result = NULL;
    nullPending = false;
    graph = resolveLocalQuery(graphId);
    assertex(graph);
}

void CHThorLocalResultSpillActivity::ready()
{
    CHThorSimpleActivityBase::ready(); 
    result = graph->createResult(helper.querySequence(), LINK(rowAllocator));
    nullPending = false;
}


const void * CHThorLocalResultSpillActivity::nextRow()
{
    const void * ret = input->nextRow();
    if (ret)
    {
        if (nullPending)
        {
            result->addRowOwn(NULL);
            nullPending = false;
        }
        LinkRoxieRow(ret);
        result->addRowOwn(ret);
        processed++;
    }
    else
        nullPending = true;

    return ret;
}

void CHThorLocalResultSpillActivity::stop()
{
    for (;;)
    {
        const void * ret = input->nextRow();
        if (!ret)
        {
            if (nullPending)
                break;
            nullPending = true;
        }
        else
        {
            if (nullPending)
            {
                result->addRowOwn(NULL);
                nullPending = false;
            }
            result->addRowOwn(ret);
        }
    }
    CHThorSimpleActivityBase::stop(); 
}


//=====================================================================================================

CHThorLoopActivity::CHThorLoopActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLoopArg &_arg, ThorActivityKind _kind)
 : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    flags = helper.getFlags();
    maxIterations = 0;
}

CHThorLoopActivity::~CHThorLoopActivity()
{
    ForEachItemIn(idx, loopPending)
        ReleaseRoxieRow(loopPending.item(idx));
}

void CHThorLoopActivity::ready()
{
    curInput = &input->queryStream();
    eof = false;
    loopCounter = 1;
    CHThorSimpleActivityBase::ready(); 
    maxIterations = helper.numIterations();
    if ((int)maxIterations < 0) maxIterations = 0;
    finishedLooping = ((kind == TAKloopcount) && (maxIterations == 0));
    if ((flags & IHThorLoopArg::LFnewloopagain) && !helper.loopFirstTime())
        finishedLooping = true;
    extractBuilder.clear();
    helper.createParentExtract(extractBuilder);
}


const void * CHThorLoopActivity::nextRow()
{
    if (eof)
        return NULL;

    unsigned emptyIterations = 0;
    for (;;)
    {
        for (;;)
        {
            const void * ret = curInput->nextRow();
            if (!ret)
            {
                ret = curInput->nextRow();      // more cope with groups somehow....
                if (!ret)
                {
                    if (finishedLooping)
                    {
                        eof = true;
                        return NULL;
                    }
                    break;
                }
            }

            if (finishedLooping || 
                ((flags & IHThorLoopArg::LFfiltered) && !helper.sendToLoop(loopCounter, ret)))
            {
                processed++;
                return ret;
            }
            loopPending.append(ret);
        }

        switch (kind)
        {
        case TAKloopdataset:
            {
                if (!(flags & IHThorLoopArg::LFnewloopagain))
                {
                    if (!helper.loopAgain(loopCounter, loopPending.ordinality(), (const void * *)loopPending.getArray()))
                    {
                        if (loopPending.ordinality() == 0)
                        {
                            eof = true;
                            return NULL;
                        }

                        arrayInput.init(&loopPending);
                        curInput = &arrayInput;
                        finishedLooping = true;
                        continue;       // back to the input loop again
                    }
                }
                break;
            }
        case TAKlooprow:
            if (loopPending.empty())
            {
                finishedLooping = true;
                eof = true;
                return NULL;
            }
            break;
        }

        if (loopPending.ordinality())
            emptyIterations = 0;
        else
        {
            //note: any outputs which didn't go around the loop again, would return the record, reinitializing emptyIterations
            emptyIterations++;
            if (emptyIterations > EMPTY_LOOP_LIMIT)
                throw MakeStringException(0, "Executed LOOP with empty input and output %u times", emptyIterations);
            if (emptyIterations % 32 == 0)
                DBGLOG("Executing LOOP with empty input and output %u times", emptyIterations);
        }

        void * counterRow = NULL;
        if (flags & IHThorLoopArg::LFcounter)
        {
            counterRow = queryRowManager()->allocate(sizeof(thor_loop_counter_t), activityId);
            *((thor_loop_counter_t *)counterRow) = loopCounter;
        }

        Owned<IHThorGraphResults> curResults = loopGraph->execute(counterRow, loopPending, extractBuilder.getbytes());
        if (flags & IHThorLoopArg::LFnewloopagain)
        {
            IHThorGraphResult * result = curResults->queryResult(helper.loopAgainResult());
            assertex(result);
            const void * row = result->queryRow(0);
            assertex(row);
            //Result is a row which contains a single boolean field.
            if (!((const bool *)row)[0])
                finishedLooping = true;
        }
        resultInput.init(curResults->queryResult(0));
        curInput = &resultInput;

        loopCounter++;
        if ((kind == TAKloopcount) && (loopCounter > maxIterations))
            finishedLooping = true;
    }
}


void CHThorLoopActivity::stop()
{
    ForEachItemIn(idx, loopPending)
        ReleaseRoxieRow(loopPending.item(idx));
    loopPending.kill();
    CHThorSimpleActivityBase::stop(); 
}

//---------------------------------------------------------------------------

CHThorGraphLoopResultReadActivity::CHThorGraphLoopResultReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopResultReadArg &_arg, ThorActivityKind _kind, __int64 graphId) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(&_arg)
{
    physicalRecordSize = outputMeta;
    grouped = outputMeta.isGrouped();
    result = NULL;
    graph = resolveLocalQuery(graphId);
}

CHThorGraphLoopResultReadActivity::CHThorGraphLoopResultReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _arg, ThorActivityKind _kind, __int64 graphId, unsigned _sequence, bool _grouped) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(NULL)
{
    physicalRecordSize = outputMeta;
    sequence = _sequence;
    grouped = _grouped;
    result = NULL;
    graph = resolveLocalQuery(graphId);
}

void CHThorGraphLoopResultReadActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    if (helper)
        sequence = helper->querySequence();
    if ((int)sequence >= 0)
        result = graph->queryGraphLoopResult(sequence);
    else
        result = NULL;
    curRow = 0;
}


const void *CHThorGraphLoopResultReadActivity::nextRow()
{
    if (result)
    {
        const void * next = result->queryRow(curRow++);
        if (next)
        {
            processed++;
            LinkRoxieRow(next);
            return (void *)next;
        }
    }
    return NULL;
}

//=====================================================================================================

CHThorGraphLoopResultWriteActivity::CHThorGraphLoopResultWriteActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopResultWriteArg &_arg, ThorActivityKind _kind, __int64 graphId)
 : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    graph = resolveLocalQuery(graphId);
}

void CHThorGraphLoopResultWriteActivity::execute()
{
    IHThorGraphResult * result = graph->createGraphLoopResult(LINK(rowAllocator));
    for (;;)
    {
        const void *nextrec = input->nextRow();
        if (!nextrec)
        {
            nextrec = input->nextRow();
            if (!nextrec)
                break;
            result->addRowOwn(NULL);
        }
        result->addRowOwn(nextrec);
    }
}

//=====================================================================================================

class CCounterMeta : implements IOutputMetaData, public CInterface
{
public:
    IMPLEMENT_IINTERFACE

    virtual size32_t getRecordSize(const void *rec)         { return sizeof(thor_loop_counter_t); }
    virtual size32_t getMinRecordSize() const               { return sizeof(thor_loop_counter_t); }
    virtual size32_t getFixedSize() const                   { return sizeof(thor_loop_counter_t); }
    virtual void toXML(const byte * self, IXmlWriter & out) { }
    virtual unsigned getVersion() const                     { return OUTPUTMETADATA_VERSION; }
    virtual unsigned getMetaFlags()                         { return 0; }
    virtual void destruct(byte * self)  {}
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputMetaData * querySerializedDiskMeta() { return this; }
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) {}
    virtual IOutputMetaData * queryChildMeta(unsigned i) { return NULL; }
    virtual const RtlRecord &queryRecordAccessor(bool expand) const { throwUnexpected(); } // could provide a static implementation if needed
};

//=====================================================================================================

CHThorGraphLoopActivity::CHThorGraphLoopActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopArg &_arg, ThorActivityKind _kind)
 : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    flags = helper.getFlags();
    maxIterations = 0;
    counterMeta.setown(new CCounterMeta);
}

void CHThorGraphLoopActivity::ready()
{
    executed = false;
    resultIndex = 0;
    CHThorSimpleActivityBase::ready(); 
    maxIterations = helper.numIterations();
    if ((int)maxIterations < 0) maxIterations = 0;
    loopResults.setown(agent.createGraphLoopResults());
    extractBuilder.clear();
    helper.createParentExtract(extractBuilder);
    rowAllocator.setown(agent.queryCodeContext()->getRowAllocator(queryOutputMeta(), activityId));
    rowAllocatorCounter.setown(agent.queryCodeContext()->getRowAllocator(counterMeta, activityId));
}


const void * CHThorGraphLoopActivity::nextRow()
{
    if (!executed)
    {
        executed = true;

        IHThorGraphResult * inputResult = loopResults->createResult(0, LINK(rowAllocator));
        for (;;)
        {
            const void * ret = input->nextRow();
            if (!ret)
            {
                ret = input->nextRow();
                if (!ret)
                    break;
                inputResult->addRowOwn(NULL);
            }
            inputResult->addRowOwn(ret);
        }

        for (unsigned loopCounter = 1; loopCounter <= maxIterations; loopCounter++)
        {
            void * counterRow = NULL;
            if (flags & IHThorGraphLoopArg::GLFcounter)
            {
                counterRow = rowAllocatorCounter->createRow();
                *((thor_loop_counter_t *)counterRow) = loopCounter;
                counterRow = rowAllocatorCounter->finalizeRow(sizeof(thor_loop_counter_t), counterRow, sizeof(thor_loop_counter_t));
            }
            loopGraph->execute(counterRow, loopResults, extractBuilder.getbytes());
        }

        int iNumResults = loopResults->ordinality();
        finalResult = loopResults->queryResult(iNumResults-1); //Get the last result, which isnt necessarily 'maxIterations'
    }

    const void * next = finalResult->getOwnRow(resultIndex++);
    if (next)
        processed++;
    return next;
}


void CHThorGraphLoopActivity::stop()
{
    rowAllocator.clear();
    finalResult = NULL;
    loopResults.clear();
    CHThorSimpleActivityBase::stop(); 
}

//=====================================================================================================

CHThorParallelGraphLoopActivity::CHThorParallelGraphLoopActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopArg &_arg, ThorActivityKind _kind)
 : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    flags = helper.getFlags();
    maxIterations = 0;
}

void CHThorParallelGraphLoopActivity::ready()
{
    executed = false;
    resultIndex = 0;
    CHThorSimpleActivityBase::ready(); 
    maxIterations = helper.numIterations();
    if ((int)maxIterations < 0) maxIterations = 0;
    loopResults.setown(agent.createGraphLoopResults());
    extractBuilder.clear();
    helper.createParentExtract(extractBuilder);
    rowAllocator.setown(agent.queryCodeContext()->getRowAllocator(queryOutputMeta(), activityId));
}


const void * CHThorParallelGraphLoopActivity::nextRow()
{
    if (!executed)
    {
        executed = true;

        IHThorGraphResult * inputResult = loopResults->createResult(0, LINK(rowAllocator));
        for (;;)
        {
            const void * ret = input->nextRow();
            if (!ret)
            {
                ret = input->nextRow();
                if (!ret)
                    break;
                inputResult->addRowOwn(NULL);
            }
            inputResult->addRowOwn(ret);
        }

//      The lack of separation between pre-creation and creation means this would require cloning lots of structures.
//      not implemented for the moment.
//      loopGraph->executeParallel(loopResults, extractBuilder.getbytes(), maxIterations);

        finalResult = loopResults->queryResult(maxIterations);
    }

    const void * next = finalResult->getOwnRow(resultIndex++);
    if (next)
        processed++;
    return next;
}


void CHThorParallelGraphLoopActivity::stop()
{
    rowAllocator.clear();
    finalResult = NULL;
    loopResults.clear();
    CHThorSimpleActivityBase::stop(); 
}

//=====================================================================================================

LibraryCallOutput::LibraryCallOutput(CHThorLibraryCallActivity * _owner, unsigned _output, IOutputMetaData * _meta) : owner(_owner), output(_output), meta(_meta)
{
    processed = 0;
}

const void * LibraryCallOutput::nextRow()
{
    if (!gotRows)
    {
        result.set(owner->getResultRows(output));
        gotRows = true;
    }

    const void * ret = result->getOwnRow(curRow++);
    if (ret)
        processed++;
    return ret;
}

bool LibraryCallOutput::isGrouped()
{
    return meta->isGrouped();
}

IOutputMetaData * LibraryCallOutput::queryOutputMeta() const
{
    return meta;
}

void LibraryCallOutput::ready()
{
    owner->ready();
    gotRows = false;
    result.clear();
    curRow = 0;
}

void LibraryCallOutput::stop()
{
    owner->stop();
    result.clear();
}

void LibraryCallOutput::resetEOF()
{
    throwUnexpected();
}

void LibraryCallOutput::updateProgress(IStatisticGatherer &progress) const
{
    owner->updateOutputProgress(progress, *this, processed);
}


CHThorLibraryCallActivity::CHThorLibraryCallActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLibraryCallArg &_arg, ThorActivityKind _kind, IPropertyTree * node)
 : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    libraryName.set(node->queryProp("att[@name=\"libname\"]/@value"));
    interfaceHash = node->getPropInt("att[@name=\"_interfaceHash\"]/@value", 0);
    bool embedded = node->getPropBool("att[@name=\"embedded\"]/@value", false) ;
    if (embedded)
    {
        embeddedGraphName.set(node->queryProp("att[@name=\"graph\"]/@value"));
        if (!embeddedGraphName)
            embeddedGraphName.set(libraryName);
    }

    Owned<IPropertyTreeIterator> iter = node->getElements("att[@name=\"_outputUsed\"]");
    ForEach(*iter)
    {
        unsigned whichOutput = iter->query().getPropInt("@value");
        IOutputMetaData * meta = helper.queryOutputMeta(whichOutput);
        outputs.append(*new LibraryCallOutput(this, whichOutput, meta));
    }

    state = StateCreated;
}

IHThorGraphResult * CHThorLibraryCallActivity::getResultRows(unsigned whichOutput)
{
    CriticalBlock procedure(cs);

    if (!results)
    {
        if (libraryName.length() == 0)
            libraryName.setown(helper.getLibraryName());
        helper.createParentExtract(extractBuilder);
        results.setown(agent.executeLibraryGraph(libraryName, interfaceHash, activityId, embeddedGraphName, extractBuilder.getbytes()));
    }

    return results->queryResult(whichOutput);
}


IHThorInput * CHThorLibraryCallActivity::queryOutput(unsigned idx)
{
    assert(outputs.isItem(idx));
    return &outputs.item(idx);
}

void CHThorLibraryCallActivity::updateOutputProgress(IStatisticGatherer &progress, const LibraryCallOutput & _output, unsigned __int64 numProcessed) const
{
    LibraryCallOutput & output = const_cast<LibraryCallOutput &>(_output);
    updateProgressForOther(progress, activityId, subgraphId, outputs.find(output), numProcessed);
}


void CHThorLibraryCallActivity::ready()
{
    CriticalBlock procedure(cs);
    if (state != StateReady)
    {
        results.clear();
        CHThorSimpleActivityBase::ready();
        state = StateReady;
    }
}


const void * CHThorLibraryCallActivity::nextRow()
{
    throwUnexpected();
}


void CHThorLibraryCallActivity::stop()
{
    CriticalBlock procedure(cs);
    if (state != StateDone)
    {
        results.clear();
        CHThorSimpleActivityBase::stop(); 
    }
}


//=====================================================================================================

class CHThorNWayInputActivity : public CHThorSimpleActivityBase, implements IHThorNWayInput
{
    IHThorNWayInputArg & helper;
    InputArrayType inputs;
    InputArrayType selectedInputs;

public:
    CHThorNWayInputActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayInputArg &_arg, ThorActivityKind _kind) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
    {
    }

    virtual void ready()
    {
        bool selectionIsAll;
        size32_t selectionLen;
        rtlDataAttr selection;
        helper.getInputSelection(selectionIsAll, selectionLen, selection.refdata());
        selectedInputs.kill();
        if (selectionIsAll)
        {
            ForEachItemIn(i, inputs)
                selectedInputs.append(inputs.item(i));
        }
        else
        {
            const size32_t * selections = (const size32_t *)selection.getdata();
            unsigned max = selectionLen/sizeof(size32_t);
            for (unsigned i = 0; i < max; i++)
            {
                unsigned nextIndex = selections[i];
                //Check there are no duplicates.....  Assumes there are a fairly small number of inputs, so n^2 search is ok.
                for (unsigned j=i+1; j < max; j++)
                {
                    if (nextIndex == selections[j])
                        throw MakeStringException(100, "Selection list for nway input can not contain duplicates");
                }
                if (!inputs.isItem(nextIndex-1))
                    throw MakeStringException(100, "Index %d in RANGE selection list is out of range", nextIndex);

                selectedInputs.append(inputs.item(nextIndex-1));
            }
        }

        ForEachItemIn(i2, selectedInputs)
            selectedInputs.item(i2)->ready();
    }

    virtual void setInput(unsigned idx, IHThorInput *_in)
    {
        assertex(idx == inputs.ordinality());
        inputs.append(_in);
    }

    virtual const void * nextRow()
    {
        throwUnexpected();
    }

    virtual void updateProgress(IStatisticGatherer &progress) const
    {
//      CHThorSimpleActivityBase::updateProgress(progress);
        ForEachItemIn(i, inputs)
            inputs.item(i)->updateProgress(progress);
    }

    virtual unsigned numConcreteOutputs() const
    {
        return selectedInputs.ordinality();
    }

    virtual IHThorInput * queryConcreteInput(unsigned idx) const
    {
        if (selectedInputs.isItem(idx))
            return selectedInputs.item(idx);
        return NULL;
    }
};

//=====================================================================================================

class CHThorNWayGraphLoopResultReadActivity : public CHThorSimpleActivityBase, implements IHThorNWayInput
{
    IHThorNWayGraphLoopResultReadArg & helper;
    CIArrayOf<CHThorActivityBase> inputs;
    __int64 graphId;
    bool grouped;

public:
    CHThorNWayGraphLoopResultReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayGraphLoopResultReadArg &_arg, ThorActivityKind _kind, __int64 _graphId) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
    {
        grouped = helper.isGrouped();
        graphId = _graphId;
    }

    virtual bool isGrouped() 
    { 
        return grouped; 
    }

    virtual void ready()
    {
        bool selectionIsAll;
        size32_t selectionLen;
        rtlDataAttr selection;
        helper.getInputSelection(selectionIsAll, selectionLen, selection.refdata());
        if (selectionIsAll)
            throw MakeStringException(100, "ALL not yet supported for NWay graph inputs");

        unsigned max = selectionLen / sizeof(size32_t);
        const size32_t * selections = (const size32_t *)selection.getdata();
        for (unsigned i = 0; i < max; i++)
        {
            CHThorActivityBase * resultInput = new CHThorGraphLoopResultReadActivity(agent, activityId, subgraphId, helper, kind, graphId, selections[i], grouped);
            inputs.append(*resultInput);
            resultInput->ready();
        }
    }

    virtual void stop()
    {
        inputs.kill();
    }

    virtual void setInput(unsigned idx, IHThorInput *_in)
    {
        throwUnexpected();
    }

    virtual const void * nextRow()
    {
        throwUnexpected();
    }

    virtual unsigned numConcreteOutputs() const
    {
        return inputs.ordinality();
    }

    virtual IHThorInput * queryConcreteInput(unsigned idx) const
    {
        if (inputs.isItem(idx))
            return &inputs.item(idx);
        return NULL;
    }
};

//=====================================================================================================

CHThorNWaySelectActivity::CHThorNWaySelectActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNWaySelectArg &_arg, ThorActivityKind _kind) : CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    selectedInput = NULL;
}

void CHThorNWaySelectActivity::stop()
{
    selectedInput = NULL;
    CHThorMultiInputActivity::stop();
}

void CHThorNWaySelectActivity::ready()
{
    CHThorMultiInputActivity::ready();
    unsigned whichInput = helper.getInputIndex();

    selectedInput = NULL;
    if (whichInput--)
    {
        ForEachItemIn(i, inputs)
        {
            IHThorInput * cur = inputs.item(i);
            IHThorNWayInput * nWayInput = dynamic_cast<IHThorNWayInput *>(cur);
            if (nWayInput)
            {
                unsigned numRealInputs = nWayInput->numConcreteOutputs();
                if (whichInput < numRealInputs)
                    selectedInput = nWayInput->queryConcreteInput(whichInput);
                whichInput -= numRealInputs;
            }
            else
            {
                if (whichInput == 0)
                    selectedInput = cur;
                whichInput -= 1;
            }
            if (selectedInput)
                break;
        }
    }
}

const void * CHThorNWaySelectActivity::nextRow()
{
    if (!selectedInput)
        return NULL;
    return selectedInput->nextRow();
}


const void * CHThorNWaySelectActivity::nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
{
    if (!selectedInput)
        return NULL;
    return selectedInput->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
}

IInputSteppingMeta * CHThorNWaySelectActivity::querySteppingMeta()
{
    if (selectedInput)
        return selectedInput->querySteppingMeta();
    return NULL;
}

//=====================================================================================================
CHThorStreamedIteratorActivity::CHThorStreamedIteratorActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorStreamedIteratorArg &_arg, ThorActivityKind _kind) 
    : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
}

void CHThorStreamedIteratorActivity::ready()
{
    CHThorSimpleActivityBase::ready();
    rows.setown(helper.createInput());
}

const void *CHThorStreamedIteratorActivity::nextRow()
{
    assertex(rows);
    const void * next = rows->nextRow();
    if (next)
        processed++;
    return next;
}

void CHThorStreamedIteratorActivity::stop()
{
    if (rows)
    {
        rows->stop();
        rows.clear();
    }
}


//=====================================================================================================

CHThorExternalActivity::CHThorExternalActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorExternalArg &_arg, ThorActivityKind _kind, IPropertyTree * _graphNode) 
: CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), graphNode(_graphNode)
{
}

void CHThorExternalActivity::ready()
{
    CHThorMultiInputActivity::ready();
    //must be called after onStart()
    processor.setown(helper.createProcessor());
    processor->onCreate(agent.queryCodeContext(), graphNode);
    ForEachItemIn(idx, inputs)
    {
        Owned<CHThorInputAdaptor> adaptedInput = new CHThorInputAdaptor(inputs.item(idx));
        processor->addInput(idx, adaptedInput);
    }
    processor->start();
    if (outputMeta.getMinRecordSize() > 0)
        rows.setown(processor->createOutput(0));
}

const void *CHThorExternalActivity::nextRow()
{
    assertex(rows);
    const void * next = rows->nextRow();
    if (next)
        processed++;
    return next;
}

void CHThorExternalActivity::execute()
{
    assertex(!rows);
    processor->execute();
}

void CHThorExternalActivity::reset()
{
    rows.clear();
    processor->reset();
    processor.clear();
}

void CHThorExternalActivity::stop()
{
    if (rows)
    {
        rows->stop();
        rows.clear();
    }
    processor->stop();
}


//=====================================================================================================

MAKEFACTORY(DiskWrite);
MAKEFACTORY(Iterate);
MAKEFACTORY(Filter);
MAKEFACTORY(Aggregate);
MAKEFACTORY(Rollup);
MAKEFACTORY(Project);
MAKEFACTORY(PrefetchProject);
MAKEFACTORY(FilterProject);

extern HTHOR_API IHThorActivity * createGroupDedupActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg & arg, ThorActivityKind kind)
{
    if(arg.compareAll())
        return new CHThorGroupDedupAllActivity(_agent, _activityId, _subgraphId, arg, kind);
    else if (arg.keepLeft() && !arg.keepBest())
        return new CHThorGroupDedupKeepLeftActivity(_agent, _activityId, _subgraphId, arg, kind);
    else
        return new CHThorGroupDedupKeepRightActivity(_agent, _activityId, _subgraphId, arg, kind);
}

MAKEFACTORY(HashDedup);
MAKEFACTORY(Group);
MAKEFACTORY(Degroup);
MAKEFACTORY_ARG(GroupSort, Sort);
MAKEFACTORY(Join);
MAKEFACTORY_ARG(SelfJoin, Join);
MAKEFACTORY_ARG(LookupJoin, HashJoin);
MAKEFACTORY(AllJoin);
MAKEFACTORY(WorkUnitWrite);
MAKEFACTORY(DictionaryWorkUnitWrite);
MAKEFACTORY(FirstN);
MAKEFACTORY(InlineTable);
MAKEFACTORY_ARG(Concat, Funnel);
MAKEFACTORY(Apply);
MAKEFACTORY(Sample);
MAKEFACTORY(Normalize);
MAKEFACTORY(NormalizeChild);
MAKEFACTORY(NormalizeLinkedChild);
MAKEFACTORY(Distribution);
MAKEFACTORY(RemoteResult);
MAKEFACTORY(ChooseSets);
MAKEFACTORY_ARG(ChooseSetsLast, ChooseSetsEx);
MAKEFACTORY_ARG(ChooseSetsEnth, ChooseSetsEx);
MAKEFACTORY(WorkunitRead);
MAKEFACTORY(PipeRead);
MAKEFACTORY(PipeWrite);
MAKEFACTORY(CsvWrite);
MAKEFACTORY(XmlWrite);
MAKEFACTORY(PipeThrough);
MAKEFACTORY(If);

extern HTHOR_API IHThorActivity *createChildIfActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIfArg &arg, ThorActivityKind kind)
{
    return new CHThorIfActivity(_agent, _activityId, _subgraphId, arg, kind);
}

extern HTHOR_API IHThorActivity *createHashAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorHashAggregateArg &arg, ThorActivityKind kind, bool _isGroupedAggregate)
{
    return new CHThorHashAggregateActivity(_agent, _activityId, _subgraphId, arg, kind, _isGroupedAggregate);
}

MAKEFACTORY(Null);
MAKEFACTORY(SideEffect);
MAKEFACTORY(Action);
MAKEFACTORY(SelectN);
MAKEFACTORY(Spill);
MAKEFACTORY(Limit);
MAKEFACTORY_ARG(SkipLimit, Limit);
MAKEFACTORY_ARG(OnFailLimit, Limit);
MAKEFACTORY(Catch);
MAKEFACTORY_ARG(SkipCatch, Catch);
MAKEFACTORY(CountProject);
MAKEFACTORY(IndexWrite);
MAKEFACTORY(Parse);
MAKEFACTORY(Enth);
MAKEFACTORY(TopN);
MAKEFACTORY(XmlParse);
MAKEFACTORY(Merge);
MAKEFACTORY_ARG(HttpRowCall, HttpCall);
MAKEFACTORY_ARG(SoapRowCall, SoapCall);
MAKEFACTORY_ARG(SoapRowAction, SoapAction);
MAKEFACTORY_ARG(SoapDatasetCall, SoapCall);
MAKEFACTORY_ARG(SoapDatasetAction, SoapAction);
MAKEFACTORY(DatasetResult);
MAKEFACTORY(RowResult);
MAKEFACTORY(ChildIterator);

extern HTHOR_API IHThorActivity *createDummyActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &arg, ThorActivityKind kind)
{
    return new CHThorDummyActivity(_agent, _activityId, _subgraphId, arg, kind);
}

MAKEFACTORY_EXTRA(WhenAction,EclGraphElement *)
MAKEFACTORY_EXTRA(LibraryCall, IPropertyTree *)
MAKEFACTORY(ChildNormalize)
MAKEFACTORY(ChildAggregate)
MAKEFACTORY(ChildGroupAggregate)
MAKEFACTORY(ChildThroughNormalize)

MAKEFACTORY(DiskRead)
MAKEFACTORY(DiskNormalize)
MAKEFACTORY(DiskAggregate)
MAKEFACTORY(DiskCount)
MAKEFACTORY(DiskGroupAggregate)
MAKEFACTORY(CsvRead)
MAKEFACTORY(XmlRead)

MAKEFACTORY_EXTRA(LocalResultRead, __int64)
MAKEFACTORY_EXTRA(LocalResultWrite, __int64)
MAKEFACTORY_EXTRA(DictionaryResultWrite, __int64)
MAKEFACTORY_EXTRA(LocalResultSpill, __int64)
MAKEFACTORY_EXTRA(GraphLoopResultRead, __int64)
MAKEFACTORY_EXTRA(GraphLoopResultWrite, __int64)
MAKEFACTORY_EXTRA(NWayGraphLoopResultRead, __int64)

MAKEFACTORY(Combine)
MAKEFACTORY(RollupGroup)
MAKEFACTORY(Regroup)
MAKEFACTORY(CombineGroup)
MAKEFACTORY(Case)
MAKEFACTORY(LinkedRawIterator)
MAKEFACTORY(GraphLoop)
MAKEFACTORY(Loop)
MAKEFACTORY(Process)
MAKEFACTORY(Grouped)
MAKEFACTORY(Sorted)
MAKEFACTORY(Trace)
MAKEFACTORY(NWayInput)
MAKEFACTORY(NWaySelect)
MAKEFACTORY(NonEmpty)
MAKEFACTORY(FilterGroup);
MAKEFACTORY(StreamedIterator);
MAKEFACTORY_EXTRA(External, IPropertyTree *);

IHThorException * makeHThorException(ThorActivityKind kind, unsigned activityId, unsigned subgraphId, int code, char const * format, ...)
{
    va_list args;
    va_start(args, format);
    IHThorException * ret = new CHThorException(code, format, args, MSGAUD_user, kind, activityId, subgraphId);
    va_end(args);
    return ret;
}

IHThorException * makeHThorException(ThorActivityKind kind, unsigned activityId, unsigned subgraphId, IException * exc)
{
    return new CHThorException(exc, kind, activityId, subgraphId);
}

IHThorException * makeHThorException(ThorActivityKind kind, unsigned activityId, unsigned subgraphId, IException * exc, char const * extra)
{
    return new CHThorException(exc, extra, kind, activityId, subgraphId);
}
