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

#ifndef _THDISKBASESLAVE_IPP
#define _THDISKBASESLAVE_IPP

#include "jio.hpp"
#include "jhash.hpp"

#include "dautils.hpp"

#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"
#include "slave.ipp"

class CDiskReadSlaveActivityBase;
class CDiskPartHandlerBase : public CPartHandler, implements IThorDiskCallback
{
protected:
    OwnedIFile iFile;
    Owned<IPartDescriptor> partDesc;
    bool compressed, blockCompressed, firstInGroup, checkFileCrc;
    unsigned storedCrc, which;
    StringAttr filename, logicalFilename;
    unsigned __int64 fileBaseOffset;
    const char *kindStr;
    CRuntimeStatisticCollection fileStats;
    CDiskReadSlaveActivityBase &activity;
    CriticalSection statsCs;

    bool eoi;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDiskPartHandlerBase(CDiskReadSlaveActivityBase &activity);
    virtual void close(CRC32 &fileCRC) = 0;
    virtual void open();

    virtual void stop();
    virtual const void *nextRow() = 0;
    virtual void gatherStats(CRuntimeStatisticCollection & merged)
    {
        merged.merge(fileStats);
    }

// IThorDiskCallback
    virtual offset_t getFilePosition(const void * row);
    virtual offset_t getLocalFilePosition(const void * row);
    virtual const char * queryLogicalFilename(const void * row);

// CPartHandler
    virtual void setPart(IPartDescriptor *_partDesc, unsigned partNoSerialized);

    virtual offset_t getLocalOffset()=0;
};

//////////////////////////////////////////////

void getPartsMetaInfo(ThorDataLinkMetaInfo &metaInfo, CThorDataLink &link, unsigned nparts, IPartDescriptor **partDescs, CDiskPartHandlerBase *partHandler);

//////////////////////////////////////////////

class CDiskReadSlaveActivityBase : public CSlaveActivity
{
    Owned<IRowInterfaces> diskRowIf;
protected:
    StringAttr logicalFilename;
    StringArray subfileLogicalFilenames;
    IArrayOf<IPartDescriptor> partDescs;
    IHThorDiskReadBaseArg *helper;
    bool checkFileCrc, gotMeta, crcCheckCompressed, markStart;
    ThorDataLinkMetaInfo cachedMetaInfo;
    Owned<CDiskPartHandlerBase> partHandler;
    Owned<IExpander> eexp;
    rowcount_t diskProgress;

public:
    CDiskReadSlaveActivityBase(CGraphElementBase *_container);
    const char *queryLogicalFilename(unsigned index);
    IRowInterfaces * queryDiskRowInterfaces();
    void start();

    
// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData);
    virtual void kill();
    virtual void serializeStats(MemoryBuffer &mb);
friend class CDiskPartHandlerBase;
};

class CDiskWriteSlaveActivityBase : public ProcessSlaveActivity, implements ICopyFileProgress
{
protected:
    IHThorDiskWriteArg *diskHelperBase;
    Owned<IFileIO> outputIO;
    Owned<IExtRowWriter> out;
    Owned<IFileIOStream> outraw;
    Owned<IPartDescriptor> partDesc;
    StringAttr fName;
    CRC32 fileCRC;
    bool compress, grouped, calcFileCrc, rfsQueryParallel;
    offset_t uncompressedBytesWritten;
    unsigned replicateDone;
    Owned<ICompressor> ecomp;
    Owned<IThorDataLink> input;
    unsigned usageCount;
    CDfsLogicalFileName dlfn;
    StringBuffer tempExternalName;
    CRuntimeStatisticCollection fileStats;
    CriticalSection statsCs;

    void open();
    void removeFiles();
    void close();
    virtual void write() = 0;

public:
    CDiskWriteSlaveActivityBase(CGraphElementBase *container);
    void init(MemoryBuffer &data, MemoryBuffer &slaveData);
    virtual void abort();
    virtual void serializeStats(MemoryBuffer &mb);

// ICopyFileProgress
    virtual CFPmode onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize);

// IThorSlaveProcess overloaded methods
    virtual void kill();
    virtual void process();
    virtual void endProcess();
    virtual void processDone(MemoryBuffer &mb);
    virtual bool wantRaw() { return false; }
};

rowcount_t getFinalCount(CSlaveActivity &activity);
void sendPartialCount(CSlaveActivity &activity, rowcount_t partialCount);

class CPartialResultAggregator
{
    CSlaveActivity &activity;
public:
    CPartialResultAggregator(CSlaveActivity &_activity) : activity(_activity) { }
    const void *getResult();
    void sendResult(const void *row);
    void cancelGetResult();
};

#endif // _THDISKBASESLAVE_IPP
