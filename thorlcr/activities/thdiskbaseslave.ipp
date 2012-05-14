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

#ifndef _THDISKBASESLAVE_IPP
#define _THDISKBASESLAVE_IPP

#include "jio.hpp"
#include "jhash.hpp"

#include "dautils.hpp"

#include "thcrc.hpp"
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

    CDiskReadSlaveActivityBase &activity;

    bool eoi;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDiskPartHandlerBase(CDiskReadSlaveActivityBase &activity);
    virtual void close(CRC32 &fileCRC) = 0;
    virtual void open();

    virtual void stop();
    virtual const void *nextRow() = 0;

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
    Owned<IExtRowWriter> out;
    Owned<IFileIOStream> outraw;
    Owned<IPartDescriptor> partDesc;
    StringAttr logicalFilename, fName;
    CRC32 fileCRC;
    bool compress, grouped, calcFileCrc, rfsQueryParallel;
    offset_t uncompressedBytesWritten;
    unsigned replicateDone;
    Owned<ICompressor> ecomp;
    Owned<IThorDataLink> input;
    unsigned usageCount;
    CDfsLogicalFileName dlfn;
    StringBuffer tempExternalName;

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
