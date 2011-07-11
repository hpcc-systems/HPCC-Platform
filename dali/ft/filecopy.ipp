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

#ifndef FILECOPY_IPP
#define FILECOPY_IPP

#include "jptree.hpp"
#include "jlog.hpp"
#include "filecopy.hpp"
#include "ftbase.ipp"
#include "daft.hpp"
#include "daftformat.ipp"


//----------------------------------------------------------------------------

class SimplePartFilter : public CInterface, implements IDFPartFilter
{
public:
    SimplePartFilter(IDFPartFilter * _nextFilter)   { nextFilter.set(_nextFilter); }
    IMPLEMENT_IINTERFACE

    virtual bool includePart(unsigned part)     
    { 
        if (nextFilter && !nextFilter->includePart(part))
            return false;
        if (include.isItem(part)) 
            return include.item(part); 
        return false;
     }

public:
    Owned<IDFPartFilter> nextFilter;
    BoolArray   include;
};

//----------------------------------------------------------------------------

class FileSprayer;
class FileTransferThread : public Thread, implements IAbortRequestCallback
{
public:
    FileTransferThread(FileSprayer & _sprayer, byte _action, const SocketEndpoint & _ep, bool _calcCRC, const char *_wuid);

    void addPartition(PartitionPoint & nextPartition, OutputProgress & nextProgress);
    unsigned __int64 getInputSize();
    void go(Semaphore & _sem);
    void logIfRunning(StringBuffer &list);
    void setErrorOwn(IException * e);

    virtual int run();
    virtual bool abortRequested() { return isAborting(); }
    
protected:
    bool catchReadBuffer(ISocket * socket, MemoryBuffer & msg, unsigned timeout);

public:
    Linked<IException>          error;
    bool                        ok;

protected:
    bool isAborting();
    bool performTransfer();
    bool transferAndSignal();

protected:
    CachedPasswordProvider      passwordProvider;
    FileSprayer &               sprayer;
    SocketEndpoint              ep;
    PartitionPointArray         partition;
    OutputProgressArray         progress;
    Semaphore *                 sem;
    byte                        action;
    bool                        calcCRC;
    LogMsgJobInfo               job;
    bool                        allDone;
    bool                        started;
    StringAttr                  wuid;
};

typedef CIArrayOf<FileTransferThread> TransferArray;

//----------------------------------------------------------------------------

#define UNKNOWN_PART_SIZE       ((offset_t)-1)
struct FilePartInfo : public CInterface
{
public:
    FilePartInfo(const RemoteFilename & _filename);
    FilePartInfo();

    bool canPush();
    void extractExtra(IPartDescriptor &part);
    void extractExtra(IDistributedFilePart &part);

private:
    void init();

public:
    RemoteFilename          filename;
    RemoteFilename          mirrorFilename;
    offset_t                offset;
    offset_t                size;               // expanded size
    offset_t                psize;              // physical (compressed) size
    unsigned                headerSize;
    offset_t                xmlHeaderLength;
    offset_t                xmlFooterLength;
    unsigned                crc;
    CDateTime               modifiedTime;
    bool                    hasCRC;
};

typedef CIArrayOf<FilePartInfo> FilePartInfoArray;


//----------------------------------------------------------------------------

class DALIFT_API TargetLocation : public CInterface
{
public:
    TargetLocation() { }
    TargetLocation(RemoteFilename & _filename) : filename(_filename) { }

    bool                canPull();
    const IpAddress &   queryIP()           { return filename.queryIP(); }

public:
    RemoteFilename      filename;
    CDateTime           modifiedTime;
};
typedef CIArrayOf<TargetLocation> TargetLocationArray;


//----------------------------------------------------------------------------

class FileSizeThread : public Thread
{
public:
    FileSizeThread(FilePartInfoArray & _queue, CriticalSection & _cs, bool _isCompressed, bool _errorIfMissing);

    virtual int run();
            bool wait(unsigned timems);

            void queryThrowError()  { if (error) throw error.getLink(); }

protected:
    Semaphore                   sem;
    Linked<IException>          error;
    FilePartInfoArray &         queue;
    CriticalSection &           cs;
    bool                        errorIfMissing;
    bool                        isCompressed;
    Owned<FilePartInfo>         cur;
    unsigned                    copy;
};

//----------------------------------------------------------------------------

typedef Linked<IDistributedFile> DistributedFileAttr;

class FileSprayer : public CInterface, public IFileSprayer
{
    friend class FileTransferThread;
    friend class AsyncAfterTransfer;
    friend class AsyncExtractBlobInfo;
public:
    FileSprayer(IPropertyTree * _options, IPropertyTree * _progress, IRemoteConnection * _recoveryConnection, const char *_wuid);
    IMPLEMENT_IINTERFACE

    virtual void removeSource();
    virtual void setPartFilter(IDFPartFilter * _filter);
    virtual void setAbort(IAbortRequestCallback * _abort);
    virtual void setProgress(IDaftProgress * _progress);
    virtual void setReplicate(bool _replicate);
    virtual void setSource(IDistributedFile * source);
    virtual void setSource(IFileDescriptor * source);
    virtual void setSource(IDistributedFilePart * part);
    virtual void setSourceTarget(IFileDescriptor * fd, DaftReplicateMode mode);
    virtual void setTarget(IDistributedFile * target);
    virtual void setTarget(IFileDescriptor * target, unsigned copy);
    virtual void setTarget(IGroup * target);
    virtual void setTarget(INode * target);
    virtual void spray();

    void updateProgress(const OutputProgress & newProgress);
    unsigned numParallelSlaves();
    void setError(const SocketEndpoint & ep, IException * e);
    bool canLocateSlaveForNode(const IpAddress &ip);

protected:
    void addEmptyFilesToPartition(unsigned from, unsigned to);
    void addEmptyFilesToPartition();
    void addHeaderFooter(size32_t len, const void * data, unsigned idx, bool before);
    void addHeaderFooter(const char * data, unsigned idx, bool before);
    void addTarget(unsigned idx, INode * node);
    void afterGatherFileSizes();
    void afterTransfer();
    bool allowSplit();
    void analyseFileHeaders(bool setcurheadersize);
    void assignPartitionFilenames();
    void beforeTransfer();
    bool calcCRC();
    bool calcInputCRC();
    unsigned __int64 calcSizeReadAlready();
    void calculateOne2OnePartition();
    void calculateMany2OnePartition();
    void calculateNoSplitPartition();
    void calculateSplitPrefixPartition(const char * splitPrefix);
    void calculateSprayPartition();
    void calculateOutputOffsets();
    void calibrateProgress();
    void checkFormats();
    void checkForOverlap();
    void cleanupRecovery();
    void cloneHeaderFooter(unsigned idx, bool isHeader);
    void commonUpSlaves();
    PartitionPoint & createLiteral(size32_t len, const void * data, unsigned idx);
    void derivePartitionExtra();
    bool disallowImplicitReplicate();
    void displayPartition();
    void expandTarget();
    void extractSourceFormat(IPropertyTree * props);
    void gatherFileSizes(bool errorIfMissing);
    void gatherFileSizes(FilePartInfoArray & fileSizeQueue, bool errorIfMissing);
    void gatherMissingSourceTarget(IFileDescriptor * source);
    unsigned __int64 getSizeReadAlready();
    void insertHeaders();
    bool isAborting();
    void locateXmlHeader(IFileIO * io, unsigned headerSize, offset_t & xmlHeaderLength, offset_t & xmlFooterLength);
    bool needToCalcOutput();
    unsigned numParallelConnections(unsigned limit);
    void performTransfer();
    void pullParts();
    void pushParts();
    const char * queryFixedSlave();
    const char * querySlaveExecutable(const IpAddress &ip, StringBuffer &ret);
    const char * querySplitPrefix();
    bool restorePartition();
    void savePartition();
    void setCopyCompressedRaw();
    void setSource(IFileDescriptor * source, unsigned copy, unsigned mirrorCopy = (unsigned)-1);
    void updateTargetProperties();
    bool usePullOperation();
    void updateSizeRead();
    void waitForTransferSem(Semaphore & sem);

private:
    bool calcUsePull();

protected:
    CIArrayOf<FilePartInfo> sources;
    Linked<IDistributedFile> distributedTarget;
    Linked<IDistributedFile> distributedSource;
    TargetLocationArray     targets;
    FileFormat              srcFormat;
    FileFormat              tgtFormat;
    Owned<IDFPartFilter>    filter;
    Linked<IPropertyTree>   options;
    Linked<IPropertyTree>   progressTree;
    IRemoteConnection *     recoveryConnection;
    PartitionPointArray     partition;
    OutputProgressArray     progress;
    IDaftProgress *         progressReport;
    IAbortRequestCallback * abortChecker;
    LogMsgJobInfo           job;
    offset_t                totalSize;
    unsigned __int64        sizeToBeRead;
    bool                    replicate;
    bool                    unknownSourceFormat;
    bool                    unknownTargetFormat;
    Owned<IException>       error;
    TransferArray           transferSlaves;
    CriticalSection         soFarCrit;
    CriticalSection         errorCS;
    Owned<IPropertyTree>    srcAttr;
    unsigned                lastAbortCheckTick;
    unsigned                lastSDSTick;
    unsigned                lastOperatorTick;
    unsigned                numSlavesCompleted;
    bool                    calcedPullPush;
    bool                    cachedUsePull;
    bool                    cachedInputCRC;
    bool                    calcedInputCRC;
    bool                    isRecovering;
    bool                    allowRecovery;
    bool                    isSafeMode;
    bool                    mirroring;
    bool                    aborting;
    bool                    compressedInput;
    bool                    compressOutput;
    bool                    copyCompressed;
    unsigned __int64        totalLengthRead;
    unsigned                throttleNicSpeed;
    unsigned                lastProgressTick;
    StringAttr              wuid; // used for logging
    bool                    progressDone;  // set true once done to prevent excessive progress calls
    size32_t                transferBufferSize;
    StringAttr              encryptKey;
    StringAttr              decryptKey;
};



#endif
