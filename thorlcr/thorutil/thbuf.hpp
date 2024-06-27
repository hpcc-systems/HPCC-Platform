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

#ifndef __THBUF__
#define __THBUF__

#include "jiface.hpp"

#include "eclhelper.hpp"
#include "jqueue.tpp"
#include "jbuff.hpp"
#include "jcrc.hpp"
#include "thorcommon.hpp"
#include "thmem.hpp"

#ifdef GRAPH_EXPORTS
    #define graph_decl DECL_EXPORT
#else
    #define graph_decl DECL_IMPORT
#endif

typedef QueueOf<const void,true> ThorRowQueue;



struct CommonBufferRowRWStreamOptions
{
    offset_t storageBlockSize = 256 * 1024;             // block size of read/write streams
    size32_t minCompressionBlockSize = 256 * 1024;      // minimum block size for compression
    memsize_t totalCompressionBufferSize = 3000 * 1024; // compression buffer size of read streams (split between writer and outputs)
    memsize_t inMemMaxMem = 2000 * 1024;                // before spilling begins.
    offset_t writeAheadSize = 2000 * 1024;              // once spilling, maximum size to write ahead
    unsigned heapFlags = roxiemem::RHFunique|roxiemem::RHFblocked;
};

struct LookAheadOptions : CommonBufferRowRWStreamOptions
{
    LookAheadOptions()
    {
        // override defaults
        totalCompressionBufferSize = 2000 * 1024; // compression buffer size of read streams (split between writer and outputs)
    }
    offset_t tempFileGranularity = 1000 * 0x100000; // 1GB
};


interface ISmartRowBuffer: extends IRowStream
{
    virtual IRowWriter *queryWriter() = 0;
};

class CActivityBase;
extern graph_decl ISmartRowBuffer * createSmartBuffer(CActivityBase *activity, const char * tempname, 
                                                      size32_t buffsize, 
                                                      IThorRowInterfaces *rowif
                                                      ); 


extern graph_decl ISmartRowBuffer * createSmartInMemoryBuffer(CActivityBase *activity,
                                                      IThorRowInterfaces *rowIf,
                                                      size32_t buffsize);


extern graph_decl ISmartRowBuffer * createCompressedSpillingRowStream(CActivityBase *activity, const char * tempBasename, bool grouped, IThorRowInterfaces *rowif, const LookAheadOptions &options, ICompressHandler *compressHandler);

struct SharedRowStreamReaderOptions : public CommonBufferRowRWStreamOptions
{
    memsize_t inMemReadAheadGranularity = 128 * 1024;  // granularity (K) of read ahead
    rowcount_t inMemReadAheadGranularityRows = 64;     // granularity (rows) of read ahead. NB: whichever granularity is hit first
};
interface ISharedRowStreamReader : extends IInterface
{
    virtual IRowStream *queryOutput(unsigned output) = 0;
    virtual void cancel()=0;
    virtual void reset() = 0;
    virtual unsigned __int64 getStatistic(StatisticKind kind) const = 0;
};


// Multiple readers, one writer
interface ISharedSmartBufferCallback
{
    virtual void paged() = 0;
    virtual void blocked() = 0;
    virtual void unblocked() = 0;
};

interface ISharedSmartBufferRowWriter : extends IRowWriter
{
    virtual void putRow(const void *row, ISharedSmartBufferCallback *callback) = 0; // extended form of putRow, which signals when pages out via callback
};

interface ISharedSmartBuffer : extends ISharedRowStreamReader
{
    virtual ISharedSmartBufferRowWriter *getWriter() = 0;
};

extern graph_decl ISharedSmartBuffer *createSharedSmartMemBuffer(CActivityBase *activity, unsigned outputs, IThorRowInterfaces *rowif, unsigned buffSize=((unsigned)-1));
extern graph_decl ISharedSmartBuffer *createSharedSmartDiskBuffer(CActivityBase *activity, const char *tempname, unsigned outputs, IThorRowInterfaces *rowif);
extern graph_decl ISharedRowStreamReader *createSharedFullSpillingWriteAhead(CActivityBase *_activity, unsigned numOutputs, IRowStream *_input, bool _inputGrouped, const SharedRowStreamReaderOptions &options, IThorRowInterfaces *_rowIf, const char *tempFileName, ICompressHandler *compressHandler);

interface IRowWriterMultiReader : extends IRowWriter
{
    virtual IRowStream *getReader() = 0;
};

extern graph_decl IRowWriterMultiReader *createOverflowableBuffer(CActivityBase &activity, IThorRowInterfaces *rowif, EmptyRowSemantics emptyRowSemantics, bool shared=false, unsigned spillPriority=SPILL_PRIORITY_OVERFLOWABLE_BUFFER);
// NB first write all then read (not interleaved!)

// Multiple writers, one reader
interface IRowMultiWriterReader : extends IRowStream
{
    virtual IRowWriter *getWriter() = 0;
    virtual void abort() = 0;
};

#define DEFAULT_WR_READ_GRANULARITY 10000 // Amount reader extracts when empty to avoid contention with writer
#define DEFAULT_WR_WRITE_GRANULARITY 1000 // Amount writers buffer up before committing to output
extern graph_decl IRowMultiWriterReader *createSharedWriteBuffer(CActivityBase *activity, IThorRowInterfaces *rowif, unsigned limit, unsigned readGranularity=DEFAULT_WR_READ_GRANULARITY, unsigned writeGranularity=DEFAULT_WR_WRITE_GRANULARITY);


#endif
