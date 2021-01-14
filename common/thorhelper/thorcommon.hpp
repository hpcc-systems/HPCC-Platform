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

#ifndef THORCOMMON_HPP
#define THORCOMMON_HPP

#include "jiface.hpp"
#include "jcrc.hpp"
#include "jlzw.hpp"
#include "jsort.hpp"
#include "jdebug.hpp"
#include "jfile.hpp"
#include "eclhelper.hpp"
#include "rtldynfield.hpp"
#include "thorhelper.hpp"
#include "thorxmlwrite.hpp"

static unsigned const defaultDaliResultLimit = 10; // MB
static unsigned const defaultMaxCsvRowSize = 10; // MB


#define OPT_OUTPUTLIMIT_LEGACY    "outputLimit"             // OUTPUT Mb limit (legacy property name, renamed to outputLimitMb in 5.2)
#define OPT_OUTPUTLIMIT           "outputLimitMb"           // OUTPUT Mb limit                                                               (default = 10 [MB])
#define OPT_MAXCSVROWSIZE         "maxCsvRowSizeMb"         // Upper limit on csv read line size                                             (default = 10 [MB])
#define OPT_VALIDATE_FILE_TYPE    "validateFileType"        // Validate that diskread file type matches                                      (default = true)


class THORHELPER_API CSizingSerializer : implements IRowSerializerTarget
{
    size32_t totalsize;
public:
    inline CSizingSerializer() { reset(); }
    inline void reset() { totalsize = 0; }
    inline size32_t size() { return totalsize; }
    virtual void put(size32_t len, const void * ptr);
    virtual size32_t beginNested(size32_t count);
    virtual void endNested(size32_t position);
};

class THORHELPER_API CMemoryRowSerializer: implements IRowSerializerTarget
{
    MemoryBuffer & buffer;
    unsigned nesting;
public:
    inline CMemoryRowSerializer(MemoryBuffer & _buffer) 
        : buffer(_buffer)
    {
        nesting = 0;
    }
    virtual void put(size32_t len, const void * ptr);
    virtual size32_t beginNested(size32_t count);
    virtual void endNested(size32_t sizePos);
};


// useful package
interface IRowInterfaces: extends IInterface
{
    virtual IEngineRowAllocator * queryRowAllocator()=0;  
    virtual IOutputRowSerializer * queryRowSerializer()=0; 
    virtual IOutputRowDeserializer * queryRowDeserializer()=0; 
    virtual IOutputMetaData *queryRowMetaData()=0;
    virtual unsigned queryActivityId() const=0;
    virtual ICodeContext *queryCodeContext()=0;
};

extern THORHELPER_API void useMemoryMappedRead(bool on);

extern THORHELPER_API IRowInterfaces *createRowInterfaces(IOutputMetaData *meta, unsigned actid, unsigned heapFlags, ICodeContext *context);


enum RowReaderWriterFlags
{
    rw_grouped        = 0x1,
    rw_crc            = 0x2,
    rw_extend         = 0x4,
    rw_compress       = 0x8,
    rw_compressblkcrc = 0x10, // block compression, this sets/checks crc's at block level
    rw_fastlz         = 0x20, // if rw_compress
    rw_autoflush      = 0x40,
    rw_buffered       = 0x80,
    rw_lzw            = 0x100, // if rw_compress
    rw_lz4            = 0x200, // if rw_compress
    rw_sparse         = 0x400, // NB: mutually exclusive with rw_grouped
    rw_lz4hc          = 0x800  // if rw_compress
};
#define DEFAULT_RWFLAGS (rw_buffered|rw_autoflush|rw_compressblkcrc)
inline bool TestRwFlag(unsigned flags, RowReaderWriterFlags flag) { return 0 != (flags & flag); }

#define COMP_MASK (rw_compress|rw_compressblkcrc|rw_fastlz|rw_lzw|rw_lz4|rw_lz4hc)
#define COMP_TYPE_MASK (rw_fastlz|rw_lzw|rw_lz4|rw_lz4hc)
inline void setCompFlag(const char *compStr, unsigned &flags)
{
    flags &= ~COMP_TYPE_MASK;
    if (!isEmptyString(compStr))
    {
        if (0 == stricmp("FLZ", compStr))
            flags |= rw_fastlz;
        else if (0 == stricmp("LZW", compStr))
            flags |= rw_lzw;
        else if (0 == stricmp("LZ4HC", compStr))
            flags |= rw_lz4hc;
        else // not specifically FLZ, LZW, or FL4HC so set to default LZ4
            flags |= rw_lz4;
    }
    else // default is LZ4
        flags |= rw_lz4;
}

inline unsigned getCompMethod(unsigned flags)
{
    unsigned compMethod = COMPRESS_METHOD_LZ4;
    if (TestRwFlag(flags, rw_lzw))
        compMethod = COMPRESS_METHOD_LZW;
    else if (TestRwFlag(flags, rw_fastlz))
        compMethod = COMPRESS_METHOD_FASTLZ;
    else if (TestRwFlag(flags, rw_lz4hc))
        compMethod = COMPRESS_METHOD_LZ4HC;

    return compMethod;
}

inline unsigned getCompMethod(const char *compStr)
{
    unsigned compMethod = COMPRESS_METHOD_LZ4;
    if (!isEmptyString(compStr))
    {
        if (0 == stricmp("FLZ", compStr))
            compMethod = COMPRESS_METHOD_FASTLZ;
        else if (0 == stricmp("LZW", compStr))
            compMethod = COMPRESS_METHOD_LZW;
        else if (0 == stricmp("LZ4HC", compStr))
            compMethod = COMPRESS_METHOD_LZ4HC;
    }
    return compMethod;
}

interface IExtRowStream: extends IRowStream
{
    virtual offset_t getOffset() const = 0;
    virtual offset_t getLastRowOffset() const = 0;
    virtual unsigned __int64 queryProgress() const = 0;
    using IRowStream::stop;
    virtual void stop(CRC32 *crcout) = 0;
    virtual const byte *prefetchRow() = 0;
    virtual void prefetchDone() = 0;
    virtual void reinit(offset_t offset,offset_t len,unsigned __int64 maxrows) = 0;
    virtual unsigned __int64 getStatistic(StatisticKind kind) = 0;
    virtual void setFilters(IConstArrayOf<IFieldFilter> &filters) = 0;
};

interface IExtRowWriter: extends IRowWriter
{
    virtual offset_t getPosition() = 0;
    using IRowWriter::flush;
    virtual void flush(CRC32 *crcout) = 0;
};

enum EmptyRowSemantics { ers_forbidden, ers_allow, ers_eogonly };
inline unsigned mapESRToRWFlags(EmptyRowSemantics emptyRowSemantics)
{
    switch (emptyRowSemantics)
    {
        case ers_allow:
            return rw_sparse;
        case ers_eogonly:
            return rw_grouped;
        default:
            return 0;
    }
}

inline EmptyRowSemantics extractESRFromRWFlags(unsigned rwFlags)
{
    if (TestRwFlag(rwFlags, rw_sparse))
        return ers_allow;
    else if (TestRwFlag(rwFlags, rw_grouped))
        return ers_eogonly;
    else
        return ers_forbidden;
}

interface ITranslator : extends IInterface
{
    virtual IOutputMetaData &queryActualFormat() const = 0;
    virtual const IDynamicTransform &queryTranslator() const = 0;
    virtual const IKeyTranslator *queryKeyedTranslator() const = 0;
};
interface IExpander;
extern THORHELPER_API IExtRowStream *createRowStreamEx(IFileIO *fileIO, IRowInterfaces *rowIf, offset_t offset, offset_t len, unsigned __int64 maxrows, unsigned rwFlags, ITranslator *translatorContainer=nullptr, IVirtualFieldCallback * _fieldCallback=nullptr);
extern THORHELPER_API IExtRowStream *createRowStream(IFile *file, IRowInterfaces *rowif, unsigned flags=DEFAULT_RWFLAGS, IExpander *eexp=nullptr, ITranslator *translatorContainer=nullptr, IVirtualFieldCallback * _fieldCallback=nullptr);
extern THORHELPER_API IExtRowStream *createRowStreamEx(IFile *file, IRowInterfaces *rowif, offset_t offset=0, offset_t len=(offset_t)-1, unsigned __int64 maxrows=(unsigned __int64)-1, unsigned flags=DEFAULT_RWFLAGS, IExpander *eexp=nullptr, ITranslator *translatorContainer=nullptr, IVirtualFieldCallback * _fieldCallback = nullptr);
interface ICompressor;
extern THORHELPER_API IExtRowWriter *createRowWriter(IFile *file, IRowInterfaces *rowIf, unsigned flags=DEFAULT_RWFLAGS, ICompressor *compressor=NULL, size32_t compressorBlkSz=0);
extern THORHELPER_API IExtRowWriter *createRowWriter(IFileIO *fileIO, IRowInterfaces *rowIf, unsigned flags=DEFAULT_RWFLAGS, size32_t compressorBlkSz=0);
extern THORHELPER_API IExtRowWriter *createRowWriter(IFileIOStream *strm, IRowInterfaces *rowIf, unsigned flags=DEFAULT_RWFLAGS); // strm should be unbuffered

interface THORHELPER_API IDiskMerger : extends IInterface
{
    virtual void put(const void **rows, unsigned numrows) = 0;
    virtual void putIndirect(const void ***rowptrs, unsigned numrows) = 0; // like put only with an additional dereference, i.e. row i is *(rowptrs[i])
    virtual void put(ISortedRowProvider * rows) = 0;
    virtual IRowStream *merge(ICompare *icompare,bool partdedup=false) = 0;
    virtual count_t mergeTo(IRowWriter *dest,ICompare *icompare,bool partdedup=false) = 0; // alternative to merge
    virtual IRowWriter *createWriteBlock() = 0;
};

extern THORHELPER_API IDiskMerger *createDiskMerger(IRowInterfaces *rowInterfaces, IRowLinkCounter *linker, const char *tempnamebase);


#ifdef HAS_GOOD_CYCLE_COUNTER
 #define TIME_ACTIVITIES
#endif

class THORHELPER_API ActivityTimeAccumulator
{
    friend class ActivityTimer;
public:
    ActivityTimeAccumulator()
    {
        reset();
    }
public:
    cycle_t startCycles; // Wall clock time of first entry to this activity
    cycle_t totalCycles; // Time spent in this activity
    cycle_t endCycles;   // Wall clock time of last entry to this activity
    unsigned __int64 firstRow; // Timestamp of first row (nanoseconds since epoch)
    cycle_t firstExitCycles;    // Wall clock time of first exit from this activity
    cycle_t blockedCycles;  // Time spent blocked

    // Return the total amount of time (in nanoseconds) spent in this activity (first entry to last exit)
    inline unsigned __int64 elapsed() const { return cycle_to_nanosec(endCycles-startCycles); }
    // Return the total amount of time (in nanoseconds) spent in the first call of this activity (first entry to first exit)
    inline unsigned __int64 latency() const { return cycle_to_nanosec(latencyCycles()); }
    inline cycle_t latencyCycles() const { return firstExitCycles-startCycles; }

    void addStatistics(IStatisticGatherer & builder) const;
    void addStatistics(CRuntimeStatisticCollection & merged) const;
    void merge(const ActivityTimeAccumulator & other);

    void reset()
    {
        startCycles = 0;
        totalCycles = 0;
        endCycles = 0;
        firstRow = 0;
        firstExitCycles = 0;
        blockedCycles = 0;
    }
};

#ifdef TIME_ACTIVITIES

class ActivityTimer
{
    unsigned __int64 startCycles;
    ActivityTimeAccumulator &accumulator;
protected:
    const bool enabled;
    bool isFirstRow;
public:
    ActivityTimer(ActivityTimeAccumulator &_accumulator, const bool _enabled)
    : accumulator(_accumulator), enabled(_enabled), isFirstRow(false)
    {
        if (enabled)
        {
            startCycles = get_cycles_now();
            if (!accumulator.firstRow)
            {
                isFirstRow = true;
                accumulator.startCycles = startCycles;
                accumulator.firstRow = getTimeStampNowValue();
            }
        }
        else
            startCycles = 0;
    }

    ~ActivityTimer()
    {
        if (enabled)
        {
            cycle_t nowCycles = get_cycles_now();
            accumulator.endCycles = nowCycles;
            cycle_t elapsedCycles = nowCycles - startCycles;
            accumulator.totalCycles += elapsedCycles;
            if (isFirstRow)
                accumulator.firstExitCycles = nowCycles;
        }
    }
};

class SimpleActivityTimer
{
    cycle_t startCycles;
    cycle_t &accumulator;
protected:
    const bool enabled;
public:
    inline SimpleActivityTimer(cycle_t &_accumulator, const bool _enabled)
    : accumulator(_accumulator), enabled(_enabled)
    {
        if (enabled)
            startCycles = get_cycles_now();
        else
            startCycles = 0;
    }

    inline ~SimpleActivityTimer()
    {
        if (enabled)
        {
            cycle_t nowCycles = get_cycles_now();
            cycle_t elapsedCycles = nowCycles - startCycles;
            accumulator += elapsedCycles;
        }
    }
};

class BlockedActivityTimer
{
    unsigned __int64 startCycles;
    ActivityTimeAccumulator &accumulator;
protected:
    const bool enabled;
public:
    BlockedActivityTimer(ActivityTimeAccumulator &_accumulator, const bool _enabled)
        : accumulator(_accumulator), enabled(_enabled)
    {
        if (enabled)
            startCycles = get_cycles_now();
        else
            startCycles = 0;
    }

    ~BlockedActivityTimer()
    {
        if (enabled)
        {
            cycle_t elapsedCycles = get_cycles_now() - startCycles;
            accumulator.blockedCycles += elapsedCycles;
        }
    }
};
#else
struct ActivityTimer
{
    inline ActivityTimer(ActivityTimeAccumulator &_accumulator, const bool _enabled) { }
};
struct SimpleActivityTimer
{
    inline SimpleActivityTimer(cycle_t &_accumulator, const bool _enabled) { }
};
struct BlockedActivityTimer
{
    inline BlockedActivityTimer(ActivityTimeAccumulator &_accumulator, const bool _enabled) { }
};
#endif

class THORHELPER_API IndirectCodeContext : implements ICodeContext
{
public:
    IndirectCodeContext(ICodeContext * _ctx = NULL) : ctx(_ctx) {}

    void set(ICodeContext * _ctx) { ctx = _ctx; }

    virtual const char *loadResource(unsigned id)
    {
        return ctx->loadResource(id);
    }
    virtual void setResultBool(const char *name, unsigned sequence, bool value)
    {
        ctx->setResultBool(name, sequence, value);
    }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data)
    {
        ctx->setResultData(name, sequence, len, data);
    }
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val)
    {
        ctx->setResultDecimal(stepname, sequence, len, precision, isSigned, val);
    }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size)
    {
        ctx->setResultInt(name, sequence, value, size);
    }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data)
    {
        ctx->setResultRaw(name, sequence, len, data);
    }
    virtual void setResultReal(const char * stepname, unsigned sequence, double value)
    {
        ctx->setResultReal(stepname, sequence, value);
    }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer)
    {
        ctx->setResultSet(name, sequence, isAll, len, data, transformer);
    }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str)
    {
        ctx->setResultString(name, sequence, len, str);
    }
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size)
    {
        ctx->setResultUInt(name, sequence, value, size);
    }
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str)
    {
        ctx->setResultUnicode(name, sequence, len, str);
    }
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value)
    {
        ctx->setResultVarString(name, sequence, value);
    }
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value)
    {
        ctx->setResultVarUnicode(name, sequence, value);
    }
    virtual bool getResultBool(const char * name, unsigned sequence)
    {
        return ctx->getResultBool(name, sequence);
    }
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence)
    {
        ctx->getResultData(tlen, tgt, name, sequence);
    }
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence)
    {
        ctx->getResultDecimal(tlen, precision, isSigned, tgt, stepname, sequence);
    }
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        ctx->getResultRaw(tlen, tgt, name, sequence, xmlTransformer, csvTransformer);
    }
    virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        ctx->getResultSet(isAll, tlen, tgt, name, sequence, xmlTransformer, csvTransformer);
    }
    virtual __int64 getResultInt(const char * name, unsigned sequence)
    {
        return ctx->getResultInt(name, sequence);
    }
    virtual double getResultReal(const char * name, unsigned sequence)
    {
        return ctx->getResultReal(name, sequence);
    }
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence)
    {
        ctx->getResultString(tlen, tgt, name, sequence);
    }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence)
    {
        ctx->getResultStringF(tlen, tgt, name, sequence);
    }
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence)
    {
        ctx->getResultUnicode(tlen, tgt, name, sequence);
    }
    virtual char *getResultVarString(const char * name, unsigned sequence)
    {
        return ctx->getResultVarString(name, sequence);
    }
    virtual UChar *getResultVarUnicode(const char * name, unsigned sequence)
    {
        return ctx->getResultVarUnicode(name, sequence);
    }
    virtual unsigned getResultHash(const char * name, unsigned sequence)
    {
        return ctx->getResultHash(name, sequence);
    }
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence)
    {
        return ctx->getExternalResultHash(wuid, name, sequence);
    }
    virtual char *getWuid()
    {
        return ctx->getWuid();
    }
    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        ctx->getExternalResultRaw(tlen, tgt, wuid, stepname, sequence, xmlTransformer, csvTransformer);
    }
    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract)
    {
        ctx->executeGraph(graphName, realThor, parentExtractSize, parentExtract);
    }
    virtual char * getExpandLogicalName(const char * logicalName)
    {
        return ctx->getExpandLogicalName(logicalName);
    }
    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char *source)
    {
        ctx->addWuException(text, code, severity, source);
    }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort)
    {
        ctx->addWuAssertFailure(code, text, filename, lineno, column, isAbort);
    }
    virtual IUserDescriptor *queryUserDescriptor()
    {
        return ctx->queryUserDescriptor();
    }
    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal)
    {
        return ctx->resolveChildQuery(activityId, colocal);
    }
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash)
    {
        return ctx->getDatasetHash(name, hash);
    }
    virtual unsigned getNodes()
    {
        return ctx->getNodes();
    }
    virtual unsigned getNodeNum()
    {
        return ctx->getNodeNum();
    }
    virtual char *getFilePart(const char *logicalPart, bool create)
    {
        return ctx->getFilePart(logicalPart, create);
    }
    virtual unsigned __int64 getFileOffset(const char *logicalPart)
    {
        return ctx->getFileOffset(logicalPart);
    }
    virtual IDistributedFileTransaction *querySuperFileTransaction()
    {
        return ctx->querySuperFileTransaction();
    }
    virtual char *getEnv(const char *name, const char *defaultValue) const 
    {
        return ctx->getEnv(name, defaultValue); 
    }
    virtual char *getJobName()
    {
        return ctx->getJobName();
    }
    virtual char *getJobOwner()
    {
        return ctx->getJobOwner();
    }
    virtual char *getClusterName()
    {
        return ctx->getClusterName();
    }
    virtual char *getGroupName()
    {
        return ctx->getGroupName();
    }
    virtual char * queryIndexMetaData(char const * lfn, char const * xpath)
    {
        return ctx->queryIndexMetaData(lfn, xpath);
    }
    virtual unsigned getPriority() const
    {
        return ctx->getPriority();
    }
    virtual char *getPlatform()
    {
        return ctx->getPlatform();
    }
    virtual char *getOS()
    {
        return ctx->getOS();
    }
    virtual IEclGraphResults * resolveLocalQuery(__int64 activityId)
    {
        return ctx->resolveLocalQuery(activityId);
    }
    virtual char *getEnv(const char *name, const char *defaultValue)
    {
        return ctx->getEnv(name, defaultValue);
    }
    virtual unsigned logString(const char *text) const
    {
        return ctx->logString(text);
    }
    virtual const IContextLogger &queryContextLogger() const
    {
        return ctx->queryContextLogger();
    }
    virtual IDebuggableContext *queryDebugContext() const
    {
        return ctx->queryDebugContext();
    }
    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
    {
        return ctx->getRowAllocator(meta, activityId);
    }
    virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const
    {
        return ctx->getRowAllocatorEx(meta, activityId, heapFlags);
    }
    virtual const char *cloneVString(const char *str) const
    {
        return ctx->cloneVString(str);
    }
    virtual const char *cloneVString(size32_t len, const char *str) const
    {
        return ctx->cloneVString(len, str);
    }
    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override
    {
        ctx->getResultRowset(tcount, tgt, name, sequence, _rowAllocator, isGrouped, xmlTransformer, csvTransformer);
    }
    virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override
    {
        ctx->getResultDictionary(tcount, tgt, _rowAllocator, name, sequence, xmlTransformer, csvTransformer, hasher);
    }
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
    {
        convertRowToXML(lenResult, result, info, row, flags);
    }
    virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
    {
        convertRowToJSON(lenResult, result, info, row, flags);
    }
    virtual const void * fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
    {
        return ctx->fromXml(_rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
    }
    virtual const void * fromJson(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
    {
        return ctx->fromJson(_rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
    }
    virtual IEngineContext *queryEngineContext()
    {
        return ctx->queryEngineContext();
    }
    virtual char *getDaliServers()
    {
        return ctx->getDaliServers();
    }
    virtual IWorkUnit *updateWorkUnit() const
    {
        return ctx->updateWorkUnit();
    }
    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name)
    {
        return ctx->registerTimer(activityId, name);
    }
    virtual unsigned getGraphLoopCounter() const override
    {
        return ctx->getGraphLoopCounter();
    }
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned audience, const char *source) override
    {
        ctx->addWuExceptionEx(text, code, severity, audience, source);
    }
protected:
    ICodeContext * ctx;
};

extern THORHELPER_API bool isActivitySink(ThorActivityKind kind);
extern THORHELPER_API bool isActivitySource(ThorActivityKind kind);
extern THORHELPER_API const char * getActivityText(ThorActivityKind kind);

extern THORHELPER_API void setProcessAffinity(const char * cpus);
extern THORHELPER_API void setAutoAffinity(unsigned curProcess, unsigned processPerNode, const char * optNodes);
extern THORHELPER_API void bindMemoryToLocalNodes();

extern THORHELPER_API IOutputMetaData *getDaliLayoutInfo(IPropertyTree const &props);
extern THORHELPER_API bool getDaliLayoutInfo(MemoryBuffer &layoutBin, IPropertyTree const &props);

/* Returns a dynamic translator (as 1st parameter) given a generated expected format, the published format and the desired projectedFormat,
 * providing translation mode and crc's allow translation. Returns true if translator created.
 * NB: translator and keyedTranslator are expected to be empty before calling.
*/
extern THORHELPER_API bool getTranslators(Owned<const IDynamicTransform> &translator, const char *tracing, unsigned expectedCrc, IOutputMetaData *expectedFormat, unsigned publishedCrc, IOutputMetaData *publishedFormat, unsigned projectedCrc, IOutputMetaData *projectedFormat, RecordTranslationMode mode);
// Same as above, but will also return a key field translator in 2nd parameter. Returns true if translator created.
extern THORHELPER_API bool getTranslators(Owned<const IDynamicTransform> &translator, Owned<const IKeyTranslator> &keyedTranslator, const char *tracing, unsigned expectedCrc, IOutputMetaData *expectedFormat, unsigned publishedCrc, IOutputMetaData *publishedFormat, unsigned projectedCrc, IOutputMetaData *projectedFormat, RecordTranslationMode mode);
// Returns a ITranslator that gives access to a dynamic translator, keyed translator and the format used
extern THORHELPER_API ITranslator *getTranslators(const char *tracing, unsigned expectedCrc, IOutputMetaData *expectedFormat, unsigned publishedCrc, IOutputMetaData *publishedFormat, unsigned projectedCrc, IOutputMetaData *projectedFormat, RecordTranslationMode mode);

inline bool isActivityCodeSigned(IPropertyTree &graphNode)
{
    if (!isEmptyString(graphNode.queryProp("att[@name=\"signedBy\"]/@value")))
        return true;
    return false;
}

#endif // THORHELPER_HPP
