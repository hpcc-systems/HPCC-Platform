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
#include "jsort.hpp"
#include "jdebug.hpp"
#include "jfile.hpp"
#include "eclhelper.hpp"
#include "thorhelper.hpp"
#include "thorxmlwrite.hpp"

static unsigned const defaultDaliResultOutputMax = 2000; // MB
static unsigned const defaultDaliResultLimit = 10; // MB
static unsigned const defaultMaxCsvRowSize = 10; // MB


#define OPT_OUTPUTLIMIT_LEGACY    "outputLimit"             // OUTPUT Mb limit (legacy property name, renamed to outputLimitMb in 5.2)
#define OPT_OUTPUTLIMIT           "outputLimitMb"           // OUTPUT Mb limit                                                               (default = 10 [MB])
#define OPT_MAXCSVROWSIZE         "maxCsvRowSizeMb"         // Upper limit on csv read line size                                             (default = 10 [MB])


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

extern THORHELPER_API IRowInterfaces *createRowInterfaces(IOutputMetaData *meta, unsigned actid, ICodeContext *context);


enum RowReaderWriterFlags
{
    rw_grouped        = 0x1,
    rw_crc            = 0x2,
    rw_extend         = 0x4,
    rw_compress       = 0x8,
    rw_compressblkcrc = 0x10, // block compression, this sets/checks crc's at block level
    rw_fastlz         = 0x20, // if rw_compress
    rw_autoflush      = 0x40,
    rw_buffered       = 0x80
};
#define DEFAULT_RWFLAGS (rw_buffered|rw_autoflush|rw_compressblkcrc)
inline bool TestRwFlag(unsigned flags, RowReaderWriterFlags flag) { return 0 != (flags & flag); }

interface IExtRowStream: extends IRowStream
{
    virtual offset_t getOffset() = 0;
    virtual void stop(CRC32 *crcout=NULL) = 0;
    virtual const void *prefetchRow(size32_t *sz=NULL) = 0;
    virtual void prefetchDone() = 0;
    virtual void reinit(offset_t offset,offset_t len,unsigned __int64 maxrows) = 0;
    virtual unsigned __int64 getStatistic(StatisticKind kind) = 0;
};

interface IExtRowWriter: extends IRowWriter
{
    virtual offset_t getPosition() = 0;
    virtual void flush(CRC32 *crcout=NULL) = 0;
};

interface IExpander;
extern THORHELPER_API IExtRowStream *createRowStream(IFile *file, IRowInterfaces *rowif, unsigned flags=DEFAULT_RWFLAGS, IExpander *eexp=NULL);
extern THORHELPER_API IExtRowStream *createRowStreamEx(IFile *file, IRowInterfaces *rowif, offset_t offset=0, offset_t len=(offset_t)-1, unsigned __int64 maxrows=(unsigned __int64)-1, unsigned flags=DEFAULT_RWFLAGS, IExpander *eexp=NULL);
interface ICompressor;
extern THORHELPER_API IExtRowWriter *createRowWriter(IFile *file, IRowInterfaces *rowIf, unsigned flags=DEFAULT_RWFLAGS, ICompressor *compressor=NULL);
extern THORHELPER_API IExtRowWriter *createRowWriter(IFileIO *fileIO, IRowInterfaces *rowIf, unsigned flags=DEFAULT_RWFLAGS);
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

extern THORHELPER_API void testDiskSort();



#define TIME_ACTIVITIES
class ActivityTimeAccumulator
{
    friend class ActivityTimer;
public:
    ActivityTimeAccumulator()
    {
        startCycles = 0;
        totalCycles = 0;
        endCycles = 0;
        firstRow = 0;
        firstExitCycles = 0;
    }
public:
    cycle_t startCycles; // Wall clock time of first entry to this activity
    cycle_t totalCycles; // Time spent in this activity
    cycle_t endCycles;   // Wall clock time of last entry to this activity
    unsigned __int64 firstRow; // Timestamp of first row (nanoseconds since epoch)
    cycle_t firstExitCycles;    // Wall clock time of first exit from this activity

    // Return the total amount of time (in nanoseconds) spent in this activity (first entry to last exit)
    inline unsigned __int64 elapsed() const { return cycle_to_nanosec(endCycles-startCycles); }
    // Return the total amount of time (in nanoseconds) spent in the first call of this activity (first entry to first exit)
    inline unsigned __int64 latency() const { return cycle_to_nanosec(firstExitCycles-startCycles); }

    void addStatistics(IStatisticGatherer & builder) const
    {
        if (totalCycles)
        {
            builder.addStatistic(StWhenFirstRow, firstRow);
            builder.addStatistic(StTimeElapsed, elapsed());
            builder.addStatistic(StTimeTotalExecute, cycle_to_nanosec(totalCycles));
            builder.addStatistic(StTimeFirstExecute, latency());
        }
    }
};

#ifdef TIME_ACTIVITIES
#include "jdebug.hpp"

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
#else
struct ActivityTimer
{
    inline ActivityTimer(ActivityTimeAccumulator &_accumulator, const bool _enabled) { }
};
struct SimpleActivityTimer
{
    inline SimpleActivityTimer(unsigned __int64 &_accumulator, const bool _enabled) { }
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
    virtual const char *cloneVString(const char *str) const
    {
        return ctx->cloneVString(str);
    }
    virtual const char *cloneVString(size32_t len, const char *str) const
    {
        return ctx->cloneVString(len, str);
    }
    virtual void getResultRowset(size32_t & tcount, byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        ctx->getResultRowset(tcount, tgt, name, sequence, _rowAllocator, isGrouped, xmlTransformer, csvTransformer);
    }
    virtual void getResultDictionary(size32_t & tcount, byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher)
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
protected:
    ICodeContext * ctx;
};

extern THORHELPER_API bool isActivitySink(ThorActivityKind kind);
extern THORHELPER_API bool isActivitySource(ThorActivityKind kind);
extern THORHELPER_API const char * getActivityText(ThorActivityKind kind);


#endif // THORHELPER_HPP
