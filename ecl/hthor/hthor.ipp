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
#ifndef HTHOR_IPP_INCL
#define HTHOR_IPP_INCL

/* hthor debug options (can be set by #option or in Options section on eclserver) (omits those for internal/testing use only)
   hthorDiskWriteSizeLimit
   hthorSpillThreshold
   outputLimit (for pipe)
   layoutTranslation
   hthorMemoryLimit
 */

#include "jliball.hpp"
#include "hthor.hpp"
#include "dadfs.hpp"
#include "csvsplitter.hpp"
#include "thorxmlread.hpp"
#include "thorpipe.hpp"
#include "thorrparse.ipp"
#include "rtlkey.hpp"
#include "thorsoapcall.hpp"
#include "thorcommon.ipp"
#include "roxielmj.hpp"
#include "eclrtl_imp.hpp"
#include "rtlcommon.hpp"
#include "rtlds_imp.hpp"
#include "rtlread_imp.hpp"
#include "rtlrecord.hpp"
#include "roxiemem.hpp"
#include "roxierowbuff.hpp"

#include "thorread.hpp"

roxiemem::IRowManager * queryRowManager();
using roxiemem::OwnedConstRoxieRow;
using roxiemem::DynamicRoxieOutputRowArray;

//---------------------------------------------------------------------------
class EclGraph;

class DECL_EXCEPTION CHThorException : implements IHThorException, public CInterface
{
public: 
    IMPLEMENT_IINTERFACE;
    CHThorException(int _code, char const * format, va_list args, MessageAudience _audience, ThorActivityKind _kind, unsigned _activityId, unsigned _subgraphId)  __attribute__((format(printf,3,0)))
    : code(_code), audience(_audience), kind(_kind), activityId(_activityId), subgraphId(_subgraphId)
    {
        msg.valist_appendf(format, args);
    }
    CHThorException(IException * exc, ThorActivityKind _kind, unsigned _activityId, unsigned _subgraphId) : code(exc->errorCode()), audience(exc->errorAudience()), kind(_kind), activityId(_activityId), subgraphId(_subgraphId)
    {
        exc->errorMessage(msg);
        exc->Release();
    }
    CHThorException(IException * exc, char const * extra, ThorActivityKind _kind, unsigned _activityId, unsigned _subgraphId) : code(exc->errorCode()), audience(exc->errorAudience()), kind(_kind), activityId(_activityId), subgraphId(_subgraphId)
    {
        exc->errorMessage(msg);
        exc->Release();
        msg.append(" (").append(extra).append(")");
    }
    
    virtual int errorCode() const { return code; }
    virtual StringBuffer & errorMessage(StringBuffer & buff) const { return buff.appendf("%s (in %s G%u E%u)", msg.str(), getActivityText(kind), subgraphId, activityId); }
    virtual MessageAudience errorAudience() const { return audience; }

private:
    int code;
    StringBuffer msg;
    MessageAudience audience;
    ThorActivityKind kind;
    unsigned activityId;
    unsigned subgraphId;
};

class CRowBuffer : public CInterface
{
public:
    CRowBuffer(IRecordSize * _recsize, bool _grouped);

    bool pull(IHThorInput * input, unsigned __int64 rowLimit);
    void insert(const void * row);

    void clear();
    inline unsigned __int64 queryCount() const { return count; }
    const void * next();

protected:
    Linked<IRecordSize> recsize;
    bool grouped;
    size32_t fixsize;
    unsigned __int64 count;
    aindex_t index;
    OwnedRowArray buff;
};

class CHThorStreamMerger : public CStreamMerger
{
public:
    CHThorStreamMerger() : CStreamMerger(true) {}

    void initInputs(unsigned _numInputs, IHThorInput ** _inputArray)
    {
        CStreamMerger::initInputs(_numInputs);
        inputArray = _inputArray;
    }

    virtual bool pullInput(unsigned i, const void * seek, unsigned numFields, const SmartStepExtra * stepExtra)
    {
        bool matched = true;
        const void *next;
        if (seek)
        {
            //MORE: Should think about implementing isCompleteMatch in hthor
            bool inputIsCompleteMatch;
            next = inputArray[i]->nextRowGE(seek, numFields, inputIsCompleteMatch, *stepExtra);
        }
        else
        {
            next = inputArray[i]->nextRow();
            if (!next)
                next = inputArray[i]->nextRow();
        }

        pending[i] = next;
        pendingMatches[i] = matched;
        return (next != NULL);
    }

    virtual void releaseRow(const void * row)
    {
        ReleaseRoxieRow(row);
    }

protected:
    IHThorInput **inputArray;
};

static bool verifyFormatCrc(unsigned helperCrc, IDistributedFile * df, char const * super, bool isIndex, bool fail)
{
    IPropertyTree &props = df->queryAttributes();
    if(props.hasProp("@formatCrc"))
    {
        unsigned dfsCrc = props.getPropInt("@formatCrc");
        if(helperCrc != dfsCrc)
        {
            StringBuffer msg;
            msg.append(isIndex ? "Index" : "Dataset").append(" layout does not match published layout for ").append(isIndex ? "index" : "file").append(" ").append(df->queryLogicalName());
            if(super)
                msg.append(" (in super").append(isIndex ? "index" : "file").append(" ").append(super).append(")");
            if(fail)
                throw MakeStringException(0, "%s", msg.str());
            UWARNLOG("%s", msg.str());
            //MORE: Should we add a warning, similar to the following:
            //agent.addWuException(msg.str(), WRN_UseLayoutTranslation, SeverityWarning, "hthor");
            return false;
        }
    }
    return true;
}

static bool verifyFormatCrcSuper(unsigned helperCrc, IDistributedFile * df, bool isIndex, bool fail)
{
    if(!df || (helperCrc == 0)) return true;
    IDistributedSuperFile * super = df->querySuperFile();
    if(super)
    {
        Owned<IDistributedFileIterator> superIterator = super->getSubFileIterator(true);
        for(superIterator->first(); superIterator->isValid(); superIterator->next())
            if(!verifyFormatCrc(helperCrc, &superIterator->query(), super->queryLogicalName(), isIndex, fail))
                return false;
        return true;
    }
    else
    {
        return verifyFormatCrc(helperCrc, df, NULL, isIndex, fail);
    }
}

#define IMPLEMENT_SINKACTIVITY \
    virtual unsigned queryOutputs() const { return 0; } \
    virtual const void * nextRow() { throwUnexpected(); } \
    virtual bool isGrouped() { throwUnexpected(); } \
    virtual IOutputMetaData * queryOutputMeta() const   { throwUnexpected(); } 

class CHThorActivityBase : public CInterface, implements IHThorActivity, implements IHThorInput, implements IEngineRowStream
{
protected:
    enum ActivityState { StateCreated, StateReady, StateDone };
    IHThorInput *input = nullptr;
    IHThorArg & help;
    ThorActivityKind kind;
    EclGraph & graph;
    IAgentContext &agent;
    CachedOutputMetaData outputMeta;
    unsigned __int64 processed = 0;
    unsigned __int64 initialProcessed = 0;
    unsigned activityId;
    unsigned subgraphId;
    IEngineRowAllocator *rowAllocator = nullptr;

    CHThorActivityBase(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _help, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorActivityBase();
    IException * makeWrappedException(IException * e) const;
    IException * makeWrappedException(IException * e, char const * extra) const;

public:
    IMPLEMENT_IINTERFACE;
    virtual void setInput(unsigned, IHThorInput *);
    virtual IHThorInput *queryOutput(unsigned);
    virtual void ready();
    virtual void execute();
    virtual void extractResult(unsigned & len, void * & ret);
    virtual void stop();
    virtual void resetEOF();
    virtual void setBoundGraph(IHThorBoundLoopGraph * graph) { UNIMPLEMENTED; }
    virtual __int64 getCount();
    virtual unsigned queryOutputs() const { return 1; }
    virtual void updateProgress(IStatisticGatherer &progress) const;
    virtual void updateProgressForOther(IStatisticGatherer &progress, unsigned otherActivity, unsigned otherSubgraph) const;
    unsigned __int64 queryProcessed() const { return processed; }
    virtual unsigned queryId() const { return activityId; }
    virtual ThorActivityKind getKind() const  { return kind; };

    virtual bool needsAllocator() const { return false; }       
    void createRowAllocator();                                  
    virtual bool isPassThrough();
    virtual IEngineRowStream &queryStream() { return *this; }
    inline const void *ungroupedNextRow() { return IEngineRowStream::ungroupedNextRow(); }

protected:
    void updateProgressForOther(IStatisticGatherer &progress, unsigned otherActivity, unsigned otherSubgraph, unsigned whichOutput, unsigned __int64 numProcessed) const;

protected:
    ILocalEclGraphResults * resolveLocalQuery(__int64 graphId);
};

class CHThorSimpleActivityBase : public CHThorActivityBase
{
public:
    CHThorSimpleActivityBase(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _help, ThorActivityKind _kind, EclGraph & _graph);

    virtual IHThorInput *queryOutput(unsigned index);

    //interface IHThorInput
    virtual bool isGrouped();

    virtual IOutputMetaData * queryOutputMeta() const;
};

class CHThorSteppableActivityBase : public CHThorSimpleActivityBase
{
public:
    CHThorSteppableActivityBase(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _help, ThorActivityKind _kind, EclGraph & _graph);

    virtual void setInput(unsigned, IHThorInput *);
    virtual IInputSteppingMeta * querySteppingMeta();

protected:
    IInputSteppingMeta * inputStepping;
    IRangeCompare * stepCompare;
};

struct IFileDescriptor;
class CHThorDiskWriteActivity : public CHThorActivityBase 
{
protected:
    IHThorGenericDiskWriteArg &helper;
    bool extend = false;
    bool overwrite = false;
    Owned<IFileIO> io;
    Linked<IFileIOStream> diskout;
    StringBuffer lfn;
    StringAttr filename;
    OwnedIFile file;
    unsigned helperFlags;
    bool incomplete;
    bool grouped;
    bool blockcompressed;
    bool encrypted;
    bool outputPlaneCompressed = false;
    bool useGenericReadWrites = false;
    CachedOutputMetaData serializedOutputMeta;
    offset_t uncompressedBytesWritten;
    Owned<ILogicalRowWriter> outSeq;
    unsigned __int64 numRecords = 0;
    stat_type numDiskWrites = 0;
    cost_type diskAccessCost = 0;
    Owned<ClusterWriteHandler> clusterHandler;
    offset_t sizeLimit = 0;
    Owned<IRowInterfaces> rowIf;
    StringBuffer mangledHelperFileName;
    OwnedConstRoxieRow nextrow; // needed for grouped spill
    Owned<IPropertyTree> formatOptions; // used by generic I/O
    Owned<IPropertyTree> providerOptions; // used by generic I/O

    virtual bool isOutputTransformed() { return false; }
    virtual void setFormat(IFileDescriptor * desc);
    virtual bool isFixedWidth() 
    { 
        return (input->queryOutputMeta()->querySerializedDiskMeta()->isFixedSize());
    }

    void resolve();
    void open();
    void close();
    void publish();
    void updateProgress(IStatisticGatherer &progress) const override;
    void updateWorkUnitResult(unsigned __int64 reccount);
    void finishOutput();
    bool next();
    const void *getNext(); 
    void checkSizeLimit();
    virtual bool needsAllocator() const { return true; }
public:
    IMPLEMENT_SINKACTIVITY;

    CHThorDiskWriteActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorGenericDiskWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorDiskWriteActivity();
    virtual void execute();
    virtual void ready();
    virtual void stop();
};

class CHThorSpillActivity : public CHThorDiskWriteActivity
{
public:
    CHThorSpillActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSpillArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void setInput(unsigned, IHThorInput *);
    virtual void stop();
    virtual void execute();
    virtual void ready();

    //interface IHThorInput
    virtual IHThorInput *queryOutput(unsigned index) { assertex(index==0); return this; }
    virtual bool isGrouped()                { return input->isGrouped(); }
    virtual const void *nextRow();
    virtual IOutputMetaData * queryOutputMeta() const   { return input->queryOutputMeta(); }
};

class CHThorCsvWriteActivity : public CHThorDiskWriteActivity
{
    IHThorCsvWriteArg &helper;
    CSVOutputStream csvOutput;

    virtual bool isOutputTransformed() { return true; }
    virtual void setFormat(IFileDescriptor * desc);
    virtual bool isFixedWidth() { return false; }
public:
    CHThorCsvWriteActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorCsvWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
};

class CHThorXmlWriteActivity : public CHThorDiskWriteActivity
{
    IHThorXmlWriteArg &helper;
    StringBuffer rowTag;
    unsigned headerLength;
    unsigned footerLength;

    virtual bool isOutputTransformed() { return true; }
    virtual void setFormat(IFileDescriptor * desc);
    virtual bool isFixedWidth() { return false; }
public:
    CHThorXmlWriteActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorXmlWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
};

class CHThorIndexWriteActivity : public CHThorActivityBase 
{
    IHThorIndexWriteArg &helper;
    Owned<ClusterWriteHandler> clusterHandler;
    StringAttr filename;
    StringBuffer defaultIndexCompression;
    Owned<IFile> file;
    bool incomplete;
    bool defaultNoSeek = false;
    offset_t sizeLimit = 0;
    unsigned __int64 duplicateKeyCount = 0;
    unsigned __int64 cummulativeDuplicateKeyCount = 0;
    unsigned __int64 totalLeafNodes = 0;
    unsigned __int64 totalBranchNodes = 0;
    unsigned __int64 totalBlobNodes = 0;
    stat_type numDiskWrites = 0;
    cost_type diskAccessCost = 0;
    void close();
    void buildLayoutMetadata(Owned<IPropertyTree> & metadata);
    virtual void updateProgress(IStatisticGatherer &progress) const override
    {
        CHThorActivityBase::updateProgress(progress);
        StatsActivityScope scope(progress, activityId);
        progress.addStatistic(StNumDuplicateKeys, cummulativeDuplicateKeyCount);
        progress.addStatistic(StNumDiskWrites, numDiskWrites);
        progress.addStatistic(StCostFileAccess, diskAccessCost);
        progress.addStatistic(StNumLeafCacheAdds, totalLeafNodes);
        progress.addStatistic(StNumNodeCacheAdds, totalBranchNodes);
        progress.addStatistic(StNumBlobCacheAdds, totalBlobNodes);
    }

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorIndexWriteActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorIndexWriteActivity();
    virtual void execute() override;
};

class IPipeWriteOwner
{
public:
    virtual void openPipe(const void * row, bool displayTitle) = 0;
    virtual const void * nextInput(size32_t & inputSize) = 0;
    virtual void closePipe() = 0;
    virtual void writeTranslatedText(const void * row) = 0;
};

class CHThorIterateActivity : public CHThorSimpleActivityBase
{
    IHThorIterateArg &helper;
    OwnedConstRoxieRow defaultRecord;
    OwnedConstRoxieRow left;
    OwnedConstRoxieRow right;
    unsigned __int64 counter;

public:
    CHThorIterateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIterateArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorProcessActivity : public CHThorSimpleActivityBase
{
    IHThorProcessArg &helper;
    OwnedConstRoxieRow curRight;
    OwnedConstRoxieRow initialRight;
    unsigned __int64 counter = 0;
    Owned<IEngineRowAllocator> rightRowAllocator;

public:
    CHThorProcessActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorProcessArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorProcessActivity();

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorRollupActivity : public CHThorSimpleActivityBase
{
    IHThorRollupArg &helper;
    OwnedConstRoxieRow left;
    OwnedConstRoxieRow prev;
    OwnedConstRoxieRow right;
public:
    CHThorRollupActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorRollupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorRollupActivity();

    virtual void stop();
    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorGroupDedupActivity : public CHThorSimpleActivityBase
{
public:
    CHThorGroupDedupActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();

protected:
    IHThorDedupArg &helper;
    unsigned     numKept;
    unsigned     numToKeep;
};

class CHThorGroupDedupKeepLeftActivity : public CHThorGroupDedupActivity
{
public:
    CHThorGroupDedupKeepLeftActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();

    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector);
    virtual void resetEOF();
    virtual void setInput(unsigned, IHThorInput *);
    virtual IInputSteppingMeta * querySteppingMeta();
private:
    OwnedConstRoxieRow prev;
    IInputSteppingMeta * inputStepping;
};

class CHThorGroupDedupKeepRightActivity : public CHThorGroupDedupActivity
{
public:
    CHThorGroupDedupKeepRightActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();

private:
    OwnedConstRoxieRow kept;
    bool         firstDone;
    ICompare * compareBest;
};

class CHThorGroupDedupAllActivity : public CHThorSimpleActivityBase
{
public:
    CHThorGroupDedupAllActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();

private:
    bool calcNextDedupAll();
    void dedupRange(unsigned first, unsigned last, OwnedRowArray & group);

private:
    IHThorDedupArg &helper;
    OwnedRowArray survivors;
    aindex_t     survivorIndex;
    bool         keepLeft;
    bool         firstDone;
    ICompare *   primaryCompare;

};

class HashDedupElement
{
public:
    HashDedupElement(unsigned _hash, const void *_keyRow)
        : hash(_hash), keyRow(_keyRow)
    {
    }
    ~HashDedupElement()                 { ReleaseRoxieRow(keyRow); }
    inline unsigned queryHash() const   { return hash; }
    inline const void * queryRow() const { return keyRow; }
    inline const void * getRow()
    {
        const void * row = keyRow;
        keyRow = nullptr;
        hash = 0;
        return row;
    }
private:
    unsigned hash;
    const void *keyRow;
};

class HashDedupTable : public SuperHashTable
{
public:
    HashDedupTable(IHThorHashDedupArg & _helper)
        : helper(_helper)
    {
        queryBestCompare = helper.queryCompareBest();
    }
    virtual ~HashDedupTable()
    { 
        _releaseAll();
    }
    virtual void onAdd(void *et)    {}
    virtual void onRemove(void *et)
    {
        const HashDedupElement *element = reinterpret_cast<const HashDedupElement *>(et);
        delete element;
    }
    virtual unsigned getHashFromElement(const void *et) const 
    { 
        const HashDedupElement *element = reinterpret_cast<const HashDedupElement *>(et);
        return element->queryHash(); 
    }
    virtual unsigned getHashFromFindParam(const void *fp) const         { throwUnexpected(); }
    virtual const void * getFindParam(const void *et) const             { throwUnexpected(); }
    virtual bool matchesFindParam(const void *et, const void *key, unsigned fphash) const 
    { 
        const HashDedupElement *element = reinterpret_cast<const HashDedupElement *>(et);
        if (fphash != element->queryHash())
            return false;
        return (helper.queryKeyCompare()->docompare(element->queryRow(), key) == 0); 
    }
    virtual bool matchesElement(const void *et, const void *searchET) const { throwUnexpected(); }

    inline unsigned hashFromElement(const void *et) const               { throwUnexpected(); }
    inline void setRowAllocator(IEngineRowAllocator * _keyRowAllocator) { keyRowAllocator.setown(_keyRowAllocator); }

    bool insert(const void * row);
    bool insertBest(const void * row);

private:
    IHThorHashDedupArg & helper;
    Owned<IEngineRowAllocator> keyRowAllocator;
    ICompare * queryBestCompare;
};

class CHThorHashDedupActivity : public CHThorSimpleActivityBase
{
public:
    CHThorHashDedupActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorHashDedupArg & _arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void ready();
    virtual void stop();
    virtual const void *nextRow();
    virtual bool needsAllocator() const { return true; }    

private:
    IHThorHashDedupArg & helper;
    HashDedupTable table;
    bool keepBest;
    bool hashTableFilled;
    SuperHashIteratorOf<HashDedupElement> hashDedupTableIter;
};

class CHThorNormalizeActivity : public CHThorSimpleActivityBase
{
    IHThorNormalizeArg &helper;
    OwnedConstRoxieRow inbuff;
    unsigned numThisRow;
    unsigned curRow;
    unsigned __int64 numProcessedLastGroup;
public:
    CHThorNormalizeActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorNormalizeActivity();

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorNormalizeChildActivity : public CHThorSimpleActivityBase
{
    IHThorNormalizeChildArg &helper;
    OwnedConstRoxieRow inbuff;
    unsigned curRow;
    unsigned __int64 numProcessedLastGroup;
    INormalizeChildIterator * cursor;
    void * curChildRow;

public:
    CHThorNormalizeChildActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeChildArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorNormalizeChildActivity();

    virtual void stop();
    virtual void ready();
    virtual bool needsAllocator() const { return true; }    
    
    //interface IHThorInput
    virtual const void *nextRow();

    void normalizeRecord();

protected:
    bool advanceInput();
};

class CHThorNormalizeLinkedChildActivity : public CHThorSimpleActivityBase
{
    IHThorNormalizeLinkedChildArg &helper;
    OwnedConstRoxieRow curParent;
    OwnedConstRoxieRow curChild;
    unsigned __int64 numProcessedLastGroup;

public:
    CHThorNormalizeLinkedChildActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeLinkedChildArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorNormalizeLinkedChildActivity();

    virtual void stop();
    virtual void ready();

    //interface IHThorInput
    const void *nextRow();

protected:
    bool advanceInput();
};


class CHThorProjectActivity : public CHThorSimpleActivityBase
{
    IHThorProjectArg &helper;
    unsigned __int64 numProcessedLastGroup;
public:
    CHThorProjectActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorProjectArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorProjectActivity();

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorPrefetchProjectActivity : public CHThorSimpleActivityBase
{
    IHThorPrefetchProjectArg &helper;
    unsigned __int64 recordCount;
    unsigned __int64 numProcessedLastGroup;
    bool eof;
    IThorChildGraph *child;

public:
    CHThorPrefetchProjectActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorPrefetchProjectArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual bool needsAllocator() const { return true; }

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorFilterProjectActivity : public CHThorSimpleActivityBase
{
    IHThorFilterProjectArg &helper;
    unsigned __int64 recordCount;
    unsigned __int64 numProcessedLastGroup;
    bool eof;
public:
    CHThorFilterProjectActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorFilterProjectArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorFilterProjectActivity();

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorCountProjectActivity : public CHThorSimpleActivityBase
{
    IHThorCountProjectArg &helper;
    unsigned __int64 recordCount;
    unsigned __int64 numProcessedLastGroup;
public:
    CHThorCountProjectActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorCountProjectArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorCountProjectActivity();

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorFilterActivity : public CHThorSteppableActivityBase
{
    IHThorFilterArg &helper;
    bool anyThisGroup;
    bool eof;
public:
    CHThorFilterActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorFilterArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();

    //interface IHThorInput
    virtual const void *nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector);
    virtual void resetEOF();
};

class CHThorFilterGroupActivity : public CHThorSteppableActivityBase
{
    IHThorFilterGroupArg &helper;
    OwnedRowArray pending;
    aindex_t nextIndex;
    bool eof;
public:
    CHThorFilterGroupActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorFilterGroupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
};

class CHThorLimitActivity : public CHThorSteppableActivityBase
{
    IHThorLimitArg &helper;
    unsigned __int64 numGot;
    unsigned __int64 rowLimit;
public:
    CHThorLimitActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();

    //interface IHThorInput
    virtual const void *nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
};

class CHThorSkipLimitActivity : public CHThorSimpleActivityBase
{
public:
    CHThorSkipLimitActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void ready();
    virtual void stop();
    virtual const void * nextRow();

    virtual void onLimitExceeded() { buffer->clear(); }

protected:
    IHThorLimitArg &helper;
    unsigned __int64 rowLimit;
    Owned<CRowBuffer> buffer;
};

class CHThorOnFailLimitActivity : public CHThorSkipLimitActivity
{
public:
    CHThorOnFailLimitActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    
    virtual bool needsAllocator() const { return true; }    
    virtual void onLimitExceeded();
};

class CHThorCatchActivity : public CHThorSteppableActivityBase
{
    IHThorCatchArg &helper;
public:
    CHThorCatchActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorCatchArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual const void *nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
};

class CHThorSkipCatchActivity : public CHThorSimpleActivityBase
{
public:
    CHThorSkipCatchActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorCatchArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void stop();
    virtual const void * nextRow();

protected:
    void onException(IException *E);
    IHThorCatchArg &helper;
    Owned<CRowBuffer> buffer;
};

class CHThorIfActivity : public CHThorSimpleActivityBase
{
    IHThorIfArg &helper;
    IHThorInput *inputTrue;
    IHThorInput *inputFalse;
    IHThorInput *selectedInput;
public:
    CHThorIfActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIfArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual const void *nextRow();
    virtual void setInput(unsigned, IHThorInput *);
    virtual void ready();
    virtual void stop();
    virtual void updateProgress(IStatisticGatherer &progress) const
    {
        CHThorSimpleActivityBase::updateProgress(progress);
        inputTrue->updateProgress(progress);
        if (inputFalse)
            inputFalse->updateProgress(progress);
    }   
};

class CHThorSampleActivity : public CHThorSimpleActivityBase
{
    IHThorSampleArg &helper;
    unsigned numSamples;
    unsigned numToSkip;
    unsigned whichSample;
    bool anyThisGroup;
public:
    CHThorSampleActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSampleArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorAggregateActivity : public CHThorSimpleActivityBase
{
    IHThorAggregateArg &helper;
    bool eof = false;
public:
    CHThorAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorAggregateArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
    virtual bool isGrouped()                                            { return false; }
};

class CHThorHashAggregateActivity : public CHThorSimpleActivityBase
{
    RowAggregator aggregated;

    bool eof = false;
    bool gathered = false;
    bool isGroupedAggregate;
public:
    CHThorHashAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorHashAggregateArg &_arg, ThorActivityKind _kind, EclGraph & _graph, bool _isGroupedAggregate);

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorSelectNActivity : public CHThorSimpleActivityBase
{
    IHThorSelectNArg &helper;
    bool finished;
    const void *defaultRow();
public:
    CHThorSelectNActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSelectNArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorFirstNActivity : public CHThorSimpleActivityBase
{
    IHThorFirstNArg &helper;
    __uint64 doneThisGroup = 0;
    __uint64 limit = 0;  // You would think int was enough for most practical cases...
    __uint64 skip = 0;
    bool finished = false;
    bool grouped = false;
public:
    CHThorFirstNActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorFirstNArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual bool isGrouped()                { return grouped; }

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorChooseSetsActivity : public CHThorSimpleActivityBase
{
    IHThorChooseSetsArg &helper;
    unsigned numSets = 0;
    unsigned * setCounts = nullptr;
    bool finished = false;
public:
    CHThorChooseSetsActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsArg &_arg, ThorActivityKind , EclGraph & _graph);
    ~CHThorChooseSetsActivity();

    virtual void ready();

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorChooseSetsExActivity : public CHThorSimpleActivityBase
{
protected:
    IHThorChooseSetsExArg &helper;
    unsigned numSets = 0;
    unsigned * setCounts = nullptr;
    count_t * limits = nullptr;
    bool finished = false;
    OwnedRowArray gathered;
    aindex_t curIndex = 0;
    virtual bool includeRow(const void * row) = 0;
    virtual void calculateSelection() = 0;
public:
    CHThorChooseSetsExActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsExArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorChooseSetsExActivity();

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorChooseSetsLastActivity : public CHThorChooseSetsExActivity
{
    unsigned * numToSkip;
protected:
    virtual bool includeRow(const void * row);
    virtual void calculateSelection();
public:
    CHThorChooseSetsLastActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsExArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorChooseSetsLastActivity();

    virtual void ready();
};

class CHThorChooseSetsEnthActivity : public CHThorChooseSetsExActivity
{
    unsigned __int64 * counter;
protected:
    virtual bool includeRow(const void * row);
    virtual void calculateSelection();
public:
    CHThorChooseSetsEnthActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsExArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorChooseSetsEnthActivity();

    virtual void ready();
};

class CHThorGroupActivity : public CHThorSteppableActivityBase
{
    IHThorGroupArg &helper;
    OwnedConstRoxieRow next; 
    bool endPending = false;
    bool firstDone = false;
public:
    CHThorGroupActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorGroupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
    virtual bool isGrouped();
};

class CHThorDegroupActivity : public CHThorSteppableActivityBase
{
public:
    CHThorDegroupActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDegroupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual const void *nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
    virtual bool isGrouped();
};

class ISorter : public IInterface
{
public:
    virtual ~ISorter() {}
    virtual bool addRow(const void * next) = 0;
    virtual void performSort() = 0;
    virtual void spillSortedToDisk(IDiskMerger * merger) = 0;
    virtual const void * getNextSorted() = 0;
    virtual void killSorted() = 0;
    virtual const DynamicRoxieOutputRowArray & getRowArray() = 0;
    virtual void flushRows() = 0;
    virtual unsigned numCommitted() const = 0;
    virtual void setActivityId(unsigned _activityId) = 0;
};

class CHThorGroupSortActivity : public CHThorSimpleActivityBase, implements roxiemem::IBufferedRowCallback
{
   enum {
        InitialSortElements = 0,
        //The number of rows that can be added without entering a critical section,
        //and therefore also the max number of rows that wont get freed during a spill
        CommitStep = 32
    };
public:
    CHThorGroupSortActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSortArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    virtual void stop();
    virtual const void *nextRow();

    virtual IOutputMetaData * queryOutputMeta() const { return outputMeta; }

    //interface roxiemem::IBufferedRowCallback
    virtual unsigned getSpillCost() const;
    virtual unsigned getActivityId() const;
    virtual bool freeBufferedRows(bool critical);

private:
    bool sortAndSpillRows();
    void createSorter();
    void getSorted();

protected:
    IHThorSortArg &helper;
    bool gotSorted;
    Owned<ISorter> sorter;
    bool sorterIsConst = false;
    Owned<IDiskMerger> diskMerger;
    Owned<IRowStream> diskReader;
};

class CSimpleSorterBase : public ISorter, public CInterface
{
public:
    CSimpleSorterBase(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta) : rowManager(_rowManager), compare(_compare),
        rowsToSort(_rowManager, _initialSize, _commitDelta, UNKNOWN_ROWSET_ID), finger(0) {}
    virtual ~CSimpleSorterBase()                            { killSorted(); }
    IMPLEMENT_IINTERFACE;
    virtual bool addRow(const void * next)                  { return rowsToSort.append(next); }
    virtual void spillSortedToDisk(IDiskMerger * merger);
    virtual const void * getNextSorted()
    {
        if (finger < rowsToSort.numCommitted())
        {
            const void * * rows = rowsToSort.getBlock(finger);
            const void * row = rows[finger];
            rows[finger++] = NULL;
            return row;
        }
        else
            return NULL;
    }
    virtual void killSorted()                               { rowsToSort.kill(); finger = 0;}
    virtual const DynamicRoxieOutputRowArray & getRowArray()     { return rowsToSort; }
    virtual void flushRows()                                { rowsToSort.flush(); }
    virtual size32_t numCommitted() const                   { return rowsToSort.numCommitted(); }
    virtual void setActivityId(unsigned _activityId)        { activityId = _activityId; rowsToSort.setAllocatorId(_activityId); }

protected:
    roxiemem::IRowManager * rowManager;
    unsigned activityId = 0;
    ICompare * compare;
    DynamicRoxieOutputRowArray rowsToSort;
    aindex_t finger;
};

class CQuickSorter : public CSimpleSorterBase
{
public:
    CQuickSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta) : CSimpleSorterBase(_compare, _rowManager, _initialSize, _commitDelta) {}
    virtual void performSort();
};

class CParallelQuickSorter : public CSimpleSorterBase
{
public:
    CParallelQuickSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta) : CSimpleSorterBase(_compare, _rowManager, _initialSize, _commitDelta) {}
    virtual void performSort();
};

class CParallelTaskQuickSorter : public CSimpleSorterBase
{
public:
    CParallelTaskQuickSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta) : CSimpleSorterBase(_compare, _rowManager, _initialSize, _commitDelta) {}
    virtual void performSort();
};

class CStableSorter : public CSimpleSorterBase
{
public:
    CStableSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta, roxiemem::IBufferedRowCallback * _rowCB)
     : CSimpleSorterBase(_compare, _rowManager, _initialSize, _commitDelta), index(NULL), indexCapacity(0), commitDelta(_commitDelta) {}
    virtual ~CStableSorter() { killSorted(); }

    virtual bool addRow(const void * next);
    virtual void spillSortedToDisk(IDiskMerger * merger);
    virtual void killSorted();

protected:
    void *** index;
    roxiemem::rowidx_t indexCapacity;
    unsigned commitDelta;
};

class CStableQuickSorter : public CStableSorter
{
public:
    CStableQuickSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta, roxiemem::IBufferedRowCallback * _rowCB) : CStableSorter(_compare, _rowManager, _initialSize, _commitDelta, _rowCB){}

    virtual void performSort();
};

class CParallelStableQuickSorter : public CStableSorter
{
public:
    CParallelStableQuickSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta, roxiemem::IBufferedRowCallback * _rowCB) : CStableSorter(_compare, _rowManager, _initialSize, _commitDelta, _rowCB){}

    virtual void performSort();
};

class CParallelTaskStableQuickSorter : public CStableSorter
{
public:
    CParallelTaskStableQuickSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta, roxiemem::IBufferedRowCallback * _rowCB) : CStableSorter(_compare, _rowManager, _initialSize, _commitDelta, _rowCB){}

    virtual void performSort();
};

class CStableMergeSorter : public CStableSorter
{
public:
    CStableMergeSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta, roxiemem::IBufferedRowCallback * _rowCB) : CStableSorter(_compare, _rowManager, _initialSize, _commitDelta, _rowCB){}

    virtual void performSort();
};

class CParallelStableMergeSorter : public CStableSorter
{
public:
    CParallelStableMergeSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta, roxiemem::IBufferedRowCallback * _rowCB) : CStableSorter(_compare, _rowManager, _initialSize, _commitDelta, _rowCB){}

    virtual void performSort();
};

class CHeapSorter :  public CSimpleSorterBase
{
public:
    CHeapSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta) : CSimpleSorterBase(_compare, _rowManager, _initialSize, _commitDelta), heapsize(0) {}
    virtual ~CHeapSorter() { killSorted(); }

    virtual void performSort();
    virtual void spillSortedToDisk(IDiskMerger * merger);
    virtual const void * getNextSorted();
    virtual void killSorted();

private:
    UnsignedArray heap;
    unsigned heapsize;
};

class CInsertionSorter : public CSimpleSorterBase
{
public:
    CInsertionSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta) : CSimpleSorterBase(_compare, _rowManager, _initialSize, _commitDelta) {}
    virtual void performSort();
};

class CStableInsertionSorter : public CSimpleSorterBase
{
public:
    CStableInsertionSorter(ICompare * _compare, roxiemem::IRowManager * _rowManager, size32_t _initialSize, size32_t _commitDelta) : CSimpleSorterBase(_compare, _rowManager, _initialSize, _commitDelta) {}
    virtual void performSort();
};

class CHThorGroupedActivity : public CHThorSimpleActivityBase
{
    IHThorGroupedArg &helper;
    OwnedConstRoxieRow next[3];
    unsigned nextRowIndex;
    bool firstDone;
public:
    CHThorGroupedActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorGroupedArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorSortedActivity : public CHThorSteppableActivityBase
{
    IHThorSortedArg &helper;
    ICompare * compare;
    OwnedConstRoxieRow next; 
    bool firstDone;
public:
    CHThorSortedActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSortedArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
};

class CHThorTraceActivity : public CHThorSteppableActivityBase
{
    IHThorTraceArg &helper;
    roxiemem::OwnedRoxieString name;
    unsigned keepLimit;
    unsigned skip;
    unsigned sample;
    bool traceEnabled;
public:
    CHThorTraceActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorTraceArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
protected:
    void onTrace(const void *row);
};

class CHThorJoinActivity : public CHThorActivityBase
{
    enum { JSfill, JSfillleft, JSfillright, JScollate, JScompare, JSleftonly, JSrightonly } state;

    IHThorJoinArg &helper;
    ICompare * collate;
    ICompare * collateupper;
    IHThorInput *input1;
    bool leftOuterJoin;
    bool rightOuterJoin;
    bool exclude;
    bool limitFail;
    bool limitOnFail;
    unsigned keepLimit;
    unsigned joinLimit;
    unsigned atmostLimit;
    unsigned abortLimit;
    bool betweenjoin;

    OwnedRowArray right;
    OwnedConstRoxieRow left;
    OwnedConstRoxieRow pendingRight;
    unsigned rightIndex;
    unsigned joinCounter;
    BoolArray matchedRight;
    bool matchedLeft;
    Owned<IException> failingLimit;
    ConstPointerArray filteredRight;
    Owned<IRHLimitedCompareHelper> limitedhelper;
    Owned<IEngineRowStream> sortedLeftInput;
    Owned<IEngineRowStream> groupedSortedRightInput;

//MORE: Following are good candidates for a join base class + others
    OwnedConstRoxieRow defaultLeft;
    OwnedConstRoxieRow defaultRight;
    RtlDynamicRowBuilder outBuilder;

    Owned<IEngineRowAllocator> defaultLeftAllocator;    
    Owned<IEngineRowAllocator> defaultRightAllocator;   

private:
    void * cloneOrReturnOutput(memsize_t thisSize);
    void fillLeft();
    void fillRight();
    const void *nextRightInGroup();
    //bool getMatchingRecords();
    //bool queryAdvanceCursors();
    const void * joinRecords(const void * curLeft, const void * curRight, unsigned counter, unsigned flags);
    const void * groupDenormalizeRecords(const void * curLeft, ConstPointerArray & rows, unsigned flags);
    const void * joinException(const void * curLeft, IException * except);
    void failLimit();
    void createDefaultLeft();   
    void createDefaultRight();  

public:
    CHThorJoinActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorJoinArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    
    virtual void setInput(unsigned, IHThorInput *);
    IHThorInput *queryOutput(unsigned index) { return this; }

    //interface IHThorInput
    virtual const void *nextRow();

    virtual bool isGrouped();

    virtual IOutputMetaData * queryOutputMeta() const { return outputMeta; }
    virtual void updateProgress(IStatisticGatherer &progress) const
    {
        CHThorActivityBase::updateProgress(progress);
        if (input1)
            input1->updateProgress(progress);
    }   
};

class CHThorSelfJoinActivity : public CHThorActivityBase
{
    IHThorJoinArg &helper;
    ICompare * collate;
    bool leftOuterJoin;
    bool rightOuterJoin;
    bool exclude;
    bool limitFail;
    bool limitOnFail;
    unsigned keepLimit;
    unsigned joinLimit;
    unsigned atmostLimit;
    unsigned abortLimit;

    OwnedRowArray group;
    bool matchedLeft;
    BoolArray matchedRight;
    unsigned leftIndex;
    unsigned rightIndex;
    unsigned rightOuterIndex;
    unsigned joinCounter;
    bool eof;
    bool doneFirstFill;

    OwnedConstRoxieRow lhs;
    OwnedConstRoxieRow defaultLeft;
    OwnedConstRoxieRow defaultRight;
    RtlDynamicRowBuilder outBuilder;
    Owned<IException> failingLimit;
    bool failingOuterAtmost;
    Owned<IEngineRowAllocator> defaultAllocator;    
    Owned<IRHLimitedCompareHelper> limitedhelper;
    Owned<CRHDualCache> dualcache;
    Owned<IEngineRowStream> groupedInput;
    IRowStream *dualCacheInput;
private:
    bool fillGroup();
    const void * joinRecords(const void * curLeft, const void * curRight, unsigned counter, unsigned flags, IException * except);
    void failLimit(const void * next);

public:
    CHThorSelfJoinActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorJoinArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    IHThorInput *queryOutput(unsigned index) { return this; }

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();

    virtual bool isGrouped();

    virtual IOutputMetaData * queryOutputMeta() const { return outputMeta; }
};

class CHThorLookupJoinActivity : public CHThorActivityBase
{
private:
    class LookupTable : public CInterface
    {
    public:
        LookupTable(unsigned _size, ICompare * _leftRightCompare, ICompare * _rightCompare, IHash * _leftHash, IHash * _rightHash, bool _dedupOnAdd);
        ~LookupTable();
        bool add(const void * right);
        const void * find(const void * left) const;
        const void * findNext(const void * left) const;
    private:
        void advance() const;
        const void * doFind(const void * left) const;
    private:
        ICompare * leftRightCompare;
        ICompare * rightCompare;
        IHash * leftHash;
        IHash * rightHash;
        bool dedupOnAdd;
        unsigned size;
        unsigned mask;
        OwnedConstRoxieRow * table;
        unsigned mutable fstart;
        unsigned mutable findex;
        static unsigned const BadIndex;
    };

    IHThorHashJoinArg & helper;
    IHThorInput * input1 = nullptr;
    bool leftOuterJoin = false;
    bool exclude = false;
    bool many = true;
    bool dedupRHS = false;
    unsigned keepLimit = 0;
    unsigned atmostLimit = 0;
    unsigned limitLimit = 0;
    unsigned joinCounter = 0;
    bool limitFail = false;
    bool limitOnFail = false;
    bool hasGroupLimit = false;
    bool isSmartJoin = false;
    unsigned keepCount = 0;
    OwnedConstRoxieRow defaultRight;
    RtlDynamicRowBuilder outBuilder;
    Owned<LookupTable> table;
    bool eog = false;
    bool matchedGroup = false;
    OwnedConstRoxieRow left;
    bool gotMatch = false;
    ConstPointerArray rightGroup;
    aindex_t rightGroupIndex = 0;
    Owned<IException> failingLimit;

    ConstPointerArray filteredRight;
    Owned<IEngineRowAllocator> defaultRightAllocator;   

private:
    void loadRight();
    const void * groupDenormalizeRecords(const void * curLeft, ConstPointerArray & rows, unsigned flags);
    const void * joinRecords(const void * left, const void * right, unsigned counter, unsigned flags);
    const void * joinException(const void * left, IException * except);
    const void * getRightFirst() { if(hasGroupLimit) return fillRightGroup(); else return table->find(left); }
    const void * getRightNext() { if(hasGroupLimit) return readRightGroup(); else return table->findNext(left); }
    const void * fillRightGroup();
    const void * readRightGroup() { if(rightGroup.isItem(rightGroupIndex)) return rightGroup.item(rightGroupIndex++); else return NULL; }
    void failLimit();
    void createDefaultRight();  

    const void * nextRowJoin();
    const void * nextRowDenormalize();

public:
    CHThorLookupJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorHashJoinArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    
    virtual void setInput(unsigned index, IHThorInput * _input);
    IHThorInput * queryOutput(unsigned index) { return this; }

    //interface IHThorInput
    virtual const void * nextRow();
    virtual void updateProgress(IStatisticGatherer &progress) const
    {
        CHThorActivityBase::updateProgress(progress);
        if (input1)
            input1->updateProgress(progress);
    }   

    virtual bool isGrouped();

    virtual IOutputMetaData * queryOutputMeta() const { return outputMeta; }
};

class CHThorAllJoinActivity : public CHThorActivityBase
{
private:
    IHThorAllJoinArg & helper;
    IHThorInput * input1;
    bool leftIsGrouped;
    bool leftOuterJoin;
    bool exclude;
    unsigned keepLimit;
    OwnedConstRoxieRow defaultRight;
    RtlDynamicRowBuilder outBuilder;
    bool started;
    bool eog;
    bool eos;
    bool matchedGroup;
    OwnedConstRoxieRow left;
    bool matchedLeft;
    unsigned countForLeft;
    unsigned rightIndex;
    unsigned rightOrdinality;
    unsigned joinCounter;
    OwnedRowArray rightset;
    ConstPointerArray filteredRight;
    BoolArray matchedRight;
    Owned<IEngineRowAllocator> defaultRightAllocator;   

private:
    void loadRight();
    const void * joinRecords(const void * left, const void * right, unsigned counter, unsigned flags);
    const void * groupDenormalizeRecords(const void * left, ConstPointerArray & rows, unsigned flags);
    void createDefaultRight();  
public:
    CHThorAllJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorAllJoinArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();
    virtual void setInput(unsigned index, IHThorInput * _input);
    IHThorInput * queryOutput(unsigned index) { return this; }
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void * nextRow();
    virtual void updateProgress(IStatisticGatherer &progress) const
    {
        CHThorActivityBase::updateProgress(progress);
        if (input1)
            input1->updateProgress(progress);
    }   

    virtual bool isGrouped();

    virtual IOutputMetaData * queryOutputMeta() const { return outputMeta; }
};

class CHThorWorkUnitWriteActivity : public CHThorActivityBase
{
    IHThorWorkUnitWriteArg &helper;
    bool grouped;

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorWorkUnitWriteActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorWorkUnitWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
};

class CHThorDictionaryWorkUnitWriteActivity : public CHThorActivityBase
{
    IHThorDictionaryWorkUnitWriteArg &helper;

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorDictionaryWorkUnitWriteActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDictionaryWorkUnitWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
    virtual bool needsAllocator() const { return true; }
};

class CHThorRemoteResultActivity : public CHThorActivityBase
{
    IHThorRemoteResultArg &helper;

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorRemoteResultActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorRemoteResultArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
};


class CHThorResultActivity : public CHThorActivityBase
{
protected:
    MemoryBuffer rowdata;

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorResultActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void extractResult(unsigned & len, void * & ret);
};

class CHThorDatasetResultActivity : public CHThorResultActivity
{
public:
    IMPLEMENT_SINKACTIVITY;

    CHThorDatasetResultActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDatasetResultArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
};

class CHThorRowResultActivity : public CHThorResultActivity
{
public:
    IMPLEMENT_SINKACTIVITY;

    CHThorRowResultActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorRowResultArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
};

class CHThorInlineTableActivity : public CHThorSimpleActivityBase
{
    IHThorInlineTableArg &helper;
    __uint64 curRow;
    __uint64 numRows;

public:
    CHThorInlineTableActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorInlineTableArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual bool needsAllocator() const { return true; }

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorNullActivity : public CHThorSimpleActivityBase
{
public:
    CHThorNullActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual const void *nextRow();
};


class CHThorSideEffectActivity : public CHThorSimpleActivityBase
{
    IHThorSideEffectArg &helper;
public:
    CHThorSideEffectActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSideEffectArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual const void *nextRow();
};


class CHThorActionActivity : public CHThorSimpleActivityBase
{
    IHThorActionArg &helper;
public:
    CHThorActionActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorActionArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual const void *nextRow();
    virtual void execute();
};

class CHThorWhenActionActivity : public CHThorSimpleActivityBase
{
    EclGraphElement * graphElement;
public:
    CHThorWhenActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind, EclGraph & _graph, EclGraphElement * _graphElement);

    virtual void execute();
    virtual const void *nextRow();
    virtual void ready();
    virtual void stop();
};

class CHThorDummyActivity : public CHThorSimpleActivityBase
{
public:
    CHThorDummyActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual const void *nextRow();
    virtual void execute();
};


class CHThorChildIteratorActivity : public CHThorSimpleActivityBase
{
    IHThorChildIteratorArg &helper;
    bool started;
    bool eof;

public:
    CHThorChildIteratorActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChildIteratorArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
    virtual void ready();
};


class CHThorLinkedRawIteratorActivity : public CHThorSimpleActivityBase
{
    IHThorLinkedRawIteratorArg &helper;

public:
    CHThorLinkedRawIteratorActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLinkedRawIteratorArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual const void *nextRow();
};

typedef PointerArrayOf<IHThorInput> InputArrayType;

class CHThorMultiInputActivity : public CHThorSimpleActivityBase
{
protected:
    InputArrayType inputs;

public:
    CHThorMultiInputActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();
    virtual void resetEOF();
    virtual void setInput(unsigned, IHThorInput *);

    //interface IHThorInput
    virtual void updateProgress(IStatisticGatherer &progress) const;
};

class CHThorCaseActivity : public CHThorMultiInputActivity
{
    IHThorCaseArg &helper;
    IHThorInput *selectedInput;

public:
    CHThorCaseActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorCaseArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorConcatActivity : public CHThorMultiInputActivity
{
    IHThorFunnelArg &helper;
    IHThorInput *curInput;
    unsigned inputIdx;
    bool eogSeen;
    bool grouped;
    bool anyThisGroup;

public:
    CHThorConcatActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorFunnelArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();

    //interface IHThorInput
    virtual const void *nextRow();

    virtual bool isGrouped()                { return grouped; }
};

class CHThorNonEmptyActivity : public CHThorMultiInputActivity
{
    IHThorNonEmptyArg &helper;
    IHThorInput * selectedInput;
    bool grouped;

public:
    CHThorNonEmptyActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorNonEmptyArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();

    //interface IHThorInput
    virtual const void *nextRow();

    virtual bool isGrouped()                { return grouped; }
};

class CHThorRegroupActivity : public CHThorMultiInputActivity
{
    unsigned inputIndex;
    bool eof;
    unsigned __int64 numProcessedLastGroup;
public:
    CHThorRegroupActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorRegroupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();

    //interface IHThorInput
    virtual const void *nextRow();

    virtual bool isGrouped()                { return true; }

protected:
    const void * nextFromInputs();
};

class CHThorCombineActivity : public CHThorMultiInputActivity
{
    IHThorCombineArg &helper;
    unsigned __int64 numProcessedLastGroup;
public:
    CHThorCombineActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorCombineArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();

protected:
    void nextInputs(OwnedRowArray & out);
};

class CHThorCombineGroupActivity : public CHThorSimpleActivityBase
{
    IHThorCombineGroupArg &helper;
    unsigned __int64 numProcessedLastGroup = 0;
    IHThorInput *input1 = nullptr;
public:
    CHThorCombineGroupActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorCombineGroupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();
    virtual void setInput(unsigned, IHThorInput *);
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();

protected:
    void nextInputs(ConstPointerArray & out);
};

class CHThorRollupGroupActivity : public CHThorSimpleActivityBase
{
    IHThorRollupGroupArg &helper;
    bool eof;
public:
    CHThorRollupGroupActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorRollupGroupArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    
    virtual bool isGrouped() { return false; }

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorApplyActivity : public CHThorActivityBase 
{
    IHThorApplyArg &helper;
public:
    IMPLEMENT_SINKACTIVITY;

    CHThorApplyActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorApplyArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
};

class CHThorDistributionActivity : public CHThorActivityBase
{
    IHThorDistributionArg &helper;

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorDistributionActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDistributionArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual void execute();
    virtual bool needsAllocator() const { return false; }
};

class CHThorDiskReadActivity;

class CHThorWorkunitReadActivity : public CHThorSimpleActivityBase
{
    IHThorWorkunitReadArg &helper;
    bool grouped = false;
    bool eogPending = false;
    bool first = true;

    // for case where the workunit result was written to disk, so that the activity must dynamically change into a disk read
    Owned<IHThorDiskReadArg> diskreadHelper;
    Owned<CHThorDiskReadActivity> diskread;

    Owned<IOutputRowDeserializer> rowDeserializer;  
    MemoryBuffer resultBuffer;
    Owned<IBufferedSerialInputStream> bufferStream;
    CThorStreamDeserializerSource deserializer;         

public:
    CHThorWorkunitReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorWorkunitReadArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorWorkunitReadActivity();

    void checkForDiskRead();

    virtual void ready();
    virtual void stop();
    virtual bool isGrouped() { return grouped; }
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorParseActivity : public CHThorSimpleActivityBase, implements IMatchedAction
{
public:
    CHThorParseActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorParseArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorParseActivity();

    virtual void ready();
    virtual void stop();

//interface IMatchedAction
    unsigned onMatch(ARowBuilder & self, const void * curRecord, IMatchedResults * results, IMatchWalker * walker);
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();

protected:
    bool processRecord(const void * in);

protected:
    IHThorParseArg &helper;
    INlpParseAlgorithm * algorithm;
    INlpParser * parser;
    bool anyThisGroup;
    INlpResultIterator * rowIter;
    OwnedConstRoxieRow in;
    char * curSearchText;
    size32_t curSearchTextLen;
};

class CHThorEnthActivity : public CHThorSimpleActivityBase
{
public:
    CHThorEnthActivity(IAgentContext & agent, unsigned _activityId, unsigned _subgraphId, IHThorEnthArg & _arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual void ready();
    virtual void stop();
    virtual const void * nextRow();
    virtual bool isGrouped() { return false; }

protected:
    inline bool wanted()
    {       
        counter += numerator;
        if(counter >= denominator)
        {
            counter -= denominator;
            return true;
        }       
        return false;   
    }

    void start();

protected:
    IHThorEnthArg & helper;
    unsigned __int64 numerator;
    unsigned __int64 denominator;
    bool started;
    RtlDynamicRowBuilder outBuilder;
    unsigned __int64 counter;
};

class CHThorTopNActivity : public CHThorSimpleActivityBase
{
public:
    CHThorTopNActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorTopNArg & _arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorTopNActivity();

    virtual void ready();
    virtual void stop();

    //interface IHThorInput
    virtual const void *nextRow();
    virtual bool isGrouped() { return false; }

private:
    IHThorTopNArg & helper;
    __int64 limit;
    ICompare const & compare;
    const void * * sorted;
    unsigned sortedCount;
    bool eof;
    bool eoi;
    unsigned curIndex;
    bool hasBest;
    bool grouped;

private:
    bool abortEarly();
    void getSorted();
};

class CHThorXmlParseActivity : public CHThorSimpleActivityBase, implements IXMLSelect
{
public:
    IMPLEMENT_IINTERFACE;

    CHThorXmlParseActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorXmlParseArg & _arg, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorXmlParseActivity();

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();

    //iface IXMLSelect
    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        lastMatch.set(&entry);
    }
private:
    IHThorXmlParseArg & helper;
    bool srchStrNeedsFree;
    OwnedConstRoxieRow in;
    unsigned __int64 numProcessedLastGroup;
    char * srchStr;
    Owned<IXMLParse> xmlParser;
    Owned<IColumnProvider> lastMatch;
};

//Web Service Call base
class CHThorWSCBaseActivity : public CHThorSimpleActivityBase, implements IWSCRowProvider
{
public:
    CHThorWSCBaseActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorWebServiceCallArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    CHThorWSCBaseActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorWebServiceCallActionArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    IMPLEMENT_IINTERFACE;

    virtual void stop();

    // IWSCRowProvider
    virtual IHThorWebServiceCallActionArg * queryActionHelper() { return &helper; };
    virtual IHThorWebServiceCallArg * queryCallHelper() { return callHelper; };
    virtual const void * getNextRow() { return NULL; };
    virtual void releaseRow(const void * r) { ReleaseRoxieRow(r); }
    virtual unsigned queryActivityId() const override { return activityId; }

protected:
    Owned<IWSCHelper> WSChelper;
    IHThorWebServiceCallActionArg & helper;
    IHThorWebServiceCallArg *   callHelper;
    StringBuffer authToken;
    CriticalSection crit;

    void init();
};

class CHThorWSCRowCallActivity : public CHThorWSCBaseActivity
{
public:
    CHThorWSCRowCallActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorWebServiceCallArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual bool needsAllocator() const { return true; }    

    virtual const void *nextRow();

private:
};

class CHThorHttpRowCallActivity : extends CHThorWSCRowCallActivity
{
public:
    CHThorHttpRowCallActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorHttpCallArg &_arg, ThorActivityKind _kind, EclGraph & _graph)
      : CHThorWSCRowCallActivity(agent, _activityId, _subgraphId, _arg, _kind, _graph)
    {
    }
    virtual const void *nextRow();
};

class CHThorSoapRowCallActivity : extends CHThorWSCRowCallActivity
{
public:
    CHThorSoapRowCallActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapCallArg &_arg, ThorActivityKind _kind, EclGraph & _graph)
      : CHThorWSCRowCallActivity(agent, _activityId, _subgraphId, _arg, _kind, _graph)
    {
    }
    virtual const void *nextRow();
};

class CHThorSoapRowActionActivity : public CHThorWSCBaseActivity
{
public:
    CHThorSoapRowActionActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapActionArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    IMPLEMENT_SINKACTIVITY

    virtual void execute();
};

class CHThorSoapDatasetCallActivity : public CHThorWSCBaseActivity
{
public:
    CHThorSoapDatasetCallActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapCallArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual const void *nextRow();
    virtual bool needsAllocator() const { return true; }    

    // IWSCRowProvider
    virtual const void * getNextRow();
};

class CHThorSoapDatasetActionActivity : public CHThorWSCBaseActivity
{
public:
    CHThorSoapDatasetActionActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapActionArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    IMPLEMENT_SINKACTIVITY

    virtual void execute();

    // IWSCRowProvider
    virtual const void * getNextRow();
};

//------------------------------------- New implementations ------------------------------------------

class CHThorChildNormalizeActivity : public CHThorSimpleActivityBase
{
    IHThorChildNormalizeArg &helper;
    bool started;
    bool eof;

public:
    CHThorChildNormalizeActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChildNormalizeArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
    virtual void ready();
};


class CHThorChildAggregateActivity : public CHThorSimpleActivityBase
{
    IHThorChildAggregateArg &helper;
    bool eof;

public:
    CHThorChildAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChildAggregateArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
    virtual void ready();
};

class CHThorChildGroupAggregateActivity : public CHThorSimpleActivityBase, public IHThorGroupAggregateCallback
{
    IHThorChildGroupAggregateArg &helper;
    RowAggregator aggregated;
    bool eof;
    bool gathered;

public:
    CHThorChildGroupAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChildGroupAggregateArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    IMPLEMENT_IINTERFACE

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();

//IHThorGroupAggregateCallback
    virtual void processRow(const void * src);
};


class CHThorChildThroughNormalizeActivity : public CHThorSimpleActivityBase
{
    IHThorChildThroughNormalizeArg &helper;
    OwnedConstRoxieRow lastInput;
    RtlDynamicRowBuilder outBuilder;
    unsigned __int64 numProcessedLastGroup;
    bool ok;

public:
    CHThorChildThroughNormalizeActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorChildThroughNormalizeArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
    virtual void ready();
    virtual void stop();
};


class CHThorDiskReadBaseActivity : public CHThorActivityBase, implements IThorDiskCallback
{
protected:
    IHThorDiskReadBaseArg &helper;
    OwnedIFile inputfile;
    OwnedIFileIO inputfileio;
    Owned<IBufferedSerialInputStream> inputstream;
    StringAttr tempFileName;
    Owned<IDistributedFilePartIterator> dfsParts;
    Owned<ILocalOrDistributedFile> ldFile;
    Owned<IException> saveOpenExc;
    size32_t fixedDiskRecordSize;
    Owned<IOutputMetaData> actualDiskMeta;
    IOutputMetaData *expectedDiskMeta;
    IOutputMetaData *projectedDiskMeta;
    unsigned partNum;
    bool eofseen;
    bool opened;
    bool compressed;
    bool rowcompressed;
    bool blockcompressed;
    MemoryAttr encryptionkey;
    bool persistent;
    bool grouped;
    bool isCodeSigned = false;
    enum ReadType:byte { rt_unknown, rt_binary, rt_csv, rt_xml, rt_json } readType = rt_unknown;
    RecordTranslationMode recordTranslationModeHint = RecordTranslationMode::Unspecified;
    unsigned __int64 stopAfter = 0;
    unsigned __int64 remoteLimit = 0;
    unsigned __int64 localOffset;
    unsigned __int64 offsetOfPart;
    stat_type numDiskReads = 0;
    cost_type diskAccessCost = 0;
    StringBuffer mangledHelperFileName;
    StringAttr logicalFileName;
    StringArray subfileLogicalFilenames;
    Owned<ISuperFileDescriptor> superfile;
    const IDynamicTransform *translator = nullptr;
    const IKeyTranslator *keyedTranslator = nullptr;
    Owned<ITranslator> translators;
    IPointerArrayOf<IOutputMetaData> actualLayouts;  // Do we need to keep more than one?
    IConstArrayOf<IFieldFilter> fieldFilters;  // These refer to the expected layout
    RowFilter actualFilter;               // This refers to the actual disk layout

    void checkFileType(IDistributedFile *file);
    void close();
    virtual void open();
    void resolve();
    virtual void verifyRecordFormatCrc() {} // do nothing here as (currently, and probably by design) not available for CSV and XML, so only implement for binary
    virtual void gatherInfo(IFileDescriptor * fileDesc);
    virtual void calcFixedDiskRecordSize() = 0;
    virtual bool openNext();
    StringBuffer &translateLFNtoLocal(const char *filename, StringBuffer &localName);
    virtual void closepart();
    bool checkOpenedFile(char const * filename, char const * filenamelist);
    inline void queryUpdateProgress()
    {
        agent.reportProgress(NULL);
    }

    RecordTranslationMode getLayoutTranslationMode()
    {
        if (recordTranslationModeHint != RecordTranslationMode::Unspecified)
            return recordTranslationModeHint;
        return agent.getLayoutTranslationMode();
    }

public:
    CHThorDiskReadBaseActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskReadBaseArg &_arg, ThorActivityKind _kind, IPropertyTree *node, EclGraph & _graph);
    ~CHThorDiskReadBaseActivity();
    IMPLEMENT_IINTERFACE

    virtual void ready();
    virtual void stop();

    IHThorInput *queryOutput(unsigned index)                { return this; }

//interface IHThorInput
    virtual bool isGrouped()                                { return grouped; }             
    virtual IOutputMetaData * queryOutputMeta() const               { return outputMeta; }

//interface IFilePositionProvider
    virtual unsigned __int64 getFilePosition(const void * row);
    virtual unsigned __int64 getLocalFilePosition(const void * row);
    virtual const char * queryLogicalFilename(const void * row) { return logicalFileName.get(); }
    virtual const byte * lookupBlob(unsigned __int64 id) { UNIMPLEMENTED; }
    virtual void updateProgress(IStatisticGatherer &progress) const override;
};

class CHThorBinaryDiskReadBase : public CHThorDiskReadBaseActivity, implements IIndexReadContext
{
protected:
    IHThorCompoundBaseArg & segHelper;
    Owned<ISourceRowPrefetcher> prefetcher;
    Owned<IOutputRowDeserializer> deserializer;
    CThorContiguousRowBuffer prefetchBuffer;
    CThorStreamDeserializerSource deserializeSource;
    MemoryBuffer translatedRow;
public:
    CHThorBinaryDiskReadBase(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskReadBaseArg &_arg, IHThorCompoundBaseArg & _segHelper, ThorActivityKind _kind, IPropertyTree *_node, EclGraph & _graph);

    virtual void ready();

    //interface IIndexReadContext
    virtual void append(IKeySegmentMonitor *segment) override { throwUnexpected(); }
    virtual void append(FFoption option, const IFieldFilter * filter) override;

protected:
    virtual void verifyRecordFormatCrc() { ::verifyFormatCrcSuper(helper.getDiskFormatCrc(), ldFile?ldFile->queryDistributedFile():NULL, false, true); }
    virtual void open();
    virtual bool openNext();
    virtual void closepart();

    inline bool segMonitorsMatch(const void * buffer)
    {
        if (actualFilter.numFilterFields())
        {
            const RtlRecord &actual = actualDiskMeta->queryRecordAccessor(true);
            unsigned numOffsets = actual.getNumVarFields() + 1;
            size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
            RtlRow row(actual, nullptr, numOffsets, variableOffsets);
            row.setRow(buffer, 0);  // Use lazy offset calculation
            return actualFilter.matches(row);
        }
        else
            return true;
    }

    virtual void calcFixedDiskRecordSize();
};


class CHThorDiskReadActivity : public CHThorBinaryDiskReadBase
{
    typedef CHThorBinaryDiskReadBase PARENT;
protected:
    IHThorDiskReadArg &helper;
    bool needTransform;
    byte eogPending;
    unsigned __int64 lastGroupProcessed;
    RtlDynamicRowBuilder outBuilder;
    unsigned __int64 limit;

public:
    CHThorDiskReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskReadArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree *node);

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};


class CHThorCsvReadActivity : public CHThorDiskReadBaseActivity
{
    typedef CHThorDiskReadBaseActivity PARENT;
public:
    CHThorCsvReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorCsvReadArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree *node);
    ~CHThorCsvReadActivity();
    virtual const void *nextRow();
    virtual void ready();
    virtual bool needsAllocator() const { return true; }    
    virtual void stop();

protected:
    void checkOpenNext();
    virtual bool openNext();
    virtual void gatherInfo(IFileDescriptor * fileDesc);
    virtual void calcFixedDiskRecordSize();

protected:
    IHThorCsvReadArg &  helper;
    unsigned            headerLines;
    size32_t            maxDiskSize;
    CSVSplitter         csvSplitter;    
    unsigned __int64 limit;
    size32_t maxRowSize;
};

class CHThorXmlReadActivity : public CHThorDiskReadBaseActivity, implements IXMLSelect
{
    typedef CHThorDiskReadBaseActivity PARENT;
public:
    IMPLEMENT_IINTERFACE;

    CHThorXmlReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorXmlReadArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree *_node);
    
    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    
    virtual const void *nextRow();

    //iface IXMLSelect
    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        localOffset = startOffset;
        lastMatch.set(&entry);
    }

protected:
    virtual bool openNext();
    virtual void closepart();
    virtual void gatherInfo(IFileDescriptor * fileDesc);
    virtual void calcFixedDiskRecordSize();

protected:
    IHThorXmlReadArg &  helper;
    Owned<IXmlToRowTransformer> rowTransformer;
    Owned<IPropertyTree> root;
    Owned<IPropertyTreeIterator> rows;
    XmlDatasetColumnProvider columns;
    Owned<IXMLParse> xmlParser;
    Owned<IColumnProvider> lastMatch;
    unsigned __int64 limit;
};


class CHThorDiskNormalizeActivity : public CHThorBinaryDiskReadBase
{
    typedef CHThorBinaryDiskReadBase PARENT;
protected:
    IHThorDiskNormalizeArg &helper;
    RtlDynamicRowBuilder outBuilder;
    unsigned __int64 limit;
    size32_t lastSizeRead;
    bool expanding;

    const void * createNextRow();
    virtual void gatherInfo(IFileDescriptor * fileDesc);

public:
    CHThorDiskNormalizeActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskNormalizeArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree *_node);

    virtual void stop();
    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};


class CHThorDiskAggregateActivity : public CHThorBinaryDiskReadBase
{
    typedef CHThorBinaryDiskReadBase PARENT;
protected:
    IHThorDiskAggregateArg &helper;
    RtlDynamicRowBuilder outBuilder;
    MemoryBuffer translatedRow;
    bool finished;

    virtual void gatherInfo(IFileDescriptor * fileDesc);

public:
    CHThorDiskAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskAggregateArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree *_node);

    virtual void stop();
    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};


class CHThorDiskCountActivity : public CHThorBinaryDiskReadBase
{
    typedef CHThorBinaryDiskReadBase PARENT;
protected:
    IHThorDiskCountArg &helper;
    bool finished;

    virtual void gatherInfo(IFileDescriptor * fileDesc);

public:
    CHThorDiskCountActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskCountArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree *_node);
    ~CHThorDiskCountActivity();

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};


//MORE: Could common up some of the code with the class above
class CHThorDiskGroupAggregateActivity : public CHThorBinaryDiskReadBase, implements IHThorGroupAggregateCallback
{
    typedef CHThorBinaryDiskReadBase PARENT;
protected:
    IHThorDiskGroupAggregateArg &helper;
    RowAggregator aggregated;
    MemoryBuffer translatedRow;
    bool eof;
    bool gathered;

    virtual void processRow(const void * next);
    virtual void gatherInfo(IFileDescriptor * fileDesc);

public:
    CHThorDiskGroupAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskGroupAggregateArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree *_node);
    IMPLEMENT_IINTERFACE

    virtual void ready();
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};


class CHThorLocalResultReadActivity : public CHThorSimpleActivityBase
{
    IHThorLocalResultReadArg &helper;
    IRecordSize * physicalRecordSize;
    IHThorGraphResult * result;
    ILocalEclGraphResults * graph;
    unsigned curRow;
    bool grouped;

public:
    CHThorLocalResultReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultReadArg &_arg, ThorActivityKind _kind, EclGraph & _graph, __int64 graphId);

    virtual void ready();
    virtual bool isGrouped() { return grouped; }
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorLocalResultWriteActivity : public CHThorActivityBase
{
    IHThorLocalResultWriteArg &helper;
    ILocalEclGraphResults * graph;

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorLocalResultWriteActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph, __int64 graphId);
    virtual void execute();
    virtual bool needsAllocator() const { return true; }
};

class CHThorDictionaryResultWriteActivity : public CHThorActivityBase
{
    IHThorDictionaryResultWriteArg &helper;
    ILocalEclGraphResults * graph;

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorDictionaryResultWriteActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorDictionaryResultWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph, __int64 graphId);
    virtual void execute();
    virtual bool needsAllocator() const { return true; }
};

class CHThorLocalResultSpillActivity : public CHThorSimpleActivityBase
{
    IHThorLocalResultSpillArg &helper;
    ILocalEclGraphResults * graph;
    IHThorGraphResult * result;
    bool nullPending;

public:
    CHThorLocalResultSpillActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultSpillArg &_arg, ThorActivityKind _kind, EclGraph & _graph, __int64 graphId);
    virtual const void *nextRow();

protected:
    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }    
};


class LocalResultInput : public CInterfaceOf<IEngineRowStream>
{
public:
    void init(IHThorGraphResult * _result)      
    { 
        result.set(_result); 
        curRow = 0; 
    }

    virtual const void * nextRow()
    {
        return result->getOwnRow(curRow++);
    }

    virtual void stop()
    {
    }

    virtual void resetEOF()
    {
        throwUnexpected();  // Should never be called on a source stream
    }

protected:
    Owned<IHThorGraphResult> result;
    unsigned curRow;
};



class ConstPointerArrayInput : public CInterfaceOf<IEngineRowStream>
{
public:
    void init(ConstPointerArray * _array)       { array = _array; curRow = 0; }

    virtual const void * nextRow()
    {
        if (array->isItem(curRow))
        {
            const void * ret = array->item(curRow);
            array->replace(NULL, curRow);
            curRow++;
            return ret;
        }
        return NULL;
    }

    virtual void stop()
    {
    }
    virtual void resetEOF()
    {
        throwUnexpected();  // Should never be called on a source stream
    }

protected:
    ConstPointerArray * array;
    unsigned curRow;
};

class CHThorLoopActivity : public CHThorSimpleActivityBase
{
    IHThorLoopArg &helper;
    IEngineRowStream * curInput;
    ConstPointerArray loopPending; //MORE: would be safer and neater to use an OwnedRowArray, but would need to change prototype of IHThorBoundLoopGraph::execute
    ConstPointerArrayInput arrayInput;
    LocalResultInput resultInput; 
    unsigned maxIterations;
    unsigned loopCounter;
    bool finishedLooping;
    bool eof;
    unsigned flags;
    Owned<IHThorBoundLoopGraph> loopGraph;
    rtlRowBuilder extractBuilder;

public:
    CHThorLoopActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLoopArg &helper, ThorActivityKind _kind, EclGraph & _graph);
    ~CHThorLoopActivity();
    virtual const void *nextRow();
    virtual void setBoundGraph(IHThorBoundLoopGraph * graph) { loopGraph.set(graph); }
    virtual bool needsAllocator() const { return true; }    

protected:
    virtual void ready();
    virtual void stop();
};


class CHThorGraphLoopResultReadActivity : public CHThorSimpleActivityBase
{
    IHThorGraphLoopResultReadArg * helper;
    ILocalEclGraphResults * graph;
    IRecordSize * physicalRecordSize;
    IHThorGraphResult * result;
    unsigned curRow;
    bool grouped;
    unsigned sequence;

public:
    CHThorGraphLoopResultReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopResultReadArg &_arg, ThorActivityKind _kind, EclGraph & _graph, __int64 graphId);
    CHThorGraphLoopResultReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _arg, ThorActivityKind _kind, EclGraph & _graph, __int64 graphId, unsigned _sequence, bool _grouped);

    virtual void ready();
    virtual bool isGrouped() { return grouped; }
    virtual bool needsAllocator() const { return true; }    

    //interface IHThorInput
    virtual const void *nextRow();
};

class CHThorGraphLoopResultWriteActivity : public CHThorActivityBase
{
    ILocalEclGraphResults * graph;

public:
    IMPLEMENT_SINKACTIVITY;

    CHThorGraphLoopResultWriteActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopResultWriteArg &_arg, ThorActivityKind _kind, EclGraph & _graph, __int64 graphId);
    virtual void execute();
    virtual bool needsAllocator() const { return true; }
};


class CHThorGraphLoopActivity : public CHThorSimpleActivityBase
{
    IHThorGraphLoopArg &helper;
    unsigned maxIterations;
    unsigned resultIndex;
    bool executed;
    unsigned flags;
    Owned<IHThorBoundLoopGraph> loopGraph;
    Owned<IHThorGraphResults> loopResults;
    IHThorGraphResult * finalResult;
    rtlRowBuilder extractBuilder;
    Owned<IEngineRowAllocator> rowAllocator;
    Owned<IEngineRowAllocator> rowAllocatorCounter;
    Linked<IOutputMetaData> counterMeta;

public:
    CHThorGraphLoopActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual const void *nextRow();
    virtual void setBoundGraph(IHThorBoundLoopGraph * graph) { loopGraph.set(graph); }
    virtual bool needsAllocator() const { return true; }    

protected:
    virtual void ready();
    virtual void stop();
};


class CHThorParallelGraphLoopActivity : public CHThorSimpleActivityBase
{
    IHThorGraphLoopArg &helper;
    unsigned maxIterations;
    unsigned resultIndex;
    bool executed;
    unsigned flags;
    Owned<IHThorBoundLoopGraph> loopGraph;
    Owned<IHThorGraphResults> loopResults;
    IHThorGraphResult * finalResult;
    rtlRowBuilder extractBuilder;
    Owned<IEngineRowAllocator> rowAllocator;

public:
    CHThorParallelGraphLoopActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopArg &_arg, ThorActivityKind _kind, EclGraph & _graph);
    virtual const void *nextRow();
    virtual void setBoundGraph(IHThorBoundLoopGraph * graph) { loopGraph.set(graph); }

protected:
    virtual void ready();
    virtual void stop();
};


class CHThorLibraryCallActivity;
class LibraryCallOutput : public CInterface, public IHThorInput, public IEngineRowStream
{
public:
    LibraryCallOutput(CHThorLibraryCallActivity * _owner, unsigned _output, IOutputMetaData * _meta);

    virtual const void * nextRow();
    virtual bool isGrouped();
    virtual IOutputMetaData * queryOutputMeta() const;

    virtual void ready();
    virtual void stop();
    virtual void resetEOF();
    virtual IEngineRowStream &queryStream() { return *this; }
    virtual void updateProgress(IStatisticGatherer &progress) const;

protected:
    IMPLEMENT_IINTERFACE;
    CHThorLibraryCallActivity * owner;
    unsigned output;
    Linked<IOutputMetaData> meta;
    Linked<IHThorGraphResult> result;
    unsigned curRow;
    bool gotRows;
    unsigned __int64 processed;
};

class CHThorLibraryCallActivity : public CHThorSimpleActivityBase
{
    friend class LibraryCallOutput;

    IHThorLibraryCallArg &helper;
    rtlRowBuilder extractBuilder;
    CriticalSection cs;
    Owned<IHThorGraphResults> results;
    ActivityState state;
    StringAttr libraryName;
    StringAttr embeddedGraphName;
    unsigned interfaceHash;

    CIArrayOf<LibraryCallOutput> outputs;
    Owned<IHThorBoundLoopGraph> libraryGraph;

public:
    CHThorLibraryCallActivity (IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorLibraryCallArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree * node);
    virtual const void *nextRow();
    virtual IHThorInput *queryOutput(unsigned idx);

    IHThorGraphResult * getResultRows(unsigned whichOutput);

protected:
    void updateOutputProgress(IStatisticGatherer &progress, const LibraryCallOutput & output, unsigned __int64 numProcessed) const;

protected:
    virtual void ready();
    virtual void stop();
};


class CHThorNWaySelectActivity : public CHThorMultiInputActivity
{
protected:
    IHThorNWaySelectArg & helper;
    IHThorInput * selectedInput;

public:
    CHThorNWaySelectActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorNWaySelectArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    //interface IHThorInput
    virtual void stop();
    virtual void ready();
    virtual const void * nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
    virtual IInputSteppingMeta * querySteppingMeta();
};

class CHThorStreamedIteratorActivity : public CHThorSimpleActivityBase
{
    IHThorStreamedIteratorArg &helper;
    Owned<IRowStream> rows;

public:
    CHThorStreamedIteratorActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorStreamedIteratorArg &_arg, ThorActivityKind _kind, EclGraph & _graph);

    virtual void ready();
    virtual void stop();
    virtual const void *nextRow();
};

class CHThorInputAdaptor : public ITypedRowStream, public CInterface
{
public:
    CHThorInputAdaptor(IHThorInput * _input) : input(_input) {}
    IMPLEMENT_IINTERFACE;

    virtual IOutputMetaData * queryOutputMeta() const { return input->queryOutputMeta(); }
    virtual const void *nextRow() { return input->nextRow(); }
    virtual void stop() { }

protected:
//  Linked<IHThorInput> input;
    IHThorInput * input;    // not currently a linkable interface
};

class SingleNodeActivityContext : public IThorActivityContext
{
public:
    SingleNodeActivityContext(unsigned _numStrands, unsigned _curStrand) : strands(_numStrands), curStrand(_curStrand) { assertex(curStrand < strands); }

    virtual bool isLocal() const override { return false; }
    virtual unsigned numSlaves() const override { return 1; }
    virtual unsigned numStrands() const override { return strands; }
    virtual unsigned querySlave() const override { return 0; }
    virtual unsigned queryStrand() const override { return curStrand; }
protected:
    unsigned strands;
    unsigned curStrand;
};


class CHThorExternalActivity : public CHThorMultiInputActivity
{
    IHThorExternalArg &helper;
    Owned<IRowStream> rows;
    Linked<IPropertyTree> graphNode;
    SingleNodeActivityContext activityContext;
    IArrayOf<CHThorInputAdaptor> inputAdaptors;
public:
    CHThorExternalActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorExternalArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree * _graphNode);

    virtual void ready() override;
    virtual void stop() override;

    virtual void execute() override;

    virtual const void *nextRow() override;
    virtual void setInput(unsigned index, IHThorInput *_input) override;

    virtual bool isGrouped() override { return outputMeta.isGrouped(); }
};


class CHThorNewDiskReadBaseActivity : public CHThorActivityBase, implements IThorDiskCallback, implements IIndexReadContext
{
protected:
    struct InputFileInfo : public CInterface
    {
        IDistributedFile * file;
        Owned<IOutputMetaData> actualMeta;
        Owned<const IPropertyTree> providerOptions;
        Owned<const IPropertyTree> formatOptions;
        unsigned actualCrc;
    };

    IHThorGenericDiskReadBaseArg &helper;
    IHThorCompoundBaseArg & segHelper;
    IDiskRowReader * activeReader = nullptr;
    IArrayOf<IDiskRowReader> readers;
    ILogicalRowStream * inputRowStream = nullptr;
    StringBuffer mangledHelperFileName;
    StringAttr tempFileName;
    const char * logicalFileName = "";
    CIArrayOf<InputFileInfo> subfiles;
    Owned<ISuperFileDescriptor> superfile;
    Owned<IDistributedFilePartIterator> dfsParts;
    Owned<ILocalOrDistributedFile> ldFile;
    Owned<IPropertyTree> spillPlane;
    IOutputMetaData *expectedDiskMeta = nullptr;
    IOutputMetaData *projectedDiskMeta = nullptr;
    IConstArrayOf<IFieldFilter> fieldFilters;  // These refer to the expected layout
    Owned<IPropertyTree> formatOptions;
    Owned<IPropertyTree> providerOptions;
    unsigned partNum = 0;
    unsigned helperFlags;
    RecordTranslationMode recordTranslationModeHint = RecordTranslationMode::Unspecified;
    bool useRawStream = false; // Constant for the lifetime of the activity
    bool grouped = false;
    bool outputGrouped = false;
    bool opened = false;
    bool finishedParts = false;
    bool isCodeSigned = false;
    bool useGenericReadWrites = false;
    unsigned __int64 stopAfter = 0;
    unsigned __int64 offsetOfPart = 0;
    void close();
    void resolveFile();
    virtual void verifyRecordFormatCrc();
    virtual void gatherInfo(IFileDescriptor * fileDesc);
    StringBuffer &translateLFNtoLocal(const char *filename, StringBuffer &localName);

    inline void queryUpdateProgress()
    {
        agent.reportProgress(NULL);
    }

    RecordTranslationMode getLayoutTranslationMode()
    {
        if (recordTranslationModeHint != RecordTranslationMode::Unspecified)
            return recordTranslationModeHint;
        return agent.getLayoutTranslationMode();
    }

    const char * queryReadFormat()
    {
        if (!useGenericReadWrites)
            return "flat";
        const char * readFormat = helper.queryFormat();
        //MORE: Later this should return null, and use the type of the file if it is a distibuted file
        if (!readFormat)
            readFormat = "flat";
        return readFormat;
    }

public:
    CHThorNewDiskReadBaseActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorGenericDiskReadBaseArg &_arg, IHThorCompoundBaseArg & _segHelper, ThorActivityKind _kind, IPropertyTree *node, EclGraph & _graph);
    ~CHThorNewDiskReadBaseActivity();
    IMPLEMENT_IINTERFACE

    virtual void ready();
    virtual void stop();

    IHThorInput *queryOutput(unsigned index)                { return this; }

//interface IHThorInput
    virtual bool isGrouped()                                { return grouped; }
    virtual IOutputMetaData * queryOutputMeta() const               { return outputMeta; }

//interface IFilePositionProvider
    virtual unsigned __int64 getFilePosition(const void * row);
    virtual unsigned __int64 getLocalFilePosition(const void * row);
    virtual const char * queryLogicalFilename(const void * row);
    virtual const byte * lookupBlob(unsigned __int64 id) { UNIMPLEMENTED; }

//interface IIndexReadContext
    virtual void append(IKeySegmentMonitor *segment) override { throwUnexpected(); }
    virtual void append(FFoption option, const IFieldFilter * filter) override;

protected:
    bool openFirstPart();
    void initStream(IDiskRowReader * reader, const char * filename);
    InputFileInfo * extractFileInformation(IDistributedFile * fileDesc, const IPropertyTree * curFormatOptions, const IPropertyTree * curProviderOptions);
    bool openFilePart(const char * filename);
    bool openFilePart(ILocalOrDistributedFile * localFile, IDistributedFilePart * filePart, unsigned whichPart);
    void setEmptyStream();

    virtual void open();
    virtual bool openNext();
    virtual void closepart();

    bool openNextPart(bool prevWasMissing);
    IDiskRowReader * ensureRowReader(const char * format, bool streamRemote, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, IOutputMetaData & projected, unsigned actualCrc, IOutputMetaData & actual, const IPropertyTree * providerOptions, const IPropertyTree * formatOptions);
};


class CHThorNewDiskReadActivity : public CHThorNewDiskReadBaseActivity
{
    typedef CHThorNewDiskReadBaseActivity PARENT;
protected:
    IHThorNewDiskReadArg &helper;
    bool needTransform;
    bool hasMatchFilter;
    unsigned __int64 lastGroupProcessed;
    RtlDynamicRowBuilder outBuilder;
    unsigned __int64 limit;
    unsigned __int64 remoteLimit = 0;

public:
    CHThorNewDiskReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorNewDiskReadArg &_arg, ThorActivityKind _kind, EclGraph & _graph, IPropertyTree *node);

    virtual void ready();
    virtual void stop();
    virtual bool needsAllocator() const { return true; }

    //interface IHThorInput
    virtual const void *nextRow();

protected:
    void onLimitExceeded();
};


//---------------------------------------------------------------------------------------------------------------------

#define MAKEFACTORY(NAME) \
extern HTHOR_API IHThorActivity * create ## NAME ## Activity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThor ## NAME ## Arg &arg, ThorActivityKind kind, EclGraph & _graph) \
{   return new CHThor ## NAME ##Activity(_agent, _activityId, _subgraphId, arg, kind, _graph); }

#define MAKEFACTORY_ARG(NAME, ARGNAME) \
extern HTHOR_API IHThorActivity * create ## NAME ## Activity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThor ## ARGNAME ## Arg &arg, ThorActivityKind kind, EclGraph & _graph) \
{   return new CHThor ## NAME ##Activity(_agent, _activityId, _subgraphId, arg, kind, _graph); }

#define MAKEFACTORY_EXTRA(NAME, EXTRATYPE) \
extern HTHOR_API IHThorActivity * create ## NAME ## Activity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThor ## NAME ## Arg &arg, ThorActivityKind kind, EclGraph & _graph, EXTRATYPE extra) \
{   return new CHThor ## NAME ##Activity(_agent, _activityId, _subgraphId, arg, kind, _graph, extra); }

extern ILocalOrDistributedFile *resolveLFNFlat(IAgentContext &agent, const char *logicalName, const char *errorTxt, bool optional, bool privilegedUser);

#endif // HTHOR_IPP_INCL

