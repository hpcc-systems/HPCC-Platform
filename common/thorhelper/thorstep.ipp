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

#ifndef THORSTEP_IPP_INCL
#define THORSTEP_IPP_INCL

#include "jset.hpp"
#include "jqueue.tpp"
#include "thorcommon.ipp"
#include "thorstep.hpp"

enum
{
    MJFneveroptimize        = 0x40000000,
    MJFalwaysoptimize       = 0x80000000,
};

//---------------------------------------------------------------------------

//It would make a lot of sense for the following interfaces to be identical...
//currently the primeRows() is sufficiently different to cause issues, otherwise they're close...

interface ISteppedJoinRowGenerator : public IInterface
{
    virtual void beforeProcessCandidates(const void * _equalityRow, bool needToVerifyNext, const bool * matched) = 0;
    virtual void afterProcessCandidates() = 0;
    virtual void cleanupAllCandidates() = 0;
    virtual void afterProcessingAll() = 0;
    virtual const void * nextOutputRow() = 0;
    virtual const void * nextOutputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra) = 0;
};

//---------------------------------------------------------------------------

class THORHELPER_API CSteppingMeta : public CInterface, implements IInputSteppingMeta
{
public:
    CSteppingMeta()
    {
        numFields = 0;
        fields = NULL;
        rangeCompare = NULL;
        distance = NULL;
        stepFlags = 0;
        priority = 0;
        distributed = false;
        postFiltered = false;
        hadStepExtra = false;
    }

    void intersect(IInputSteppingMeta * inputMeta);
    void init(ISteppingMeta * meta, bool _postFiltered)
    {
        init(meta->getNumFields(), meta->queryFields(), meta->queryCompare(), meta->queryDistance(), _postFiltered, 0);
    }

    void init(unsigned _numFields, const CFieldOffsetSize * _fields, IRangeCompare * _rangeCompare, IDistanceCalculator * _distance, bool _postFiltered, unsigned flags)
    {
        hadStepExtra = false;
        numFields = _numFields;
        fields = _fields;
        rangeCompare = _rangeCompare;
        distance = _distance;
        postFiltered = _postFiltered;
        stepFlags = flags;
        priority = 1.0e+300;
    }
    void intersectPriority(double value)
    {
        if (priority > value)
            priority = value;
    }
    void removePriority() { stepFlags &= ~SSFhaspriority; }
    void setDistributed() { distributed = true; }
    void setExtra(IHThorSteppedSourceExtra * extra)
    {
        hadStepExtra = true;
        stepFlags = extra->getSteppedFlags();
        priority = extra->getPriority();
    }

    virtual unsigned getNumFields()
    {
        return numFields;
    }

    virtual const CFieldOffsetSize * queryFields()
    {
        return fields;
    }

    virtual IRangeCompare * queryCompare()
    {
        return rangeCompare;
    }

    virtual IDistanceCalculator * queryDistance()
    {
        return distance;
    }

    virtual bool isDistributed() const                      { return distributed; }
    virtual bool hasPostFilter() 
    { 
        return postFiltered;
    }

    virtual unsigned getSteppedFlags() const { return stepFlags; }
    virtual double getPriority() { return priority; }

protected:
    double priority;
    const CFieldOffsetSize * fields;
    IRangeCompare * rangeCompare;
    IDistanceCalculator * distance;
    unsigned stepFlags;
    unsigned numFields;
    bool postFiltered;
    bool hadStepExtra;
    bool distributed;
};

//---------------------------------------------------------------------------

class LinkedRowQueue : public QueueOf<const void, false>
{
public:
    LinkedRowQueue()
    {
    }
    ~LinkedRowQueue()
    {
        loop
        {
            const void * next = dequeue();
            if (!next)
                break;
            rtlReleaseRow(next);
        }
    }
};
    

//---------------------------------------------------------------------------

/*
 This is like a lexer with a one row look ahead.  skip() is needed to move onto the next record.
 
 This class handles the row reading lookahead for the join inputs.
 There are a couple of complications to improve the efficiency of GSS
 a) It is inefficient to provide one seek record at a time to the second lowest frequency input.  So this class
    allows a set of rows to be read ahead, and stored, so they can be passed as a block to the second term.  These
    are stored in the seekRows member.
 b) If you have more than 2 terms (and especially if the second term does little to reduce the candidate set), then
    we really need to pass a block of seeks to the third term.  However to get a block of seeks for the second term
    it means we need to read even further ahead on the primary term.  So the fields from the previous
    seek are sometimes transferred to the readAheadRows member (to avoid them being passed as seek pointers)
    
 This is not designed to support multi-threaded access.
 */
 
class THORHELPER_API CSteppedInputLookahead : public CInterface, implements IMultipleStepSeekInfo
{
public:
    CSteppedInputLookahead(ISteppedInput * _input, IInputSteppingMeta * _inputStepping, IEngineRowAllocator * _rowAllocator, IRangeCompare * _compare, bool _paranoid);
    ~CSteppedInputLookahead();

    bool canSmartStep() const { return inputStepping != NULL; }

    const void * consume();
    const void * next();
    const void * nextGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);
    unsigned queryMaxStepable(ISteppingMeta * callerStepping) const;
    IDistanceCalculator * queryDistance() { return inputStepping ? inputStepping->queryDistance() : NULL; }
    void resetEOF();
    void setAlwaysReadExact();
    void setReadAhead(bool value);
    void setRestriction(const void * _value, unsigned _num);
    void skip();
    const void * skipnext();

    IMultipleStepSeekInfo * createMutipleReadWrapper();
    void createMultipleSeekWrapper(IMultipleStepSeekInfo * wrapper);

//interface IMultipleStepSeekInfo
    virtual void ensureFilled(const void * seek, unsigned numFields, unsigned maxcount);
    virtual unsigned ordinality() const;
    virtual const void * querySeek(unsigned i) const;
    
//inline helper functions
    inline void clearPending() { if (readAheadRow) skip(); }
    inline bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return input->gatherConjunctions(collector); }
    inline unsigned getStepFlags() { return inputStepping ? inputStepping->getSteppedFlags() : 0; }
    inline double getPriority() { return inputStepping ? inputStepping->getPriority() : 0.0; }
    inline bool readsRowsRemotely() { return inputStepping && inputStepping->isDistributed(); }
    inline bool hasPriority() { return (getStepFlags() & SSFhaspriority) != 0; }
    inline bool hasPostFilter() { return isPostFiltered; }
    inline const void * next(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        if (seek)
            return nextGE(seek, numFields, wasCompleteMatch, stepExtra);
        return next();
    }
    inline const void * nextEQ(const void * seek, unsigned numFields, const SmartStepExtra & stepExtra)
    {
        if (isPostFiltered)
        {
            bool isExactMatch = true;
            const void * ret = nextGE(seek, numFields, isExactMatch, stepExtra);
            if (!ret || isExactMatch)
                return ret;
            return NULL;
        }
        return next();
    }
    inline void resetInputEOF() { input->resetEOF(); }

private:
    //These functions should always be called to get the next row, never call input->next... directly
    const void * nextInputRow();
    const void * nextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);

protected:
    void fill();
    void initStepping(IInputSteppingMeta * inputStepping, ISteppingMeta * callerStepping);
    inline bool includeInOutput(const void * row)
    {
        return !restrictValue || !row || compare->docompare(row, restrictValue, numRestrictFields) == 0;
    }

private:
    Linked<ISteppedInput> input;
    LinkedRowQueue readAheadRows; // rows that have been read from the input to provide seekRows for other terms
    LinkedRowQueue seekRows;      // unique read-ahead rows that are >= the last seek position provided to ensureFilled()

protected:
    IRangeCompare * compare;
    const void * readAheadRow;
    IInputSteppingMeta * inputStepping;
    IMultipleStepSeekInfo * lowestFrequencyInput;
    Linked<IEngineRowAllocator> rowAllocator;
    const void * restrictValue;
    const void * previousReadAheadRow;
    unsigned maxFields;
    unsigned numStepableFields;
    unsigned numRestrictFields;
    unsigned stepFlagsMask;
    unsigned stepFlagsValue;
    bool paranoid;
    bool readAheadRowIsExactMatch;
    bool isPostFiltered;
};

class DummySteppedInput : public CSteppedInputLookahead
{
public:
    DummySteppedInput() : CSteppedInputLookahead(NULL, NULL, NULL, NULL, false) {}
};

typedef CIArrayOf<CSteppedInputLookahead> CSteppedInputLookaheadArray;


class PreservedRow
{
public:
    inline PreservedRow(IEngineRowAllocator * _allocator) : allocator(_allocator) { row = NULL; }
    inline ~PreservedRow() { kill(); }

    inline const void * query() { return row; }
    inline void clear() { kill(); row = NULL; }
    inline void set(const void * value) { kill(); row = allocator->linkRow(value); }
    inline void setown(const void * value) { kill(); row = value; }

protected:
    inline void kill() { if (row) allocator->releaseRow(const_cast<void *>(row)); }

protected:
    Linked<IEngineRowAllocator> allocator;
    const void * row;
};

//---------------------------------------------------------------------------

class THORHELPER_API CSteppedCandidateMerger : public CStreamMerger
{
public:
    CSteppedCandidateMerger(unsigned _numEqualFields) : CStreamMerger(true)
    {
        numEqualFields = _numEqualFields;
        candidateRow = NULL;
        inputArray = NULL;
        equalCompare = NULL;
    }
    ~CSteppedCandidateMerger() { assertex(!candidateRow); }

    void init(IEngineRowAllocator * _allocator, ICompare * _equalCompare, ICompare * _mergeCompare, bool _dedup, IRangeCompare * _rangeCompare)
    {
        CStreamMerger::init(_mergeCompare, _dedup, _rangeCompare);
        rowAllocator.set(_allocator);
        equalCompare = _equalCompare;
    }

    void initInputs(CSteppedInputLookaheadArray * _inputArray)
    {
        CStreamMerger::initInputs(_inputArray->ordinality());
        inputArray = _inputArray;
    }

    virtual bool pullInput(unsigned i, const void * seek, unsigned numFields, const SmartStepExtra * stepExtra)
    {
        CSteppedInputLookahead * input = &inputArray->item(i);
        const void *next;
        bool matches = true;
        if (seek)
        {
            next = input->nextGE(seek, numFields, matches, *stepExtra);
        }
        else if (candidateRow)
        {
            //Read ahead using the equality row, so we don't start reading too many rows from unfiltered datasets
            //Note: return mismatches only has a meaning for rows that don't match the equality fields
            SmartStepExtra equalExtra(SSEFreturnMismatches, NULL);
            bool isExactMatch = true;
            next = input->nextGE(candidateRow, numEqualFields, isExactMatch, equalExtra);

            //Optimization: If the post filter doesn't match then it can't match the candidate row.
            if (!isExactMatch)
                next = NULL;
        }
        else
            next = input->next();

        if (next && includeInCandidates(next))
        {
            pending[i] = input->consume();
            pendingMatches[i] = matches;
            return true;
        }

        pending[i] = NULL;
        return false;
    }

    virtual void releaseRow(const void * row)
    {
        rowAllocator->releaseRow(row);
    }

    inline void setCandidateRow(const void * _equalityRow)
    {
        candidateRow = _equalityRow;
    }

    inline bool includeInCandidates(const void * row)
    {
        return !candidateRow || equalCompare->docompare(candidateRow, row) == 0;
    }

protected:
    CSteppedInputLookaheadArray * inputArray;
    Linked<IEngineRowAllocator> rowAllocator;
    ICompare * equalCompare;
    const void * candidateRow;
    unsigned numEqualFields;
};


class THORHELPER_API CUnfilteredSteppedMerger : public CInterface, implements ISteppedJoinRowGenerator
{
public:
    CUnfilteredSteppedMerger(unsigned _numEqualFields) : merger(_numEqualFields)
    {
        firstCandidateRows = NULL;
    }
    ~CUnfilteredSteppedMerger()
    {
        delete [] firstCandidateRows;
    }

    IMPLEMENT_IINTERFACE

    void init(IEngineRowAllocator * _allocator, ICompare * _equalCompare, ICompare * _mergeCompare, bool _dedup, IRangeCompare * _rangeCompare)
    {
        merger.init(_allocator, _equalCompare, _mergeCompare, _dedup, _rangeCompare);
    }

    void initInputs(CSteppedInputLookaheadArray * _inputArray)
    {
        inputArray = _inputArray;
        merger.initInputs(_inputArray);
        firstCandidateRows = new const void *[_inputArray->ordinality()];
    }

//ISteppedJoinRowGenerator
    virtual void beforeProcessCandidates(const void * _equalityRow, bool needToVerifyNext, const bool * matched);

    virtual void afterProcessCandidates()
    {
        merger.setCandidateRow(NULL);
        merger.reset();
    }

    virtual void cleanupAllCandidates()
    {
        merger.reset();     // not strictly necessary...
    }

    virtual void afterProcessingAll()
    {
        merger.cleanup();
    }
        
    virtual const void * nextOutputRow()
    {
        return merger.nextRow();
    }

    virtual const void * nextOutputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        return merger.nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
    }

protected:
    CSteppedCandidateMerger merger;
    CSteppedInputLookaheadArray * inputArray;
    const void * * firstCandidateRows;
};


//---------------------------------------------------------------------------

class CFilteredInputBuffer : public CInterface
{
public:
    CFilteredInputBuffer(IEngineRowAllocator * _allocator, IRangeCompare * _stepCompare, ICompare * _equalCompare, CSteppedInputLookahead * _input, unsigned _numEqualFields);
    ~CFilteredInputBuffer();

    inline bool isFullyMatched() const { return numMatched == rows.ordinality(); }
    inline bool isMatched(unsigned i) const { return matched->test(i); }
#ifdef _DEBUG
    //check that we never tag something as matched if already matched, since also keeping a match tally for optimization.
    inline void noteMatch(unsigned i) { bool prev = matched->testSet(i, true); assertex(!prev); numMatched++; }
#else
    inline void noteMatch(unsigned i) { matched->set(i, true); numMatched++; }
#endif
    const void * consume();
    const void * consumeGE(const void * seek, unsigned numFields);
    void fill(const void * equalityRow);
    void removeMatched();
    void removeUnmatched();
    void reset();

protected:
    inline void append(const void * next) { rows.append(next); }
    void remove(unsigned i);

public:
    IEngineRowAllocator * allocator;
    ICompare * equalCompare;
    IRangeCompare * stepCompare;
    CSteppedInputLookahead * input;
    ConstPointerArray rows;
    Owned<IBitSet> matched;
    unsigned numMatched;
    unsigned readIndex;
    unsigned numEqualFields;
};

typedef CIArrayOf<CFilteredInputBuffer> CFilteredInputBufferArray;

class THORHELPER_API CFilteredMerger : public CStreamMerger
{
public:
    CFilteredMerger() : CStreamMerger(true) { inputArray = NULL; rowAllocator = NULL; }

    void init(IEngineRowAllocator * _allocator, ICompare * _mergeCompare, bool _dedup, IRangeCompare * _rangeCompare)
    {
        CStreamMerger::init(_mergeCompare, _dedup, _rangeCompare);
        rowAllocator = _allocator;
    }

    void initInputs(CFilteredInputBufferArray * _inputArray)
    {
        CStreamMerger::initInputs(_inputArray->ordinality());
        inputArray = _inputArray;
    }

    virtual bool pullInput(unsigned i, const void * seek, unsigned numFields, const SmartStepExtra * stepExtra)
    {
        CFilteredInputBuffer * input = &inputArray->item(i);
        const void * next = seek ? input->consumeGE(seek, numFields) : input->consume();
        pending[i] = next;
        pendingMatches[i] = true;
        return (next != NULL);
    }

    virtual void releaseRow(const void * row)
    {
        rowAllocator->releaseRow(row);
    }

protected:
    CFilteredInputBufferArray * inputArray;
    IEngineRowAllocator * rowAllocator;
};


class CFilteredSteppedMerger : public CInterface, implements ISteppedJoinRowGenerator
{
public:
    CFilteredSteppedMerger();
    ~CFilteredSteppedMerger();
    IMPLEMENT_IINTERFACE

    void init(IEngineRowAllocator * _allocator, IHThorNWayMergeJoinArg & helper, CSteppedInputLookaheadArray * _inputArray);

//ISteppedJoinRowGenerator
    virtual void beforeProcessCandidates(const void * _equalityRow, bool needToVerifyNext, const bool * matched);
    virtual void afterProcessCandidates();
    virtual void cleanupAllCandidates();
    virtual void afterProcessingAll();
    virtual const void * nextOutputRow();
    virtual const void * nextOutputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);

protected:
    void postFilterRows();
    void tagMatches();
    bool tagMatches(unsigned level, unsigned numRows);

protected:
    const void * * matches;
    ICompare * equalCompare;
    ICompareEq * extraCompare;
    INaryCompareEq * globalCompare;
    unsigned numInputs;
    unsigned minMatches;
    unsigned maxMatches;
    unsigned fullyMatchedLevel;
    CIArrayOf<CFilteredInputBuffer> inputs;
    CFilteredMerger merger;
    byte joinKind;
};

//---------------------------------------------------------------------------

class CSteppedConjunctionOptimizer;
class CJoinGenerator;
class THORHELPER_API CMergeJoinProcessor : public CInterface, implements ISteppedInput, implements ISteppedJoin
{
public:
    CMergeJoinProcessor(IHThorNWayMergeJoinArg & _arg);
    ~CMergeJoinProcessor();
    IMPLEMENT_IINTERFACE

    //interface IHThorInput
    virtual void afterProcessing();
    virtual void beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator);
    virtual bool candidatesExhaustEquality() { return true; }
    
    void addInput(ISteppedInput * _input);
    const void * nextRow();
    const void * nextGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);
    void queryResetEOF();

//interface ISteppedInput
    virtual const void * nextInputRow();
    virtual const void * nextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector);
    virtual IInputSteppingMeta * queryInputSteppingMeta()       { return &thisSteppingMeta; }
    virtual void resetEOF();

//interface ISteppedJoin - defined for conjunction optimizer
    virtual unsigned getNumEqualFields() const { return numEqualFields; }
    virtual ISteppingMeta * querySteppingMeta() const { return mergeSteppingMeta; }
    virtual void startRestrictedJoin(const void * equalityRow, unsigned numEqualityFields);
    virtual void stopRestrictedJoin();
    virtual void markRestrictedJoin(unsigned numEqualityFields)
    {
        combineConjunctions = false;
    }
protected:
    virtual bool findCandidates(const void * seekValue, unsigned numSeekFields) = 0;
    virtual void finishCandidates();
    
    inline const void * nextCandidate() { return outputProcessor->nextOutputRow(); }
    inline const void * nextCandidateGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra) 
    { return outputProcessor->nextOutputRowGE(seek, numFields, wasCompleteMatch, stepExtra); }

    inline void clearMatches() { memset(matched, false, numInputs); }
    inline bool hasCandidates() { return candidateEqualityRow != NULL; }
    inline void cleanupCandidates()
    {
        if (hasCandidates())
            finishCandidates();
    }
    void connectRemotePriorityInputs();
    bool createConjunctionOptimizer();
    void setCandidateRow(const void * row, bool inputsMayBeEmpty, const bool * matched);

protected:
    void createTempSeekBuffer();
    void createMerger();
    void createEqualityJoinProcessor();

protected:
    IHThorNWayMergeJoinArg & helper;
    Linked<IEngineRowAllocator> inputAllocator;             // this can only be used after beforeProcessing() has been called.
    Linked<IEngineRowAllocator> outputAllocator;            // and shouldn't really be used after afterProcessing() has been called.
    CSteppingMeta thisSteppingMeta;
    ISteppingMeta * mergeSteppingMeta;
    IRangeCompare * stepCompare;
    ICompare * equalCompare;
    ICompareEq * equalCompareEq;
    Owned<ISteppedJoinRowGenerator> outputProcessor;
    bool * matched;
    unsigned numEqualFields;
    unsigned numExternalEqualFields;
    unsigned numInputs;
    unsigned flags;
    size32_t maxSeekRecordSize;
    IArrayOf<ISteppedInput> rawInputs;
    CSteppedInputLookaheadArray inputs;
    CSteppedInputLookaheadArray orderedInputs;
    UnsignedArray searchOrder;
    void * candidateEqualityRow;
    void * tempSeekBuffer;
    const void * lowestSeekRow;
    CSteppedConjunctionOptimizer * conjunctionOptimizer;
    bool eof;
    bool combineConjunctions;
    bool allInputsAreOuterInputs;
};


//---------------------------------------------------------------------------

class THORHELPER_API CAndMergeJoinProcessor : public CMergeJoinProcessor
{
public:
    CAndMergeJoinProcessor(IHThorNWayMergeJoinArg & _arg);

protected:
    virtual void beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator);
    virtual bool findCandidates(const void * seekValue, unsigned numSeekFields);

private:
    unsigned nextToMatch(unsigned lastInput) const;
};

//---------------------------------------------------------------------------

class THORHELPER_API CAndLeftMergeJoinProcessor : public CMergeJoinProcessor
{
public:
    CAndLeftMergeJoinProcessor(IHThorNWayMergeJoinArg & _arg);

    virtual void beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator);
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector);

protected:
    virtual bool findCandidates(const void * seekValue, unsigned numSeekFields);

protected:
    bool isLeftOnly;
};

//---------------------------------------------------------------------------

class BestMatchItem : public CInterface
{
public:
    unsigned duplicates;
    const void * value;
    unsigned input;
};

//Retain an ordered list of the best matches so far, assuming a small number of inputs, and a relatively large chance of duplicates.
//Store an ordered list of matches, with the entry for the first unique value containing a duplicate count
class BestMatchManager
{
public:
    void init(ICompare * _compare, unsigned numInputs);
    void kill();

    void associate(unsigned input, const void * value);
    void remove(unsigned whichValue);

    unsigned getInput(unsigned whichValue, unsigned inputIndex) const;
    unsigned getInput0(unsigned inputIndex) const;
    inline unsigned numInputs() const                       { return numEntries; }
    inline unsigned numInputs0() const                      { return matches.item(0).duplicates; }
    unsigned numInputs(unsigned whichValue) const;
    inline const void * queryValue0(unsigned whichValue) const  { return matches.item(0).value; }
    const void * queryValue(unsigned whichValue) const;

protected:
    unsigned getValueOffset(unsigned idx) const;

protected:
    CIArrayOf<BestMatchItem> matches;
    ICompare * compare;
    unsigned numEntries;
};


class THORHELPER_API CMofNMergeJoinProcessor : public CMergeJoinProcessor
{
public:
    CMofNMergeJoinProcessor(IHThorNWayMergeJoinArg & _arg);

protected:
    virtual void afterProcessing();
    virtual void beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator);
    virtual bool findCandidates(const void * seekValue, unsigned numSeekFields);
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector);

private:
    unsigned nextToMatch() const;

protected:
    unsigned minMatches;
    unsigned maxMatches;
    unsigned numActive;
    BestMatchManager matches;
    bool * alive;
    bool * candidateMask;
};


//---------------------------------------------------------------------------

class RowQueue : public QueueOf<const void, false>
{
public:
    RowQueue(IEngineRowAllocator * _allocator) : allocator(_allocator) {}
    ~RowQueue() { kill(); }

    void kill()
    {
        loop
        {
            const void * next = dequeue();
            if (!next)
                return;
            allocator->releaseRow(next);
        }
    }

    void skip()
    {
        allocator->releaseRow(dequeue());
    }

protected:
    Linked<IEngineRowAllocator> allocator;
};


class CNaryJoinLookaheadQueue : public CInterface
{
public:
    CNaryJoinLookaheadQueue(IEngineRowAllocator * _inputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookahead * _input, CNaryJoinLookaheadQueue * _left, const void * * _activeRowPtr);

    bool beforeProcessCandidates(const void * _equalityRow, bool needToVerifyNext);

    inline bool setCandidateRange(unsigned __int64 maxDistance, bool canConsumeBeyondMax)
    {
        curRow = 0;
        maxRow = readAheadTo(maxDistance, canConsumeBeyondMax);
        return maxRow != 0;
    }

    inline bool setCandidateLowest()
    {
        curRow = 0;
        maxRow = 1;
        assertex(rows.ordinality());
        return true;
    }

    inline bool setCandidateAll()
    {
        curRow = 0;
        maxRow = rows.ordinality();
        return maxRow != 0;
    }

    void clearPending();
    bool firstSelection();
    bool nextSelection();
    void readCandidateAll();

    // throw away, and skip all records < minDistance, return true if there is a record <= maxDistance
    bool ensureCandidateExists(unsigned __int64 minDistance, unsigned __int64 maxDistance);
    bool checkExistsGE(const void * seek, unsigned numFields);
    
    inline void updateContext(CNaryJoinLookaheadQueue * _left, const void * * _activeRowPtr)
    {
        left = _left;
        activeRowPtr = _activeRowPtr;
    }

    inline const void * activeRow() const { return rows.item(curRow); }
    inline bool empty() { return rows.ordinality() == 0; }
    inline const void * next() { return rows.head(); }
    inline const void * nextUnmatched() { return unmatchedRows.head(); }
    inline void noteMatched() { if (matchedLeft) matchedLeft->set(curRow + numSkipped, true); }
           void skip();
    inline void skipUnmatched() { unmatchedRows.skip(); }
    const void * nextUnqueued();
    bool ensureNonEmpty();
    bool flushUnmatched();
    void trackUnmatched() { matchedLeft.setown(createThreadSafeBitSet()); }

    inline void consumeNextInput() { rows.enqueue(input->consume()); }

protected:
    bool findValidSelection(unsigned initialRow);
    unsigned readAheadTo(unsigned __int64 maxDistance, bool canConsumeBeyondMax);       // read all records <= value, return #records that match

protected:
    IHThorNWayMergeJoinArg & helper;
    ICompareEq * equalCompareEq;
    ICompareEq * nonSteppedCompareEq;
    IRangeCompare * stepCompare;
    const void * equalityRow;
    CNaryJoinLookaheadQueue * left;
    Linked<CSteppedInputLookahead> input;
    Owned<IBitSet> matchedLeft;
    const void * * activeRowPtr;
    RowQueue rows;
    RowQueue unmatchedRows;
    unsigned maxRow;
    unsigned curRow;
    unsigned numSkipped;
    unsigned numEqualFields;
    bool done;
};

class CNaryJoinLookaheadQueue;

typedef CIArrayOf<CNaryJoinLookaheadQueue> LookaheadQueueArray;

class THORHELPER_API CJoinLowestInputMerger : public CStreamMerger
{
public:
    CJoinLowestInputMerger(LookaheadQueueArray & _inputs) : CStreamMerger(false), inputs(_inputs) {}

    void init(IEngineRowAllocator * _allocator, ICompare * _compare, IRangeCompare * _rangeCompare)
    {
        // I'm not convinced dedup will work properly. if next() removes dedup of the top item on the lowest input, (e..g, if anchored R->L)
        //would be ok if inputs were inverted for that case.
        CStreamMerger::init(_compare, false, _rangeCompare);
        rowAllocator.set(_allocator);
    }

    void initInputs()
    {
        CStreamMerger::initInputs(inputs.ordinality());
    }

    virtual void consumeInput(unsigned i)
    {
        throwUnexpected();
    }

    virtual bool pullInput(unsigned i, const void * seek, unsigned numFields, const SmartStepExtra * stepExtra)
    {
        assertex(!seek);
        CNaryJoinLookaheadQueue & input = inputs.item(i);
        input.ensureNonEmpty();
        const void * next = input.next();           //NB: Does not consume from input queue
        pending[i] = (void *)next;                  //should really be const void *
        pendingMatches[i] = true;
        return next != NULL;
    }

    virtual void releaseRow(const void * row)
    {
        //Do not dispose - this never has ownership of the rows
    }

    virtual void skipInput(unsigned i)
    {
        inputs.item(i).skip();
        pending[i] = NULL;
    }

    bool skipNextLowest()
    {
        skipRow();
        ensureNext();
        return (activeInputs == inputs.ordinality());
    }

protected:
    LookaheadQueueArray & inputs;
    Linked<IEngineRowAllocator> rowAllocator;
};


class CJoinGenerator : public CInterface, implements ISteppedJoinRowGenerator
{
public:
    CJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs);
    ~CJoinGenerator();
    IMPLEMENT_IINTERFACE

    virtual void afterProcessCandidates();
    virtual void beforeProcessCandidates(const void * _equalityRow, bool needToVerifyNext, const bool * matched);
    virtual void cleanupAllCandidates();
    virtual void afterProcessingAll() {}
    virtual const void * nextOutputRow();
    virtual const void * nextOutputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);

protected:
    virtual bool gatherNextCandidates() = 0;
    virtual bool nextCandidate() = 0;

    bool firstSelection();
    bool nextSelection();

protected:
    IHThorNWayMergeJoinArg & helper;
    Linked<IEngineRowAllocator> inputAllocator;
    Linked<IEngineRowAllocator> outputAllocator;
    IRangeCompare * stepCompare;
    INaryCompareEq * globalCompare;
    CNaryJoinLookaheadQueue * lastActiveInput;
    const void * * rows;
    unsigned numActiveInputs;
    CIArrayOf<CNaryJoinLookaheadQueue> inputs;
    enum { JSfirst, JSdone, JShascandidate, JSnextcandidate, JSgathercandidates } state;
    byte joinKind;
    bool isSpecialLeftJoin;

};


//Handle situation where must match L->R or R->L
class CEqualityJoinGenerator : public CJoinGenerator
{
public:
    CEqualityJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs);

protected:
    virtual void afterProcessCandidates();
    virtual bool gatherNextCandidates();
    virtual bool nextCandidate();

//new virtuals
    virtual void selectLowestInput();
    virtual bool selectNextLowestInput();

protected:
    bool doGatherNextCandidates();
    void prefetchAllCandidates();

protected:
    CNaryJoinLookaheadQueue * lowestInput;          // first input containing any records.
};



class CSortedEqualityJoinGenerator : public CEqualityJoinGenerator
{
public:
    CSortedEqualityJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs);
    ~CSortedEqualityJoinGenerator();

protected:
    virtual void afterProcessCandidates();
    virtual void selectLowestInput();
    virtual bool selectNextLowestInput();

protected:
    CJoinLowestInputMerger lowestSpotter;
};



class CRangeJoinGenerator : public CJoinGenerator
{
public:
    CRangeJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs);

protected:
    __int64 maxRightBeforeLeft;
    __int64 maxLeftBeforeRight;
};


//Handle situation where must match L->R or R->L
class CAnchoredRangeJoinGenerator : public CRangeJoinGenerator
{
public:
    CAnchoredRangeJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs);

protected:
    virtual bool gatherNextCandidates();
    virtual bool nextCandidate();
    virtual const void * nextOutputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);

    bool doGatherNextCandidates();
    bool nextMatchesAnyConsumed();

    inline __int64 maxDistanceAfterLowest(unsigned searchInput) const
    {
        if (searchInput < iLowest)
            return maxRightBeforeLeft * (iLowest - searchInput);
        else
            return maxLeftBeforeRight * (searchInput - iLowest);
    }

protected:
    unsigned iLowest;
    CNaryJoinLookaheadQueue * lowestInput;

};



class CProximityRangeJoinGenerator : public CRangeJoinGenerator
{
public:
    CProximityRangeJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs);
    ~CProximityRangeJoinGenerator();

    virtual void afterProcessCandidates();

protected:
    virtual bool gatherNextCandidates();
    virtual bool nextCandidate();

    bool gatherNextCandidates(unsigned iLowest);

protected:
    CJoinLowestInputMerger lowestSpotter;
};



class THORHELPER_API CProximityJoinProcessor : public CMergeJoinProcessor
{
public:
    CProximityJoinProcessor(IHThorNWayMergeJoinArg & _helper);

    virtual void beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator);
    virtual bool candidatesExhaustEquality() { return false; }

    virtual bool findCandidates(const void * seekValue, unsigned numSeekFields);

protected:
    unsigned nextToMatch() const;
    unsigned getBestToSeekFrom(unsigned seekInput) const;
    __int64 maxDistanceBefore(unsigned fixedInput, unsigned searchInput) const;

    inline bool equalityComponentMatches(const void * left, const void * right) const
    {
        return equalCompareEq->match(left, right);
    }

    inline unsigned __int64 getRangeValue(unsigned whichInput) const
    {
        return helper.extractRangeValue(inputs.item(whichInput).next());
    }

protected:
    __int64 maxRightBeforeLeft;                 // only valid between beforeProcessing() and afterProcessing()
    __int64 maxLeftBeforeRight;
};

#endif
