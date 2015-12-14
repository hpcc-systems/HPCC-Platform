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

#include "jlib.hpp"
#include "jlog.hpp"
#include "jqueue.tpp"
#include "jexcept.hpp"
#include "thorcommon.hpp"
#include "thorstep.ipp"
#include "thorstep2.ipp"
#include "roxiemem.hpp"

#ifdef _DEBUG
#define CHECK_CONSISTENCY
#endif

using roxiemem::OwnedConstRoxieRow;

const static SmartStepExtra knownLowestFrequencyTermStepExtra(SSEFreadAhead, NULL);
const static SmartStepExtra unknownFrequencyTermStepExtra(SSEFreturnMismatches, NULL);
const static SmartStepExtra nonSeekStepExtra(SSEFreturnUnbufferedMatches, NULL);                    // if doing next() instead of nextGE()
const static SmartStepExtra nonBufferedMatchStepExtra(SSEFreturnUnbufferedMatches, NULL);
const static SmartStepExtra nonBufferedMismatchStepExtra(SSEFreturnMismatches, NULL);

bool stepFieldsMatch(const CFieldOffsetSize * leftFields, unsigned leftIndex, const CFieldOffsetSize * rightFields, unsigned rightIndex)
{
    const CFieldOffsetSize * leftField = leftFields + leftIndex;
    const CFieldOffsetSize * rightField = rightFields + rightIndex;
    return (leftField->offset == rightField->offset) && (leftField->size == rightField->size);
}

bool stepFieldsMatch(ISteppingMeta * leftMeta, unsigned leftIndex, ISteppingMeta * rightMeta, unsigned rightIndex)
{
    if ((leftIndex >= leftMeta->getNumFields()) || (rightIndex >= rightMeta->getNumFields()))
        return false;

    return stepFieldsMatch(leftMeta->queryFields(), leftIndex, rightMeta->queryFields(), rightIndex);
}

unsigned getNumMatchingFields(ISteppingMeta * inputStepping, ISteppingMeta * callerStepping)
{
    unsigned numStepableFields = 0;
    if (inputStepping && callerStepping)
    {
        //Determine where the stepping fields overlap, and work out the extent.
        unsigned inputCount = inputStepping->getNumFields();
        for (unsigned i=0; i < inputCount; i++)
        {
            if (!stepFieldsMatch(callerStepping, i, inputStepping, i))
                break;
            numStepableFields++;
        }
    }
    return numStepableFields;
}

void verifySteppingCompatible(ISteppingMeta * inputStepping, ISteppingMeta * callerStepping)
{
    if (inputStepping && callerStepping)
    {
        //Determine where the stepping fields overlap, and work out the extent.
        unsigned parentCount = callerStepping->getNumFields();
        unsigned inputCount = inputStepping->getNumFields();
        unsigned max = parentCount < inputCount ? parentCount : inputCount;
        for (unsigned i=0; i < max; i++)
        {
            if (!stepFieldsMatch(callerStepping, i, inputStepping, i))
                throw MakeStringException(999, "Stepping field %d, input and join do not match", i);
        }
    }
}

//---------------------------------------------------------------------------

void CSteppingMeta::intersect(IInputSteppingMeta * inputMeta)
{
    if (inputMeta)
    {
        unsigned maxFields = inputMeta->getNumFields();
        if (maxFields > numFields)
            maxFields = numFields;
        for (unsigned curField = 0; curField < maxFields; curField++)
        {
            if (!stepFieldsMatch(inputMeta->queryFields(), curField, fields, curField))
            {
                numFields = curField;
                break;
            }
        }
        if (inputMeta->hasPostFilter())
            postFiltered = true;
        if (inputMeta->isDistributed())
            setDistributed();

        unsigned inputFlags = inputMeta->getSteppedFlags();
        double inputPriority = inputMeta->getPriority();
        if (hadStepExtra)
        {
            stepFlags &= inputFlags;
            if (priority != inputPriority)
                stepFlags &= ~SSFhaspriority;
        }
        else
        {
            hadStepExtra = true;
            stepFlags = inputFlags;
            priority = inputPriority;
        }
    }
    else
        numFields = 0;
}

//---------------------------------------------------------------------------

CSteppedInputLookahead::CSteppedInputLookahead(ISteppedInput * _input, IInputSteppingMeta * _inputStepping, IEngineRowAllocator * _rowAllocator, IRangeCompare * _compare, bool _paranoid) 
: input(_input), compare(_compare)
{
    maxFields = compare ? compare->maxFields() : 0;
    readAheadRow = NULL;
    readAheadRowIsExactMatch = true;
    stepFlagsMask = 0;
    stepFlagsValue = 0;
    paranoid = _paranoid;
    previousReadAheadRow = NULL;
    rowAllocator.set(_rowAllocator);
    inputStepping = _inputStepping;
    numStepableFields = inputStepping ? inputStepping->getNumFields() : 0;
    isPostFiltered = inputStepping ? inputStepping->hasPostFilter() : false;
    setRestriction(NULL, 0);
    lowestFrequencyInput = NULL;
}

CSteppedInputLookahead::~CSteppedInputLookahead()
{
    if (previousReadAheadRow)
        rowAllocator->releaseRow(previousReadAheadRow);
    if (readAheadRow)
        rowAllocator->releaseRow(readAheadRow);
}


const void * CSteppedInputLookahead::nextInputRow()
{
    if (readAheadRows.ordinality())
        return readAheadRows.dequeue();
    return input->nextInputRow();
}
    
const void * CSteppedInputLookahead::nextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    while (readAheadRows.ordinality())
    {
        OwnedConstRoxieRow next = readAheadRows.dequeue();
        if (compare->docompare(next, seek, numFields) >= 0)
        {
            assertex(wasCompleteMatch);
            return (void *)next.getClear();
        }
    }
    return input->nextInputRowGE(seek, numFields, wasCompleteMatch, stepExtra);
}

void CSteppedInputLookahead::ensureFilled(const void * seek, unsigned numFields, unsigned maxcount)
{
    const void * lastSeekRow = NULL;
    //Remove any rows from the seek list that occur before the new seek row
    while (seekRows.ordinality())
    {
        const void * next = seekRows.head();
        if (compare->docompare(next, seek, numFields) >= 0)
        {
            //update the seek pointer to the best value - so that lowestInputProvider can skip its seekRows if necessary
            seek = seekRows.tail();
            lastSeekRow = seek;
            break;
        }
        rowAllocator->releaseRow(seekRows.dequeue());
    }

    //Could the current readahead row be part of the seek set.
    if (readAheadRow && compare->docompare(readAheadRow, seek, numFields) >= 0)
    {
        //Check not already added - could conceivably happen after rows are read directly beyond the matching seeks.
        if (!lastSeekRow || compare->docompare(readAheadRow, lastSeekRow, numFields) > 0)
        {
            seekRows.enqueue(rowAllocator->linkRow(readAheadRow));
            lastSeekRow = readAheadRow;
            seek = readAheadRow;
        }
    }

    //Return mismatches is selected because we don't want it to seek exact matches beyond the last seek position
    unsigned flags = (SSEFreturnMismatches & ~stepFlagsMask) | stepFlagsValue;
    SmartStepExtra inputStepExtra(flags, lowestFrequencyInput);
    seekRows.ensure(maxcount);
    while (seekRows.ordinality() < maxcount)
    {
        bool wasCompleteMatch = true;
        const void * next = input->nextInputRowGE(seek, numFields, wasCompleteMatch, inputStepExtra);
        if (!next)
            break;
        //wasCompleteMatch can be false if we've just read the last row returned from a block of reads, 
        //but if so the next read request will do another blocked read, so just ignore this one.
        if (wasCompleteMatch)
        {
            readAheadRows.enqueue(next);
            if (!lastSeekRow || compare->docompare(next, lastSeekRow, numFields) > 0)
            {
                //Only record unique seek positions in the seek rows
                seekRows.enqueue(rowAllocator->linkRow(next));
                lastSeekRow = next;
            }
            //update the seek pointer to the best value.  
            seek = next;
        }
        else
            rowAllocator->releaseRow(next);
    }
}

unsigned CSteppedInputLookahead::ordinality() const
{
    return seekRows.ordinality();
}

const void * CSteppedInputLookahead::querySeek(unsigned i) const
{
    return seekRows.item(i);
}

const void * CSteppedInputLookahead::consume()
{
    if (!readAheadRow)
        fill();
    if (!includeInOutput(readAheadRow))
        return NULL;

    if (paranoid && readAheadRow)
    {
        if (previousReadAheadRow)
            rowAllocator->releaseRow(previousReadAheadRow);
        previousReadAheadRow = rowAllocator->linkRow(readAheadRow);
    }

    const void * ret = readAheadRow;
    readAheadRow = NULL;
    readAheadRowIsExactMatch = true;
    return ret;
}

IMultipleStepSeekInfo * CSteppedInputLookahead::createMutipleReadWrapper()
{
    return this;
}

void CSteppedInputLookahead::createMultipleSeekWrapper(IMultipleStepSeekInfo * wrapper)
{
    lowestFrequencyInput = wrapper;
}

void CSteppedInputLookahead::fill()
{
    readAheadRowIsExactMatch = true;
    if (restrictValue && numStepableFields)
    {
        //note - this will either return a valid value to be included in the range,
        //or if invalid then it must be out of range -> will fail includeInOutput later,
        //but we may as well keep the row 
        unsigned numFields = numRestrictFields < numStepableFields ? numRestrictFields : numStepableFields;

        //Default to returning mismatches, but could be overidden from outside
        unsigned flags = (SSEFreturnMismatches & ~stepFlagsMask) | stepFlagsValue;
        SmartStepExtra inputStepExtra(flags, lowestFrequencyInput);
        readAheadRow = nextInputRowGE(restrictValue, numFields, readAheadRowIsExactMatch, inputStepExtra);

        if (paranoid && readAheadRow)
        {
            int c = compare->docompare(readAheadRow, restrictValue, numFields);
            if (c < 0)
                throw MakeStringException(1001, "Input to stepped join preceeds seek point");
            if ((c == 0) && !readAheadRowIsExactMatch)
                throw MakeStringException(1001, "Input to stepped join returned mismatch that matched equality fields");
        }
    }
    else
    {
        //Unusual.  Normally we will step the input but this branch can occur for some unusual joins - e.g. a LEFT ONLY stepped join.
        //Likely to cause problems if it occurs on anything other than the lowest frequency input if the index is remote
        readAheadRow = nextInputRow();
    }

    if (paranoid && readAheadRow && previousReadAheadRow && compare)
    {
        if (compare->docompare(previousReadAheadRow, readAheadRow, maxFields) > 0)
            throw MakeStringException(1001, "Input to stepped join isn't sorted as expected");
    }
}

const void * CSteppedInputLookahead::next()
{
    if (!readAheadRowIsExactMatch)
    {
        if (includeInOutput(readAheadRow))
            skip();
        else
            return NULL;
    }

    if (!readAheadRow)
        fill();

    if (!includeInOutput(readAheadRow))
        return NULL;

    return readAheadRow;
}

const void * CSteppedInputLookahead::nextGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    if (readAheadRow)
    {
        int c = compare->docompare(readAheadRow, seek, numFields);
        if (c >= 0)
        {
            if (!includeInOutput(readAheadRow))
                return NULL;
            if (readAheadRowIsExactMatch)
                return readAheadRow;
            //readAheadRow is beyond seek point => ok to return an incomplete match
            if (stepExtra.returnMismatches() && (c != 0))
            {
                wasCompleteMatch = readAheadRowIsExactMatch;
                return readAheadRow;
            }
        }
        skip();
    }

    if (numStepableFields)
    {
        //This class is directly told whether it should be using readAhead, so need to create a modified stepExtra
        unsigned flags = (stepExtra.queryFlags() & ~stepFlagsMask) | stepFlagsValue;
        SmartStepExtra inputStepExtra(flags, lowestFrequencyInput);
        unsigned stepFields = (numFields <= numStepableFields) ? numFields : numStepableFields;
        loop
        {
            readAheadRowIsExactMatch = true;
            readAheadRow = nextInputRowGE(seek, stepFields, readAheadRowIsExactMatch, inputStepExtra);

            if (paranoid && readAheadRow)
            {
                int c = compare->docompare(readAheadRow, seek, stepFields);
                if (c < 0)
                    throw MakeStringException(1001, "Input to stepped join preceeds seek point");
                if ((c == 0) && !readAheadRowIsExactMatch)
                    throw MakeStringException(1001, "Input to stepped join returned mismatch that matched equality fields");
            }

            if (!readAheadRow || !includeInOutput(readAheadRow))
                return NULL;

            if (numFields <= numStepableFields)
            {
                wasCompleteMatch = readAheadRowIsExactMatch;
                return readAheadRow;
            }

            //if !readAheadRowIsExactMatch then isCompleteMatch must have been provided => ok to return a mismatch
            //if mismatch on stepFields, then must have mismatch on numFields (since stepFields <= numFields) => can return now
            if (!readAheadRowIsExactMatch)
            {
                wasCompleteMatch = readAheadRowIsExactMatch;
                return readAheadRow;
            }

            if (compare->docompare(readAheadRow, seek, numFields) >= 0)
            {
                wasCompleteMatch = readAheadRowIsExactMatch;
                return readAheadRow;
            }

            skip();
        }

        //now need to do an incremental seek on the subsequent fields to find an exact value >
    }

    //now narrow down
    loop
    {
        const void * cur = next();
        if (!cur)
            return NULL;
        if (compare->docompare(cur, seek, numFields) >= 0)
            return cur;
        skip();
    }
}

unsigned CSteppedInputLookahead::queryMaxStepable(ISteppingMeta * callerStepping) const
{
    return getNumMatchingFields(inputStepping, callerStepping);
}

void CSteppedInputLookahead::setAlwaysReadExact()
{
    //can be used to force reading only exact matches (for the known lowest priority input)
    stepFlagsMask |= SSEFreturnMismatches;
}

void CSteppedInputLookahead::setReadAhead(bool value)
{ 
    //This never removes readahead if requested somewhere else, so don't update the mask.
    if (value)
        stepFlagsValue |= SSEFreadAhead;
    else
        stepFlagsValue &= ~SSEFreadAhead;
}

void CSteppedInputLookahead::setRestriction(const void * _value, unsigned _num)
{
    restrictValue = _value;
    numRestrictFields = _num;
}

void CSteppedInputLookahead::resetEOF()
{
    if (numRestrictFields == 0)
        resetInputEOF();
}

void CSteppedInputLookahead::skip()
{
    if (paranoid)
    {
        if (previousReadAheadRow)
            rowAllocator->releaseRow(previousReadAheadRow);
        previousReadAheadRow = readAheadRow;
    }
    else
    {
        if (readAheadRow)
            rowAllocator->releaseRow(readAheadRow);
    }

    //NB: Don't read ahead until we have to...
    readAheadRow = NULL;
    readAheadRowIsExactMatch = true;
}

const void * CSteppedInputLookahead::skipnext()
{
    skip();
    return next();
}

//---------------------------------------------------------------------------

void CUnfilteredSteppedMerger::beforeProcessCandidates(const void * _equalityRow, bool needToVerifyNext, const bool * matched)
{
    merger.setCandidateRow(_equalityRow);
    unsigned numInputs = inputArray->ordinality();
    for (unsigned i=0; i< numInputs; i++)
    {
        if (!needToVerifyNext || matched[i])
            firstCandidateRows[i] = inputArray->item(i).consume();
        else
            firstCandidateRows[i] = NULL;
    }
    merger.primeRows(firstCandidateRows);
}

//---------------------------------------------------------------------------


CFilteredInputBuffer::CFilteredInputBuffer(IEngineRowAllocator * _allocator, IRangeCompare * _stepCompare, ICompare * _equalCompare, CSteppedInputLookahead * _input, unsigned _numEqualFields)
{
    allocator = _allocator;
    stepCompare = _stepCompare;
    equalCompare = _equalCompare;
    input = _input;
    matched.setown(createThreadSafeBitSet());
    numMatched = 0;
    readIndex = 0;
    numEqualFields = _numEqualFields;
}

CFilteredInputBuffer::~CFilteredInputBuffer()
{
}


const void * CFilteredInputBuffer::consume() 
{ 
    if (!rows.isItem(readIndex))
        return NULL;
    const void * ret = rows.item(readIndex);
    rows.replace(NULL, readIndex);
    readIndex++;
    return ret;
}

const void * CFilteredInputBuffer::consumeGE(const void * seek, unsigned numFields)
{
    while (rows.isItem(readIndex))
    {
        const void * cur = rows.item(readIndex);
        if (stepCompare->docompare(cur, seek, numFields) >= 0)
        {
            rows.replace(NULL, readIndex);
            readIndex++;
            return cur;
        }
        readIndex++;
    }
    return NULL;
}

void CFilteredInputBuffer::fill(const void * equalityRow)
{
    const void * next = input->consume();
    assertex(next);
    append(next);
    if (equalityRow)
    {
        loop
        {
            bool matches = true;
            SmartStepExtra stepExtra(SSEFreturnMismatches, NULL);
            const void * next = input->nextGE(equalityRow, numEqualFields, matches, stepExtra);
            if (!next || !matches || equalCompare->docompare(equalityRow, next) != 0)
                break;
            append(input->consume());
        }
    }
    else
    {
        loop
        {
            const void * next = input->consume();
            if (!next)
                break;
            append(next);
        }
    }
}

void CFilteredInputBuffer::removeMatched()
{
    ForEachItemInRev(i, rows)
    {
        if (isMatched(i))
            remove(i);
    }
}

void CFilteredInputBuffer::removeUnmatched()
{
    ForEachItemInRev(i, rows)
    {
        if (!isMatched(i))
            remove(i);
    }
}

void CFilteredInputBuffer::remove(unsigned i)
{
    const void * row = rows.item(i);
    rows.remove(i);
    allocator->releaseRow(row);
}


void CFilteredInputBuffer::reset()
{
    ForEachItemIn(i, rows)
    {
        const void * cur = rows.item(i);
        if (cur)
            allocator->releaseRow(cur);
    }
    rows.kill();
    matched->reset();
    numMatched = 0;
    readIndex = 0;
}

CFilteredSteppedMerger::CFilteredSteppedMerger()
{
    matches = NULL;
    joinKind = 0;
    numInputs = 0;
    equalCompare = NULL;
    extraCompare = NULL;
    globalCompare = NULL;
    minMatches = 0;
    maxMatches = 0;
    fullyMatchedLevel = 0;
}

CFilteredSteppedMerger::~CFilteredSteppedMerger()
{
    delete [] matches;
}

void CFilteredSteppedMerger::init(IEngineRowAllocator * _allocator, IHThorNWayMergeJoinArg & helper, CSteppedInputLookaheadArray * inputArray)
{
    unsigned flags = helper.getJoinFlags();
    joinKind = (flags & IHThorNWayMergeJoinArg::MJFkindmask);
    numInputs = inputArray->ordinality();
    matches = new const void * [numInputs];
    equalCompare = helper.queryEqualCompare();
    extraCompare = helper.queryNonSteppedCompare();
    globalCompare = NULL;
    unsigned numEqualFields = helper.numEqualFields();
    if (flags & IHThorNWayMergeJoinArg::MJFglobalcompare)
        globalCompare = helper.queryGlobalCompare();

    if (joinKind == IHThorNWayMergeJoinArg::MJFmofn)
    {
        minMatches = helper.getMinMatches();
        maxMatches = helper.getMaxMatches();
    }
    else
    {
        minMatches = numInputs;
        maxMatches = numInputs;
    }
    IRangeCompare * stepCompare = helper.querySteppingMeta()->queryCompare();
    ForEachItemIn(i, *inputArray)
        inputs.append(*new CFilteredInputBuffer(_allocator, stepCompare, equalCompare, &inputArray->item(i), numEqualFields));
    merger.init(_allocator, helper.queryMergeCompare(), (flags & IHThorNWayMergeJoinArg::MJFdedup) != 0, stepCompare);
    merger.initInputs(&inputs);
}


//ISteppedJoinRowGenerator
void CFilteredSteppedMerger::beforeProcessCandidates(const void * equalityRow, bool needToVerifyNext, const bool * matched)
{
    //Exaustively read from each of the inputs into each of the buffers
    ForEachItemIn(i, inputs)
    {
        if (!needToVerifyNext || matched[i])
            inputs.item(i).fill(equalityRow);
    }
    postFilterRows();

    //No point priming the rows here - will be just as efficient to use the default action
}

void CFilteredSteppedMerger::afterProcessCandidates()
{
    ForEachItemIn(i, inputs)
        inputs.item(i).reset();
    merger.reset();
}

void CFilteredSteppedMerger::cleanupAllCandidates()
{
    merger.reset();     // not strictly necessary...
}

void CFilteredSteppedMerger::afterProcessingAll()
{
    merger.cleanup();
}
    
const void * CFilteredSteppedMerger::nextOutputRow()
{
    return merger.nextRow();
}

const void * CFilteredSteppedMerger::nextOutputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    return merger.nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
}

bool CFilteredSteppedMerger::tagMatches(unsigned level, unsigned numRows)
{
    CFilteredInputBuffer & right = inputs.item(level);
    unsigned maxLevel = inputs.ordinality()-1;
    ConstPointerArray & curRows = right.rows;
    if (curRows.ordinality())
    {
        bool valid = false;
        const void * lhs = matches[numRows-1];
        ForEachItemIn(i, curRows)
        {
            //If we have had a match at this level, and this item is already matched,
            //and all levels higher than this have already been completely matched),
            //then no need to check this item (and its children) again, since it won't change anything.
            bool alreadyMatched = right.isMatched(i);
            if (!valid || level + 1 < fullyMatchedLevel || !alreadyMatched)
            {
                const void * rhs = curRows.item(i);
                unsigned matchedRows = numRows;

                bool recurse;
                if (!extraCompare || extraCompare->match(lhs, rhs))
                {
                    matches[matchedRows++] = rhs;
                    recurse = matchedRows <= maxMatches;
                }
                else
                {
                    //for mofn, check enough levels left to create a potential match, for others it will fail.
                    unsigned remain = maxLevel-level;
                    recurse = (numRows + remain >= minMatches);
                }

                if (recurse)
                {
                    bool isFullMatch;
                    if (level == maxLevel)
                        isFullMatch = (!globalCompare || globalCompare->match(matchedRows, matches));
                    else
                        isFullMatch = tagMatches(level+1, matchedRows);

                    if (isFullMatch)
                    {
                        valid = true;
                        if (!alreadyMatched)
                            right.noteMatch(i);

                        //If the previous level is fully matched, and so is this one - then update the minimum fully matched level
                        if ((level + 1 == fullyMatchedLevel) && right.isFullyMatched())
                            fullyMatchedLevel--;

                        //If all rows in this level and above are fully matched, then iterating any further will have no effect.
                        //Could potentially reduce a O(N^m) to O(mN) if the majority of elements match.
                        if (level >= fullyMatchedLevel)
                            break;
                    }
                }
            }
        }
        return valid;
    }
    else
    {
        //mofn may still be ok with a skipped level or two
        unsigned remain = maxLevel-level;
        if (numRows + remain >= minMatches)
        {
            if (level == maxLevel)
                return (!globalCompare || globalCompare->match(numRows, matches));
            else
                return tagMatches(level+1, numRows);
        }
        return false;
    }

}


void CFilteredSteppedMerger::tagMatches()
{
    unsigned numInputs = inputs.ordinality();
    fullyMatchedLevel = numInputs;

    //for m of n, need to start matching at levels 0,1,.. numLevels - minMatches 
    unsigned iterateLevels = numInputs - minMatches;
    for (unsigned level =0; level <= iterateLevels; level++)
    {
        CFilteredInputBuffer & left = inputs.item(level);
        ForEachItemIn(i, left.rows)
        {
            matches[0] = left.rows.item(i);
            bool thisMatched;

            //mofn(1) may not have another level, to just check global compare.
            if (level == numInputs-1)
                thisMatched = (!globalCompare || globalCompare->match(1, matches));
            else
                thisMatched = tagMatches(level+1, 1);

            if (thisMatched)
            {
                if (!left.isMatched(i))
                    left.noteMatch(i);

                //Check if this level, and all above are now fully matched.  If so, we're done.
                if ((level + 1 == fullyMatchedLevel) && left.isFullyMatched())
                {
                    fullyMatchedLevel--;
                    break;
                }
            }
        }

        if (level >= fullyMatchedLevel)
            break;
    }
}


void CFilteredSteppedMerger::postFilterRows()
{
    tagMatches();

    unsigned max = inputs.ordinality();
    switch (joinKind)
    {
    case IHThorNWayMergeJoinArg::MJFinner:
    case IHThorNWayMergeJoinArg::MJFmofn:
        {
            for (unsigned i=0; i < max; i++)
                inputs.item(i).removeUnmatched();
            break;
        }
    case IHThorNWayMergeJoinArg::MJFleftouter:
        {
            for (unsigned i=1; i < max; i++)
                inputs.item(i).removeUnmatched();
            break;
        }
    case IHThorNWayMergeJoinArg::MJFleftonly:
        {
            inputs.item(0).removeMatched();
            unsigned max = inputs.ordinality();
            for (unsigned i=1; i < max; i++)
                inputs.item(i).reset();
            break;
        }
    }
}


//---------------------------------------------------------------------------

CMergeJoinProcessor::CMergeJoinProcessor(IHThorNWayMergeJoinArg & _arg) : helper(_arg)
{
    mergeSteppingMeta = helper.querySteppingMeta();
    assertex(mergeSteppingMeta);
    stepCompare = mergeSteppingMeta->queryCompare();
    equalCompare = helper.queryEqualCompare();
    equalCompareEq = helper.queryEqualCompareEq();
    numEqualFields = helper.numEqualFields();
    flags = helper.getJoinFlags();

    matched = NULL;
    candidateEqualityRow = NULL;
    numExternalEqualFields = 0;
    conjunctionOptimizer = NULL;
    tempSeekBuffer = NULL;
    lowestSeekRow = NULL;
    combineConjunctions = true;
    allInputsAreOuterInputs = false;
    maxSeekRecordSize = 0;
    numInputs = 0;
    eof = true;

    assertex(helper.numOrderFields() == mergeSteppingMeta->getNumFields());
    bool hasPostfilter = false;
    thisSteppingMeta.init(mergeSteppingMeta->getNumFields(), mergeSteppingMeta->queryFields(), stepCompare, mergeSteppingMeta->queryDistance(), hasPostfilter, SSFhaspriority|SSFisjoin);
}

CMergeJoinProcessor::~CMergeJoinProcessor()
{
    afterProcessing();
}


void CMergeJoinProcessor::addInput(ISteppedInput * _input)
{
    IInputSteppingMeta * _meta = _input->queryInputSteppingMeta();
    verifySteppingCompatible(_meta, mergeSteppingMeta);
    rawInputs.append(*LINK(_input));
    if (_meta)
    {
        if (!_meta->hasPriority())
            thisSteppingMeta.removePriority();
        else
            thisSteppingMeta.intersectPriority(_meta->getPriority());

        if (_meta->isDistributed())
            thisSteppingMeta.setDistributed();
    }
    else
        thisSteppingMeta.removePriority();
}

void CMergeJoinProcessor::afterProcessing()
{
    cleanupCandidates();
    if (outputProcessor)
    {
        outputProcessor->afterProcessingAll();
        outputProcessor.clear();
    }
    if (conjunctionOptimizer)
    {
        conjunctionOptimizer->afterProcessing();
        delete conjunctionOptimizer;
        conjunctionOptimizer = NULL;
    }
    delete [] matched;
    matched = NULL;
    inputs.kill();
    rawInputs.kill();
    orderedInputs.kill();
    if (lowestSeekRow)
    {
        inputAllocator->releaseRow(lowestSeekRow);
        lowestSeekRow = NULL;
    }
    if (tempSeekBuffer)
    {
        inputAllocator->releaseRow(tempSeekBuffer);
        tempSeekBuffer = NULL;
    }

    //Now free the allocators
    inputAllocator.clear();
    outputAllocator.clear();
    maxSeekRecordSize = 0;
}


void CMergeJoinProcessor::createTempSeekBuffer()
{
    tempSeekBuffer = inputAllocator->createRow();
#ifdef _DEBUG
    //Clear the complete tempSeekBBuffer record, so that toXML() can be used to trace the seek row in roxie
    if (helper.getJoinFlags() & IHThorNWayMergeJoinArg::MJFhasclearlow)
    {
        RtlStaticRowBuilder rowBuilder(tempSeekBuffer, inputAllocator->queryOutputMeta()->getMinRecordSize());
        helper.createLowInputRow(rowBuilder);
    }
#endif
}


void CMergeJoinProcessor::beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator)
{
    inputAllocator.set(_inputAllocator);
    outputAllocator.set(_outputAllocator);
    //The seek components must all be fixed width, so the seek record size must be <= the minimum size of the input record
    maxSeekRecordSize = inputAllocator->queryOutputMeta()->getMinRecordSize();

    bool paranoid = (flags & IHThorNWayMergeJoinArg::MJFassertsorted) != 0;
    ForEachItemIn(i1, rawInputs)
    {
        ISteppedInput & cur = rawInputs.item(i1);
        inputs.append(* new CSteppedInputLookahead(&cur, cur.queryInputSteppingMeta(), inputAllocator, stepCompare, paranoid));
    }

    if (flags & IHThorNWayMergeJoinArg::MJFhasclearlow)
    {
        RtlDynamicRowBuilder rowBuilder(inputAllocator);
        size32_t size = helper.createLowInputRow(rowBuilder);
        lowestSeekRow = rowBuilder.finalizeRowClear(size);
    }

    cleanupCandidates();
    eof = false;
    numInputs = inputs.ordinality();
    matched = new bool[numInputs];
    if (numInputs == 0)
        eof = true;

    //Sort the inputs by the preferred processing order (if provided), ensuring no duplicates
    clearMatches();
    ForEachItemIn(i2, searchOrder)
    {
        unsigned next = searchOrder.item(i2);
        if (next < numInputs && !matched[next])
        {
            orderedInputs.append(OLINK(inputs.item(next)));
            matched[next] = true;
        }
    }

    //MORE: We really should move the most-stepable inputs to the start
    for (unsigned i3 = 0; i3 < numInputs; i3++)
    {
        if (!matched[i3])
            orderedInputs.append(OLINK(inputs.item(i3)));
    }
}


bool CMergeJoinProcessor::createConjunctionOptimizer()
{
    if (inputs.ordinality())
    {
        conjunctionOptimizer = new CSteppedConjunctionOptimizer(inputAllocator, helper, this);
        if (gatherConjunctions(*conjunctionOptimizer) && conjunctionOptimizer->worthCombining())
        {
            conjunctionOptimizer->beforeProcessing();
            return true;
        }

        delete conjunctionOptimizer;
        conjunctionOptimizer = NULL;
    }
    combineConjunctions = false;
    return false;
}


void CMergeJoinProcessor::createMerger()
{
    ICompareEq * extraCompare = helper.queryNonSteppedCompare();
    bool hasGlobalCompare = (flags & IHThorNWayMergeJoinArg::MJFglobalcompare) != 0;

    if (!extraCompare && !hasGlobalCompare)
    {
        Owned<CUnfilteredSteppedMerger> simpleMerger = new CUnfilteredSteppedMerger(numEqualFields);
        simpleMerger->init(inputAllocator, equalCompare, helper.queryMergeCompare(), (flags & IHThorNWayMergeJoinArg::MJFdedup) != 0, stepCompare);
        simpleMerger->initInputs(&inputs);
        outputProcessor.setown(simpleMerger.getClear());
    }
    else
    {
        Owned<CFilteredSteppedMerger> simpleMerger = new CFilteredSteppedMerger();
        simpleMerger->init(inputAllocator, helper, &inputs);
        outputProcessor.setown(simpleMerger.getClear());
    }
}


void CMergeJoinProcessor::createEqualityJoinProcessor()
{
    if (numEqualFields >= helper.numOrderFields())
        outputProcessor.setown(new CEqualityJoinGenerator(inputAllocator, outputAllocator, helper, inputs));
    else
        outputProcessor.setown(new CSortedEqualityJoinGenerator(inputAllocator, outputAllocator, helper, inputs));
}


void CMergeJoinProcessor::finishCandidates()
{
    if (outputProcessor)
        outputProcessor->afterProcessCandidates();

    assertex(hasCandidates());
    inputAllocator->releaseRow(candidateEqualityRow);
    candidateEqualityRow = NULL;
}

bool CMergeJoinProcessor::gatherConjunctions(ISteppedConjunctionCollector & collector)
{
    allInputsAreOuterInputs = true;
    ForEachItemIn(i, inputs)
    {
        CSteppedInputLookahead & cur = inputs.item(i);
        if (!cur.gatherConjunctions(collector))
            collector.addInput(cur);
        else
        {
            collector.addPseudoInput(cur);
            allInputsAreOuterInputs = false;
        }
    }
    collector.addJoin(*this);
    return true;
}

const void * CMergeJoinProcessor::nextInputRow()
{
    if (!hasCandidates() && !findCandidates(NULL, 0))
        return NULL;

    loop
    {
        const void * next = nextCandidate();
        if (next)
            return next;
        finishCandidates();

        //Abort early if externally optimized, and not proximity (since they may not have read all records for this equality)
        if ((numEqualFields == numExternalEqualFields) && candidatesExhaustEquality())
            return NULL;

        if (!findCandidates(NULL, 0))
            return NULL;
    }
}

const void * CMergeJoinProcessor::nextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    //First check the next row from the candidates, it may be ok.
    if (hasCandidates())
    {
        unsigned compareFields = numFields < numEqualFields ? numFields : numEqualFields;
        //check whether the candidates could possibly return the match
        if (stepCompare->docompare(candidateEqualityRow, seek, compareFields) == 0)
        {
            const void * next = nextCandidateGE(seek, numFields, wasCompleteMatch, stepExtra);
            if (next)
                return next;            // note must match equality to have been returned.
        }

        finishCandidates();
    }

    if (!findCandidates(seek, numFields))
        return NULL;
    return nextInputRow();
}


void CMergeJoinProcessor::resetEOF()
{
    ForEachItemIn(i, inputs)
        inputs.item(i).resetEOF();
}


void CMergeJoinProcessor::queryResetEOF()
{
    resetEOF();
}

const void * CMergeJoinProcessor::nextRow()
{
    if (conjunctionOptimizer)
        return conjunctionOptimizer->next();

    if (combineConjunctions)
    {
        if (numExternalEqualFields == 0)
        {
            if (createConjunctionOptimizer())
                return conjunctionOptimizer->next();
        }
        else
            combineConjunctions = false;            // being used inside a conjunction optimizer => don't create another..
    }
    return nextInputRow();
}

const void * CMergeJoinProcessor::nextGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    if (conjunctionOptimizer)
        return conjunctionOptimizer->nextGE(seek, numFields, wasCompleteMatch, stepExtra);

    if (combineConjunctions)
    {
        if (createConjunctionOptimizer())
            return conjunctionOptimizer->nextGE(seek, numFields, wasCompleteMatch, stepExtra);
    }
    return nextInputRowGE(seek, numFields, wasCompleteMatch, stepExtra);
}


void CMergeJoinProcessor::startRestrictedJoin(const void * equalityRow, unsigned numEqualityFields)
{
    assertex(numExternalEqualFields == 0);
    numExternalEqualFields = numEqualityFields;
    eof = false;
}

void CMergeJoinProcessor::stopRestrictedJoin()
{
    numExternalEqualFields = 0;
    if (hasCandidates())
        finishCandidates();

    //There are no more matches for this (outer) equality condition, so all active rows need to be thrown away.
    if (outputProcessor)
        outputProcessor->cleanupAllCandidates();
}

void CMergeJoinProcessor::setCandidateRow(const void * row, bool inputsMayBeEmpty, const bool * matched)
{
    candidateEqualityRow = inputAllocator->linkRow(row);
    const void * restrictionRow = (numEqualFields == numExternalEqualFields) ? NULL : candidateEqualityRow;
    outputProcessor->beforeProcessCandidates(restrictionRow, inputsMayBeEmpty, matched);
}

void CMergeJoinProcessor::connectRemotePriorityInputs()
{
    CIArrayOf<OrderedInput> orderedInputs;

    ForEachItemIn(i, inputs)
    {
        CSteppedInputLookahead & cur = inputs.item(i);
        if (!cur.hasPriority() || !cur.readsRowsRemotely())
            return;

        orderedInputs.append(*new OrderedInput(cur, i, false));
    }
    orderedInputs.sort(compareInitialInputOrder);
    associateRemoteInputs(orderedInputs, orderedInputs.ordinality());
    combineConjunctions = false;
}

//---------------------------------------------------------------------------

CAndMergeJoinProcessor::CAndMergeJoinProcessor(IHThorNWayMergeJoinArg & _arg) : CMergeJoinProcessor(_arg)
{
}

void CAndMergeJoinProcessor::beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator)
{
    CMergeJoinProcessor::beforeProcessing(_inputAllocator, _outputAllocator);
    connectRemotePriorityInputs();
    if (flags & IHThorNWayMergeJoinArg::MJFtransform)
        createEqualityJoinProcessor();
    else
        createMerger();
}

bool CAndMergeJoinProcessor::findCandidates(const void * seekValue, unsigned numSeekFields)
{
    if (eof)
        return false;

    const bool inputsMustMatchEquality = (numEqualFields == numExternalEqualFields);
    const void * equalValue;
    unsigned firstInput = 0;
    if (inputsMustMatchEquality && allInputsAreOuterInputs)
    {
        //special case - all inputs are already advanced to the correct place, so just start generating candidates
        //for nested conjunctions they may already be exausted though
        equalValue = orderedInputs.item(firstInput).next();
        if (!equalValue)
        {
            eof = true;
            return false;
        }
    }
    else
    {
        if (!seekValue)
        {
            numSeekFields = numEqualFields;
            seekValue = lowestSeekRow;
        }
        bool matchedCompletely = true;
        equalValue = orderedInputs.item(firstInput).next(seekValue, numSeekFields, matchedCompletely, unknownFrequencyTermStepExtra);
        if (!equalValue)
        {
            eof = true;
            return false;
        }

        PreservedRow savedRow(inputAllocator);
        unsigned matchCount = 0;
        clearMatches();
        if (matchedCompletely)
        {
            matched[firstInput] = true;
            matchCount++;
        }
        else
        {
            equalValue = orderedInputs.item(firstInput).consume();
            savedRow.setown(equalValue);
        }

        unsigned lastInput = firstInput;
        while (matchCount != numInputs)
        {
            unsigned nextInput = nextToMatch(lastInput);
            lastInput = nextInput;
            bool matchedCompletely = true;
            const void * nextRow = orderedInputs.item(nextInput).nextGE(equalValue, numEqualFields, matchedCompletely, unknownFrequencyTermStepExtra);
            if (!nextRow)
            {
                eof = true;
                return false;
            }

#ifdef CHECK_CONSISTENCY
            if (inputsMustMatchEquality)
            {
                if (equalCompare->docompare(nextRow, equalValue) != 0)
                    throw MakeStringException(1001, "Input to stepped join isn't sorted as expected");
            }
            else
            {
                if (equalCompare->docompare(nextRow, equalValue) < 0)
                    throw MakeStringException(1001, "Input to stepped join isn't sorted as expected");
            }
#endif

            if (!inputsMustMatchEquality)
            {
                if (!equalCompareEq->match(nextRow, equalValue))
                {
                    //value didn't match => skip all the previously matched entries.
                    for (unsigned i=0; i < numInputs; i++)
                    {
                        if (matched[i])
                        {
                            matched[i] = false;
                            orderedInputs.item(i).skip();
                            if (--matchCount == 0)
                                break;
                        }
                    }

                    if (!matchedCompletely)
                    {
                        //Need to preserve nextRow, otherwise it will be gone after we skip
                        equalValue = orderedInputs.item(nextInput).consume();
                        savedRow.setown(equalValue);
                    }
                    else
                        equalValue = nextRow;
                }
            }

            if (matchedCompletely)
            {
                matched[nextInput] = true;
                matchCount++;
            }
        }
    }

    //Set up the mergeProcessor with the appropriate inputs.  NB: inputs, not orderedInputs, and prime the initial rows to avoid extra comparisons
    //with the candidate.
    //clone one of the rows
    setCandidateRow(equalValue, false, NULL);
    return true;
}

unsigned CAndMergeJoinProcessor::nextToMatch(unsigned lastInput) const
{
    for (unsigned i=0; i < numInputs; i++)
    {
        //Don't seek on the last input again (it may have found a keyed match, but not matched the post filter)
        if ((i != lastInput) && !matched[i])
            return i;
    }
    throwUnexpected();
}

//---------------------------------------------------------------------------

CAndLeftMergeJoinProcessor::CAndLeftMergeJoinProcessor(IHThorNWayMergeJoinArg & _arg) : CMergeJoinProcessor(_arg)
{
    combineConjunctions = false;            // No advantage using this as the base of a combined conjunction
    isLeftOnly = (flags & IHThorNWayMergeJoinArg::MJFkindmask) == IHThorNWayMergeJoinArg::MJFleftonly;

    //Left only with a not stepped comparison needs to be done as a left outer at the stepping level
    if (isLeftOnly && (helper.queryNonSteppedCompare() || helper.queryGlobalCompare()))
        isLeftOnly = false;

}

void CAndLeftMergeJoinProcessor::beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator)
{
    CMergeJoinProcessor::beforeProcessing(_inputAllocator, _outputAllocator);
    createTempSeekBuffer();

    if (flags & IHThorNWayMergeJoinArg::MJFtransform)
        createEqualityJoinProcessor();
    else
        createMerger();
}

bool CAndLeftMergeJoinProcessor::findCandidates(const void * seekValue, unsigned numSeekFields)
{
    if (eof)
        return false;

    CSteppedInputLookahead & input0 = inputs.item(0);
    bool wasMatched = true;
    const void * lhs = input0.next(seekValue, numSeekFields, wasMatched, nonBufferedMatchStepExtra);
    assertex(wasMatched);
    if (!lhs)
    {
        eof = true;
        return false;
    }

    unsigned matchCount = 1;
    while (matchCount != numInputs)
    {
        bool matchedCompletely = true;      // we don't care what the next rhs value is - as long as it can't match the left
        const void * rhs = orderedInputs.item(matchCount).nextGE(lhs, numEqualFields, matchedCompletely, unknownFrequencyTermStepExtra);
        if (rhs)
        {
            int c = equalCompare->docompare(rhs, lhs);
            if (c < 0)
                throw MakeStringException(1001, "Input to stepped join isn't sorted as expected");
            if (c == 0)
            {
                assertex(matchedCompletely);
                //previously the (matchCount+1) test wasn't here, so it aborted as soon as there was any match.
                if (isLeftOnly && (matchCount+1 == numInputs))
                {
                    if (numEqualFields == numExternalEqualFields)
                    {
                        //I think this is worth doing here... 
                        //Skip input0 to a mismatch value, so the optimizer doesn't waste time reading extra equalities
                        RtlStaticRowBuilder rowBuilder(tempSeekBuffer, maxSeekRecordSize);
                        bool calculatedNextSeek = helper.createNextJoinValue(rowBuilder, lhs);
                        input0.skip();  // invalidates lhs
                        if (calculatedNextSeek)
                        {
                            bool wasMatched = true;
                            input0.nextGE(tempSeekBuffer, numEqualFields, wasMatched, nonBufferedMatchStepExtra);
                        }

                        eof = true;
                        return false;
                    }

                    //Create the next join value if that is possible
                    RtlStaticRowBuilder rowBuilder(tempSeekBuffer, maxSeekRecordSize);
                    bool calculatedNextSeek = helper.createNextJoinValue(rowBuilder, lhs);
                    input0.skip();  // invalidates lhs
                    bool wasMatched = true;
                    if (calculatedNextSeek)
                        lhs = input0.nextGE(tempSeekBuffer, numEqualFields, wasMatched, nonBufferedMatchStepExtra);
                    else
                        lhs = input0.next();

                    if (!lhs)
                    {
                        eof = true;
                        return false;
                    }
                    matchCount = 0; //incremented at tail of loop
                }
            }
            else
                break;
        }
        else
            break;
        matchCount++;
    }

    clearMatches();
    matched[0] = true;
    if (matchCount != numInputs)
    {
        //Failed to match completely => generate a match for just the left.  Skip any matched rows so far and break out.
        for (unsigned i=1; i < matchCount; i++)
            orderedInputs.item(i).skip();
        matchCount = 1;
    }
    else
    {
        for (unsigned i=1; i < numInputs; i++)
            matched[i] = true;
    }

    //LEFT ONLY will only merge 1 stream, LEFT OUTER will merge as many as match LEFT
    setCandidateRow(lhs, true, matched);
    return true;
}

bool CAndLeftMergeJoinProcessor::gatherConjunctions(ISteppedConjunctionCollector & collector)
{
    CSteppedInputLookahead & cur = inputs.item(0);
    if (!cur.gatherConjunctions(collector))
        collector.addInput(cur);
    collector.addJoin(*this);
    return true;
}

//---------------------------------------------------------------------------

void BestMatchManager::associate(unsigned input, const void * value)
{
    unsigned curIndex = 0;
    while (curIndex != numEntries)
    {
        BestMatchItem & cur = matches.item(curIndex);
        int c = compare->docompare(value, cur.value);
        if (c <= 0)
        {
            if (c == 0)
            {
                //insert at the end of the duplicates
                curIndex += cur.duplicates;
                cur.duplicates++;
            }

            //Move a record at the end of the list to the correct position, ready for updating.
            if (curIndex != numEntries)
                matches.rotateR(curIndex, numEntries);

            break;  // now go and modify record at position curIndex
        }
        curIndex += cur.duplicates;
    }

    assertex(matches.isItem(curIndex));
    BestMatchItem & inserted = matches.item(curIndex);
    inserted.duplicates = 1;
    inserted.value = value;
    inserted.input = input;
    numEntries++;
    return;

}

unsigned BestMatchManager::getValueOffset(unsigned idx) const
{
    unsigned offset = 0;
    while (idx--)
        offset += matches.item(offset).duplicates;
    return offset;
}

void BestMatchManager::init(ICompare * _compare, unsigned numInputs)
{
    compare = _compare;
    numEntries = 0;
    for (unsigned i=0; i < numInputs; i++)
        matches.append(* new BestMatchItem);
}


void BestMatchManager::kill()
{
    matches.kill();
}

unsigned BestMatchManager::getInput(unsigned whichValue, unsigned inputIndex) const
{
    return matches.item(getValueOffset(whichValue) + inputIndex).input;
}

unsigned BestMatchManager::getInput0(unsigned inputIndex) const
{
    return matches.item(inputIndex).input;
}

unsigned BestMatchManager::numInputs(unsigned whichValue) const
{
    return matches.item(getValueOffset(whichValue)).duplicates;
}

void BestMatchManager::remove(unsigned whichValue)
{
    unsigned offset = getValueOffset(whichValue);
    unsigned duplicates = matches.item(offset).duplicates;
    matches.rotateLN(offset, numEntries-1, duplicates);
    numEntries -= duplicates;
}

const void * BestMatchManager::queryValue(unsigned whichValue) const
{
    return matches.item(getValueOffset(whichValue)).value;
}

//---------------------------------------------------------------------------


CMofNMergeJoinProcessor::CMofNMergeJoinProcessor(IHThorNWayMergeJoinArg & _arg) : CMergeJoinProcessor(_arg)
{
    combineConjunctions = false;
    alive = NULL;
    candidateMask = NULL;
    minMatches = 0;
    maxMatches = 0;
    numActive = 0;
}


void CMofNMergeJoinProcessor::afterProcessing()
{
    delete [] alive;
    delete [] candidateMask;
    alive = NULL;
    candidateMask = NULL;
    matches.kill();
    CMergeJoinProcessor::afterProcessing();
}

void CMofNMergeJoinProcessor::beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator)
{
    CMergeJoinProcessor::beforeProcessing(_inputAllocator, _outputAllocator);
    if (flags & IHThorNWayMergeJoinArg::MJFtransform)
        createEqualityJoinProcessor();
    else
        createMerger();

    minMatches = helper.getMinMatches();
    maxMatches = helper.getMaxMatches();
    if (minMatches == 0)
        throw MakeStringException(99, "Need a non-zero minimum number of matches");

    alive = new bool [numInputs];
    candidateMask = new bool [numInputs];
    for (unsigned i= 0; i < numInputs; i++)
        alive[i] = true;
    numActive = numInputs;
    matches.init(equalCompare, numInputs);
}


bool CMofNMergeJoinProcessor::findCandidates(const void * originalSeekValue, unsigned numOriginalSeekFields)
{
    if (numActive < minMatches)
        return false;
    
    unsigned numFreeToMismatch = numActive - minMatches;
    const void * seekValue = originalSeekValue;
    unsigned numSeekFields = numOriginalSeekFields;
    //This should be true, because after candidates are matched their values are removed.
    assertex(matches.numInputs() <= numFreeToMismatch);             // 

    //MORE: This needs rewriting, so that mismatches are handled coorectly.  In particular, 
    while (matches.numInputs() < numActive)
    {
        unsigned nextInput = nextToMatch();
        bool matchedCompletely = true;

        // MORE: This needs rewriting, so that mismatches are handled coorectly.  In particular, the matches need to retain information about whether
        // they matched fully, since that will optimize where could be sought next.
        const void * value = inputs.item(nextInput).next(seekValue, numSeekFields, matchedCompletely, nonBufferedMatchStepExtra);
        //NOTE: matchedCompletely is currently always true.  More work is needed if not true.
        assertex(matchedCompletely);
        if (value)
        {
            if (matchedCompletely)
            {
                matched[nextInput] = true;
                matches.associate(nextInput, value);
            }
        }
        else
        {
            alive[nextInput] = false;
            numActive--;
            numFreeToMismatch--;
            if (numActive < minMatches)
                return false;
        }

        unsigned matchCount = matches.numInputs();
        if (matchCount > numFreeToMismatch)
        {
            unsigned numMatch0 = matches.numInputs0();
            if ((matchCount - numMatch0 > numFreeToMismatch) || (numMatch0 > maxMatches))
            {
                //clear seekValue, because seek value won't be valid any more after the skips - may be updated later.
                seekValue = originalSeekValue;
                numSeekFields = numOriginalSeekFields;

                //No way that the first element is going to match, so remove all inputs associated with it.
                for (unsigned i= 0; i < numMatch0; i++)
                {
                    unsigned input = matches.getInput0(i);
                    inputs.item(input).skip();
                    matched[input] = false;
                }
                matches.remove(0);
            }
            //Lowest element now provides the best seek position.
            if (matches.numInputs() > numFreeToMismatch)
            {
                seekValue = matches.queryValue(0);
                numSeekFields = numEqualFields;
            }
        }
    }

    //matches(0) contains the next match set, set a set of flags indicating which inputs to use
    unsigned numMatches = matches.numInputs0();
    for (unsigned i1=0; i1< numInputs; i1++)
        candidateMask[i1] = false;
    for (unsigned i2=0; i2 < numMatches; i2++)
        candidateMask[matches.getInput0(i2)] = true;

    setCandidateRow(matches.queryValue(0), true, candidateMask);

    //Now cleanup housekeeping, so that findCandidates() is ready to find the next block
    for (unsigned i3=0; i3 < numInputs; i3++)
        if (candidateMask[i3])
            matched[i3] = false;
    matches.remove(0);

    return true;
}


bool CMofNMergeJoinProcessor::gatherConjunctions(ISteppedConjunctionCollector & collector)
{
    //MORE: We may need to create pseudo inputs in order to process these optimially.
    return false;
}

unsigned CMofNMergeJoinProcessor::nextToMatch() const
{
    for (unsigned i= 0; i < numInputs; i++)
    {
        if (alive[i] && !matched[i])
            return i;
    }
    throwUnexpected();
}


//---------------------------------------------------------------------------

/*

  NOTES on the distances... this is far from simple once you get arbitrary trees involved.

  given a join expression right.x between left.x - a and left.x + b    i.e. up to a Before and b after
  a is maxRightBeforeLeft
  b is maxLeftBeforeRight

  We define a function D(x,y) which is the maximum value which can be deducted from row a, to provide a valid value for row b

  Given a tree of join expressions, we can calculate a distance function between any pair of inputs.
  J1(a,b,c) = D(i,i+1)=4, D(i+1,i) = 10
  J2(d, e) = D(i,i+1)=-1, D(i+1,i) = 12
  J3(J1, J2) = D(i,i+1)=0 D(i+1,i) = 5
  =>

  For each join we define
  D(i, lowest)  - maximum value to deduct from row i to obtain the lowest
  D(highest, i) - maximum value to deduct from highest to obtain row i
  by definition these must both be >= 0, for a simple input they are both 0

  A join's extend is given by
  D(highest,lowest) = max(D(highest,i)+D(i,lowest))

  We're only interested in the maximum values, which are obtained by the maximum distances between the elements.  The lowest and highest memebers
  of the group are going to be the ends.  So we use the maximum of those distances to work out D(i,low) and D(high, i), being careful to only use 
  the range if it is valid (e.g., the end must be possible to be the highest/lowest)

  D(a,b) = 4, D(b,a) = 10
  D(b,c) = 4, D(c,b) = 10
  D(a,c) = 8, D(c,a) = 20
  D(d,e) = -1, D(e,d) = 12
  D(a, lowest) = 8   D(highest, a) = 20
  D(b, lowest) = 10  D(highest, b) = 10
  D(c, lowest) = 20  D(highest, c) = 8
  D(highest, lowest) = 28

  Then assuming the left is the highest and the right is the lowest we have
  D(a,e) = DJ1(a, lowest) + DJ3(i,i+1) + DJ2(highest, e)
         = 8 + 0 + 0

  For >2 terms, you also need to take into account the size of a term given by D(highest,lowest)
*/

inline unsigned __int64 adjustRangeValue(unsigned __int64 rangeValue, __int64 delta)
{
    if ((delta >= 0) || (rangeValue > (unsigned __int64)-delta))
        return rangeValue + delta;
    return 0;
}



//---------------------------------------------------------------------------

//This class is created for each each input of a nary-join, it maintains a queue of potential records.
CNaryJoinLookaheadQueue::CNaryJoinLookaheadQueue(IEngineRowAllocator * _inputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookahead * _input, CNaryJoinLookaheadQueue * _left, const void * * _activeRowPtr) : helper(_helper), rows(_inputAllocator), unmatchedRows(_inputAllocator)
{
    equalCompareEq = helper.queryEqualCompareEq();
    nonSteppedCompareEq = helper.queryNonSteppedCompare();
    numEqualFields = helper.numEqualFields();
    stepCompare = helper.querySteppingMeta()->queryCompare();

    input.set(_input);
    activeRowPtr = _activeRowPtr;
    left = _left;
    equalityRow = NULL;
    curRow = 0;
    maxRow = 0;
    numSkipped = 0;
    done = true;
}

bool CNaryJoinLookaheadQueue::beforeProcessCandidates(const void * _equalityRow, bool needToVerifyNext)
{
    done = false;
    equalityRow = _equalityRow;
    rows.kill();
    numSkipped = 0;
    if (matchedLeft)
        matchedLeft->reset();

    // next is guaranteed to match the equality condition for AND, proximity but not for m of n/left outer...
    if (!needToVerifyNext || nextUnqueued())
    {
        consumeNextInput();
        return true;
    }
    return false;
}

void CNaryJoinLookaheadQueue::clearPending()
{
    rows.kill();
}

bool CNaryJoinLookaheadQueue::ensureNonEmpty()
{
    if (rows.ordinality())
        return true;
    if (nextUnqueued())
    {
        consumeNextInput();
        return true;
    }
    return false;
}

bool CNaryJoinLookaheadQueue::firstSelection()
{
    if (!left)
    {
        assertex(maxRow != 0);
        curRow = 0;
        *activeRowPtr = rows.item(curRow);
        return true;
    }
    
    if (!left->firstSelection())
        return false;

    return findValidSelection(0);
}


bool CNaryJoinLookaheadQueue::findValidSelection(unsigned initialRow)
{
    assertex(left);

    const unsigned max = maxRow;
    unsigned candidateRow = initialRow;
    loop
    {
        const void * leftRow = left->activeRow();
        while (candidateRow < max)
        {
            const void * rightRow = rows.item(candidateRow);
            if (!nonSteppedCompareEq || nonSteppedCompareEq->match(leftRow, rightRow))
            {
                curRow = candidateRow;
                *activeRowPtr = rightRow;
                return true;
            }
            candidateRow++;
        }
        if (!left->nextSelection())
            return false;
        candidateRow = 0;
    }
}


const void * CNaryJoinLookaheadQueue::nextUnqueued()
{
    if (equalityRow)
    {
        bool matches = true;
        const void * next = input->nextGE(equalityRow, numEqualFields, matches, nonBufferedMismatchStepExtra);
        if (next && matches && equalCompareEq->match(next, equalityRow))
            return next;
        return NULL;
    }
    else
        return input->next();
}

bool CNaryJoinLookaheadQueue::nextSelection()
{
    if (left)
        return findValidSelection(curRow+1);

    curRow++;
    if (curRow >= maxRow)
        return false;

    *activeRowPtr = rows.item(curRow);
    return true;
}


bool CNaryJoinLookaheadQueue::ensureCandidateExists(unsigned __int64 minDistance, unsigned __int64 maxDistance)
{
    loop
    {
        const void * next = rows.head();
        if (!next)
            break;
        unsigned __int64 distance = helper.extractRangeValue(next);
        if (distance >= minDistance)
        {
            assertex(distance <= maxDistance);
            return true;
        }
        rows.skip();
    }

    loop
    {
        const void * next = nextUnqueued();
        if (!next)
            return false;
        unsigned __int64 distance = helper.extractRangeValue(next);
        if (distance >= minDistance)
        {
            if (distance <= maxDistance)
            {
                consumeNextInput();
                return true;
            }
            return false;
        }
        input->skip();
    }
}


bool CNaryJoinLookaheadQueue::checkExistsGE(const void * seek, unsigned numFields)
{
    loop
    {
        const void * next = rows.head();
        if (!next)
            return false;
        if (stepCompare->docompare(next, seek, numFields) >= 0)
            return true;
        rows.skip();
    }
}


unsigned CNaryJoinLookaheadQueue::readAheadTo(unsigned __int64 maxDistance, bool canConsumeBeyondMax)
{
    const void * tail = rows.tail();
    if (tail && helper.extractRangeValue(tail) > maxDistance)
    {
        unsigned limit = rows.ordinality() - 1;
        //Already have all the records, return how many...
        while (limit > 0)
        {
            const void * prev = rows.item(limit-1);
            if (helper.extractRangeValue(prev) <= maxDistance)
                return limit;
            --limit;
        }
        return 0;
    }

    while (!done)
    {
        const void * next = nextUnqueued();
        if (!next)
        {
            done = true;
            break;
        }
        //This is a bit nasty.  We need to consume the next value to ensure that the lowest spotter always has the next valid
        //but it means we might be reading this input for too long
        if (helper.extractRangeValue(next) > maxDistance)
        {
            if (!canConsumeBeyondMax)
                break;
            consumeNextInput();
            return rows.ordinality()-1;
        }
        consumeNextInput();
    }
    return rows.ordinality();
}


void CNaryJoinLookaheadQueue::readCandidateAll()
{
    loop
    {
        const void * next = nextUnqueued();
        if (!next)
            return;

        consumeNextInput();
    }
}


void CNaryJoinLookaheadQueue::skip()
{
    if (matchedLeft && !matchedLeft->test(numSkipped))
    {
        unmatchedRows.enqueue(rows.dequeue());
    }
    else
        rows.skip();

    numSkipped++; 
}

bool CNaryJoinLookaheadQueue::flushUnmatched()
{
    while (rows.ordinality())
        skip();
    return unmatchedRows.ordinality() != 0;
}


//---------------------------------------------------------------------------

CProximityJoinProcessor::CProximityJoinProcessor(IHThorNWayMergeJoinArg & _helper) :
    CMergeJoinProcessor(_helper)
{
    maxRightBeforeLeft = 0;
    maxLeftBeforeRight = 0;
}

void CProximityJoinProcessor::beforeProcessing(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator)
{
    CMergeJoinProcessor::beforeProcessing(_inputAllocator, _outputAllocator);
    createTempSeekBuffer();

    //Have to delay creating the actual join joinProcessor because maxRightBeforeLeft() etc. can be onStart dependant.
    maxRightBeforeLeft = helper.maxRightBeforeLeft();
    maxLeftBeforeRight = helper.maxLeftBeforeRight();

    //Handle phrases using a different class i) because the general scheme doesn't quite work and ii) for efficiency
    if (flags & IHThorNWayMergeJoinArg::MJFtransform)
    {
        if ((maxRightBeforeLeft < 0 || maxLeftBeforeRight < 0))
            outputProcessor.setown(new CAnchoredRangeJoinGenerator(inputAllocator, outputAllocator, helper, inputs));
        else
            outputProcessor.setown(new CProximityRangeJoinGenerator(inputAllocator, outputAllocator, helper, inputs));
    }
    else
        createMerger();
}


bool CProximityJoinProcessor::findCandidates(const void * seekValue, unsigned numSeekFields)
{
    unsigned firstInput = 0;//searchOrder.item(0);
    bool wasCompleteMatch = true;
    if (eof || !inputs.item(firstInput).next(seekValue, numSeekFields, wasCompleteMatch, nonBufferedMatchStepExtra))
        return false;

    unsigned matchCount = 1;
    clearMatches();
    matched[firstInput] = true;

    const unsigned numJoinFields = numEqualFields + 1;
    const bool inputsMustMatchEquality = (numEqualFields == numExternalEqualFields);
    while (matchCount != numInputs)
    {
        unsigned nextInput = nextToMatch();
        unsigned baseInput = getBestToSeekFrom(nextInput);
        RtlStaticRowBuilder rowBuilder(tempSeekBuffer, maxSeekRecordSize);
        helper.adjustRangeValue(rowBuilder, inputs.item(baseInput).next(), -maxDistanceBefore(baseInput, nextInput));
        bool wasCompleteMatch = true;
        //MORE: Would it help to allow mismatches?  I would have thought so, but there was a previous comment sayimg "I don't think so because of the range calculation"
        const void * nextRow = inputs.item(nextInput).nextGE(tempSeekBuffer, numJoinFields, wasCompleteMatch, nonBufferedMatchStepExtra);
        assertex(wasCompleteMatch);
        if (!nextRow)
        {
            eof = true;
            return false;
        }

        if (inputsMustMatchEquality || equalityComponentMatches(nextRow, tempSeekBuffer))
        {
            //Now check if this new record causes other records to be too far away
            unsigned __int64 thisRangeValue = helper.extractRangeValue(nextRow);
            for (unsigned i=0; i<numInputs; i++)
            {
                if (matched[i])
                {
                    unsigned __int64 seekRangeValue = adjustRangeValue(thisRangeValue, -maxDistanceBefore(nextInput, i));
                    if (getRangeValue(i) < seekRangeValue)
                    {
                        inputs.item(i).skip();
                        matched[i] = false;
                        if (--matchCount == 0)
                            break;
                    }
                }
            }
        }
        else
        {
            for (unsigned i=0; i<numInputs; i++)
            {
                if (matched[i])
                {
                    inputs.item(i).skip();
                    matched[i] = false;
                    matchCount--;
                }
            }
        }

        matched[nextInput] = true;
        matchCount++;
    }

    setCandidateRow(inputs.item(0).next(), false, NULL);
    return true;
}


__int64 CProximityJoinProcessor::maxDistanceBefore(unsigned fixedInput, unsigned searchInput) const
{
    assertex(outputProcessor);      // sanity check to ensure this isn't called before maxXBeforeY are set up

    if (searchInput < fixedInput)
        return maxLeftBeforeRight * (fixedInput - searchInput);
    else
        return maxRightBeforeLeft * (searchInput - fixedInput);
}

unsigned CProximityJoinProcessor::nextToMatch() const
{
    for (unsigned i=0; i < numInputs; i++)
    {
        unsigned next = i;//searchOrder.item(i);
        if (!matched[next])
            return next;
    }
    throwUnexpected();
}


//Choose the input to seek from that restricts the input being sought the most.
unsigned CProximityJoinProcessor::getBestToSeekFrom(unsigned seekInput) const
{
    unsigned __int64 bestRangeValue = 0;
    unsigned best = NotFound;

    //MORE: This can be optimized!
    for (unsigned i=0; i < numInputs; i++)
    {
        if (matched[i])
        {
            //Calculate the value of the distance
            __int64 distanceBefore = maxDistanceBefore(i, seekInput);
            unsigned __int64 rangeValue = adjustRangeValue(getRangeValue(i), -distanceBefore);
            if (rangeValue >= bestRangeValue)
            {
                bestRangeValue = rangeValue;
                best = i;
            }
        }
    }
    assertex(best != NotFound);
    return best;
}



//---------------------------------------------------------------------------

//NULL passed to CSteppedInputLookahead first parameter means nextGE() must be overridden
CJoinGenerator::CJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs) : 
    helper(_helper), inputAllocator(_inputAllocator), outputAllocator(_outputAllocator)
{
    state = JSdone;
    unsigned flags = helper.getJoinFlags();
    stepCompare = helper.querySteppingMeta()->queryCompare();
    globalCompare = NULL;
    if (flags & IHThorNWayMergeJoinArg::MJFglobalcompare)
        globalCompare = helper.queryGlobalCompare();

    unsigned numInputs = _inputs.ordinality();
    rows = new const void * [numInputs];

    CNaryJoinLookaheadQueue * prev = NULL;
    ForEachItemIn(i, _inputs)
    {
        CNaryJoinLookaheadQueue * queue = new CNaryJoinLookaheadQueue(inputAllocator, helper, &_inputs.item(i), prev, rows + i);
        inputs.append(*queue);
        prev = queue;
    }
    isSpecialLeftJoin = false;
    numActiveInputs = numInputs;
    lastActiveInput = numInputs ? &inputs.tos() : NULL;

    joinKind = (flags & IHThorNWayMergeJoinArg::MJFkindmask);
    switch (joinKind)
    {
    case IHThorNWayMergeJoinArg::MJFleftonly:
    case IHThorNWayMergeJoinArg::MJFleftouter:
        if (helper.queryNonSteppedCompare() || globalCompare)
        {
            isSpecialLeftJoin = true;
            if (numInputs)
                inputs.item(0).trackUnmatched();
        }
        break;
    case IHThorNWayMergeJoinArg::MJFmofn:
        if (helper.queryNonSteppedCompare() || globalCompare)
            throw MakeStringException(99, "MOFN JOIN with non stepped condition not yet supported");
        break;
    }
}

CJoinGenerator::~CJoinGenerator()
{
    delete [] rows;
}


void CJoinGenerator::beforeProcessCandidates(const void * candidateRow, bool needToVerifyNext, const bool * matched)
{
    if (needToVerifyNext)
    {
        numActiveInputs = 0;
        CNaryJoinLookaheadQueue * prev = NULL;
        ForEachItemIn(i, inputs)
        {
            CNaryJoinLookaheadQueue & cur = inputs.item(i);
            if (cur.beforeProcessCandidates(candidateRow, needToVerifyNext))
            {
                cur.updateContext(prev, rows + numActiveInputs);
                prev = &cur;
                numActiveInputs++;
            }
        }
        lastActiveInput = prev;
    }
    else
    {
        ForEachItemIn(i, inputs)
            inputs.item(i).beforeProcessCandidates(candidateRow, needToVerifyNext);
    }
    state = JSfirst;
}

void CJoinGenerator::cleanupAllCandidates()
{
    //Remove all pending candidates - only called if outer join optimization is enabled
    //afterProcessCandidates() will already have been called.
    ForEachItemIn(i, inputs)
        inputs.item(i).clearPending();
}

void CJoinGenerator::afterProcessCandidates()
{
}

const void * CJoinGenerator::nextOutputRow()
{
    RtlDynamicRowBuilder rowBuilder(outputAllocator, false);
    loop
    {
        if (isSpecialLeftJoin)
        {
            CNaryJoinLookaheadQueue & left = inputs.item(0);
            loop
            {
                const void * unmatchedLeft = left.nextUnmatched();
                if (!unmatchedLeft)
                    break;
                rowBuilder.ensureRow();
                size32_t retSize = helper.transform(rowBuilder, 1, &unmatchedLeft);
                left.skipUnmatched();
                if (retSize)
                    return rowBuilder.finalizeRowClear(retSize);
            }
        }

        switch (state)
        {
        case JSdone:
            if (isSpecialLeftJoin)
            {
                CNaryJoinLookaheadQueue & left = inputs.item(0);
                left.readCandidateAll();
                if (left.flushUnmatched())
                    break;      // round again
            }
            return NULL;
        case JShascandidate:
            {
                state = JSnextcandidate;
                //If is left only join, and has an additional equality criteria, then ignore matches.
                //If left only, and no extra equality - or only one dataset has matches, then all matches are real left only matches
                if (isSpecialLeftJoin && (joinKind == IHThorNWayMergeJoinArg::MJFleftonly) && (numActiveInputs != 1))
                    break;

                rowBuilder.ensureRow();
                size32_t retSize = helper.transform(rowBuilder, numActiveInputs, rows);
                if (retSize)
                    return rowBuilder.finalizeRowClear(retSize);
                break;
            }
        case JSnextcandidate:
            if (nextCandidate())
                state = JShascandidate;
            else
            {
                if (state != JSdone)
                    state = JSgathercandidates;
            }
            break;
        case JSfirst:
        case JSgathercandidates:
            if (gatherNextCandidates())
                state = JShascandidate;
            else
                state = JSdone;
            break;
        default:
            throwUnexpected();
        }
    }
}


const void * CJoinGenerator::nextOutputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    //A stupid version.  We could possibly skip on the lowest value if we knew the fields were assigned from the lowest value in the input
    //which would potentially save a lot of transforms.
    //would also probably need the input to match the output.
    loop
    {
        const void * next = nextOutputRow();
        if (!next || stepCompare->docompare(next, seek, numFields) >= 0)
            return next;
        outputAllocator->releaseRow(next);
    }
}


bool CJoinGenerator::firstSelection()
{
    if (lastActiveInput->firstSelection())
    {
        if (globalCompare && !globalCompare->match(numActiveInputs, rows))
            return nextSelection();
        if (isSpecialLeftJoin)
            inputs.item(0).noteMatched();
        return true;
    }
    return false;
}


bool CJoinGenerator::nextSelection()
{
    while (lastActiveInput->nextSelection())
    {
        if (!globalCompare || globalCompare->match(numActiveInputs, rows))
        {
            if (isSpecialLeftJoin)
                inputs.item(0).noteMatched();
            return true;
        }
    }
    return false;
}


//---------------------------------------------------------------------------

CEqualityJoinGenerator::CEqualityJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs) : 
    CJoinGenerator(_inputAllocator, _outputAllocator, _helper, _inputs)
{
    lowestInput = NULL;
}

void CEqualityJoinGenerator::afterProcessCandidates()
{
    lowestInput = NULL;
    CJoinGenerator::afterProcessCandidates();
}

bool CEqualityJoinGenerator::nextCandidate()
{
    if (nextSelection())
        return true;

    selectNextLowestInput();
    return false;
}

/*

  o Walk the input which is guaranteed to be the lowest
  o Once that is done throw away that record, and choose the next.

  */
bool CEqualityJoinGenerator::doGatherNextCandidates()
{
    ForEachItemIn(iInput, inputs)
    {
        CNaryJoinLookaheadQueue & curInput = inputs.item(iInput);
        if (&curInput != lowestInput)
            curInput.setCandidateAll();
        else
            curInput.setCandidateLowest();
    }

    return firstSelection();
}


bool CEqualityJoinGenerator::gatherNextCandidates()
{
    if (state == JSfirst)
    {
        prefetchAllCandidates();
        selectLowestInput();
    }
    else if (lowestInput->empty())
        return false;

    loop
    {
        if (doGatherNextCandidates())
            return true;

        if (!selectNextLowestInput())
            return false;
    }
}


void CEqualityJoinGenerator::prefetchAllCandidates()
{
    //could be done in parallel, but 
    ForEachItemIn(i, inputs)
    {
        CNaryJoinLookaheadQueue & curInput = inputs.item(i);
        curInput.readCandidateAll();
    }
}


void CEqualityJoinGenerator::selectLowestInput()
{
    ForEachItemIn(i, inputs)
    {
        CNaryJoinLookaheadQueue & curInput = inputs.item(i);
        if (!curInput.empty())
        {
            lowestInput = &curInput;
            return;
        }
    }
    throwUnexpected();
}


bool CEqualityJoinGenerator::selectNextLowestInput()
{
    lowestInput->skip();
    if (lowestInput->empty())
    {
        state = JSdone;
        return false;
    }
    return true;
}


//---------------------------------------------------------------------------

CSortedEqualityJoinGenerator::CSortedEqualityJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs) : 
    CEqualityJoinGenerator(_inputAllocator, _outputAllocator, _helper, _inputs), lowestSpotter(inputs)
{
    lowestSpotter.init(inputAllocator, helper.queryMergeCompare(), helper.querySteppingMeta()->queryCompare());
    lowestSpotter.initInputs();
}

CSortedEqualityJoinGenerator::~CSortedEqualityJoinGenerator()
{
    lowestSpotter.cleanup();
}

void CSortedEqualityJoinGenerator::afterProcessCandidates()
{
    lowestSpotter.reset();
    CEqualityJoinGenerator::afterProcessCandidates();
}

void CSortedEqualityJoinGenerator::selectLowestInput()
{
    unsigned iLowest = lowestSpotter.queryNextInput();
    assertex(iLowest != NotFound);
    lowestInput = &inputs.item(iLowest);
}


bool CSortedEqualityJoinGenerator::selectNextLowestInput()
{
    lowestSpotter.skipRow();
    if (lowestInput->empty())
    {
        state = JSdone;
        return false;
    }

    CSortedEqualityJoinGenerator::selectLowestInput();
    return true;
}

//---------------------------------------------------------------------------

CRangeJoinGenerator::CRangeJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs) : 
    CJoinGenerator(_inputAllocator, _outputAllocator, _helper, _inputs)
{
    maxRightBeforeLeft = helper.maxRightBeforeLeft();
    maxLeftBeforeRight = helper.maxLeftBeforeRight();
}

//---------------------------------------------------------------------------

CAnchoredRangeJoinGenerator::CAnchoredRangeJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs) : 
    CRangeJoinGenerator(_inputAllocator, _outputAllocator, _helper, _inputs)
{
    iLowest = maxRightBeforeLeft < 0 ? 0 : inputs.ordinality()-1;
    lowestInput = &inputs.item(iLowest);
}

bool CAnchoredRangeJoinGenerator::nextCandidate()
{
    if (nextSelection())
        return true;

    lowestInput->skip();
    return false;
}

/*

  o Walk the input which is guaranteed to be the lowest
  o Once that is done throw away that record, and choose the next.

  */

bool CAnchoredRangeJoinGenerator::doGatherNextCandidates()
{
    const void * lowestRow = lowestInput->next();
    if (!lowestRow)
        return false;

    unsigned __int64 lowestDistance = helper.extractRangeValue(lowestRow);

    ForEachItemIn(iInput, inputs)
    {
        CNaryJoinLookaheadQueue & curInput = inputs.item(iInput);
        if (iInput != iLowest)
        {
            __int64 maxLowestBeforeCur = maxDistanceAfterLowest(iInput);
            assertex(maxLowestBeforeCur > 0);
            unsigned __int64 maxDistance = lowestDistance + maxLowestBeforeCur;     
            if (!curInput.setCandidateRange(maxDistance, false))
                return false;
        }
    }

    lowestInput->setCandidateLowest();
    return firstSelection();
}


const void * CAnchoredRangeJoinGenerator::nextOutputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    //Note: Skip any lower values that are less than seek value, but don't read any more
    if (!lowestInput->checkExistsGE(seek, numFields))
        return NULL;
    return CRangeJoinGenerator::nextOutputRowGE(seek, numFields, wasCompleteMatch, stepExtra);
}


bool CAnchoredRangeJoinGenerator::nextMatchesAnyConsumed()
{
    const void * lowestRow = lowestInput->next();
    bool consumePending = false;
    if (!lowestRow)
    {
        lowestRow = lowestInput->nextUnqueued();
        if (!lowestRow)
            return false;
        consumePending = true;
    }

    //Throw any non-matching rows away, and return true if there are no other rows left.
    unsigned __int64 lowestDistance = helper.extractRangeValue(lowestRow);
    ForEachItemIn(iInput, inputs)
    {
        CNaryJoinLookaheadQueue & curInput = inputs.item(iInput);
        if (iInput != iLowest)
        {
            //note: maxRightBeforeLeft is -minRightAfterLeft
            __int64 minCurAfterLowest;
            if (iInput < iLowest)
                minCurAfterLowest = (-maxLeftBeforeRight) * (iLowest - iInput);
            else
                minCurAfterLowest = (-maxRightBeforeLeft) * (iInput - iLowest);
            assertex(minCurAfterLowest >= 0);

            if (!curInput.ensureCandidateExists(lowestDistance+minCurAfterLowest, lowestDistance + maxDistanceAfterLowest(iInput)))
                return false;
        }
    }

    //A potential match, so consume the potential start word and try again
    if (consumePending)
        lowestInput->consumeNextInput();
    return true;
}


bool CAnchoredRangeJoinGenerator::gatherNextCandidates()
{
    loop
    {
        if (!nextMatchesAnyConsumed())
            return false;

        if (doGatherNextCandidates())
            return true;

        lowestInput->skip();
    }
}

//---------------------------------------------------------------------------

CProximityRangeJoinGenerator::CProximityRangeJoinGenerator(IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator, IHThorNWayMergeJoinArg & _helper, CSteppedInputLookaheadArray & _inputs) : 
    CRangeJoinGenerator(_inputAllocator, _outputAllocator, _helper, _inputs), lowestSpotter(inputs)
{
    lowestSpotter.init(inputAllocator, helper.queryMergeCompare(), helper.querySteppingMeta()->queryCompare());
    lowestSpotter.initInputs();
}

CProximityRangeJoinGenerator::~CProximityRangeJoinGenerator()
{
    lowestSpotter.cleanup();
}


void CProximityRangeJoinGenerator::afterProcessCandidates()
{
    lowestSpotter.reset();
    CRangeJoinGenerator::afterProcessCandidates();
}

bool CProximityRangeJoinGenerator::nextCandidate()
{
    if (nextSelection())
        return true;

    if (!lowestSpotter.skipNextLowest())
        state = JSdone;
    return false;
}


/*

  First version.....

  o Walk the input datasets in the order lowest first.
  o Perform the cross product of that record with all others that could possibly match.
  o Once that is done throw away that record, and choose the next.
  o Abort as soon as any of the inputs contains no records within potential range.

  */
bool CProximityRangeJoinGenerator::gatherNextCandidates(unsigned iLowest)
{
    CNaryJoinLookaheadQueue & lowestInput = inputs.item(iLowest);
    const void * lowestRow = lowestInput.next();
    unsigned __int64 lowestDistance = helper.extractRangeValue(lowestRow);

    ForEachItemIn(iInput, inputs)
    {
        CNaryJoinLookaheadQueue & curInput = inputs.item(iInput);
        if (iInput != iLowest)
        {
            __int64 maxLowestBeforeCur;
            if (iInput < iLowest)
                maxLowestBeforeCur = maxRightBeforeLeft * (iLowest - iInput);
            else
                maxLowestBeforeCur = maxLeftBeforeRight * (iInput - iLowest);

            assertex(maxLowestBeforeCur >= 0);          // should have created an anchored variant if not true

            // maxLowestBeforeCur = maxCurAfterLowest
            unsigned __int64 maxDistance = lowestDistance + maxLowestBeforeCur;     
            if (!curInput.setCandidateRange(maxDistance, true))
                return false;
        }
        else
            curInput.setCandidateLowest();
    }

    return firstSelection();
}


bool CProximityRangeJoinGenerator::gatherNextCandidates()
{
    loop
    {
        unsigned iLowest = lowestSpotter.queryNextInput();
        assertex(iLowest != NotFound);

        if (gatherNextCandidates(iLowest))
            return true;

        //It would be really nice to break out early if there were no more potential matches, but even if there
        //is only one matching stream we need to keep walking the consumed records, because the later records
        //may pull in the relevant related records, and we can't sensibly put back the consumed records
        if (!lowestSpotter.skipNextLowest())
        {
            //No more records within this document => can't ever match
            state = JSdone;
            return false;
        }
    }
}

