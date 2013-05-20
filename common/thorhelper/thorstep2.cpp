/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "thorstep2.ipp"

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------

const static SmartStepExtra knownLowestFrequencyTermStepExtra(SSEFreadAhead, NULL);
const static SmartStepExtra knownOtherFrequencyTermStepExtra(SSEFreturnMismatches, NULL);
const static SmartStepExtra presumedLowestFrequencyTermStepExtra(SSEFreturnMismatches|SSEFreadAhead, NULL);
const static SmartStepExtra presumedLowestFrequencyTermReseekStepExtra(SSEFreadAhead, NULL);
const static SmartStepExtra presumedOtherFrequencyTermStepExtra(SSEFreturnMismatches, NULL);
const static SmartStepExtra unknownFrequencyTermStepExtra(SSEFreturnMismatches, NULL);

//#define TRACE_JOIN_OPTIMIZATION

/*

It is REALLY important that we pick the best input for skipping on, and there are a few issues:

a) We don't want occasional jumps in a more signficant join componet - e.g., part / source from (part, source, doc)  
   to pick an otherwise infrequent term
b) Ordering within the data (e.g., by state) can mean that one input may occasionally skip a long way
c) It is really important to know what the best input is - because we want to post filter that input as heavily as possible
d) It hardly matters which other input is chosen in combination with the best input, because they are all likely to end up 
   (i) not matching unless they are highly correlated
   (ii) not skipping anything in the least frequent index.
e) Spending a little bit of time working out the best input will pay dividends

So the latest approach is as follows: 

1) Use the median skip distance from the last 3 values - that should prevent insignficant join components skewing the results

2) Make sure all inputs have been read at least 3 times before we allow the best input to be post filtered.

3) Iterate through each of the other less significant inputs in turn, so that if the best input changes, or we got it wrong, or an input
   occasionally skips a long way (e.g. state) it will get corrected/used. 

  */
int compareInitialInputOrder(CInterface * * _left, CInterface * * _right)
{
    OrderedInput * left = static_cast<OrderedInput *>(*_left);
    OrderedInput * right = static_cast<OrderedInput *>(*_right);

    if (left->hasPriority())
    {
        if (!right->hasPriority())
            return -1;

        if (left->getPriority() < right->getPriority())
            return -1;
        if (left->getPriority() > right->getPriority())
            return +1;
        return (int)(left->originalIndex - right->originalIndex);
    }
    else
    {
        if (right->hasPriority())
            return +1;
    }

    if (left->canOptimizeOrder())
    {
        if (!right->canOptimizeOrder())
            return -1;
    }
    else
    {
        if (right->canOptimizeOrder())
            return +1;
    }

    return (int)(left->originalIndex - right->originalIndex);
}



OrderedInput::OrderedInput(CSteppedInputLookahead & _input, unsigned _originalIndex, bool hasDistance) : originalIndex(_originalIndex)
{
    input.set(&_input);
    matched = false;
    skipCost = 1;                   // This should be enhanced...
    numSamples = 0;
    nextDistanceIndex = 0;
    distanceCalculator = NULL;
    stepFlags = input->getStepFlags();
    priority = input->getPriority();

    if (hasDistance)
        distanceCalculator = _input.queryDistance();

    optimizeOrder = !hasPriority() && (distanceCalculator && input->canSmartStep());
    // make it low initially to ensure that all inputs are sought to get some kind of idea of how good they are
    medianDistance.set(0,0);
}

void OrderedInput::updateSequentialDistance(const void * seek, const void * next, unsigned numFields, bool seekToSameLocation, bool nowMatched)
{
    //Note: With large numbers of matches in all documents, the ordered inputs will remain stable,
    //because the first input will have a better distance than the exact matches.
    assertex(distanceCalculator);
    SkipDistance thisDistance;
    thisDistance.field = distanceCalculator->getDistance(thisDistance.distance, seek, next, numFields);

#ifdef IGNORE_SEEK_TO_SELF
    //If the previous seek was a nextGE() that returned a row which didn't match the criteia, that then matched
    //a different index, and we then get a subsequent call which does match property on the same record
    //then we shouldn't include this is the skipping distance.
    if (thisDistance.field == DISTANCE_EXACT_MATCH)
    {
        assertex(nowMatched);
        if (nowMatched && seekToSameLocation)
            return;
    }
#endif

    distances[nextDistanceIndex].set(thisDistance);
    if (nextDistanceIndex == MaxDistanceSamples-1)
        nextDistanceIndex = 0;
    else
        nextDistanceIndex++;

    if (numSamples < MaxDistanceSamples)
        numSamples++;

    if (numSamples < MaxDistanceSamples)
    {
        //choose the last to start with - fairly arbitrary.
        medianDistance.set(thisDistance);
    }
    else
    {
        int c01 = distances[0].compare(distances[1]);
        unsigned median;
        if (c01 == 0)
        {
            //Same => the median must be the same as either of them.
            median = 0;
        }
        else
        {
            int c02 = distances[0].compare(distances[2]);
            if (c01 < 0)
            {
                if (c02 >= 0)
                {
                    //c <= a < b
                    median = 0;
                }
                else
                {
                    //a < b, a < c  => smallest of b,c
                    int c12 = distances[1].compare(distances[2]);
                    median = c12 <= 0 ? 1 : 2;
                }
            }
            else
            {
                if (c02 <= 0)
                {
                    // c >= a > b
                    median = 0;
                }
                else
                {
                    // a > b, a > c  => median is largest if b,c
                    int c12 = distances[1].compare(distances[2]);
                    median = c12 >= 0 ? 1 : 2;
                }
            }
        }
#ifdef TRACE_JOIN_OPTIMIZATION
        if (medianDistance.compare(distances[median]) != 0)
            DBGLOG("Median for input %d changed from %d:%" I64F "d to %d:%" I64F "d", 
                    originalIndex, medianDistance.field, medianDistance.distance, 
                                   distances[median].field, distances[median].distance);
#endif
        medianDistance.set(distances[median]);
    }
}


//------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*

  We walk the tree commoning up all conjunctions that share at least one equal field.

  numEqualFields = intersection of all conjunctions that are being combined
  numMergeFields = merge fields of root conjunction
  NB: numEqualityFields <= numMergeFields;

  //create INewSteppedInput wrappers for non-conjunction inputs
  //the XConjunctionOptimizers implement the stepped input otherwise.


  */

CSteppedConjunctionOptimizer::CSteppedConjunctionOptimizer(IEngineRowAllocator * _inputAllocator, IHThorNWayMergeJoinArg & _arg, ISteppedInput * _root) : helper(_arg), inputAllocator(_inputAllocator)
{
    mergeSteppingMeta = helper.querySteppingMeta();
    assertex(mergeSteppingMeta);
    stepCompare = mergeSteppingMeta->queryCompare();
    equalCompare = helper.queryEqualCompare();
    numEqualFields = helper.numEqualFields();
    numInputs = 0;
    numOptimizeInputs = 0;
    numPriorityInputs = 0;
    equalityRow = NULL;
    prevEqualityRow = NULL;
    prevPartitionRow = NULL;
    partitionCompare = NULL;
    if (helper.getJoinFlags() & IHThorNWayMergeJoinArg::MJFhaspartition)
        partitionCompare = helper.queryPartitionCompareEq();
    rootActivity = _root;
    lowestSeekRow = NULL;
    seekMatchTicker = 0;
    inputsHaveMedian = false;       // only relevant if (numOptimizeInputs != 0)
    inputHasPostfilter = false;
    inputIsDistributed = false;
    eof = false;
    maxOptimizeInput = 0;
}

CSteppedConjunctionOptimizer::~CSteppedConjunctionOptimizer()
{
    afterProcessing();
}


void CSteppedConjunctionOptimizer::addInput(CSteppedInputLookahead & _input)
{
    inputs.append(OLINK(_input));
    if (_input.hasPostFilter())
        inputHasPostfilter = true;
    if (_input.hasPriority())
        numPriorityInputs++;
    if (_input.readsRowsRemotely())
        inputIsDistributed = true;
    numInputs++;
}

void CSteppedConjunctionOptimizer::addPseudoInput(CSteppedInputLookahead & _input)
{
    pseudoInputs.append(OLINK(_input));
}

void CSteppedConjunctionOptimizer::addJoin(ISteppedJoin & _join)
{
    unsigned thisEqualFields = _join.getNumEqualFields();
    assertex(thisEqualFields);
    if (numEqualFields > thisEqualFields)
        numEqualFields = thisEqualFields;
    joins.append(OLINK(_join));
}

void CSteppedConjunctionOptimizer::afterProcessing()
{
    if (hasCandidates())
        finishCandidates();

    inputs.kill();
    orderedInputs.kill();
    if (prevEqualityRow)
    {
        inputAllocator->releaseRow(prevEqualityRow);
        prevEqualityRow = NULL;
    }
    if (lowestSeekRow)
    {
        inputAllocator->releaseRow(lowestSeekRow);
        lowestSeekRow = NULL;
    }
    if (prevPartitionRow)
    {
        inputAllocator->releaseRow(prevPartitionRow);
        prevPartitionRow = NULL;
    }

}

void associateRemoteInputs(CIArrayOf<OrderedInput> & orderedInputs, unsigned numPriorityInputs)
{
    //If we know for sure the primary input, then tag it as worth reading ahead - otherwise it will be dynamically set later.
    if (numPriorityInputs > 0)
    {
        OrderedInput & input0 = orderedInputs.item(0);
        //Only read ahead etc. if this is a real index - not if it is a join.
        if (!input0.isJoin())
        {
            input0.setReadAhead(true);
            input0.setAlwaysReadExact();
        }
    }

    //Work out the last input of known priority which is read remotely.
    unsigned maxPriorityRemote = numPriorityInputs;
    while ((maxPriorityRemote >= 2) && !orderedInputs.item(maxPriorityRemote-1).readsRowsRemotely())
        maxPriorityRemote--;

    //If the second ordered input is known to be read remotely, then we want to send multiple seek requests at the same time.
    //MORE: Maybe we should consider doing this to all other inputs if only one priority input is known.
    if (maxPriorityRemote >= 2)
    {
        for (unsigned i=1; i < maxPriorityRemote; i++)
        {
            IMultipleStepSeekInfo * seekInfo = orderedInputs.item(i-1).createMutipleReadWrapper();
            orderedInputs.item(i).createMultipleSeekWrapper(seekInfo);
        }
    }
}


void CSteppedConjunctionOptimizer::beforeProcessing()
{
    //NB: This function is only called once, after we have decided it is worth processing.
    assertex(!eof);     // just check it isn't called more than once
    assertex(numInputs);

    bool hasDistance = (helper.getJoinFlags() & IHThorNWayMergeJoinArg::MJFhasdistance) != 0;
    for (unsigned i3 = 0; i3 < numInputs; i3++)
    {
        OrderedInput & next = *new OrderedInput(inputs.item(i3), i3, hasDistance);
        orderedInputs.append(next);
        if (next.canOptimizeOrder())
            numOptimizeInputs++;
    }
    //Sort so that inputs are ordered (priority-inputs, optimizable, non-optimizable)
    orderedInputs.sort(compareInitialInputOrder);

    //If only a single re-orderable input, treat it as unorderable.
    if (numOptimizeInputs == 1)
    {
        assertex(orderedInputs.item(numPriorityInputs).canOptimizeOrder());
        orderedInputs.item(numPriorityInputs).stopOptimizeOrder();
        numOptimizeInputs = 0;
    }

    maxOptimizeInput = numPriorityInputs + numOptimizeInputs;

    associateRemoteInputs(orderedInputs, numPriorityInputs);
    
    //MORE: If some inputs have known priority, and other remote inputs don't, then we could consider
    //      connecting the unknown inputs to the last known inputs.
    ForEachItemIn(i4, joins)
        joins.item(i4).markRestrictedJoin(numEqualFields);

    assertex(helper.getJoinFlags() & IHThorNWayMergeJoinArg::MJFhasclearlow);       // Don't support (very) old workunits that don't define this..
    if (helper.getJoinFlags() & IHThorNWayMergeJoinArg::MJFhasclearlow)
    {
        RtlDynamicRowBuilder rowBuilder(inputAllocator);
        size32_t size = helper.createLowInputRow(rowBuilder);
        lowestSeekRow = rowBuilder.finalizeRowClear(size);
    }
}


void CSteppedConjunctionOptimizer::finishCandidates()
{
    rootActivity->resetEOF();
    ForEachItemIn(i1, inputs)
        inputs.item(i1).setRestriction(NULL, 0);
    ForEachItemIn(i2, joins)
        joins.item(i2).stopRestrictedJoin();
    ForEachItemIn(i3, pseudoInputs)
        pseudoInputs.item(i3).clearPending();
    if (prevEqualityRow)
        inputAllocator->releaseRow(prevEqualityRow);
    prevEqualityRow = equalityRow;
    equalityRow = NULL;
}


const void * CSteppedConjunctionOptimizer::next()
{
    if (!hasCandidates() && !findCandidates(NULL, 0))
        return NULL;

    loop
    {
        const void * next = rootActivity->nextInputRow();
        if (next)
            return next;
        finishCandidates();

        if (!findCandidates(NULL, 0))
            return NULL;
    }
}

const void * CSteppedConjunctionOptimizer::nextGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    //First check the next row from the candidates, it may be ok.
    if (hasCandidates())
    {
        if (numFields <= numEqualFields)
        {
            if (stepCompare->docompare(seek, equalityRow, numFields) == 0)
                return next();
        }
        else
        {
            //This would be pretty unusual. - proximity(or(x and y), z) might trigger it.
            if (stepCompare->docompare(seek, equalityRow, numEqualFields) == 0)
            {
                const void * nextRow = rootActivity->nextInputRowGE(seek, numFields, wasCompleteMatch, stepExtra);
                if (nextRow)
                    return nextRow;
            }
        }
        finishCandidates();
    }

    if (!findCandidates(seek, numFields))
        return NULL;
    //MORE: If isCompleteMatch is provided, and seek row doesn't match returned, then shouldn't do complex join processing. 
    return next();
}


/*
I considered using a (doubly) linked list for storing the ordered inputs as an alternative to an expanding array,
but decided the array was simplest, and probably more efficient.
A linked list would require 6 pointer modifications regardless of the number of places moved by the input, the
expanding array requires (#places moved +1) pointers moved, which is likely to be smaller, and traversing the array is more
efficient.
  */

void CSteppedConjunctionOptimizer::getInputOrderText(StringBuffer & s)
{
    ForEachItemIn(i, orderedInputs)
    {
        if (i) s.append(" ");
        s.append(orderedInputs.item(i).originalIndex);
    }
}

void CSteppedConjunctionOptimizer::updateOrder(unsigned whichInput)
{
    //Change the position of this input in the ordering list, based on the distance skipped.
    OrderedInput & input = orderedInputs.item(whichInput);
    if ((whichInput > numPriorityInputs) && input.skipsFasterThan(orderedInputs.item(whichInput-1)))
    {
        unsigned prev = whichInput-1;
        while ((prev > numPriorityInputs) && input.skipsFasterThan(orderedInputs.item(prev-1)))
            prev--;

        //If this is probably now the lowest frequmcy input, enable read ahead for that input
        if ((prev == 0) && inputsHaveMedian)
        {
            orderedInputs.item(0).setReadAhead(false);
            input.setReadAhead(true);
        }

        orderedInputs.rotateR(prev, whichInput);

#ifdef TRACE_JOIN_OPTIMIZATION
        StringBuffer ordered;
        getInputOrderText(ordered);
        DBGLOG("Input %d promoted from %d to %d [%s]", input.originalIndex, whichInput, prev, ordered.str());
#endif
    }
    else if ((whichInput+1 < maxOptimizeInput) && orderedInputs.item(whichInput+1).skipsFasterThan(input))
    {
        unsigned next = whichInput+1;
        while ((next+1 < maxOptimizeInput) && orderedInputs.item(next+1).skipsFasterThan(input))
            next++;

        //If this is no longer probably the lowest frequmcy input, enable read ahead for other input
        if ((whichInput == 0) && inputsHaveMedian)
        {
            orderedInputs.item(1).setReadAhead(true);
            input.setReadAhead(false);
        }

        orderedInputs.rotateL(whichInput, next);
#ifdef TRACE_JOIN_OPTIMIZATION
        StringBuffer ordered;
        getInputOrderText(ordered);
        DBGLOG("Input %d demoted from %d to %d [%s]", input.originalIndex, whichInput, next, ordered.str());
#endif
    }
}

//This is the key function, once we have a candidate match we are very likely to generate some records.
bool CSteppedConjunctionOptimizer::findCandidates(const void * seekValue, unsigned numSeekFields)
{
    if (eof)
        return false;

    for (unsigned i=0; i < numInputs; i++)
        orderedInputs.item(i).matched = false;

    const void * equalValue;
    if (!seekValue)
    {
        //Always use next() with some buffer if possible - so that the early termination without post filtering
        //can be done (&matchedCompletely)
        numSeekFields = numEqualFields;
        if (prevEqualityRow)
            equalValue = prevEqualityRow;
        else
            equalValue = lowestSeekRow;
    }
    else
        equalValue = seekValue;

    PreservedRow savedRow(inputAllocator);
    unsigned matchCount = 0;
    unsigned inputProvidingSeek = (unsigned)NotFound;
    while (matchCount != numInputs)
    {
        unsigned nextInput = nextToMatch();
        OrderedInput & curInput = orderedInputs.item(nextInput);
        bool canOptimizeInputOrder = curInput.canOptimizeOrder();
        const SmartStepExtra * extra;
        if (!canOptimizeInputOrder)
            extra = (nextInput == 0) ? &knownLowestFrequencyTermStepExtra : &knownOtherFrequencyTermStepExtra;
        else if (inputsHaveMedian)
            extra = (nextInput == 0) ? &presumedLowestFrequencyTermStepExtra : &presumedOtherFrequencyTermStepExtra;
        else
            extra = &unknownFrequencyTermStepExtra;

        unsigned curOriginalInput = curInput.originalIndex;
        bool isReSeek = (inputProvidingSeek == curOriginalInput);
        bool matchedCompletely = true;
        const void * nextRow = curInput.next(equalValue, numSeekFields, isReSeek, matchedCompletely, *extra);

        //If we're seeking the most promising row, and it returned a mismatch, and even after seeking it is the best term for seeking,
        //then force the input to return a real result.  (Since this sign. reduces the number of lookups in the second input)
        if (!matchedCompletely && (nextInput == 0) && inputsHaveMedian && nextRow)
        {
            if (curInput.skipsFasterThan(orderedInputs.item(1)))
            {
                matchedCompletely = true;
                nextRow = curInput.next(equalValue, numSeekFields, true, matchedCompletely, presumedLowestFrequencyTermReseekStepExtra);
                assertex(matchedCompletely);
            }
        }

        if (!nextRow)
        {
            eof = true;
            return false;
        }

        //Allow a partition option to indicate when the data is likely to change distribution
        //and for it to be worth us re-calculating the medians for all inputs
        if (partitionCompare && (numOptimizeInputs != 0) && (nextInput == 0))
        {
            if (prevPartitionRow)
            {
                if (!partitionCompare->match(nextRow, prevPartitionRow))
                {
                    if (numPriorityInputs == 0)
                        orderedInputs.item(0).setReadAhead(false);
                    void * linked = inputAllocator->linkRow(nextRow);
                    inputAllocator->releaseRow(prevPartitionRow);
                    prevPartitionRow = linked;
                    inputsHaveMedian = false;
                    seekMatchTicker = 0;
                }
            }
            else
                prevPartitionRow = inputAllocator->linkRow(nextRow);
        }

        int c = 0;
        if (equalValue)
        {
            c = stepCompare->docompare(nextRow, equalValue, numSeekFields);
            if (canOptimizeInputOrder)
            {
                if (inputsHaveMedian)
                {
                    //Only increment the seek counter if not the first input - since that is always picked if
                    //not matched,
                    if (nextInput != 0)
                        seekMatchTicker++;
                }
                else
                {
                    seekMatchTicker++;
                    if (!inputsHaveMedian && (seekMatchTicker == numOptimizeInputs*MaxDistanceSamples))
                    {
                        inputsHaveMedian = true;
                        if (numPriorityInputs == 0)
                            orderedInputs.item(0).setReadAhead(true);
                    }
                }
            }
        }

        assertex(c >= 0);
        if (c > 0)
        {
            if (matchCount)
            {
                //value didn't match => skip all the previously matched entries.
                for (unsigned i=0; i < numInputs; i++)
                {
                    OrderedInput & cur = orderedInputs.item(i);
                    if ((i != nextInput) && cur.matched)
                    {
                        cur.skip();
                        if (--matchCount == 0)
                            break;
                    }
                }
            }

            if (!matchedCompletely)
            {
                //Need to preserve nextRow, otherwise it will be gone after we skip
                nextRow = curInput.consumeAndSkip();
                savedRow.setown(nextRow);
            }
            else
            {
                matchCount++;
            }

            inputProvidingSeek = curOriginalInput;
        }
        else
        {
            assertex(matchedCompletely);        // should only be false if the stepped conditions fail to match
            matchCount++;
        }

        equalValue = nextRow;                   // update equalRow, partly because we shouldn't assume that seekValue can be linked

        if (canOptimizeInputOrder)
            updateOrder(nextInput);
        numSeekFields = numEqualFields;
    }

    //Set up the merger with the appropriate inputs.  NB: inputs, not orderedInputs, and prime the initial rows to avoid extra comparisons
    //with the candidate.
    //clone one of the rows
    equalityRow = inputAllocator->linkRow(equalValue);
    ForEachItemIn(i1, inputs)
        inputs.item(i1).setRestriction(equalityRow, numEqualFields);
    ForEachItemIn(i2, joins)
        joins.item(i2).startRestrictedJoin(equalityRow, numEqualFields);
    return true;
}

unsigned CSteppedConjunctionOptimizer::nextToMatch() const
{
    //A heuristic for picking the next input to pull.
    //If each of the distance-capable inputs has been matched 3 times then
    //  pick the 1st ordered unmatched input
    //else
    //  pick the first input that hasn't been read enough times yet.
    if (numOptimizeInputs != 0)
    {
        if (!inputsHaveMedian)
        {
            unsigned requiredSamples = seekMatchTicker / numOptimizeInputs;
            //First try and find an unmatched optimizable input
            for (unsigned i=0; i < numInputs; i++)
            {
                OrderedInput & cur = orderedInputs.item(i);
                if (!cur.matched && cur.canOptimizeOrder())
                {
                    if (cur.getNumSamples() == requiredSamples)
                        return i;
                }
            }

            //fall back to trying an input that can't be optimized
        }
    }

    for (unsigned i=0; i < numInputs; i++)
    {
        if (!orderedInputs.item(i).matched)
            return i;
    }
    throwUnexpected();
}


bool CSteppedConjunctionOptimizer::worthCombining() 
{ 
    unsigned flags = helper.getJoinFlags();
    if (flags & MJFneveroptimize)
        return false;
    if (flags & MJFalwaysoptimize)
        return true;
    return joins.ordinality() > 1 || 
           inputs.ordinality() > 2 ||
           inputHasPostfilter ||
           (numPriorityInputs > 0) || 
           inputIsDistributed; 
}



/*

  How do you tell which is the best?
  a) cost = total # if skipping index reads.  (possibly different values for within block, and random)
  b) benefit = total # of docs skipped (between requested and received records)
  c) would need some kind of running average.
  
  The following is an outline of the approach:

  *) The root conjunction walks its children to get a list of all conjunction inputs, and a separate list of conjunctions.
     [Note, LEFT ONLY and MofN won't add all their inputs.  MofN may need pseudo inputs]
  *) The criteria for joining is the intersection of the conjunction equality conditions. (i.e. lowest number of equality fields)
  *) All these inputs are skipped to the appropriate places, purely as a pre-execution optimization.
     In particular left only and the complications of the proximity are not addressed.
  *) The non-root conjunctions would be marked to indicate that they didn't need to do this.
  *) A simple AND with non-conjunction inputs would similarly not do it.  (Although it may want to because of the order optimization).
  *) Once a match is found, a restriction is set on all raw inputs on the range of values peek() can return.
  *) All joins are reset (if an eof flag/merger or something needs reseting), and tagged to indicate the number of equality fields being filtered
     externally.
  *) find/nextCandidates() is executed as expected on the input rows.  When it fails, it bails out early if the outer restriction matches this.
  *) When next() fails it.
     a) clears all the restrictions.
     b) calculates the next stepping record.
     c) does a seekGE() on that new steeping record
     d) If can't calulate, do next() on best input until a different row is obtained + use that to seek.

  *) You could also do this initial conjunction processing for the proximity as well (it would further restrict by segment).  However you may end
     up having to save and restore the equality restriction on the inputs, and it is unlikely to gain much.

  
  **** Does the failure to expand M of N's inputs cause problems?  May want an option to return after the first failed input I guess.  
  Could effectively be managed by creating I(0)..I(m-1) pseudo inputs, where input I(i) ensures there are i+1 matches.  
  Internally the M of N would optimize its input order.
  - initial version doesn't bother.

  * The cost/benefit for an input would be stored in the input so could be shared.



  Sentence:
  ~~~~~~~~

  Sentence needs a range of values returned from the LHS to implement, unless represented in ECL as a ternary operator.

  IThorSteppingInput
  {
        virtual void * peek(void * & upper);                            // next record?
        virtual void * peekGE(seek, #fields, void * &upper);            // what would the next item be - minimum for those fields, but not guaranteed.
  }



  a) Proximity requires a transform or other complex cross product to generate the rows.


  Other operators:
  Sentance:
        (a op b) not contain <s>
        lhs->peek(low, high);
        rhs->peekGE(low);
        if (


  Container:
        <x>f()</x>
        lhs = <x></x>
        rhs = f()
        lhs->peek(lowL, highL);
        rhs->peekGE(lowL, lowR, highR);
        if (!in range)
            if (lowR < lo)
                lhs->peekLT(lowR);?
            else
                lhs=->
            



  */
