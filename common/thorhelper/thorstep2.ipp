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

#ifndef THORSTEP2_IPP_INCL
#define THORSTEP2_IPP_INCL

#include "jqueue.tpp"
#include "thorcommon.ipp"
#include "thorstep.ipp"

#define IGNORE_SEEK_TO_SELF

enum { MaxDistanceSamples = 3 };

//---------------------------------------------------------------------------

class SkipDistance
{
public:
    SkipDistance() { field = 0; distance = 0; };

    //-1 less signficant, 0 same, +1 more signficant
    int compare(const SkipDistance & other) const
    {
        //More how can we incorporate some concept of the cost?
        //Much better to pull from a simple input than another complex join, but how can you quantify.
        //e.g., a read from a small inline table, may be much better than smart stepping on an index!
        //The lower the field, the more significant
        if (field < other.field)
            return +1;

        if (field > other.field)
            return -1;

        if (field >= DISTANCE_EXACT_MATCH)
        {
            //skipDistance not signficant.
            //More also take cost into account?)
            return 0;
        }

        //Higher the skip distance the better
        if (distance < other.distance)
            return -1;
        if (distance > other.distance)
            return +1;
        return 0;
    }

    inline void set(unsigned _field, unsigned __int64 _distance) { field = _field; distance = _distance; }
    inline void set(const SkipDistance & other) { field = other.field; distance = other.distance; }
    
public:
    unsigned __int64 distance;
    unsigned field;
};

class OrderedInput : public CInterface
{
public:
    OrderedInput(CSteppedInputLookahead & _input, unsigned _originalIndex, bool hasDistance);

    //MORE: Possibly better if wasCompleteMatch not passed, and queries instead.
    inline const void * next(const void * seek, unsigned numFields, bool seekToSameLocation, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
#ifdef _DEBUG
        assertex(seek);
#endif
        const void * next = input->next(seek, numFields, wasCompleteMatch, stepExtra);
        matched = wasCompleteMatch;
        if (!next)
            return NULL;
        if (optimizeOrder)
        {
            if (seek && stepExtra.returnMismatches())       // only update distance when postfilter can be short circuited.
                updateSequentialDistance(seek, next, numFields, seekToSameLocation, matched);
        }
        return next;
    }

    inline void skip()
    {
        matched = false;
        input->skip();
    }

    inline const void * consumeAndSkip()
    {
        matched = false;
        return input->consume();
    }

    inline bool canOptimizeOrder() { return optimizeOrder; }
    inline unsigned getNumSamples() { return numSamples; }
    inline bool hasPriority() const { return (stepFlags & SSFhaspriority) != 0; }
    inline bool isJoin() const { return (stepFlags & SSFisjoin) != 0; }
    inline double getPriority() { return priority; }
    inline bool readsRowsRemotely() { return input->readsRowsRemotely(); }

    inline void setAlwaysReadExact() { input->setAlwaysReadExact(); }
    inline void setReadAhead(bool _value) { input->setReadAhead(_value); }
    inline void stopOptimizeOrder() { optimizeOrder = false; }

    inline IMultipleStepSeekInfo * createMutipleReadWrapper() { return input->createMutipleReadWrapper(); }
    inline void createMultipleSeekWrapper(IMultipleStepSeekInfo * wrapper) { input->createMultipleSeekWrapper(wrapper); }

    //Could possibly improve by averaging <n> distances.  But would be more complicated, and may not be any better.
    bool skipsFasterThan(const OrderedInput & other) const
    {
        return medianDistance.compare(other.medianDistance) > 0;
    }

    void updateSequentialDistance(const void * seek, const void * next, unsigned numFields, bool seekToSameLocation, bool nowMatched);

protected:
    double priority;
    Owned<CSteppedInputLookahead> input;
    unsigned numSamples;
    unsigned nextDistanceIndex;
    SkipDistance distances[MaxDistanceSamples];
    SkipDistance medianDistance;
    IDistanceCalculator * distanceCalculator;
    unsigned skipCost;
    unsigned stepFlags;
    bool optimizeOrder;

public:
    const unsigned originalIndex;
    bool matched;
};

//NOTE: The lifetime of this class is withing a start()/stop() of the containing activity.
class THORHELPER_API CSteppedConjunctionOptimizer : implements ISteppedConjunctionCollector
{
public:
    CSteppedConjunctionOptimizer(IEngineRowAllocator * _inputAllocator, IHThorNWayMergeJoinArg & _arg, ISteppedInput * _root);
    ~CSteppedConjunctionOptimizer();

    virtual void addInput(CSteppedInputLookahead & _input);
    virtual void addPseudoInput(CSteppedInputLookahead & _input);
    virtual void addJoin(ISteppedJoin & _input);

    //interface IHThorInput
    void afterProcessing();
    void beforeProcessing();
    
    const void * next();
    const void * nextGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);
    //Once it reorders the inputs dynamically, the following will be better
    bool worthCombining();

protected:
    inline bool hasCandidates() const { return equalityRow != NULL; }
    inline bool optimizePostFilter(unsigned nextInput) { return (nextInput != 0) || !inputsHaveMedian; }
    void getInputOrderText(StringBuffer & s);
    void finishCandidates();
    void updateOrder(unsigned whichInput);

private:

protected:
    IHThorNWayMergeJoinArg & helper;
    Linked<IEngineRowAllocator> inputAllocator;
    ISteppingMeta * mergeSteppingMeta;
    IRangeCompare * stepCompare;
    ICompare * equalCompare;
    ICompareEq * partitionCompare;
    ISteppedInput * rootActivity;               // linking would create a cycle
    void * equalityRow;
    void * prevEqualityRow;
    const void * lowestSeekRow;
    void * prevPartitionRow;
    unsigned numEqualFields;
    unsigned numInputs;
    unsigned numPriorityInputs;
    unsigned numOptimizeInputs;
    unsigned maxOptimizeInput;
    unsigned seekMatchTicker;                   // not really a counter - used to select which input is read next
    CSteppedInputLookaheadArray inputs;
    CSteppedInputLookaheadArray pseudoInputs;
    CIArrayOf<OrderedInput> orderedInputs;
    IArrayOf<ISteppedJoin> joins;
    bool eof;
    bool inputsHaveMedian;
    bool inputHasPostfilter;
    bool inputIsDistributed;

protected:
    virtual bool findCandidates(const void * seekValue, unsigned numSeekFields);

private:
    unsigned nextToMatch() const;
};

void associateRemoteInputs(CIArrayOf<OrderedInput> & orderedInputs, unsigned numPriorityInputs);
int compareInitialInputOrder(CInterface * * _left, CInterface * * _right);

#endif
