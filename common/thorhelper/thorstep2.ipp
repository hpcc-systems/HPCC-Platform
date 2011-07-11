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
    inline bool hasPriority() { return (stepFlags & SSFhaspriority) != 0; }
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

#endif
