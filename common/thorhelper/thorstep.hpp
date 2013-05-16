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

#ifndef THORSTEP_HPP_INCL
#define THORSTEP_HPP_INCL

#include "thorcommon.ipp"

//---------------------------------------------------------------------------

//Can be extended, since it is only every implemented within the engines
interface IInputSteppingMeta : public ISteppingMeta
{
//??virtual ISteppingMeta * querySteppingMeta() const = 0
    virtual bool hasPostFilter() = 0;                           // make this const?
    virtual bool isDistributed() const = 0;
    virtual unsigned getSteppedFlags() const = 0;
    virtual double getPriority() = 0;

    inline bool hasPriority() const { return (getSteppedFlags() & SSFhaspriority) != 0; }
};


interface ISteppedJoin : public IInterface
{
public:
    virtual unsigned getNumEqualFields() const = 0;
    virtual ISteppingMeta * querySteppingMeta() const = 0;
    virtual void markRestrictedJoin(unsigned numEqualityFields) = 0;
    virtual void startRestrictedJoin(const void * equalityRow, unsigned numEqualityFields) = 0;
    virtual void stopRestrictedJoin() = 0;
};

class CSteppedInputLookahead;
interface ISteppedConjunctionCollector
{
public:
    virtual void addInput(CSteppedInputLookahead & _input) = 0;
    virtual void addPseudoInput(CSteppedInputLookahead & _input) = 0;
    virtual void addJoin(ISteppedJoin & _join) = 0;
};

interface IMultipleStepSeekInfo
{
    virtual void ensureFilled(const void * seek, unsigned numFields, unsigned maxcount) = 0;                // max count probably needs to be calculated from buffer/seek row size.
    virtual unsigned ordinality() const = 0;
    virtual const void * querySeek(unsigned i) const = 0;
};


//Use a set of named flags rather than a set of booleans because it makes the constructors clearer.
enum
{
    SSEFreturnUnbufferedMatches             = 0x0000,

    //if the returned row matches the seek fields, then it must also match the post filter.
    //if the row doesn't match the seek fields, then it should be returned if the following flag is set.
    SSEFreturnMismatches            = 0x0001,

    //Should we readahead extra rows from this input?  Normally set for a known, or likely, lowest frequency term.
    SSEFreadAhead                   = 0x0002,

    //We may want a flag for a LEFT ONLY join to ensure only the first match is returned.  It could only be set if no post filter on the join.
    SSEFonlyReturnFirstSeekMatch    = 0x0004,

};
    
struct SmartStepExtra
{
    inline SmartStepExtra(unsigned _flags, IMultipleStepSeekInfo * _extraSeeks)
        : flags(_flags), extraSeeks(_extraSeeks)
    {}
    inline SmartStepExtra(const SmartStepExtra & other)
        : flags(other.flags), extraSeeks(other.extraSeeks)
    {}

//Input fields
    inline bool returnMismatches() const { return (flags & SSEFreturnMismatches) != 0; }
    inline bool readAheadManyResults() const { return (flags & SSEFreadAhead) != 0; }
    inline bool onlyReturnFirstSeekMatch() const { return (flags & SSEFonlyReturnFirstSeekMatch) != 0; }

    inline unsigned queryFlags() const { return flags; }

    //If extraSeeks is provided, then it is used to provide a list of additional seek positions - but only as a hint.  
    //If provided, then we are interested in any rows that match the seek positions (and match the post filter), or the row
    //following the last seek position (postfilter determined by returmMismatches)
    inline IMultipleStepSeekInfo * queryExtraSeeks() const { return extraSeeks; }

    inline void setReturnMismatches() { flags |= SSEFreturnMismatches; }
    inline void setReadAhead() { flags |= SSEFreadAhead; }

    inline void set(unsigned _flags, IMultipleStepSeekInfo * _extraSeeks)
    {
        flags = _flags;
        extraSeeks = _extraSeeks;
    }

protected:
    IMultipleStepSeekInfo * extraSeeks;
    unsigned flags;
};

interface ISteppedInput : public IInterface
{
public:
    virtual const void * nextInputRow() = 0;
    virtual const void * nextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra) = 0;
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) = 0;
    virtual IInputSteppingMeta * queryInputSteppingMeta() = 0;
    virtual void resetEOF() = 0;
};

//GH->RKC I'm slightly concerned about the number of parameters on nextInputRowGE(), but they all have different lifetimes, so I think it is correct.


/*
When should the input be read-ahead, and when should mismatches be returned?
Current thinking:
i) The input should be read-ahead
 a) If the input is known to contain the lowest frequemcy term.
 b) If all medians are known, and this is the lowest frequency term.

ii) Mismatches for non equal seek positions should be returned
 a) If the priority of the input isn't known
 b) Except if all medians are known, and a row from the lowest frequency term mismatches and it is still the lowest frequemcy input
 
Note: The buffering and mismatch semantics are generally ignored by the merge join code.  
      An exception is left only join.
*/

#endif
