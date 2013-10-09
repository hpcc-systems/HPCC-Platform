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

#ifndef __THORPARSE_IPP_
#define __THORPARSE_IPP_

//MORE: Most of this should really be in a thorrparse.ipp instead.  Move later.

#include "unicode/utf.h"
#include "thorrparse.hpp"
#include "eclhelper.hpp"

//MORE: How know if processing unicode.

class NlpState;
interface INlpMatchedAction
{
public:
    virtual bool onMatch(NlpState & matched) = 0;
};

class THORHELPER_API NlpState
{
public:
    NlpState(INlpMatchedAction * _action, NlpInputFormat _inputFormat, size32_t len, const void * text);

    void pushMatch(MatchState & match, MatchSaveState & save);
    void popMatch(const MatchSaveState & save);
    void markFinish(MatchSaveState & save);
    void unmarkFinish(const MatchSaveState & save);

public:
    const byte * cur;
    const byte * start;
    const byte * end;
    MatchState top;
    MatchState * curMatch;
    MatchState * * next;
    INlpMatchedAction * matchAction;
    NlpInputFormat inputFormat;
    unsigned charSize;
    int score;
};

#define UNKNOWN_INSTANCE    ((unsigned)-1)

class NlpMatchSearchInstance
{
public:
    unsigned lastExactMatchDepth;
    unsigned nextIndex;
};

class THORHELPER_API NlpMatchPath : public CInterface
{
public:
    NlpMatchPath(MemoryBuffer & in);
    NlpMatchPath(const UnsignedArray & _ids, const UnsignedArray & _indices);
    ~NlpMatchPath();

    void serialize(MemoryBuffer & buffer) const;

    inline unsigned numItems() const { return ids.ordinality(); }
    inline unsigned getId(unsigned i) const { return ids.item(i); }
    inline unsigned getIndex(unsigned i) const { return indices.item(i); }
    inline bool matchAny(unsigned i) const { return indices.item(i) == UNKNOWN_INSTANCE; }

    inline unsigned nextExactMatchIndex(unsigned from) const
    {
        for (unsigned i=from; i < indices.ordinality(); i++)
        {
            unsigned cur = indices.item(i);
            if (cur != UNKNOWN_INSTANCE)
                return cur;
        }
        return 0;
    }

protected:
    UnsignedArray ids;
    UnsignedArray indices;
};


class CMatchedElement : public CInterface, public IMatchedElement
{
public:
    CMatchedElement(MatchState * _cur)                      { cur = _cur; }
    IMPLEMENT_IINTERFACE

    virtual const byte * queryStartPtr() const              { return cur->start; }
    virtual const byte * queryEndPtr() const                { return cur->end; }
    virtual const byte * queryRow() const                   { return NULL; }

protected:
    MatchState * cur;
};

class NoMatchElement : public CInterface, public IMatchedElement
{
public:
    IMPLEMENT_IINTERFACE

    virtual const byte * queryStartPtr() const              { return ptr; }
    virtual const byte * queryEndPtr() const                { return ptr; }
    virtual const byte * queryRow() const                   { return NULL; }

public:
    const byte * ptr;
};

class MatchWalker2MatchedElement : public CInterface, public IMatchedElement
{
public:
    MatchWalker2MatchedElement(IMatchWalker * _cur)         { cur.set(_cur); }
    IMPLEMENT_IINTERFACE

    virtual const byte * queryStartPtr() const              { return (const byte *)cur->queryMatchStart(); }
    virtual const byte * queryEndPtr() const                { return (const byte *)cur->queryMatchStart() + cur->queryMatchSize(); }
    virtual const byte * queryRow() const                   { return NULL; }

protected:
    Owned<IMatchWalker> cur;
};

class THORHELPER_API CMatchedResultInfo : public CInterface
{
    friend class CMatchedResults;
public:
    CMatchedResultInfo();
    
    virtual void deserialize(MemoryBuffer & buffer);
    virtual void serialize(MemoryBuffer & buffer) const;

    virtual void addResult(const UnsignedArray & ids, const UnsignedArray & indices) = 0;

    void setFormat(NlpInputFormat value)                        { inputFormat = value; }

protected:
    virtual NlpMatchPath * createMatchPath(MemoryBuffer & in) = 0;

public:
    CIArrayOf<NlpMatchPath> matchResults;
    byte inputFormat;
};


class THORHELPER_API CMatchedResults : public CInterface, implements IMatchedResults
{
public:
    CMatchedResults(CMatchedResultInfo * _def);
    ~CMatchedResults();
    IMPLEMENT_IINTERFACE
    
    void kill();

    //IMatchedResults
    virtual bool getMatched(unsigned idx);
    virtual size32_t getMatchLength(unsigned idx);
    virtual size32_t getMatchPosition(unsigned idx);
    virtual void getMatchText(size32_t & outlen, char * & out, unsigned idx);
    virtual void getMatchUnicode(size32_t & outlen, UChar * & out, unsigned idx);
    virtual void getMatchUtf8(size32_t & outlen, char * & out, unsigned idx);
    virtual byte * queryMatchRow(unsigned idx);
    virtual byte * queryRootResult();

protected:
    CMatchedResultInfo * def;
    IMatchedElement * * matched;
    const byte * in;
    NoMatchElement notMatched;
    const byte * rootResult;
};


class NlpMatchWalker : public CInterface, public IMatchWalker
{
public:
    NlpMatchWalker(MatchState * state) { curMatch = state; }
    IMPLEMENT_IINTERFACE

    virtual IAtom * queryName();
    virtual unsigned queryID()                              { return curMatch->id; }
    virtual size32_t queryMatchSize();
    virtual const void * queryMatchStart();
    virtual unsigned numChildren();
    virtual IMatchWalker * getChild(unsigned idx);

protected:
    MatchState * curMatch;
};


class THORHELPER_API NlpAlgorithm : public CInterface, implements INlpParseAlgorithm
{
public:
    NlpAlgorithm(CMatchedResultInfo * _matched);
    ~NlpAlgorithm();
    IMPLEMENT_IINTERFACE

    virtual void serialize(MemoryBuffer & out);
            void deserialize(MemoryBuffer & in);

    virtual void setChoose(bool _chooseMin, bool _chooseMax, bool _chooseBest, bool _singleChoicePerLine); 
    virtual void setJoin(bool _notMatched, bool _notMatchedOnly); 
    virtual void setLimit(size32_t _maxLength); 
    virtual void setOptions(MatchAction _matchAction, ScanAction _scanAction, NlpInputFormat _inputFormat, unsigned _keepLimit, unsigned _atMostLimit);

public:
    MatchAction matchAction;
    ScanAction scanAction;
    NlpInputFormat inputFormat;
    bool addedSeparators;
    unsigned keepLimit;
    unsigned atMostLimit;
    byte charWidth;
    CMatchedResultInfo * matchInfo;
    size32_t maxLength;
    bool notMatched;
    bool notMatchedOnly;
    bool chooseMin;
    bool chooseMax;
    bool chooseBest;
    bool singleChoicePerLine;
};

//---------------------------------------------------------------------------

class THORHELPER_API IDfaPattern : public IInterface
{
public:
    virtual void init(unsigned numStates) = 0;
    virtual void beginState(unsigned id) = 0;
    virtual void setStateAccept(unsigned id) = 0;
    virtual void endState() = 0;
    virtual void addTransition(unsigned next, unsigned nextState) = 0;
    virtual void finished() = 0;
    virtual void init(unsigned numStates, unsigned approxTransitions) = 0;
};


struct AsciiDfaState
{
    inline bool accepts() const                             { return acceptID != NotFound; }

    unsigned delta;
    byte min;
    byte max;
    unsigned acceptID;
};

class THORHELPER_API AsciiDfa
{
    friend class AsciiDfaBuilder;
public:
    AsciiDfa();
    ~AsciiDfa();

    void init(unsigned _numStates);
    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);

    unsigned getNumStates() const           { return numStates; }
    unsigned getNumTransitions() const      { return numTransitions; }
    unsigned getAccepts(const AsciiDfaState & state, unsigned idx) const;
    const AsciiDfaState * queryStates() const { return states; }
    unsigned * queryTransitions() const     { return transitions; }
    void setEmpty();
    void toXML(StringBuffer & out, unsigned detail);

protected:
    unsigned numAccepts;
    unsigned numStates;
    unsigned numTransitions;
    unsigned * accepts;
    AsciiDfaState * states;
    unsigned * transitions;
};
unsigned getMaximumMatchLength(AsciiDfa & dfa, unsigned len, const byte * start);

class THORHELPER_API AsciiDfaBuilder : public CInterface, implements IDfaPattern
{
public:
    AsciiDfaBuilder(AsciiDfa & _dfa);
    IMPLEMENT_IINTERFACE

    virtual void addTransition(unsigned next, unsigned nextState);
    virtual void init(unsigned _numStates);
    virtual void init(unsigned numStates, unsigned approxTransitions);
    virtual void beginState(unsigned id);
    virtual void setStateAccept(unsigned id);
    virtual void endState();
    virtual void finished();

private:
    void reallocTransitions(unsigned level);

protected:
    AsciiDfa & dfa;

    unsigned curState;
    unsigned firstTransition;
    unsigned maxTransitions;
    UnsignedArray accepts;
};


void deserializeBoolArray(unsigned len, bool * values, MemoryBuffer & in);
void serializeBoolArray(MemoryBuffer & out, unsigned len, const bool * values);

INlpParseAlgorithm * createRegexParser(MemoryBuffer & buffer, IOutputMetaData * outRecordSize, byte kind);
INlpParseAlgorithm * createTomitaParser(MemoryBuffer & buffer, IOutputMetaData * outRecordSize);

#endif /* __THORPARSE_HPP_ */
