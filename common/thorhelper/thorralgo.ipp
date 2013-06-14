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

#ifndef __THORRALGO_IPP_
#define __THORRALGO_IPP_

#include "unicode/utf.h"
#include "thorparse.ipp"
#include "thorrparse.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "thorcommon.ipp"

#ifdef _DEBUG
//#define TRACE_REGEX
#endif


class RegexMatchSearchInstance : public NlpMatchSearchInstance
{
public:
    MatchState * find(MatchState * top, const NlpMatchPath & path, unsigned depth);
};

class THORHELPER_API RegexMatchPath : public NlpMatchPath
{
public:
    RegexMatchPath(MemoryBuffer & in) : NlpMatchPath(in) {}
    RegexMatchPath(const UnsignedArray & _ids, const UnsignedArray & _indices) : NlpMatchPath(_ids, _indices) {}

    IMatchedElement * getMatch(MatchState * top, bool removeTrailingSeparator) const;
};

class THORHELPER_API CRegexMatchedResultInfo : public CMatchedResultInfo
{
public:
    virtual void addResult(const UnsignedArray & ids, const UnsignedArray & indices)
                                                            { matchResults.append(*new RegexMatchPath(ids, indices)); }
    virtual NlpMatchPath * createMatchPath(MemoryBuffer & in) { return new RegexMatchPath(in); }
};


class THORHELPER_API CRegexMatchedResults : public CMatchedResults
{
public:
    CRegexMatchedResults(CMatchedResultInfo * _def) : CMatchedResults(_def) {}

    void extractResults(MatchState & matched, const byte * _in, bool _removeTrailingSeparator);
};

//--------------------------------------------------------------------------
// stack-free implementation of the regex matcher

enum { 
        RSinit,             // initial state - should never stay in this state..
        RSnextfollow,       // ready to process the next following item
        RSretry,            // failed to process following, try to match again.
        RSfinished,         // backtrack if we get to here again
        RSrepeat,           // process repeat then follow.
        RSfollowonly,       // only process following, don't try to rematch.
        RSrepeatonly,       // only process repeat, don't do following.
        RSdonerepeat,
};

enum {
// static information
        RFbeginToken            = 0x0001,
};

class RegexMatchStateSave;
class RegexRepeatInstance;
class ActiveStage
{
public:
    inline RegexMatchAction nextAction(RegexState & state)  { return pattern->nextAction(*this, state); }

    inline void cleanup(RegexState & state)                     { pattern->killStage(*this, state); }
    inline unsigned getState()                              { return state; }
#ifdef TRACE_REGEX
    ActiveStage & setState(unsigned _state);
#else
    inline ActiveStage & setState(unsigned _state)  { state = _state; return *this; }
#endif

    inline bool isBeginToken()                              { return (flags & RFbeginToken) != 0; }

    inline ActiveStage & setMatched()
    {
        setState(RSnextfollow);
        nextFollow = 0;
        return *this;
    }

public:
    RegexPattern * pattern;
    const byte * followPosition;
    unsigned nextFollow;
    union
    {
        MatchSaveState saved;
        RegexMatchStateSave * matched;
        RegexPattern * nextPattern;
        unsigned prevPotentialMatches;
        RegexRepeatInstance * repeatInstance;
        const byte * limit;
    } extra;
    byte state;
    byte flags;
};

inline ActiveStage & Array__Member2Param(ActiveStage & src)            { return src; }  
inline void Array__Assign(ActiveStage & dest, ActiveStage & src)     { dest = src; }
inline bool Array__Equal(const ActiveStage & m, const ActiveStage & p) { UNIMPLEMENTED; }

MAKECopyArrayOf(ActiveStage, ActiveStage &, ActiveStageArray);

//---------------------------------------------------------------------------

class RegexState;
// Used to represent a single match in the regular expression tree.  Also 
class THORHELPER_API RegexMatchState : public CInterface, public MatchState
{
public:
    RegexMatchState() : MatchState() { }
    RegexMatchState(IAtom * _name, regexid_t _id) : MatchState(_name, _id) { }
    RegexMatchState(RegexNamed * owner) : MatchState(owner->queryName(), owner->queryID()) {}
    IMPLEMENT_IINTERFACE

    using MatchState::reset;
    void reset(RegexNamed * owner) { MatchState::reset(owner->queryName(), owner->queryID()); }
};

class THORHELPER_API RegexMatchStateSave : public RegexMatchState
{
public:
    RegexMatchStateSave() : RegexMatchState() { }
    RegexMatchStateSave(IAtom * _name, regexid_t _id) : RegexMatchState(_name, _id) { }
    RegexMatchStateSave(RegexNamed * owner) : RegexMatchState(owner) {}

public:
    MatchSaveState save;
};

struct RegexMatchInfo;
class RegexStateCache
{
public:
    RegexMatchState * createState(RegexNamed * def);
    RegexMatchStateSave * createStateSave(RegexNamed * def);
    RegexMatchStateSave * createStateSave(IAtom * _name, regexid_t _id);
    void destroyState(RegexMatchState * state);
    void destroyStateSave(RegexMatchStateSave * state);

    CIArrayOf<RegexMatchState> matchStates;
    CIArrayOf<RegexMatchStateSave> matchStateSaves;
    ConstPointerArray potentialMatches;
};

class RegexState : public NlpState
{
public:
    RegexState(RegexStateCache & _cache, unsigned _implementation, INlpHelper * _helper, INlpMatchedAction * _action, NlpInputFormat _inputFormat, size32_t _len, const void * _text)
    : NlpState(_action, _inputFormat, _len, _text), cache(_cache)
    { implementation = _implementation; numMatched = 0; curActiveStage = NotFound; helper = _helper; }

    RegexState(const RegexState & _state, INlpMatchedAction * _action, size32_t _len, const void * _text)
    : NlpState(_action, _state.inputFormat, _len, _text), cache(_state.cache)
    { 
        implementation = _state.implementation; numMatched = 0; curActiveStage = NotFound; helper = _state.helper;
    }

    void processPattern(RegexPattern * grammar);
    ActiveStage & pushStage();
    void popStage();

protected:
    inline bool hasActiveStage()                            { return (curActiveStage != NotFound); }
    inline ActiveStage & topStage()                         { return stages.item(curActiveStage); }

public:
    RegexStateCache & cache;
    RegexPatternCopyArray stack;
    IMatchedAction * processor;
    const byte * nextScanPosition;
    const void * row;
    unsigned numMatched;
    RegexMatchInfo * best;
    bool lengthIsLimited;
    INlpHelper * helper;
    ActiveStageArray stages;
    unsigned curActiveStage;
    RegexPatternCopyArray namedStack;                   // used to maintain a stack of which pattern to process next when a named pattern is completed.
    unsigned implementation;
};

// Actuall classes used for matching regular expressions.

struct RegexSerializeState
{
    RegexPatternArray patterns;
    RegexNamedArray named;
};

struct RegexXmlState : public RegexSerializeState
{
    void addLink(RegexPattern & cur) { if ((visited.find(cur) == NotFound) && (toVisit.find(cur) == NotFound)) toVisit.add(OLINK(cur),0); }
    void reset() { visited.kill(); toVisit.kill(); }
    RegexPatternArray visited;
    RegexPatternArray toVisit;
    unsigned detail;
};

//--------------------------------------------------------------------------

class THORHELPER_API RegexNullPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexNull; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
};

class THORHELPER_API RegexRecursivePattern : public RegexPattern
{
    friend class RegexEndRecursivePattern;
public:
    virtual ThorRegexKind getKind() { return ThorRegexRecursive; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
private:
    ConstPointerArray positionStack;
};

class THORHELPER_API RegexEndRecursivePattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexEndRecursive; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
};

class THORHELPER_API RegexAsciiBasePattern : public RegexPattern
{
public:
    RegexAsciiBasePattern(unsigned _len=0, const char * _text=NULL) { len = _len; text.set(_text, _len); }

//serialization code....
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

protected:
    unsigned len;
    StringAttr text;
};

class THORHELPER_API RegexAsciiPattern : public RegexAsciiBasePattern
{
public:
    RegexAsciiPattern(unsigned _len=0, const char * _text=NULL) : RegexAsciiBasePattern(_len, _text) {}

    virtual ThorRegexKind getKind() { return ThorRegexAscii; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

protected:
    inline bool doMatch(RegexState & state);
};

//I prefix means case insensitive.
class THORHELPER_API RegexAsciiIPattern : public RegexAsciiBasePattern
{
public:
    RegexAsciiIPattern(unsigned _len=0, const char * _text=NULL) : RegexAsciiBasePattern(_len, _text) {}

    virtual ThorRegexKind getKind() { return ThorRegexAsciiI; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

protected:
    inline bool doMatch(RegexState & state);
};


class THORHELPER_API RegexUnicodePattern : public RegexPattern
{
public:
    RegexUnicodePattern() {}
    RegexUnicodePattern(unsigned _len, const UChar * _text) { text.set(_len*2, _text); }

    virtual ThorRegexKind getKind() { return ThorRegexUnicode; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

//serialization code....
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);

protected:
    inline bool doMatch(RegexState & state);

protected:
    MemoryAttr text;
};

//Case insensitive simple comparison.
class THORHELPER_API RegexUnicodeIPattern : public RegexPattern
{
public:
    RegexUnicodeIPattern() {}
    RegexUnicodeIPattern(unsigned _len, const UChar * _text);

    virtual ThorRegexKind getKind() { return ThorRegexUnicodeI; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

//serialization code....
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);

protected:
    inline bool doMatch(RegexState & state);

protected:
    MemoryAttr lower;
    MemoryAttr upper;
};

//A full case, no locale string comparison.
class THORHELPER_API RegexUnicodeFIPattern : public RegexPattern
{
    RegexUnicodeFIPattern() {}
    RegexUnicodeFIPattern(unsigned _len, const UChar * _text);

    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

protected:
    inline bool doMatch(RegexState & state);

protected:
    MemoryAttr folded;
};

class THORHELPER_API RegexUtf8Pattern : public RegexPattern
{
public:
    RegexUtf8Pattern() {}
    RegexUtf8Pattern(unsigned _len, const char * _text) { text.set(rtlUtf8Length(_len, _text), _text); }

    virtual ThorRegexKind getKind() { return ThorRegexUtf8; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

//serialization code....
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);

protected:
    inline bool doMatch(RegexState & state);

protected:
    MemoryAttr text;
};

//Case insensitive simple comparison.
class THORHELPER_API RegexUtf8IPattern : public RegexPattern
{
public:
    RegexUtf8IPattern() {}
    RegexUtf8IPattern(unsigned _len, const char * _text);

    virtual ThorRegexKind getKind() { return ThorRegexUtf8I; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

//serialization code....
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);

protected:
    inline bool doMatch(RegexState & state);

protected:
    MemoryAttr lower;
    MemoryAttr upper;
};

// ---- Simple patterns ------------------------------

class THORHELPER_API RegexSetBasePattern : public RegexPattern
{
public:
    virtual void setInvert(bool value) = 0;
    virtual void addRange(unsigned low, unsigned high) = 0;
};

class THORHELPER_API RegexAsciiSetBasePattern : public RegexSetBasePattern
{
public:
    RegexAsciiSetBasePattern();

    virtual void setInvert(bool value);

    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

//serialization code....
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);
    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

protected:
    virtual void addSpecialRange(unsigned value);
    inline bool doMatch(RegexState & state);

protected:
    bool include[256];
    bool inverted;
};

class THORHELPER_API RegexAsciiSetPattern : public RegexAsciiSetBasePattern
{
public:
    virtual void addRange(unsigned low, unsigned high);

    virtual ThorRegexKind getKind() { return ThorRegexAsciiSet; }
};

class THORHELPER_API RegexAsciiISetPattern : public RegexAsciiSetBasePattern
{
public:
    virtual void addRange(unsigned low, unsigned high);

    virtual ThorRegexKind getKind() { return ThorRegexAsciiISet; }
};

// ---- Simple patterns ------------------------------

//Handles utf8 and unicode.
class THORHELPER_API RegexUnicodeSetPattern : public RegexSetBasePattern
{
public:
    RegexUnicodeSetPattern(bool _caseSensitive = false);

    virtual void addRange(unsigned low, unsigned high);
    virtual void setInvert(bool value);

    virtual ThorRegexKind getKind() { return ThorRegexUnicodeSet; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

//serialization code....
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);
    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

protected:
    inline bool doMatch(RegexState & state);

protected:
    UnsignedArray from;
    UnsignedArray to;
    bool inverted;
    bool caseSensitive;
};

// ---- Simple patterns ------------------------------

class THORHELPER_API RegexAnyCharPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexAnyChar; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
};

class THORHELPER_API RegexPenaltyPattern : public RegexPattern
{
public:
    RegexPenaltyPattern(int _penalty = 0)   { penalty = _penalty; }

    virtual ThorRegexKind getKind() { return ThorRegexPenalty; }
    virtual RegexMatchAction match(RegexState & state);

    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);

//serialization code....
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);
    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

protected:
    int penalty;
};

class THORHELPER_API RegexStartPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexStart; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
};

class THORHELPER_API RegexFinishPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexFinish; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
};

class THORHELPER_API RegexBeginTokenPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexBeginToken; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
};

class THORHELPER_API RegexEndTokenPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexEndToken; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual RegexMatchAction nextAction(ActiveStage & stage, RegexState & state);
};

class THORHELPER_API RegexDonePattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexDone; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
};

class THORHELPER_API RegexEndNestedPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexEndNested; }
    virtual RegexMatchAction match(RegexState & state);

    virtual void killStage(ActiveStage & stage, RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
};

// ---- Assertion patterns ------------------------------

class THORHELPER_API MatchedAssertionAction : public INlpMatchedAction
{
public:
    MatchedAssertionAction()            { matched = false; }

    bool isMatched()                    { return matched; }
    virtual bool onMatch(NlpState & state)  { matched = true; return matched; }

private:
    bool matched;
};

class THORHELPER_API ExactMatchedAssertionAction : public INlpMatchedAction
{
public:
    ExactMatchedAssertionAction()           { matched = false; }

    bool isMatched()                        { return matched; }
    virtual bool onMatch(NlpState & state)  { matched = (state.cur == state.end); return matched; }

private:
    bool matched;
};

class THORHELPER_API RegexAssertionPattern : public RegexPattern
{
public:
    virtual void dispose(); // needed to free the structure because it is a cyclic graph
    virtual bool gather(RegexSerializeState & state);

    virtual void serializeLinks(MemoryBuffer & out, RegexSerializeState & state);
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializeLinks(MemoryBuffer & in, RegexSerializeState & state);
    virtual void deserializePattern(MemoryBuffer & in);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

    virtual void setSubPattern(RegexPattern * _pattern)     { pattern.set(_pattern); }

protected:
    Owned<RegexPattern> pattern;
    bool invert;
};


//
class THORHELPER_API RegexAssertNextPattern : public RegexAssertionPattern
{
public:
    RegexAssertNextPattern(bool _invert = false) { invert = _invert; }

    virtual ThorRegexKind getKind() { return ThorRegexAssertNext; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

protected:
    bool nextMatches(RegexState & state);
};

class THORHELPER_API RegexAssertPrevPattern : public RegexAssertionPattern
{
public:
    RegexAssertPrevPattern(bool _invert = false, unsigned _minSize = 0, unsigned _maxSize = 0) { invert = _invert; minSize = _minSize; maxSize = _maxSize; }

    virtual ThorRegexKind getKind() { return ThorRegexAssertPrev; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);

protected:
    bool prevMatches(RegexState & state);
    
protected:
    unsigned minSize;
    unsigned maxSize;
};


//---------------------------------------------------------------------------

class THORHELPER_API MatchedCheckAction : public INlpMatchedAction
{
public:
    MatchedCheckAction()            { matched = false; }

    bool isMatched()                    { return matched; }
    virtual bool onMatch(NlpState & state)  { matched = (state.cur == state.end); return matched; }

private:
    bool matched;
};

class THORHELPER_API RegexBeginCheckPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexBeginCheck; }
    virtual RegexMatchAction match(RegexState & state);

    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);
};


class THORHELPER_API RegexCheckPattern : public RegexPattern
{
public:
    RegexCheckPattern(bool _invert, bool _stripSeparator = false) { invert = _invert; stripSeparator = _stripSeparator; }

    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);

    inline const byte * getEndMatch(RegexState & state);

protected:
    bool stripSeparator;
    bool invert;
};



class THORHELPER_API RegexCheckInPattern : public RegexCheckPattern
{
public:
    RegexCheckInPattern(bool _invert = false, bool _stripSeparator = false) : RegexCheckPattern(_invert, _stripSeparator) { }

    virtual ThorRegexKind getKind() { return ThorRegexCheck; }

    virtual void dispose(); // needed to free the structure because it is a cyclic graph
    virtual bool gather(RegexSerializeState & state);
    virtual void serializeLinks(MemoryBuffer & out, RegexSerializeState & state);
    virtual void deserializeLinks(MemoryBuffer & in, RegexSerializeState & state);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);

    virtual void setSubPattern(RegexPattern * _pattern)     { pattern.set(_pattern); }

protected:
    Owned<RegexPattern> pattern;
};


class THORHELPER_API RegexCheckLengthPattern : public RegexCheckPattern
{
public:
    RegexCheckLengthPattern(bool _invert = false, bool _stripSeparator = false, unsigned _minExpectedBytes = 0, unsigned _maxExpectedBytes = 0) :
        RegexCheckPattern(_invert, _stripSeparator) { minExpectedBytes = _minExpectedBytes; maxExpectedBytes = _maxExpectedBytes; }

    virtual ThorRegexKind getKind() { return ThorRegexCheckLength; }

    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);

protected:
    inline bool doMatch(RegexState & state);

protected:
    unsigned minExpectedBytes;
    unsigned maxExpectedBytes;
};


class THORHELPER_API RegexValidatePattern : public RegexCheckPattern
{
public:
    RegexValidatePattern(bool _stripSeparator, unsigned idx) : RegexCheckPattern(false, _stripSeparator) { validatorIndex = idx; }

    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);
    virtual void deserializeLinks(MemoryBuffer & in, RegexSerializeState & state);

    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);
    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

protected:
    virtual bool doMatch(RegexState & state) = 0;

protected:
    unsigned validatorIndex;
};

class THORHELPER_API RegexValidateAscAsAscPattern : public RegexValidatePattern
{
public:
    RegexValidateAscAsAscPattern(bool _stripSeparator = false, unsigned idx=0) : RegexValidatePattern(_stripSeparator, idx) {}

    virtual ThorRegexKind getKind() { return ThorRegexValidateAscAsAsc; }

protected:
    virtual bool doMatch(RegexState & state);
};

class THORHELPER_API RegexValidateAscAsUniPattern : public RegexValidatePattern
{
public:
    RegexValidateAscAsUniPattern(bool _stripSeparator = false, unsigned idx=0) : RegexValidatePattern(_stripSeparator, idx) {}

    virtual ThorRegexKind getKind() { return ThorRegexValidateAscAsUni; }

protected:
    virtual bool doMatch(RegexState & state);
};

class THORHELPER_API RegexValidateUniAsAscPattern : public RegexValidatePattern
{
public:
    RegexValidateUniAsAscPattern(bool _stripSeparator = false, unsigned idx=0) : RegexValidatePattern(_stripSeparator, idx) {}

    virtual ThorRegexKind getKind() { return ThorRegexValidateUniAsAsc; }

protected:
    virtual bool doMatch(RegexState & state);
};

class THORHELPER_API RegexValidateUniAsUniPattern : public RegexValidatePattern
{
public:
    RegexValidateUniAsUniPattern(bool _stripSeparator = false, unsigned idx=0) : RegexValidatePattern(_stripSeparator, idx) {}

    virtual ThorRegexKind getKind() { return ThorRegexValidateUniAsUni; }

protected:
    virtual bool doMatch(RegexState & state);
};


class THORHELPER_API RegexValidateUtf8AsAscPattern : public RegexValidatePattern
{
public:
    RegexValidateUtf8AsAscPattern(bool _stripSeparator = false, unsigned idx=0) : RegexValidatePattern(_stripSeparator, idx) {}

    virtual ThorRegexKind getKind() { return ThorRegexValidateUtf8AsAsc; }

protected:
    virtual bool doMatch(RegexState & state);
};

class THORHELPER_API RegexValidateUtf8AsUniPattern : public RegexValidatePattern
{
public:
    RegexValidateUtf8AsUniPattern(bool _stripSeparator = false, unsigned idx=0) : RegexValidatePattern(_stripSeparator, idx) {}

    virtual ThorRegexKind getKind() { return ThorRegexValidateUtf8AsUni; }

protected:
    virtual bool doMatch(RegexState & state);
};

// ---- Named patterns ------------------------------

class THORHELPER_API RegexNamedPattern : public RegexPattern
{
    class THORHELPER_API RegexEndNamedPattern : public RegexPattern
    {
    public:
        virtual ThorRegexKind getKind() { return ThorRegexEndNamed; }
        virtual RegexMatchAction match(RegexState & state);
        virtual void killStage(ActiveStage & stage, RegexState & state);
        virtual RegexMatchAction beginMatch(RegexState & state);
        virtual RegexMatchAction nextAction(ActiveStage & stage, RegexState & state);
    public:
        RegexNamedPattern * named;
    };

    friend class RegexEndNamedPattern;
public:
    RegexNamedPattern() { end.named = this; }

    virtual ThorRegexKind getKind() { return ThorRegexNamed; }
    virtual RegexMatchAction match(RegexState & state);

    virtual void killStage(ActiveStage & stage, RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

    virtual void dispose(); // needed to free the structure because it is a cyclic graph
    virtual bool gather(RegexSerializeState & state);
    virtual void serializeLinks(MemoryBuffer & out, RegexSerializeState & state);
    virtual void deserializeLinks(MemoryBuffer & in, RegexSerializeState & state);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

    virtual void setBody(RegexNamed * name)                 { def.set(name); }

protected:
    virtual void getTraceText(StringBuffer & s);

protected:
    Owned<RegexNamed> def;
    RegexEndNamedPattern end;
};


class THORHELPER_API RegexBeginSeparatorPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexBeginSeparator; }
    virtual RegexMatchAction match(RegexState & state);

    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);
};

class THORHELPER_API RegexEndSeparatorPattern : public RegexPattern
{
public:
    virtual ThorRegexKind getKind() { return ThorRegexEndSeparator; }
    virtual RegexMatchAction match(RegexState & state);

    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual RegexMatchAction nextAction(ActiveStage & stage, RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);

    inline RegexMatchAction mapAction(RegexMatchAction action) 
    { 
        if (action == RegexMatchBacktrack)
            return RegexMatchBacktrackToken;
        return action;
    }
};

//---- Repeats - a bit complicated --------------------------------

class THORHELPER_API RegexRepeatPattern : public RegexPattern
{
    friend class RegexRepeatInstance;
public:
    RegexRepeatPattern() {}
    RegexRepeatPattern(unsigned _min, unsigned _max, bool _greedy) { minLimit = _min; maxLimit = _max; greedy = _greedy; }

    virtual ThorRegexKind getKind() { return ThorRegexRepeat; }
    virtual RegexMatchAction match(RegexState & state);

    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual RegexMatchAction nextAction(ActiveStage & stage, RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);

    virtual void dispose(); // needed to free the structure because it is a cyclic graph
    virtual bool gather(RegexSerializeState & state);
    virtual void serializeLinks(MemoryBuffer & out, RegexSerializeState & state);
    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializeLinks(MemoryBuffer & in, RegexSerializeState & state);
    virtual void deserializePattern(MemoryBuffer & in);

    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);


    virtual void setBody(RegexNamed * name)                 { def.set(name); }

protected:
    RegexMatchAction match(RegexState & state, unsigned curMin, unsigned curMax);
    RegexMatchAction matchNextInstance(RegexState & state, unsigned curMin, unsigned curMax);

    RegexMatchAction beginMatchNextInstance(RegexState & state, unsigned curMin, unsigned curMax);
    RegexMatchAction beginMatch(RegexState & state, unsigned curMin, unsigned curMax);
    inline RegexMatchAction nextRepeatMatch(ActiveStage & stage, RegexState & state);

protected:
    Owned<RegexNamed> def;
    unsigned minLimit;
    unsigned maxLimit;
    bool greedy;
};

class THORHELPER_API RegexRepeatInstance : public RegexPattern
{
public:
    RegexRepeatInstance(RegexRepeatPattern * _def, unsigned _minLimit, unsigned _maxLimit) { def = _def; minLimit = _minLimit; maxLimit = _maxLimit; }

    virtual ThorRegexKind getKind() { return ThorRegexRepeatInstance; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

    virtual void serializePattern(MemoryBuffer & out)   { UNIMPLEMENTED; }
    virtual void deserializePattern(MemoryBuffer & in)  { UNIMPLEMENTED; }

private:
    RegexRepeatPattern * def;
    unsigned minLimit;
    unsigned maxLimit;
};


//--------------------------------------------------------------------------

class THORHELPER_API RegexDfaPattern : public RegexPattern
{
public:
    virtual IDfaPattern * createBuilder() = 0;
};


class THORHELPER_API RegexAsciiDfaPattern : public RegexDfaPattern
{
public:
    RegexAsciiDfaPattern(bool _matchesToken = false);

    virtual ThorRegexKind getKind() { return ThorRegexAsciiDFA; }
    virtual RegexMatchAction match(RegexState & state);

    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual RegexMatchAction nextAction(ActiveStage & stage, RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);

    virtual IDfaPattern * createBuilder()       { return new AsciiDfaBuilder(dfa); }

    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);
    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

protected:
    AsciiDfa    dfa;
    bool matchesToken;
};


class THORHELPER_API RegexUnicodeDfaPattern : public RegexDfaPattern
{
public:
    RegexUnicodeDfaPattern();
    ~RegexUnicodeDfaPattern();

    virtual ThorRegexKind getKind() { return ThorRegexUnicodeDFA; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);

    virtual IDfaPattern * createBuilder()       { UNIMPLEMENTED; return NULL; }

    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);
};


//--------------------------------------------------------------------------

class THORHELPER_API RegexRepeatAnyPattern : public RegexPattern
{
public:
    RegexRepeatAnyPattern(unsigned _low=0, unsigned _high=0, bool _minimal=false);

    virtual ThorRegexKind getKind() { return ThorRegexRepeatAny; }
    virtual RegexMatchAction match(RegexState & state);
    virtual RegexMatchAction beginMatch(RegexState & state);
    virtual RegexMatchAction nextAction(ActiveStage & stage, RegexState & state);

    virtual void serializePattern(MemoryBuffer & out);
    virtual void deserializePattern(MemoryBuffer & in);
    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

protected:
    unsigned low;
    unsigned high;
    bool minimal;
};



/*
       ThorRegexDFA, ThorRegexIDFA, ThorRegexSet, ThorRegexISet,
       ThorRegexUnicode, ThorRegexUnicodeI, 
       ThorRegexMax
*/

class THORHELPER_API RegexAlgorithm : public NlpAlgorithm
{
    friend class RegexParser;
public:
    RegexAlgorithm(IOutputMetaData * outRecordSize, byte _kind);
    ~RegexAlgorithm();
    IMPLEMENT_IINTERFACE

    virtual INlpParser * createParser(ICodeContext * ctx, unsigned activityId, INlpHelper * helper, IHThorParseArg * arg);
    virtual void init(IHThorParseArg & arg);

    virtual void serialize(MemoryBuffer & out);
            void deserialize(MemoryBuffer & out);
            void match(RegexState & state);

public:
    unsigned minPatternLength;
    Owned<RegexPattern> grammar;
    Owned<IOutputMetaData> outRecordSize;
    byte kind;
};

#endif /* __THORRPARSE_HPP_ */
