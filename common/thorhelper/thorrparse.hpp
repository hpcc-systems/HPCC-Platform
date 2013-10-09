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

#ifndef __THORRPARSE_HPP_
#define __THORRPARSE_HPP_

#include "thorparse.hpp"

class RegexPattern;
class RegexNamed;
class RegexState;
struct RegexSerializeState;
struct RegexXmlState;

typedef CIArrayOf<RegexPattern> RegexPatternArray;
typedef CIArrayOf<RegexNamed> RegexNamedArray;
typedef CopyCIArrayOf<RegexPattern> RegexPatternCopyArray;


//Used for serialization:
enum ThorRegexKind { ThorRegexNone, 
       ThorRegexNull, ThorRegexAnyChar, ThorRegexAsciiDFA, ThorRegexUnicodeDFA, 
       ThorRegexAscii, ThorRegexAsciiI, ThorRegexAsciiSet, ThorRegexAsciiISet,
       ThorRegexUnicode, ThorRegexUnicodeI, ThorRegexUnicodeSet, ThorRegexUnicodeISet,
       ThorRegexStart, ThorRegexFinish,
       ThorRegexBeginToken, ThorRegexEndToken,
       ThorRegexBeginSeparator, ThorRegexEndSeparator,
       ThorRegexRepeat,     // used for repeats with counts.
       ThorRegexBeginCheck, ThorRegexAssertNext, ThorRegexAssertPrev, 
       ThorRegexCheckLength,
       ThorRegexCheck,
       ThorRegexValidateAscAsAsc, ThorRegexValidateUniAsAsc, ThorRegexValidateAscAsUni, ThorRegexValidateUniAsUni,
       ThorRegexNamed,
       ThorRegexEndNamed,
       ThorRegexEndNested,
       ThorRegexDone,
       ThorRegexRecursive,
       ThorRegexEndRecursive,
       ThorRegexPenalty,
       ThorRegexRepeatAny,
       ThorRegexMax,
       ThorRegexValidateUtf8AsAsc, ThorRegexValidateUtf8AsUni,
       ThorRegexUtf8, ThorRegexUtf8I, 
       
       //Temporary - not serialized...
       ThorRegexRepeatInstance
};

//RegexMatchContinue is processed the same as RegexMatchBacktrack, but returned in different contexts.
enum RegexMatchAction { RegexMatchDone, RegexMatchBacktrack, RegexMatchBacktrackToken, RegexMatchContinue  };

class ActiveStage;
class RegexMatchStateSave;
class THORHELPER_API RegexPattern : public CInterface
{
public:
    RegexPattern();

    virtual ThorRegexKind getKind() = 0;
    virtual RegexMatchAction match(RegexState & state) = 0;

    virtual RegexMatchAction beginMatch(RegexState & state) = 0;
    virtual RegexMatchAction nextAction(ActiveStage & stage, RegexState & state);
    virtual void killStage(ActiveStage & stage, RegexState & state);

//serialization code....
    virtual void dispose(); // needed to free the structure because it is a cyclic graph
    virtual bool gather(RegexSerializeState & state);
    virtual void serializePattern(MemoryBuffer & out);
    virtual void serializeLinks(MemoryBuffer & out, RegexSerializeState & state);
    virtual void deserializePattern(MemoryBuffer & in);
    virtual void deserializeLinks(MemoryBuffer & in, RegexSerializeState & state);

    virtual void toXML(StringBuffer & out, RegexXmlState & state);
    virtual void toXMLattr(StringBuffer & out, RegexXmlState & state);

//construction
    virtual void addLink(RegexPattern * link)               { next.append(OLINK(*link)); }
    virtual void setBody(RegexNamed * name)                 { UNIMPLEMENTED; }
    virtual void setSubPattern(RegexPattern * _pattern)     { UNIMPLEMENTED; }

public:
    RegexMatchAction traceMatch(RegexState & state);
    void gatherNext(RegexSerializeState & state);
    void clearGathered()                                    { gathered = false; }
    virtual void getTraceText(StringBuffer & s);

protected:
    inline RegexMatchAction pushMatched(RegexState & state);

    RegexMatchAction matchNext(RegexState & state);
    inline RegexMatchAction markFinishContinueMatch(RegexState & state);

    RegexMatchAction nextChild(ActiveStage & stage, RegexState & state);
    ActiveStage & pushStage(RegexState & state);
    ActiveStage & pushStageBeginMatch(RegexState & state, RegexMatchStateSave * matched);
    RegexMatchAction pushStageEndMatch(RegexState & state);
    void cleanupBeginMatch(ActiveStage & stage, RegexState & state);
    void cleanupEndMatch(ActiveStage & stage, RegexState & state);

protected:
    RegexPatternArray   next;
    bool gathered;
};
typedef Owned<RegexPattern> OwnedRegexPattern;


class MatchState;
class THORHELPER_API RegexNamed : public CInterface
{
public:
    RegexNamed() { name = NULL; id = 0; }
    RegexNamed(IAtom * _name, regexid_t _id) { name = _name; id = _id; }

    inline IAtom * queryName() { return name; }
    inline regexid_t queryID() { return id; }
    RegexMatchAction match(RegexState & state, RegexPattern * instance);
    RegexMatchAction match(RegexState & state, RegexPattern * instance, MatchState & match);

    RegexMatchAction beginMatch(RegexState & state);

//serialization...
    void dispose(); // needed to free the structure because it is a cyclic graph
    void gather(RegexSerializeState & state);
    void serializePattern(MemoryBuffer & out);
    void serializeLinks(MemoryBuffer & out, RegexSerializeState & state);
    void deserializePattern(MemoryBuffer & in);
    void deserializeLinks(MemoryBuffer & in, RegexSerializeState & state);

    void toXML(StringBuffer & out, RegexXmlState & state);

//construction
    void setFirst(RegexPattern * value)     { first.set(value); }

protected:
    OwnedRegexPattern first;
    IAtom * name;
    regexid_t id;
};


extern THORHELPER_API void serializeRegex(MemoryBuffer & out, RegexPattern * root);
extern THORHELPER_API RegexPattern * deserializeRegex(MemoryBuffer & in);
extern THORHELPER_API void regexToXml(StringBuffer & out, RegexPattern * root, unsigned detail);

extern THORHELPER_API bool isAsciiMatch(unsigned code, unsigned next);
extern THORHELPER_API bool isUnicodeMatch(unsigned code, unsigned next);


#endif /* __THORRPARSE_HPP_ */
