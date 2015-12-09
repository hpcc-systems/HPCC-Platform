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

#ifndef U_OVERRIDE_CXX_ALLOCATION
#define U_OVERRIDE_CXX_ALLOCATION 0 // Enabling this forces all allocation of ICU objects to ICU's heap, but is incompatible with jmemleak
#endif
#include "jliball.hpp"
#include "junicode.hpp"
#include "thorrparse.ipp"
#include "thorregex.hpp"
#include "eclrtl.hpp"
#include "thorcommon.ipp"

#include "unicode/utf.h"
#include "unicode/uchar.h"
#include "unicode/schriter.h"
#include "unicode/coll.h"
#include "unicode/ustring.h"


#ifdef TRACE_REGEX
#define MATCH   traceMatch
#else
#define MATCH   match
#endif

#define TRANSFORM_BEFORE_CREATE_LIMIT           4096                // Over this size, transform into a temp

#define REGEX_VERSION 4

#define REGEXERR_IncompatibleVersion            1000
#define REGEXERR_MatchFailure                   1001

#define REGEXERR_IncompatibleVersion_Text       "Serialized regex library is not compatible, please rebuild workunit"
#define REGEXERR_MatchFailure_Text              "Failed to match pattern: pattern too complicated (offset %d)"

static const char * getPatternText(ThorRegexKind kind)
{
    switch (kind)
    {
    case ThorRegexNull:         return "Null";
    case ThorRegexAnyChar:      return "AnyChar";
    case ThorRegexAsciiDFA:     return "AsciiDFA";
    case ThorRegexUnicodeDFA:   return "UnicodeDFA";
    case ThorRegexAscii:        return "Ascii";
    case ThorRegexAsciiI:       return "AsciiI";
    case ThorRegexAsciiSet:     return "AsciiSet";
    case ThorRegexAsciiISet:    return "AsciiISet";
    case ThorRegexUnicode:      return "Unicode";
    case ThorRegexUnicodeI:     return "UnicodeI";
    case ThorRegexUnicodeSet:   return "UnicodeSet";
    case ThorRegexUnicodeISet:  return "UnicodeISet";
    case ThorRegexUtf8:         return "Utf8";
    case ThorRegexUtf8I:        return "Utf8I";
    case ThorRegexStart:        return "Start";
    case ThorRegexFinish:       return "Finish";
    case ThorRegexBeginToken:   return "BeginToken";
    case ThorRegexEndToken:     return "EndToken";
    case ThorRegexBeginSeparator: return "BeginSeparator";
    case ThorRegexEndSeparator: return "EndSeparator";
    case ThorRegexRepeat:       return "Repeat";
    case ThorRegexBeginCheck:   return "BeginCheck";
    case ThorRegexCheck:        return "Check";
    case ThorRegexCheckLength:  return "CheckLength";
    case ThorRegexAssertNext:   return "AssertNext";
    case ThorRegexAssertPrev:   return "AssertPrev";
    case ThorRegexNamed:        return "Named";
    case ThorRegexEndNamed:     return "EndNamed";
    case ThorRegexEndNested:    return "EndNested";
    case ThorRegexDone:         return "Done";
    case ThorRegexRepeatInstance: return "RepeatInstance";
    case ThorRegexValidateAscAsAsc: return "ValidateAscAsAsc";
    case ThorRegexValidateUniAsAsc: return "ValidateUniAsAsc" ;
    case ThorRegexValidateUtf8AsAsc: return "ValidateUtf8AsAsc" ;
    case ThorRegexValidateAscAsUni: return "ValidateAscAsUni" ;
    case ThorRegexValidateUniAsUni: return "ValidateUniAsUni";
    case ThorRegexValidateUtf8AsUni: return "ValidateUtf8AsUni" ;
    case ThorRegexRecursive:    return "Recursive";
    case ThorRegexEndRecursive: return "EndRecursive";
    case ThorRegexPenalty:      return "Penalty";
    case ThorRegexRepeatAny:    return "RepeatAny";
    }
    UNIMPLEMENTED;
}

static const char * resultText[] = { "Done", "Backtrack", "TokenBack", "Continue" };
#ifdef TRACE_REGEX
static unsigned patternDepth;
#endif

bool isAsciiMatch(unsigned code, unsigned next)
{
    switch (code)
    {
    case RCCalnum: return isalnum(next) != 0;
    case RCCcntrl: return iscntrl(next) != 0;
    case RCClower: return islower(next) != 0;
    case RCCupper: return isupper(next) != 0;
    case RCCspace: return isspace(next) != 0;
    case RCCalpha: return isalpha(next) != 0;
    case RCCdigit: return isdigit(next) != 0;
    case RCCprint: return isprint(next) != 0;
    case RCCblank: return (next == ' ' || next == '\t');            // return isblank(next);
    case RCCgraph: return isgraph(next) != 0;
    case RCCpunct: return ispunct(next) != 0;
    case RCCxdigit: return isxdigit(next) != 0;
    case RCCany:   return true;
    default:
        UNIMPLEMENTED;
    }
}

bool isUnicodeMatch(unsigned code, unsigned next)
{
    switch (code)
    {
    case RCCalnum: return u_isalnum(next) != 0;
    case RCCcntrl: return u_iscntrl(next) != 0;
    case RCClower: return u_islower(next) != 0;
    case RCCupper: return u_isupper(next) != 0;
    case RCCspace: return u_isWhitespace(next) != 0;
    case RCCalpha: return u_isalpha(next) != 0;
    case RCCdigit: return u_isdigit(next) != 0;
    case RCCprint: return u_isprint(next) != 0;
    case RCCblank: return u_isspace(next) != 0 || (next == '\t');
    case RCCgraph: return u_isprint(next) && !u_isspace(next);
    case RCCpunct: return u_isprint(next) && !(u_isspace(next) || u_isalnum(next));
    case RCCxdigit: return (next < 128) && isxdigit(next);          // should be good enough.
    case RCCany:   return true;
    default:
        UNIMPLEMENTED;
    }
}



//Anyone got some better code - this works, but isn't wonderfully efficient.
//works by accessing esp[-4096] and seeing if it dies.
inline void checkStackOverflow(unsigned offset)
{
    try
    {
        doStackProbe();
    }
    catch (...)
    {
        throwError1(REGEXERR_MatchFailure, offset);
    }
}
        


//---------------------------------------------------------------------------

void serializeLink(MemoryBuffer & out, RegexPattern * pattern, RegexSerializeState & state)
{
    unsigned match = state.patterns.find(*pattern);
    assertex(match != NotFound);
    out.append(match);
}

RegexPattern * deserializeLink(MemoryBuffer & in, RegexSerializeState & state)
{
    unsigned match;
    in.read(match);
    return LINK(&state.patterns.item(match));
}

void serializeName(MemoryBuffer & out, RegexNamed * pattern, RegexSerializeState & state)
{
    unsigned match = state.named.find(*pattern);
    assertex(match != NotFound);
    out.append(match);
}

RegexNamed * deserializeName(MemoryBuffer & in, RegexSerializeState & state)
{
    unsigned match;
    in.read(match);
    return LINK(&state.named.item(match));
}

MemoryBuffer & serializeKind(MemoryBuffer & out, ThorRegexKind kind)
{
    return out.append((byte)kind);
}

void deserialize(MemoryBuffer & in, IAtom * & name)
{
    StringAttr x;
    in.read(x);
    name = createAtom(x);
}

//---------------------------------------------------------------------------

static MatchState * findTrailingSeparator(MatchState & matched)
{
    MatchState * child = matched.firstChild;
    while (child)
    {
        //find the last child.
        MatchState * next = child->next;
        while (next)
        {
            child = next;
            next = child->next;
        }
        //If the last child is a separator that matches the last offset of the matched token, adjust the end
        if (child->queryName() == separatorTagAtom)
            return child;
        child = child->firstChild;
    }
    return NULL;
}


static void removeTrailingSeparator(MatchState & matched)
{
    MatchState * child = findTrailingSeparator(matched);
    if (child && child->end == matched.end)
        matched.end = child->start;
}

MatchState * RegexMatchSearchInstance::find(MatchState * top, const NlpMatchPath & path, unsigned depth)
{
    regexid_t id = path.getId(depth);
    do
    {
        if (top->queryID() == id)
        {
            bool matchAny = path.matchAny(depth);
            if (matchAny || (nextIndex == 1))
            {
                if (depth+1 == path.numItems())
                {
                    lastExactMatchDepth = depth+1;
                    return top;
                }

                if (!matchAny)
                {
                    lastExactMatchDepth = depth+1;
                    nextIndex = path.nextExactMatchIndex(depth+1);
                }

                MatchState * ret = NULL;
                unsigned prevExactMatchDepth = lastExactMatchDepth;
                if (top->firstChild)
                    ret = find(top->firstChild, path, depth+1);
                //If must match a child, or one of children had a required match then we have a result
                if (!matchAny || (prevExactMatchDepth != lastExactMatchDepth))
                    return ret;
            }
            else
                nextIndex--;
        }
        else
        {
            if (top->firstChild)
            {
                unsigned prevExactMatchDepth = lastExactMatchDepth;
                MatchState * ret = find(top->firstChild, path, depth);
                //return if matched another level - may have failed to match, or matched completely
                if (prevExactMatchDepth != lastExactMatchDepth)
                    return ret;
            }
        }
        top = top->next;
    } while (top);
    return NULL;
}

IMatchedElement * RegexMatchPath::getMatch(MatchState * top, bool removeTrailingSep) const
{
    RegexMatchSearchInstance search;
    search.lastExactMatchDepth = 0;
    search.nextIndex = nextExactMatchIndex(0);
    MatchState * state = search.find(top, *this, 0);
    if (!state)
        return NULL;
    if (removeTrailingSep)
        removeTrailingSeparator(*state);
    return new CMatchedElement(state);
}

//---------------------------------------------------------------------------

void CRegexMatchedResults::extractResults(MatchState & top, const byte * _in, bool removeTrailingSeparator)
{
    in = _in;
    notMatched.ptr = in;
    ForEachItemIn(idx, def->matchResults)
    {
        ::Release(matched[idx]);
        matched[idx] = ((RegexMatchPath&)def->matchResults.item(idx)).getMatch(&top, removeTrailingSeparator);
        if (!matched[idx]) matched[idx] = LINK(&notMatched);
    }
}

//---------------------------------------------------------------------------

static const char * stateText[] = { "Init", "Follow", "Retry", "Finished", "Repeat", "FollowOnly", "RepeatOnly", "DoneRepeat" };

#ifdef TRACE_REGEX
ActiveStage & ActiveStage::setState(unsigned _state)
{ 
    DBGLOG("%*s[%p]Change state to %s", patternDepth, "", pattern, stateText[_state]);
    state = _state; 
    return *this; 
}
#endif

//---------------------------------------------------------------------------
static void disposeLink(Owned<RegexPattern> & pattern)
{
    if (pattern)
    {
        RegexPattern * cur = pattern.getClear();
        cur->dispose();
        cur->Release();
    }
}


static void disposeName(Owned<RegexNamed> & named)
{
    if (named)
    {
        RegexNamed * cur = named.getClear();
        cur->dispose();
        cur->Release();
    }
}

RegexPattern::RegexPattern()
{
    gathered = false;
}

void RegexPattern::dispose()
{
    while (next.ordinality())
    {
        RegexPattern & cur = next.popGet();
        cur.dispose();
        cur.Release();
    }
}


bool RegexPattern::gather(RegexSerializeState & state)
{
    if (gathered)
        return false;
    gathered = true;
    state.patterns.append(OLINK(*this));
    gatherNext(state);
    return true;
}

void RegexPattern::gatherNext(RegexSerializeState & state)
{
    ForEachItemIn(idx, next)
        next.item(idx).gather(state);
}


void RegexPattern::serializePattern(MemoryBuffer & out)
{
    serializeKind(out, getKind());
}

void RegexPattern::serializeLinks(MemoryBuffer & out, RegexSerializeState & state)
{
    out.append((unsigned)next.ordinality());
    ForEachItemIn(idx, next)
        serializeLink(out, &next.item(idx), state);
}

void RegexPattern::deserializePattern(MemoryBuffer & in)
{
}

void RegexPattern::deserializeLinks(MemoryBuffer & in, RegexSerializeState & state)
{
    unsigned nextCount;
    in.read(nextCount);
    for (unsigned idx=0; idx < nextCount; idx++)
        next.append(*deserializeLink(in, state));
}



RegexMatchAction RegexPattern::matchNext(RegexState & state)
{
    unsigned num = next.ordinality();
    assertex(num);
    if (num == 1)
        return next.item(0).MATCH(state);

    const byte * saved = state.cur;
    for (unsigned idx = 0; idx < num; idx++)
    {
        RegexMatchAction ret = next.item(idx).MATCH(state);
        if (ret != RegexMatchBacktrack)
            return ret;
        state.cur = saved;
    }
    return RegexMatchBacktrack;
}

inline RegexMatchAction RegexPattern::pushMatched(RegexState & state)
{ 
    pushStage(state).setMatched(); 
    return RegexMatchContinue;
}

inline RegexMatchAction RegexPattern::markFinishContinueMatch(RegexState & state)
{
    MatchSaveState saved;
    state.markFinish(saved);
    RegexMatchAction ret = matchNext(state);
    state.unmarkFinish(saved);
    return ret;
}


ActiveStage & RegexPattern::pushStageBeginMatch(RegexState & state, RegexMatchStateSave * matched)
{
    ActiveStage & stage = pushStage(state);
    stage.setMatched();
    stage.extra.matched = matched;
    state.pushMatch(*matched, matched->save);
#ifdef TRACE_REGEX
    DBGLOG("%*s[%p]Push Begin Match", patternDepth, "", stage.pattern);
#endif
    return stage;
}

void RegexPattern::cleanupBeginMatch(ActiveStage & stage, RegexState & state)
{
#ifdef TRACE_REGEX
    DBGLOG("%*s[%p]Pop Begin Match", patternDepth, "", stage.pattern);
#endif
    state.popMatch(stage.extra.matched->save);
    state.cache.destroyStateSave(stage.extra.matched);
    stage.extra.matched = NULL;
}

RegexMatchAction RegexPattern::pushStageEndMatch(RegexState & state)
{
    ActiveStage & stage = pushStage(state);
    stage.setMatched();
    state.markFinish(stage.extra.saved);
#ifdef TRACE_REGEX
    DBGLOG("%*s[%p]Push End Match", patternDepth, "", stage.pattern);
#endif
    return RegexMatchContinue;
}


void RegexPattern::cleanupEndMatch(ActiveStage & stage, RegexState & state)
{
#ifdef TRACE_REGEX
    DBGLOG("%*s[%p]Pop End Match", patternDepth, "", stage.pattern);
#endif
    state.unmarkFinish(stage.extra.saved);
}


RegexMatchAction RegexPattern::traceMatch(RegexState & state)
{
    static unsigned i;
    StringBuffer s,t;
    unsigned len = (size32_t)(state.end-state.cur);
    if (len > 10) len = 10;
    getTraceText(t);
    s.appendN(i++, ' ').appendf("Begin %s [%p] >%.*s<", t.str(), this, len, state.cur);
    DBGLOG("%s", s.str());
    RegexMatchAction ret = match(state);
    s.clear().appendN(--i, ' ').appendf("End   %s [%p] = %s", t.str(), this, resultText[ret]);
    DBGLOG("%s", s.str());
    return ret;
}

void RegexPattern::getTraceText(StringBuffer & s)
{
    s.append(getPatternText(getKind()));
}

void RegexPattern::toXML(StringBuffer & out, RegexXmlState & state)
{
    out.appendf("\t<expr id=\"%d\"", state.patterns.find(*this));
    toXMLattr(out,state);
    if (next.ordinality())
    {
        out.append(">").newline();
        ForEachItemIn(idx, next)
        {
            out.appendf("\t\t<next id=\"%d\"/>", state.patterns.find(next.item(idx))).newline();
        }
        out.append("\t</expr>").newline();
        ForEachItemInRev(idx2, next)
            state.addLink(next.item(idx2));
    }
    else
        out.append("/>").newline();
}

void RegexPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    out.appendf(" kind=\"%s\"", getPatternText(getKind()));
}


void RegexPattern::killStage(ActiveStage & stage, RegexState & state)
{
}

RegexMatchAction RegexPattern::nextChild(ActiveStage & stage, RegexState & state)
{
    if (next.isItem(stage.nextFollow))
    {
        RegexPattern & child = next.item(stage.nextFollow);
        stage.nextFollow++;
        state.cur = stage.followPosition;
        return child.beginMatch(state);
    }
    stage.setState(RSretry);
    return RegexMatchContinue;
}

RegexMatchAction RegexPattern::nextAction(ActiveStage & stage, RegexState & state)
{
    switch (stage.getState())
    {
    case RSinit:
        throwUnexpected();
    case RSnextfollow:
        return nextChild(stage, state);
    case RSfollowonly:
        {
            RegexMatchAction action = nextChild(stage, state);
            if (stage.getState() == RSretry)
                stage.setState(RSfinished);
            return action;
        }
    case RSretry:
    case RSfinished:
        state.popStage();
        return RegexMatchBacktrack;
    }
    UNIMPLEMENTED;
}


ActiveStage & RegexPattern::pushStage(RegexState & state)
{
#ifdef TRACE_REGEX
    StringBuffer t;
    unsigned len = state.end-state.cur;
    if (len > 10) len = 10;
    getTraceText(t);
    DBGLOG("%*s[%p]Push %s >%.*s<", patternDepth++, "", this, t.str(), len, state.cur);
#endif
    ActiveStage & cur = state.pushStage();
    cur.pattern = this;
    cur.followPosition = state.cur;
    cur.state = RSinit;
    cur.flags = 0;
    return cur;
}


//---------------------------------------------------------------------------

RegexMatchAction RegexNullPattern::match(RegexState & state)
{
    return matchNext(state);
}

RegexMatchAction RegexNullPattern::beginMatch(RegexState & state)
{
    return pushMatched(state);
}

RegexMatchAction RegexBeginTokenPattern::match(RegexState & state)
{
    RegexMatchAction ret = matchNext(state);
    if (ret == RegexMatchBacktrackToken)
        return RegexMatchBacktrack;
    return ret;
}

RegexMatchAction RegexBeginTokenPattern::beginMatch(RegexState & state)
{
    ActiveStage & stage = pushStage(state).setMatched();
    stage.flags |= RFbeginToken; 
    return RegexMatchContinue;
}

RegexMatchAction RegexEndTokenPattern::match(RegexState & state)
{
    RegexMatchAction ret = matchNext(state);
    if (ret == RegexMatchBacktrack)
        return RegexMatchBacktrackToken;
    return ret;
}

RegexMatchAction RegexEndTokenPattern::beginMatch(RegexState & state)
{
    return pushMatched(state);
}

RegexMatchAction RegexEndTokenPattern::nextAction(ActiveStage & stage, RegexState & state)
{
    switch (stage.getState())
    {
    case RSretry:
    case RSfinished:
        state.popStage();
        return RegexMatchBacktrackToken;
    default:
        return RegexPattern::nextAction(stage, state);
    }
}

RegexMatchAction RegexStartPattern::match(RegexState & state)
{
    if (state.cur == state.start)
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexStartPattern::beginMatch(RegexState & state)
{
    if (state.cur == state.start)
        return pushMatched(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexFinishPattern::match(RegexState & state)
{
    if (state.cur == state.end)
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexFinishPattern::beginMatch(RegexState & state)
{
    if (state.cur == state.end)
        return pushMatched(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexAnyCharPattern::match(RegexState & state)
{
    if (state.cur != state.end)
    {
        state.cur += state.charSize;
        return matchNext(state);
    }
    return RegexMatchBacktrack;
}

RegexMatchAction RegexAnyCharPattern::beginMatch(RegexState & state)
{
    if (state.cur != state.end)
    {
        state.cur += state.charSize;
        return pushMatched(state);
    }
    return RegexMatchBacktrack;
}

//---------------------------------------------------------------------------

RegexMatchAction RegexPenaltyPattern::match(RegexState & state)
{
    state.score -= penalty;
    RegexMatchAction ret = matchNext(state);
    state.score += penalty;
    return ret;
}

RegexMatchAction RegexPenaltyPattern::beginMatch(RegexState & state)
{
    state.score -= penalty;
    return pushMatched(state);
}

void RegexPenaltyPattern::killStage(ActiveStage & stage, RegexState & state)
{
    state.score += penalty;
}

void RegexPenaltyPattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    out.append(penalty);
}

void RegexPenaltyPattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    in.read(penalty);
}


void RegexPenaltyPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);
    out.append(" penalty=\"").append(penalty).append("\"");
}


//---------------------------------------------------------------------------

void RegexAsciiBasePattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    out.append(text);
}

void RegexAsciiBasePattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    in.read(text);
    len = (size32_t)strlen(text);
}


void RegexAsciiBasePattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);
    out.append(" text=\"");
    encodeXML(text, out, ENCODE_WHITESPACE);
    out.append("\"");
}


inline bool RegexAsciiPattern::doMatch(RegexState & state)
{
    const byte * cur = state.cur;
    const byte * end = state.end;
    if (cur + len <= end)
    {
        if (memcmp(cur, text.get(), len) == 0)
        {
            state.cur = cur + len;
            return true;
        }
    }
    return false;
}

RegexMatchAction RegexAsciiPattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexAsciiPattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

inline bool RegexAsciiIPattern::doMatch(RegexState & state)
{
    const byte * cur = state.cur;
    const byte * end = state.end;
    if (cur + len <= end)
    {
        if (memicmp(cur, text.get(), len) == 0)
        {
            state.cur = cur + len;
            return true;
        }
    }
    return false;
}

RegexMatchAction RegexAsciiIPattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexAsciiIPattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

//---------------------------------------------------------------------------

RegexMatchAction RegexRecursivePattern::match(RegexState & state)
{
    if ((positionStack.ordinality() == 0) || (positionStack.tos() != state.cur))
    {
        positionStack.append(state.cur);
        state.stack.append(*this);
        RegexMatchAction ret = matchNext(state);
        state.stack.pop();
        positionStack.pop();
        return ret;
    }
    else
        throw MakeStringException(0, "Grammar is left recursive - cannot yet process!");
    return RegexMatchBacktrack;
}

RegexMatchAction RegexRecursivePattern::beginMatch(RegexState & state)
{
    //MORE!
    return pushMatched(state);
}


RegexMatchAction RegexEndRecursivePattern::match(RegexState & state)
{
    RegexRecursivePattern & top = (RegexRecursivePattern &)state.stack.popGet();
    const void * saved = top.positionStack.popGet();
    RegexMatchAction ret = matchNext(state);
    top.positionStack.append(saved);
    state.stack.append(top);
    return ret;
}


RegexMatchAction RegexEndRecursivePattern::beginMatch(RegexState & state)
{
    //MORE!
    return pushMatched(state);
}


void encodeUnicode(StringBuffer & out, size32_t len, const UChar * text)
{
    UnicodeString unicode(text, len);
    unsigned len8 = unicode.extract(0, unicode.length(), 0, 0, "UTF-8");
    char * text8 = (char *)malloc(len8);
    unicode.extract(0, unicode.length(), text8, len8, "UTF-8");
    encodeXML(text8, out, ENCODE_WHITESPACE, len8, true);
    free(text8);
}


void RegexUnicodePattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    ::serialize(out, text);
}

void RegexUnicodePattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    ::deserialize(in, text);
}


void RegexUnicodePattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);

    out.appendf(" text=\"");
    encodeUnicode(out, (size32_t)(text.length()/2), (const UChar *)text.get());
    out.append("\"");
}



inline bool RegexUnicodePattern::doMatch(RegexState & state)
{
    const byte * cur = state.cur;
    const byte * end = state.end;
    size32_t len = (size32_t)text.length();
    if (cur + len <= end)
    {
        if (memcmp(cur, text.get(), len) == 0)
        {
            state.cur = cur + len;
            return true;
        }
    }
    return false;
}

RegexMatchAction RegexUnicodePattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexUnicodePattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

//---------------------------------------------------------------------------

RegexUnicodeIPattern::RegexUnicodeIPattern(unsigned _len, const UChar * _text)
{
    UChar * curLower = (UChar *)lower.allocate(_len*2);
    UChar * curUpper = (UChar *)upper.allocate(_len*2);
    for (unsigned i = 0; i< _len; i++)
    {
        curLower[i] = (UChar)u_tolower(_text[i]);
        curUpper[i] = (UChar)u_toupper(_text[i]);
    }

}

void RegexUnicodeIPattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    ::serialize(out, lower);
    ::serialize(out, upper);
}

void RegexUnicodeIPattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    ::deserialize(in, lower);
    ::deserialize(in, upper);
}


void RegexUnicodeIPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);

    out.appendf(" text=\"");
    encodeUnicode(out, (size32_t)(lower.length()/2), (const UChar *)lower.get());
    out.append("\"");
}



inline bool RegexUnicodeIPattern::doMatch(RegexState & state)
{
    const byte * start = state.cur;
    const byte * end = state.end;

    size32_t size = (size32_t)lower.length();
    if (start + size <= end)
    {
        unsigned i;
        unsigned len = size/sizeof(UChar);
        const UChar * cur = (const UChar *)state.cur;
        const UChar * curLower = (const UChar *)lower.get();
        const UChar * curUpper = (const UChar *)upper.get();
        for (i = 0; i < len; i++)
        {
            UChar next = cur[i];
            if ((next != curLower[i]) && (next != curUpper[i]))
                return false;
        }
        state.cur += size;
        return true;
    }
    return false;
}

RegexMatchAction RegexUnicodeIPattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}


RegexMatchAction RegexUnicodeIPattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

RegexUnicodeFIPattern::RegexUnicodeFIPattern(unsigned _len, const UChar * _text)
{
    MemoryBuffer foldedBuffer;

    UChar temp[2];
    UErrorCode error = U_ZERO_ERROR;
    for (unsigned i = 0; i< _len; i++)
    {
        unsigned foldLen = u_strFoldCase(temp, _elements_in(temp), _text+i, 1, U_FOLD_CASE_DEFAULT, &error);
        foldedBuffer.append(foldLen, &temp);
    }
    folded.set(foldedBuffer.length(), foldedBuffer.toByteArray());
}

bool RegexUnicodeFIPattern::doMatch(RegexState & state)
{
    const UChar * cur = (const UChar *)state.cur;
    const UChar * end = (const UChar *)state.end;

    UChar temp[2];
    UErrorCode error = U_ZERO_ERROR;
    const UChar * curMatchText = (const UChar *)folded.get();
    const UChar * endMatchText = (const UChar *)((const byte *)folded.get() + folded.length());
    while (cur != end)
    {
        unsigned foldLen = u_strFoldCase(temp, _elements_in(temp), cur, 1, U_FOLD_CASE_DEFAULT, &error);
        if (temp[0] != *curMatchText)
            return false;
        assertex(foldLen == 1 || foldLen == 2);
        if (foldLen == 2)
        {
            if (curMatchText + foldLen > endMatchText)
                return false;
            if (temp[1] != curMatchText[1])
                return false;
        }
        cur += 1;
        curMatchText += foldLen;
        if (curMatchText == endMatchText)
        {
            state.cur = (const byte *)cur;
            return true;
        }
    }
    return false;
}

RegexMatchAction RegexUnicodeFIPattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}


RegexMatchAction RegexUnicodeFIPattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

//---------------------------------------------------------------------------

void RegexUtf8Pattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    ::serialize(out, text);
}

void RegexUtf8Pattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    ::deserialize(in, text);
}


void RegexUtf8Pattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);

    out.appendf(" text=\"");
    encodeXML((const char *)text.get(), out, ENCODE_WHITESPACE, (size32_t)text.length(), true);
    out.append("\"");
}



inline bool RegexUtf8Pattern::doMatch(RegexState & state)
{
    const byte * cur = state.cur;
    const byte * end = state.end;
    size32_t size = (size32_t)text.length();
    if (cur + size <= end)
    {
        if (memcmp(cur, text.get(), size) == 0)
        {
            state.cur = cur + size;
            return true;
        }
    }
    return false;
}

RegexMatchAction RegexUtf8Pattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexUtf8Pattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

//---------------------------------------------------------------------------

RegexUtf8IPattern::RegexUtf8IPattern(unsigned _len, const char * _text)
{
    //Store unicode lowercase and uppercase versions, and compare the incoming utf a character at a time.
    UChar * curLower = (UChar *)lower.allocate(_len*sizeof(UChar));
    UChar * curUpper = (UChar *)upper.allocate(_len*sizeof(UChar));
    const byte * cur = (const byte *)_text;
    const byte * end = cur + rtlUtf8Size(_len, cur);
    for (unsigned i = 0; i< _len; i++)
    {
        UChar next = readUtf8Character((size32_t)(end-cur), cur);
        curLower[i] = (UChar)u_tolower(next);
        curUpper[i] = (UChar)u_toupper(next);
    }

}

void RegexUtf8IPattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    ::serialize(out, lower);
    ::serialize(out, upper);
}

void RegexUtf8IPattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    ::deserialize(in, lower);
    ::deserialize(in, upper);
}


void RegexUtf8IPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);

    out.appendf(" text=\"");
    encodeUnicode(out, (size32_t)(lower.length()/2), (const UChar *)lower.get());
    out.append("\"");
}



inline bool RegexUtf8IPattern::doMatch(RegexState & state)
{
    const byte * end = state.end;

    size32_t size = (size32_t)lower.length();
    size32_t len = size/(size32_t)sizeof(UChar);
    const byte * cur = state.cur;
    const UChar * curLower = (const UChar *)lower.get();
    const UChar * curUpper = (const UChar *)upper.get();
    for (unsigned i = 0; i < len; i++)
    {
        if (cur == end)
            return false;

        UChar next = readUtf8Character((size32_t)(end-cur), cur);
        if ((next != curLower[i]) && (next != curUpper[i]))
            return false;
    }
    state.cur = cur;
    return true;
}

RegexMatchAction RegexUtf8IPattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}


RegexMatchAction RegexUtf8IPattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

//---------------------------------------------------------------------------

RegexAsciiSetBasePattern::RegexAsciiSetBasePattern()
{
    inverted = false;
    for (unsigned i = 0; i < 256; i++)
        include[i] = inverted;
}


void RegexAsciiSetBasePattern::setInvert(bool newValue)
{
    //can call before or after processing the list.
    if (inverted != newValue)
    {
        for (unsigned i = 0; i < 256; i++)
            include[i] ^= true;
        inverted = newValue;
    }
}

void RegexAsciiSetBasePattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    serializeBoolArray(out, 256, include);
    out.append(inverted);
}

void RegexAsciiSetBasePattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    deserializeBoolArray(256, include, in);
    in.read(inverted);
}


inline bool RegexAsciiSetBasePattern::doMatch(RegexState & state)
{
    const byte * cur = state.cur;
    const byte * end = state.end;
    if (cur < end)
    {
        if (include[*cur])
        {
            state.cur = cur + 1;
            return true;
        }
    }
    return false;
}

RegexMatchAction RegexAsciiSetBasePattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexAsciiSetBasePattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

void RegexAsciiSetBasePattern::addSpecialRange(unsigned value)
{
    for (unsigned i = 0; i < 256; i++)
    {
        if (isAsciiMatch(value, i))
            include[i] = !inverted;
    }
}

void RegexAsciiSetBasePattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexSetBasePattern::toXMLattr(out, state);
    out.append(" set=\"");
    if (inverted)
        out.append("^");
    for (unsigned i = 0; i < 256; i++)
    {
        if (include[i] != inverted)
        {
            char c = i;
            encodeXML(&c, out, ENCODE_WHITESPACE, 1);
        }
    }
    out.append("\"");
}



void RegexAsciiSetPattern::addRange(unsigned low, unsigned high)
{
    if (low & RegexSpecialMask)
    {
        assertex(low == high);
        addSpecialRange(low);
    }
    else
    {
        assertex(low < 256 && high < 256);
        for (unsigned i = low; i <= high; i++)
            include[i] = !inverted;
    }
}

void RegexAsciiISetPattern::addRange(unsigned low, unsigned high)
{
    if (low & RegexSpecialMask)
    {
        assertex(low == high);
        addSpecialRange(low);
    }
    else
    {
        assertex(low < 256 && high < 256);
        for (unsigned i = low; i <= high; i++)
        {
            include[tolower(i)] = !inverted;
            include[toupper(i)] = !inverted;
        }
    }
}

//---------------------------------------------------------------------------

RegexUnicodeSetPattern::RegexUnicodeSetPattern(bool _caseSensitive)
{
    inverted = false;
    caseSensitive = _caseSensitive;
}

void RegexUnicodeSetPattern::addRange(unsigned low, unsigned high)
{
    if (!caseSensitive)
    {
        //This isn't really good enough - probably need to use string for the high and low values to allow collation
        //comparison, and to allow SS to match correctly.
        if (low & RegexSpecialMask)
        {
            if ((low == RCClower) || (low == RCCupper))
                low = RCCalpha;
        }
        else
        {
            low = u_foldCase(low, U_FOLD_CASE_DEFAULT);
            high = u_foldCase(high, U_FOLD_CASE_DEFAULT);
        }
    }
    from.append(low);
    to.append(high);
}

void RegexUnicodeSetPattern::setInvert(bool value)
{
    inverted = value;
}

bool RegexUnicodeSetPattern::doMatch(RegexState & state)
{
    const byte * cur = (const byte *)state.cur;
    const byte * end = (const byte *)state.end;

    if (cur < end)
    {
        UChar next;
        if (state.inputFormat == NlpUnicode)
        {
            next = *(const UChar *)cur;
            cur += sizeof(UChar);
        }
        else
        {
            next = readUtf8Character((size32_t)(end-cur), cur);
        }

        if (!caseSensitive)
            next = (UChar)u_foldCase(next, U_FOLD_CASE_DEFAULT);        // MORE: Doesn't cope with full folding of SS

        ForEachItemIn(idx, from)
        {
            unsigned low = from.item(idx);
            if (low & RegexSpecialMask)
            {
                if (isUnicodeMatch(low, next))
                {
                    if (inverted)
                        return false;
                    state.cur = cur;
                    return true;
                }
            }
            else if ((next >= low) && (next <= to.item(idx)))
            {
                if (inverted)
                    return false;
                state.cur = cur;
                return true;
            }
        }
        if (inverted)
        {
            state.cur = cur;
            return true;
        }
    }
    return false;
}


RegexMatchAction RegexUnicodeSetPattern::match(RegexState & state)
{
    if (doMatch(state))
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexUnicodeSetPattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushMatched(state);
    return RegexMatchBacktrack;
}

void RegexUnicodeSetPattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    out.append(caseSensitive);
    out.append(inverted);
    out.append(from.ordinality());
    ForEachItemIn(idx, from)
    {
        out.append(from.item(idx));
        out.append(to.item(idx));
    }
}

void RegexUnicodeSetPattern::deserializePattern(MemoryBuffer & in)
{
    unsigned count;
    RegexPattern::deserializePattern(in);
    in.read(caseSensitive);
    in.read(inverted);
    in.read(count);
    for (unsigned idx=0; idx < count; idx++)
    {
        unsigned value;
        in.read(value);
        from.append(value);
        in.read(value);
        to.append(value);
    }
}

void RegexUnicodeSetPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexSetBasePattern::toXMLattr(out, state);
    if (state.detail >= 100)
    {
        if (inverted)
            out.append(" inverted");
        out.append(" set=\"");
        ForEachItemIn(idx, from)
        {
            if (idx) 
                out.append(",");
            out.append(from.item(idx));
            if (from.item(idx) != to.item(idx))
                out.append("..").append(to.item(idx));
        }
        out.append("\"");
    }
}




//---------------------------------------------------------------------------

RegexMatchAction RegexDonePattern::match(RegexState & state)
{
    state.top.end = state.cur;
    if (state.matchAction->onMatch(state))
        return RegexMatchDone;
    return RegexMatchBacktrack;
}


RegexMatchAction RegexDonePattern::beginMatch(RegexState & state)
{
    state.top.end = state.cur;
    if (state.matchAction->onMatch(state))
        return RegexMatchDone;
    return RegexMatchBacktrack;
}


//---------------------------------------------------------------------------

RegexMatchAction RegexEndNestedPattern::match(RegexState & state)
{
    RegexPattern & top = state.stack.popGet();
    RegexMatchAction ret = top.MATCH(state);
    state.stack.append(top);
    return ret;
}


RegexMatchAction RegexEndNestedPattern::beginMatch(RegexState & state)
{
    ActiveStage & stage = pushStage(state);
    stage.setState(RSfinished);
    stage.extra.nextPattern = &state.namedStack.popGet();
    return stage.extra.nextPattern->beginMatch(state);
}

void RegexEndNestedPattern::killStage(ActiveStage & stage, RegexState & state)
{
    state.namedStack.append(*stage.extra.nextPattern);
}


//---------------------------------------------------------------------------

void RegexAssertionPattern::dispose()
{
    RegexPattern::dispose();
    disposeLink(pattern);
}


bool RegexAssertionPattern::gather(RegexSerializeState & state)
{
    bool ok = RegexPattern::gather(state);
    if (ok)
        pattern->gather(state);
    return ok;
}

void RegexAssertionPattern::serializeLinks(MemoryBuffer & out, RegexSerializeState & state)
{
    RegexPattern::serializeLinks(out, state);
    serializeLink(out, pattern, state);
}

void RegexAssertionPattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    out.append(invert);
}

void RegexAssertionPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);
    out.appendf(" sub=\"%d\" invert=\"%d\"", state.patterns.find(*pattern), invert);
    state.addLink(*pattern);
}

void RegexAssertionPattern::deserializeLinks(MemoryBuffer & in, RegexSerializeState & state)
{
    RegexPattern::deserializeLinks(in, state);
    pattern.setown(deserializeLink(in, state));
}

void RegexAssertionPattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    in.read(invert);
}

bool RegexAssertNextPattern::nextMatches(RegexState & state)
{
    //Sub function to reduce stack usage
    MatchedAssertionAction matchedFlag;
    RegexState localState(state, &matchedFlag, (size32_t)(state.end - state.cur), state.cur);
    localState.processPattern(pattern);
    return matchedFlag.isMatched();
}


RegexMatchAction RegexAssertNextPattern::match(RegexState & state)
{
    if (nextMatches(state) != invert)
        return matchNext(state);
    return RegexMatchBacktrack;
}


RegexMatchAction RegexAssertNextPattern::beginMatch(RegexState & state)
{
    if (nextMatches(state) != invert)
        return pushMatched(state);
    return RegexMatchBacktrack;
}


bool RegexAssertPrevPattern::prevMatches(RegexState & state)
{
    //Sub function to reduce stack usage
    ExactMatchedAssertionAction matchedFlag;
    //This will work efficiently on fixed size patterns, but its painful for variable length
    //It would work much better if we reversed the pattern, reversed the string, and then matched.
    //I'll implement it when someone complains!

    unsigned sizeBefore = (size32_t)(state.cur - state.start);
    if (sizeBefore >= minSize)
    {
        unsigned maxToCheck = sizeBefore < maxSize ? sizeBefore : maxSize;
        for (unsigned sizeToCheck = minSize; sizeToCheck <= maxToCheck; sizeToCheck += state.charSize)
        {
            //MORE: A test of finish will incorrectly succeed
            RegexState localState(state, &matchedFlag, sizeBefore, state.start);
            localState.cur = state.cur - sizeToCheck;
            localState.processPattern(pattern);
            if (matchedFlag.isMatched())
                return true;
        }
    }
    return false;
}

RegexMatchAction RegexAssertPrevPattern::match(RegexState & state)
{
    if (prevMatches(state) != invert)
        return matchNext(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexAssertPrevPattern::beginMatch(RegexState & state)
{
    if (prevMatches(state) != invert)
        return pushMatched(state);
    return RegexMatchBacktrack;
}

void RegexAssertPrevPattern::serializePattern(MemoryBuffer & out)
{
    RegexAssertionPattern::serializePattern(out);
    out.append(minSize);
    out.append(maxSize);
}

void RegexAssertPrevPattern::deserializePattern(MemoryBuffer & in)
{
    RegexAssertionPattern::deserializePattern(in);
    in.read(minSize);
    in.read(maxSize);
}


void RegexAssertPrevPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexAssertionPattern::toXMLattr(out, state);
    out.appendf(" min=\"%d\" max=\"%d\"", minSize, maxSize);
}


//---------------------------------------------------------------------------

RegexMatchAction RegexBeginCheckPattern::match(RegexState & state)
{
    MatchState matched;
    MatchSaveState saved;

    state.pushMatch(matched, saved);
    RegexMatchAction ret = matchNext(state);
    state.popMatch(saved);
    return ret;
}


RegexMatchAction RegexBeginCheckPattern::beginMatch(RegexState & state)
{
    RegexMatchStateSave * matched = state.cache.createStateSave(NULL, 0);
    pushStageBeginMatch(state, matched);
    return RegexMatchContinue;
}

void RegexBeginCheckPattern::killStage(ActiveStage & stage, RegexState & state)
{
    cleanupBeginMatch(stage, state);
}




void RegexCheckPattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    out.append(invert);
    out.append(stripSeparator);
}

void RegexCheckPattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    in.read(invert);
    in.read(stripSeparator);
}

inline const byte * RegexCheckPattern::getEndMatch(RegexState & state)
{
    const byte * end = state.cur;
    if (stripSeparator)
    {
        MatchState * child = findTrailingSeparator(*state.curMatch);
        if (child && child->end == end)
            end = child->start;
    }
    return end;
}



RegexMatchAction RegexCheckInPattern::match(RegexState & state)
{
    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    MatchedCheckAction matchedFlag;
    RegexState localState(state, &matchedFlag, (size32_t)(end - start), start);
    pattern->MATCH(localState);

    if (matchedFlag.isMatched() ^ invert)
        return markFinishContinueMatch(state);
    return RegexMatchBacktrack;
}


RegexMatchAction RegexCheckInPattern::beginMatch(RegexState & state)
{
    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    MatchedCheckAction matchedFlag;
    RegexState localState(state, &matchedFlag, (size32_t)(end - start), start);
    localState.helper = state.helper;
    localState.processPattern(pattern);

    if (matchedFlag.isMatched() ^ invert)
        return pushStageEndMatch(state);
    return RegexMatchBacktrack;
}

void RegexCheckInPattern::killStage(ActiveStage & stage, RegexState & state)
{
    cleanupEndMatch(stage, state);
}

void RegexCheckInPattern::dispose()
{
    RegexCheckPattern::dispose();
    disposeLink(pattern);
}


bool RegexCheckInPattern::gather(RegexSerializeState & state)
{
    bool ok = RegexCheckPattern::gather(state);
    if (ok)
        pattern->gather(state);
    return ok;
}

void RegexCheckInPattern::serializeLinks(MemoryBuffer & out, RegexSerializeState & state)
{
    RegexCheckPattern::serializeLinks(out, state);
    serializeLink(out, pattern, state);
}

void RegexCheckInPattern::deserializeLinks(MemoryBuffer & in, RegexSerializeState & state)
{
    RegexCheckPattern::deserializeLinks(in, state);
    pattern.setown(deserializeLink(in, state));
}


void RegexCheckInPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexCheckPattern::toXMLattr(out, state);
    out.appendf(" sub=\"%d\"", state.patterns.find(*pattern));
    state.addLink(*pattern);
}


inline bool RegexCheckLengthPattern::doMatch(RegexState & state)
{
    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    unsigned matchSize = (size32_t)(end - start);
    unsigned matchLength;
    if (state.inputFormat == NlpUtf8)
        matchLength = rtlUtf8Length(matchSize, start);
    else
        matchLength = matchSize;

    bool inRange = ((matchLength >= minExpectedBytes) && (matchLength <= maxExpectedBytes));
    return (inRange != invert);
}

RegexMatchAction RegexCheckLengthPattern::match(RegexState & state)
{
    if (doMatch(state))
        return markFinishContinueMatch(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexCheckLengthPattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushStageEndMatch(state);
    return RegexMatchBacktrack;
}

void RegexCheckLengthPattern::killStage(ActiveStage & stage, RegexState & state)
{
    cleanupEndMatch(stage, state);
}

void RegexCheckLengthPattern::serializePattern(MemoryBuffer & out)
{
    RegexCheckPattern::serializePattern(out);
    out.append(minExpectedBytes);
    out.append(maxExpectedBytes);
}

void RegexCheckLengthPattern::deserializePattern(MemoryBuffer & in)
{
    RegexCheckPattern::deserializePattern(in);
    in.read(minExpectedBytes);
    in.read(maxExpectedBytes);
}

void RegexCheckLengthPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexCheckPattern::toXMLattr(out, state);
    out.appendf(" expected=\"%d..%d\"", minExpectedBytes, maxExpectedBytes);
}

//---------------------------------------------------------------------------

void RegexValidatePattern::serializePattern(MemoryBuffer & out)
{
    RegexCheckPattern::serializePattern(out);
    out.append(validatorIndex);
}

void RegexValidatePattern::deserializePattern(MemoryBuffer & in)
{
    RegexCheckPattern::deserializePattern(in);
    in.read(validatorIndex);
}

void RegexValidatePattern::deserializeLinks(MemoryBuffer & in, RegexSerializeState & state)
{
    RegexCheckPattern::deserializeLinks(in, state);
}

void RegexValidatePattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexCheckPattern::toXMLattr(out, state);
    out.appendf(" validator=\"%d\"", validatorIndex);
}


RegexMatchAction RegexValidatePattern::match(RegexState & state)
{
    if (doMatch(state))
        return markFinishContinueMatch(state);
    return RegexMatchBacktrack;
}

RegexMatchAction RegexValidatePattern::beginMatch(RegexState & state)
{
    if (doMatch(state))
        return pushStageEndMatch(state);
    return RegexMatchBacktrack;
}

void RegexValidatePattern::killStage(ActiveStage & stage, RegexState & state)
{
    cleanupEndMatch(stage, state);
}


bool RegexValidateAscAsAscPattern::doMatch(RegexState & state)
{
    IStringValidator * validator = (IStringValidator *)state.helper->queryValidator(validatorIndex);
    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    unsigned matchSize = (size32_t)(end - start);
    return validator->isValid(matchSize, (const char *)start);
}

bool RegexValidateUniAsAscPattern::doMatch(RegexState & state)
{
    IStringValidator * validator = (IStringValidator *)state.helper->queryValidator(validatorIndex);

    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    size32_t matchSize = (size32_t)(end - start);

    unsigned asciiLen;
    char * asciiText;
    rtlUnicodeToStrX(asciiLen, asciiText, matchSize/2, (const UChar *)start);
    bool ok = validator->isValid(asciiLen, asciiText);
    rtlFree(asciiText);
    return ok;
}

bool RegexValidateUtf8AsAscPattern::doMatch(RegexState & state)
{
    IStringValidator * validator = (IStringValidator *)state.helper->queryValidator(validatorIndex);

    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    unsigned matchSize = (size32_t)(end - start);

    unsigned asciiLen;
    char * asciiText;
    rtlUnicodeToStrX(asciiLen, asciiText, rtlUtf8Length(matchSize, start), (const UChar *)start);
    bool ok = validator->isValid(asciiLen, asciiText);
    rtlFree(asciiText);
    return ok;
}

bool RegexValidateAscAsUniPattern::doMatch(RegexState & state)
{
    IUnicodeValidator * validator = (IUnicodeValidator *)state.helper->queryValidator(validatorIndex);

    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    unsigned matchSize = (size32_t)(end - start);

    unsigned unicodeLen;
    UChar * unicodeText;
    rtlStrToUnicodeX(unicodeLen, unicodeText, matchSize, (const char *)start);
    bool ok = validator->isValid(unicodeLen, unicodeText);
    rtlFree(unicodeText);
    return ok;
}

bool RegexValidateUniAsUniPattern::doMatch(RegexState & state)
{
    IUnicodeValidator * validator = (IUnicodeValidator *)state.helper->queryValidator(validatorIndex);

    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    unsigned matchSize = (size32_t)(end - start);
    return (validator->isValid(matchSize/2, (const UChar *)start));
}

bool RegexValidateUtf8AsUniPattern::doMatch(RegexState & state)
{
    IUnicodeValidator * validator = (IUnicodeValidator *)state.helper->queryValidator(validatorIndex);

    const byte * start = state.curMatch->start;
    const byte * end = getEndMatch(state);
    unsigned matchSize = (size32_t)(end - start);

    unsigned unicodeLen;
    UChar * unicodeText;
    rtlStrToUnicodeX(unicodeLen, unicodeText, rtlUtf8Length(matchSize, start), (const char *)start);
    bool ok = validator->isValid(unicodeLen, unicodeText);
    rtlFree(unicodeText);
    return ok;
}

//---------------------------------------------------------------------------

void RegexNamedPattern::getTraceText(StringBuffer & s)
{
    s.append(getPatternText(getKind()));
    if (def->queryName())
        s.append(" ").append(def->queryName());
}

RegexMatchAction RegexNamedPattern::match(RegexState & state)
{
#ifdef __64BIT__
    // 64-bit systems have more space for stack
    RegexMatchState matched(def);
    return def->match(state, &end, matched);
#else
    //Allocate on the heap to make a stack fault less likely
    RegexMatchState * matched = state.cache.createState(def);
    RegexMatchAction ret = def->match(state, &end, *matched);
    state.cache.destroyState(matched);
    return ret;
#endif
}

void RegexNamedPattern::dispose()
{
    RegexPattern::dispose();
    disposeName(def);
}


bool RegexNamedPattern::gather(RegexSerializeState & state)
{
    bool ok = RegexPattern::gather(state);
    if (ok)
        def->gather(state);
    return ok;
}

void RegexNamedPattern::serializeLinks(MemoryBuffer & out, RegexSerializeState & state)
{
    RegexPattern::serializeLinks(out, state);
    serializeName(out, def, state);
}

void RegexNamedPattern::deserializeLinks(MemoryBuffer & in, RegexSerializeState & state)
{
    RegexPattern::deserializeLinks(in, state);
    def.setown(deserializeName(in, state));
}

void RegexNamedPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);
    if (def->queryName())
        out.appendf(" name=\"%s\"", def->queryName()->queryStr());
    out.appendf(" body=\"%d\"", state.named.find(*def));
}



RegexMatchAction RegexNamedPattern::RegexEndNamedPattern::match(RegexState & state)
{
    MatchSaveState saved;
    state.markFinish(saved);
    RegexMatchAction ret = named->matchNext(state);
    state.unmarkFinish(saved);
    return ret;
}


RegexMatchAction RegexNamedPattern::RegexEndNamedPattern::beginMatch(RegexState & state)
{
    return pushStageEndMatch(state);
}

RegexMatchAction RegexNamedPattern::RegexEndNamedPattern::nextAction(ActiveStage & stage, RegexState & state)
{
    switch (stage.getState())
    {
    case RSnextfollow:
        return named->nextChild(stage, state);
    default:
        return RegexPattern::nextAction(stage, state);
    }
}

void RegexNamedPattern::RegexEndNamedPattern::killStage(ActiveStage & stage, RegexState & state)
{
    cleanupEndMatch(stage, state);
}



RegexMatchAction RegexNamedPattern::beginMatch(RegexState & state)
{
    RegexMatchStateSave * matched = state.cache.createStateSave(def);
    ActiveStage & stage = pushStageBeginMatch(state, matched);
    stage.setState(RSfinished);                 // so children don't get processed.
    state.namedStack.append(end);

    return def->beginMatch(state);
}

void RegexNamedPattern::killStage(ActiveStage & stage, RegexState & state)
{
    cleanupBeginMatch(stage, state);
    state.namedStack.pop();
}



//---------------------------------------------------------------------------

RegexMatchAction RegexBeginSeparatorPattern::match(RegexState & state)
{
    MatchState matched(separatorTagAtom, 0);
    MatchSaveState saved;

    state.pushMatch(matched, saved);
    RegexMatchAction ret = matchNext(state);
    state.popMatch(saved);
    return ret;
}


RegexMatchAction RegexBeginSeparatorPattern::beginMatch(RegexState & state)
{
    RegexMatchStateSave * matched = state.cache.createStateSave(separatorTagAtom, 0);
    pushStageBeginMatch(state, matched);
    return RegexMatchContinue;
}

void RegexBeginSeparatorPattern::killStage(ActiveStage & stage, RegexState & state)
{
    cleanupBeginMatch(stage, state);
}


RegexMatchAction RegexEndSeparatorPattern::match(RegexState & state)
{
    return mapAction(markFinishContinueMatch(state));
}


RegexMatchAction RegexEndSeparatorPattern::beginMatch(RegexState & state)
{
    return mapAction(pushStageEndMatch(state));
}

RegexMatchAction RegexEndSeparatorPattern::nextAction(ActiveStage & stage, RegexState & state)
{
    return mapAction(RegexPattern::nextAction(stage, state));
}

void RegexEndSeparatorPattern::killStage(ActiveStage & stage, RegexState & state)
{
    cleanupEndMatch(stage, state);
}


//---------------------------------------------------------------------------

RegexMatchAction RegexRepeatPattern::match(RegexState & state)
{
    return match(state, minLimit, maxLimit);
}

RegexMatchAction RegexRepeatPattern::matchNextInstance(RegexState & state, unsigned curMin, unsigned curMax)
{
    Owned<RegexRepeatInstance> instance = new RegexRepeatInstance(this, curMin ? curMin-1 : 0, curMax-1);
    return def->match(state, instance);
}



RegexMatchAction RegexRepeatPattern::match(RegexState & state, unsigned curMin, unsigned curMax)
{
    if (curMax == 0)
        return matchNext(state);

    checkStackOverflow((size32_t)(state.cur-state.start));
    Owned<RegexRepeatInstance> instance = new RegexRepeatInstance(this, curMin ? curMin-1 : 0, curMax-1);
    if (curMin != 0)
        return def->match(state, instance);

    const byte * saved = state.cur;
    if (greedy) 
    {
        RegexMatchAction ret = def->match(state, instance);
        if (ret != RegexMatchBacktrack)
            return ret;
        state.cur = saved;
        return matchNext(state);
    }
    else
    {
        RegexMatchAction ret = matchNext(state);
        if (ret != RegexMatchBacktrack)
            return ret;
        state.cur = saved;
        return def->match(state, instance);
    }
}


void RegexRepeatPattern::dispose()
{
    RegexPattern::dispose();
    disposeName(def);
}


bool RegexRepeatPattern::gather(RegexSerializeState & state)
{
    bool ok = RegexPattern::gather(state);
    if (ok)
        def->gather(state);
    return ok;
}

void RegexRepeatPattern::serializePattern(MemoryBuffer & out)
{
    RegexPattern::serializePattern(out);
    out.append(minLimit);
    out.append(maxLimit);
    out.append(greedy);
}

void RegexRepeatPattern::serializeLinks(MemoryBuffer & out, RegexSerializeState & state)
{
    RegexPattern::serializeLinks(out, state);
    serializeName(out, def, state);
}

void RegexRepeatPattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    in.read(minLimit);
    in.read(maxLimit);
    in.read(greedy);
}


void RegexRepeatPattern::deserializeLinks(MemoryBuffer & in, RegexSerializeState & state)
{
    RegexPattern::deserializeLinks(in, state);
    def.setown(deserializeName(in, state));
}


void RegexRepeatPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);
    out.appendf(" min=\"%d\" max=\"%d\" greedy=\"%d\" body=\"%d\"", minLimit, maxLimit, greedy, state.named.find(*def));
}


RegexMatchAction RegexRepeatInstance::match(RegexState & state)
{
    return def->match(state, minLimit, maxLimit);
}

//--- stack friedly implementation

RegexMatchAction RegexRepeatPattern::beginMatch(RegexState & state)
{
    return beginMatch(state, minLimit, maxLimit);
}

RegexMatchAction RegexRepeatPattern::beginMatchNextInstance(RegexState & state, unsigned curMin, unsigned curMax)
{
    Owned<RegexRepeatInstance> instance = new RegexRepeatInstance(this, curMin ? curMin-1 : 0, curMax-1);
    return def->match(state, instance);
}



RegexMatchAction RegexRepeatPattern::beginMatch(RegexState & state, unsigned curMin, unsigned curMax)
{
    ActiveStage & stage = pushStage(state);
    stage.nextFollow = 0;

    if (curMax == 0)
    {
        stage.extra.repeatInstance = NULL;
        stage.setState(RSfollowonly);
        return RegexMatchContinue;
    }

    RegexRepeatInstance * instance = new RegexRepeatInstance(this, curMin ? curMin-1 : 0, curMax-1);
    stage.extra.repeatInstance = instance;
    if (curMin != 0)
        stage.setState(RSrepeatonly);
    else if (greedy)
        stage.setState(RSrepeat);
    else
        stage.setState(RSnextfollow);
    return RegexMatchContinue;
}


void RegexRepeatPattern::killStage(ActiveStage & stage, RegexState & state)
{
    if (state.namedStack.ordinality() && stage.extra.repeatInstance)
    {
        if (&state.namedStack.tos() == stage.extra.repeatInstance)
            state.namedStack.pop();
    }
    delete stage.extra.repeatInstance;
}


inline RegexMatchAction RegexRepeatPattern::nextRepeatMatch(ActiveStage & stage, RegexState & state)
{
    state.namedStack.append(*stage.extra.repeatInstance);
    return def->beginMatch(state);
}

RegexMatchAction RegexRepeatPattern::nextAction(ActiveStage & stage, RegexState & state)
{
    switch (stage.getState())
    {
    case RSretry:
        assertex(!greedy);
        state.cur = stage.followPosition;
        //fall through to repeat-only code.
    case RSrepeatonly:
        stage.setState(RSfinished);
        return nextRepeatMatch(stage, state);
    case RSrepeat:
        stage.setState(RSdonerepeat);           // ensure namedStack is popped before processing children
        stage.nextFollow = 0;
        return nextRepeatMatch(stage, state);
    case RSdonerepeat:
        state.namedStack.pop();
        stage.setState(RSfollowonly);
        stage.nextFollow = 0;
        return RegexPattern::nextAction(stage, state);
    default:
        return RegexPattern::nextAction(stage, state);
    }
}


RegexMatchAction RegexRepeatInstance::beginMatch(RegexState & state)
{
    return def->beginMatch(state, minLimit, maxLimit);
}


//---------------------------------------------------------------------------

static const int MultiAcceptMask = 0x80000000;

AsciiDfa::AsciiDfa()
{
    numStates = 0;
    numTransitions = 0;
    numAccepts = 0;
    states = NULL;
    transitions = NULL;
    accepts = NULL;
}


AsciiDfa::~AsciiDfa()
{
    delete [] states;
    free(transitions);
    free(accepts);
}


void AsciiDfa::init(unsigned _numStates)
{
    numStates = _numStates;
    states = new AsciiDfaState[numStates];
    memset(states, 0, sizeof(*states)*numStates);
}


unsigned AsciiDfa::getAccepts(const AsciiDfaState & state, unsigned idx) const
{
    unsigned acceptId = state.acceptID;
    if (acceptId == NotFound) return NotFound;
    if (acceptId & MultiAcceptMask)
    {
        unsigned * values = accepts + (acceptId & ~MultiAcceptMask);
        return values[idx];
    }
    else
        return (idx == 0) ? acceptId : NotFound;
}

void AsciiDfa::serialize(MemoryBuffer & out)
{ 
    out.append(numStates).append(numTransitions).append(numAccepts);
    out.append(numStates * sizeof(*states), states);
    out.append(numTransitions * sizeof(*transitions), transitions);
    out.append(numAccepts * sizeof(*accepts), accepts);
}

void AsciiDfa::deserialize(MemoryBuffer & in)
{
    in.read(numStates).read(numTransitions).read(numAccepts);
    states = new AsciiDfaState[numStates];
    transitions = (unsigned *)malloc(numTransitions * sizeof(*transitions));
    accepts = (unsigned *)malloc(numAccepts * sizeof(*accepts));
    in.read(numStates * sizeof(*states), states);
    in.read(numTransitions * sizeof(*transitions), transitions);
    in.read(numAccepts * sizeof(*accepts), accepts);
}

void AsciiDfa::setEmpty()
{
    init(1);
    states[0].delta = 0;
    states[0].min = 255;
    states[0].max = 0;
    states[0].acceptID = NotFound;
}


void AsciiDfa::toXML(StringBuffer & out, unsigned detail)
{
    out.appendf(" numStates=\"%d\" numTransitions=\"%d\"", numStates, numTransitions);

    if (detail > 100)
    {
        out.newline();
        for (unsigned i=0; i < numStates; i++)
        {
            unsigned min = states[i].min;
            unsigned max = states[i].max;
            out.append("\t\t").append(i).append(": ").append(min).append("..").append(max);
            if (states[i].accepts())
            {
                out.append("  Accepts:");
                for (unsigned k = 0;;k++)
                {
                    unsigned id = getAccepts(states[i], k);
                    if (id == NotFound) break;
                    out.append(" ").append(id);
                }
            }
            for (unsigned j = min; j<= max; j++)
            {
                if (((j-min) & 15)==0)
                    out.newline().append("\t\t\t");
                out.append(j).append("->").append(transitions[states[i].delta + j]).append(' ');
            }
            out.newline();
        }
    }
}


AsciiDfaBuilder::AsciiDfaBuilder(AsciiDfa & _dfa) : dfa(_dfa)
{
    maxTransitions = 0;
}

void AsciiDfaBuilder::addTransition(unsigned next, unsigned nextState)
{
    if (dfa.states[curState].min > next)
        dfa.states[curState].min = next;
    if (dfa.states[curState].max < next)
        dfa.states[curState].max = next;
    dfa.transitions[firstTransition+next] = nextState;
}

void AsciiDfaBuilder::init(unsigned _numStates)
{
    dfa.init(_numStates);
}


void AsciiDfaBuilder::init(unsigned _numStates, unsigned approxTransitions)
{
    dfa.init(_numStates);
    reallocTransitions(approxTransitions+256*2);
}

void AsciiDfaBuilder::reallocTransitions(unsigned required)
{
    if (required > maxTransitions)
    {
        if (maxTransitions < 0x1000)
            maxTransitions = 0x1000;
        while (maxTransitions < required)
            maxTransitions += 0x1000;
        dfa.transitions = (unsigned *)realloc(dfa.transitions, maxTransitions * sizeof(*dfa.transitions));
    }
}


void AsciiDfaBuilder::beginState(unsigned id)
{
    curState = id;
    dfa.states[curState].min = 255;
    dfa.states[curState].max = 0;
    dfa.states[curState].acceptID = NotFound;
    firstTransition = dfa.numTransitions;

    //ensure there are enough transitions...
    if (firstTransition + 256 > maxTransitions)
    {
        reallocTransitions(firstTransition + 256 );
        assertex(dfa.transitions);
    }
    for (unsigned i =0; i < 256; i++)
        dfa.transitions[firstTransition+i] = NotFound;
}

void AsciiDfaBuilder::setStateAccept(unsigned id)
{
    AsciiDfaState & cur = dfa.states[curState];
    assertex((id & MultiAcceptMask) == 0);
    if (cur.acceptID != NotFound)
    {
        if (!(cur.acceptID & MultiAcceptMask))
        {
            accepts.append(cur.acceptID);
            cur.acceptID = (accepts.ordinality()-1)|MultiAcceptMask;
        }
        accepts.append(id);
    }
    else
        cur.acceptID = id;
}


void AsciiDfaBuilder::endState()
{
    AsciiDfaState & cur = dfa.states[curState];
    if ((cur.acceptID != NotFound) && (cur.acceptID & MultiAcceptMask))
        accepts.append(NotFound);
    unsigned minEntry = cur.min;
    unsigned maxEntry = cur.max;
    if (minEntry <= maxEntry)
    {
        for (unsigned i = minEntry; i <= maxEntry; i++)
            dfa.transitions[firstTransition+i-minEntry] = dfa.transitions[firstTransition+i];
        cur.delta = firstTransition-minEntry;
        dfa.numTransitions += (maxEntry - minEntry + 1);
    }
    else
        cur.delta = 0;  // no transitions from this state.
}

void AsciiDfaBuilder::finished()
{
    unsigned numAccepts = accepts.ordinality();
    dfa.numAccepts = numAccepts;
    dfa.accepts = (unsigned *)malloc(sizeof(unsigned)*numAccepts);
    memcpy(dfa.accepts, accepts.getArray(), numAccepts*sizeof(unsigned));
}
//---------------------------------------------------------------------------

RegexAsciiDfaPattern::RegexAsciiDfaPattern(bool _matchesToken)
{
    matchesToken = _matchesToken;
}


RegexMatchAction RegexAsciiDfaPattern::match(RegexState & state)
{
    const byte * cur = state.cur;
    const byte * end = state.end;
    unsigned activeState = 0;
    const AsciiDfaState * states = dfa.queryStates();
    unsigned * transitions = dfa.queryTransitions();
    if (matchesToken)
    {
        //MORE: Store only one because we know we could never be backtracked - e.g., because this is always a token.
        const byte * best = NULL;
        loop
        {
            if (states[activeState].accepts())
                best = cur;
            if (cur == end)
                break;
            byte next = *cur++;
            if (next < states[activeState].min)
                break;
            if (next > states[activeState].max)
                break;
            activeState = transitions[states[activeState].delta + next];
            if (activeState == NotFound)
                break;
        }

        if (best)
        {
            state.cur = best;
            RegexMatchAction ret = matchNext(state);
            if (ret == RegexMatchBacktrackToken)
                ret = RegexMatchBacktrack;
            return ret;
        }
        return RegexMatchBacktrack;
    }
    else
    {
        ConstPointerArray & potentialMatches = state.cache.potentialMatches;
        unsigned prevPotentialMatches = potentialMatches.ordinality();
        loop
        {
            if (states[activeState].accepts())
                potentialMatches.append(cur);
            if (cur == end)
                break;
            byte next = *cur++;
            if (next < states[activeState].min)
                break;
            if (next > states[activeState].max)
                break;
            activeState = transitions[states[activeState].delta + next];
            if (activeState == NotFound)
                break;
        }

        while (potentialMatches.ordinality() > prevPotentialMatches)
        {
            state.cur = (const byte *)potentialMatches.popGet();
            RegexMatchAction ret = matchNext(state);
            if (ret != RegexMatchBacktrack)
            {
                potentialMatches.trunc(prevPotentialMatches);
                return ret;
            }
        }
        return RegexMatchBacktrack;
    }
}


void RegexAsciiDfaPattern::serializePattern(MemoryBuffer & out)
{ 
    RegexDfaPattern::serializePattern(out);
    dfa.serialize(out);
    out.append(matchesToken);
}

void RegexAsciiDfaPattern::deserializePattern(MemoryBuffer & in)
{
    RegexDfaPattern::deserializePattern(in);
    dfa.deserialize(in);
    in.read(matchesToken);
}

void RegexAsciiDfaPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexDfaPattern::toXMLattr(out, state);
    dfa.toXML(out, state.detail);
    if (matchesToken)
        out.append(" token");
}

void RegexAsciiDfaPattern::killStage(ActiveStage & stage, RegexState & state)
{
    ConstPointerArray & potentialMatches = state.cache.potentialMatches;
    unsigned prevPotentialMatches = stage.extra.prevPotentialMatches;
    potentialMatches.trunc(prevPotentialMatches);
}

RegexMatchAction RegexAsciiDfaPattern::beginMatch(RegexState & state)
{
    const byte * cur = state.cur;
    const byte * end = state.end;
    unsigned activeState = 0;
    const AsciiDfaState * states = dfa.queryStates();
    unsigned * transitions = dfa.queryTransitions();
    const byte * best = NULL;
    ConstPointerArray & potentialMatches = state.cache.potentialMatches;
    const unsigned prevPotentialMatches = potentialMatches.ordinality();
    loop
    {
        if (states[activeState].accepts())
        {
            if (!best || matchesToken)
                best = cur;
            else
            {
                if (prevPotentialMatches == potentialMatches.ordinality())
                    potentialMatches.append(best);
                potentialMatches.append(cur);
            }
        }
        if (cur == end)
            break;
        byte next = *cur++;
        if (next < states[activeState].min)
            break;
        if (next > states[activeState].max)
            break;
        activeState = transitions[states[activeState].delta + next];
        if (activeState == NotFound)
            break;
    }

    if (!best)
        return RegexMatchBacktrack;

    ActiveStage & stage = pushStage(state);
    stage.extra.prevPotentialMatches = prevPotentialMatches;
    if (matchesToken)
        stage.flags |= RFbeginToken; 

    //Only a single match, therefore no need to backtrack.
    if (prevPotentialMatches == potentialMatches.ordinality())
    {
        stage.followPosition = best;
        stage.setMatched();
    }
    else
    {
        stage.setState(RSretry);
    }
    return RegexMatchContinue;
}

RegexMatchAction RegexAsciiDfaPattern::nextAction(ActiveStage & stage, RegexState & state)
{
    ConstPointerArray & potentialMatches = state.cache.potentialMatches;
    unsigned prevPotentialMatches = stage.extra.prevPotentialMatches;
    assertex(prevPotentialMatches <= potentialMatches.ordinality());
    switch (stage.getState())
    {
    case RSretry:
        {
            if (prevPotentialMatches < potentialMatches.ordinality())
            {
                stage.followPosition = (const byte *)potentialMatches.popGet();
                stage.setMatched();
                return RegexMatchContinue;
            }
            return RegexPattern::nextAction(stage, state);
        }
    default:
        return RegexPattern::nextAction(stage, state);
    }
}

//---------------------------------------------------------------------------

RegexUnicodeDfaPattern::RegexUnicodeDfaPattern()
{
}


RegexUnicodeDfaPattern::~RegexUnicodeDfaPattern()
{
}


RegexMatchAction RegexUnicodeDfaPattern::match(RegexState & state)
{
    /*
    const byte * cur = state.cur;
    const byte * end = state.end;
    unsigned activeState = 0;
    ConstPointerArray matches;
    loop
    {
        //MORE: It would be better to store only one if we knew we could never be backtracked - e.g., if this was always a token.
        if (states[activeState].accepts)
            matches.append(cur);
        if (cur == end)
            break;
        byte next = *cur++;
        if (next < states[activeState].min)
            break;
        if (next > states[activeState].max)
            break;
        activeState = transitions[states[activeState].delta + next];
    }

    while (matches.ordinality())
    {
        state.cur = (const byte *)matches.tos();
        matches.pop();
        RegexMatchAction ret = matchNext(state);
        if (ret != RegexMatchBacktrack)
            return ret;
    }
    */
    return RegexMatchBacktrack;
}


RegexMatchAction RegexUnicodeDfaPattern::beginMatch(RegexState & state)
{
    UNIMPLEMENTED;
    return RegexMatchBacktrack;
}


void RegexUnicodeDfaPattern::serializePattern(MemoryBuffer & out)
{ 
}

void RegexUnicodeDfaPattern::deserializePattern(MemoryBuffer & in)
{
}

//---------------------------------------------------------------------------

RegexRepeatAnyPattern::RegexRepeatAnyPattern(unsigned _low, unsigned _high, bool _minimal)
{
    low = _low;
    high = _high;
    minimal = _minimal;
}


RegexMatchAction RegexRepeatAnyPattern::match(RegexState & state)
{
    unsigned max = (size32_t)(state.end - state.cur);
    if (max > high)
        max = high;
    const byte * start = state.cur;
    if (low > max)
        return RegexMatchBacktrack;

    if (minimal)
    {
        for (unsigned i = low; i <= max; i++)
        {
            state.cur = start+i;
            RegexMatchAction ret = matchNext(state);
            if (ret != RegexMatchBacktrack)
                return ret;
        }
        return RegexMatchBacktrack;
    }
    else
    {
        unsigned i = max;
        loop
        {
            state.cur = start+i;
            RegexMatchAction ret = matchNext(state);
            if (ret != RegexMatchBacktrack)
                return ret;
            if (i == low)
                return RegexMatchBacktrack;
            i--;
        }
    }
}


RegexMatchAction RegexRepeatAnyPattern::beginMatch(RegexState & state)
{
    unsigned max = (size32_t)(state.end - state.cur);
    if (max > high)
        max = high;
    const byte * start = state.cur;
    if (low > max)
        return RegexMatchBacktrack;

    ActiveStage & stage = pushStage(state).setMatched();
    if (minimal)
    {
        stage.followPosition = start+low;
        stage.extra.limit = start+max;
    }
    else
    {
        stage.followPosition = start+max;
        stage.extra.limit = start+low;
    }
    return RegexMatchContinue;
}

RegexMatchAction RegexRepeatAnyPattern::nextAction(ActiveStage & stage, RegexState & state)
{
    switch (stage.getState())
    {
    case RSretry:
        if (minimal)
        {
            if (stage.followPosition < stage.extra.limit)
            {
                stage.followPosition++;
                stage.setMatched();
                return RegexMatchContinue;
            }
        }
        else
        {
            if (stage.followPosition > stage.extra.limit)
            {
                stage.followPosition++;
                stage.setMatched();
                return RegexMatchContinue;
            }
        }
        return RegexPattern::nextAction(stage, state);
    default:
        return RegexPattern::nextAction(stage, state);
    }
}


void RegexRepeatAnyPattern::serializePattern(MemoryBuffer & out)
{ 
    RegexPattern::serializePattern(out);
    out.append(low).append(high).append(minimal);
}

void RegexRepeatAnyPattern::deserializePattern(MemoryBuffer & in)
{
    RegexPattern::deserializePattern(in);
    in.read(low).read(high).read(minimal);
}

void RegexRepeatAnyPattern::toXMLattr(StringBuffer & out, RegexXmlState & state)
{
    RegexPattern::toXMLattr(out, state);
    out.appendf(" low=\"%u\" high=\"%u\" greedy=\"%d\"", low, high, !minimal);
}


//---------------------------------------------------------------------------

RegexMatchAction RegexNamed::match(RegexState & state, RegexPattern * instance, MatchState & matched)
{
    MatchSaveState saved;

    checkStackOverflow((size32_t)(state.cur-state.start));
    state.pushMatch(matched, saved);
    RegexMatchAction ret = match(state, instance);
    state.popMatch(saved);
    return ret;
}


RegexMatchAction RegexNamed::match(RegexState & state, RegexPattern * instance)
{
    state.stack.append(*instance);
    RegexMatchAction ret = first->MATCH(state);
    state.stack.pop();
    return ret;
}


void RegexNamed::serializeLinks(MemoryBuffer & out, RegexSerializeState & state)
{
    serializeLink(out, first, state);
}

void RegexNamed::dispose()
{
    disposeLink(first);
}


void RegexNamed::gather(RegexSerializeState & state)
{
    if (state.named.find(*this) == NotFound)
    {
        state.named.append(OLINK(*this));
        first->gather(state);
    }
}

void RegexNamed::deserializeLinks(MemoryBuffer & in, RegexSerializeState & state)
{
    first.setown(deserializeLink(in, state));
}


void RegexNamed::serializePattern(MemoryBuffer & out)
{
    if (name)
    {
        StringBuffer lowerName;
        lowerName.append(str(name)).toLowerCase();
        out.append(lowerName.str());
    }
    else
        out.append(str(name));

    out.append(id);
}


void RegexNamed::deserializePattern(MemoryBuffer & in)
{
    ::deserialize(in, name);
    in.read(id);
}

void graphToXML(StringBuffer & out, RegexXmlState & state, RegexPattern * first)
{
    state.reset();
    state.addLink(*first);
    while (state.toVisit.ordinality())
    {
        RegexPattern & next = state.toVisit.item(0);
        state.toVisit.remove(0, true);
        state.visited.append(next);
        next.toXML(out, state);
    }
}

void RegexNamed::toXML(StringBuffer & out, RegexXmlState & state)
{
    StringBuffer lowerName;
    lowerName.append(str(name)).toLowerCase();
    out.appendf("<named id=\"%d\" name=\"%s\" matchid=\"%d\" first=\"%d\">", state.named.find(*this), lowerName.str(), id, state.patterns.find(*first)).newline();
    graphToXML(out, state, first);
    //first->toXML(out, state);
    out.append("</named>").newline();
}


RegexMatchAction RegexNamed::beginMatch(RegexState & state)
{
    return first->beginMatch(state);
}


//-----------------------------------------------------------------------

void regexToXml(StringBuffer & out, RegexPattern * root, unsigned detail)
{
    RegexXmlState state;
    state.detail = detail;
    root->gather(state);

    out.appendf("<regex root=\"%d\">", state.patterns.find(*root)).newline();
    graphToXML(out, state, root);
    ForEachItemIn(in, state.named)
        state.named.item(in).toXML(out, state);
    out.append("</regex>").newline();
}


void serializeRegex(MemoryBuffer & out, RegexPattern * root)
{
    RegexSerializeState state;
    root->gather(state);

    unsigned version = REGEX_VERSION;
    out.append(version);
    out.append((unsigned)state.patterns.ordinality());
    out.append((unsigned)state.named.ordinality());

    ForEachItemIn(ip, state.patterns)
        state.patterns.item(ip).serializePattern(out);
    ForEachItemIn(in, state.named)
        state.named.item(in).serializePattern(out);
    ForEachItemIn(ip2, state.patterns)
        state.patterns.item(ip2).serializeLinks(out, state);
    ForEachItemIn(in2, state.named)
        state.named.item(in2).serializeLinks(out, state);

    ForEachItemIn(i, state.patterns)
        state.patterns.item(i).clearGathered();
}

RegexPattern * deserializeRegex(MemoryBuffer & in)
{
    RegexSerializeState state;

    unsigned idx;
    unsigned version;
    unsigned numPatterns, numNamed;
    in.read(version);
    if (version != REGEX_VERSION)
        throwError(REGEXERR_IncompatibleVersion);
    in.read(numPatterns);
    in.read(numNamed);

    for (idx=0; idx < numPatterns; idx++)
    {
        byte _kind;
        ThorRegexKind kind;
        in.read(_kind);
        kind = (ThorRegexKind)_kind;

        RegexPattern * next;
        switch (kind)
        {
        case ThorRegexNull:         next = new RegexNullPattern();                      break;
        case ThorRegexAnyChar:      next = new RegexAnyCharPattern();                   break;
        case ThorRegexAsciiDFA:     next = new RegexAsciiDfaPattern();                  break;
//      case ThorRegexIDFA:         next = new RegexIDFAPattern();                      break;
        case ThorRegexAscii:        next = new RegexAsciiPattern();                     break;
        case ThorRegexAsciiI:       next = new RegexAsciiIPattern();                    break;
        case ThorRegexAsciiSet:     next = new RegexAsciiSetPattern();                  break;
        case ThorRegexAsciiISet:    next = new RegexAsciiISetPattern();                 break;
        case ThorRegexUnicode:      next = new RegexUnicodePattern();                   break;
        case ThorRegexUnicodeI:     next = new RegexUnicodeIPattern();                  break;
        case ThorRegexUnicodeSet:   next = new RegexUnicodeSetPattern();                break;
        case ThorRegexUtf8:         next = new RegexUtf8Pattern();                      break;
        case ThorRegexUtf8I:        next = new RegexUtf8IPattern();                     break;
//      case ThorRegexUnicodeISet:  next = new RegexUnicodeISetPattern();               break;
        case ThorRegexStart:        next = new RegexStartPattern();                     break;
        case ThorRegexFinish:       next = new RegexFinishPattern();                    break;
        case ThorRegexBeginToken:   next = new RegexBeginTokenPattern();                break;
        case ThorRegexEndToken:     next = new RegexEndTokenPattern();                  break;
        case ThorRegexBeginSeparator:next = new RegexBeginSeparatorPattern();           break;
        case ThorRegexEndSeparator: next = new RegexEndSeparatorPattern();              break;
        case ThorRegexRepeat:       next = new RegexRepeatPattern();                    break;
        case ThorRegexCheck:        next = new RegexCheckInPattern();                   break;
        case ThorRegexCheckLength:  next = new RegexCheckLengthPattern();               break;
        case ThorRegexBeginCheck:   next = new RegexBeginCheckPattern();                break;
        case ThorRegexAssertNext:   next = new RegexAssertNextPattern();                break;
        case ThorRegexAssertPrev:   next = new RegexAssertPrevPattern();                break;
        case ThorRegexNamed:        next = new RegexNamedPattern();                     break;
        case ThorRegexEndNested:    next = new RegexEndNestedPattern();                 break;
        case ThorRegexDone:         next = new RegexDonePattern();                      break;
        case ThorRegexValidateAscAsAsc: next = new RegexValidateAscAsAscPattern();      break;
        case ThorRegexValidateUniAsAsc: next = new RegexValidateUniAsAscPattern();      break;
        case ThorRegexValidateUtf8AsAsc: next = new RegexValidateUtf8AsAscPattern();    break;
        case ThorRegexValidateAscAsUni: next = new RegexValidateAscAsUniPattern();      break;
        case ThorRegexValidateUniAsUni: next = new RegexValidateUniAsUniPattern();      break;
        case ThorRegexValidateUtf8AsUni: next = new RegexValidateUtf8AsUniPattern();    break;
        case ThorRegexRecursive:    next = new RegexRecursivePattern();                 break;
        case ThorRegexEndRecursive: next = new RegexEndRecursivePattern();              break;
        case ThorRegexPenalty:      next = new RegexPenaltyPattern();                   break;
        case ThorRegexRepeatAny:    next = new RegexRepeatAnyPattern();                 break;
        default:
            UNIMPLEMENTED;
        }
        next->deserializePattern(in);
        state.patterns.append(*next);
    }

    for (idx=0; idx < numNamed; idx++)
    {
        RegexNamed * next = new RegexNamed();
        next->deserializePattern(in);
        state.named.append(*next);
    }

    for (idx=0; idx < numPatterns; idx++)
        state.patterns.item(idx).deserializeLinks(in, state);
    for (idx=0; idx < numNamed; idx++)
        state.named.item(idx).deserializeLinks(in, state);

    return LINK(&state.patterns.item(0));
}


//---------------------------------------------------------------------------

RegexMatchState * RegexStateCache::createState(RegexNamed * def)
{
    if (matchStates.ordinality())
    {
        RegexMatchState * ret = &matchStates.popGet();
        ret->reset(def);
        return ret;
    }

    return new RegexMatchState(def);
}

void RegexStateCache::destroyState(RegexMatchState * state)
{
    matchStates.append(*state);
}


RegexMatchStateSave * RegexStateCache::createStateSave(RegexNamed * def)
{
    if (matchStateSaves.ordinality())
    {
        RegexMatchStateSave * ret = &matchStateSaves.popGet();
        ret->reset(def);
        return ret;
    }

    return new RegexMatchStateSave(def);
}

RegexMatchStateSave * RegexStateCache::createStateSave(IAtom * _name, regexid_t _id)
{
    if (matchStateSaves.ordinality())
    {
        RegexMatchStateSave * ret = &matchStateSaves.popGet();
        ret->reset(_name, _id);
        return ret;
    }

    return new RegexMatchStateSave(_name, _id);
}

void RegexStateCache::destroyStateSave(RegexMatchStateSave * state)
{
    matchStateSaves.append(*state);
}


void RegexState::processPattern(RegexPattern * grammar)
{
    if (implementation == NLPAregexStack)
    {
        grammar->MATCH(*this);
        return;
    }

    RegexMatchAction action = grammar->beginMatch(*this);
    switch (action)
    {
    case RegexMatchDone:
        while (hasActiveStage())
            popStage();
        return;
    case RegexMatchBacktrack:
    case RegexMatchContinue:
        break;
    case RegexMatchBacktrackToken:
        throwUnexpected();
        break;
    }

    while (hasActiveStage())
    {
#ifdef TRACE_REGEX
        StringBuffer itemText;
        itemText.appendf("[%p]", topStage().pattern);
#endif
        action = topStage().nextAction(*this);
#ifdef TRACE_REGEX
        DBGLOG("%*s%snextAction returned %s", patternDepth, "", itemText.str(), resultText[action]);
#endif

        switch (action)
        {
        case RegexMatchDone:
            {
                while (hasActiveStage())
                    popStage();
                return;
            }
        case RegexMatchContinue:
        case RegexMatchBacktrack:
            break;
        case RegexMatchBacktrackToken:
            {
                bool finished = false;
                do
                {
                    finished = topStage().isBeginToken();
                    popStage();
                } while (!finished);
                break;
            }
        }
    }

    assertex(namedStack.ordinality() == 0);
    assertex(curMatch == &top);
}


ActiveStage & RegexState::pushStage()
{
    curActiveStage++;
    if (!stages.isItem(curActiveStage))
    {
        ActiveStage temp;
        //initialise not needed, but it stops a warning from the compiler
        temp.pattern = NULL;
        temp.state = RSinit;
        stages.append(temp);
    }
    return stages.element(curActiveStage);
}

void RegexState::popStage()
{
#ifdef TRACE_REGEX
    ActiveStage & tos = topStage();
    StringBuffer t;
    tos.pattern->getTraceText(t);
    DBGLOG("%*s[%p]Pop %s", --patternDepth, "", tos.pattern, t.str());
#endif
    stages.element(curActiveStage).cleanup(*this);
    curActiveStage--;
}

/*
RegexMatchAction RegexDfa::beginMatch(RegexState & state)
{
    //walk to find matches
    if (only one then save it).
        if (>1 create a list and add all items onto it)
    if (matched)
    {
        if (single)
        {
        }
        else
        {
            state.pushState(this).setStage(RSretry);
            newState->extra.matches = matches;
        }
            

        state.namedAction.append(*end);
    return firstElement->beginMatch(state);
}

RegexMatchAction RegexDfa::nextAction(ActiveStage & stage, RegexState & state)
{
    if (stage.extra.matches->ordinality())
    {
        state.cur = stage.extra.matches.pop();
        stage.setMatched();
        return RegexMatchContinue;
    }
    return RegexMatchBacktrack;
}

*/
    

//---------------------------------------------------------------------------

RegexMatchInfo * RegexMatches::createMatch()
{
    RegexMatchInfo * row = new RegexMatchInfo(NULL);
    results.append(*row);
    return row;
}


void RegexMatches::reset()
{
    results.kill();
}

//---------------------------------------------------------------------------

RegexAlgorithm::RegexAlgorithm(IOutputMetaData * _outRecordSize, byte _kind) : NlpAlgorithm(new CRegexMatchedResultInfo)
{
    outRecordSize.set(_outRecordSize);
    kind = _kind;
}

RegexAlgorithm::~RegexAlgorithm()
{
    if (grammar)
        grammar->dispose();
}

INlpParser * RegexAlgorithm::createParser(ICodeContext * ctx, unsigned activityId, INlpHelper * helper, IHThorParseArg * arg)
{
    return new RegexParser(ctx, this, helper, activityId);
}

void RegexAlgorithm::init(IHThorParseArg & arg)
{
}

void RegexAlgorithm::serialize(MemoryBuffer & out)
{
    out.append(kind);
    NlpAlgorithm::serialize(out);
    out.append(minPatternLength);
    serializeRegex(out, grammar);
}

void RegexAlgorithm::deserialize(MemoryBuffer & in)
{
    NlpAlgorithm::deserialize(in);
    in.read(minPatternLength);
    grammar.setown(deserializeRegex(in));
}


INlpParseAlgorithm * createRegexParser(MemoryBuffer & buffer, IOutputMetaData * outRecordSize, byte kind)
{
    RegexAlgorithm * ret = new RegexAlgorithm(outRecordSize, kind);
    ret->deserialize(buffer);
    return ret;
}

void RegexAlgorithm::match(RegexState & state)
{
    state.processPattern(grammar);
}


//---------------------------------------------------------------------------

RegexParser::RegexParser(ICodeContext * ctx, RegexAlgorithm * _algo, INlpHelper * _helper, unsigned _activityId) : matched(_algo->matchInfo)
{
    algo = LINK(_algo);
    IOutputMetaData * outputMeta = algo->outRecordSize;

    outputAllocator.setown(ctx->getRowAllocator(outputMeta, _activityId));
    helper = _helper;
    results.first();
    charWidth = algo->charWidth;
}

RegexParser::~RegexParser()
{
    matched.kill();
    algo->Release();
}

bool RegexParser::performMatch(IMatchedAction & action, const void * row, unsigned len, const void * data)
{
    try
    {
        results.reset();
        len *= charWidth;
        size32_t maxSize = algo->maxLength*charWidth;

        const byte * start = (const byte *)data;
        const byte * endData = start + len;
        const byte * end = endData - algo->minPatternLength;

        RegexState state(cache, algo->kind, helper, this, algo->inputFormat, len, start);
        if (len >= algo->minPatternLength)
        {
            state.row = row;
            state.processor = &action;
            state.best = NULL;
            for (const byte * curScan = start; curScan <= end;)
            {
                state.cur = curScan;
                state.top.start = curScan;
                state.nextScanPosition = NULL;
                state.score = 0;
                if (!algo->singleChoicePerLine)
                    state.best = NULL;
                if ((size32_t)(endData - curScan) > maxSize)
                {
                    state.end = curScan + (maxSize + charWidth);
                    state.lengthIsLimited = true;
                }
                else
                {
                    state.end = endData;
                    state.lengthIsLimited = false;
                }
                algo->match(state);
                if (state.numMatched >= algo->keepLimit)
                    break;
                if (state.numMatched > algo->atMostLimit)
                {
                    results.reset();
                    return false;
                }
                if (algo->scanAction == INlpParseAlgorithm::NlpScanWhole)
                    break;
                if (state.numMatched && (algo->scanAction == INlpParseAlgorithm::NlpScanNone))
                    break;
                if (state.nextScanPosition && (algo->scanAction == INlpParseAlgorithm::NlpScanNext) && (curScan != state.nextScanPosition))
                    curScan = state.nextScanPosition;
                else
                    curScan += charWidth;
            }
        }

        if (state.numMatched == 0)
        {
            if (algo->notMatchedOnly || algo->notMatched)
            {
                //Create a row where nothing matches.
                MatchState & top = state.top;
                top.end = top.start;
                matched.extractResults(top, state.start, algo->addedSeparators);
                NlpMatchWalker walker(&top);
                
                const void * matchRow = createMatchRow(state, walker);
                if (matchRow)
                    results.appendOwnResult(matchRow);
            }
        }
        else if (algo->notMatchedOnly)
        {
            results.reset();
        }

        //Now reduce the number of matches according to min/max/best
        return results.first();
    }
    catch (IException *)
    {
        throw;
    }
    catch (...)
    {
        throwError1(REGEXERR_MatchFailure, 0);
        return false;
    }
}


const void * RegexParser::createMatchRow(RegexState & state, NlpMatchWalker & walker)
{
    RtlDynamicRowBuilder row(outputAllocator);
    unsigned rowSize = state.processor->onMatch(row, state.row, &matched, &walker);
    if (rowSize)
        return row.finalizeRowClear(rowSize);
    return NULL;
}
    


bool RegexParser::onMatch(NlpState & _state)
{
    if ((algo->scanAction == INlpParseAlgorithm::NlpScanWhole) && (_state.cur != _state.end))
        return false;

    RegexState & state = (RegexState &)_state;
    if (state.lengthIsLimited && state.cur == state.end)
        return false;

    MatchState & top = state.top;
    const byte * end = top.end;
    unsigned length =  (size32_t)(end - top.start);

    RegexMatchInfo * result = NULL;
    if (algo->chooseMin || algo->chooseMax || algo->chooseBest)
    {
        RegexMatchInfo * best = state.best;
        if (best)
        {
            int comp = 0;
            if (algo->chooseMin)
                comp = (int)(length - best->length);
            else if (algo->chooseMax)
                comp = (int)(best->length - length);
            if (algo->chooseBest && (comp == 0))
                comp = (int)(best->score - state.score);
            if (comp >= 0)
                return false;
        }
        else
        {
            state.best = results.createMatch();
            state.numMatched++;
        }
        result = state.best;
        state.nextScanPosition = end;
    }
    else
    {
        if (!state.nextScanPosition || state.nextScanPosition > end)
            state.nextScanPosition = end;
    }


    matched.extractResults(top, state.start, algo->addedSeparators);
    NlpMatchWalker walker(&top);

    const void * newRow = createMatchRow(state, walker);
    if (!newRow)
        return false;

    if (!result)
    {
        result = results.appendOwnResult(newRow);
        state.numMatched++;
    }
    else
    {
        result->setown(newRow);
    }
    
    result->score = state.score;
    result->length = length;
    if (algo->matchAction == INlpParseAlgorithm::NlpMatchAll)
    {
        if (state.numMatched >= algo->keepLimit)
            return true;
        if (state.numMatched > algo->atMostLimit)
            return true;
        return false;
    }
    return true;
}

/*
#if 0

RegexMatchAction RegexDFAPattern::match(MatchState & state)
{
    const byte * savedCur = state.cur;
    while (!fail)
    {
        if (consume & in a match state), add it to a list else fail
            else
    }
    ForEachItemInrev(matches)
    {
        state.cur =matches.item(idx);
        if (matchNext(state) == RegexMatchDone)
            return RegexMatchDone;
    }
    state.cur = savedCur;
    return RegexMatchBacktrack;
}



Converting a NFA:
States, transition(symbol)->*states

to a DFA
States, transition(symbol)->state

Create a set of set-of-nstates Dstates that can be reached from the initial.
ForEachUnmarkedDState(T)
    ForEachSymbol(x), 
       U = possible states that could be reach after that
       if U not in DStates, add it.
       Save DTrans[T,a] = U
    End
End

All states that can be reached at all onto a stack


regular expressions:
    *+? have highest 
    then concat
    then
    
    r|(s|t) == (r|s)|t
    (rs)t == r(st)
    r(s|t) == rs|rt
    er == r == re
    r* = (r|e)*
    r** = r*

Regular expression to NFA:
    Fairly simple - gives a list of Node(All transitions and actions)

Running a NFA:
    Simple algorithmn maintains a sets of possible states.  So all calculated at the same time.
    - Solves problem of repeatadly searching, but I don't think it could be used for matching text,
      because no positioning information is maintained.

    - Use an ordered list of next-states which 

Lazy 
}}}


#endif
*/
