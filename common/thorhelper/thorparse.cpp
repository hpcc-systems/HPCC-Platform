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

#include "jliball.hpp"
#include "thorparse.ipp"
#include "thorregex.hpp"
#include "eclrtl.hpp"
#include "eclhelper.hpp"

IAtom * separatorTagAtom;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    separatorTagAtom = createAtom("<separator>");
    return true;
}

//---------------------------------------------------------------------------
void deserializeBoolArray(unsigned len, bool * values, MemoryBuffer & in)
{
    for (unsigned i = 0; i < len; i+= 8)
    {
        unsigned char next;
        in.read(next);
        unsigned max = i+8 <= len ? 8 : len - i;
        for (unsigned j=0; j<max; j++)
            values[i+j] = (next & (1 << j)) != 0;
    }
}


void serializeBoolArray(MemoryBuffer & out, unsigned len, const bool * values)
{
    for (unsigned i = 0; i < len; i+= 8)
    {
        unsigned char next = 0;
        unsigned max = i+8 <= len ? 8 : len - i;
        for (unsigned j=0; j<max; j++)
            if (values[i+j]) next |= (1 << j);
        out.append(next);
    }
}

//---------------------------------------------------------------------------
    
NlpState::NlpState(INlpMatchedAction * _action, NlpInputFormat _inputFormat, size32_t len, const void * text)
{
    matchAction = _action;
    inputFormat = _inputFormat;
    charSize = (inputFormat == NlpUnicode) ? sizeof(UChar) : sizeof(char);
    start = (const byte *)text;
    cur = start;
    end = start + len;
    curMatch = &top;
    next = &top.firstChild;
    top.start = start;
    top.parent = NULL;
}

void NlpState::pushMatch(MatchState & match, MatchSaveState & save)
{
    save.savedNext = next;
    save.savedMatch = curMatch;
    match.parent = curMatch;
    match.start = cur;
    *next = &match;
    next = &match.firstChild;
    curMatch = &match;
};

void NlpState::popMatch(const MatchSaveState & save)
{
    next = save.savedNext;
    *next = NULL;
    curMatch = save.savedMatch;
}

void NlpState::markFinish(MatchSaveState & save)
{
    save.savedMatch = curMatch;
    save.savedNext = next;
    curMatch->end = cur;
    next = &curMatch->next;
    curMatch = curMatch->parent;
}

void NlpState::unmarkFinish(const MatchSaveState & save)
{
    next = save.savedNext;
    curMatch = save.savedMatch;
}

//---------------------------------------------------------------------------
    
NlpMatchPath::NlpMatchPath(const UnsignedArray & _ids, const UnsignedArray & _indices)
{
    assert(_ids.ordinality() == _indices.ordinality());
    ForEachItemIn(idx, _ids)
    {
        ids.append(_ids.item(idx));
        indices.append(_indices.item(idx));
    }
}


NlpMatchPath::NlpMatchPath(MemoryBuffer & in)
{
    unsigned num;
    in.read(num);
    for (unsigned idx = 0; idx < num; idx++)
    {
        unsigned index, id;
        in.read(id);
        in.read(index);
        ids.append(id);
        indices.append(index);
    }
}

NlpMatchPath::~NlpMatchPath()
{
}

void NlpMatchPath::serialize(MemoryBuffer & out) const
{
    unsigned num = ids.ordinality();
    out.append(num);
    for (unsigned idx = 0; idx < num; idx++)
    {
        out.append(ids.item(idx));
        out.append(indices.item(idx));
    }
}

//---------------------------------------------------------------------------

CMatchedResultInfo::CMatchedResultInfo()
{
    inputFormat = NlpAscii;
}

void CMatchedResultInfo::deserialize(MemoryBuffer & in)
{
    unsigned num;
    in.read(inputFormat);
    in.read(num);
    for (unsigned idx = 0; idx < num; idx++)
    {
        NlpMatchPath & cur = *createMatchPath(in);
        matchResults.append(cur);
    }
}

void CMatchedResultInfo::serialize(MemoryBuffer & out) const
{
    unsigned num = matchResults.ordinality();
    out.append(inputFormat);
    out.append(num);
    for (unsigned idx = 0; idx < num; idx++)
        matchResults.item(idx).serialize(out);
}

//---------------------------------------------------------------------------

CMatchedResults::CMatchedResults(CMatchedResultInfo * _def)
{
    in = NULL;
    def = _def;
    unsigned num = def->matchResults.ordinality();
    matched = new IMatchedElement *[num];
    for (unsigned i=0; i<num; i++)
        matched[i] = NULL;
}

CMatchedResults::~CMatchedResults()
{
    kill();
}

bool CMatchedResults::getMatched(unsigned idx)              
{ 
    return matched[idx] != &notMatched; 
}

size32_t CMatchedResults::getMatchLength(unsigned idx)          
{ 
    const IMatchedElement * cur = matched[idx];
    const byte * start = cur->queryStartPtr(); 
    size32_t size = (size32_t)(cur->queryEndPtr() - start); 
    size32_t len;

    switch (def->inputFormat)
    {
    case NlpAscii:
        len = size;
        break;
    case NlpUtf8:
        len = rtlUtf8Length(size, start);
        break;
    case NlpUnicode:
        len = size / sizeof(UChar);
        break;
    default:
        throwUnexpected();
    }
    return len;
}


size32_t CMatchedResults::getMatchPosition(unsigned idx)        
{
    IMatchedElement * cur = matched[idx];
    if (cur == &notMatched)
        return 0;
    size32_t pos = (size32_t)(cur->queryStartPtr() - in);
    switch (def->inputFormat)
    {
    case NlpUtf8:
        pos = rtlUtf8Length(pos, in);
        break;
    case NlpUnicode:
        pos = pos / sizeof(UChar);
        break;
    }
    return pos+1;
}

void CMatchedResults::getMatchText(size32_t & outlen, char * & out, unsigned idx)
{
    const IMatchedElement * cur = matched[idx];
    const byte * start = cur->queryStartPtr(); 
    size32_t size = (size32_t)(cur->queryEndPtr() - start); 

    switch (def->inputFormat)
    {
    case NlpAscii:
        rtlStrToStrX(outlen, out, size, start);
        break;
    case NlpUtf8:
        {
            //could use codepage2codepage if worried about efficiency...
            unsigned len = rtlUtf8Length(size, start);
            rtlUtf8ToStrX(outlen, out, len, (const char *)start);
            break;
        }
    case NlpUnicode:
        rtlUnicodeToStrX(outlen, out, size/sizeof(UChar), (const UChar *)start);
        break;
    }
}

void CMatchedResults::getMatchUnicode(size32_t & outlen, UChar * & out, unsigned idx)
{
    const IMatchedElement * cur = matched[idx];
    const byte * start = cur->queryStartPtr();
    size32_t size = (size32_t)(cur->queryEndPtr() - start);

    switch (def->inputFormat)
    {
    case NlpAscii:
        rtlStrToUnicodeX(outlen, out, size, (const char *)start);
        break;
    case NlpUtf8:
        {
            //could use codepage2codepage if worried about efficiency...
            unsigned len = rtlUtf8Length(size, start);
            rtlUtf8ToUnicodeX(outlen, out, len, (const char *)start);
            break;
        }
        break;
    case NlpUnicode:
        rtlUnicodeToUnicodeX(outlen, out, size/sizeof(UChar), (const UChar*)start);
        break;
    }
}

void CMatchedResults::getMatchUtf8(size32_t & outlen, char * & out, unsigned idx)
{
    const IMatchedElement * cur = matched[idx];
    const byte * start = cur->queryStartPtr();
    size32_t size = (size32_t)(cur->queryEndPtr() - start);

    switch (def->inputFormat)
    {
    case NlpAscii:
        rtlStrToUtf8X(outlen, out, size, (const char *)start);
        break;
    case NlpUtf8:
        {
            //could use codepage2codepage if worried about efficiency...
            unsigned len = rtlUtf8Length(size, start);
            rtlUtf8ToUtf8X(outlen, out, len, (const char *)start);
            break;
        }
    case NlpUnicode:
        rtlUnicodeToUtf8X(outlen, out, size/sizeof(UChar), (const UChar*)start);
        break;
    }
}

byte * CMatchedResults::queryMatchRow(unsigned idx)
{
    const IMatchedElement * cur = matched[idx];
    return (byte *)cur->queryRow();
}

//MORE: Allow access to attributes at any location on the tree.
byte * CMatchedResults::queryRootResult()
{
    return (byte *)rootResult;
}

void CMatchedResults::kill()
{
    if (matched)
    {
        unsigned num = def->matchResults.ordinality();
        for (unsigned i=0; i < num; i++)
            ::Release(matched[i]);
        delete [] matched;
        matched = NULL;
    }
}

//---------------------------------------------------------------------------


IAtom * NlpMatchWalker::queryName()
{ 
    return curMatch->queryName(); 
}

size32_t NlpMatchWalker::queryMatchSize()
{
    return (size32_t)(curMatch->end - curMatch->start);
}

const void * NlpMatchWalker::queryMatchStart()
{
    return curMatch->start;
}

unsigned NlpMatchWalker::numChildren()
{
    unsigned count = 0;
    MatchState * cur = curMatch->firstChild;
    while (cur)
    {
        count++;
        cur = cur->next;
    }
    return count;
}

IMatchWalker * NlpMatchWalker::getChild(unsigned numToSkip)
{
    MatchState * cur = curMatch->firstChild;
    while (cur && numToSkip)
    {
        numToSkip--;
        cur = cur->next;
    }
    if (cur)
        return new NlpMatchWalker(cur);
    return NULL;
}

//------------------------------------------------------

static bool hasChildren(IMatchWalker * walker)
{
    for (unsigned i=0;;i++)
    {
        Owned<IMatchWalker> child = walker->getChild(i);
        if (!child)
            return false;
        if (child->queryName() != separatorTagAtom)
            return true;
    }
}

static StringBuffer & getElementText(StringBuffer & s, IMatchWalker * walker)
{
    unsigned len = walker->queryMatchSize();
    const char * text = (const char *)walker->queryMatchStart();
    return s.append(len, text);
}


static void expandElementText(StringBuffer & s, IMatchWalker * walker)
{
    getElementText(s.append('"'), walker).append('"');
}

static void getDefaultParseTree(StringBuffer & s, IMatchWalker * cur)
{
    IAtom * name = cur->queryName();
    if (name != separatorTagAtom)
    {
        if (name)
        {
            StringBuffer lowerName;
            lowerName.append(name).toLowerCase();
            s.append(lowerName);
        }
        if (hasChildren(cur))
        {
            s.append("[");
            for (unsigned i=0;;i++)
            {
                Owned<IMatchWalker> child = cur->getChild(i);
                if (!child)
                    break;

                getDefaultParseTree(s, child);
                s.append(" ");
            }
            s.setLength(s.length()-1);
            s.append("]");
        }
        else
            expandElementText(s, cur);
    }
}


void getDefaultParseTree(IMatchWalker * walker, unsigned & len, char * & text)
{
    StringBuffer s;
    getDefaultParseTree(s, walker);
    len = s.length();
    text = s.detach();
}


static void getXmlParseTree(StringBuffer & s, IMatchWalker * walker, unsigned indent)
{
    IAtom * name = walker->queryName();
    if (name != separatorTagAtom)
    {
        unsigned max = walker->numChildren();
        if (!name)
        {
            if (hasChildren(walker))
            {
                for (unsigned i=0; i<max; i++)
                {
                    Owned<IMatchWalker> child = walker->getChild(i);
                    getXmlParseTree(s, child, indent);
                }
            }
            else
                getElementText(s, walker);
        }
        else
        {
            StringBuffer lowerName;
            lowerName.append(name).toLowerCase();

            s.pad(indent).append('<').append(lowerName).append('>');
            if (hasChildren(walker))
            {
                s.newline();
                for (unsigned i=0; i<max; i++)
                {
                    Owned<IMatchWalker> child = walker->getChild(i);
                    getXmlParseTree(s, child, indent+1);
                }
                s.pad(indent);
            }
            else
                getElementText(s, walker);
            s.append("</").append(lowerName).append('>').newline();
        }
    }
}


void getXmlParseTree(IMatchWalker * walker, unsigned & len, char * & text)
{
    StringBuffer s;
    getXmlParseTree(s, walker, 0);
    len = s.length();
    text = s.detach();
}

//------------------------------------------------------

unsigned getMaximumMatchLength(AsciiDfa & dfa, unsigned len, const byte * start)
{
    const byte * cur = start;
    const byte * end = start+len;
    unsigned activeState = 0;
    const AsciiDfaState * states = dfa.queryStates();
    unsigned * transitions = dfa.queryTransitions();
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
        return (size32_t)(best-start);
    return NotFound;
}

//------------------------------------------------------

NlpAlgorithm::NlpAlgorithm(CMatchedResultInfo * _matched)
{
    matchInfo = _matched;
    addedSeparators = false;
    notMatched = false;
    notMatchedOnly = false;
    chooseMin = false;
    chooseMax = false;
    chooseBest = false;
    singleChoicePerLine = false;
    inputFormat = NlpAscii;
    keepLimit = UINT_MAX;
    atMostLimit = UINT_MAX;
    charWidth = sizeof(char);
}

NlpAlgorithm::~NlpAlgorithm()
{
    ::Release(matchInfo);
}

void NlpAlgorithm::setOptions(MatchAction _matchAction, ScanAction _scanAction, NlpInputFormat _inputFormat, unsigned _keepLimit, unsigned _atMostLimit)
{
    matchAction = _matchAction;
    scanAction = _scanAction;
    inputFormat = _inputFormat;
    keepLimit = _keepLimit ? _keepLimit : UINT_MAX;
    atMostLimit = _atMostLimit ? _atMostLimit : UINT_MAX;
    charWidth = (inputFormat == NlpUnicode) ? sizeof(UChar) : sizeof(char);
}

void NlpAlgorithm::setChoose(bool _chooseMin, bool _chooseMax, bool _chooseBest, bool _singleChoicePerLine)
{
    chooseMin = _chooseMin;
    chooseMax = _chooseMax;
    chooseBest = _chooseBest;
    singleChoicePerLine = _singleChoicePerLine;
}

void NlpAlgorithm::setJoin(bool _notMatched, bool _notMatchedOnly)
{
    notMatched = _notMatched;
    notMatchedOnly = _notMatchedOnly;
}

void NlpAlgorithm::setLimit(unsigned _maxLength)
{
    maxLength = _maxLength;
}

void NlpAlgorithm::serialize(MemoryBuffer & out)
{
    out.append((unsigned)matchAction);
    out.append((unsigned)scanAction);
    out.append((byte)inputFormat);
    out.append(keepLimit);
    out.append(atMostLimit);
    out.append(charWidth);
    out.append(addedSeparators);
    out.append(notMatched);
    out.append(notMatchedOnly);
    out.append(chooseMin);
    out.append(chooseMax);
    out.append(chooseBest);
    out.append(singleChoicePerLine);
    out.append(maxLength);
    matchInfo->serialize(out);
}

void NlpAlgorithm::deserialize(MemoryBuffer & in)
{
    unsigned temp;
    byte tempByte;
    in.read(temp); matchAction = (MatchAction)temp;
    in.read(temp); scanAction = (ScanAction)temp;
    in.read(tempByte); inputFormat = (NlpInputFormat)tempByte;
    in.read(keepLimit);
    in.read(atMostLimit);
    in.read(charWidth);
    in.read(addedSeparators);
    in.read(notMatched);
    in.read(notMatchedOnly);
    in.read(chooseMin);
    in.read(chooseMax);
    in.read(chooseBest);
    in.read(singleChoicePerLine);
    in.read(maxLength);
    matchInfo->deserialize(in);
}

INlpParseAlgorithm * createThorParser(MemoryBuffer & buffer, IOutputMetaData * outRecordSize)
{
    byte kind;
    buffer.read(kind);

    switch (kind)
    {
    case NLPAregexStack:
    case NLPAregexHeap:
        return createRegexParser(buffer, outRecordSize, kind);
    case NLPAtomita:
        return createTomitaParser(buffer, outRecordSize);
    default:
        UNIMPLEMENTED;
    }
}

INlpParseAlgorithm * createThorParser(IResourceContext *ctx, IHThorParseArg & helper)
{
    unsigned len;
    const void * data;
    helper.queryCompiled(ctx, len, data);
    MemoryBuffer compressed, buffer;
    compressed.setBuffer(len, (void *)data, false);
    decompressToBuffer(buffer, compressed);

    INlpParseAlgorithm * algorithm = createThorParser(buffer, helper.queryOutputMeta());
    algorithm->init(helper);
    return algorithm;
}
