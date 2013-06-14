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

#ifndef __THORPARSE_HPP_
#define __THORPARSE_HPP_

#ifdef _WIN32
 #ifdef THORHELPER_EXPORTS
  #define THORHELPER_API __declspec(dllexport)
 #else
  #define THORHELPER_API __declspec(dllimport)
 #endif
#else
 #define THORHELPER_API
#endif

typedef unsigned regexid_t;
enum { NLPAregexStack, NLPAtomita, NLPAregexHeap };

interface IMatchWalker : public IInterface
{
public:
    virtual IAtom * queryName() = 0;
    virtual unsigned queryID() = 0;
    virtual size32_t queryMatchSize() = 0;
    virtual const void * queryMatchStart() = 0;
    virtual unsigned numChildren() = 0;
    virtual IMatchWalker * getChild(unsigned idx) = 0;
};

interface IMatchedResults;
class ARowBuilder;
interface IMatchedAction
{
public:
    virtual size32_t onMatch(ARowBuilder & rowBuilder, const void * in, IMatchedResults * results, IMatchWalker * walker) = 0;
};

interface IMatchedElement : public IInterface
{
    virtual const byte * queryStartPtr() const = 0;
    virtual const byte * queryEndPtr() const = 0;
    virtual const byte * queryRow() const = 0;
};

class RegexNamed;
extern IAtom * separatorTagAtom;
//MORE: Remove the vmt to make constructing more efficient... use id and name fields instead.
class THORHELPER_API MatchState
{
public:
    MatchState() { next = NULL; firstChild = NULL; name = NULL; id = 0; }   // other fields get filled in later.
    MatchState(IAtom * _name, regexid_t _id) { next = NULL; firstChild = NULL; name = _name; id = _id; }  // other fields get filled in later.

    inline IAtom * queryName()                              { return name; }
    inline regexid_t queryID()                            { return id; }

    inline void reset(IAtom * _name, regexid_t _id) { next = NULL; firstChild = NULL; name = _name; id = _id; }

public:
    const byte * start;
    const byte * end;
    MatchState * next;
    MatchState * firstChild;
    MatchState * parent;
    IAtom * name;
    regexid_t id;
};

class MatchSaveState
{
public:
    MatchState * savedMatch;
    MatchState * * savedNext;
};


interface INlpResultIterator
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual const void * getRow() = 0;          // returns linked row.
};

interface INlpParser : public IInterface
{
public:
    // Currently has state, to remove it pass an iterator class to performMatch()
    virtual bool performMatch(IMatchedAction & action, const void * record, unsigned len, const void * data) = 0;
    virtual void reset() = 0;
    // only valid after performMatch has been called, and whilst the parameters passed to performMatch aren't freed.
    virtual INlpResultIterator * queryResultIter() = 0;     
};


interface INlpHelper;
interface IHThorParseArg;
interface IResourceContext;
interface ICodeContext;
interface IOutputMetaData;
enum NlpInputFormat { NlpAscii, NlpUnicode, NlpUtf8 };
interface INlpParseAlgorithm : public IInterface
{
    enum MatchAction    { NlpMatchFirst, NlpMatchAll };
    enum ScanAction     { NlpScanWhole, NlpScanNone, NlpScanNext, NlpScanAll };

public:
    //MORE: This should be implemented so that we can have interchangable algorithms, 
    //and so they can be implemented as add on bits of the system.
    virtual void setOptions(MatchAction _matchAction, ScanAction _scanAction, NlpInputFormat _inputFormat, unsigned _keepLimit, unsigned _atMostLimit) = 0;
    virtual void setChoose(bool _chooseMin, bool _chooseMax, bool _chooseBest, bool _chooseBestScan) = 0; 
    virtual void setJoin(bool _notMatched, bool _notMatchedOnly) = 0; 
    virtual void setLimit(size32_t _maxLength) = 0; 
    virtual void serialize(MemoryBuffer & out) = 0;

    virtual void init(IHThorParseArg & arg) = 0;
    virtual INlpParser * createParser(ICodeContext * ctx, unsigned activityId, INlpHelper * helper, IHThorParseArg * arg) = 0;
};

extern THORHELPER_API INlpParseAlgorithm * createThorParser(MemoryBuffer & buffer, IOutputMetaData * outRecordSize);
extern THORHELPER_API INlpParseAlgorithm * createThorParser(IResourceContext *ctx, IHThorParseArg & helper);
extern THORHELPER_API void getDefaultParseTree(IMatchWalker * walker, unsigned & len, char * & text);
extern THORHELPER_API void getXmlParseTree(IMatchWalker * walker, unsigned & len, char * & text);

#endif /* __THORPARSE_HPP_ */
