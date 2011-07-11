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
enum { NLPAregex, NLPAtomita, NLPAregex2 };

interface IMatchWalker : public IInterface
{
public:
    virtual _ATOM queryName() = 0;
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
extern _ATOM separatorTagAtom;
//MORE: Remove the vmt to make constructing more efficient... use id and name fields instead.
class THORHELPER_API MatchState
{
public:
    MatchState() { next = NULL; firstChild = NULL; name = NULL; id = 0; }   // other fields get filled in later.
    MatchState(_ATOM _name, regexid_t _id) { next = NULL; firstChild = NULL; name = _name; id = _id; }  // other fields get filled in later.

    inline _ATOM queryName()                              { return name; }
    inline regexid_t queryID()                            { return id; }

public:
    const byte * start;
    const byte * end;
    MatchState * next;
    MatchState * firstChild;
    MatchState * parent;
    _ATOM name;
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
