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

#ifndef __THORRPARSE_IPP_
#define __THORRPARSE_IPP_

#include "thorralgo.ipp"
#include "roxiemem.hpp"

//--------------------------------------------------------------------------

//NB: This is malloced..
struct RegexMatchInfo : public CInterface
{
public:
    inline RegexMatchInfo(const void * _row) : row(_row) { }

    inline void setown(const void * _row) { row.setown(_row); }

public:
    roxiemem::OwnedConstRoxieRow row;
    unsigned length;
    int      score;
};


class RegexMatches : public CInterface, public INlpResultIterator
{
    enum { MaxCachedResult=100 };

public:
    RegexMatchInfo * appendOwnResult(const void * _row) 
    { 
        RegexMatchInfo * match = new RegexMatchInfo(_row);
        results.append(*match);
        return match;
    }

    RegexMatchInfo * createMatch();

    void reset();
    virtual bool first() 
    { 
        cur = 0; 
        return results.isItem(cur);
    }
    virtual bool next()
    {
        cur++;
        return results.isItem(cur);
    }
    virtual bool isValid()
    {
        return results.isItem(cur);
    }
    virtual const void * getRow()
    {
        return rtlLinkRow(results.item(cur).row);
    }

protected:
    unsigned cur;
    CIArrayOf<RegexMatchInfo> results;
};

class THORHELPER_API RegexParser : public CInterface, public INlpMatchedAction, public INlpParser
{
public:
    RegexParser(ICodeContext * ctx, RegexAlgorithm * _algo, INlpHelper * _helper, unsigned _activityId);
    ~RegexParser();
    IMPLEMENT_IINTERFACE

    virtual bool performMatch(IMatchedAction & action, const void * in, unsigned len, const void * data);
    virtual INlpResultIterator * queryResultIter() { return &results; }
    virtual void reset() { results.reset(); }

    virtual bool onMatch(NlpState & matched);

    const void * createMatchRow(RegexState & state, NlpMatchWalker & walker);

public:
    INlpHelper * helper;
    Owned<IEngineRowAllocator> outputAllocator;
    RegexAlgorithm * algo;
    RegexMatches results;
    CRegexMatchedResults matched;
    RegexStateCache cache;
    unsigned charWidth;
};

#endif /* __THORRPARSE_HPP_ */
