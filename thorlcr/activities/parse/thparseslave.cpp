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


#include "platform.h"
#include <limits.h>
#include "eclhelper.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"
#include "thorparse.hpp"
#include "jlzw.hpp"
#include "eclrtl.hpp"

#include "thparseslave.ipp"

class CParseSlaveActivity : public CSlaveActivity, implements IMatchedAction
{
    typedef CSlaveActivity PARENT;

    IHThorParseArg *helper;
    OwnedConstThorRow curRow;
    Owned<INlpParseAlgorithm> algorithm;
    Owned<INlpParser> parser;
    bool anyThisGroup;
    char * curSearchText;
    size32_t curSearchTextLen;
    INlpResultIterator * rowIter;
    Owned<IEngineRowAllocator> allocator;

public:
    CParseSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = (IHThorParseArg *)queryHelper();
        anyThisGroup = false;
        curSearchTextLen = 0;
        curSearchText = NULL;
        algorithm.setown(createThorParser(queryCodeContext(), *helper));
        parser.setown(algorithm->createParser(queryCodeContext(), (unsigned)container.queryId(), helper->queryHelper(), helper));
        rowIter = parser->queryResultIter();
        allocator.set(queryRowAllocator());
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    ~CParseSlaveActivity()
    {
        if (helper->searchTextNeedsFree())
            rtlFree(curSearchText);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
    }
    virtual void stop() override
    {
        parser->reset();
        PARENT::stop();
    }
    void processRecord(const void * in)
    {
        if (helper->searchTextNeedsFree())
            rtlFree(curSearchText);

        curSearchTextLen = 0;
        curSearchText = NULL;
        helper->getSearchText(curSearchTextLen, curSearchText, in);

        parser->performMatch(*this, in, curSearchTextLen, curSearchText);
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        while (!abortSoon) {
            if (rowIter->isValid()) {
                anyThisGroup = true;
                OwnedConstThorRow r = rowIter->getRow();
                dataLinkIncrement();
                rowIter->next();
                return r.getClear();
            }
            curRow.setown(inputStream->nextRow());

            if (!curRow) {
                if (anyThisGroup) {
                    anyThisGroup = false;
                    break;
                }
                curRow.setown(inputStream->nextRow());
                if (!curRow)
                    break;
            }

            processRecord(curRow.get());
            rowIter->first();
        }
        
        return NULL;
    }
    virtual bool isGrouped() const override
    { 
        return queryInput(0)->isGrouped();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
    }
// IMatchedAction impl.
    size32_t onMatch(ARowBuilder & rowBuilder, const void * row, IMatchedResults *results, IMatchWalker * walker)
    {
        return helper->transform(rowBuilder, row, results, walker);
    }
};

CActivityBase *createParseSlave(CGraphElementBase *container)
{
    return new CParseSlaveActivity(container);
}



