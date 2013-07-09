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

class CParseSlaveActivity : public CSlaveActivity, public CThorDataLink, implements IMatchedAction
{
    IHThorParseArg *helper;
    IThorDataLink *input;
    OwnedConstThorRow curRow;
    Owned<INlpParseAlgorithm> algorithm;
    Owned<INlpParser> parser;
    bool anyThisGroup;
    char * curSearchText;
    size32_t curSearchTextLen;
    INlpResultIterator * rowIter;
    Owned<IEngineRowAllocator> allocator;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CParseSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        anyThisGroup = false;
        curSearchTextLen = 0;
        curSearchText = NULL;

    }
    ~CParseSlaveActivity()
    {
        if (helper->searchTextNeedsFree())
            rtlFree(curSearchText);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = (IHThorParseArg *)queryHelper();
        algorithm.setown(createThorParser(queryCodeContext(), *helper));
        parser.setown(algorithm->createParser(queryCodeContext(), (unsigned)container.queryId(), helper->queryHelper(), helper));
        rowIter = parser->queryResultIter();
        rowIter->first();
        allocator.set(queryRowAllocator());
    } 
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);
        dataLinkStart();
    }
    void stop()
    { 
        stopInput(input);
        dataLinkStop();
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
        ActivityTimer t(totalCycles, timeActivities, NULL);
        while (!abortSoon) {
            if (rowIter->isValid()) {
                anyThisGroup = true;
                OwnedConstThorRow r = rowIter->getRow();
                dataLinkIncrement();
                rowIter->next();
                return r.getClear();
            }
            curRow.setown(input->nextRow());

            if (!curRow) {
                if (anyThisGroup) {
                    anyThisGroup = false;
                    break;
                }
                curRow.setown(input->nextRow());
                if (!curRow)
                    break;
            }

            processRecord(curRow.get());
            rowIter->first();
        }
        
        return NULL;
    }
    bool isGrouped()
    { 
        return inputs.item(0)->isGrouped();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
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



