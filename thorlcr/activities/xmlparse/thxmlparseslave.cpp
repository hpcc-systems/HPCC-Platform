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

#include "jlib.hpp"
#include "thorxmlread.hpp"

#include "slave.ipp"
#include "thactivityutil.ipp"
#include "eclrtl.hpp"

class CXmlParseSlaveActivity : public CSlaveActivity, implements IXMLSelect
{
    typedef CSlaveActivity PARENT;

    IHThorXmlParseArg *helper;
    bool eogNext;
    bool anyThisGroup;
    Linked<IColumnProvider> lastMatch;
    char *searchStr;
    Owned<IXMLParse> xmlParser;
    OwnedConstThorRow nxt;
    Owned<IEngineRowAllocator> allocator;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CXmlParseSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorXmlParseArg *> (queryHelper());
        searchStr = NULL;
        appendOutputLinked(this);
    }

// IXMLSelect
    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        lastMatch.set(&entry);
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        allocator.set(queryRowAllocator());
    }
    virtual void kill()
    {
        CSlaveActivity::kill();
        xmlParser.clear();
        if (searchStr && helper->searchTextNeedsFree())
            rtlFree(searchStr);
    }
// IThorDataLink methods
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        anyThisGroup = false;
        eogNext = false;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (abortSoon)
            return NULL;
        if (eogNext)
        {
            eogNext = false;
            anyThisGroup = false;
            return NULL;
        }
        try
        {
            for (;;)
            {
                if (xmlParser)
                {
                    for (;;)
                    {
                        if (!xmlParser->next())
                        {
                            if (helper->searchTextNeedsFree())
                            {
                                rtlFree(searchStr);
                                searchStr = NULL;
                            }
                            xmlParser.clear();
                            break;
                        }
                        if (lastMatch)
                        {
                            RtlDynamicRowBuilder row(allocator);
                            size32_t sizeGot;
                            try { sizeGot = helper->transform(row, nxt, lastMatch); }
                            catch (IException *e) 
                            { 
                                ActPrintLog(e, "In helper->transform()");
                                throw;
                            }
                            lastMatch.clear();
                            if (sizeGot == 0)
                                continue; // not sure if this will ever be possible in this context.
                            dataLinkIncrement();
                            anyThisGroup = true;
                            OwnedConstThorRow ret = row.finalizeRowClear(sizeGot);
                            return ret.getClear();
                        }
                    }
                }
                nxt.setown(inputStream->nextRow());
                if (!nxt && !anyThisGroup)
                    nxt.setown(inputStream->nextRow());
                if (!nxt)
                    break;
                unsigned len;
                helper->getSearchText(len, searchStr, nxt);
                OwnedRoxieString xmlIteratorPath(helper->getXmlIteratorPath());
                xmlParser.setown(createXMLParse(searchStr, len, xmlIteratorPath, *this, ptr_noRoot, helper->requiresContents()));
            }
        }
        catch (IOutOfMemException *e)
        {
            StringBuffer s("XMLParse actId(");
            s.append(container.queryId()).append(") out of memory.").newline();
            s.append("INTERNAL ERROR ").append(e->errorCode()).append(": ");
            e->errorMessage(s);
            e->Release();
            throw MakeActivityException(this, 0, "%s", s.str());
        }
        catch (IException *e)
        {
            StringBuffer s("XMLParse actId(");
            s.append(container.queryId());
            s.append(") INTERNAL ERROR ").append(e->errorCode()).append(": ");
            e->errorMessage(s);
            e->Release();
            throw MakeActivityException(this, 0, "%s", s.str());
        }
        eogNext = false;
        anyThisGroup = false;
        return NULL;
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.fastThrough = true; // ish
    }
};

activityslaves_decl CActivityBase *createXmlParseSlave(CGraphElementBase *container)
{
    return new CXmlParseSlaveActivity(container);
}
