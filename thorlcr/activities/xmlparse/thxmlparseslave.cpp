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

#include "jlib.hpp"
#include "thorxmlread.hpp"

#include "slave.ipp"
#include "thactivityutil.ipp"
#include "eclrtl.hpp"

class CXmlParseSlaveActivity : public CSlaveActivity, public CThorDataLink, implements IXMLSelect
{
    IHThorXmlParseArg *helper;
    bool eogNext;
    bool anyThisGroup;
    Linked<IColumnProvider> lastMatch;
    char *searchStr;
    Owned<IXMLParse> xmlParser;
    OwnedConstThorRow nxt;
    IThorDataLink *input;
    Owned<IEngineRowAllocator> allocator;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CXmlParseSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        searchStr = NULL;
        input = NULL;
    }

// IXMLSelect
    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        lastMatch.set(&entry);
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorXmlParseArg *> (queryHelper());
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
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        anyThisGroup = false;
        eogNext = false;
        input = inputs.item(0);
        startInput(input);
        dataLinkStart("XMLPARSE", container.queryId());
    }
    virtual void stop()
    {
        stopInput(inputs.item(0));
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
            loop
            {
                if (xmlParser)
                {
                    loop
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
                nxt.setown(input->nextRow());
                if (!nxt && !anyThisGroup)
                    nxt.setown(input->nextRow());
                if (!nxt)
                    break;
                unsigned len;
                helper->getSearchText(len, searchStr, nxt);
                xmlParser.setown(createXMLParse(searchStr, len, helper->queryIteratorPath(), *this, xr_noRoot, helper->requiresContents()));
            }
        }
        catch (IOutOfMemException *e)
        {
            StringBuffer s("XMLParse actId(");
            s.append(container.queryId()).append(") out of memory.").newline();
            s.append("INTERNAL ERROR ").append(e->errorCode()).append(": ");
            e->errorMessage(s);
            e->Release();
            throw MakeActivityException(this, 0, s.str());
        }
        catch (IException *e)
        {
            StringBuffer s("XMLParse actId(");
            s.append(container.queryId());
            s.append(") INTERNAL ERROR ").append(e->errorCode()).append(": ");
            e->errorMessage(s);
            e->Release();
            throw MakeActivityException(this, 0, s.str());
        }
        eogNext = false;
        anyThisGroup = false;
        return NULL;
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true; // ish
    }
};

activityslaves_decl CActivityBase *createXmlParseSlave(CGraphElementBase *container)
{
    return new CXmlParseSlaveActivity(container);
}
