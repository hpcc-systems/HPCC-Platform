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
#include "mpbase.hpp"
#include "mputil.hpp"
#include "mptag.hpp"

#include "eclhelper.hpp"

#include "thexception.hpp"
#include "thresult.ipp"
#include "deftype.hpp"

class CResultActivityMaster : public CMasterActivity
{
    mptag_t replyTag;
public:
    CResultActivityMaster(CMasterGraphElement *info) : CMasterActivity(info) { }
    virtual void init()
    {
        replyTag = createReplyTag();
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, replyTag);
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(replyTag);
    }
    virtual void process()
    {
        CMasterActivity::process();

        bool results = false;
        unsigned nslaves = container.queryJob().querySlaves();
        while (nslaves--)
        {
            CMessageBuffer mb;
            if (abortSoon || !receiveMsg(mb, RANK_ALL, replyTag, NULL)) break;
            StringBuffer str;
            mb.getSender().getUrlStr(str);
            size32_t sz;
            mb.read(sz);
            if (sz)
            {
                if (results)
                    throw MakeThorException(TE_UnexpectedMultipleSlaveResults, "Received greater than one result from slaves");
                IHThorRemoteResultArg *helper = (IHThorRemoteResultArg *)queryHelper();
                Owned<IRowInterfaces> resultRowIf = createRowInterfaces(helper->queryOutputMeta(), queryActivityId(), queryCodeContext());
                CThorStreamDeserializerSource mds(sz, mb.readDirect(sz));
                RtlDynamicRowBuilder rowBuilder(resultRowIf->queryRowAllocator());
                size32_t sz = resultRowIf->queryRowDeserializer()->deserialize(rowBuilder, mds);
                OwnedConstThorRow result = rowBuilder.finalizeRowClear(sz);
                helper->sendResult(result);
                results = true;
            }
        }
        if (!results && !abortSoon)
        {
            ActPrintLog("WARNING: no results");
            IHThorRemoteResultArg *helper = (IHThorRemoteResultArg *)queryHelper();
            //helper->sendResult(NULL);
            // Jake I think this always cores (so raise exception instead)
            throw MakeThorException(TE_UnexpectedMultipleSlaveResults, "Received no results from slaves");

        }
    }
};


CActivityBase *createResultActivityMaster(CMasterGraphElement *container)
{
    return new CResultActivityMaster(container);
}
