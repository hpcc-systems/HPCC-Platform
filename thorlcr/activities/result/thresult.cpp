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
#include "mpbase.hpp"
#include "mputil.hpp"
#include "mptag.hpp"

#include "eclhelper.hpp"

#include "thmem.hpp"
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
        CMasterActivity::init();
        replyTag = queryMPServer().createReplyTag();
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
                Owned<IRowInterfaces> resultRowIf = createRowInterfaces(helper->queryOutputMeta(), queryId(), queryCodeContext());
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
