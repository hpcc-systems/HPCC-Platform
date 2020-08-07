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

#include "thmerge.ipp"
#include "tsorta.hpp"

//
// LocalMergeActivityMaster

class LocalMergeActivityMaster : public CMasterActivity
{
public:
    LocalMergeActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
    }
};

    
//
// GlobalMergeActivityMaster

class GlobalMergeActivityMaster : public CMasterActivity
{
    mptag_t replyTag;
public:
    GlobalMergeActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
    }
    virtual void init()
    {
        CMasterActivity::init();
        replyTag = queryMPServer().createReplyTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(replyTag);
    }
    virtual void process()
    {
        CMasterActivity::process();     
        IHThorMergeArg *helper = (IHThorMergeArg *)queryHelper();   
        Owned<IThorRowInterfaces> rowif = createRowInterfaces(helper->queryOutputMeta());
        CThorKeyArray sample(*this, rowif,helper->querySerialize(),helper->queryCompare(),helper->queryCompareKey(),helper->queryCompareRowKey());

        unsigned n = container.queryJob().querySlaves();
        OwnedMalloc<mptag_t> replytags(n);
        OwnedMalloc<mptag_t> intertags(n);
        unsigned i;
        for (i=0;i<n;i++) {
            replytags[i] = TAG_NULL;
            intertags[i] = TAG_NULL;
        }
        try {
            for (i=0;i<n;i++) {
                if (abortSoon)
                    return;
                CMessageBuffer mb;
                ::ActPrintLog(this, thorDetailedLogLevel, "Merge process, Receiving on tag %d",replyTag);
                rank_t sender;
                if (!receiveMsg(mb, RANK_ALL, replyTag, &sender)||abortSoon) 
                    return;
                ::ActPrintLog(this, thorDetailedLogLevel, "Merge process, Received sample from %d",sender);
                sender--;
                assertex((unsigned)sender<n);
                assertex(replytags[(unsigned)sender]==TAG_NULL);
                deserializeMPtag(mb,replytags[(unsigned)sender]);
                deserializeMPtag(mb,intertags[(unsigned)sender]);
                sample.deserialize(mb,true);
            }
            ActPrintLog("GlobalMergeActivityMaster::process samples merged");
            sample.createSortedPartition(n);
            ActPrintLog("GlobalMergeActivityMaster::process partition generated");
            for (i=0;i<n;i++) {
                if (abortSoon)
                    break;
                CMessageBuffer mb;
                mb.append(n);
                for (unsigned j = 0;j<n;j++)
                    serializeMPtag(mb,intertags[j]);
                sample.serialize(mb);
                ::ActPrintLog(this, thorDetailedLogLevel, "Merge process, Replying to node %d tag %d",i+1,replytags[i]);
                if (!queryJobChannel().queryJobComm().send(mb, (rank_t)i+1, replytags[i]))
                    break;
            }
        
        }
        catch (IException *e) {
            ActPrintLog(e, "MERGE");
            throw;
        }
        ActPrintLog("GlobalMergeActivityMaster::process exit");
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, replyTag);
    }
};

    
CActivityBase *createMergeActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new LocalMergeActivityMaster(container);
    else
        return new GlobalMergeActivityMaster(container);
}
