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

#include "thmerge.ipp"
#include "tsorta.hpp"

#ifdef _DEBUG
#define _TRACE
#endif

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
    void init()
    {
        replyTag = createReplyTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(replyTag);
    }
    void process()
    {
        ActPrintLog("GlobalMergeActivityMaster::process");
        CMasterActivity::process();     
        IHThorMergeArg *helper = (IHThorMergeArg *)queryHelper();   
        Owned<IRowInterfaces> rowif = createRowInterfaces(helper->queryOutputMeta(),queryActivityId(),queryCodeContext());
        CThorKeyArray sample(*this, rowif,helper->querySerialize(),helper->queryCompare(),helper->queryCompareKey(),helper->queryCompareRowKey());

        unsigned n = container.queryJob().querySlaves();
        mptag_t *replytags = new mptag_t[n];
        mptag_t *intertags = new mptag_t[n];
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
#ifdef _TRACE
                ActPrintLog("Merge process, Receiving on tag %d",replyTag);
#endif
                rank_t sender;
                if (!receiveMsg(mb, RANK_ALL, replyTag, &sender)||abortSoon) 
                    return;
#ifdef _TRACE
                ActPrintLog("Merge process, Received sample from %d",sender);
#endif
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
                    return;
                CMessageBuffer mb;
                mb.append(n);
                for (unsigned j = 0;j<n;j++)
                    serializeMPtag(mb,intertags[j]);
                sample.serialize(mb);
#ifdef _TRACE
                ActPrintLog("Merge process, Replying to node %d tag %d",i+1,replytags[i]);
#endif
                if (!container.queryJob().queryJobComm().send(mb, (rank_t)i+1, replytags[i]))
                    return;
            }
        
        }
        catch (IException *e) {
            delete [] replytags;
            delete [] intertags;
            ActPrintLog(e, "MERGE");
            throw;
        }
        delete [] replytags;
        delete [] intertags;
        ActPrintLog("GlobalMergeActivityMaster::process exit");
    }

    void abort()
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


