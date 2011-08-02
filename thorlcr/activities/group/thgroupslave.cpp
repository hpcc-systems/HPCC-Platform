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

#include "thgroupslave.ipp"

#include "thactivityutil.ipp"
#include "thorport.hpp"

class GroupSlaveActivity : public CSlaveActivity, public CThorDataLink
{

private:
    IHThorGroupArg * helper;
    bool eogNext, prevEog, eof;
    unsigned short transferAcceptPort;
    Owned<CGroupTransfer> rollover;
    bool lastNode;
    bool rolloverEnabled;
    IThorDataLink *input;
    OwnedConstThorRow next;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    
    GroupSlaveActivity(CGraphElementBase *_container, bool _rollover) 
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        rolloverEnabled = _rollover;
        transferAcceptPort = allocPort(1);      // for transfer 
    }
    ~GroupSlaveActivity()
    {
        if (rollover)
            rollover->abort();
        freePort(transferAcceptPort);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);

        helper = static_cast <IHThorGroupArg *> (queryHelper());

        if (rolloverEnabled)
        {
            SocketEndpoint ep;
            ep.setLocalHost(transferAcceptPort);
            ep.serialize(slaveData);
        }
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        ActPrintLog(rolloverEnabled ? "GROUP: is global" : "GROUP: is local");
        eogNext = prevEog = eof = false;
        if(rolloverEnabled)
        {
#ifdef _TESTING
            ActPrintLog("Node number = %d, Total Nodes = %d", container.queryJob().queryMyRank(), container.queryJob().querySlaves());
#endif
            lastNode = (container.queryJob().queryMyRank() == container.queryJob().querySlaves());

            rollover.clear(); // JCSMORE - should be albe to reuse the CGroupTranser obj.
            rollover.setown(new CGroupTransfer(&container, queryRowAllocator(), queryRowSerializer(), queryRowDeserializer(), transferAcceptPort));
        }

        input = inputs.item(0);
        startInput(input);
        dataLinkStart("GROUP", container.queryId());        

        getNext(); // prime inputBuffer

        if (rolloverEnabled&&(container.queryJob().queryMyRank() > 1))  // 1st node can have nothing to send
        {
            CThorRowArray sendGroup;
            sendGroup.setSizing(true,true);
            rowcount_t sentRecs = 0;
            if (next)
            {
                ActPrintLog("GROUP: Sending first group to previous node(%d)", container.queryJob().queryMyRank()-1);
                try
                {
                    do                          // 1st group goes to rollover
                    {
                        sendGroup.append(next.getClear()); 
                        if (abortSoon) {
                            break; //always send group even when aborting
                        }
                        sentRecs++;
                    } while (getNext() && helper->isSameGroup(sendGroup.item(sendGroup.ordinality()-1), next));
                }
                catch (IThorRowArrayException *e)
                {
                    IException *e2 = MakeActivityException(this, e, "Group (%"ACTPF"d) [during rollover]", container.queryId());
                    e->Release();
                    throw e2;
                }
            }                       
            MemoryBuffer mb;
            getInitializationData(container.queryJob().queryMyRank()-1-1, mb);
            SocketEndpoint ep;
            ep.deserialize(mb);
            rollover->send(ep,sendGroup);   
            ActPrintLog("GROUP: %"RCPF"d records sent", sentRecs);
        }
    }


    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }


    bool getNext()
    {
        next.setown(input->ungroupedNextRow());
        if(next) 
            return true;
        if (rolloverEnabled && !lastNode) {
            next.setown(rollover->nextRow());
            if (next) 
                return true;
        }
        return false;
    }


    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eogNext || eof)
        {
            eogNext = false;
            return NULL;
        }
        
        OwnedConstThorRow prev = next.getClear();
        if( getNext() && !helper->isSameGroup(prev, next)) 
            eogNext = true;
        if (prev)
        {
            dataLinkIncrement();
            return prev.getClear();
        }
        if(prevEog) 
            eof = true;
        prevEog = true;
        return NULL;
    }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        if (rolloverEnabled) {
            info.isSequential = true;
            info.unknownRowsOutput = true; // don't know how many rolled over
        }
        calcMetaInfoSize(info,inputs.item(0));
    }

    virtual bool isGrouped() { return true; }
};


CActivityBase *createGroupSlave(CGraphElementBase *container)
{
    return new GroupSlaveActivity(container, true);
}


CActivityBase *createLocalGroupSlave(CGraphElementBase *container)
{
    return new GroupSlaveActivity(container, false);
}
