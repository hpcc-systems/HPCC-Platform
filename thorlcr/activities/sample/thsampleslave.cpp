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


#include "thsampleslave.ipp"

class SampleSlaveActivity : public CSlaveActivity, public CThorDataLink
{

    IHThorSampleArg * helper;
    unsigned numSamples, whichSample, numToSkip;
    bool anyThisGroup;
    bool eogNext;
    IThorDataLink *input;


public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SampleSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorSampleArg *> (queryHelper());
    }


    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);
        eogNext = false;
        anyThisGroup = false;
        numSamples = helper->getProportion();
        whichSample = helper->getSampleNumber();
        numToSkip = whichSample ? whichSample - 1 : 0;
        dataLinkStart();
    }


    void stop()
    {
        dataLinkStop();
        stopInput(input);
    }


    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        while(!abortSoon)
        {
            OwnedConstThorRow row = input->nextRow();
            if(!row)    {
                numToSkip = whichSample ? whichSample - 1 : 0;
                if(anyThisGroup) {
                    anyThisGroup = false;           
                    break;
                }
                row.setown(input->nextRow());
                if(!row) 
                    break;
            }
            if(numToSkip == 0) {
                anyThisGroup = true;
                numToSkip = numSamples - 1;
                dataLinkIncrement();
                return row.getClear();
            }
            numToSkip--;
        }
        return NULL;
    }


    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        info.fastThrough = true;
        calcMetaInfoSize(info,inputs.item(0));
    }

    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
};

CActivityBase *createSampleSlave(CGraphElementBase *container)
{
    return new SampleSlaveActivity(container);
}


