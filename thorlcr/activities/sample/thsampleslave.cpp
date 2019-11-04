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


#include "thsampleslave.ipp"

class SampleSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorSampleArg * helper;
    unsigned numSamples, whichSample, numToSkip;
    bool anyThisGroup;
    bool eogNext;

public:
    SampleSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorSampleArg *> (queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eogNext = false;
        anyThisGroup = false;
        numSamples = helper->getProportion();
        whichSample = helper->getSampleNumber();
        numToSkip = whichSample ? whichSample - 1 : 0;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        while(!abortSoon)
        {
            OwnedConstThorRow row = inputStream->nextRow();
            if(!row)    {
                numToSkip = whichSample ? whichSample - 1 : 0;
                if(anyThisGroup) {
                    anyThisGroup = false;           
                    break;
                }
                row.setown(inputStream->nextRow());
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
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        info.fastThrough = true;
        calcMetaInfoSize(info, queryInput(0));
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
};

CActivityBase *createSampleSlave(CGraphElementBase *container)
{
    return new SampleSlaveActivity(container);
}


