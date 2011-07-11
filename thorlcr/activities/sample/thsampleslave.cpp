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


#include "thsampleslave.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/sample/thsampleslave.cpp $ $Id: thsampleslave.cpp 62376 2011-02-04 21:59:58Z sort $");


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
        dataLinkStart("SAMPLE", container.queryId());
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


