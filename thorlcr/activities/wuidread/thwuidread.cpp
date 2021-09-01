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
#include "thexception.hpp"
#include "thorfile.hpp"
#include "thactivitymaster.ipp"
#include "../diskread/thdiskread.ipp"
#include "thwuidread.ipp"

class CWorkUnitReadMaster : public CMasterActivity
{
public:
    CWorkUnitReadMaster(CMasterGraphElement * info) : CMasterActivity(info) { }

    virtual void handleSlaveMessage(CMessageBuffer &msg) override
    {
        IHThorWorkunitReadArg *helper = (IHThorWorkunitReadArg *)queryHelper();
        size32_t lenData;
        void *tempData;
        OwnedRoxieString fromWuid(helper->getWUID());
        if (fromWuid)
            queryCodeContext()->getExternalResultRaw(lenData, tempData, fromWuid, helper->queryName(), helper->querySequence(), helper->queryXmlTransformer(), helper->queryCsvTransformer());
        else
            queryCodeContext()->getResultRaw(lenData, tempData, helper->queryName(), helper->querySequence(), helper->queryXmlTransformer(), helper->queryCsvTransformer());
        msg.clear();
        msg.setBuffer(lenData, tempData, true);
        queryJobChannel().queryJobComm().reply(msg);
    }
};

CActivityBase *createWorkUnitActivityMaster(CMasterGraphElement *container)
{
    return new CWorkUnitReadMaster(container);
}
