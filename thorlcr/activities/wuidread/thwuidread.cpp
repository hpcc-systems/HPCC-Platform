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

    virtual void handleSlaveMessage(CMessageBuffer &msg)
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
        msg.append(lenData, tempData);
        queryJobChannel().queryJobComm().reply(msg);
    }
};

static bool getWorkunitResultFilename(CGraphElementBase &container, StringBuffer & diskFilename, const char * wuid, const char * stepname, int sequence)
{
    try
    {
        ICodeContextExt &codeContext = *QUERYINTERFACE(container.queryCodeContext(), ICodeContextExt);
        Owned<IConstWUResult> result;
        if (wuid)
            result.setown(codeContext.getExternalResult(wuid, stepname, sequence));
        else
            result.setown(codeContext.getResultForGet(stepname, sequence));
        if (!result)
            throw MakeThorException(TE_FailedToRetrieveWorkunitValue, "Failed to find value %s:%d in workunit %s", stepname?stepname:"(null)", sequence, wuid?wuid:"(null)");

        SCMStringBuffer tempFilename;
        result->getResultFilename(tempFilename);
        if (tempFilename.length() == 0)
            return false;

        diskFilename.append("~").append(tempFilename.str());
        return true;
    }
    catch (IException * e) 
    {
        StringBuffer text; 
        e->errorMessage(text); 
        e->Release();
        throw MakeThorException(TE_FailedToRetrieveWorkunitValue, "Failed to find value %s:%d in workunit %s [%s]", stepname?stepname:"(null)", sequence, wuid?wuid:"(null)", text.str());
    }
    return false;
}


CActivityBase *createWorkUnitActivityMaster(CMasterGraphElement *container)
{
    StringBuffer diskFilename;
    IHThorWorkunitReadArg *wuReadHelper = (IHThorWorkunitReadArg *)container->queryHelper();
    OwnedRoxieString fromWuid(wuReadHelper->getWUID());
    if (getWorkunitResultFilename(*container, diskFilename, fromWuid, wuReadHelper->queryName(), wuReadHelper->querySequence()))
    {
        Owned<IHThorDiskReadArg> diskReadHelper = createWorkUnitReadArg(diskFilename, LINK(wuReadHelper));
        Owned<CActivityBase> retAct = createDiskReadActivityMaster(container, diskReadHelper);
        return retAct.getClear();
    }
    else
        return new CWorkUnitReadMaster(container);
}
