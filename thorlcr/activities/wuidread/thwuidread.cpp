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


#include "thexception.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/wuidread/thwuidread.cpp $ $Id: thwuidread.cpp 62376 2011-02-04 21:59:58Z sort $");

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
        const char *wuid = helper->queryWUID();
        if (wuid)
            queryCodeContext()->getExternalResultRaw(lenData, tempData, wuid, helper->queryName(), helper->querySequence(), helper->queryXmlTransformer(), helper->queryCsvTransformer());
        else
            queryCodeContext()->getResultRaw(lenData, tempData, helper->queryName(), helper->querySequence(), helper->queryXmlTransformer(), helper->queryCsvTransformer());
        msg.clear();
        msg.append(lenData, tempData);
        container.queryJob().queryJobComm().reply(msg);
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
    if (getWorkunitResultFilename(*container, diskFilename, wuReadHelper->queryWUID(), wuReadHelper->queryName(), wuReadHelper->querySequence()))
    {
        Owned<IHThorDiskReadArg> diskReadHelper = createWorkUnitReadArg(diskFilename, LINK(wuReadHelper));
        Owned<CActivityBase> retAct = createDiskReadActivityMaster(container, diskReadHelper);
        return retAct.getClear();
    }
    else
        return new CWorkUnitReadMaster(container);
}
