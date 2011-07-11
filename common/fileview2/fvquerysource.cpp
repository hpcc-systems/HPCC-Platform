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

#include "jliball.hpp"
#include "eclrtl.hpp"

#include "hqlexpr.hpp"
#include "hqlthql.hpp"
#include "fvresultset.ipp"
#include "fileview.hpp"
#include "fvquerysource.ipp"

#include "fvwugen.hpp"


QueryDataSource::QueryDataSource(IConstWUResult * _wuResult, const char * _wuid, const char * _username, const char * _password)
{
    wuResult.set(_wuResult);
    wuid.set(_wuid);
    username.set(_username);
    password.set(_password);
}


QueryDataSource::~QueryDataSource()
{
    if (browseWuid)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        factory->deleteWorkUnit(browseWuid);
    }
}

bool QueryDataSource::createBrowseWU()
{
    StringAttr dataset, datasetDefs;
    StringAttrAdaptor a1(dataset), a2(datasetDefs);
    wuResult->getResultDataset(a1, a2);

    if (!dataset || !datasetDefs)
        return false;
    StringBuffer fullText;
    fullText.append(datasetDefs).append(dataset);
    OwnedHqlExpr parsed = parseQuery(fullText.str());
    if (!parsed)
        return false;

    HqlExprAttr selectFields = parsed.getLink();
    if (selectFields->getOperator() == no_output)
        selectFields.set(selectFields->queryChild(0));

    OwnedHqlExpr browseWUcode = buildQueryViewerEcl(selectFields);
    if (!browseWUcode)
        return false;
    returnedRecord.set(browseWUcode->queryChild(0)->queryRecord());

    StringAttr tempAttr;
    StringAttrAdaptor temp(tempAttr);
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> parent = factory->openWorkUnit(wuid, false);

    SCMStringBuffer user;
    StringAttrAdaptor acluster(cluster);
    parent->getClusterName(acluster);
    parent->getUser(user);

    Owned<IWorkUnit> workunit = factory->createWorkUnit(NULL, "fileViewer", user.str());
    workunit->setUser(user.str());
    workunit->setClusterName(cluster);
    workunit->setCustomerId(parent->getCustomerId(temp).str());
    workunit->setCompareMode(CompareModeOff);   // ? parent->getCompareMode()
    StringAttrAdaptor bwa(browseWuid); workunit->getWuid(bwa);

    workunit->setDebugValueInt("importImplicitModules", false, true);
    workunit->setDebugValueInt("importAllModules", false, true);
    workunit->setDebugValueInt("forceFakeThor", 1, true);

    StringBuffer jobName;
    jobName.append("FileView for ").append(wuid).append(":").append("x");
    workunit->setJobName(jobName.str());

    StringBuffer eclText;
    toECL(browseWUcode, eclText, true);
    Owned<IWUQuery> query = workunit->updateQuery();
    query->setQueryText(eclText.str());
    query->setQueryName(jobName.str());

    return true;
}


bool QueryDataSource::init()
{
    return createBrowseWU();
}



void QueryDataSource::improveLocation(__int64 row, RowLocation & location)
{
#if 0
    if (!diskMeta->isFixedSize())
        return;

    if (location.bestRow <= row && location.bestRow + DISKREAD_PAGE_SIZE > row)
        return;

    assertex(row >= 0);
    //Align the row so the chunks don't overlap....
    location.bestRow = (row / DISKREAD_PAGE_SIZE) * DISKREAD_PAGE_SIZE;
    location.bestOffset = location.bestRow * diskMeta->fixedSize();
#endif
}




bool QueryDataSource::loadBlock(__int64 startRow, offset_t startOffset)
{
    MemoryBuffer temp;

    //enter scope....>
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> wu = factory->updateWorkUnit(browseWuid);
        Owned<IWUResult> lower = wu->updateVariableByName(LOWER_LIMIT_ID);
        lower->setResultInt(startOffset);
        lower->setResultStatus(ResultStatusSupplied);

        Owned<IWUResult> dataResult = wu->updateResultBySequence(0);
        dataResult->setResultRaw(0, NULL, ResultFormatRaw);
        dataResult->setResultStatus(ResultStatusUndefined);
        wu->clearExceptions();
        if (wu->getState() != WUStateUnknown)
            wu->setState(WUStateCompiled);

        //Owned<IWUResult> count = wu->updateVariableByName(RECORD_LIMIT_ID);
        //count->setResultInt64(fetchSize);
    }

    //Resubmit the query...
    submitWorkUnit(browseWuid, username, password);
    WUState finalState = waitForWorkUnitToComplete(browseWuid, -1, true);
    if(!((finalState == WUStateCompleted) || (finalState == WUStateWait)))
        return false;

    //Now extract the results...
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(browseWuid, false);
    Owned<IConstWUResult> dataResult = wu->getResultBySequence(0);
    MemoryBuffer2IDataVal xxx(temp); dataResult->getResultRaw(xxx, NULL, NULL);

    if (temp.length() == 0)
        return false;

    RowBlock * rows;
    if (returnedMeta->isFixedSize())
        rows = new FilePosFixedRowBlock(temp, startRow, startOffset, returnedMeta->fixedSize());
    else
        rows = new FilePosVariableRowBlock(temp, startRow, startOffset, returnedMeta, true);
    cache.addRowsOwn(rows);
    return true;
}
