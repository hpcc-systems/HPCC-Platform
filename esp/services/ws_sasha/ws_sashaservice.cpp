/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems.

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

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

#include "ws_sashaservice.hpp"
#include "jlib.hpp"
#include "TpWrapper.hpp"

constexpr const char* FEATURE_URL = "SashaAccess";

ISashaCmdExecutor* CWSSashaEx::createSashaCommandExecutor(const char* archiverType)
{
    SocketEndpoint ep;
    getSashaServiceEP(ep, archiverType, true);
    return createSashaCmdExecutor(ep);
}

ISashaCmdExecutor* CWSSashaEx::querySashaCommandExecutor(CWUTypes wuType)
{
    CriticalBlock block(sect);
    if (wuType == CWUTypes_ECL)
    {
        if (!eclWUSashaCommandExecutor)
            eclWUSashaCommandExecutor.setown(createSashaCommandExecutor(wuArchiverType));
        return eclWUSashaCommandExecutor.get();
    }

    if (!dfuWUSashaCommandExecutor)
        dfuWUSashaCommandExecutor.setown(createSashaCommandExecutor(dfuwuArchiverType));
    return dfuWUSashaCommandExecutor.get();
}

void CWSSashaEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
}

bool CWSSashaEx::onGetVersion(IEspContext& context, IEspGetVersionRequest& req, IEspResultResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.GetVersion: Permission denied.");

        StringBuffer results;
        querySashaCommandExecutor(CWUTypes_ECL)->getVersion(results);
        resp.setResult(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CWSSashaEx::setSuccess(ISashaCmdExecutor* executor, IEspResultResponse& resp)
{
    StringBuffer results;
    executor->getLastServerMessage(results);
    resp.setResult(results);
}

void CWSSashaEx::setFailure(const char* wuid, const char* action, IEspResultResponse& resp)
{
    StringBuffer results;
    if (isEmptyString(wuid))
        results.appendf("No workunits %s.", action);
    else
        results.appendf("Workunit %s not %s", wuid, action);
    resp.setResult(results);
}

bool CWSSashaEx::onArchiveWU(IEspContext& context, IEspArchiveWURequest& req, IEspResultResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.ArchiveWU: Permission denied.");

        const char* wuid = req.getWuid();
        CWUTypes wuType = req.getWUType();

        ISashaCmdExecutor* executor = querySashaCommandExecutor(wuType);
        bool sashaCmdSuccess = false;
        if (req.getDeleteOnSuccess())
        {
            if (wuType == CWUTypes_ECL)
                sashaCmdSuccess = executor->archiveECLWorkUnit(isEmptyString(wuid) ? "*" : wuid);
            else
                sashaCmdSuccess = executor->archiveDFUWorkUnit(isEmptyString(wuid) ? "*" : wuid);
        }
        else
        {
            if (wuType == CWUTypes_ECL)
                sashaCmdSuccess = executor->backupECLWorkUnit(isEmptyString(wuid) ? "*" : wuid);
            else
                sashaCmdSuccess = executor->backupDFUWorkUnit(isEmptyString(wuid) ? "*" : wuid);
        }
        if (sashaCmdSuccess)
            setSuccess(executor, resp);
        else
            setFailure(wuid, "archived", resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSSashaEx::onRestoreWU(IEspContext& context, IEspRestoreWURequest& req, IEspResultResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.RestoreWU: Permission denied.");

        const char* wuid = req.getWuid();
        CWUTypes wuType = req.getWUType();

        ISashaCmdExecutor* executor = querySashaCommandExecutor(wuType);
        bool sashaCmdSuccess = false;
        if (wuType == CWUTypes_ECL)
            sashaCmdSuccess = executor->restoreECLWorkUnit(isEmptyString(wuid) ? "*" : wuid);
        else
            sashaCmdSuccess = executor->restoreDFUWorkUnit(isEmptyString(wuid) ? "*" : wuid);
        if (sashaCmdSuccess)
            setSuccess(executor, resp);
        else
            setFailure(wuid, "restored", resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSSashaEx::onListWU(IEspContext& context, IEspListWURequest& req, IEspResultResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.ListWU: Permission denied.");

        Owned<ListWURequests> listWURequests = new ListWURequests(req.getOutputFields(), req.getArchived(), req.getOnline());
        const char* wuid = req.getWuid();
        listWURequests->wuid.set(wuid);
        listWURequests->cluster.set(req.getCluster());
        listWURequests->owner.set(req.getOwner());
        listWURequests->jobName.set(req.getJobName());
        listWURequests->state.set(req.getState());
        normalizeDateReq(req.getFromDate(), listWURequests->fromDate);
        normalizeDateReq(req.getToDate(), listWURequests->toDate);
        listWURequests->includeDT = req.getIncludeDT();
        listWURequests->beforeWU.set(req.getBeforeWU());
        listWURequests->afterWU.set(req.getAfterWU());
        listWURequests->maxNumberWUs = req.getMaxNumberWUs();
        listWURequests->descending = req.getDescending();

        CWUTypes wuType = req.getWUType();
        ISashaCmdExecutor* executor = querySashaCommandExecutor(wuType);

        bool sashaCmdSuccess = false;
        if (wuType == CWUTypes_ECL)
            sashaCmdSuccess = executor->listECLWorkUnit(listWURequests);
        else
            sashaCmdSuccess = executor->listDFUWorkUnit(listWURequests);
        if (sashaCmdSuccess)
            setSuccess(executor, resp);
        else
            setFailure(wuid, "found", resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CWSSashaEx::normalizeDateReq(const char* dateString, StringBuffer& date)
{
    if (isEmptyString(dateString))
        return;

    if (strlen(dateString) == 8)
    { //Assuming the dateString is in YYYYMMDD format
        date.set(dateString);
        return;
    }

    CDateTime dt;
    dt.setString(dateString);

    unsigned year, month, day, hour, minute, second, nano;
    dt.getDate(year, month, day, true);
    date.setf("%04d%02d%02d", year, month, day);
}
