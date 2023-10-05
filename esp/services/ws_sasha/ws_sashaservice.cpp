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
#include "sashacli.hpp"

using namespace ws_sasha;

constexpr const char* FEATURE_URL = "SashaAccess";

ISashaCommand* CWSSashaEx::setSashaCommand(SashaCommandAction action, bool dfu, SocketEndpoint& serverep)
{
    getSashaServiceEP(serverep, dfu ? dfuwuArchiverType : wuArchiverType, true);
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(action);
    return cmd.getClear();
}

void CWSSashaEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
}

bool CWSSashaEx::onGetVersion(IEspContext& context, IEspGetVersionRequest& req, IEspResultResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.GetVersion: Permission denied.");

        SocketEndpoint ep;
        Owned<ISashaCommand> cmd = setSashaCommand(SCA_GETVERSION, false, ep);
        if (!cmd.get())
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in set Sasha command");

        StringBuffer results;
        runSashaCommand(ep, cmd, nullptr, results, true);
        resp.setResult(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

static void addListWUFilters(IEspListWURequest& req, ISashaCommand* cmd)
{
    if (!isEmptyString(req.getWuid()))
        cmd->addId(req.getWuid());
    else
        cmd->addId("*");
    if (!isEmptyString(req.getCluster()))
        cmd->setCluster(req.getCluster());
    if (!isEmptyString(req.getOwner()))
        cmd->setOwner(req.getOwner());
    if (!isEmptyString(req.getJobName()))
        cmd->setJobName(req.getJobName());
    if (!isEmptyString(req.getState()))
        cmd->setState(req.getState());
    if (!isEmptyString(req.getBeforeDate()))
        cmd->setBefore(req.getBeforeDate());
    if (!isEmptyString(req.getAfterDate()))
        cmd->setAfter(req.getAfterDate());

    cmd->setArchived(req.getArchived());
    cmd->setOnline(req.getOnline());

    if (!isEmptyString(req.getOutputFields()))
        cmd->setOutputFormat(req.getOutputFields());
    cmd->setStart(req.getPageStartFrom());
    int pageSize = req.getPageSize();
    cmd->setLimit(pageSize > 0 ? pageSize : 500);
}
bool CWSSashaEx::onListWU(IEspContext& context, IEspListWURequest& req, IEspResultResponse& resp)
{
    //Notes: GUI may need to show: "This command may take some time - ok to continue?"
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.ListWU: Permission denied.");

        SashaCommandAction action = SCA_LIST;
        if (req.getIncludeDT())
            action = SCA_LISTDT;
        CWUTypes wuType = req.getWUType();

        SocketEndpoint ep;
        Owned<ISashaCommand> cmd = setSashaCommand(action, wuType == CWUTypes_ECL ? false : true, ep);
        if (!cmd.get())
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in set Sasha command");

        addListWUFilters(req, cmd);

        StringBuffer results;
        runSashaCommand(ep, cmd, nullptr, results, true);
        resp.setResult(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CWSSashaEx::onArchiveWU(IEspContext& context, IEspArchiveWURequest& req, IEspResultResponse& resp)
{
    //Notes: GUI may need to show: "This command may take some time - ok to continue?"
    //Notes: GUI may need to show: "N workunits will be restored/archived, Continue (Y/N)?"

    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.ArchiveWU: Permission denied.");

        SocketEndpoint ep;
        Owned<ISashaCommand> cmd = setSashaCommand(SCA_ARCHIVE, req.getWUType() == CWUTypes_ECL ? false : true, ep);
        if (!cmd.get())
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in set Sasha command");

        if (!isEmptyString(req.getWuid()))
            cmd->addId(req.getWuid());
        else
            cmd->addId("*"); //archive all WUs? Do we really need this or a WUID must be specified?

        StringBuffer results;
        runSashaCommand(ep, cmd, nullptr, results, true);
        resp.setResult(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSSashaEx::onRestoreWU(IEspContext& context, IEspRestoreWURequest& req, IEspResultResponse& resp)
{
    //Notes: GUI may need to show: "This command may take some time - ok to continue?"
    //Notes: GUI may need to show: "N workunits will be restored/archived, Continue (Y/N)?"

    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.RestoreWU: Permission denied.");

        SocketEndpoint ep;
        Owned<ISashaCommand> cmd = setSashaCommand(SCA_RESTORE, req.getWUType() == CWUTypes_ECL ? false : true, ep);
        if (!cmd.get())
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in set Sasha command");

        if (!isEmptyString(req.getWuid()))
            cmd->addId(req.getWuid());
        else
            cmd->addId("*");

        StringBuffer results;
        runSashaCommand(ep, cmd, nullptr, results, true);
        resp.setResult(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSSashaEx::onXREF(IEspContext& context, IEspXREFRequest& req, IEspResultResponse& resp)
{
    //Notes: GUI may need to show: "This command may take some time - ok to continue?"

    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_SASHA_ACCESS_DENIED, "WSSashaEx.XREF: Permission denied.");

        const char* cluster = req.getCluster();
        if (isEmptyString(cluster))
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "WSSashaEx.XREF: Cluster name not specified.");

        SocketEndpoint ep;
        Owned<ISashaCommand> cmd = setSashaCommand(SCA_XREF, false, ep);
        if (!cmd.get())
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in set Sasha command");

        cmd->setCluster(cluster);

        StringBuffer results;
        runSashaCommand(ep, cmd, nullptr, results, true);
        resp.setResult(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
