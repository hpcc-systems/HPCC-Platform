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

#pragma warning (disable : 4786)
#pragma warning (disable : 4297)  // function assumed not to throw an exception but does

#include "platform.h"
#include "fileservices.hpp"
#include "workunit.hpp"
#include "jio.hpp"
#include "jmisc.hpp"
#include "daft.hpp"
#include "dasess.hpp"
#include "dautils.hpp"
#include "daaudit.hpp"
#include "dfuwu.hpp"
#include "ws_fs_esp.ipp"
#include "rmtsmtp.hpp"
#include "dfuplus.hpp"
#include "daclient.hpp"
#include "dasds.hpp"
#include "enginecontext.hpp"
#include "environment.hpp"
#include "ws_dfsclient.hpp"

#define USE_DALIDFS
#define SDS_LOCK_TIMEOUT  10000

#define FILESERVICES_VERSION "FILESERVICES 2.1.4"

static const char * compatibleVersions[] = {
    "FILESERVICES 2.1  [a68789cfb01d00ef6dc362e52d5eac0e]", // linux version
    "FILESERVICES 2.1.1",
    "FILESERVICES 2.1.2",
    "FILESERVICES 2.1.3",
    "FILESERVICES 2.1.4",
    NULL };

static const char * EclDefinition = nullptr;//Definitions specified in lib_fileservices.ecllib

#define WAIT_SECONDS 30

FILESERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = FILESERVICES_VERSION;
    pb->moduleName = "lib_fileservices";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "FileServices library";

    return true;
}

namespace nsFileservices {

    IPluginContext * parentCtx = NULL;

static IConstWorkUnit * getWorkunit(ICodeContext * ctx)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    StringAttr wuid;
    wuid.setown(ctx->getWuid());
    return factory->openWorkUnit(wuid);
}

static void setWorkunitState(ICodeContext * ctx, WUState state, const char * msg)
{
    Owned<IWorkUnit> wu = ctx->updateWorkUnit();
    if (wu)
    {
        wu->setState(state);//resets stateEx
        if (msg)
            wu->setStateEx(msg);
        wu->commit();
    }
}

#ifndef _CONTAINERIZED
static IConstEnvironment * openDaliEnvironment()
{
    if (daliClientActive())
    {
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
        return factory->openEnvironment();
    }
    return NULL;
}

static IPropertyTree *getEnvironmentTree(IConstEnvironment * daliEnv)
{
    if (daliEnv)
        return &daliEnv->getPTree(); // No need to clone since daliEnv ensures connection stays alive.
    return getHPCCEnvironment();
}
#endif

static void setServerAccess(CClientFileSpray &server, IConstWorkUnit * wu)
{
    SCMStringBuffer token;
    wu->getWorkunitDistributedAccessToken(token);
    server.setUsernameToken(wu->queryUser(), token.str(), "");//use workunit token as password
}

static StringArray availableWsFS;
static CriticalSection espURLcrit;

static void addConfiguredWsFSUrl(const char * url)
{
    CriticalBlock b(espURLcrit);
    availableWsFS.appendUniq(url);
}

static void setContainerLocalCertificate(IEspClientRpcSettings &rpc)
{
#ifdef _CONTAINERIZED
    //will only affect HTTPS
    rpc.setMtlsSecretName("local");
#endif
}

static bool contactWsFS(const char * espurl, IConstWorkUnit * wu)
{
    CClientFileSpray server;
    server.addServiceUrl(espurl);
    setServerAccess(server, wu);

    try
    {
        Owned<IClientEchoDateTime> req = server.createEchoDateTimeRequest();
        setContainerLocalCertificate(req->rpc());
        Owned<IClientEchoDateTimeResponse> result = server.EchoDateTime(req);

        if (result->getResult())
            return true;
    }
    catch(IException *ie)
    {
        StringBuffer error;
        ie->errorMessage(error);
        PROGLOG("Could not contact WsFS: '%s': %s",espurl, error.str());
        ie->Release();
    }
    catch(...)
    {
        PROGLOG("Could not contact WsFS: '%s'",espurl);
    }
    return false;
}

static const char * getNextAliveWsFSURL(IConstWorkUnit * wu)
{
    CriticalBlock b(espURLcrit);

    for (int index = 0; index < availableWsFS.length(); index++)
    {
        const char * currentUrl = availableWsFS.item(index);
        if (contactWsFS(currentUrl, wu))
            return currentUrl;
    }

    return nullptr;
}

static bool isUrlListEmpty()
{
    CriticalBlock b(espURLcrit);
    return availableWsFS.length() == 0;
}

static const char *getAccessibleEspServerURL(const char *param, IConstWorkUnit * wu)
{
    if (param&&*param)
        return param;

    CriticalBlock b(espURLcrit);
    if (isUrlListEmpty())
    {
#ifdef _CONTAINERIZED
        // Look for 'eclservices' esp service, fallback to 'eclwatch' service.
        Owned<IPropertyTree> globalConfig = getGlobalConfig();
        Owned<IPropertyTreeIterator> iter = globalConfig->getElements("services");
        IPropertyTree * match = nullptr;
        ForEach(*iter)
        {
            IPropertyTree & cur = iter->query();
            //Ecl queries should be connecting to the internal service - not the public services.
            if (cur.getPropBool("@public"))
                continue;

            const char *type = cur.queryProp("@type");
            if (streq("eclservices", type))
            {
                match = &cur;
                break;
            }
            else if (streq("eclwatch", type))
                match = &cur;
        }

        if (match)
        {
            const char * espService = match->queryProp("@name");
            const char *protocol = match->getPropBool("@tls") ? "https" : "http";
            unsigned port = match->getPropInt("@port", 8010);

            VStringBuffer espURL("%s://%s:%u/FileSpray", protocol, espService, port);
            addConfiguredWsFSUrl(espURL.str());
        }
#else
        Owned<IConstEnvironment> daliEnv = openDaliEnvironment();
        Owned<IPropertyTree> env = getEnvironmentTree(daliEnv);

        if (env.get())
        {
            StringBuffer wsFSUrl;
            StringBuffer espInstanceComputerName;
            StringBuffer bindingProtocol;
            StringBuffer xpath;
            StringBuffer instanceAddress;
            StringBuffer espServiceType;

            Owned<IPropertyTreeIterator> espProcessIter = env->getElements("Software/EspProcess");
            ForEach(*espProcessIter)
            {
                Owned<IPropertyTreeIterator> espBindingIter = espProcessIter->query().getElements("EspBinding");
                ForEach(*espBindingIter)
                {
                    espBindingIter->query().getProp("@service",wsFSUrl.clear());
                    xpath.setf("Software/EspService[@name=\"%s\"]/Properties/@type", wsFSUrl.str());

                    if(env->getProp(xpath.str(), espServiceType.clear()))
                    {
                        if (!espServiceType.isEmpty() && (strieq(espServiceType.str(),"WsSMC")|| strieq(espServiceType.str(),"FileSpray_Serv")))
                        {
                            if (espBindingIter->query().getProp("@protocol",bindingProtocol.clear()))
                            {
                                Owned<IPropertyTreeIterator> espInstanceIter = espProcessIter->query().getElements("Instance");
                                ForEach(*espInstanceIter)
                                {
                                    if (espInstanceIter->query().getProp("@computer",espInstanceComputerName.clear()))
                                    {
                                        xpath.setf("Hardware/Computer[@name=\"%s\"]/@netAddress",espInstanceComputerName.str());
                                        if (env->getProp(xpath.str(),instanceAddress.clear()))
                                        {
                                            wsFSUrl.setf("%s://%s:%d/FileSpray", bindingProtocol.str(), instanceAddress.str(), espBindingIter->query().getPropInt("@port",8010)); // FileSpray seems to be fixed
                                            addConfiguredWsFSUrl(wsFSUrl.str());
                                        }
                                    }
                                }
                            }
                        }//EclWatch || ws_fs binding
                    }
                }//ESPBinding
            }//ESPProcess
        }
#endif

        if (isUrlListEmpty())
            throw MakeStringException(-1,"Could not find any WS FileSpray in the target HPCC configuration.");
    }

    const char * nextWsFSUrl = getNextAliveWsFSURL(wu);
    if (!nextWsFSUrl||!*nextWsFSUrl)
        throw MakeStringException(-1,"Could not contact any of the configured WS FileSpray instances, check HPCC configuration and system health.");

    PROGLOG("FileServices: Targeting ESP WsFileSpray URL: %s", nextWsFSUrl);

    return nextWsFSUrl;
}

StringBuffer & constructLogicalName(IConstWorkUnit * wu, const char * partialLogicalName, StringBuffer & result)
{
    if (partialLogicalName == NULL)
        throw MakeStringException(0, "Logical Name Cannot be blank");

    if (*partialLogicalName == '~')
        ++partialLogicalName;
    else if (wu)
    {
        StringBuffer prefix;
        wu->getScope(StringBufferAdaptor(prefix));
        if (prefix.length())
            result.append(prefix).append("::");
    }
    result.append(partialLogicalName);
    if ((result.length()>0)&&(strstr(result.str(),"::")==NULL)&&(result.charAt(0)!='#'))
        result.insert(0,".::");
    return result;
}

StringBuffer & constructLogicalName(ICodeContext * ctx, const char * partialLogicalName, StringBuffer & result)
{
    Owned<IConstWorkUnit> wu;
    if (partialLogicalName&&(*partialLogicalName != '~'))
        wu.setown(getWorkunit(ctx));
    return constructLogicalName(wu, partialLogicalName, result);
}

static void WUmessage(ICodeContext *ctx, ErrorSeverity sev, const char *fn, const char *msg)
{
    StringBuffer s("fileservices");
    if (fn)
        s.append(", ").append(fn);
    ctx->addWuExceptionEx(msg, 0, sev, MSGAUD_audit, s.str()); // use plain code context
    return;
}

static void AuditMessage(ICodeContext *ctx,
                         const char *func,
                         const char *lfn1,
                         const char *lfn2=NULL)
{
    // FileServices,WUID,user,function,LFN1,LFN2
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    StringBuffer aln;
    StringAttr wuid;
    wuid.setown(ctx->getWuid());
    aln.append(",FileAccess,FileServices,").append(func).append(',').append(wuid).append(',');
    if (udesc)
        udesc->getUserName(aln);
    if (lfn1&&*lfn1) {
        aln.append(',').append(lfn1);
        if (lfn2&&*lfn2) {
            aln.append(',').append(lfn2);
        }
    }
    LOG(MCauditInfo,"%s",aln.str());
}


}//namespace
using namespace nsFileservices;

FILESERVICES_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

FILESERVICES_API char * FILESERVICES_CALL fsGetBuildInfo(void)
{
    return CTXSTRDUP(parentCtx, FILESERVICES_VERSION);
}

//-------------------------------------------------------------------------------------------------------------------------------------------

FILESERVICES_API void FILESERVICES_CALL fsDeleteLogicalFile(ICodeContext *ctx, const char *name,bool ifexists)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);

    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    StringBuffer uname;
    DBGLOG("Deleting NS logical file %s for user %s", lfn.str(),udesc?udesc->getUserName(uname).str():"");
    if (queryDistributedFileDirectory().removeEntry(lfn.str(),udesc,transaction, INFINITE, true))
    {
        StringBuffer s("DeleteLogicalFile ('");
        s.append(lfn);
        if (transaction->active())
            s.append("') added to transaction");
        else
            s.append("') done");
        WUmessage(ctx,SeverityInformation,NULL,s.str());
        AuditMessage(ctx,"DeleteLogicalFile",lfn.str());

    }
    else if (!ifexists)
    {
        throw MakeStringException(0, "Could not delete file %s", lfn.str());
    }
}


FILESERVICES_API bool FILESERVICES_CALL fsFileExists(ICodeContext *ctx, const char *name, bool physical)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);
    if (physical)
        return queryDistributedFileDirectory().existsPhysical(lfn.str(),ctx->queryUserDescriptor());
    return wsdfs::exists(lfn.str(), ctx->queryUserDescriptor(), false, false, INFINITE);
}

FILESERVICES_API bool FILESERVICES_CALL fsFileValidate(ICodeContext *ctx, const char *name)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);

    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = wsdfs::lookup(lfn.str(), udesc, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser, INFINITE);
    if (df)
    {
        Owned<IDistributedFilePartIterator> partIter = df->getIterator();
        ForEach(*partIter)
        {
            IDistributedFilePart & part = partIter->query();
            unsigned numCopies = part.numCopies();
            bool gotone = false;
            offset_t partSize = (offset_t)-1;
            for (unsigned copy=0; copy < numCopies; copy++)
            {
                RemoteFilename remote;
                part.getFilename(remote,copy);
                OwnedIFile file = createIFile(remote);

                if (file->exists())
                {
                    offset_t thisSize = file->size();
                    if (gotone && (partSize != thisSize))
                        throw MakeStringException(0, "Inconsistent file sizes for %s", lfn.str());
                    partSize = thisSize;
                    gotone = true;
                }
            }
            if (!gotone)
                return false;
        }
        return true;
    }

    return false;
}

FILESERVICES_API void FILESERVICES_CALL fsSetReadOnly(ICodeContext *ctx, const char *name, bool ro)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);

    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IException> error;
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc, AccessMode::tbdWrite, false, false, nullptr, defaultPrivilegedUser);
    if (df)
    {
        LOG(MCauditInfo, "Set ReadOnly:  %s", name);
        Owned<IDistributedFilePartIterator> partIter = df->getIterator();
        ForEach(*partIter)
        {
            IDistributedFilePart & part = partIter->query();
            unsigned numCopies = part.numCopies();
            for (unsigned copy=0; copy < numCopies; copy++)
            {
                RemoteFilename remote;
                part.getFilename(remote,copy);
                OwnedIFile file = createIFile(remote);

                try
                {
                    file->setReadOnly(ro);
                }
                catch (IException * e)
                {
                    EXCLOG(e);
                    e->Release();
                }
            }
        }
        return;
    }

    if (!error)
        error.setown(MakeStringException(0, "Could not find logical file %s", lfn.str()));
    throw error.getClear();
}

FILESERVICES_API void FILESERVICES_CALL implementRenameLogicalFile(ICodeContext *ctx, const char *oldname, const char *newname, const bool overwrite)
{
    StringBuffer oldLfn, newLfn;
    constructLogicalName(ctx, oldname, oldLfn);
    constructLogicalName(ctx, newname, newLfn);

    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    IDistributedFileDirectory &distributedDirectory = queryDistributedFileDirectory();

    if (!distributedDirectory.exists(oldLfn.str(), udesc, false, false))
        throw MakeStringException(0, "Old file %s doesn't exists.", oldLfn.str());

    if (overwrite && distributedDirectory.exists(newLfn.str(), udesc, false, false))
        fsDeleteLogicalFile(ctx, newname, true);

    try {
        distributedDirectory.renamePhysical(oldLfn.str(), newLfn.str(), udesc, transaction);
        StringBuffer s("RenameLogicalFile ('");
        s.append(oldLfn).append(", '").append(newLfn).append("') done");
        WUmessage(ctx, SeverityInformation, NULL, s.str());
        AuditMessage(ctx, "RenameLogicalFile", oldLfn.str(), newLfn.str());
    }
    catch (IException *e)
    {
        StringBuffer s;
        e->errorMessage(s);
        WUmessage(ctx, SeverityWarning, "RenameLogicalFile", s.str());
        throw e;
     }
}

FILESERVICES_API void FILESERVICES_CALL fsRenameLogicalFile(ICodeContext *ctx, const char *oldname, const char *newname)
{
    implementRenameLogicalFile(ctx, oldname, newname, false);
}

FILESERVICES_API void FILESERVICES_CALL fsRenameLogicalFile_v2(ICodeContext *ctx, const char *oldname, const char *newname, const bool overwrite)
{
    implementRenameLogicalFile(ctx, oldname, newname, overwrite);
}


FILESERVICES_API void FILESERVICES_CALL fsSendEmail_v3(ICodeContext * ctx, const char * to, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender, const char *cc, const char *bcc, bool highPriority, bool termJobOnFail)
{
    StringArray warnings;
    sendEmail( to, cc, bcc, subject, body, mailServer, port, sender, &warnings, highPriority, termJobOnFail);
    ForEachItemIn(i,warnings)
        WUmessage(ctx, SeverityWarning, "SendEmail", warnings.item(i));
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmail_v2(ICodeContext * ctx, const char * to, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender, const char *cc, const char *bcc, bool highPriority)
{
    fsSendEmail_v3(ctx, to, subject, body, mailServer, port, sender, cc, bcc, highPriority, true);
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmail(ICodeContext * ctx, const char * to, const char * subject, const char * body, const char * mailServer, unsigned port, const char * sender)
{
    fsSendEmail_v3(ctx, to, subject, body, mailServer, port, sender, nullptr, nullptr, false, true);
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmailAttachText_v3(ICodeContext * ctx, const char * to, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, const char *cc, const char *bcc, bool highPriority, bool termJobOnFail)
{
    StringArray warnings;
    sendEmailAttachText(to, cc, bcc, subject, body, attachment, mimeType, attachmentName, mailServer, port, sender, &warnings, highPriority, termJobOnFail);
    ForEachItemIn(i,warnings)
        WUmessage(ctx, SeverityWarning, "SendEmailAttachText", warnings.item(i));
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmailAttachText_v2(ICodeContext * ctx, const char * to, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, const char *cc, const char *bcc, bool highPriority)
{
    StringArray warnings;
    sendEmailAttachText(to, cc, bcc, subject, body, attachment, mimeType, attachmentName, mailServer, port, sender, &warnings, highPriority, true);
    ForEachItemIn(i,warnings)
        WUmessage(ctx, SeverityWarning, "SendEmailAttachText", warnings.item(i));
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmailAttachText(ICodeContext * ctx, const char * to, const char * subject, const char * body, const char * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender)
{
    fsSendEmailAttachText_v3(ctx, to, subject, body, attachment, mimeType, attachmentName, mailServer, port, sender, nullptr, nullptr, false, true);
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmailAttachData_v3(ICodeContext * ctx, const char * to, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, const char *cc, const char *bcc, bool highPriority, bool termJobOnFail)
{
    StringArray warnings;
    sendEmailAttachData(to, cc, bcc, subject, body, lenAttachment, attachment, mimeType, attachmentName, mailServer, port, sender, &warnings, highPriority, termJobOnFail);
    ForEachItemIn(i,warnings)
        WUmessage(ctx, SeverityWarning, "SendEmailAttachData", warnings.item(i));
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmailAttachData_v2(ICodeContext * ctx, const char * to, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender, const char *cc, const char *bcc, bool highPriority)
{
    StringArray warnings;
    sendEmailAttachData(to, cc, bcc, subject, body, lenAttachment, attachment, mimeType, attachmentName, mailServer, port, sender, &warnings, highPriority, true);
    ForEachItemIn(i,warnings)
        WUmessage(ctx, SeverityWarning, "SendEmailAttachData", warnings.item(i));
}

FILESERVICES_API void FILESERVICES_CALL fsSendEmailAttachData(ICodeContext * ctx, const char * to, const char * subject, const char * body, size32_t lenAttachment, const void * attachment, const char * mimeType, const char * attachmentName, const char * mailServer, unsigned int port, const char * sender)
{
    fsSendEmailAttachData_v3(ctx, to, subject, body, lenAttachment, attachment, mimeType, attachmentName, mailServer, port, sender, nullptr, nullptr, false, true);
}



FILESERVICES_API char * FILESERVICES_CALL fsCmdProcess(const char *prog, const char *src)
{
    throw makeStringException(0, "CmdProcess no longer supported for security reasons");
}


FILESERVICES_API void FILESERVICES_CALL fsCmdProcess2(unsigned & tgtLen, char * & tgt, const char *prog, unsigned srcLen, const char * src)
{
    throw makeStringException(0, "CmdProcess no longer supported for security reasons");
}


static void blockUntilComplete(const char * label, IClientFileSpray &server, ICodeContext *ctx, const char * wuid, int timeOut, StringBuffer *stateout=NULL, bool monitoringok=false)
{
    if (!wuid || strcmp(wuid, "") == 0)
        return;

    if (timeOut == 0)
        return;

    CTimeMon time(timeOut);

    unsigned polltime = 1;

    VStringBuffer reason("Blocked by fileservice activity: %s, workunit: %s", label, wuid);
    setWorkunitState(ctx, WUStateBlocked, reason.str());
    bool isStartTimeRecorded = false;
    while(true)
    {

        Owned<IClientGetDFUWorkunit> req = server.createGetDFUWorkunitRequest();
        setContainerLocalCertificate(req->rpc());
        req->setWuid(wuid);
        Owned<IClientGetDFUWorkunitResponse> result = server.GetDFUWorkunit(req);

        const IMultiException* excep = &result->getExceptions();
        if ((excep != NULL) && (excep->ordinality() > 0))
        {
            setWorkunitState(ctx, WUStateRunning, NULL);
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            throw MakeStringExceptionDirect(0, errmsg.str());
        }

        IConstDFUWorkunit & dfuwu = result->getResult();
        DFUstate state = (DFUstate)dfuwu.getState();
        bool aborting = false;
        Owned<IWorkUnit> wu = ctx->updateWorkUnit(); // may return NULL
        if (wu.get()) { // if updatable (e.g. not hthor with no agent context)
            aborting = wu->aborting();
            StringBuffer wuScope, ElapsedLabel, RemainingLabel;
            StringBuffer labelbuf(label);
            wuScope.appendf("w%u:>dfu:>%s:%s", ctx->getWorkflowId(), labelbuf.toLowerCase().str(), dfuwu.getID());
            ElapsedLabel.append(wuScope).append(" (Elapsed) ");
            RemainingLabel.append(wuScope).append(" (Remaining) ");
            //MORE: I think this are intended to replace the timing information, but will currently combine
            updateWorkunitStat(wu, SSTdfuworkunit, wuScope, StTimeElapsed, ElapsedLabel, milliToNano(time.elapsed()));
            updateWorkunitStat(wu, SSTdfuworkunit, wuScope, StTimeRemaining, RemainingLabel, milliToNano(dfuwu.getSecsLeft()*1000));
            stat_type costFileAccess = money2cost_type(dfuwu.getFileAccessCost());
            updateWorkunitStat(wu, SSTdfuworkunit, wuScope, StCostFileAccess, "", costFileAccess);
            wu->setApplicationValue(label, dfuwu.getID(), dfuwu.getSummaryMessage(), true);
            if (!isStartTimeRecorded)
            {
                switch (state)
                {
                case DFUstate_started:
                case DFUstate_aborting:
                case DFUstate_monitoring:
                case DFUstate_aborted:
                case DFUstate_failed:
                case DFUstate_finished:

                    const char * whenStarted = dfuwu.getTimeStarted();
                    if (!isEmptyString(whenStarted))
                    {
                        CDateTime startedAt;
                        startedAt.setString(whenStarted);
                        updateWorkunitStat(wu, SSTdfuworkunit, wuScope, StWhenStarted, 0, startedAt.getTimeStamp());
                        isStartTimeRecorded = true;
                    }
                    break;
                }
            }
            wu->commit();
            wu.clear();
        }

        if (stateout)
            stateout->clear().append(dfuwu.getStateMessage());
        switch(state)
        {
        case DFUstate_unknown:
        case DFUstate_scheduled:
        case DFUstate_queued:
        case DFUstate_started:
        case DFUstate_aborting:
            break;

        case DFUstate_monitoring:
            if (monitoringok)
            {
                setWorkunitState(ctx, WUStateRunning, NULL);
                return;
            }
            break;

        case DFUstate_aborted:
        case DFUstate_failed:
            setWorkunitState(ctx, WUStateRunning, NULL);
            throw MakeStringException(0, "DFUServer Error %s", dfuwu.getSummaryMessage());

        case DFUstate_finished:
            setWorkunitState(ctx, WUStateRunning, NULL);
            return;
        }

        if (aborting)
        {
            Owned<IClientAbortDFUWorkunit> abortReq = server.createAbortDFUWorkunitRequest();
            setContainerLocalCertificate(abortReq->rpc());
            abortReq->setWuid(wuid);
            Linked<IClientAbortDFUWorkunitResponse> abortResp = server.AbortDFUWorkunit(abortReq);

            setWorkunitState(ctx, WUStateRunning, NULL);

            //  Add warning of DFU Abort Request - should this be information  ---
            StringBuffer s("DFU Workunit Abort Requested for ");
            s.append(wuid);
            WUmessage(ctx,SeverityWarning,"blockUntilComplete",s.str());
            throw MakeStringException(0, "Workunit abort request received");
        }

        if (time.timedout()) {
            unsigned left = dfuwu.getSecsLeft();
            setWorkunitState(ctx, WUStateRunning, NULL);
            if (left)
                throw MakeStringException(0, "%s timed out, DFU Secs left:  %d)", label, left);
            throw MakeStringException(0, "%s timed out)", label);

        }


        Sleep(polltime*1000);
        polltime *= 2;
        if (polltime>WAIT_SECONDS)
            polltime = WAIT_SECONDS;

    }
    setWorkunitState(ctx, WUStateRunning, NULL);
}


//----------------------------------------------------------------------------------

FILESERVICES_API char * FILESERVICES_CALL implementSprayFixed(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane = nullptr, unsigned destinationNumParts = 0, bool noCommon = true)
{
    LOG(MCauditInfo, "Spray:  %s", destinationLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort, wu));
    setServerAccess(server, wu);

    Owned<IClientSprayFixed> req = server.createSprayFixedRequest();
    setContainerLocalCertificate(req->rpc());
    StringBuffer logicalName;
    constructLogicalName(wu, destinationLogicalName, logicalName);

    req->setSourceIP(sourceIP);
    req->setSourcePath(sourcePath);
    req->setSourcePlane(sourcePlane);
    req->setSourceRecordSize(recordSize);
    req->setDestGroup(destinationGroup);
    req->setDestLogicalName(logicalName.str());
    req->setDestNumParts(destinationNumParts);
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    req->setCompress(compress);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);

    if (failIfNoSourceFile)
        req->setFailIfNoSourceFile(true);

    req->setExpireDays(expireDays);

    if (!isEmptyString(dfuServerQueue))
        req->setDFUServerQueue(dfuServerQueue);

    if (noSplit)
        req->setNosplit(true);

    req->setNoCommon(noCommon);

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Dfu Spray Fixed");
    clientSpan->setSpanAttribute("destinationFilename", logicalName);
    try
    {
        Owned<IClientSprayFixedResponse> result = server.SprayFixed(req);

        StringBuffer wuid(result->getWuid());
        if (!wuid.length())
        {
            const IMultiException* excep = &result->getExceptions();
            if ((excep != NULL) && (excep->ordinality() > 0))
            {
                StringBuffer errmsg;
                excep->errorMessage(errmsg);
                throw MakeStringExceptionDirect(0, errmsg.str());
            }
            else
            {
                throw MakeStringExceptionDirect(0, "Result's dfu WUID is empty");
            }
        }

        wu.clear();

        clientSpan->setSpanAttribute("wuid", wuid);
        blockUntilComplete("Spray", server, ctx, wuid, timeOut);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}

FILESERVICES_API void FILESERVICES_CALL fsSprayFixed(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    CTXFREE(parentCtx, implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, -1, nullptr, false));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayFixed_v2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays)
{
    CTXFREE(parentCtx, implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, nullptr, false));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayFixed_v3(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue)
{
    CTXFREE(parentCtx, implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, false));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayFixed_v4(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit)
{
    CTXFREE(parentCtx, implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayFixed_v5(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts)
{
    CTXFREE(parentCtx, implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayFixed_v6(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts, bool noCommon)
{
    CTXFREE(parentCtx, implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts, noCommon));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayFixed(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    return implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, -1, nullptr, false);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayFixed_v2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays)
{
    return implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, nullptr, false);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayFixed_v3(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue)
{
    return implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, false);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayFixed_v4(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit)
{
    return implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayFixed_v5(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts)
{
    return implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayFixed_v6(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int recordSize, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts, bool noCommon)
{
    return implementSprayFixed(ctx, sourceIP, sourcePath, recordSize, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts, noCommon);
}

//----------------------------------------------------------------------------------

static char * implementSprayVariable(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * sourceCsvEscape, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane = nullptr, unsigned destinationNumParts = 0, bool noCommon = true)
{
    LOG(MCauditInfo, "Spray:  %s", destinationLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);

    Owned<IClientSprayVariable> req = server.createSprayVariableRequest();
    setContainerLocalCertificate(req->rpc());
    StringBuffer logicalName;
    constructLogicalName(wu, destinationLogicalName, logicalName);

    req->setSourceIP(sourceIP);
    req->setSourcePath(sourcePath);
    req->setSourcePlane(sourcePlane);
    req->setSourceMaxRecordSize(sourceMaxRecordSize);
    req->setSourceFormat(CDFUfileformat::decode(encoding));
    req->setSourceCsvSeparate(sourceCsvSeparate);
    if (sourceCsvSeparate && *sourceCsvSeparate == '\0')
        req->setNoSourceCsvSeparator(true);
    req->setSourceCsvTerminate(sourceCsvTerminate);
    req->setSourceCsvQuote(sourceCsvQuote);
    if (sourceCsvEscape && *sourceCsvEscape)
        req->setSourceCsvEscape(sourceCsvEscape);
    req->setDestGroup(destinationGroup);
    req->setDestLogicalName(logicalName.str());
    req->setDestNumParts(destinationNumParts);
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    req->setCompress(compress);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);
    if (failIfNoSourceFile)
        req->setFailIfNoSourceFile(true);
    if (recordStructurePresent)
        req->setRecordStructurePresent(true);
    if (!quotedTerminator)
        req->setQuotedTerminator(false);
    req->setExpireDays(expireDays);

    if (!isEmptyString(dfuServerQueue))
        req->setDFUServerQueue(dfuServerQueue);

    if (noSplit)
        req->setNosplit(true);
    req->setNoCommon(noCommon);

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Dfu Spray Variable");
    clientSpan->setSpanAttribute("destinationFilename", logicalName);
    try
    {
        Owned<IClientSprayResponse> result = server.SprayVariable(req);

        StringBuffer wuid(result->getWuid());
        if (!wuid.length())
        {
            const IMultiException* excep = &result->getExceptions();
            if ((excep != NULL) && (excep->ordinality() > 0))
            {
                StringBuffer errmsg;
                excep->errorMessage(errmsg);
                throw MakeStringExceptionDirect(0, errmsg.str());
            }
            else
            {
                throw MakeStringExceptionDirect(0, "Result's dfu WUID is empty");
            }
        }

        wu.clear();

        clientSpan->setSpanAttribute("wuid", wuid);
        blockUntilComplete("Spray", server, ctx, wuid, timeOut);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, NULL, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, false, true, "ascii", -1, nullptr, false));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, NULL, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, false, true, "ascii", -1, nullptr, false);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, false, true, "ascii", -1, nullptr, false));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, false, true, "ascii", -1, nullptr, false);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable_v3(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, true, "ascii", -1, nullptr, false));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable_v3(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, true, "ascii", -1, nullptr, false);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable_v4(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, "ascii", -1, nullptr, false));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable_v4(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, "ascii", -1, nullptr, false);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable_v5(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, -1, nullptr, false));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable_v5(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, -1, nullptr, false);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable_v6(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, nullptr, false));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable_v6(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, nullptr, false);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable_v7(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, dfuServerQueue, false));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable_v7(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, dfuServerQueue, false);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable_v8(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue, bool noSplit)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, dfuServerQueue, noSplit));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable_v8(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue, bool noSplit)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, dfuServerQueue, noSplit);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable_v9(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable_v9(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts);
}

FILESERVICES_API void FILESERVICES_CALL fsSprayVariable_v10(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts, bool noCommon)
{
    CTXFREE(parentCtx, implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts, noCommon));
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayVariable_v10(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char * sourceCsvSeparate, const char * sourceCsvTerminate, const char * sourceCsvQuote, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, const char * csvEscape, bool failIfNoSourceFile, bool recordStructurePresent, bool quotedTerminator, const char * encoding, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts, bool noCommon)
{
    return implementSprayVariable(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceCsvSeparate, sourceCsvTerminate, sourceCsvQuote, csvEscape, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, recordStructurePresent, quotedTerminator, encoding, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts, noCommon);
}


//----------------------------------------------------------------------------------

FILESERVICES_API char * FILESERVICES_CALL implementSprayXml(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts=0, bool noCommon = true)
{
    LOG(MCauditInfo, "Spray:  %s", destinationLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);

    Owned<IClientSprayVariable> req = server.createSprayVariableRequest();
    setContainerLocalCertificate(req->rpc());
    StringBuffer logicalName;
    constructLogicalName(wu, destinationLogicalName, logicalName);

    DFUfileformat dfufmt;
    if (sourceEncoding == NULL)
        dfufmt = DFUff_utf8;
    else
        dfufmt = CDFUfileformat::decode(sourceEncoding);

    req->setSourceIP(sourceIP);
    req->setSourcePath(sourcePath);
    req->setSourcePlane(sourcePlane);
    req->setSourceMaxRecordSize(sourceMaxRecordSize);
    req->setSourceFormat(dfufmt);
    req->setSourceRowTag(sourceRowTag);
    req->setDestGroup(destinationGroup);
    req->setDestLogicalName(logicalName.str());
    req->setDestNumParts(destinationNumParts);
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    req->setCompress(compress);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);
    if (failIfNoSourceFile)
        req->setFailIfNoSourceFile(true);

    req->setExpireDays(expireDays);

    if (!isEmptyString(dfuServerQueue))
        req->setDFUServerQueue(dfuServerQueue);

    if (noSplit)
        req->setNosplit(true);

    req->setNoCommon(noCommon);

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Dfu Spray Xml");
    clientSpan->setSpanAttribute("destinationFilename", logicalName);
    try
    {
        Owned<IClientSprayResponse> result = server.SprayVariable(req);

        StringBuffer wuid(result->getWuid());
        if (!wuid.length())
        {
            const IMultiException* excep = &result->getExceptions();
            if ((excep != NULL) && (excep->ordinality() > 0))
            {
                StringBuffer errmsg;
                excep->errorMessage(errmsg);
                throw MakeStringExceptionDirect(0, errmsg.str());
            }
            else
            {
                throw MakeStringExceptionDirect(0, "Result's dfu WUID is empty");
            }
        }

        wu.clear();

        clientSpan->setSpanAttribute("wuid", wuid);
        blockUntilComplete("Spray", server, ctx, wuid, timeOut);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}


FILESERVICES_API void FILESERVICES_CALL fsSprayXml(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    CTXFREE(parentCtx, implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, -1, nullptr, false, nullptr));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayXml_v2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays)
{
    CTXFREE(parentCtx, implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, nullptr, false, nullptr));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayXml_v3(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue)
{
    CTXFREE(parentCtx, implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, false, nullptr));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayXml_v4(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit)
{
    CTXFREE(parentCtx, implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, nullptr));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayXml_v5(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts)
{
    CTXFREE(parentCtx, implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayXml_v6(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts, bool noCommon)
{
    CTXFREE(parentCtx, implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts, noCommon));
}


FILESERVICES_API char * FILESERVICES_CALL fsfSprayXml(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile)
{
    return implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, -1, nullptr, false, nullptr);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayXml_v2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays)
{
    return implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, nullptr, false, nullptr);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayXml_v3(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue)
{
    return implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, false, nullptr);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayXml_v4(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit)
{
    return implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, nullptr);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayXml_v5(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts)
{
    return implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayXml_v6(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowTag, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * sourcePlane, unsigned destinationNumParts, bool noCommon)
{
    return implementSprayXml(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowTag, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, sourcePlane, destinationNumParts, noCommon);
}


//----------------------------------------------------------------------------------

FILESERVICES_API char * FILESERVICES_CALL implementSprayJson(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowPath, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * username, const char * userPw, const char * sourcePlane, unsigned destinationNumParts=0, bool noCommon = true)
{
    LOG(MCauditInfo, "Spray JSON: %s", destinationLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);

    Owned<IClientSprayVariable> req = server.createSprayVariableRequest();
    setContainerLocalCertificate(req->rpc());
    StringBuffer logicalName;
    constructLogicalName(wu, destinationLogicalName, logicalName);

    DFUfileformat dfufmt;
    if (sourceEncoding == NULL)
        dfufmt = DFUff_utf8;
    else
        dfufmt = CDFUfileformat::decode(sourceEncoding);

    req->setSourceIP(sourceIP);
    req->setSourcePath(sourcePath);
    req->setSourcePlane(sourcePlane);
    req->setSourceMaxRecordSize(sourceMaxRecordSize);
    req->setSourceFormat(dfufmt);
    req->setSourceRowTag(sourceRowPath);
    req->setDestGroup(destinationGroup);
    req->setDestLogicalName(logicalName.str());
    req->setDestNumParts(destinationNumParts);
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    req->setCompress(compress);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);
    if (failIfNoSourceFile)
        req->setFailIfNoSourceFile(true);

    req->setIsJSON(true);
    req->setExpireDays(expireDays);

    if (!isEmptyString(dfuServerQueue))
        req->setDFUServerQueue(dfuServerQueue);

    if (noSplit)
        req->setNosplit(true);

    req->setNoCommon(noCommon);

    // Store username/psw
    if (!isEmptyString(username))
    {
        req->setSrcUsername(username);
        if (!isEmptyString(userPw))
            req->setSrcPassword(userPw);
    }

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Dfu Spray Json");
    clientSpan->setSpanAttribute("destinationFilename", logicalName);
    try
    {
        Owned<IClientSprayResponse> result = server.SprayVariable(req);

        StringBuffer wuid(result->getWuid());
        if (!wuid.length())
        {
            const IMultiException* excep = &result->getExceptions();
            if ((excep != NULL) && (excep->ordinality() > 0))
            {
                StringBuffer errmsg;
                excep->errorMessage(errmsg);
                throw MakeStringExceptionDirect(0, errmsg.str());
            }
            else
            {
                throw MakeStringExceptionDirect(0, "Result's dfu WUID is empty");
            }
        }

        wu.clear();

        clientSpan->setSpanAttribute("wuid", wuid);
        blockUntilComplete("Spray", server, ctx, wuid, timeOut);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}


FILESERVICES_API void FILESERVICES_CALL fsSprayJson(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowPath, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * username, const char * userPw)
{
    CTXFREE(parentCtx, implementSprayJson(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowPath, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, username, userPw, nullptr));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayJson_v2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowPath, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * username, const char * userPw, const char * sourcePlane, unsigned destinationNumParts)
{
    CTXFREE(parentCtx, implementSprayJson(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowPath, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, username, userPw, sourcePlane, destinationNumParts));
}

FILESERVICES_API void FILESERVICES_CALL fsSprayJson_v3(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowPath, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * username, const char * userPw, const char * sourcePlane, unsigned destinationNumParts, bool noCommon)
{
    CTXFREE(parentCtx, implementSprayJson(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowPath, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, username, userPw, sourcePlane, destinationNumParts, noCommon));
}


FILESERVICES_API char * FILESERVICES_CALL fsfSprayJson(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowPath, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * username, const char * userPw)
{
    return implementSprayJson(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowPath, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, username, userPw, nullptr);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayJson_v2(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowPath, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * username, const char * userPw, const char * sourcePlane, unsigned destinationNumParts)
{
    return implementSprayJson(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowPath, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, username, userPw, sourcePlane, destinationNumParts);
}

FILESERVICES_API char * FILESERVICES_CALL fsfSprayJson_v3(ICodeContext *ctx, const char * sourceIP, const char * sourcePath, int sourceMaxRecordSize, const char *sourceRowPath, const char *sourceEncoding, const char * destinationGroup, const char * destinationLogicalName, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool compress, bool failIfNoSourceFile, int expireDays, const char * dfuServerQueue, bool noSplit, const char * username, const char * userPw, const char * sourcePlane, unsigned destinationNumParts, bool noCommon)
{
    return implementSprayJson(ctx, sourceIP, sourcePath, sourceMaxRecordSize, sourceRowPath, sourceEncoding, destinationGroup, destinationLogicalName, timeOut, espServerIpPort, maxConnections, overwrite, replicate, compress, failIfNoSourceFile, expireDays, dfuServerQueue, noSplit, username, userPw, sourcePlane, destinationNumParts, noCommon);
}

//----------------------------------------------------------------------------------

static char * implementDespray(ICodeContext *ctx, const char * sourceLogicalName, const char * destinationIP, const char * destinationPath, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, const char * destinationPlane = nullptr)
{
    LOG(MCauditInfo, "Despray:  %s", sourceLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    if (wu)
        setServerAccess(server, wu);

    Owned<IClientDespray> req = server.createDesprayRequest();
    setContainerLocalCertificate(req->rpc());
    StringBuffer logicalName;
    constructLogicalName(wu, sourceLogicalName, logicalName);

    req->setSourceLogicalName(logicalName.str());
    req->setDestIP(destinationIP);
    req->setDestPath(destinationPath);
    req->setDestPlane(destinationPlane);
    req->setOverwrite(overwrite);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Dfu Despray");
    clientSpan->setSpanAttribute("sourceFilename", logicalName);
    try
    {
        Owned<IClientDesprayResponse> result = server.Despray(req);

        StringBuffer wuid(result->getWuid());
        if (!wuid.length())
        {
            const IMultiException* excep = &result->getExceptions();
            if ((excep != NULL) && (excep->ordinality() > 0))
            {
                StringBuffer errmsg;
                excep->errorMessage(errmsg);
                throw MakeStringExceptionDirect(0, errmsg.str());
            }
            else
            {
                throw MakeStringExceptionDirect(0, "Result's dfu WUID is empty");
            }
        }

        wu.clear();

        clientSpan->setSpanAttribute("wuid", wuid);
        blockUntilComplete("Despray", server, ctx, wuid, timeOut);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}

FILESERVICES_API void FILESERVICES_CALL fsDespray(ICodeContext *ctx, const char * sourceLogicalName, const char * destinationIP, const char * destinationPath, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite)
{
    CTXFREE(parentCtx, implementDespray(ctx, sourceLogicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, overwrite));
}

FILESERVICES_API char * FILESERVICES_CALL fsfDespray(ICodeContext *ctx, const char * sourceLogicalName, const char * destinationIP, const char * destinationPath, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite)
{
    return implementDespray(ctx, sourceLogicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, overwrite);
}

FILESERVICES_API void FILESERVICES_CALL fsDespray2(ICodeContext *ctx, const char * sourceLogicalName, const char * destinationIP, const char * destinationPath, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, const char * destinationPlane)
{
    CTXFREE(parentCtx, implementDespray(ctx, sourceLogicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, overwrite, destinationPlane));
}

FILESERVICES_API char * FILESERVICES_CALL fsfDespray2(ICodeContext *ctx, const char * sourceLogicalName, const char * destinationIP, const char * destinationPath, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, const char * destinationPlane)
{
    return implementDespray(ctx, sourceLogicalName, destinationIP, destinationPath, timeOut, espServerIpPort, maxConnections, overwrite, destinationPlane);
}

FILESERVICES_API char * FILESERVICES_CALL implementCopy(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression, bool noSplit, int expireDays, bool ensure, bool wrap)
{
    LOG(MCauditInfo, "Copy:  %s%s", sourceLogicalName,asSuperfile?" as superfile":"");

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);

    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);

    Owned<IClientCopy> req = server.createCopyRequest();
    setContainerLocalCertificate(req->rpc());
    if (asSuperfile)
        req->setSuperCopy(true);

    StringBuffer _sourceLogicalName, _destinationLogicalName;
    constructLogicalName(wu, sourceLogicalName, _sourceLogicalName);
    constructLogicalName(wu, destinationLogicalName, _destinationLogicalName);

    req->setSourceLogicalName(_sourceLogicalName.str());
    req->setDestLogicalName(_destinationLogicalName.str());
    if ((destinationGroup != NULL) && (*destinationGroup != '\0'))
        req->setDestGroup(destinationGroup);
    if ((sourceDali != NULL) && (*sourceDali != '\0'))
        req->setSourceDali(sourceDali);
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    if (compress)
        req->setCompress(true);
    req->setPreserveCompression(preserveCompression);
    if (forcePush)
        req->setPush(true);
    if (transferBufferSize > 0)
        req->setTransferBufferSize(transferBufferSize);
    if (maxConnections != -1)
        req->setMaxConnections(maxConnections);
    if (noSplit)
        req->setNosplit(true);
    if (ensure)
        req->setEnsure(true);
    if (wrap)
        req->setWrap(true);
    req->setExpireDays(expireDays);

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Dfu Copy");
    clientSpan->setSpanAttribute("sourceFilename", sourceLogicalName);
    clientSpan->setSpanAttribute("destinationFilename", destinationLogicalName);
    try
    {
        Owned<IClientCopyResponse> result = server.Copy(req);

        StringBuffer wuid(result->getResult());
        if (!wuid.length())
        {
            const IMultiException* excep = &result->getExceptions();
            if ((excep != NULL) && (excep->ordinality() > 0))
            {
                StringBuffer errmsg;
                excep->errorMessage(errmsg);
                throw MakeStringExceptionDirect(0, errmsg.str());
            }
            else
            {
                throw MakeStringExceptionDirect(0, "Result's dfu WUID is empty");
            }
        }

        wu.clear();

        clientSpan->setSpanAttribute("wuid", wuid);
        blockUntilComplete("Copy", server, ctx, wuid, timeOut);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}

FILESERVICES_API void FILESERVICES_CALL fsCopy(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize)
{
    CTXFREE(parentCtx, implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, true, false, -1, false, false));
}

FILESERVICES_API void FILESERVICES_CALL fsCopy_v2(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression)
{
    CTXFREE(parentCtx, implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression, false, -1, false, false));
}

FILESERVICES_API void FILESERVICES_CALL fsCopy_v3(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression, bool noSplit, int expireDays)
{
    CTXFREE(parentCtx, implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression, noSplit, expireDays, false, false));
}

FILESERVICES_API void FILESERVICES_CALL fsCopy_v4(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression, bool noSplit, int expireDays, bool ensure)
{
    CTXFREE(parentCtx, implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression, noSplit, expireDays, ensure, false));
}

FILESERVICES_API void FILESERVICES_CALL fsCopy_v5(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression, bool noSplit, int expireDays, bool ensure, bool wrap)
{
    CTXFREE(parentCtx, implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression, noSplit, expireDays, ensure, wrap));
}

FILESERVICES_API char * FILESERVICES_CALL fsfCopy(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize)
{
    return implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, true, false, -1, false, false);
}

FILESERVICES_API char * FILESERVICES_CALL fsfCopy_v2(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression)
{
    return implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression, false, -1, false, false);
}

FILESERVICES_API char * FILESERVICES_CALL fsfCopy_v3(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression, bool noSplit, int expireDays)
{
    return implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression, noSplit, expireDays, false, false);
}

FILESERVICES_API char * FILESERVICES_CALL fsfCopy_v4(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression, bool noSplit, int expireDays, bool ensure)
{
    return implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression, noSplit, expireDays, ensure, false);
}

FILESERVICES_API char * FILESERVICES_CALL fsfCopy_v5(ICodeContext *ctx, const char * sourceLogicalName, const char *destinationGroup, const char * destinationLogicalName, const char * sourceDali, int timeOut, const char * espServerIpPort, int maxConnections, bool overwrite, bool replicate, bool asSuperfile, bool compress, bool forcePush, int transferBufferSize, bool preserveCompression, bool noSplit, int expireDays, bool ensure, bool wrap)
{
    return implementCopy(ctx, sourceLogicalName, destinationGroup, destinationLogicalName, sourceDali, timeOut, espServerIpPort, maxConnections, overwrite, replicate, asSuperfile, compress, forcePush, transferBufferSize, preserveCompression, noSplit, expireDays, ensure, wrap);
}


FILESERVICES_API void FILESERVICES_CALL fsReplicate(ICodeContext *ctx, const char * sourceLogicalName,int timeOut, const char * espServerIpPort)
{
    CTXFREE(parentCtx, fsfReplicate(ctx, sourceLogicalName, timeOut, espServerIpPort));
}

FILESERVICES_API char * FILESERVICES_CALL fsfReplicate(ICodeContext *ctx, const char * sourceLogicalName, int timeOut, const char * espServerIpPort)
{
    LOG(MCauditInfo, "REPLICATE:  %s", sourceLogicalName);

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);

    Owned<IClientReplicate> req = server.createReplicateRequest();
    setContainerLocalCertificate(req->rpc());
    StringBuffer logicalName;
    constructLogicalName(wu, sourceLogicalName, logicalName);

    req->setSourceLogicalName(logicalName.str());

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Dfu Spray Fixed");
    clientSpan->setSpanAttribute("destinationFilename", logicalName);
    try
    {
        Owned<IClientReplicateResponse> result = server.Replicate(req);

        StringBuffer wuid(result->getWuid());
        if (!wuid.length())
        {
            const IMultiException* excep = &result->getExceptions();
            if ((excep != NULL) && (excep->ordinality() > 0))
            {
                StringBuffer errmsg;
                excep->errorMessage(errmsg);
                throw MakeStringExceptionDirect(0, errmsg.str());
            }
            else
            {
                throw MakeStringExceptionDirect(0, "Result's dfu WUID is empty");
            }
        }

        wu.clear();

        clientSpan->setSpanAttribute("wuid", wuid);
        blockUntilComplete("Replicate", server, ctx, wuid, timeOut);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}



//===========================================================================================
// SuperFile API

/*
CreateSuperFile(const varstring lsuperfn, boolean sequentialparts=false);
boolean SuperFileExists(const varstring lsuperfn);
DeleteSuperFile(const varstring lsuperfn,boolean deletesub=false);
unsigned4 GetSuperFileSubCount(const varstring lsuperfn);
varstring GetSuperFileSubName(const varstring lsuperfn,unsigned4 filenum);
unsigned4 FindSuperFileSubName(const varstring lsuperfn,const varstring lfn);
StartSuperFileTransaction();
AddSuperFile(const varstring lsuperfn,const varstring lfn,unsigned4 atpos=0);
RemoveSuperFile(const varstring lsuperfn,const varstring lfn,boolean del=false);
ClearSuperFile(const varstring lsuperfn,boolean del=false);
RemoveOwnedSubFiles(const varstring lsuperfn,boolean del=false);
SwapSuperFile(const varstring lsuperfn1,const varstring lsuperfn2);
ReplaceSuperFile(const varstring lsuperfn,const varstring lfn,const varstring bylfn);
FinishSuperFileTransaction(boolean rollback=false);
*/

class CImplicitSuperTransaction
{
    IDistributedFileTransaction *transaction;
public:
    CImplicitSuperTransaction(IDistributedFileTransaction *_transaction)
    {
        if (!_transaction->active()) // then created implicitly
        {
            transaction = _transaction;
            transaction->start();
        }
        else
            transaction = NULL;
    }
    ~CImplicitSuperTransaction() noexcept(false)
    {
        if (transaction)
            transaction->commit();
    }
};


static bool lookupSuperFile(ICodeContext *ctx, const char *lsuperfn, AccessMode accessMode, Owned<IDistributedSuperFile> &file, bool throwerr, StringBuffer &lsfn, bool allowforeign, bool cacheFiles=false)
{
    lsfn.clear();
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    constructLogicalName(ctx, lsuperfn, lsfn);
    if (!allowforeign) {
        CDfsLogicalFileName dlfn;
        dlfn.set(lsfn.str());
        if (dlfn.isForeign())
            throw MakeStringException(0, "Foreign superfile not allowed: %s", lsfn.str());
    }
    file.setown(transaction->lookupSuperFile(lsfn.str(), accessMode));
    if (file.get())
        return true;
    if (throwerr)
        throw MakeStringException(0, "Could not locate superfile: %s", lsfn.str());
    return false;
}

static ISimpleSuperFileEnquiry *getSimpleSuperFileEnquiry(ICodeContext *ctx, const char *lsuperfn)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    if (transaction->active())
        return NULL;
    StringBuffer lsfn;
    constructLogicalName(ctx, lsuperfn, lsfn);
    return queryDistributedFileDirectory().getSimpleSuperFileEnquiry(lsfn.str(),"Fileservices",ctx->queryUserDescriptor());
}

static void CheckNotInTransaction(ICodeContext *ctx, const char *fn)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    if (transaction->active()) {
        StringBuffer s("Operation not part of transaction : ");
        s.append(fn);
        WUmessage(ctx,SeverityWarning,fn,s.str());
    }
}

FILESERVICES_API void FILESERVICES_CALL fsCreateSuperFile(ICodeContext *ctx, const char *lsuperfn, bool sequentialparts, bool ifdoesnotexist)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    StringBuffer lsfn;
    constructLogicalName(ctx, lsuperfn, lsfn);
    Owned<IDistributedSuperFile> file = queryDistributedFileDirectory().createSuperFile(lsfn,udesc,!sequentialparts,ifdoesnotexist,transaction);
    StringBuffer s("CreateSuperFile ('");
    s.append(lsfn).append("') done");
    AuditMessage(ctx,"CreateSuperFile",lsfn.str());
    WUmessage(ctx,SeverityInformation,NULL,s.str());
}

FILESERVICES_API bool FILESERVICES_CALL fsSuperFileExists(ICodeContext *ctx, const char *lsuperfn)
{
    StringBuffer lsfn;
    constructLogicalName(ctx, lsuperfn, lsfn);
    return wsdfs::exists(lsfn, ctx->queryUserDescriptor(), false, true, INFINITE);
}

FILESERVICES_API void FILESERVICES_CALL fsDeleteSuperFile(ICodeContext *ctx, const char *lsuperfn,bool deletesub)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    bool found = lookupSuperFile(ctx, lsuperfn, AccessMode::writeMeta, file, false, lsfn, false);
    file.clear(); // MORE: this should really be exists(file)
    StringBuffer s("DeleteSuperFile ('");
    s.append(lsfn).appendf("')");
    if (found) {
        queryDistributedFileDirectory().removeSuperFile(lsfn.str(), deletesub, udesc, transaction);
        if (transaction->active())
            s.append(" action added to transaction");
        else
            s.append(" done");
    } else {
        s.append(" file not found");
    }
    WUmessage(ctx,SeverityInformation,NULL,s.str());
    if (found)
        AuditMessage(ctx,"DeleteSuperFile",lsfn.str());
}

FILESERVICES_API unsigned FILESERVICES_CALL fsGetSuperFileSubCount(ICodeContext *ctx, const char *lsuperfn)
{
    Owned<ISimpleSuperFileEnquiry> enq = getSimpleSuperFileEnquiry(ctx, lsuperfn);
    if (enq)
        return enq->numSubFiles();
    CImplicitSuperTransaction implicitTransaction(ctx->querySuperFileTransaction());
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    lookupSuperFile(ctx, lsuperfn, AccessMode::readMeta, file, true, lsfn, true);
    return file->numSubFiles();
}

FILESERVICES_API char *  FILESERVICES_CALL fsGetSuperFileSubName(ICodeContext *ctx, const char *lsuperfn,unsigned filenum, bool abspath)
{
    StringBuffer ret;
    if (abspath)
        ret.append('~');
    Owned<ISimpleSuperFileEnquiry> enq = getSimpleSuperFileEnquiry(ctx, lsuperfn);
    if (enq) {
        if (!filenum||!enq->getSubFileName(filenum-1,ret))
            return CTXSTRDUP(parentCtx, "");
        return ret.detach();
    }
    CImplicitSuperTransaction implicitTransaction(ctx->querySuperFileTransaction());
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    lookupSuperFile(ctx, lsuperfn, AccessMode::readMeta, file, true, lsfn, true);
    if (!filenum||filenum>file->numSubFiles())
        return CTXSTRDUP(parentCtx, "");
    ret.append(file->querySubFile(filenum-1).queryLogicalName());
    return ret.detach();
}

FILESERVICES_API unsigned FILESERVICES_CALL fsFindSuperFileSubName(ICodeContext *ctx, const char *lsuperfn,const char *_lfn)
{
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Owned<ISimpleSuperFileEnquiry> enq = getSimpleSuperFileEnquiry(ctx, lsuperfn);
    if (enq) {
        unsigned n = enq->findSubName(lfn.str());
        return (n==NotFound)?0:n+1;
    }
    CImplicitSuperTransaction implicitTransaction(ctx->querySuperFileTransaction());
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    lookupSuperFile(ctx, lsuperfn, AccessMode::readMeta, file, true, lsfn, true);
    unsigned n = 0;
    // could do with better version of this TBD
    Owned<IDistributedFileIterator> iter = file->getSubFileIterator();
    ForEach(*iter) {
        n++;
        if (stricmp(iter->query().queryLogicalName(),lfn.str())==0)
            return n;
    }
    return 0;
}

FILESERVICES_API void FILESERVICES_CALL fsStartSuperFileTransaction(IGlobalCodeContext *gctx)
{
    fslStartSuperFileTransaction(gctx->queryCodeContext());
}

FILESERVICES_API void FILESERVICES_CALL fslStartSuperFileTransaction(ICodeContext *ctx)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    transaction->start();
    WUmessage(ctx,SeverityInformation,NULL,"StartSuperFileTransaction");
}

FILESERVICES_API void FILESERVICES_CALL fsAddSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn,const char *_lfn,unsigned atpos,bool addcontents, bool strict)
{
    fslAddSuperFile(gctx->queryCodeContext(),lsuperfn,_lfn,atpos,addcontents,strict);
}


FILESERVICES_API void FILESERVICES_CALL fslAddSuperFile(ICodeContext *ctx, const char *lsuperfn,const char *_lfn,unsigned atpos,bool addcontents, bool strict)
{
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    // NB: if adding contents, tell lookupSuperFile to cache the subfiles in the transaction
    if (!lookupSuperFile(ctx, lsuperfn, AccessMode::writeMeta, file, strict, lsfn, false, addcontents)) {
        // auto create
        fsCreateSuperFile(ctx,lsuperfn,false,false);
        lookupSuperFile(ctx, lsuperfn, AccessMode::writeMeta, file, true, lsfn, false, addcontents);
    }
    // Never add super file to itself
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    if (stricmp(file->queryLogicalName(), lfn.str()) == 0) {
        throw MakeStringException(0, "AddSuperFile: Adding super file %s to itself!", file->queryLogicalName());
    }
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    if  (strict||addcontents) {
        Owned<IDistributedSuperFile> subfile;
        subfile.setown(transaction->lookupSuperFile(lfn.str(), AccessMode::writeMeta));
        if (!subfile.get())
            throw MakeStringException(0, "AddSuperFile%s: Could not locate super file %s", addcontents?"(addcontents)":"",lfn.str());
        if (strict&&(subfile->numSubFiles()<1))
            throw MakeStringException(0, "AddSuperFile: Adding empty super file %s", lfn.str());
    }
    StringBuffer other;
    if (atpos>1)
        other.append("#").append(atpos);
    {
        CImplicitSuperTransaction implicitTransaction(transaction);
        file->addSubFile(lfn.str(),atpos>0,(atpos>1)?other.str():NULL,addcontents,transaction);
        file.clear(); // Must clear file before implicit transaction executed in destructor
    }
    StringBuffer s("AddSuperFile ('");
    s.append(lsfn).append("', '");
    s.append(lfn).append('\'');
    if (atpos)
        s.append(", ").append(atpos);
    if (addcontents)
        s.append(", addcontents");
    s.append(") ");
    if (transaction->active())
        s.append("trans");
    else
        s.append("done");
    WUmessage(ctx,SeverityInformation,NULL,s.str());
    AuditMessage(ctx,"AddSuperFile",lsfn.str(),lfn.str());
}

FILESERVICES_API void FILESERVICES_CALL fsRemoveSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn,const char *_lfn,bool del,bool remcontents)
{
    fslRemoveSuperFile(gctx->queryCodeContext(),lsuperfn,_lfn,del,remcontents);
}

FILESERVICES_API void FILESERVICES_CALL fslRemoveSuperFile(ICodeContext *ctx, const char *lsuperfn,const char *_lfn,bool del,bool remcontents)
{
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    StringBuffer lfn;
    if (_lfn)
        constructLogicalName(ctx, _lfn, lfn);
    lookupSuperFile(ctx, lsuperfn, AccessMode::writeMeta, file, true, lsfn, false, true);
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    {
        CImplicitSuperTransaction implicitTransaction(transaction);
        file->removeSubFile(_lfn?lfn.str():NULL,del,remcontents,transaction);
        file.clear(); // Must clear file before implicit transaction executed in destructor
    }
    StringBuffer s;
    if (_lfn)
        s.append("RemoveSuperFile ('");
    else
        s.append("ClearSuperFile ('");
    s.append(lsfn).append('\'');
    if (_lfn)
        s.append(", '").append(lfn.str()).append('\'');
    if (del)
        s.append(", del");
    if (remcontents)
        s.append(", remcontents");
    s.append(") ");
    if (transaction->active())
        s.append("trans");
    else
        s.append("done");
    WUmessage(ctx,SeverityInformation,NULL,s.str());
    AuditMessage(ctx,"RemoveSuperFile",lsfn.str(),lfn.str());
}

FILESERVICES_API void FILESERVICES_CALL fsClearSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn,bool del)
{
    fsRemoveSuperFile(gctx,lsuperfn,NULL,del);
}

FILESERVICES_API void FILESERVICES_CALL fsDeleteOwnedSubFiles(IGlobalCodeContext *gctx, const char *lsuperfn) // Obsolete
{
    fslRemoveOwnedSubFiles(gctx->queryCodeContext(), lsuperfn, false);
}

FILESERVICES_API void FILESERVICES_CALL fsRemoveOwnedSubFiles(IGlobalCodeContext *gctx, const char *lsuperfn, bool del)
{
    fslRemoveOwnedSubFiles(gctx->queryCodeContext(), lsuperfn, del);
}

FILESERVICES_API void FILESERVICES_CALL fslRemoveOwnedSubFiles(ICodeContext *ctx, const char *lsuperfn, bool del)
{
    Owned<IDistributedSuperFile> file;
    StringBuffer lsfn;
    lookupSuperFile(ctx, lsuperfn, AccessMode::tbdWrite, file, true, lsfn, false, true);
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    {
        CImplicitSuperTransaction implicitTransaction(transaction);
        file->removeOwnedSubFiles(del,transaction);
        file.clear(); // Must clear file before implicit transaction executed in destructor
    }
    VStringBuffer s("RemoveOwnedSubFiles ('%s'", lsfn.str());
    if (del)
        s.append(", del");
    s.append(") ");
    if (transaction->active())
        s.append("trans");
    else
        s.append("done");
    WUmessage(ctx,SeverityInformation,NULL,s.str());
    AuditMessage(ctx,"RemoveOwnedSubFiles",lsfn.str());
}

FILESERVICES_API void FILESERVICES_CALL fslClearSuperFile(ICodeContext *ctx, const char *lsuperfn,bool del)
{
    fslRemoveSuperFile(ctx,lsuperfn,NULL,del);
}
FILESERVICES_API void FILESERVICES_CALL fsSwapSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn1,const char *lsuperfn2)
{
    fslSwapSuperFile(gctx->queryCodeContext(),lsuperfn1,lsuperfn2);
}


FILESERVICES_API void FILESERVICES_CALL fslSwapSuperFile(ICodeContext *ctx, const char *lsuperfn1,const char *lsuperfn2)
{
    StringBuffer lsfn1;
    StringBuffer lsfn2;
    Owned<IDistributedSuperFile> file1;
    Owned<IDistributedSuperFile> file2;
    lookupSuperFile(ctx, lsuperfn1, AccessMode::writeMeta, file1, true, lsfn1,false);
    lookupSuperFile(ctx, lsuperfn2, AccessMode::writeMeta, file2, true,lsfn2,false);

    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    {
        CImplicitSuperTransaction implicitTransaction(transaction);
        file1->swapSuperFile(file2,transaction);
        // Must clear files before implicit transaction executed in destructor
        file1.clear();
        file2.clear();
    }
    StringBuffer s("SwapSuperFile ('");
    s.append(lsfn1).append("', '");
    s.append(lsfn2).append("') '");
    if (transaction->active())
        s.append("trans");
    else
        s.append("done");
    WUmessage(ctx,SeverityInformation,NULL,s.str());
    AuditMessage(ctx,"SwapSuperFile",lsfn1.str(),lsfn2.str());
}

FILESERVICES_API void FILESERVICES_CALL fsReplaceSuperFile(IGlobalCodeContext *gctx, const char *lsuperfn,const char *lfn,const char *bylfn)
{
    fslReplaceSuperFile(gctx->queryCodeContext(),lsuperfn,lfn,bylfn);
}

FILESERVICES_API void FILESERVICES_CALL fslReplaceSuperFile(ICodeContext *ctx, const char *lsuperfn,const char *lfn,const char *bylfn)
{
    unsigned at = fsFindSuperFileSubName(ctx,lsuperfn,lfn);
    if (!at)
        return;
    fslRemoveSuperFile(ctx,lsuperfn,lfn);
    fslAddSuperFile(ctx,lsuperfn,bylfn,at);
}

FILESERVICES_API void FILESERVICES_CALL fsFinishSuperFileTransaction(IGlobalCodeContext *gctx, bool rollback)
{
    fslFinishSuperFileTransaction(gctx->queryCodeContext(),rollback);
}

FILESERVICES_API void FILESERVICES_CALL fslFinishSuperFileTransaction(ICodeContext *ctx, bool rollback)
{
    IDistributedFileTransaction *transaction = ctx->querySuperFileTransaction();
    assertex(transaction);
    if (transaction->active()) {
        if (rollback)
            transaction->rollback();
        else
            transaction->commit();
        StringBuffer s("FinishSuperFileTransaction ");
        if (rollback)
            s.append("rollback");
        else
            s.append("commit");
        WUmessage(ctx,SeverityInformation,NULL,s.str());
    }
    else {
        StringBuffer s("Invalid FinishSuperFileTransaction ");
        if (rollback)
            s.append("rollback");
        else
            s.append("done");
        s.append(", transaction not active");
        WUmessage(ctx,SeverityInformation,NULL,s.str());
    }
}


static char * implementForeignLogicalFileName(ICodeContext *ctx, const char *_lfn, const char *foreigndali, bool abspath, bool omitClusterPrefix)
{
    StringBuffer lfns;

    Owned<IConstWorkUnit> wu;
    if (!omitClusterPrefix && _lfn&&(*_lfn != '~'))
        wu.setown(getWorkunit(ctx));
    constructLogicalName(wu, _lfn, lfns);
    CDfsLogicalFileName lfn;
    lfn.set(lfns.str());
    if (foreigndali&&*foreigndali) {
        SocketEndpoint ep(foreigndali);
        lfn.setForeign(ep,false);
    }
    else
        lfn.clearForeign();
    StringBuffer ret;
    if (abspath)
        ret.append('~');
    lfn.get(ret);
    return ret.detach();
}

FILESERVICES_API char *  FILESERVICES_CALL fsForeignLogicalFileName(ICodeContext *ctx, const char *lfn, const char *foreigndali, bool abspath)
{
    return implementForeignLogicalFileName(ctx, lfn, foreigndali, abspath, false);
}

FILESERVICES_API char *  FILESERVICES_CALL fsForeignLogicalFileName_v2(ICodeContext *ctx, const char *lfn, const char *foreigndali, bool abspath, bool omitClusterPrefix)
{
    return implementForeignLogicalFileName(ctx, lfn, foreigndali, abspath, omitClusterPrefix);
}


FILESERVICES_API char *  FILESERVICES_CALL fsExternalLogicalFileName(const char *location,const char *path,bool abspath)
{
    StringBuffer ret;
    if (abspath)
        ret.append('~');
    CDfsLogicalFileName lfn;
    lfn.setExternal(location,path);
    return lfn.get(ret).detach();
}


FILESERVICES_API char *  FILESERVICES_CALL fsWaitDfuWorkunit(IGlobalCodeContext *gctx, const char *wuid, int timeout, const char * espServerIpPort)
{
    return fslWaitDfuWorkunit(gctx->queryCodeContext(),wuid,timeout,espServerIpPort);
}

FILESERVICES_API char *  FILESERVICES_CALL fslWaitDfuWorkunit(ICodeContext *ctx, const char *wuid, int timeout, const char * espServerIpPort)
{
    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);
    StringBuffer s("Waiting for DFU Workunit ");
    s.append(wuid);
    WUmessage(ctx,SeverityInformation,"WaitDfuWorkunit",s.str());
    StringBuffer state;
    wu.clear();
    blockUntilComplete("WaitDfuWorkunit", server, ctx, wuid, timeout, &state);
    s.clear().append("Finished waiting for DFU Workunit ").append(wuid).append(" state=").append(state.str());
    WUmessage(ctx,SeverityInformation,"WaitDfuWorkunit",s.str());
    return state.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsAbortDfuWorkunit(IGlobalCodeContext *gctx, const char *wuid, const char * espServerIpPort)
{
    fslAbortDfuWorkunit(gctx->queryCodeContext(),wuid,espServerIpPort);
}

FILESERVICES_API void FILESERVICES_CALL fslAbortDfuWorkunit(ICodeContext *ctx, const char *wuid, const char * espServerIpPort)
{
    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);
    Owned<IClientAbortDFUWorkunit> abortReq = server.createAbortDFUWorkunitRequest();
    setContainerLocalCertificate(abortReq->rpc());
    abortReq->setWuid(wuid);
    Linked<IClientAbortDFUWorkunitResponse> abortResp = server.AbortDFUWorkunit(abortReq);
    StringBuffer s("DFU Workunit Abort Requested for ");
    s.append(wuid);
    WUmessage(ctx,SeverityInformation,"AbortDfuWorkunit",s.str());
}

FILESERVICES_API void  FILESERVICES_CALL fsMonitorLogicalFileName(ICodeContext *ctx, const char *eventname, const char *_lfn,int shotcount, const char * espServerIpPort)
{
    CTXFREE(parentCtx, fsfMonitorLogicalFileName(ctx, eventname, _lfn,shotcount, espServerIpPort));
}

FILESERVICES_API char *  FILESERVICES_CALL fsfMonitorLogicalFileName(ICodeContext *ctx, const char *eventname, const char *_lfn,int shotcount, const char * espServerIpPort)
{
    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    if (shotcount == 0)
        shotcount = -1;

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Monitor Logical Filename");
    clientSpan->setSpanAttribute("filename", lfn);
    try
    {
        Owned<IClientDfuMonitorRequest> req = server.createDfuMonitorRequest();
        setContainerLocalCertificate(req->rpc());
        req->setEventName(eventname);
        req->setLogicalName(lfn);
        req->setShotLimit(shotcount);
        Owned<IClientDfuMonitorResponse> result = server.DfuMonitor(req);
        StringBuffer wuid(result->getWuid());
        StringBuffer s("MonitorLogicalFileName ('");
        s.append(lfn).append("'): ").append(wuid);
        WUmessage(ctx,SeverityInformation,NULL,s.str());
        wu.clear();
        if (wuid.length()!=0)
        clientSpan->setSpanAttribute("wuid", wuid);
            blockUntilComplete("MonitorLogicalFileName",server,ctx,wuid.str(),1000*60*60,NULL,true);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}

FILESERVICES_API void  FILESERVICES_CALL fsMonitorFile(ICodeContext *ctx, const char *eventname,const char *ip, const char *filename, bool sub, int shotcount, const char * espServerIpPort)
{
    CTXFREE(parentCtx, fsfMonitorFile(ctx, eventname,ip, filename, sub, shotcount, espServerIpPort));
}

FILESERVICES_API char *  FILESERVICES_CALL fsfMonitorFile(ICodeContext *ctx, const char *eventname,const char *ip, const char *filename, bool sub, int shotcount, const char * espServerIpPort)
{
    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(getAccessibleEspServerURL(espServerIpPort,wu));
    setServerAccess(server, wu);
    if (shotcount == 0)
        shotcount = -1;

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Monitor File");
    clientSpan->setSpanAttribute("filename", filename);
    try
    {
        Owned<IClientDfuMonitorRequest> req = server.createDfuMonitorRequest();
        setContainerLocalCertificate(req->rpc());
        req->setEventName(eventname);
        req->setIp(ip);
        req->setFilename(filename);
        req->setShotLimit(shotcount);
        Owned<IClientDfuMonitorResponse> result = server.DfuMonitor(req);
        StringBuffer wuid(result->getWuid());
        StringBuffer s("MonitorFile (");
        s.append(ip).append(", '").append(filename).append("'): '").append(wuid);
        WUmessage(ctx,SeverityInformation,NULL,s.str());
        wu.clear();
        if (wuid.length()!=0)
        clientSpan->setSpanAttribute("wuid", wuid);
            blockUntilComplete("MonitorFile",server,ctx,wuid.str(),1000*60*60,NULL,true);

        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}

FILESERVICES_API void FILESERVICES_CALL fsSetFileDescription(ICodeContext *ctx, const char *logicalfilename, const char *value)
{
    StringBuffer lfn;
    constructLogicalName(ctx, logicalfilename, lfn);

    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser);
    if (df) {
        DistributedFilePropertyLock lock(df);
        lock.queryAttributes().setProp("@description",value);
    }
    else
        throw MakeStringException(0, "SetFileDescription: Could not locate file %s", lfn.str());
}

FILESERVICES_API char *  FILESERVICES_CALL fsGetFileDescription(ICodeContext *ctx, const char *logicalfilename)
{
    StringBuffer lfn;
    constructLogicalName(ctx, logicalfilename, lfn);

    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = wsdfs::lookup(lfn.str(), udesc, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser, INFINITE);
    if (!df)
        throw MakeStringException(0, "GetFileDescription: Could not locate file %s", lfn.str());
    const char * ret = df->queryAttributes().queryProp("@description");
    if (ret)
        return CTXSTRDUP(parentCtx, ret);
    else
        return CTXSTRDUP(parentCtx, "");
}

static void listRemoteDirectoryFiles(RemoteFilename &rfn, const char *mask, bool sub, size32_t &__lenResult, void *&__result)
{
    MemoryBuffer mb;
    Owned<IFile> f = createIFile(rfn);
    if (f) {
        StringBuffer s;
        StringBuffer ds;
        Owned<IDirectoryIterator> di = f->directoryFiles(mask,sub);
        if (di) {
            ForEach(*di) {
                di->getName(s.clear());
                __int64 fsz = di->getFileSize();
                CDateTime dt;
                di->getModifiedTime(dt);
                size32_t sz = s.length();
                dt.getString(ds.clear());
                ds.padTo(19);
                mb.append(sz).append(sz,s.str()).append(fsz).append(19,ds.str());
            }
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsLogicalFileList(ICodeContext *ctx, size32_t & __lenResult,void * & __result, const char *mask, bool includenormal, bool includesuper, bool unknownszero, const char *foreigndali)
{
    IEngineContext *engineCtx = ctx->queryEngineContext();
    if (engineCtx && !engineCtx->allowDaliAccess())
    {
        Owned<IException> e = MakeStringException(-1, "FileServices.LogicalFileList cannot access Dali in this context - this normally means it is being called from a thor slave");
        EXCLOG(e, NULL);
        throw e.getClear();
    }
    MemoryBuffer mb;
    if (!mask||!*mask)
        mask ="*";
    StringBuffer masklower(mask);
    masklower.toLowerCase();

    Owned<INode> foreignNode;
    if (!isEmptyString(foreigndali))
    {
        SocketEndpoint ep(foreigndali);
        foreignNode.setown(createINode(ep));
    }
    Owned<IPropertyTreeIterator> iter = queryDistributedFileDirectory().getDFAttributesIterator(masklower.str(),ctx->queryUserDescriptor(),true,includesuper,foreignNode);
    if (iter) {
        StringBuffer s;
        ForEach(*iter) {
            IPropertyTree &attr=iter->query();
            const char *name = attr.queryProp("@name");
            if (!name||!*name)
                continue;
            int numsub = attr.getPropInt("@numsubfiles",-1);
            bool issuper = numsub>=0;
            if (issuper) {
                if (!includesuper)
                    continue;
            }
            else {
                if (!includenormal)
                    continue;
            }
            size32_t sz = strlen(name);
            mb.append(sz).append(sz,name);
            mb.append(issuper);
            __int64 i64;
            __int64 fsz = attr.getPropInt64("@size",-1);
            if ((fsz==-1)&&(unknownszero||(numsub==0)))
                fsz = 0;
            mb.append(fsz);
            i64 = attr.getPropInt64("@recordCount",-1);
            if ((i64==-1)&&(fsz!=-1)) {
                int rsz = attr.getPropInt("@recordSize",0);
                if (rsz>0)
                    i64 = fsz/rsz;
            }
            if ((i64==-1)&&(unknownszero||(numsub==0)))
                i64 = 0;
            mb.append(i64);
            attr.getProp("@modified",s.clear());
            s.padTo(19);
            mb.append(19,s.str());
            attr.getProp("@owner",s.clear());
            sz = s.length();
            mb.append(sz).append(sz,s.str());
            attr.getProp("@group",s.clear());
            sz = s.length();
            mb.append(sz).append(sz,s.str());
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();

}

FILESERVICES_API void FILESERVICES_CALL fsSuperFileContents(ICodeContext *ctx, size32_t & __lenResult,void * & __result, const char *lsuperfn, bool recurse)
{
    MemoryBuffer mb;
    Owned<ISimpleSuperFileEnquiry> enq;
    if (!recurse)
        enq.setown(getSimpleSuperFileEnquiry(ctx, lsuperfn));
    if (enq) {
        StringArray subs;
        enq->getContents(subs);
        ForEachItemIn(i,subs) {
            const char *name = subs.item(i);
            size32_t sz = strlen(name);
            if (!sz)
                continue;
            mb.append(sz).append(sz,name);
        }
    }
    else {
        CImplicitSuperTransaction implicitTransaction(ctx->querySuperFileTransaction());
        Owned<IDistributedSuperFile> file;
        StringBuffer lsfn;
        lookupSuperFile(ctx, lsuperfn, AccessMode::readMeta, file, true, lsfn, true);
        Owned<IDistributedFileIterator> iter = file->getSubFileIterator(recurse);
        StringBuffer name;
        ForEach(*iter) {
            iter->getName(name.clear());
            size32_t sz = name.length();
            if (!sz)
                continue;
            mb.append(sz).append(sz,name.str());
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsLogicalFileSuperOwners(ICodeContext *ctx,size32_t & __lenResult,void * & __result, const char *logicalfilename)
{
    MemoryBuffer mb;
    StringBuffer lfn;
    constructLogicalName(ctx, logicalfilename, lfn);
    StringArray owners;
    if (queryDistributedFileDirectory().getFileSuperOwners(lfn.str(),owners)) {
        ForEachItemIn(i,owners) {
            const char *name = owners.item(i);
            size32_t sz = strlen(name);
            if (!sz)
                continue;
            mb.append(sz).append(sz,name);
        }
    }
    else {
        Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc,AccessMode::tbdRead,false,true, nullptr, defaultPrivilegedUser); // lock super-owners
        if (df) {
            Owned<IDistributedSuperFileIterator> iter = df->getOwningSuperFiles();
            ForEach(*iter) {
                const char *name = iter->queryName();
                size32_t sz = strlen(name);
                if (!sz)
                    continue;
                mb.append(sz).append(sz,name);
            }
        }
        else
            throw MakeStringException(0, "LogicalFileSuperOwners: Could not locate file %s", lfn.str());
    }
    __lenResult = mb.length();
    __result = mb.detach();
}


FILESERVICES_API int  FILESERVICES_CALL fsCompareFiles(ICodeContext *ctx,const char *name1, const char *name2,bool logicalonly, bool usecrcs)
{
    StringBuffer lfn1;
    constructLogicalName(ctx, name1, lfn1);
    StringBuffer lfn2;
    constructLogicalName(ctx, name2, lfn2);
    StringBuffer retstr;
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    int ret = queryDistributedFileDirectory().fileCompare(lfn1.str(),lfn2.str(),usecrcs?DFS_COMPARE_FILES_PHYSICAL_CRCS:(logicalonly?DFS_COMPARE_FILES_LOGICAL:DFS_COMPARE_FILES_PHYSICAL),retstr,udesc);
    if (ret==DFS_COMPARE_RESULT_FAILURE)
        throw MakeStringException(ret,"CompareLogicalFiles: %s",retstr.str());
    return ret;
}

FILESERVICES_API char *  FILESERVICES_CALL fsVerifyFile(ICodeContext *ctx,const char *name,bool usecrcs)
{
    StringBuffer lfn;
    constructLogicalName(ctx, name, lfn);
    StringBuffer retstr;
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    if (queryDistributedFileDirectory().filePhysicalVerify(lfn.str(),udesc,usecrcs,retstr))
        retstr.append("OK");
    return retstr.detach();
}


// RemotePull

/*
varstring RemotePull(
                      const varstring remoteEspFsURL,                   // remote ESP URL e.g. 'http://10.173.34.60:8010/FileSpray'
                      const varstring sourceLogicalName,                // local
                      const varstring destinationGroup,                 // remote
                      const varstring destinationLogicalName,           // remote (NB full name required)
                      integer4 timeOut=-1,
                      integer4 maxConnections=-1,
                      boolean allowoverwrite=false,
                      boolean replicate=false,
                      boolean asSuperfile=false);
*/


FILESERVICES_API char * FILESERVICES_CALL fsfRemotePull_impl(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress,
                                                     bool noSplit,
                                                     int expireDays,
                                                     const char *username,
                                                     const char *userPw)
{
    LOG(MCauditInfo, "RemotePull(%s):  %s%s", remoteEspFsURL,sourceLogicalName,asSuperfile?" as superfile":"");

    CClientFileSpray server;
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    server.addServiceUrl(remoteEspFsURL);
    setServerAccess(server, wu);

    Owned<IClientCopy> req = server.createCopyRequest();
    setContainerLocalCertificate(req->rpc());
    if (asSuperfile)
        req->setSuperCopy(true);

    StringBuffer _sourceLogicalName, _destinationLogicalName;
    constructLogicalName(wu, sourceLogicalName, _sourceLogicalName);
    // destination name assumed complete (so just skip ~ *)
    while ((*destinationLogicalName=='~')||isspace(*destinationLogicalName))
        destinationLogicalName++;
    _destinationLogicalName.append(destinationLogicalName);
    if (strstr(_destinationLogicalName.str(),"::")==NULL)
        _destinationLogicalName.insert(0,".::");
    StringBuffer _destGroup;
    _destGroup.append(destinationGroup);
    req->setSourceLogicalName(_sourceLogicalName.str());
    req->setDestLogicalName(_destinationLogicalName.str());
    req->setDestGroup(_destGroup.str());
    if (compress)
        req->setCompress(true);
    if (wrap)
        req->setWrap(true);
    StringBuffer sourceDali;
    queryCoven().queryComm().queryGroup().queryNode(0).endpoint().getEndpointHostText(sourceDali);
    req->setSourceDali(sourceDali);
    req->setOverwrite(overwrite);
    req->setReplicate(replicate);
    if (forcePush)
        req->setPush(true);
    if (transferBufferSize>0)
        req->setTransferBufferSize(transferBufferSize);

    if (noSplit)
        req->setNosplit(true);

    req->setExpireDays(expireDays);

    // Handle username/psw
    if (!isEmptyString(username))
    {
        server.setUsernameToken(username, userPw, nullptr);
        req->setSrcusername(username);
        if (!isEmptyString(userPw))
           req->setSrcpassword(userPw);
    }

    OwnedActiveSpanScope clientSpan = queryThreadedActiveSpan()->createClientSpan("Dfu Remote Pull");
    clientSpan->setSpanAttribute("sourceFilename", sourceLogicalName);
    clientSpan->setSpanAttribute("destinationFilename", destinationLogicalName);
    try
    {
        Owned<IClientCopyResponse> result = server.Copy(req);

        StringBuffer wuid(result->getResult());
        if (!wuid.length())
        {
            const IMultiException* excep = &result->getExceptions();
            if ((excep != NULL) && (excep->ordinality() > 0))
            {
                StringBuffer errmsg;
                excep->errorMessage(errmsg);
                throw MakeStringExceptionDirect(0, errmsg.str());
            }
            else
            {
                throw MakeStringExceptionDirect(0, "Result's dfu WUID is empty");
            }
        }

        wu.clear();

        clientSpan->setSpanAttribute("wuid", wuid);
        blockUntilComplete("RemotePull", server, ctx, wuid, timeOut);
        return wuid.detach();
    }
    catch (IException *e)
    {
        clientSpan->recordException(e, true, true);
        throw;
    }
}

FILESERVICES_API char * FILESERVICES_CALL fsfRemotePull(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress)
{
    return fsfRemotePull_impl(ctx, remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, overwrite, replicate, asSuperfile,forcePush,transferBufferSize, wrap, compress, false, -1, nullptr, nullptr);
}

FILESERVICES_API void FILESERVICES_CALL fsRemotePull(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress)
{
    CTXFREE(parentCtx, fsfRemotePull_impl(ctx, remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, overwrite, replicate, asSuperfile,forcePush,transferBufferSize, wrap, compress, false, -1, nullptr, nullptr));

}

FILESERVICES_API char * FILESERVICES_CALL fsfRemotePull_v2(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress,
                                                     bool noSplit,
                                                     int expireDays)
{
    return fsfRemotePull_impl(ctx, remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, overwrite, replicate, asSuperfile,forcePush,transferBufferSize, wrap, compress, noSplit, expireDays, nullptr, nullptr);
}

FILESERVICES_API void FILESERVICES_CALL fsRemotePull_v2(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress,
                                                     bool noSplit,
                                                     int expireDays)
{
    CTXFREE(parentCtx, fsfRemotePull_impl(ctx, remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, overwrite, replicate, asSuperfile,forcePush,transferBufferSize, wrap, compress, noSplit, expireDays, nullptr, nullptr));

}

FILESERVICES_API char * FILESERVICES_CALL fsfRemotePull_v3(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress,
                                                     bool noSplit,
                                                     int expireDays,
                                                     const char *username,
                                                     const char *userPw)
{
    return fsfRemotePull_impl(ctx, remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, overwrite, replicate, asSuperfile,forcePush,transferBufferSize, wrap, compress, noSplit, expireDays, username, userPw);
}

FILESERVICES_API void FILESERVICES_CALL fsRemotePull_v3(ICodeContext *ctx,
                                                     const char * remoteEspFsURL,
                                                     const char * sourceLogicalName,
                                                     const char *destinationGroup,
                                                     const char * destinationLogicalName,
                                                     int timeOut,
                                                     int maxConnections,
                                                     bool overwrite,
                                                     bool replicate,
                                                     bool asSuperfile,
                                                     bool forcePush,
                                                     int transferBufferSize,
                                                     bool wrap,
                                                     bool compress,
                                                     bool noSplit,
                                                     int expireDays,
                                                     const char *username,
                                                     const char *userPw)
{
    CTXFREE(parentCtx, fsfRemotePull_impl(ctx, remoteEspFsURL, sourceLogicalName, destinationGroup, destinationLogicalName, timeOut, maxConnections, overwrite, replicate, asSuperfile,forcePush,transferBufferSize, wrap, compress, noSplit, expireDays, username, userPw));

}


FILESERVICES_API void FILESERVICES_CALL fsLogicalFileSuperSubList(ICodeContext *ctx, size32_t & __lenResult,void * & __result)
{
    MemoryBuffer mb;
    getLogicalFileSuperSubList(mb, ctx->queryUserDescriptor());
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API  void FILESERVICES_CALL fsPromoteSuperFileList(ICodeContext * ctx,bool isAllLsuperfns,size32_t  lenLsuperfns,const void * lsuperfns,const char * addhead,bool deltail,bool createonlyonesuperfile,bool reverse)
{
    CTXFREE(parentCtx, fsfPromoteSuperFileList(ctx,isAllLsuperfns,lenLsuperfns,lsuperfns,addhead,deltail,createonlyonesuperfile,reverse));
}

FILESERVICES_API  char * FILESERVICES_CALL fsfPromoteSuperFileList(ICodeContext * ctx,bool isAllLsuperfns,size32_t  lenLsuperfns,const void * lsuperfns,const char * addhead,bool deltail,bool createonlyonesuperfile,bool reverse)
{
    Owned<IConstWorkUnit> wu = getWorkunit(ctx);
    MemoryBuffer mb;
    StringBuffer lfn;
    UnsignedArray lfnofs;
    const char *s = (const char *)lsuperfns;
    // MORE - For now, we need a local transaction
    CheckNotInTransaction(ctx, "PromoteSuperFile");

    while ((size32_t)(s-(const char *)lsuperfns)<lenLsuperfns) {
        constructLogicalName(wu,s,lfn.clear());
        lfnofs.append(mb.length());
        mb.append(lfn);
        s = s+strlen(s)+1;
    }
    PointerArray lfns;
    ForEachItemIn(i,lfnofs) {
        lfns.append((void *)(mb.toByteArray()+lfnofs.item(reverse?(lfnofs.ordinality()-i-1):i)));
    }
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    StringArray toadd;
    toadd.appendListUniq(addhead, ",");
    StringBuffer addlist;
    ForEachItemIn(i1,toadd) {
        if (addlist.length())
            addlist.append(',');
        constructLogicalName(wu,toadd.item(i1),addlist);
    }
    toadd.kill();
    queryDistributedFileDirectory().promoteSuperFiles(lfns.ordinality(),(const char **)lfns.getArray(),addlist.str(),deltail,createonlyonesuperfile,udesc.get(),(unsigned)-1,toadd);
    addlist.clear();
    ForEachItemIn(i2,toadd) {
        if (addlist.length())
            addlist.append(',');
        constructLogicalName(wu,toadd.item(i2),addlist);
    }
    return addlist.detach();
}

FILESERVICES_API unsigned __int64 FILESERVICES_CALL fsGetUniqueInteger(ICodeContext * ctx, const char *foreigndali)
{
    SocketEndpoint ep;
    if (foreigndali&&*foreigndali)
        ep.set(foreigndali);
    IEngineContext *engineContext = ctx->queryEngineContext();
    if (engineContext)
        return engineContext->getGlobalUniqueIds(1,&ep);
    return getGlobalUniqueIds(1,&ep);
}

FILESERVICES_API void FILESERVICES_CALL fsAddFileRelationship(ICodeContext * ctx,const char *primary, const char *secondary, const char *primflds, const char *secflds, const char *kind, const char *cardinality, bool payload, const char *description)
{
    StringBuffer pfn;
    constructLogicalName(ctx, primary, pfn);
    StringBuffer sfn;
    constructLogicalName(ctx, secondary, sfn);
    queryDistributedFileDirectory().addFileRelationship(pfn.str(),sfn.str(),primflds,secflds,kind,cardinality,payload,ctx->queryUserDescriptor(), description);
    StringBuffer s("AddFileRelationship('");
    s.append(pfn.str()).append("','").append(sfn.str()).append("','").append(primflds?primflds:"").append("','").append(secflds?secflds:"").append("','").append(kind?kind:"").append("') done");
    WUmessage(ctx,SeverityInformation,NULL,s.str());

}

static inline void addmbstr(MemoryBuffer &mb,const char *s)
{
    size32_t sz = strlen(s);
    mb.append(sz).append(sz,s);
}

FILESERVICES_API void FILESERVICES_CALL fsFileRelationshipList(ICodeContext * ctx,size32_t & __lenResult,void * & __result,const char *primary, const char *secondary, const char *primflds, const char *secflds, const char *kind)
{
    StringBuffer pfn;
    if (primary&&*primary)
        constructLogicalName(ctx, primary, pfn);
    StringBuffer sfn;
    if (secondary&&*secondary)
        constructLogicalName(ctx, secondary, sfn);
    MemoryBuffer mb;
    Owned<IFileRelationshipIterator> iter = queryDistributedFileDirectory().lookupFileRelationships(pfn.str(),sfn.str(),primflds,secflds,kind);
    if (iter) {
        StringBuffer s;
        ForEach(*iter) {
            IFileRelationship &rel=iter->query();
            addmbstr(mb,rel.queryPrimaryFilename());
            addmbstr(mb,rel.querySecondaryFilename());
            addmbstr(mb,rel.queryPrimaryFields());
            addmbstr(mb,rel.querySecondaryFields());
            addmbstr(mb,rel.queryKind());
            addmbstr(mb,rel.queryCardinality());
            mb.append((byte)(rel.isPayload()?1:0));
            addmbstr(mb,rel.queryDescription());
        }
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsRemoveFileRelationship(ICodeContext * ctx,const char *primary, const char *secondary, const char *primflds, const char *secflds, const char *kind)
{
    StringBuffer pfn;
    if (primary&&*primary)
        constructLogicalName(ctx, primary, pfn);
    StringBuffer sfn;
    if (secondary&&*secondary)
        constructLogicalName(ctx, secondary, sfn);
    queryDistributedFileDirectory().removeFileRelationships(pfn.str(),sfn.str(),primflds,secflds,kind);
}

FILESERVICES_API void FILESERVICES_CALL fsSetColumnMapping(ICodeContext * ctx,const char *filename, const char *mapping)
{
    StringBuffer lfn;
    constructLogicalName(ctx, filename, lfn);
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),ctx->queryUserDescriptor(),AccessMode::tbdWrite,false,false,nullptr,defaultPrivilegedUser);
    if (df)
        df->setColumnMapping(mapping);
    else
        throw MakeStringException(-1, "SetColumnMapping: Could not find logical file %s", lfn.str());
}

FILESERVICES_API char *  FILESERVICES_CALL fsfGetColumnMapping(ICodeContext * ctx,const char *filename)
{
    StringBuffer lfn;
    constructLogicalName(ctx, filename, lfn);
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),ctx->queryUserDescriptor(),AccessMode::tbdWrite,false,false,nullptr,defaultPrivilegedUser);
    if (df) {
        StringBuffer mapping;
        df->getColumnMapping(mapping);
        return mapping.detach();
    }
    throw MakeStringException(-1, "GetColumnMapping: Could not find logical file %s", lfn.str());
    return NULL;
}


FILESERVICES_API char *  FILESERVICES_CALL fsfRfsQuery(const char *server, const char *query)
{
    StringBuffer ret;
    ret.append('~');
    CDfsLogicalFileName lfn;
    lfn.setQuery(server,query);
    if (!lfn.isSet())
        throw MakeStringException(-1, "RfsQuery invalid parameter");
    return lfn.get(ret).detach();
}

FILESERVICES_API void FILESERVICES_CALL fsRfsAction(const char *server, const char *query)
{
    CDfsLogicalFileName lfn;
    lfn.setQuery(server,query);
    if (!lfn.isSet())
        throw MakeStringException(-1, "RfsAction invalid parameter");
    RemoteFilename rfn;
    lfn.getExternalFilename(rfn);
    Owned<IFile> file = createIFile(rfn);
    Owned<IFileIO> fileio = file->open(IFOread);
    if (fileio) {
        // lets just try reading a byte to cause action
        byte b;
        fileio->read(0,sizeof(b),&b);
    }
}


FILESERVICES_API char *  FILESERVICES_CALL fsfGetHostName(const char *ipaddress)
{
// not a common routine (no Jlib function!) only support IPv4 initially
    StringBuffer ret;
    if (ipaddress&&*ipaddress) {
        IpAddress ip(ipaddress);
        lookupHostName(ip,ret);
    }
    else
        GetHostName(ret);
    return ret.detach();
}



FILESERVICES_API char *  FILESERVICES_CALL fsfResolveHostName(const char *hostname)
{
    StringBuffer ret;
    SocketEndpoint ep(hostname);
    //NB: Get a string for the resolved IP, not the hostname
    ep.getIpText(ret);
    return ret.detach();
}

static void checkExternalFileRights(ICodeContext *ctx,const char *scope,bool rd,bool wr)
{
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    unsigned auditflags = 0;
    if (rd)
        auditflags |= (DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED);
    if (wr)
        auditflags |= (DALI_LDAP_AUDIT_REPORT|DALI_LDAP_WRITE_WANTED);
    SecAccessFlags perm = queryDistributedFileDirectory().getFScopePermissions(scope,udesc,auditflags);
    if (wr) {
        if (!HASWRITEPERMISSION(perm)) {
            throw makeStringExceptionV(-1,"Write permission denied for scope %s", scope);
        }
    }
    if (rd) {
        if (!HASREADPERMISSION(perm)) {
            throw makeStringExceptionV(-1,"Read permission denied for scope %s", scope);
        }
    }
}

static void checkExternalFilePath(ICodeContext *ctx,IPropertyTree *plane,const char *host,
    const char *path,bool rd,bool wr,RemoteFilename &rfn)
{
    if (containsRelPaths(path)) //Detect a path like: a/../../../f
        throw makeStringExceptionV(-1,"Invalid file path %s",path);

    CDfsLogicalFileName dlfn;
    if (plane)
    {
        //allow relative path if a plane name is specified.
        if (isAbsolutePath(path))
        {
            const char *relativePath = getRelativePath(path,plane->queryProp("@prefix"));
            if (nullptr == relativePath)
                throw makeStringExceptionV(-1,"Invalid plane path %s.",path);
            path = relativePath;
        }
        dlfn.setPlaneExternal(plane->queryProp("@name"),path);
    }
    else
        dlfn.setExternal(host,path); //for backward compatibility
    dlfn.getExternalFilename(rfn);

    Owned<IFile> f = createIFile(rfn);
    if (f->exists() && (f->isDirectory()==fileBool::foundYes))
        checkExternalFileRights(ctx,dlfn.get(),rd,wr);
    else
    {
        StringBuffer scopes;
        checkExternalFileRights(ctx,dlfn.getScopes(scopes),rd,wr);
    }
}

//this function is very similar to getDropZoneAndValidateHostAndPath in TpCommon, and both should be replaced/moved so common.
static IPropertyTree *checkPlaneOrHost(const char *planeName,const char *host,const char *path)
{
    if (!isEmptyString(planeName))
    {
        Owned<IPropertyTree> plane = getDropZonePlane(planeName); //Only support DropZone for now.
        if (!plane)
            throw makeStringExceptionV(-1,"DropZone %s not found.",planeName);
        return plane.getClear();
    }

    if (!isAbsolutePath(path)) //Relative paths permitted, check host only
        path = nullptr;

    Owned<IPropertyTree> plane = findDropZonePlane(path,host,true,isContainerized());
    if (plane)
        return plane.getClear();

#ifndef _CONTAINERIZED
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (env->isDropZoneRestrictionEnabled())
        throw makeStringExceptionV(-1,"DropZone Plane not found for host %s path %s.",host,path);

    LOG(MCdebugInfo, "No matching drop zone path on '%s' to file path: '%s'",host,path);
#endif

    return nullptr;
}

static void implementMoveExternalFile(ICodeContext *ctx,const char *location,
    const char *frompath,const char *topath,const char *planename)
{
    Owned<IPropertyTree> plane = checkPlaneOrHost(planename,location,frompath);
    RemoteFilename fromrfn,torfn;
    checkExternalFilePath(ctx,plane,location,frompath,true,true,fromrfn);
    checkExternalFilePath(ctx,plane,location,topath,false,true,torfn);

    Owned<IFile> fileto = createIFile(torfn);
    if (fileto->exists())
        throw MakeStringException(-1,"fsMoveExternalFile: Destination %s already exists", topath);
    fileto.clear();
    Owned<IFile> file = createIFile(fromrfn);
    file->move(topath);
    StringBuffer s("MoveExternalFile ('");
    s.append(location).append(',').append(frompath).append(',').append(topath).append(") done");
    WUmessage(ctx,SeverityInformation,NULL,s.str());
    AuditMessage(ctx,"MoveExternalFile",frompath,topath);
}

FILESERVICES_API void  FILESERVICES_CALL fsMoveExternalFile(ICodeContext *ctx,const char *location,const char *frompath,const char *topath)
{
    implementMoveExternalFile(ctx,location,frompath,topath,nullptr);
}

FILESERVICES_API void  FILESERVICES_CALL fsMoveExternalFile_v2(ICodeContext *ctx,const char *location,
    const char *frompath,const char *topath,const char *planename)
{
    implementMoveExternalFile(ctx,location,frompath,topath,planename);
}

static void implementDeleteExternalFile(ICodeContext *ctx,const char *location,const char *path,const char *planename)
{
    Owned<IPropertyTree> plane = checkPlaneOrHost(planename,location,path);
    RemoteFilename rfn;
    checkExternalFilePath(ctx,plane,location,path,false,true,rfn);
    Owned<IFile> file = createIFile(rfn);
    file->remove();
    StringBuffer s("DeleteExternalFile ('");
    s.append(location).append(',').append(path).append(") done");
    WUmessage(ctx,SeverityInformation,NULL,s.str());
    AuditMessage(ctx,"DeleteExternalFile",path);
}

FILESERVICES_API void FILESERVICES_CALL fsDeleteExternalFile(ICodeContext *ctx,const char *location,const char *path)
{
    implementDeleteExternalFile(ctx,location,path,nullptr);
}
FILESERVICES_API void FILESERVICES_CALL fsDeleteExternalFile_v2(ICodeContext *ctx,const char *location,const char *path,const char *planename)
{
    implementDeleteExternalFile(ctx,location,path,planename);
}

static void implementCreateExternalDirectory(ICodeContext *ctx,const char *location,const char *path,const char *planename)
{
    Owned<IPropertyTree> plane = checkPlaneOrHost(planename,location,path);
    RemoteFilename rfn;
    checkExternalFilePath(ctx,plane,location,path,false,true,rfn);
    Owned<IFile> file = createIFile(rfn);
    file->createDirectory();
    StringBuffer s("CreateExternalDirectory ('");
    s.append(location).append(',').append(path).append(") done");
    WUmessage(ctx,SeverityInformation,NULL,s.str());
    AuditMessage(ctx,"CreateExternalDirectory",path);
}

FILESERVICES_API void FILESERVICES_CALL fsCreateExternalDirectory(ICodeContext *ctx,const char *location,const char *path)
{
    implementCreateExternalDirectory(ctx,location,path,nullptr);
}

FILESERVICES_API void FILESERVICES_CALL fsCreateExternalDirectory_v2(ICodeContext *ctx,const char *location,const char *path,const char *planename)
{
    implementCreateExternalDirectory(ctx,location,path,planename);
}

static void implementListPlaneDirectory(ICodeContext *ctx,const char *planename,const char *machine,const char *dir,
    const char *mask,bool sub,size32_t &lenresult,void *&result)
{
    Owned<IPropertyTree> plane = checkPlaneOrHost(planename,machine,dir);
    RemoteFilename rfn;

    // NB: If ctx == null, implies called from a compiled query that predates fsRemoteDirectory_v2
    // checkExternalFilePath cannot be performed.
    if (ctx)
        checkExternalFilePath(ctx,plane,machine,dir,true,false,rfn);
    else
    {
        // for backward compatibility

        // NB: if no ctx, also implies no planename
        SocketEndpoint ep(machine);
        if (ep.isNull())
        {
            if (machine)
                throw makeStringExceptionV(-1, "RemoteDirectory: Could not resolve host '%s'", machine);
            // This should probably throw an error, but keeping this way for backward compatibility (with old compiled queries)
            ep.setLocalHost(0);
        }
        rfn.setPath(ep,dir);
    }
    listRemoteDirectoryFiles(rfn,mask,sub,lenresult,result);
}

FILESERVICES_API void FILESERVICES_CALL fsRemoteDirectory(size32_t &lenresult,
    void *&result,const char *machine,const char *dir,const char *mask,bool sub)
{
    implementListPlaneDirectory(nullptr,nullptr,machine,dir,mask,sub,lenresult,result);
}

FILESERVICES_API void FILESERVICES_CALL fsRemoteDirectory_v2(ICodeContext *ctx,size32_t &lenresult,
    void *&result,const char *machine,const char *dir,const char *mask,bool sub,const char *planename)
{
    implementListPlaneDirectory(ctx,planename,machine,dir,mask,sub,lenresult,result);
}

FILESERVICES_API char * FILESERVICES_CALL fsfGetLogicalFileAttribute(ICodeContext * ctx,const char *_lfn,const char *attrname)
{
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = wsdfs::lookup(lfn.str(), udesc, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser, INFINITE);
    StringBuffer ret;
    if (df) {
        if (strcmp(attrname,"ECL")==0)
            df->getECL(ret);
        else if (strcmp(attrname,"clusterName")==0)
            df->getClusterName(0,ret);
        else if (strcmp(attrname,"partmask")==0)
            ret.append(df->queryPartMask());
        else if (strcmp(attrname,"directory")==0)
            ret.append(df->queryDefaultDir());
        else if (strcmp(attrname,"numparts")==0)
            ret.append(df->numParts());
        else if (strcmp(attrname,"name")==0)
            ret.append(df->queryLogicalName());
        else if (strcmp(attrname,"modified")==0) {
            CDateTime dt;
            df->getModificationTime(dt);
            dt.getString(ret);
        }
        else if (strcmp(attrname,"protected")==0) {
            IPropertyTree &attr = df->queryAttributes();
            Owned<IPropertyTreeIterator> piter = attr.getElements("Protect");
            ForEach(*piter) {
                const char *name = piter->query().queryProp("@name");
                if (name&&*name) {
                    if (ret.length())
                        ret.append(',');
                    ret.append(name);
                }
            }
        }
        else {
            StringBuffer xpath("@");
            xpath.append(attrname);
            IPropertyTree &attr = df->queryAttributes();
            attr.getProp(xpath.str(),ret);
        }
    }
    else
        throw MakeStringException(0, "GetLogicalFileAttribute: Could not find logical file %s", lfn.str());
    return ret.detach();
}

FILESERVICES_API void FILESERVICES_CALL fsProtectLogicalFile(ICodeContext * ctx, const char *_lfn, bool set)
{
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(), udesc, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser);
    if (df)
    {
        StringBuffer uname;
        udesc->getUserName(uname);
        df->setProtect(uname, set);
        if (!set)
        {
            // for backward compatibility only (see HPCC-23190)
            uname.clear().append("user:");
            udesc->getUserName(uname);
            df->setProtect(uname, false);
        }
    }
    else if (set)
        throw MakeStringException(0, "ProtectLogicalFile: Could not find logical file %s", lfn.str());
}

static bool build_dfuplus_globals(int argc, const char *argv[], IProperties * globals)
{
    for (int i = 1; i < argc; i++)
        if (strchr(argv[i],'='))
            globals->loadProp(argv[i]);

    // if server is empty we can look up the settings for the default eclservices (eclwatch if eclservices is not found)
    if (isEmptyString(globals->queryProp("server")))
    {
        const char *url = fsGetEspURL(nullptr, nullptr);
        if (!isEmptyString(url))
        {
            if (strncmp("mtls:", url, 5)==0)
            {
                if (!globals->hasProp("mtls-secret"))
                    globals->setProp("mtls-secret", "local");
                url+=5;
            }
            globals->setProp("server", url);
        }
    }

    StringBuffer tmp;
    if (globals->hasProp("encrypt")) {
        encrypt(tmp.clear(),globals->queryProp("encrypt") );  // basic encryption at this stage
        globals->setProp("encrypt",tmp.str());
    }
    if (globals->hasProp("decrypt")) {
        encrypt(tmp.clear(),globals->queryProp("decrypt") );  // basic encryption at this stage
        globals->setProp("decrypt",tmp.str());
    }

    return true;
}

FILESERVICES_API void FILESERVICES_CALL fsDfuPlusExec(ICodeContext * ctx,const char *_cmd)
{
    if (!_cmd||!*_cmd)
        return;
    MemoryBuffer mb;
    const char **argv;
    StringBuffer cmdline;
    if (strcmp(_cmd,"dfuplus ")!=0)
        cmdline.append("dfuplus ");
    cmdline.append(_cmd);
    int argc = parseCommandLine(cmdline.str(),mb,argv);

    Owned<IProperties> globals = createProperties(true);

    if (!build_dfuplus_globals(argc, argv, globals))
        throw MakeStringException(-1,"DfuPlusExec: invalid command line");
    const char* server = globals->queryProp("server");
    if (!server || !*server)
        throw MakeStringException(-1,"DfuPlusExec: server url not specified");
    const char* action = globals->queryProp("action");
    if (!action || !*action)
        throw MakeStringException(-1,"DfuPlusExec: no action specified");
    if (ctx) {
        Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
        StringBuffer tmp;
        const char* username = globals->queryProp("username");
        if (!username || !*username)
            globals->setProp("username",udesc->getUserName(tmp.clear()).str());;
        const char* passwd = globals->queryProp("password");
        if (!passwd || !*passwd)
            globals->setProp("password",udesc->getPassword(tmp.clear()).str());;
    }
    class cMsg: implements CDfuPlusMessagerIntercept
    {
        ICodeContext * ctx;
        unsigned limit;
    public:
        cMsg(ICodeContext *_ctx)
        {
            limit = 0;
            ctx = _ctx;
        }

        void info(const char *msg)
        {
            if (ctx&&(++limit<100))
                WUmessage(ctx,SeverityInformation,NULL,msg);
        }
        void err(const char *msg)
        {
            throw MakeStringException(-1,"DfuPlusExec: %s",msg);
        }

    } cmsg(ctx);
    try {

        Owned<CDfuPlusHelper> helper = new CDfuPlusHelper(LINK(globals.get()));
        helper->msgintercept = &cmsg;
        helper->doit();
    }
    catch(IException* e) {
        EXCLOG(e,"fsDfuPlusExec");
        throw;
    }
}

FILESERVICES_API char * FILESERVICES_CALL fsGetEspURL(const char *username, const char *userPW)
{
#ifdef _CONTAINERIZED
    const IPropertyTree *match = nullptr;
    const char *espService = getComponentConfigSP()->queryProp("@defaultEsp");
    Owned<IPropertyTree> globalConfig = getGlobalConfig();
    if (isEmptyString(espService))
        espService = globalConfig->queryProp("@defaultEsp");
    if (!isEmptyString(espService))
    {
        VStringBuffer service("services[@name='%s']", espService);
        match = globalConfig->queryPropTree(service.str());
    }
    if (!match)
    {
        // Look for 'eclservices' esp service, fallback to 'eclwatch' service.
        Owned<IPropertyTreeIterator> iter = globalConfig->getElements("services");
        ForEach(*iter)
        {
            const char *type = iter->query().queryProp("@type");
            if (streq("eclservices", type))
            {
                match = &iter->query();
                break;
            }
            else if (streq("eclwatch", type))
                match = &iter->query();
        }
    }
    if (match) // MORE - if not found, we could generate a warning - it implies something misconfigured!
    {
        if (!espService)
            espService = match->queryProp("@name");
        StringBuffer credentials;
        if (username && username[0] && userPW && userPW[0])
            credentials.setf("%s:%s@", username, userPW);
        else if (username && username[0])
            credentials.setf("%s@", username);

        const char *protocol = match->getPropBool("@tls") ? "https" : "http";
        unsigned port = match->getPropInt("@port", 8010);

        VStringBuffer espURL("mtls:%s://%s%s:%u", protocol, credentials.str(), espService, port);
        return espURL.detach();
    }
#else
    Owned<IConstEnvironment> daliEnv = openDaliEnvironment();
    Owned<IPropertyTree> env = getEnvironmentTree(daliEnv);

    if (env.get())
    {
        StringBuffer espURL;
        StringBuffer espInstanceComputerName;
        StringBuffer bindingProtocol;
        StringBuffer xpath;
        StringBuffer instanceAddress;
        StringBuffer espServiceType;
        StringBuffer credentials;

        Owned<IPropertyTreeIterator> espProcessIter = env->getElements("Software/EspProcess");
        ForEach(*espProcessIter)
        {
            Owned<IPropertyTreeIterator> espBindingIter = espProcessIter->query().getElements("EspBinding");
            ForEach(*espBindingIter)
            {
                espBindingIter->query().getProp("@service",espURL.clear());
                xpath.setf("Software/EspService[@name=\"%s\"]/Properties/@type", espURL.str());

                if(env->getProp(xpath.str(), espServiceType.clear()))
                {
                    if (strieq(espServiceType.str(),"WsSMC"))
                    {
                        if (espBindingIter->query().getProp("@protocol",bindingProtocol.clear()))
                        {
                            Owned<IPropertyTreeIterator> espInstanceIter = espProcessIter->query().getElements("Instance");
                            ForEach(*espInstanceIter)
                            {
                                if (espInstanceIter->query().getProp("@computer",espInstanceComputerName.clear()))
                                {
                                    xpath.setf("Hardware/Computer[@name=\"%s\"]/@netAddress",espInstanceComputerName.str());
                                    if (env->getProp(xpath.str(),instanceAddress.clear()))
                                    {
                                        if (username && username[0] && userPW && userPW[0])
                                            credentials.setf("%s:%s@", username, userPW);
                                        else if (username && username[0])
                                            credentials.setf("%s@", username);

                                        if (streq(instanceAddress.str(),"."))
                                        {
                                            SocketEndpoint ep(instanceAddress.str());
                                            ep.getHostText(instanceAddress.clear());
                                        }

                                        espURL.setf("%s://%s%s:%d", bindingProtocol.str(), credentials.str(), instanceAddress.str(), espBindingIter->query().getPropInt("@port",8010));
                                        return espURL.detach();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
#endif
    return strdup("");
}

FILESERVICES_API char * FILESERVICES_CALL fsGetDefaultDropZone()
{
    StringBuffer dropZonePath;
    Owned<IPropertyTreeIterator> dropZones = getGlobalConfigSP()->getElements("storage/planes[@category='lz']");
    if (dropZones->first())
        dropZones->query().getProp("@prefix", dropZonePath);        // Why the directory? seems a very stange choice
    return strdup(dropZonePath.str());
}

FILESERVICES_API char * FILESERVICES_CALL fsGetDefaultDropZoneName()
{
    StringBuffer dropZoneName;
    Owned<IPropertyTreeIterator> dropZones = getGlobalConfigSP()->getElements("storage/planes[@category='lz']");
    if (dropZones->first())
        dropZones->query().getProp("@name", dropZoneName);
    return strdup(dropZoneName.str());
}

FILESERVICES_API void FILESERVICES_CALL fsGetDropZones(ICodeContext *ctx, size32_t & __lenResult, void * & __result)
{
    MemoryBuffer mb;
    Owned<IPropertyTreeIterator> dropZones = getGlobalConfigSP()->getElements("storage/planes[@category='lz']");
    ForEach(*dropZones)
    {
        const char * directory = dropZones->query().queryProp("@prefix");
        size32_t sz = strlen(directory);
        mb.append(sz).append(sz,directory);
    }
    __lenResult = mb.length();
    __result = mb.detach();
}


FILESERVICES_API void FILESERVICES_CALL fsGetLandingZones(ICodeContext *ctx, size32_t & __lenResult, void * & __result)
{
    MemoryBuffer mb;
    size32_t sz;
    Owned<IPropertyTree> global = getGlobalConfig();
    Owned<IPropertyTreeIterator> dropZones = global->getElements("storage/planes[@category='lz']");
    ForEach(*dropZones)
    {
        const IPropertyTree &dropZone = dropZones->query();
        const char * name = dropZone.queryProp("@name");
        const char * directory = dropZone.queryProp("@prefix");
        const char * hostGroup = dropZone.queryProp("@hostGroup");
        // field "name"
        sz = strlen(name);
        mb.append(sz).append(sz,name);

        // field "prefix"
        sz = strlen(directory);
        mb.append(sz).append(sz,directory);

        // field "host"
        const char * host = nullptr;
        if (hostGroup)
        {
            VStringBuffer xpath("storage/hostGroups[@name='%s']", name);
            IPropertyTree * match = global->queryPropTree(xpath);
            if (match)
                host = match->queryProp("hosts[1]");
        }
        sz = host ? strlen(host) : 0;
        mb.append(sz).append(sz, host);
    }
    __lenResult = mb.length();
    __result = mb.detach();
}

FILESERVICES_API int FILESERVICES_CALL fsGetExpireDays(ICodeContext * ctx, const char *_lfn)
{
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = wsdfs::lookup(lfn.str(), udesc, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser, INFINITE);
    if (df)
        return df->getExpire(nullptr);
    else
        throw makeStringExceptionV(0, "GetExpireDays: Could not find logical file %s", lfn.str());
}

FILESERVICES_API void FILESERVICES_CALL fsSetExpireDays(ICodeContext * ctx, const char *_lfn, int expireDays)
{
    if (expireDays < 0)
        throw makeStringExceptionV(0, "SetExpireDays: expireDays parameter value should be >= 0 (%d)", expireDays);

    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc, AccessMode::tbdRead,false, false, nullptr, defaultPrivilegedUser);
    if (df)
        df->setExpire(expireDays);
    else
        throw makeStringExceptionV(0, "SetExpireDays: Could not find logical file %s", lfn.str());
}

FILESERVICES_API void FILESERVICES_CALL fsClearExpireDays(ICodeContext * ctx, const char *_lfn)
{
    StringBuffer lfn;
    constructLogicalName(ctx, _lfn, lfn);
    Linked<IUserDescriptor> udesc = ctx->queryUserDescriptor();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn.str(),udesc, AccessMode::tbdRead,false, false, nullptr, defaultPrivilegedUser);
    if (df)
        df->setExpire(-1);
    else
        throw makeStringExceptionV(0, "ClearExpireDays: Could not find logical file %s", lfn.str());
}

static CriticalSection noCommonDefCrit;
static int noCommonDef = NotFound;
FILESERVICES_API bool FILESERVICES_CALL fsGetNoCommonDefault()
{
    // Returns the boolean value of 'noCommon' property from the Software/Globals if it is defined,
    // Otherwise it returns with true, the recent default value of 'noCommon'
#ifdef _CONTAINERIZED
    return getComponentConfigSP()->getPropBool("@noCommon", true);
#else
    CriticalBlock b(noCommonDefCrit);
    if (NotFound == noCommonDef)
    {
        // Default value if it is not defined in environment.xml
        noCommonDef = true;
        Owned<IConstEnvironment> daliEnv = openDaliEnvironment();
        Owned<IPropertyTree> env = getEnvironmentTree(daliEnv);

        if (env.get())
            noCommonDef = env->getPropBool("@noCommon", noCommonDef);
    }
#endif
    return (bool)noCommonDef;
}
