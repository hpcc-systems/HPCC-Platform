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

#include <stdio.h>
#include "jlog.hpp"
#include "jfile.hpp"
#include "jargv.hpp"
#include "jflz.hpp"
#include "build-config.h"
#include "httpclient.hpp"

#include "workunit.hpp"
#include "ws_workunits.hpp"
#include "eclcmd_common.hpp"
#include "eclcmd_core.hpp"
#include "eclcmd_sign.hpp"

size32_t getMaxRequestEntityLength(EclCmdCommon &cmd)
{
    if(cmd.optServer.isEmpty())
        throw MakeStringException(-1, "Server IP not specified");

    EclCmdURL url("?config_", cmd.optServer, cmd.optPort, cmd.optSSL);
    Owned<IHttpClientContext> httpCtx = getHttpClientContext();

    StringBuffer request; //empty
    StringBuffer response;
    StringBuffer status;
    Owned<IHttpClient> httpclient = httpCtx->createHttpClient(NULL, url);
    if (cmd.optUsername.length())
        httpclient->setUserID(cmd.optUsername);
    if (cmd.optPassword.length())
         httpclient->setPassword(cmd.optPassword);
     if (0 > httpclient->sendRequest("GET", NULL, request, response, status) || !response.length() || strncmp("200", status, 3))
         throw MakeStringException(-1, "Error checking ESP configuration: %s:%s %s", cmd.optServer.str(), cmd.optPort.str(), status.str());

    Owned<IPropertyTree> config = createPTreeFromXMLString(response);
    return config->getPropInt("Software[1]/EspProcess[1]/EspProtocol[@type='http_protocol'][1]/@maxRequestEntityLength");
}

void expandDefintionsAsDebugValues(const IArrayOf<IEspNamedValue> & definitions, IArrayOf<IEspNamedValue> & debugValues)
{
    ForEachItemIn(i, definitions)
    {
        IEspNamedValue &item = definitions.item(i);
        const char *name = item.getName();
        const char *value = item.getValue();

        StringBuffer passThroughName;
        passThroughName.append("eclcc-D").append(name).append("-").append(i);
        addNamedValue(passThroughName, value, debugValues);
    }

}

void checkFeatures(IClientWsWorkunits *client, bool &useCompression, int &major, int &minor, int &point, unsigned waitMs, unsigned waitConnectMs, unsigned waitReadSec)
{
    try
    {
        Owned<IClientWUCheckFeaturesRequest> req = client->createWUCheckFeaturesRequest();
        setCmdRequestTimeouts(req->rpc(), waitMs, waitConnectMs, waitReadSec);
        Owned<IClientWUCheckFeaturesResponse> resp = client->WUCheckFeatures(req);
        useCompression = resp->getDeployment().getUseCompression();
        major = resp->getBuildVersionMajor();
        minor = resp->getBuildVersionMinor();
        point = resp->getBuildVersionPoint();
    }
    catch(IException *E) //most likely an older ESP
    {
        E->Release();
    }
    catch(...)
    {
    }
}

bool doDeploy(EclCmdWithEclTarget &cmd, IClientWsWorkunits *client, unsigned waitMs, const char *cluster, const char *name, StringBuffer *wuid, StringBuffer *wucluster, bool noarchive, bool displayWuid=true, bool compress=true)
{
    int major = 0;
    int minor = 0;
    int point = 0;
    bool useCompression = false;
    checkFeatures(client, useCompression, major, minor, point, 0, cmd.optWaitConnectMs, cmd.optWaitReadSec);

    bool compressed = false;
    if (useCompression)
    {
        MemoryBuffer mb;
        fastLZCompressToBuffer(mb, cmd.optObj.mb.length(), cmd.optObj.mb.bufferBase());
        cmd.optObj.mb.swapWith(mb);
        compressed=true;
    }

    StringBuffer s;
    if (cmd.optVerbose)
        fprintf(stdout, "\nDeploying %s\n", cmd.optObj.getDescription(s).str());

    StringBuffer objType(compressed ? "compressed_" : ""); //change compressed type string so old ESPs will fail gracefully
    Owned<IClientWUDeployWorkunitRequest> req = client->createWUDeployWorkunitRequest();
    setCmdRequestTimeouts(req->rpc(), waitMs, cmd.optWaitConnectMs, cmd.optWaitReadSec);

    switch (cmd.optObj.type)
    {
        case eclObjArchive:
            req->setObjType(objType.append("archive"));
            break;
        case eclObjSharedObject:
            req->setObjType(objType.append("shared_object"));
            break;
        case eclObjSource:
        {
            if (noarchive)
                req->setObjType(objType.append("ecl_text"));
            else
            {
                fprintf(stderr, "Failed to create archive from ECL Text\n");
                return false;
            }
            break;
        }
        default:
            fprintf(stderr, "Cannot deploy %s\n", cmd.optObj.queryTypeName());
            return false;
    }

    if (name && *name)
        req->setName(name);
    if (cluster && *cluster)
        req->setCluster(cluster);
    req->setObject(cmd.optObj.mb);
    req->setFileName(cmd.optObj.value.str());
    if ((int)cmd.optResultLimit > 0)
        req->setResultLimit(cmd.optResultLimit);
    if (cmd.optAttributePath.length())
        req->setQueryMainDefinition(cmd.optAttributePath);
    if (cmd.optSnapshot.length())
        req->setSnapshot(cmd.optSnapshot);
    expandDefintionsAsDebugValues(cmd.definitions, cmd.debugValues);
    if (cmd.debugValues.length())
    {
        req->setDebugValues(cmd.debugValues);
        cmd.debugValues.kill();
    }

    Owned<IClientWUDeployWorkunitResponse> resp;
    try
    {
         resp.setown(client->WUDeployWorkunit(req));
    }
    catch (IException *E)
    {
        StringBuffer msg;
        int code = E->errorCode();
        //ESP doesn't want to process requests that are too large, and so disconnects before reading
        //  this causes issues capturing the error returned... check if that may have been the issue
        if (code==SOAP_AUTHENTICATION_ERROR || (code==SOAP_SERVER_ERROR && streq(E->errorMessage(msg).str(), "401: Unauthorized Access")))
            throw;
        if (useCompression) //newer build, not a maxRequestEntityLength issue
            throw;
        size32_t maxEntity = getMaxRequestEntityLength(cmd); //only do the work to grab max buffersize if we've failed
        if (maxEntity > 1000)
        {
            size32_t maxBufferSize = ((maxEntity - 999) / 4) * 3; //account for soap, other parameters, and base64 encoding (n / 4 * 3)
            if (maxBufferSize && cmd.optObj.mb.length() > maxBufferSize)
            {
                fprintf(stderr, "\nError: %s is larger than maxRequestEntityLength configured for ESP allows.\n", objType.str());
                E->Release();
                return false;
            }
        }
        throw;
    }
    outputMultiExceptionsEx(resp->getExceptions()); //ignore error returned, no wuid is error

    const char *w = resp->getWorkunit().getWuid();
    if (w && *w)
    {
        if (wuid)
            wuid->clear().append(w);
        if (wucluster)
            wucluster->clear().append(resp->getWorkunit().getCluster());
        fprintf(stdout, "\n");
        if (cmd.optVerbose)
            fprintf(stdout, "Deployed\n   wuid: ");
        const char *state = resp->getWorkunit().getState();
        bool isCompiled = (strieq(state, "compiled")||strieq(state, "completed"));
        if (displayWuid || cmd.optVerbose || !isCompiled)
            fprintf(stdout, "%s\n", w);
        if (cmd.optVerbose || !isCompiled)
            fprintf(stdout, "   state: %s\n\n", state);

        unsigned errorCount=0;
        unsigned warningCount=0;
        IArrayOf<IConstECLException> &exceptions = resp->getWorkunit().getExceptions();
        ForEachItemIn(i, exceptions)
        {
            IConstECLException &e = exceptions.item(i);
            if (e.getSource())
                fprintf(stderr, "%s: ", e.getSource());
            if (e.getFileName())
                fputs(e.getFileName(), stderr);
            if (!e.getLineNo_isNull() && !e.getColumn_isNull())
                fprintf(stderr, "(%d,%d): ", e.getLineNo(), e.getColumn());

            fprintf(stderr, "%s C%d: %s\n", e.getSeverity(), e.getCode(), e.getMessage());
            if (strieq(e.getSeverity(), "warning"))
                warningCount++;
            else if (strieq(e.getSeverity(), "error"))
                errorCount++;
        }
        if (errorCount || warningCount)
            fprintf(stderr, "%d error(s), %d warning(s)\n\n", errorCount, warningCount);

        return isCompiled;
    }
    return false;
}


class EclCmdDeploy : public EclCmdWithEclTarget
{
public:
    EclCmdDeploy()
    {
        optObj.accept = eclObjWuid | eclObjArchive | eclObjSharedObject;
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdWithEclTarget::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;

        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdWithEclTarget::finalizeOptions(globals))
            return false;
        if (optObj.type==eclObjWuid)
        {
            StringBuffer s;
            fprintf(stderr, "\nWUID (%s) cannot be the target for deployment\n", optObj.getDescription(s).str());
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClientExt(WsWorkunits, *this, "?upload_"); //upload_ disables maxRequestEntityLength
        return doDeploy(*this, client, 0, optTargetCluster.get(), optName.get(), NULL, NULL, optNoArchive) ? 0 : 1;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'deploy' command creates a workunit on the HPCC system from the given ECL\n"
            "text, file, archive, shared object, or dll.  The workunit will be created in\n"
            "the 'compiled' state.\n"
            "\n"
            "ecl deploy <target> <file> [--name=<val>]\n"
            "ecl deploy <target> <archive> [--name=<val>]\n"
            "ecl deploy <target> <so|dll> [--name=<val>]\n"
            "ecl deploy <target> - [--name=<val>]\n\n"
            "   -                      specifies object should be read from stdin\n"
            "   <file>                 ecl text file to deploy\n"
            "   <archive>              ecl archive to deploy\n"
            "   <so|dll>               workunit dll or shared object to deploy\n"
            " Options:\n"
            "   -n, --name=<val>       workunit job name\n",
            stdout);
        EclCmdWithEclTarget::usage();
    }
private:
    StringAttr optName;
};

class EclCmdPublish : public EclCmdWithEclTarget
{
public:
    EclCmdPublish() : optNoActivate(false), optSuspendPrevious(false), optDeletePrevious(false),
        activateSet(false), optNoReload(false), optDontCopyFiles(false), optMsToWait(300000), optAllowForeign(false), optUpdateDfs(false),
        optUpdateSuperfiles(false), optUpdateCloneFrom(false), optDontAppendCluster(false)
    {
        optObj.accept = eclObjWuid | eclObjArchive | eclObjSharedObject;
        optTimeLimit = (unsigned) -1;
        optWarnTimeLimit = (unsigned) -1;
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            if (iter.matchOption(optObj.value, ECLOPT_WUID)||iter.matchOption(optObj.value, ECLOPT_WUID_S))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
                continue;
            if (iter.matchOption(optDaliIP, ECLOPT_DALIIP))
                continue;
            if (iter.matchOption(optSourceProcess, ECLOPT_SOURCE_PROCESS))
                continue;
            if (iter.matchOption(optMsToWait, ECLOPT_WAIT))
                continue;
            if (iter.matchOption(optTimeLimit, ECLOPT_TIME_LIMIT))
                continue;
            if (iter.matchOption(optWarnTimeLimit, ECLOPT_WARN_TIME_LIMIT))
                continue;
            if (iter.matchOption(optMemoryLimit, ECLOPT_MEMORY_LIMIT))
                continue;
            if (iter.matchOption(optPriority, ECLOPT_PRIORITY))
                continue;
            if (iter.matchOption(optComment, ECLOPT_COMMENT))
                continue;
            if (iter.matchFlag(optDontCopyFiles, ECLOPT_DONT_COPY_FILES))
                continue;
            if (iter.matchFlag(optAllowForeign, ECLOPT_ALLOW_FOREIGN))
                continue;
            if (iter.matchFlag(optNoActivate, ECLOPT_NO_ACTIVATE))
            {
                activateSet=true;
                continue;
            }
            if (iter.matchFlag(optNoReload, ECLOPT_NORELOAD))
                continue;
            bool activate; //also supports "-A-"
            if (iter.matchFlag(activate, ECLOPT_ACTIVATE)||iter.matchFlag(activate, ECLOPT_ACTIVATE_S))
            {
                activateSet=true;
                optNoActivate=!activate;
                continue;
            }
            if (iter.matchFlag(optSuspendPrevious, ECLOPT_SUSPEND_PREVIOUS)||iter.matchFlag(optSuspendPrevious, ECLOPT_SUSPEND_PREVIOUS_S))
                continue;
            if (iter.matchFlag(optDeletePrevious, ECLOPT_DELETE_PREVIOUS)||iter.matchFlag(optDeletePrevious, ECLOPT_DELETE_PREVIOUS_S))
                continue;
            if (iter.matchFlag(optUpdateDfs, ECLOPT_UPDATE_DFS))
                continue;
            if (iter.matchFlag(optUpdateSuperfiles, ECLOPT_UPDATE_SUPER_FILES))
                continue;
            if (iter.matchFlag(optUpdateCloneFrom, ECLOPT_UPDATE_CLONE_FROM))
                continue;
            if (iter.matchFlag(optDontAppendCluster, ECLOPT_DONT_APPEND_CLUSTER))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdWithEclTarget::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdWithEclTarget::finalizeOptions(globals))
            return false;
        if (!activateSet)
        {
            bool activate;
            if (extractEclCmdOption(activate, globals, ECLOPT_ACTIVATE_ENV, ECLOPT_ACTIVATE_INI, true))
                optNoActivate=!activate;
        }
        if (optNoActivate && (optSuspendPrevious || optDeletePrevious))
        {
            fputs("invalid --suspend-prev and --delete-prev require activation.\n", stderr);
            return false;
        }
        if (!optSuspendPrevious && !optDeletePrevious)
        {
            extractEclCmdOption(optDeletePrevious, globals, ECLOPT_DELETE_PREVIOUS_ENV, ECLOPT_DELETE_PREVIOUS_INI, false);
            if (!optDeletePrevious)
                extractEclCmdOption(optSuspendPrevious, globals, ECLOPT_SUSPEND_PREVIOUS_ENV, ECLOPT_SUSPEND_PREVIOUS_INI, false);
        }
        if (optSuspendPrevious && optDeletePrevious)
        {
            fputs("invalid --suspend-prev and --delete-prev are mutually exclusive options.\n", stderr);
            return false;
        }
        if (optMemoryLimit.length() && !isValidMemoryValue(optMemoryLimit))
        {
            fprintf(stderr, "invalid --memoryLimit value of %s.\n", optMemoryLimit.get());
            return false;
        }
        if (optPriority.length() && !isValidPriorityValue(optPriority))
        {
            fprintf(stderr, "invalid --priority value of %s.\n", optPriority.get());
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        CTimeMon mon(optMsToWait);
        Owned<IClientWsWorkunits> client = createCmdClientExt(WsWorkunits, *this, "?upload_"); //upload_ disables maxRequestEntityLength
        StringBuffer wuid;
        if (optObj.type==eclObjWuid)
            wuid.set(optObj.value.get());
        else if (!doDeploy(*this, client, optMsToWait, optTargetCluster.get(), optName.get(), &wuid, NULL, optNoArchive))
            return 1;

        unsigned remaining = 0;
        if (mon.timedout(&remaining))
        {
            fputs("\nTimed out during deployment, query not published\n", stderr);
            return 1;
        }

        StringBuffer descr;
        if (optVerbose)
            fprintf(stdout, "\nPublishing %s\n", wuid.str());

        unsigned clientRemaining = remaining + 100; //give the ESP method time to return from timeout before hard client stop
        Owned<IClientWUPublishWorkunitRequest> req = client->createWUPublishWorkunitRequest();
        setCmdRequestTimeouts(req->rpc(), clientRemaining, optWaitConnectMs, optWaitReadSec);
        req->setWuid(wuid.str());
        if (optDeletePrevious)
            req->setActivate(CWUQueryActivationMode_ActivateDeletePrevious);
        else if (optSuspendPrevious)
            req->setActivate(CWUQueryActivationMode_ActivateSuspendPrevious);
        else
            req->setActivate(optNoActivate ? CWUQueryActivationMode_NoActivate : CWUQueryActivationMode_Activate);

        if (optName.length())
            req->setJobName(optName.get());
        if (optTargetCluster.length())
            req->setCluster(optTargetCluster.get());
        req->setRemoteDali(optDaliIP.get());
        req->setSourceProcess(optSourceProcess);
        req->setWait(remaining);
        req->setNoReload(optNoReload);
        req->setDontCopyFiles(optDontCopyFiles);
        req->setAllowForeignFiles(optAllowForeign);
        req->setUpdateDfs(optUpdateDfs);
        req->setUpdateSuperFiles(optUpdateSuperfiles);
        req->setUpdateCloneFrom(optUpdateCloneFrom);
        req->setAppendCluster(!optDontAppendCluster);
        req->setIncludeFileErrors(true);

        if (optTimeLimit != (unsigned) -1)
            req->setTimeLimit(optTimeLimit);
        if (optWarnTimeLimit != (unsigned) -1)
            req->setWarnTimeLimit(optWarnTimeLimit);
        if (!optMemoryLimit.isEmpty())
            req->setMemoryLimit(optMemoryLimit);
        if (!optPriority.isEmpty())
            req->setPriority(optPriority);
        if (optComment.get()) //allow empty
            req->setComment(optComment);

        Owned<IClientWUPublishWorkunitResponse> resp = client->WUPublishWorkunit(req);
        const char *id = resp->getQueryId();
        if (id && *id)
        {
            const char *qs = resp->getQuerySet();
            fprintf(stdout, "\n%s/%s\n", qs ? qs : "", resp->getQueryId());
        }
        if (resp->getReloadFailed())
            fputs("\nAdded to Queryset, but request to reload queries on cluster failed\n", stderr);

        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (outputQueryFileCopyErrors(resp->getFileErrors()))
            ret = 1;

        return ret;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'publish' command creates a query in a queryset.  The query is created\n"
            "by adding a workunit to a queryset and assigning it a query name.\n"
            "\n"
            "If the query is being created from an ECL file, archive, shared object, dll,\n"
            "or text, a workunit is first created and then published to the queryset.\n"
            "\n"
            "ecl publish <target> <file> --name=<val>\n"
            "ecl publish <target> <wuid> --name=<val>\n"
            "ecl publish <target> <so|dll> --name=<val>\n"
            "ecl publish <target> <archive> --name=<val>\n"
            "ecl publish <target> - --name=<val>\n\n"
            "   -                      specifies object should be read from stdin\n"
            "   <wuid>                 workunit to publish\n"
            "   <archive>              archive to publish\n"
            "   <file>                 ECL text file to publish\n"
            "   <so|dll>               workunit dll or shared object to publish\n"
            " Options:\n"
            "   -n, --name=<val>       query name to use for published workunit\n"
            "   -A, --activate         Activate query when published (default)\n"
            "   -sp, --suspend-prev    Suspend previously active query\n"
            "   -dp, --delete-prev     Delete previously active query\n"
            "   -A-, --no-activate     Do not activate query when published\n"
            "   --no-reload            Do not request a reload of the (roxie) cluster\n"
            "   --no-files             Do not copy DFS file information for referenced files\n"
            "   --allow-foreign        Do not fail if foreign files are used in query (roxie)\n"
            "   --daliip=<IP>          The IP of the DALI to be used to locate remote files\n"
            "   -O, --overwrite        Completely replace existing DFS file information (dangerous)\n"
            "   --update-super-files   Update local DFS super-files if remote DALI has changed\n"
            "   --update-clone-from    Update local clone from location if remote DALI has changed\n"
            "   --dont-append-cluster  Only use to avoid locking issues due to adding cluster to file\n"
            "   --source-process       Process cluster to copy files from\n"
            "   --timeLimit=<ms>       Value to set for query timeLimit configuration\n"
            "   --warnTimeLimit=<ms>   Value to set for query warnTimeLimit configuration\n"
            "   --memoryLimit=<mem>    Value to set for query memoryLimit configuration\n"
            "                          format <mem> as 500000B, 550K, 100M, 10G, 1T etc.\n"
            "   --priority=<val>       set the priority for this query. Value can be LOW,\n"
            "                          HIGH, SLA, NONE. NONE will clear current setting.\n"
            "   --comment=<string>     Set the comment associated with this query\n"
            "   --wait=<ms>            Max time to wait in milliseconds\n",
            stdout);
        EclCmdWithEclTarget::usage();
    }
private:
    StringAttr optName;
    StringAttr optDaliIP;
    StringAttr optSourceProcess;
    StringAttr optMemoryLimit;
    StringAttr optPriority;
    StringAttr optComment;
    unsigned optMsToWait;
    unsigned optTimeLimit;
    unsigned optWarnTimeLimit;
    bool optNoActivate;
    bool activateSet;
    bool optNoReload;
    bool optDontCopyFiles;
    bool optSuspendPrevious;
    bool optDeletePrevious;
    bool optAllowForeign;
    bool optUpdateDfs;
    bool optUpdateSuperfiles;
    bool optUpdateCloneFrom;
    bool optDontAppendCluster; //Undesirable but here temporarily because DALI may have locking issues
};

inline unsigned nextWait(unsigned wait, unsigned waited)
{
    if (waited < 5000)
        return 1000;
    if (waited < 30000)
        return 5000;
    if (waited < 60000)
        return 10000;
    if (waited < 120000)
        return 30000;
    return 60000;
}

class EclCmdRun : public EclCmdWithEclTarget
{
public:
    EclCmdRun() : optWaitTime((unsigned)-1), optNoRoot(false), optExceptionSeverity("info")
    {
        optObj.accept = eclObjWuid | eclObjArchive | eclObjSharedObject | eclObjWuid | eclObjQuery;
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            if (matchVariableOption(iter, 'X', variables, false))
                continue;
            if (iter.matchOption(optObj.value, ECLOPT_WUID)||iter.matchOption(optObj.value, ECLOPT_WUID_S))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
                continue;
            if (iter.matchOption(optInput, ECLOPT_INPUT)||iter.matchOption(optInput, ECLOPT_INPUT_S))
                continue;
            if (iter.matchOption(optWaitTime, ECLOPT_WAIT))
                continue;
            if (iter.matchFlag(optNoRoot, ECLOPT_NOROOT))
                continue;
            if (iter.matchFlag(optPoll, ECLOPT_POLL))
                continue;
            if (iter.matchFlag(optPre64, "--pre64")) //only for troubleshooting, do not document
                continue;
            if (iter.matchOption(optExceptionSeverity, ECLOPT_EXCEPTION_LEVEL))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdWithEclTarget::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdWithEclTarget::finalizeOptions(globals))
            return false;
        if (optInput.length())
        {
            const char *in = optInput.get();
            while (*in && isspace(*in)) in++;
            if (*in!='<')
            {
                StringBuffer content;
                content.loadFile(in);
                optInput.set(content.str());
            }
        }
        return true;
    }

    inline bool isSoapRpcException(IException *E)
    {
        StringBuffer msg;
        const char *finger = E->errorMessage(msg).str();
        if (strncmp(finger, "SOAP ", 5))
            return false;
        finger+=5;
        if (!strncmp(finger, "rpc error", 9))
            return true;
        if (!strncmp(finger, "Connection error", 16))
            return true;
        return false;
    }
    int checkComplete(IClientWsWorkunits* client, IClientWUWaitRequest* req)
    {
        try
        {
            Owned<IClientWUWaitResponse> resp = client->WUWaitComplete(req);
            if (resp->getExceptions().ordinality())
                throw LINK(&resp->getExceptions());
            return resp->getStateID();
        }
        catch (IJSOCK_Exception *E)
        {
            outputExceptionEx(*E);
            E->Release();
        }
        catch (IException *E)
        {
            if (!isSoapRpcException(E))
                throw E;
            outputExceptionEx(*E);
            E->Release();
        }

        fputs("Still polling...\n", stderr);
        return WUStateUnknown; //socket issue, keep polling
    }

    int checkComplete(IClientWsWorkunits* client, IClientWUInfoRequest* req)
    {
        try
        {
            Owned<IClientWUInfoResponse> resp = client->WUInfo(req);
            if (resp->getExceptions().ordinality())
                throw LINK(&resp->getExceptions());
            int state = resp->getWorkunit().getStateID();
            switch (state)
            {
            case WUStateCompleted:
            case WUStateFailed:
            case WUStateAborted:
                return state;
            }
        }
        catch (IJSOCK_Exception *E)
        {
            outputExceptionEx(*E);
            E->Release();
            fputs("Still polling...\n", stderr);
        }
        catch (IException *E)
        {
            if (!isSoapRpcException(E))
                throw E;
            outputExceptionEx(*E);
            E->Release();
            fputs("Still polling...\n", stderr);
        }

        return WUStateUnknown; //emulate result from waitForWorkUnit which will be called for non-legacy builds
    }

    void initPollRequest(Owned<IClientWUInfoRequest> &req, IClientWsWorkunits* client, const char *wuid)
    {
        req.setown(client->createWUInfoRequest());
        req->setWuid(wuid);
        req->setTruncateEclTo64k(true);
        req->setIncludeExceptions(false);
        req->setIncludeGraphs(false);
        req->setIncludeSourceFiles(false);
        req->setIncludeResults(false);
        req->setIncludeResultsViewNames(false);
        req->setIncludeVariables(false);
        req->setIncludeTimers(false);
        req->setIncludeDebugValues(false);
        req->setIncludeApplicationValues(false);
        req->setIncludeWorkflows(false);
        req->setIncludeXmlSchemas(false);
        req->setIncludeResourceURLs(false);
        req->setSuppressResultSchemas(true);
    }

    void initPollRequest(Owned<IClientWUWaitRequest> &req, IClientWsWorkunits* client, const char *wuid)
    {
        req.setown(client->createWUWaitCompleteRequest());
        req->setWuid(wuid);
        req->setWait(0); //Just return the current state
    }

    int pollForCompletion(IClientWsWorkunits* client, const char *wuid, bool optimized)
    {
        Owned<IClientWUInfoRequest> reqInfo;
        Owned<IClientWUWaitRequest> reqQuick;

        if (optimized)
            initPollRequest(reqQuick, client, wuid);
        else
            initPollRequest(reqInfo, client, wuid);

        if (optVerbose)
            fputs("Polling for completion...\n", stdout);
        int state = WUStateUnknown;
        for(;;)
        {
            if (optimized)
                state = checkComplete(client, reqQuick);
            else
                state = checkComplete(client, reqInfo);
            if (state != WUStateUnknown)
                break;
            unsigned waited = msTick() - startTimeMs;
            if (optWaitTime!=(unsigned)-1 && waited>=optWaitTime)
                return WUStateUnknown;
            Sleep(nextWait(optWaitTime, waited));
        }
        return state;
    }

    void gatherLegacyServerResults(IClientWsWorkunits* client, const char *wuid)
    {
        if (optVerbose)
            fputs("Getting Workunit Information\n", stdout);

        Owned<IClientWUInfoRequest> req = client->createWUInfoRequest();
        setCmdRequestTimeouts(req->rpc(), optWaitTime, optWaitConnectMs, optWaitReadSec);

        req->setWuid(wuid);
        req->setIncludeExceptions(true);
        req->setIncludeGraphs(false);
        req->setIncludeSourceFiles(false);
        req->setIncludeResults(false); //ECL layout results, not xml
        req->setIncludeResultsViewNames(false);
        req->setIncludeVariables(false);
        req->setIncludeTimers(false);
        req->setIncludeDebugValues(false);
        req->setIncludeApplicationValues(false);
        req->setIncludeWorkflows(false);
        req->setIncludeXmlSchemas(false);
        req->setIncludeResourceURLs(false);
        req->setSuppressResultSchemas(true);
        Owned<IClientWUInfoResponse> resp = client->WUInfo(req);

        IConstECLWorkunit &wu = resp->getWorkunit();
        IArrayOf<IConstECLException> &exceptions = wu.getExceptions();
        unsigned count = wu.getResultCount();
        if (count<=0 && exceptions.ordinality()<=0)
            return;

        if (optVerbose)
            fputs("Getting Results\n", stdout);

        fputs("<Result>\n", stdout);
        ForEachItemIn(pos, exceptions)
        {
            IConstECLException &e = exceptions.item(pos);
            fprintf(stdout, " <Exception><Code>%d</Code><Source>%s</Source><Message>%s</Message></Exception>\n", e.getCode(), e.getSource(), e.getMessage());
        }
        Owned<IClientWUResultRequest> resReq = client->createWUResultRequest();
        resReq->setWuid(wuid);
        resReq->setSuppressXmlSchema(true);
        for (unsigned seq=0; seq<count; seq++)
        {
            resReq->setSequence(seq);
            Owned<IClientWUResultResponse> resp = client->WUResult(resReq);

            if (resp->getExceptions().ordinality())
                throw LINK(&resp->getExceptions());
            fwrite(resp->getResult(), 8, 1, stdout);
            //insert name attribute into <Dataset> tag
            fprintf(stdout, " name='%s'", resp->getName());
            fputs(resp->getResult()+8, stdout);
        }
        fputs("</Result>\n", stdout);
    }

    void getAndOutputResults(IClientWsWorkunits* client, const char *wuid)
    {
        if (optVerbose)
            fputs("Retrieving Results\n", stdout);
        Owned<IClientWUFullResultRequest> req = client->createWUFullResultRequest();
        setCmdRequestTimeouts(req->rpc(), optWaitTime, optWaitConnectMs, optWaitReadSec);

        req->setWuid(wuid);
        req->setNoRootTag(optNoRoot);
        req->setExceptionSeverity(optExceptionSeverity);

        Owned<IClientWUFullResultResponse> resp = client->WUFullResult(req);
        if (resp->getResults())
            fprintf(stdout, "%s\n", resp->getResults());
    }

    void processResults(IClientWsWorkunits* client, const char *wuid, bool optimized)
    {
        if (optimized)
            getAndOutputResults(client, wuid);
        else
            gatherLegacyServerResults(client, wuid);
    }

    int pollForResults(IClientWsWorkunits* client, const char *wuid)
    {
        int major = 0;
        int minor = 0;
        int point = 0;
        bool useCompression = false;
        checkFeatures(client, useCompression, major, minor, point, optWaitTime, optWaitConnectMs, optWaitReadSec);
        bool optimized = !optPre64 && (major>=6 && minor>=3);

        try
        {
            int state = pollForCompletion(client, wuid, optimized);
            switch (state)
            {
            case WUStateCompleted:
                processResults(client, wuid, optimized);
                return 0;
            case WUStateUnknown:
                fprintf(stderr, "Timed out waiting for %s to complete, workunit is still running.\n", wuid);
                break;
            case WUStateFailed:
            case WUStateAborted:
                fprintf(stderr, "%s %s.\n", wuid, getWorkunitStateStr((WUState)state));
                break;
            default:
                fprintf(stderr, "%s in unrecognized state.\n", wuid);
                break;
            }
        }
        catch (IMultiException *ME)
        {
            outputMultiExceptionsEx(*ME);
            ME->Release();
        }
        catch (IException *E)
        {
            StringBuffer msg;
            fprintf(stderr, "Exception polling for results: %d: %s\n", E->errorCode(), E->errorMessage(msg).str());
            E->Release();
        }
        return 1;
    }
    int getInitialRunWait()
    {
        if (!optPoll)
            return optWaitTime;
        return (optWaitTime < 10000) ? optWaitTime : 10000; //stay connected for the first 10 seconds even if polling
    }
    bool isFinalState(WUState state)
    {
        switch (state)
        {
        case WUStateUnknown:
        case WUStateArchived:
        case WUStateAborting:
        case WUStateCompleted:
        case WUStateFailed:
        case WUStateAborted:
            return true;
        }
        return false;
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClientExt(WsWorkunits, *this, "?upload_"); //upload_ disables maxRequestEntityLength
        Owned<IClientWURunRequest> req = client->createWURunRequest();
        setCmdRequestTimeouts(req->rpc(), optWaitTime, optWaitConnectMs, optWaitReadSec);
        req->setCloneWorkunit(true);
        req->setNoRootTag(optNoRoot);

        // Add a debug value to indicate that Roxie variable filenames are ok
        // We put it at front of list so that it can be overridden explicitly by user.

        Owned<IEspNamedValue> nv = createNamedValue();
        nv->setName("allowVariableRoxieFileNames");
        nv->setValue("1");
        debugValues.add(*nv.getClear(), 0);

        StringBuffer wuid;
        StringBuffer wuCluster;
        StringBuffer query;

        if (optObj.type==eclObjWuid)
        {
            req->setWuid(wuid.set(optObj.value.get()).str());
            if (optVerbose)
                fprintf(stdout, "Running workunit %s\n", wuid.str());
        }
        else if (optObj.type==eclObjQuery)
        {
            req->setQuerySet(optTargetCluster);
            req->setQuery(query.set(optObj.query.get()).str());
            if (optVerbose)
                fprintf(stdout, "Running %s/%s\n", optTargetCluster.str(), query.str());
        }
        else
        {
            req->setCloneWorkunit(false);
            if (!doDeploy(*this, client, optWaitTime, optTargetCluster.get(), optName.get(), &wuid, &wuCluster, optNoArchive, optVerbose))
                return 1;
            req->setWuid(wuid.str());
            if (optVerbose)
                fprintf(stdout, "Running deployed workunit %s\n", wuid.str());
        }

        if (wuCluster.length())
            req->setCluster(wuCluster.str());
        else if (optTargetCluster.length())
            req->setCluster(optTargetCluster.get());

        req->setWait(getInitialRunWait());
        if (optInput.length())
            req->setInput(optInput.get());
        req->setExceptionSeverity(optExceptionSeverity); //throws exception if invalid value

        expandDefintionsAsDebugValues(definitions, debugValues);
        if (debugValues.length())
            req->setDebugValues(debugValues);
        if (variables.length())
            req->setVariables(variables);

        startTimeMs = msTick();
        Owned<IClientWURunResponse> resp;
        try
        {
            resp.setown(client->WURun(req));
        }
        catch (IJSOCK_Exception *E)
        {
            if (optPoll)
                fputs("Socket error, no WUID, can't continue but workunit may still be running...\n", stderr);
            throw E;
        }
        catch (IException *E)
        {
            if (optPoll && isSoapRpcException(E))
                fputs("SOAP error, no WUID, can't continue but workunit may still be running...\n", stderr);
            throw E;
        }

        if (checkMultiExceptionsQueryNotFound(resp->getExceptions()))
        {
            //checking if it was a query was last resort.  may not have been after all, use generic language
            fprintf(stderr, "\n%s not found\n", optObj.query.str());
            return 1;
        }

        int ret = outputMultiExceptionsEx(resp->getExceptions());

        StringBuffer respwuid(resp->getWuid());
        if (!respwuid.length()) //everything below requires workunit
            return ret;
        if (optVerbose && respwuid.length() && !streq(wuid.str(), respwuid.str()))
            fprintf(stdout, "As %s\n", respwuid.str());
        WUState state = getWorkUnitState(resp->getState());
        if (optPoll && !isFinalState(state))
            return pollForResults(client, respwuid);

        switch (state)
        {
        case WUStateCompleted:
            break;
        case WUStateCompiled:
        case WUStateSubmitted:
        case WUStateCompiling:
        case WUStateUploadingFiles:
        case WUStateScheduled:
        case WUStateWait:
        case WUStateDebugPaused:
        case WUStateDebugRunning:
        case WUStatePaused:
        case WUStateBlocked:
        case WUStateRunning:
            fprintf(stderr, "%s %s\n", respwuid.str(), resp->getState());
            fprintf(stderr, "Timed out waiting for %s to complete, workunit may still be running.\n", wuid.str()); //server side waiting timed out
            ret = 2;
            break;
        case WUStateUnknown:
        case WUStateFailed:
        case WUStateArchived:
        case WUStateAborting:
        case WUStateAborted:
        default:
            fprintf(stderr, "%s %s\n", respwuid.str(), resp->getState());
            ret = 4;
        }

        if (resp->getResults())
            fprintf(stdout, "%s\n", resp->getResults());

        return ret;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'run' command executes an ECL workunit, text, file, archive, query,\n"
            "shared object, or dll on the specified HPCC target cluster.\n"
            "\n"
            "Query input can be provided in xml form via the --input parameter.  Input\n"
            "xml can be provided directly or by referencing a file\n"
            "\n"
            "ecl run <target> <file> --name=<val> [--input=<file|xml>][--wait=<i>]\n"
            "ecl run <target> <wuid> [--input=<file|xml>][--wait=<ms>]\n"
            "ecl run <target> <query> [--input=<file|xml>][--wait=<ms>]\n"
            "ecl run <target> <so|dll> [--name=<val>][--input=<file|xml>][--wait=<ms>]\n"
            "ecl run <target> <archive> --name=<val> [--input=<file|xml>][--wait=<ms>]\n"
            "ecl run <target> - --name=<val> [--input=<file|xml>][--wait=<ms>]\n\n"
            "   -                      specifies object should be read from stdin\n"
            "   <wuid>                 workunit to publish\n"
            "   <archive>              archive to publish\n"
            "   <eclfile>              ECL text file to publish\n"
            "   <so|dll>               workunit dll or shared object to publish\n"
            " Options:\n"
            "   -n, --name=<val>          job name\n"
            "   -in,--input=<file|xml>    file or xml content to use as query input\n"
            "   -X<name>=<value>          sets the stored input value (stored('name'))\n"
            "   --wait=<ms>               time to wait for completion\n"
            "   --poll                    poll for results, rather than remain connected\n"
            "   --exception-level=<level> minimum severity level for exceptions\n"
            "                             values: 'info', 'warning', 'error'\n",
            stdout);
        EclCmdWithEclTarget::usage();
    }
private:
    StringAttr optName;
    StringAttr optInput;
    StringAttr optExceptionSeverity;
    IArrayOf<IEspNamedValue> variables;
    unsigned optWaitTime = 0;
    unsigned startTimeMs = 0;
    bool optNoRoot = false;
    bool optPoll = false;
    bool optPre64 = false;  //only for troubleshooting, do not document
};

class EclCmdResults : public EclCmdCommon
{
public:
    EclCmdResults() : optExceptionSeverity("info")
    {
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                if (optWuid.length())
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionCompletion;
                }
                if (!looksLikeAWuid(arg, 'W'))
                {
                    fprintf(stderr, "\nargument should be a workunit id: %s\n", arg);
                    return EclCmdOptionCompletion;
                }
                optWuid.set(arg);
                continue;
            }

            if (iter.matchFlag(optNoRoot, ECLOPT_NOROOT))
                continue;
            if (iter.matchOption(optExceptionSeverity, ECLOPT_EXCEPTION_LEVEL))
                continue;
            if (iter.matchFlag(optPre64, "--pre64")) //only for troubleshooting, do not document
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optWuid.isEmpty())
        {
            fprintf(stderr, "No WUID provided.\n");
            return 0;
        }

        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }

    int gatherLegacyServerResults(IClientWsWorkunits* client, const char *wuid)
    {
        Owned<IClientWUInfoRequest> req = client->createWUInfoRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        req->setWuid(wuid);
        req->setIncludeExceptions(true);
        req->setIncludeGraphs(false);
        req->setIncludeSourceFiles(false);
        req->setIncludeResults(false); //ECL layout results, not xml
        req->setIncludeResultsViewNames(false);
        req->setIncludeVariables(false);
        req->setIncludeTimers(false);
        req->setIncludeDebugValues(false);
        req->setIncludeApplicationValues(false);
        req->setIncludeWorkflows(false);
        req->setIncludeXmlSchemas(false);
        req->setIncludeResourceURLs(false);
        req->setSuppressResultSchemas(true);
        Owned<IClientWUInfoResponse> resp = client->WUInfo(req);
        int ret = outputMultiExceptionsEx(resp->getExceptions());

        IConstECLWorkunit &wu = resp->getWorkunit();
        IArrayOf<IConstECLException> &exceptions = wu.getExceptions();
        fputs("<Result>\n", stdout);
        ForEachItemIn(pos, exceptions)
        {
            IConstECLException &e = exceptions.item(pos);
            fprintf(stdout, " <Exception><Code>%d</Code><Source>%s</Source><Message>%s</Message></Exception>\n", e.getCode(), e.getSource(), e.getMessage());
        }
        Owned<IClientWUResultRequest> resReq = client->createWUResultRequest();
        resReq->setWuid(wuid);
        resReq->setSuppressXmlSchema(true);
        unsigned count = wu.getResultCount();
        for (unsigned seq=0; seq<count; seq++)
        {
            resReq->setSequence(seq);
            Owned<IClientWUResultResponse> resp = client->WUResult(resReq);

            if (resp->getExceptions().ordinality())
                throw LINK(&resp->getExceptions());
            fwrite(resp->getResult(), 8, 1, stdout);
            //insert name attribute into <Dataset> tag
            fprintf(stdout, " name='%s'", resp->getName());
            fputs(resp->getResult()+8, stdout);
        }
        fputs("</Result>\n", stdout);
        return ret;
    }

    int getAndOutputResults(IClientWsWorkunits* client, const char *wuid)
    {
        Owned<IClientWUFullResultRequest> req = client->createWUFullResultRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        req->setWuid(wuid);
        req->setNoRootTag(optNoRoot);
        req->setExceptionSeverity(optExceptionSeverity);

        Owned<IClientWUFullResultResponse> resp = client->WUFullResult(req);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (resp->getResults())
            fprintf(stdout, "%s\n", resp->getResults());
        return ret;
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this); //upload_ disables maxRequestEntityLength

        int major = 0;
        int minor = 0;
        int point = 0;
        bool useCompression = false;
        checkFeatures(client, useCompression, major, minor, point, 0, optWaitConnectMs, optWaitReadSec);

        if (!optPre64 && (major>=6 && minor>=3))
            return getAndOutputResults(client, optWuid);

        return gatherLegacyServerResults(client, optWuid);
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'results' command displays the full results of a workunit in xml form.\n"
            "\n"
            "ecl results <wuid>\n"
            "ecl results <wuid> --noroot\n"
            "   <wuid>         workunit to get results from\n"
            " Options:\n"
            "   --exception-level=<level> minimum severity level for exceptions\n"
            "                             values: 'info', 'warning', 'error'\n"
            "   --noroot               output result xml without root tag\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optWuid;
    StringAttr optExceptionSeverity;
    bool optNoRoot = false;
    bool optPre64 = false; //only for troubleshooting gathering results from old servers, do not document
};


void outputQueryActionResults(const IArrayOf<IConstQuerySetQueryActionResult> &results, const char *act, const char *qs)
{
    ForEachItemIn(i, results)
    {
        IConstQuerySetQueryActionResult &item = results.item(i);
        const char *id = item.getQueryId();
        if (item.getSuccess())
            fprintf(stdout, "\n%s %s/%s\n", act, qs, id ? id : "");
        else if (item.getCode()|| item.getMessage())
        {
            const char *msg = item.getMessage();
            fprintf(stderr, "Query %s Error (%d) %s\n", id ? id : "", item.getCode(), msg ? msg : "");
        }
    }
}

class EclCmdActivate : public EclCmdWithQueryTarget
{
public:
    EclCmdActivate()
    {
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQuerySetQueryActionRequest> req = client->createWUQuerysetQueryActionRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        IArrayOf<IEspQuerySetQueryActionItem> queries;
        Owned<IEspQuerySetQueryActionItem> item = createQuerySetQueryActionItem();
        item->setQueryId(optQuery.get());
        queries.append(*item.getClear());
        req->setQueries(queries);

        req->setAction("Activate");
        req->setQuerySetName(optQuerySet.get());

        Owned<IClientWUQuerySetQueryActionResponse> resp = client->WUQuerysetQueryAction(req);
        IArrayOf<IConstQuerySetQueryActionResult> &results = resp->getResults();
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret != 0)
            return ret;

        if (results.empty())
            fprintf(stderr, "\nError Empty Result!\n");
        else
            outputQueryActionResults(results, "Activated", optQuerySet.str());
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'activate' command assigns a query to the active alias with the same\n"
            "name as the query.\n"
            "\n"
            "ecl activate <target> <query_id>\n"
            " Options:\n"
            "   <target>               name of target queryset containing query to activate\n"
            "   <query_id>             query to activate\n",
            stdout);
        EclCmdWithQueryTarget::usage();
    }
};

class EclCmdUnPublish : public EclCmdWithQueryTarget
{
public:
    EclCmdUnPublish()
    {
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQuerySetQueryActionRequest> req = client->createWUQuerysetQueryActionRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        req->setQuerySetName(optQuerySet.get());
        req->setAction("Delete");

        IArrayOf<IEspQuerySetQueryActionItem> queries;
        Owned<IEspQuerySetQueryActionItem> item = createQuerySetQueryActionItem();
        item->setQueryId(optQuery.get());
        queries.append(*item.getClear());
        req->setQueries(queries);

        Owned<IClientWUQuerySetQueryActionResponse> resp = client->WUQuerysetQueryAction(req);
        IArrayOf<IConstQuerySetQueryActionResult> &results = resp->getResults();
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret != 0)
            return ret;

        if (results.empty())
            fprintf(stderr, "\nError Empty Result!\n");
        else
            outputQueryActionResults(results, "Unpublished", optQuerySet.str());
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'unpublish' command removes a query from a target queryset.\n"
            "\n"
            "ecl unpublish <target> <query_id>\n"
            " Options:\n"
            "   <target>               name of target queryset containing the query to remove\n"
            "   <query_id>             query to remove from the queryset\n",
            stdout);
        EclCmdWithQueryTarget::usage();
    }
};

class EclCmdDeactivate : public EclCmdWithQueryTarget
{
public:
    EclCmdDeactivate()
    {
    }

    virtual int processCMD()
    {
        StringBuffer s;
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQuerySetAliasActionRequest> req = client->createWUQuerysetAliasActionRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        IArrayOf<IEspQuerySetAliasActionItem> aliases;
        Owned<IEspQuerySetAliasActionItem> item = createQuerySetAliasActionItem();
        item->setName(optQuery.get());
        aliases.append(*item.getClear());
        req->setAliases(aliases);

        req->setAction("Deactivate");
        req->setQuerySetName(optQuerySet.get());

        Owned<IClientWUQuerySetAliasActionResponse> resp = client->WUQuerysetAliasAction(req);
        IArrayOf<IConstQuerySetAliasActionResult> &results = resp->getResults();
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret != 0)
            return ret;

        if (results.empty())
            fprintf(stderr, "\nError Empty Result!\n");
        else
        {
            IConstQuerySetAliasActionResult &item = results.item(0);
            if (item.getSuccess())
                fprintf(stdout, "Deactivated alias %s/%s\n", optQuerySet.str(), optQuery.str());
            else if (item.getCode()|| item.getMessage())
                fprintf(stderr, "Error (%d) %s\n", item.getCode(), item.getMessage());
        }
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'deactivate' command removes an active query alias from the given target.\n"
            "\n"
            "ecl deactivate <target> <active_alias>\n"
            "\n"
            " Options:\n"
            "   <target>               target queryset containing alias to deactivate\n"
            "   <active_alias>         active alias to be removed from the queryset\n",
            stdout);
        EclCmdWithQueryTarget::usage();
    }
};

class EclCmdAbort : public EclCmdCommon
{
public:
    EclCmdAbort()
    {
        optObj.accept = eclObjWuid;
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        eclCmdOptionMatchIndicator retVal = EclCmdOptionNoMatch;
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (iter.matchOption(optName, ECLOPT_WUID)||iter.matchOption(optName, ECLOPT_WUID_S))
            {
                optObj.type = eclObjWuid;
                retVal = EclCmdOptionMatch;
                continue;
            }
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
            {
                optObj.type = eclObjQuery;
                retVal = EclCmdOptionMatch;
                continue;
            }
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return retVal;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }
    virtual int processCMD()
    {
        StringArray wuids;
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQueryRequest> reqQ = client->createWUQueryRequest();

        if (optObj.type == eclObjQuery)
        {
            reqQ->setJobname(optName.get());

            Owned<IClientWUQueryResponse> respQ = client->WUQuery(reqQ);
            int res = respQ->queryClientStatus();

            if (!respQ->getCount_isNull())
            {
                IArrayOf<IConstECLWorkunit>& wus = respQ->getWorkunits();

                ForEachItemIn(idx, wus)
                {
                    wuids.append(wus.item(idx).getWuid());
                }
            }
        }
        else
        {
            wuids.append(optName.get());
        }

        if (wuids.empty())
            return 0;

        // Abort
        Owned<IClientWUAbortRequest> req = client->createWUAbortRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);


        req->setWuids(wuids);
        Owned<IClientWUAbortResponse> resp = client->WUAbort(req);

        int ret = outputMultiExceptionsEx(resp->getExceptions());

        // Get status of WU(s)
        if (optObj.type == eclObjQuery)
        {
            reqQ->setJobname(optName.get());
        }
        else
        {
            reqQ->setWuid(optName.get());
        }
        Owned<IClientWUQueryResponse> respQ = client->WUQuery(reqQ);

        if (!respQ->getCount_isNull())
        {
            IArrayOf<IConstECLWorkunit>& wus = respQ->getWorkunits();

            if (!wus.empty())
            {
                if (wus.ordinality() > 1)
                {
                    ForEachItemIn(idx, wus)
                    {
                        fprintf(stdout, "%s,%s\n", wus.item(idx).getWuid(), getWorkunitStateStr((WUState) wus.item(idx).getStateID()) );
                    }
                }
                else
                {
                    fprintf(stdout, "%s\n", getWorkunitStateStr((WUState) wus.item(0).getStateID()) );
                }
            }
        }

        return ret;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'abort' command aborts one or more workunit on the HPCC system from the given WUID or job name\n"
            "The workunit(s) abort requests and the current status returns\n"
            "\n"
            "ecl abort -wu <WUID>| -n <job name>\n"
            "   WUID                   workunit ID\n"
            "   job name               workunit job name\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr         optName;
    EclObjectParameter optObj;
};

class EclCmdGetName : public EclCmdCommon
{
public:
    EclCmdGetName() : optListLimit(100)
    {
        optObj.accept = eclObjWuid;
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        eclCmdOptionMatchIndicator retVal = EclCmdOptionNoMatch;
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (iter.matchOption(optName, ECLOPT_WUID)||iter.matchOption(optName, ECLOPT_WUID_S))
            {
                optObj.type = eclObjWuid;
                retVal = EclCmdOptionMatch;
                continue;
            }
            if (iter.matchOption(optListLimit, ECLOPT_RESULT_LIMIT))
            {
                continue;
            }
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return retVal;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQueryRequest> req = client->createWUQueryRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);


        if (optName.isEmpty())
            return 0;

        req->setWuid(optName.get());

        if (optListLimit)
            req->setCount(optListLimit);

        Owned<IClientWUQueryResponse> resp = client->WUQuery(req);

        if (!resp->getCount_isNull())
        {
            IArrayOf<IConstECLWorkunit>& wus = resp->getWorkunits();
            ForEachItemIn(idx, wus)
            {
                fprintf(stdout, "%s\n", wus.item(idx).getJobname());
            }
        }

        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'getname' command returns with the workunit name from the given workunit id.\n"
            "\n"
            "ecl getname --wuid <WUID>\n"
            "\n"
            "   WUID                   workunit ID\n"
            " Options:\n"
            "   --limit=<limit>        Sets the result limit for the query, defaults to 100\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr         optName;
    EclObjectParameter optObj;
    unsigned int       optListLimit;
};

class EclCmdGetWuid : public EclCmdCommon
{
public:
    EclCmdGetWuid() : optListLimit(100)
    {

    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        eclCmdOptionMatchIndicator retVal = EclCmdOptionNoMatch;
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
            {
                retVal = EclCmdOptionMatch;
                continue;
            }
            if (iter.matchOption(optListLimit, ECLOPT_RESULT_LIMIT))
            {
                continue;
            }
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return retVal;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQueryRequest> req = client->createWUQueryRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        if (optName.isEmpty())
            return 0;

        req->setJobname(optName.get());

        Owned<IClientWUQueryResponse> resp = client->WUQuery(req);

        if (!resp->getCount_isNull())
        {
            IArrayOf<IConstECLWorkunit>& wus = resp->getWorkunits();

            ForEachItemIn(idx, wus)
            {
                if (idx == optListLimit)
                    break;

                fprintf(stdout, "%s\n", wus.item(idx).getWuid());
            }
        }

        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'getwuid' command returns with WUID(s) of the given workunit job name.\n"
            "\n"
            "ecl getwuid -n <job name> [--limit=<limit>]\n"
            "\n"
            "   job name               workunit job name\n"
            " Options:\n"
            "   --limit=<limit>        Sets the result limit for the query, defaults to 100\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr         optName;
    EclObjectParameter optObj;
    unsigned int       optListLimit;
};

class EclCmdStatus : public EclCmdCommon
{
public:
    EclCmdStatus() : optListLimit(100)
    {
        optObj.accept = eclObjWuid;
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        eclCmdOptionMatchIndicator retVal = EclCmdOptionNoMatch;
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (iter.matchOption(optName, ECLOPT_WUID)||iter.matchOption(optName, ECLOPT_WUID_S))
            {
                optObj.type = eclObjWuid;
                retVal = EclCmdOptionMatch;
                continue;
            }
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
            {
                optObj.type = eclObjQuery;
                retVal = EclCmdOptionMatch;
                continue;
            }
            if (iter.matchOption(optListLimit, ECLOPT_RESULT_LIMIT))
            {
                continue;
            }
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return retVal;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQueryRequest> req = client->createWUQueryRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        if (optName.isEmpty())
        {
            fprintf(stdout, "No WUID or job name.\n");
            return 0;
        }


        if ( optObj.type ==eclObjWuid )
            req->setWuid(optName.get());
        else
            req->setJobname(optName.get());

        Owned<IClientWUQueryResponse> resp = client->WUQuery(req);
        int res = resp->queryClientStatus();

        IArrayOf<IConstECLWorkunit>& wus = resp->getWorkunits();

        if (wus.ordinality() == 1)
        {

            if (optVerbose)
            {
                fprintf(stdout, "ID: %-18s, job name: %s, state:", wus.item(0).getWuid(), wus.item(0).getJobname());
            }

            fprintf(stdout, "%s\n", getWorkunitStateStr((WUState) wus.item(0).getStateID()) );
        }
        else
        {
            ForEachItemIn(idx, wus)
            {
                if (idx == optListLimit)
                    break;

                if (optVerbose)
                    fprintf(stdout, "ID: %s, job name: %s, state: %s\n", wus.item(idx).getWuid(), wus.item(idx).getJobname(), getWorkunitStateStr((WUState) wus.item(idx).getStateID()) );
                else
                    fprintf(stdout, "%s,%s,%s\n", wus.item(idx).getWuid(), wus.item(idx).getJobname(), getWorkunitStateStr((WUState) wus.item(idx).getStateID()) );
            }
        }

        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'status' command returns the status of the given workunit or job name.\n"
            "If there are more than one result it generates a CSV list with wuid, name and state.\n"
            "\n"
            "ecl status -wu <WUID>|-n <job name> \n"
            "\n"
            "   WUID                   workunit ID\n"
            "   name                   workunit job name\n"
            " Options:\n"
            "   --limit=<limit>        Sets the result limit for the query, defaults to 100\n"
            "   --verbose              Add field names\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr         optName;
    EclObjectParameter optObj;
    unsigned int       optListLimit;
};

class EclCmdZapGen : public EclCmdCommon
{
public:
    EclCmdZapGen()
    {

    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        eclCmdOptionMatchIndicator retVal = EclCmdOptionNoMatch;
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg != '-') //parameters don't start with '-'
            {
                if (optWuid.length())
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionCompletion;
                }
                if (!looksLikeAWuid(arg, 'W'))
                {
                    fprintf(stderr, "\nargument should be a workunit id: %s\n", arg);
                    return EclCmdOptionCompletion;
                }
                optWuid.set(arg);
                continue;
            }
            if (iter.matchOption(optPath, ECLOPT_PATH))
            {
                if ((optPath.length() > 0) && (*optPath.str() != '-'))
                {
                    retVal = EclCmdOptionMatch;
                    continue;
                }
                else
                {
                    fprintf(stderr, "\nPath should not be empty.\n");
                    return EclCmdOptionCompletion;
                }
            }
            if (iter.matchFlag(optIncThorSlave, ECLOPT_INC_THOR_SLAVE_LOGS))
            {
                retVal = EclCmdOptionMatch;
                continue;
            }
            if (iter.matchFlag(optCreateDirs,ECLOPT_CREATE_DIRS))
            {
                retVal = EclCmdOptionMatch;
                continue;
            }
            if (iter.matchOption(optProblemDesc, ECLOPT_PROBLEM_DESC))
            {
                if ((optProblemDesc.length() > 0) && (*optProblemDesc.str() != '-'))
                {
                    retVal = EclCmdOptionMatch;
                    continue;
                }
                else
                {
                    fprintf(stderr, "\nDescription should not be empty.\n");
                    return EclCmdOptionCompletion;
                }
            }
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return retVal;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if(optServer.isEmpty())
        {
            fprintf(stderr, "\nError: Server IP not specified\n");
            return false;
        }
        if(optWuid.isEmpty())
        {
            fprintf(stderr, "\nError: WUID not specified\n");
            return false;
        }

        return true;
    }
    virtual int processCMD()
    {
        //Create the file name for the output
        StringBuffer outputFile;
        if (!optPath.isEmpty())
        {
            outputFile.set(optPath.get());
            const char *p = outputFile.str();
            p = (p + strlen(p) - 1);
            if (!streq(p, PATHSEPSTR))
                outputFile.append(PATHSEPSTR);
        }
        else
            outputFile.set(".").append(PATHSEPSTR);

        outputFile.append("ZAPReport_").append(optWuid.get());
        if (!optUsername.isEmpty())
            outputFile.append('_').append(optUsername.get());
        outputFile.append(".zip");

        //Create command URL
        StringBuffer urlTail("/WUCreateAndDownloadZAPInfo?Wuid=");
        urlTail.append(optWuid.get());
        if (optIncThorSlave)
            urlTail.append("&IncludeThorSlaveLog=on");
        if (!optProblemDesc.isEmpty())
            urlTail.append("&ProblemDescription=").append(optProblemDesc.get());
        EclCmdURL eclCmdURL("WsWorkunits", !streq(optServer, ".") ? optServer : "localhost", optPort, optSSL, urlTail.str());

        //Create CURL command
        StringBuffer curlCommand("curl -v -X post");
        if (!optUsername.isEmpty())
        {
            curlCommand.append(" -u ").append(optUsername.get());
            if (!optPassword.isEmpty())
                curlCommand.append(":").append(optPassword.get());
        }
        if (optCreateDirs)
            curlCommand.append(" --create-dirs");

        curlCommand.appendf(" -o %s %s", outputFile.str(), eclCmdURL.str());

        Owned<IPipeProcess> pipe = createPipeProcess();
        if (!pipe->run(optVerbose ? "EXEC" : NULL, curlCommand.str(), NULL, false, true, true))
        {
            fprintf(stderr, "Failed to run zapgen command %s\n", curlCommand.str());
            return false;
        }

        bool isOk = true;
        bool removeOutFile = false;

        if (pipe->hasError())
        {
            StringBuffer errMsg;
            const unsigned errBuffSize = 64;
            char err[errBuffSize];
            size32_t len = 0;
            while ((len = pipe->readError(errBuffSize, err)) > 0)
                errMsg.append(err, 0, len);

            if ((strstr(errMsg.str(), "Warning") != nullptr) || (strstr(errMsg.str(), "* Failed to connect to") != nullptr))
            {
                fprintf(stderr, "Zapgen command returns with error:\n%s\n", errMsg.str());
                isOk = false;
                removeOutFile = true; // It contains the authentication error message only
            }
            else if (strstr(errMsg.str(), "Authentication problem") != nullptr)
            {
                fprintf(stderr, "\nZapgen command returned with an authentication problem. Please check your credentials.\n");
                isOk = false;
                removeOutFile = true; // It contains the authentication error message only
            }
            else if (strstr(errMsg.str(), "401 Unauthorized") != nullptr)
            {
                fprintf(stderr, "Zapgen command needs to authenticate, please provide credentials.\n");
                isOk = false;
                removeOutFile = true; // It contains the authentication error message only
            }

            if (optVerbose)
                fprintf(stderr, "Zapgen command returned with:\n%s\n", errMsg.str());

        }

        if (isOk)
        {
            fprintf(stdout, "ZAP file written to %s.\n", outputFile.str());
            return 0;
        }
        else
        {
            Owned<IFile> file = createIFile(outputFile.str());
            if (file->exists() && (removeOutFile || (0 == file->size())))
                file->remove();
            return 1;
        }
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "Create and store ZAP file of the given workunit.\n"
            "\n"
            "ecl zapgen <WUID> --path <zap_file_path>\n"
            "\n"
            "   WUID                    workunit ID\n"
            "   path                    path to store ZAP file\n"
            " Options:\n"
            "   --inc-thor-slave-logs   include Thor slave(s) log into the ZAP file\n"
            "   --description <text>    problem description string\n"
            "   --create-dirs           create necesary directory tree\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr         optWuid;
    StringAttr         optPath;
    bool               optIncThorSlave = false;
    bool               optCreateDirs = false;
    StringAttr         optProblemDesc;
};


//=========================================================================================

IEclCommand *createCoreEclCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    if (strieq(cmdname, "deploy"))
        return new EclCmdDeploy();
    if (strieq(cmdname, "publish"))
        return new EclCmdPublish();
    if (strieq(cmdname, "unpublish"))
        return new EclCmdUnPublish();
    if (strieq(cmdname, "run"))
        return new EclCmdRun();
    if (strieq(cmdname, "results"))
        return new EclCmdResults();
    if (strieq(cmdname, "activate"))
        return new EclCmdActivate();
    if (strieq(cmdname, "deactivate"))
        return new EclCmdDeactivate();
    if (strieq(cmdname, "abort"))
        return new EclCmdAbort();
    if (strieq(cmdname, "getname"))
        return new EclCmdGetName();
    if (strieq(cmdname, "getwuid"))
        return new EclCmdGetWuid();
    if (strieq(cmdname, "status"))
        return new EclCmdStatus();
    if (strieq(cmdname, "zapgen"))
        return new EclCmdZapGen();
    if (strieq(cmdname, "sign"))
        return createSignEclCommand();
    if (strieq(cmdname, "listkeyuid"))
        return createListKeyUidCommand();
    return NULL;
}
