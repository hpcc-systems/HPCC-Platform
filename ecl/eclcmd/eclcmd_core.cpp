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

#include <stdio.h>
#include "jlog.hpp"
#include "jfile.hpp"
#include "jargv.hpp"
#include "build-config.h"

#include "workunit.hpp"
#include "ws_workunits.hpp"
#include "eclcmd_common.hpp"
#include "eclcmd_core.hpp"

void outputMultiExceptions(const IMultiException &me)
{
    fprintf(stderr, "\nException(s):\n");
    aindex_t count = me.ordinality();
    for (aindex_t i=0; i<count; i++)
    {
        IException& e = me.item(i);
        StringBuffer msg;
        fprintf(stderr, "%d: %s\n", e.errorCode(), e.errorMessage(msg).str());
    }
    fprintf(stderr, "\n");
}

bool doDeploy(EclCmdWithEclTarget &cmd, IClientWsWorkunits *client, const char *cluster, const char *name, StringBuffer *wuid, bool noarchive, bool displayWuid=true)
{
    StringBuffer s;
    if (cmd.optVerbose)
        fprintf(stdout, "\nDeploying %s\n", cmd.optObj.getDescription(s).str());

    Owned<IClientWUDeployWorkunitRequest> req = client->createWUDeployWorkunitRequest();
    switch (cmd.optObj.type)
    {
        case eclObjArchive:
            req->setObjType("archive");
            break;
        case eclObjSharedObject:
            req->setObjType("shared_object");
            break;
        case eclObjSource:
        {
            if (noarchive)
                req->setObjType("ecl_text");
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
    req->setFileName(cmd.optObj.value.sget());

    Owned<IClientWUDeployWorkunitResponse> resp = client->WUDeployWorkunit(req);
    if (resp->getExceptions().ordinality())
        outputMultiExceptions(resp->getExceptions());
    const char *w = resp->getWorkunit().getWuid();
    if (w && *w)
    {
        if (wuid)
            wuid->append(w);
        fprintf(stdout, "\n");
        if (cmd.optVerbose)
            fprintf(stdout, "Deployed\n   wuid: ");
        if (displayWuid || cmd.optVerbose)
            fprintf(stdout, "%s\n", w);
        const char *state = resp->getWorkunit().getState();
        if (cmd.optVerbose)
            fprintf(stdout, "   state: %s\n", state);
        return streq(resp->getWorkunit().getState(), state);
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
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER)||iter.matchOption(optCluster, ECLOPT_CLUSTER_S))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
                continue;
            if (EclCmdWithEclTarget::matchCommandLineOption(iter, true)!=EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdWithEclTarget::finalizeOptions(globals))
            return false;
        if (optObj.value.isEmpty())
        {
            fprintf(stderr, "\nNo ECL Source, Archive, or DLL specified for deployment\n");
            return false;
        }
        if (optObj.type==eclObjTypeUnknown)
        {
            fprintf(stderr, "\nCan't determine content type of argument %s\n", optObj.value.sget());
            return false;
        }
        if (optObj.type==eclObjWuid)
        {
            StringBuffer s;
            fprintf(stderr, "\nWUID (%s) cannot be the target for deployment\n", optObj.getDescription(s).str());
            return false;
        }
        if ((optObj.type==eclObjSource || optObj.type==eclObjArchive) && optCluster.isEmpty())
        {
            fprintf(stderr, "\nCluster must be specified when deploying from ECL Source or Archive\n");
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
        VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);
        return doDeploy(*this, client, optCluster.get(), optName.get(), NULL, optNoArchive) ? 0 : 1;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl deploy --cluster=<val> --name=<val> <ecl_file|->\n"
            "ecl deploy --cluster=<val> --name=<val> <archive|->\n"
            "ecl deploy [--cluster=<val>] [--name=<val>] <so|dll|->\n\n"
            "   -                      specifies object should be read from stdin\n"
            "   <ecl_file|->           ecl text file to deploy\n"
            "   <archive|->            ecl archive to deploy\n"
            "   <so|dll|->             workunit dll or shared object to deploy\n"
            " Options:\n"
            "   -cl, --cluster=<val>   cluster to associate workunit with\n"
            "   -n, --name=<val>       workunit job name\n"
        );
        EclCmdWithEclTarget::usage();
    }
private:
    StringAttr optCluster;
    StringAttr optName;
};

class EclCmdPublish : public EclCmdWithEclTarget
{
public:
    EclCmdPublish() : optActivate(false), activateSet(false), optMsToWait(10000)
    {
        optObj.accept = eclObjWuid | eclObjArchive | eclObjSharedObject;
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (; !iter.done(); iter.next())
        {
            if (iter.matchOption(optObj.value, ECLOPT_WUID)||iter.matchOption(optObj.value, ECLOPT_WUID_S))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
                continue;
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER)||iter.matchOption(optCluster, ECLOPT_CLUSTER_S))
                continue;
            if (iter.matchOption(optCluster, ECLOPT_WAIT))
                continue;
            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE)||iter.matchFlag(optActivate, ECLOPT_ACTIVATE_S))
            {
                activateSet=true;
                continue;
            }
            if (EclCmdWithEclTarget::matchCommandLineOption(iter, true)!=EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdWithEclTarget::finalizeOptions(globals))
            return false;
        if (!activateSet)
            extractEclCmdOption(optActivate, globals, ECLOPT_ACTIVATE_ENV, ECLOPT_ACTIVATE_INI, false);
        if (optObj.value.isEmpty())
        {
            fprintf(stderr, "\nMust specify a WUID, ECL File, Archive, or shared object to publish\n");
            return false;
        }
        if (optObj.type==eclObjTypeUnknown)
        {
            fprintf(stderr, "\nCan't determine content type of argument %s\n", optObj.value.sget());
            return false;
        }
        if (optObj.type==eclObjArchive || optObj.type==eclObjSource)
        {
            if (optCluster.isEmpty())
            {
                fprintf(stderr, "\nCluster must be specified when publishing ECL Text or Archive\n");
                return false;
            }
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
        VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);

        StringBuffer wuid;
        if (optObj.type==eclObjWuid)
            wuid.set(optObj.value.get());
        else if (!doDeploy(*this, client, optCluster.get(), optName.get(), &wuid, optNoArchive))
            return 1;

        StringBuffer descr;
        if (optVerbose)
            fprintf(stdout, "\nPublishing %s\n", wuid.str());

        Owned<IClientWUPublishWorkunitRequest> req = client->createWUPublishWorkunitRequest();
        req->setWuid(wuid.str());
        req->setActivate(optActivate);
        if (optName.length())
            req->setJobName(optName.get());
        if (optCluster.length())
            req->setCluster(optCluster.get());
        req->setWait(optMsToWait);

        Owned<IClientWUPublishWorkunitResponse> resp = client->WUPublishWorkunit(req);
        const char *id = resp->getQueryId();
        if (id && *id)
        {
            const char *qs = resp->getQuerySet();
            fprintf(stdout, "\n%s/%s\n", qs ? qs : "", resp->getQueryId());
        }
        if (resp->getReloadFailed())
            fputs("\nAdded to Queryset, but request to reload queries on cluster failed\n", stderr);

        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl publish [--cluster=<val>] [--name=<val>] [--activate] <wuid>\n"
            "ecl publish [--cluster=<val>] [--name=<val>] [--activate] <so|dll|->\n"
            "ecl publish --cluster=<val> --name=<val> [--activate] <archive|->\n"
            "ecl publish --cluster=<val> --name=<val> [--activate] <ecl_file|->\n\n"
            "   -                      specifies object should be read from stdin\n"
            "   <wuid>                 workunit to publish\n"
            "   <archive|->            archive to publish\n"
            "   <ecl_file|->           ECL text file to publish\n"
            "   <so|dll|->             workunit dll or shared object to publish\n"
            " Options:\n"
            "   -cl, --cluster=<val>   cluster to publish workunit to\n"
            "                          (defaults to cluster defined inside workunit)\n"
            "   -n, --name=<val>       query name to use for published workunit\n"
            "   -A, --activate         activates query when published\n"
            "   --wait=<ms>            maximum time to wait for cluster finish updating\n"
        );
        EclCmdWithEclTarget::usage();
    }
private:
    StringAttr optCluster;
    StringAttr optName;
    int optMsToWait;
    bool optActivate;
    bool activateSet;
};

class EclCmdRun : public EclCmdWithEclTarget
{
public:
    EclCmdRun() : optWaitTime((unsigned)-1)
    {
        optObj.accept = eclObjWuid | eclObjArchive | eclObjSharedObject | eclObjWuid | eclObjQuery;
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (; !iter.done(); iter.next())
        {
            if (iter.matchOption(optObj.value, ECLOPT_WUID)||iter.matchOption(optObj.value, ECLOPT_WUID_S))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
                continue;
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER)||iter.matchOption(optCluster, ECLOPT_CLUSTER_S))
                continue;
            if (iter.matchOption(optInput, ECLOPT_INPUT)||iter.matchOption(optInput, ECLOPT_INPUT_S))
                continue;
            if (iter.matchOption(optWaitTime, ECLOPT_WAIT))
                continue;
            if (EclCmdWithEclTarget::matchCommandLineOption(iter, true)!=EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdWithEclTarget::finalizeOptions(globals))
            return false;
        if (optObj.value.isEmpty())
        {
            fprintf(stderr, "\nMust specify a Query, WUID, ECL File, Archive, or shared object to run\n");
            return false;
        }
        if (optObj.type==eclObjTypeUnknown)
        {
            fprintf(stderr, "\nCan't determine content type of argument %s\n", optObj.value.sget());
            return false;
        }
        if (optObj.type==eclObjArchive || optObj.type==eclObjSource)
        {
            if (optCluster.isEmpty())
            {
                fprintf(stderr, "\nCluster must be specified when running ECL Text or Archive\n");
                return false;
            }
        }
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
    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
        VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);

        Owned<IClientWURunRequest> req = client->createWURunRequest();
        req->setCloneWorkunit(true);

        StringBuffer wuid;
        StringBuffer queryset;
        StringBuffer query;

        if (optObj.type==eclObjWuid)
        {
            req->setWuid(wuid.set(optObj.value.get()).str());
            if (optVerbose)
                fprintf(stdout, "Running workunit %s\n", wuid.str());
        }
        else if (optObj.type==eclObjQuery)
        {
            req->setQuerySet(queryset.set(optObj.value.get()).str());
            req->setQuery(query.set(optObj.query.get()).str());
            if (optVerbose)
                fprintf(stdout, "Running query %s/%s\n", queryset.str(), query.str());
        }
        else
        {
            req->setCloneWorkunit(false);
            if (!doDeploy(*this, client, optCluster.get(), optName.get(), &wuid, optNoArchive, optVerbose))
                return 1;
            req->setWuid(wuid.str());
            if (optVerbose)
                fprintf(stdout, "Running deployed workunit %s\n", wuid.str());
        }

        if (optCluster.length())
            req->setCluster(optCluster.get());
        req->setWait((int)optWaitTime);
        if (optInput.length())
            req->setInput(optInput.get());

        Owned<IClientWURunResponse> resp = client->WURun(req);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        StringBuffer respwuid(resp->getWuid());
        if (optVerbose && respwuid.length() && !streq(wuid.str(), respwuid.str()))
            fprintf(stdout, "As %s\n", respwuid.str());
        if (!streq(resp->getState(), "completed"))
            fprintf(stderr, "%s %s\n", respwuid.str(), resp->getState());
        if (resp->getResults())
            fprintf(stdout, "%s\n", resp->getResults());

        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl run [--cluster=<val>][--input=<file|xml>][--wait=<ms>] <wuid>\n"
            "ecl run [--cluster=<c>][--input=<file|xml>][--wait=<ms>] <queryset> <query>\n"
            "ecl run [--cluster=<c>][--name=<nm>][--input=<file|xml>][--wait=<i>] <dll|->\n"
            "ecl run --cluster=<c> --name=<nm> [--input=<file|xml>][--wait=<i>] <archive|->\n"
            "ecl run --cluster=<c> --name=<nm> [--input=<file|xml>][--wait=<i>] <eclfile|->\n\n"
            "   -                      specifies object should be read from stdin\n"
            "   <wuid>                 workunit to publish\n"
            "   <archive|->            archive to publish\n"
            "   <ecl_file|->           ECL text file to publish\n"
            "   <so|dll|->             workunit dll or shared object to publish\n"
            " Options:\n"
            "   -cl, --cluster=<val>   cluster to run job on\n"
            "                          (defaults to cluster defined inside workunit)\n"
            "   -n, --name=<val>       job name\n"
            "   -in,--input=<file|xml> file or xml content to use as query input\n"
            "   --wait=<ms>            time to wait for completion\n"
        );
        EclCmdWithEclTarget::usage();
    }
private:
    StringAttr optCluster;
    StringAttr optName;
    StringAttr optInput;
    unsigned optWaitTime;
};

class EclCmdActivate : public EclCmdWithQueryTarget
{
public:
    EclCmdActivate()
    {
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
        VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);

        Owned<IClientWUQuerySetQueryActionRequest> req = client->createWUQuerysetQueryActionRequest();
        IArrayOf<IEspQuerySetQueryActionItem> queries;
        Owned<IEspQuerySetQueryActionItem> item = createQuerySetQueryActionItem();
        item->setQueryId(optQuery.get());
        queries.append(*item.getClear());
        req->setQueries(queries);

        req->setAction("Activate");
        req->setQuerySetName(optQuerySet.get());

        Owned<IClientWUQuerySetQueryActionResponse> resp = client->WUQuerysetQueryAction(req);
        IArrayOf<IConstQuerySetQueryActionResult> &results = resp->getResults();
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else if (results.empty())
            fprintf(stderr, "\nError Empty Result!\n");
        else
        {
            IConstQuerySetQueryActionResult &item = results.item(0);
            if (item.getSuccess())
                fprintf(stdout, "\nActivated %s/%s\n", optQuerySet.sget(), optQuery.sget());
            else if (item.getCode()|| item.getMessage())
                fprintf(stderr, "Error (%d) %s\n", item.getCode(), item.getMessage());
        }
        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl activate <queryset> <query>\n"
            " Options:\n"
            "   <queryset>             name of queryset containing query to activate\n"
            "   <query>                query to activate\n"
        );
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
        Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
        VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);

        Owned<IClientWUQuerySetQueryActionRequest> req = client->createWUQuerysetQueryActionRequest();

        req->setQuerySetName(optQuerySet.get());
        req->setAction("Delete");

        IArrayOf<IEspQuerySetQueryActionItem> queries;
        Owned<IEspQuerySetQueryActionItem> item = createQuerySetQueryActionItem();
        item->setQueryId(optQuery.get());
        queries.append(*item.getClear());
        req->setQueries(queries);

        Owned<IClientWUQuerySetQueryActionResponse> resp = client->WUQuerysetQueryAction(req);
        IArrayOf<IConstQuerySetQueryActionResult> &results = resp->getResults();
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else if (results.empty())
            fprintf(stderr, "\nError Empty Result!\n");
        else
        {
            IConstQuerySetQueryActionResult &item = results.item(0);
            if (item.getSuccess())
                fprintf(stdout, "\nUnpublished %s/%s\n", optQuerySet.sget(), optQuery.sget());
            else if (item.getCode()|| item.getMessage())
                fprintf(stderr, "Error (%d) %s\n", item.getCode(), item.getMessage());
        }
        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl unpublish <queryset> <query>\n"
            " Options:\n"
            "   <queryset>             name of queryset containing query to unpublish\n"
            "   <query>                query to remove from query set\n"
        );
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
        Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
        VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);

        Owned<IClientWUQuerySetAliasActionRequest> req = client->createWUQuerysetAliasActionRequest();
        IArrayOf<IEspQuerySetAliasActionItem> aliases;
        Owned<IEspQuerySetAliasActionItem> item = createQuerySetAliasActionItem();
        item->setName(optQuery.get());
        aliases.append(*item.getClear());
        req->setAliases(aliases);

        req->setAction("Deactivate");
        req->setQuerySetName(optQuerySet.get());

        Owned<IClientWUQuerySetAliasActionResponse> resp = client->WUQuerysetAliasAction(req);
        IArrayOf<IConstQuerySetAliasActionResult> &results = resp->getResults();
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else if (results.empty())
            fprintf(stderr, "\nError Empty Result!\n");
        else
        {
            IConstQuerySetAliasActionResult &item = results.item(0);
            if (item.getSuccess())
                fprintf(stdout, "Deactivated alias %s/%s\n", optQuerySet.sget(), optQuery.sget());
            else if (item.getCode()|| item.getMessage())
                fprintf(stderr, "Error (%d) %s\n", item.getCode(), item.getMessage());
        }
        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl deactivate <queryset> <query>\n"
            " Options:\n"
            "   <queryset>             queryset containing alias to deactivate\n"
            "   <query>                query to deactivate (delete)\n"
        );
        EclCmdWithQueryTarget::usage();
    }
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
    if (strieq(cmdname, "activate"))
        return new EclCmdActivate();
    if (strieq(cmdname, "deactivate"))
        return new EclCmdDeactivate();
    return NULL;
}
