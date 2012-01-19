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

class ConvertEclParameterToArchive
{
public:
    ConvertEclParameterToArchive(EclCmdWithEclTarget &_cmd) : cmd(_cmd)
    {
    }

    void appendOptPath(StringBuffer &cmdLine, const char opt, const char *path)
    {
        if (!path || !*path)
            return;
        if (*path==';')
            path++;
        cmdLine.append(" -").append(opt).append(path);
    }

    void buildCmd(StringBuffer &cmdLine)
    {
        cmdLine.set("eclcc -E");
        appendOptPath(cmdLine, 'I', cmd.optImpPath.str());
        appendOptPath(cmdLine, 'L', cmd.optLibPath.str());
        if (cmd.optManifest.length())
            cmdLine.append(" -manifest ").append(cmd.optManifest.get());
        if (streq(cmd.optObj.value.sget(), "stdin"))
            cmdLine.append(" - ");
        else
            cmdLine.append(" ").append(cmd.optObj.value.get());
    }

    bool eclcc(StringBuffer &out)
    {
        StringBuffer cmdLine;
        buildCmd(cmdLine);

        Owned<IPipeProcess> pipe = createPipeProcess();
        bool hasInput = streq(cmd.optObj.value.sget(), "stdin");
        pipe->run(cmd.optVerbose ? "EXEC" : NULL, cmdLine.str(), NULL, hasInput, true, true);

        StringBuffer errors;
        Owned<EclCmdErrorReader> errorReader = new EclCmdErrorReader(pipe, errors);
        errorReader->start();

        if (pipe->hasInput())
        {
            pipe->write(cmd.optObj.mb.length(), cmd.optObj.mb.toByteArray());
            pipe->closeInput();
        }
        if (pipe->hasOutput())
        {
           byte buf[4096];
           loop
           {
                size32_t read = pipe->read(sizeof(buf),buf);
                if (!read)
                    break;
                out.append(read, (const char *) buf);
            }
        }
        int retcode = pipe->wait();
        errorReader->join();

        if (errors.length())
            fprintf(stderr, "%s\n", errors.str());

        return (retcode == 0);
    }

    bool process()
    {
        if (cmd.optObj.type!=eclObjSource || cmd.optObj.value.isEmpty())
            return false;

        StringBuffer output;
        if (eclcc(output) && output.length() && isArchiveQuery(output.str()))
        {
            cmd.optObj.type = eclObjArchive;
            cmd.optObj.mb.clear().append(output.str());
            return true;
        }
        fprintf(stderr,"\nError creating archive\n");
        return false;
    }

private:
    EclCmdWithEclTarget &cmd;

    class EclCmdErrorReader : public Thread
    {
    public:
        EclCmdErrorReader(IPipeProcess *_pipe, StringBuffer &_errs)
            : Thread("EclToArchive::ErrorReader"), pipe(_pipe), errs(_errs)
        {
        }

        virtual int run()
        {
           byte buf[4096];
           loop
           {
                size32_t read = pipe->readError(sizeof(buf), buf);
                if (!read)
                    break;
                errs.append(read, (const char *) buf);
            }
            return 0;
        }
    private:
        IPipeProcess *pipe;
        StringBuffer &errs;
    };
};



bool doDeploy(EclCmdWithEclTarget &cmd, IClientWsWorkunits *client, const char *cluster, const char *name, StringBuffer *wuid, bool displayWuid=true)
{
    StringBuffer s;
    if (cmd.optVerbose)
        fprintf(stdout, "\nDeploying %s\n", cmd.optObj.getDescription(s).str());

    if (cmd.optObj.type==eclObjSource)
    {
        ConvertEclParameterToArchive conversion(cmd);
        if (!conversion.process())
            return false;
    }

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
            fprintf(stderr, "Failed to create archive from ECL Text\n");
            return false;
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
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME))
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
        return doDeploy(*this, client, optCluster.get(), optName.get(), NULL) ? 0 : 1;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl deploy --cluster=<cluster> --name=<name> <ecl_file|->\n"
            "ecl deploy --cluster=<cluster> --name=<name> <archive|->\n"
            "ecl deploy [--cluster=<cluster>] [--name=<name>] <so|dll|->\n\n"
            "      -                    specifies object should be read from stdin\n"
            "      <ecl_file|->         ecl text file to deploy\n"
            "      <archive|->          ecl archive to deploy\n"
            "      <so|dll|->           workunit dll or shared object to deploy\n"
            "   Options:\n"
            "      --name=<name>        workunit job name\n"
            "      --cluster=<cluster>  cluster to associate workunit with\n"
            "      --name=<name>        workunit job name\n"
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
    EclCmdPublish() : optActivate(false), activateSet(false)
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
            if (iter.matchOption(optObj.value, ECLOPT_WUID))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME))
                continue;
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER))
                continue;
            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE))
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
            if (optName.isEmpty())
            {
                fprintf(stderr, "\nQuery name must be specified when publishing an ECL Text or Archive\n");
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
        else if (!doDeploy(*this, client, optCluster.get(), optName.get(), &wuid))
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

        Owned<IClientWUPublishWorkunitResponse> resp = client->WUPublishWorkunit(req);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        const char *id = resp->getQueryId();
        if (id && *id)
        {
            const char *qs = resp->getQuerySet();
            fprintf(stdout, "\n%s/%s\n", qs ? qs : "", resp->getQueryId());
        }

        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl publish [--cluster=<cluster>] [--name=<name>] [--activate] <wuid>\n"
            "ecl publish [--cluster=<cluster>] [--name=<name>] [--activate] <so|dll|->\n"
            "ecl publish --cluster=<cluster> --name=<name> [--activate] <archive|->\n\n"
            "ecl publish --cluster=<cluster> --name=<name> [--activate] <ecl_file|->\n\n"
            "      -                    specifies object should be read from stdin\n"
            "      <wuid>               workunit to publish\n"
            "      <archive|->          archive to publish\n"
            "      <ecl_file|->         ECL text file to publish\n"
            "      <so|dll|->           workunit dll or shared object to publish\n"
            "   Options:\n"
            "      --cluster=<cluster>  cluster to publish workunit to\n"
            "                           (defaults to cluster defined inside workunit)\n"
            "      --name=<name>        query name to use for published workunit\n"
            "      --activate           activates query when published\n"
        );
        EclCmdWithEclTarget::usage();
    }
private:
    StringAttr optCluster;
    StringAttr optName;
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
            if (iter.matchOption(optObj.value, ECLOPT_WUID))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME))
                continue;
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER))
                continue;
            if (iter.matchOption(optInput, ECLOPT_INPUT))
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
                fprintf(stderr, "\nCluster must be specified when publishing ECL Text or Archive\n");
                return false;
            }
            if (optName.isEmpty())
            {
                fprintf(stderr, "\nQuery name must be specified when publishing an ECL Text or Archive\n");
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
            if (!doDeploy(*this, client, optCluster.get(), optName.get(), &wuid, optVerbose))
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
            fprintf(stderr, "As %s\n", resp->getWuid());
        if (!streq(resp->getState(), "completed"))
        {
            fprintf(stderr, "%s\n", resp->getState());
            return 1;
        }
        if (resp->getResults())
            fprintf(stdout, "%s", resp->getResults());

        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl run [--cluster=<c>][--input=<file|xml>][--wait=<ms>] <wuid>\n"
            "ecl run [--cluster=<c>][--input=<file|xml>][--wait=<ms>] <queryset> <query>\n"
            "ecl run [--cluster=<c>][--name=<nm>][--input=<file|xml>][--wait=<i>] <dll|->\n"
            "ecl run --cluster=<c> --name=<nm> [--input=<file|xml>][--wait=<i>] <archive|->\n"
            "ecl run --cluster=<c> --name=<nm> [--input=<file|xml>][--wait=<i>] <eclfile|->\n\n"
            "      -                    specifies object should be read from stdin\n"
            "      <wuid>               workunit to publish\n"
            "      <archive|->          archive to publish\n"
            "      <ecl_file|->         ECL text file to publish\n"
            "      <so|dll|->           workunit dll or shared object to publish\n"
            "   Options:\n"
            "      --cluster=<cluster>  cluster to run job on\n"
            "                           (defaults to cluster defined inside workunit)\n"
            "      --name=<name>        job name\n"
            "      --input=<file|xml>   file or xml content to use as query input\n"
            "      --wait=<ms>          time to wait for completion\n"
        );
        EclCmdWithEclTarget::usage();
    }
private:
    StringAttr optCluster;
    StringAttr optName;
    StringAttr optInput;
    unsigned optWaitTime;
};

class EclCmdActivate : public EclCmdCommon
{
public:
    EclCmdActivate()
    {
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
            if (*arg!='-')
            {
                if (optQueryId.length())
                {
                    fprintf(stderr, "\nmultiple query ids (%s and %s) not supported\n", optQueryId.sget(), arg);
                    return false;
                }
                optQueryId.set(arg);
                continue;
            }
            if (iter.matchOption(optQuerySet, ECLOPT_QUERYSET))
                continue;
            if (EclCmdCommon::matchCommandLineOption(iter, true)!=EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optQuerySet.isEmpty())
        {
            fprintf(stderr, "\nError: queryset parameter required\n");
            return false;
        }
        if (optQueryId.isEmpty())
        {
            fprintf(stderr, "\nError: queryid parameter required\n");
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

        Owned<IClientWUQuerySetQueryActionRequest> req = client->createWUQuerysetQueryActionRequest();
        IArrayOf<IEspQuerySetQueryActionItem> queries;
        Owned<IEspQuerySetQueryActionItem> item = createQuerySetQueryActionItem();
        item->setQueryId(optQueryId.get());
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
                fprintf(stdout, "\nActivated %s/%s\n", optQuerySet.sget(), optQueryId.sget());
            else if (item.getCode()|| item.getMessage())
                fprintf(stderr, "Error (%d) %s\n", item.getCode(), item.getMessage());
        }
        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl activate --queryset=<queryset> <queryid>\n"
            "   Options:\n"
            "      <queryid>             queryid of query to activate\n"
            "      --queryset=<queryset> name of queryset containing query to activate\n"
        );
        EclCmdCommon::usage();
    }
private:
    StringAttr optQuerySet;
    StringAttr optQueryId;
};


class EclCmdDeactivate : public EclCmdCommon
{
public:
    EclCmdDeactivate()
    {
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
            if (*arg!='-')
            {
                if (optAlias.length())
                {
                    fprintf(stderr, "\nmultiple aliases not supported\n");
                    return false;
                }
                optAlias.set(arg);
                continue;
            }
            if (iter.matchOption(optQuerySet, ECLOPT_QUERYSET))
                continue;
            if (EclCmdCommon::matchCommandLineOption(iter, true)!=EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optQuerySet.isEmpty())
        {
            fprintf(stderr, "\nError: queryset parameter required\n");
            return false;
        }
        if (optAlias.isEmpty())
        {
            fprintf(stderr, "\nError: alias parameter required\n");
            return false;
        }
        return true;
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
        item->setName(optAlias.get());
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
                fprintf(stdout, "Deactivated alias %s/%s\n", optQuerySet.sget(), optAlias.sget());
            else if (item.getCode()|| item.getMessage())
                fprintf(stderr, "Error (%d) %s\n", item.getCode(), item.getMessage());
        }
        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl deactivate --queryset=<queryset> <alias>\n"
            "   Options:\n"
            "      <alias>               alias to deactivate (delete)\n"
            "      queryset=<queryset>   queryset containing alias to deactivate\n"
        );
        EclCmdCommon::usage();
    }
private:
    StringAttr optQuerySet;
    StringAttr optAlias;
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
    if (strieq(cmdname, "run"))
        return new EclCmdRun();
    if (strieq(cmdname, "activate"))
        return new EclCmdActivate();
    if (strieq(cmdname, "deactivate"))
        return new EclCmdDeactivate();
    return NULL;
}
