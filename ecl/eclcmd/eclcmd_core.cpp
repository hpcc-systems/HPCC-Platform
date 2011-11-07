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

bool doDeploy(IClientWsWorkunits *client, EclObjectParameter &optObj, const char *cluster, const char *name, StringBuffer *wuid)
{
    StringBuffer s;
    fprintf(stdout, "\nDeploying %s\n", optObj.getDescription(s).str());
    Owned<IClientWUDeployWorkunitRequest> req = client->createWUDeployWorkunitRequest();
    switch (optObj.type)
    {
        case eclObjArchive:
            req->setObjType("archive");
            break;
        case eclObjSharedObject:
            req->setObjType("shared_object");
            break;
        default:
            fprintf(stdout, "Can only deploy shared objects and ECL archives\n");
            return false;
    }

    MemoryBuffer mb;
    Owned<IFile> file = createIFile(optObj.value.sget());
    Owned<IFileIO> io = file->open(IFOread);
    read(io, 0, (size32_t)file->size(), mb);
    if (name && *name)
        req->setName(name);
    if (cluster && *cluster)
        req->setCluster(cluster);
    req->setFileName(optObj.value.sget());
    req->setObject(mb);
    Owned<IClientWUDeployWorkunitResponse> resp = client->WUDeployWorkunit(req);
    if (resp->getExceptions().ordinality())
        outputMultiExceptions(resp->getExceptions());
    const char *w = resp->getWorkunit().getWuid();
    if (w && *w)
    {
        if (wuid)
            wuid->append(w);
        fprintf(stdout, "Deployed\nwuid: %s\nstate: %s\n", w, resp->getWorkunit().getState());
        return true;
    }
    return false;
}

class EclCmdDeploy : public EclCmdCommon
{
public:
    EclCmdDeploy() : optObj(eclObjSource | eclObjArchive | eclObjSharedObject)
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
                if (optObj.value.length())
                {
                    fprintf(stderr, "\nmultiple targets (%s and %s) not currently supported\n", optObj.value.sget(), arg);
                    return false;
                }
                optObj.set(arg);
                continue;
            }
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME))
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
        if (optObj.value.isEmpty())
        {
            fprintf(stderr, "\nNo target Archive or DLL specified for deployment\n");
            return false;
        }
        if (optObj.type==eclObjTypeUnknown)
        {
            fprintf(stderr, "\nCan't determine content type of argument %s\n", optObj.value.sget());
            return false;
        }
        if (optObj.type==eclObjSource)
        {
            fprintf(stderr, "\nDirect deployment of ECL source is not yet supported\n");
            return false;
        }
        if (optObj.type==eclObjWuid)
        {
            StringBuffer s;
            fprintf(stderr, "\nWUID (%s) cannot be the target for deployment\n", optObj.getDescription(s).str());
            return false;
        }
        if (optObj.type==eclObjQueryId)
        {
            StringBuffer s;
            fprintf(stderr, "\nQuery (%s) cannot be the target for deployment\n", optObj.getDescription(s).str());
            return false;
        }
        if (optObj.type==eclObjArchive && optCluster.isEmpty())
        {
            fprintf(stderr, "\nCluster must be specified when deploying an ECL Archive\n");
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
        return doDeploy(client, optObj, optCluster.get(), optName.get(), NULL) ? 0 : 1;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl deploy --cluster=<cluster> --name=<name> <archive>\n"
            "ecl deploy [--cluster=<cluster>] [--name=<name>] <so|dll>\n\n"
            "   Options:\n"
            "      <archive>            ecl archive to deploy\n"
            "      <so|dll>             workunit dll or shared object to deploy\n"
            "      --name=<name>        workunit job name\n"
            "      --cluster=<cluster>  cluster to associate workunit with\n"
            "      --name=<name>        workunit job name\n"
        );
        EclCmdCommon::usage();
    }
private:
    EclObjectParameter optObj;
    StringAttr optCluster;
    StringAttr optName;
};

class EclCmdPublish : public EclCmdCommon
{
public:
    EclCmdPublish() : optActivate(false), activateSet(false), optObj(eclObjWuid | eclObjArchive | eclObjSharedObject)
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
                if (optObj.value.length())
                {
                    fprintf(stderr, "\nmultiple targets (%s and %s) not currently supported\n", optObj.value.sget(), arg);
                    return false;
                }
                optObj.set(arg);
                continue;
            }
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
            if (EclCmdCommon::matchCommandLineOption(iter, true)!=EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (!activateSet)
            extractEclCmdOption(optActivate, globals, ECLOPT_ACTIVATE_ENV, ECLOPT_ACTIVATE_INI, false);
        if (optObj.value.isEmpty())
        {
            fprintf(stderr, "\nMust specify a WUID, Archive, or shared object to publish\n");
            return false;
        }
        if (optObj.type==eclObjTypeUnknown)
        {
            fprintf(stderr, "\nCan't determine content type of argument %s\n", optObj.value.sget());
            return false;
        }
        if (optObj.type==eclObjSource)
        {
            fprintf(stderr, "\nPublishing ECL source directly is not yet supported\n");
            return false;
        }
        if (optObj.type==eclObjQueryId)
        {
            StringBuffer s;
            fprintf(stderr, "\nQuery (%s) cannot be the target for pulishing\n", optObj.getDescription(s).str());
            return false;
        }
        if (optObj.type==eclObjArchive)
        {
            if (optCluster.isEmpty())
            {
                fprintf(stderr, "\nCluster must be specified when publishing an ECL Archive\n");
                return false;
            }
            if (optName.isEmpty())
            {
                fprintf(stderr, "\nQuery name must be specified when publishing an ECL Archive\n");
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
        else if (!doDeploy(client, optObj, optCluster.get(), optName.get(), &wuid))
            return 1;

        StringBuffer descr;
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
            fprintf(stdout, "\nPublished\nQuerySet: %s\nQueryName: %s\nQueryId: %s\n", resp->getQuerySet(), resp->getQueryName(), resp->getQueryId());

        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl publish [--cluster=<cluster>] [--name=<name>] [--activate] <wuid>\n"
            "ecl publish [--cluster=<cluster>] [--name=<name>] [--activate] <so|dll>\n"
            "ecl publish --cluster=<cluster> --name=<name> [--activate] <archive>\n\n"
            "   Options:\n"
            "      <wuid>               workunit to publish\n"
            "      <archive>            archive to publish\n"
            "      <so|dll>             workunit dll or shared object to publish\n"
            "      --cluster=<cluster>  cluster to publish workunit to\n"
            "                           (defaults to cluster defined inside workunit)\n"
            "      --name=<name>        query name to use for published workunit\n"
            "      --activate           activates query when published\n"
        );
        EclCmdCommon::usage();
    }
private:
    StringAttr optCluster;
    EclObjectParameter optObj;
    StringAttr optName;
    bool optActivate;
    bool activateSet;
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
    if (strieq(cmdname, "activate"))
        return new EclCmdActivate();
    if (strieq(cmdname, "deactivate"))
        return new EclCmdDeactivate();
    return NULL;
}
