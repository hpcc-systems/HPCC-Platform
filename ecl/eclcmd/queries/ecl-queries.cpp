/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "build-config.h"

#include "ws_workunits.hpp"

#include "eclcmd.hpp"
#include "eclcmd_common.hpp"
#include "eclcmd_core.hpp"

#define INIFILE "ecl.ini"
#define SYSTEMCONFDIR CONFIG_DIR
#define DEFAULTINIFILE "ecl.ini"
#define SYSTEMCONFFILE ENV_CONF_FILE

//=========================================================================================

class ActiveQueryMap
{
public:
    ActiveQueryMap(IConstWUQuerySetDetail &qs) : queryMap(createPTree())
    {
        IArrayOf<IConstQuerySetAlias> &aliases = qs.getAliases();
        ForEachItemIn(i, aliases)
            addMappedAlias(aliases.item(i).getId(), aliases.item(i).getName());
    }

    void addMappedAlias(const char *queryid, const char *alias)
    {
        if (queryid && *queryid && alias && *alias)
            ensureMappedQuery(queryid)->addProp("Alias", alias);
    }

    IPropertyTree *ensureMappedQuery(const char *queryid)
    {
        VStringBuffer xpath("Query[@id='%s']", queryid);
        IPropertyTree *query = queryMap->getPropTree(xpath.str());
        if (!query)
        {
            query = queryMap->addPropTree("Query", createPTree());
            query->setProp("@id", queryid);
        }
        return query;
    }

    bool isActive(const char *queryid)
    {
        VStringBuffer xpath("Query[@id='%s']", queryid);
        return queryMap->hasProp(xpath.str());
    }

    IPropertyTreeIterator *getActiveNames(const char *queryid)
    {
        VStringBuffer xpath("Query[@id='%s']/Alias", queryid);
        return queryMap->getElements(xpath.str());
    }

private:
    Linked<IPropertyTree> queryMap;
};



#define QUERYLIST_SHOW_UNFLAGGED            0x01
#define QUERYLIST_SHOW_ACTIVE               0x02
#define QUERYLIST_SHOW_SUSPENDED            0x04
#define QUERYLIST_SHOW_CLUSTER_SUSPENDED    0x08

class EclCmdQueriesList : public EclCmdCommon
{
public:
    EclCmdQueriesList() : flags(0)
    {
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                optQuerySet.set(arg);
                continue;
            }
            if (iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED)||iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED_S))
                continue;
            if (iter.matchOption(optTargetCluster, ECLOPT_TARGET)||iter.matchOption(optTargetCluster, ECLOPT_TARGET_S))
                continue;
            StringAttr temp;
            if (iter.matchOption(temp, ECLOPT_SHOW))
            {
                for (const char *ch = temp.sget(); *ch; ch++)
                {
                    switch (*ch)
                    {
                    case 'A':
                        flags |= QUERYLIST_SHOW_ACTIVE;
                        break;
                    case 'S':
                        flags |= QUERYLIST_SHOW_SUSPENDED;
                        break;
                    case 'X':
                        flags |= QUERYLIST_SHOW_CLUSTER_SUSPENDED;
                        break;
                    case 'U':
                        flags |= QUERYLIST_SHOW_UNFLAGGED;
                        break;
                    default:
                        fprintf(stderr, "Unrecognized --show flag = %c", *ch);
                        return false;
                    }
                }
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
        return true;
    }

    void outputQuery(IConstQuerySetQuery &query, ActiveQueryMap &queryMap)
    {
        const char *queryid = query.getId();
        bool isActive = queryMap.isActive(queryid);
        bool suspendedOnCluster = false;
        ForEachItemIn(idx, query.getClusters())
        {
            IConstClusterQueryState &state = query.getClusters().item(idx);
            if (strieq(state.getState(), "Suspended"))
            {
                suspendedOnCluster = true;
                break;
            }
        }

        if (flags)
        {
            if (isActive && !(flags & QUERYLIST_SHOW_ACTIVE))
                return;
            if (query.getSuspended() && !(flags & QUERYLIST_SHOW_SUSPENDED))
                return;
            if (suspendedOnCluster && !(flags & QUERYLIST_SHOW_CLUSTER_SUSPENDED))
                return;
            if (!isActive && !query.getSuspended() &&  !(flags & QUERYLIST_SHOW_UNFLAGGED))
                return;
        }
        StringBuffer line(" ");
        line.append(suspendedOnCluster ? 'X' : ' ');
        line.append(query.getSuspended() ? 'S' : ' ');
        line.append(isActive ? 'A' : ' ');
        line.append(' ').append(queryid);
        if (isActive)
        {
            Owned<IPropertyTreeIterator> activeNames = queryMap.getActiveNames(queryid);
            if (line.length() < 35)
                line.appendN(35 - line.length(), ' ');
            line.append("[");
            activeNames->first();
            while (activeNames->isValid())
            {
                line.append(activeNames->query().queryProp(NULL));
                if (activeNames->next())
                    line.append(',');
            }
            line.append("]");
        }
        fputs(line.append('\n').str(), stdout);
    }

    void outputQueryset(IConstWUQuerySetDetail &qs)
    {
        ActiveQueryMap queryMap(qs);
        if (qs.getQuerySetName())
            fprintf(stdout, "\nQuerySet: %s\n", qs.getQuerySetName());
        fputs("\nFlags Query Id                     [Active Name(s)]\n", stdout);
        fputs("----- ---------------------------- ----------------\n", stdout);

        IArrayOf<IConstQuerySetQuery> &queries = qs.getQueries();
        ForEachItemIn(id, queries)
            outputQuery(queries.item(id), queryMap);
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
        VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);

        Owned<IClientWUMultiQuerySetDetailsRequest> req = client->createWUMultiQuerysetDetailsRequest();
        req->setQuerySetName(optQuerySet.get());
        req->setClusterName(optTargetCluster.get());
        req->setFilterType("All");

        Owned<IClientWUMultiQuerySetDetailsResponse> resp = client->WUMultiQuerysetDetails(req);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else
        {
            IArrayOf<IConstWUQuerySetDetail> &querysets = resp->getQuerysets();
            ForEachItemIn(i, querysets)
                outputQueryset(querysets.item(i));
        }
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'queries list' command displays a list of the queries in one or more\n"
            "querysets. If a cluster is provided the querysets associated with that\n"
            "cluster will be shown. If no queryset or cluster is specified all querysets\n"
            "are shown.\n"
            "\n"
            "ecl queries list [<queryset>][--target=<val>][--show=<flags>]\n\n"
            " Options:\n"
            "   <queryset>             name of queryset to get list of queries for\n"
            "   -t, --target=<val>     target cluster to get list of published queries for\n"
            "   --show=<flags>         show only queries with matching flags\n"
            " Flags:\n"
            "   A                      query is active\n"
            "   S                      query is suspended in queryset\n"
//not yet   "   X                      query is suspended on selected cluster\n"
            "   U                      query with no flags set\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optTargetCluster;
    StringAttr optQuerySet;
    unsigned flags;
};

class EclCmdQueriesCopy : public EclCmdCommon
{
public:
    EclCmdQueriesCopy() : optActivate(false), optNoReload(false), optMsToWait(10000)
    {
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                if (optSourceQueryPath.isEmpty())
                    optSourceQueryPath.set(arg);
                else if (optTargetQuerySet.isEmpty())
                    optTargetQuerySet.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return false;
                }
                continue;
            }
            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE)||iter.matchFlag(optActivate, ECLOPT_ACTIVATE_S))
                continue;
            if (iter.matchFlag(optNoReload, ECLOPT_NORELOAD))
                continue;
            if (iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED)||iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED_S))
                continue;
            if (iter.matchOption(optTargetCluster, ECLOPT_TARGET)||iter.matchOption(optTargetCluster, ECLOPT_TARGET_S))
                continue;
            if (iter.matchOption(optMsToWait, ECLOPT_WAIT))
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
        if (optSourceQueryPath.isEmpty() || optTargetQuerySet.isEmpty())
        {
            fputs("source and target must both be specified.\n\n", stderr);
            return false;
        }
        if (optSourceQueryPath.get()[0]=='/' && optSourceQueryPath.get()[1]=='/' && optTargetCluster.isEmpty())
        {
            fputs("cluster must be specified for remote copies.\n\n", stderr);
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

        Owned<IClientWUQuerySetCopyQueryRequest> req = client->createWUQuerysetCopyQueryRequest();
        req->setSource(optSourceQueryPath.get());
        req->setTarget(optTargetQuerySet.get());
        req->setCluster(optTargetCluster.get());
        req->setActivate(optActivate);
        req->setWait(optMsToWait);
        req->setNoReload(optNoReload);

        Owned<IClientWUQuerySetCopyQueryResponse> resp = client->WUQuerysetCopyQuery(req);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        if (resp->getQueryId() && *resp->getQueryId())
            fprintf(stdout, "%s/%s\n\n", optTargetQuerySet.sget(), resp->getQueryId());
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'queries copy' command copies a query from one queryset to another.\n"
            "\n"
            "A query can be copied from one HPCC environment to another by using a path\n"
            "which begins with '//' followed by the IP and Port of the source EclWatch\n"
            "and then followed by the source queryset and query.\n"
            "\n"
            "ecl queries copy <source_query_path> <target_queryset> [--activate]\n"
            "\n"
            "ecl queries copy //IP:Port/queryset/query <target_queryset> [--activate]\n"
            "ecl queries copy queryset/query <target_queryset> [--activate]\n"
            "\n"
            " Options:\n"
            "   <source_query_path>    path of query to copy\n"
            "                          in the form: //ip:port/queryset/query\n"
            "                          or: queryset/query\n"
            "   <target_queryset>      name of queryset to copy the query into\n"
            "   -t, --target=<val>     Local target cluster to associate with remote workunit\n"
            "   -A, --activate         Activate the new query\n"
            "   --no-reload            Do not request a reload of the (roxie) cluster\n"
            "   --wait=<ms>            Max time to wait in milliseconds\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optSourceQueryPath;
    StringAttr optTargetQuerySet;
    StringAttr optTargetCluster;
    unsigned optMsToWait;
    bool optActivate;
    bool optNoReload;
};

IEclCommand *createEclQueriesCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    if (strieq(cmdname, "list"))
        return new EclCmdQueriesList();
    if (strieq(cmdname, "copy"))
        return new EclCmdQueriesCopy();
    return NULL;
}

//=========================================================================================

class EclQueriesCMDShell : public EclCMDShell
{
public:
    EclQueriesCMDShell(int argc, const char *argv[], EclCommandFactory _factory, const char *_version)
        : EclCMDShell(argc, argv, _factory, _version)
    {
    }

    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl queries <command> [command options]\n\n"
            "   Queries Commands:\n"
            "      list         list queries in queryset(s)\n"
            "      copy         copy a query from one queryset to another\n"
        );
    }
};

static int doMain(int argc, const char *argv[])
{
    EclQueriesCMDShell processor(argc, argv, createEclQueriesCommand, BUILD_TAG);
    return processor.run();
}

int main(int argc, const char *argv[])
{
    InitModuleObjects();
    queryStderrLogMsgHandler()->setMessageFields(0);
    unsigned exitCode = doMain(argc, argv);
    releaseAtoms();
    exit(exitCode);
}
