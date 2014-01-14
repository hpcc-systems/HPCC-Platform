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
#define QUERYLIST_SHOW_INACTIVE             (QUERYLIST_SHOW_UNFLAGGED | QUERYLIST_SHOW_SUSPENDED | QUERYLIST_SHOW_CLUSTER_SUSPENDED)

class EclCmdQueriesList : public EclCmdCommon
{
public:
    EclCmdQueriesList() : flags(0), optInactive(false)
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
                optTargetCluster.set(arg);
                continue;
            }
            if (iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED)||iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED_S))
                continue;
            if (iter.matchOption(optTargetCluster, ECLOPT_TARGET)||iter.matchOption(optTargetCluster, ECLOPT_TARGET_S))
                continue;
            if (iter.matchFlag(optInactive, ECLOPT_INACTIVE))
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
        if (optInactive)
        {
            if (flags)
            {
                fputs("--show and --inactive should not be used together.\n\n", stderr);
                return false;
            }

            flags = QUERYLIST_SHOW_INACTIVE;
        }
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
        if (!query.getTimeLimit_isNull())
        {
            if (line.length() < 34)
                line.appendN(34 - line.length(), ' ');
            line.append(' ').append(query.getTimeLimit());
        }
        if (!query.getWarnTimeLimit_isNull())
        {
            if (line.length() < 41)
                line.appendN(41 - line.length(), ' ');
            line.append(' ').append(query.getWarnTimeLimit());
        }
        if (query.getPriority())
        {
            if (line.length() < 48)
                line.appendN(48 - line.length(), ' ');
            line.append(' ').append(query.getPriority());
        }
        if (query.getMemoryLimit())
        {
            if (line.length() < 53)
                line.appendN(53 - line.length(), ' ');
            line.append(' ').append(query.getMemoryLimit());
        }
        if (query.getComment())
        {
            if (line.length() < 64)
                line.appendN(64 - line.length(), ' ');
            line.append(' ').append(query.getComment());
        }
        fputs(line.append('\n').str(), stdout);
    }

    void outputQueryset(IConstWUQuerySetDetail &qs)
    {
        ActiveQueryMap queryMap(qs);
        if (qs.getQuerySetName())
            fprintf(stdout, "\nTarget: %s\n", qs.getQuerySetName());
        fputs("\n", stdout);
        fputs("                                   Time   Warn        Memory\n", stdout);
        fputs("Flags Query Id                     Limit  Limit  Pri  Limit      Comment\n", stdout);
        fputs("----- ---------------------------- ------ ------ ---- ---------- ------------\n", stdout);

        IArrayOf<IConstQuerySetQuery> &queries = qs.getQueries();
        ForEachItemIn(id, queries)
            outputQuery(queries.item(id), queryMap);
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUMultiQuerySetDetailsRequest> req = client->createWUMultiQuerysetDetailsRequest();
        req->setQuerySetName(optTargetCluster.get());
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
            "The 'queries list' command displays a list of the queries published to one\n"
            "or more target clusters. If a target is provided the querysets associated with\n"
            "that cluster will be shown. If no queryset or cluster is specified all targets\n"
            "are shown.\n"
            "\n"
            "ecl queries list [<target>][--show=<flags>]\n\n"
            " Options:\n"
            "   <target>               Name of target cluster to get list of queries for\n"
            "   --show=<flags>         Show only queries with matching flags\n"
            "   --inactive             Show only queries that do not have an active alias\n"
            " Flags:\n"
            "   A                      Query is active\n"
            "   S                      Query is suspended in queryset\n"
//not yet   "   X                      Query is suspended on selected cluster\n"
            "   U                      Query with no flags set\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optTargetCluster;
    unsigned flags;
    bool optInactive;
};

class EclCmdQueriesCopy : public EclCmdCommon
{
public:
    EclCmdQueriesCopy() : optActivate(false), optNoReload(false), optMsToWait(10000), optDontCopyFiles(false), optOverwrite(false)
    {
        optTimeLimit = (unsigned) -1;
        optWarnTimeLimit = (unsigned) -1;
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
                if (optSourceQueryPath.isEmpty())
                    optSourceQueryPath.set(arg);
                else if (optTargetCluster.isEmpty())
                    optTargetCluster.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return false;
                }
                continue;
            }
            if (iter.matchOption(optDaliIP, ECLOPT_DALIIP))
                continue;
            if (iter.matchOption(optSourceProcess, ECLOPT_SOURCE_PROCESS))
                continue;
            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE)||iter.matchFlag(optActivate, ECLOPT_ACTIVATE_S))
                continue;
            if (iter.matchFlag(optNoReload, ECLOPT_NORELOAD))
                continue;
            if (iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED)||iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED_S))
                continue;
            if (iter.matchOption(optTargetCluster, ECLOPT_TARGET)||iter.matchOption(optTargetCluster, ECLOPT_TARGET_S))
                continue;
            if (iter.matchFlag(optDontCopyFiles, ECLOPT_DONT_COPY_FILES))
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
            if (iter.matchFlag(optOverwrite, ECLOPT_OVERWRITE)||iter.matchFlag(optOverwrite, ECLOPT_OVERWRITE_S))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
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
        if (optSourceQueryPath.isEmpty() && optTargetCluster.isEmpty())
        {
            fputs("source and target must both be specified.\n\n", stderr);
            return false;
        }
        if (optMemoryLimit.length() && !isValidMemoryValue(optMemoryLimit))
        {
            fprintf(stderr, "invalid --memoryLimit value of %s.\n\n", optMemoryLimit.get());
            return false;
        }
        if (optPriority.length() && !isValidPriorityValue(optPriority))
        {
            fprintf(stderr, "invalid --priority value of %s.\n\n", optPriority.get());
            return false;
        }

        return true;
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQuerySetCopyQueryRequest> req = client->createWUQuerysetCopyQueryRequest();
        req->setSource(optSourceQueryPath.get());
        req->setTarget(optTargetCluster.get());
        req->setCluster(optTargetCluster.get());
        req->setDaliServer(optDaliIP.get());
        req->setSourceProcess(optSourceProcess);
        req->setActivate(optActivate);
        req->setOverwrite(optOverwrite);
        req->setDontCopyFiles(optDontCopyFiles);
        req->setWait(optMsToWait);
        req->setNoReload(optNoReload);

        if (optTimeLimit != (unsigned) -1)
            req->setTimeLimit(optTimeLimit);
        if (optWarnTimeLimit != (unsigned) -1)
            req->setWarnTimeLimit(optWarnTimeLimit);
        if (!optMemoryLimit.isEmpty())
            req->setMemoryLimit(optMemoryLimit);
        if (!optPriority.isEmpty())
            req->setPriority(optPriority);
        if (!optName.isEmpty())
            req->setDestName(optName);
        if (optComment.get()) //allow empty
            req->setComment(optComment);

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
            "ecl queries copy <source_query_path> <target> [--activate]\n"
            "\n"
            "ecl queries copy //IP:Port/queryset/query <target> [--activate]\n"
            "ecl queries copy queryset/query <target> [--activate]\n"
            "\n"
            " Options:\n"
            "   <source_query_path>    Path of query to copy\n"
            "                          in the form: //ip:port/queryset/query\n"
            "                          or: queryset/query\n"
            "   <target>               Name of target cluster to copy the query to\n"
            "   --no-files             Do not copy files referenced by query\n"
            "   --daliip=<location>    Format cluster@ip/prefix\n"
            "                          ip is the address of the remote dali to use for logical file lookups\n"
            "                          cluster (optional) is the process cluster to copy files from\n"
            "                          prefix (optional) will be added to the file name for lookup on\n"
            "                          the source dali, but will not be added to the local file name\n"
            "   --source-process       Process cluster to copy files from\n"
            "   -A, --activate         Activate the new query\n"
            "   --no-reload            Do not request a reload of the (roxie) cluster\n"
            "   -O, --overwrite        Overwrite existing files\n"
            "   --wait=<ms>            Max time to wait in milliseconds\n"
            "   --timeLimit=<sec>      Value to set for query timeLimit configuration\n"
            "   --warnTimeLimit=<sec>  Value to set for query warnTimeLimit configuration\n"
            "   --memoryLimit=<mem>    Value to set for query memoryLimit configuration\n"
            "                          format <mem> as 500000B, 550K, 100M, 10G, 1T etc.\n"
            "   --priority=<val>       Set the priority for this query. Value can be LOW,\n"
            "                          HIGH, SLA, NONE. NONE will clear current setting.\n"
            "   --comment=<string>     Set the comment associated with this query\n"
            "   -n, --name=<val>       Destination query name for the copied query\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optSourceQueryPath;
    StringAttr optTargetQuerySet;
    StringAttr optTargetCluster;
    StringAttr optDaliIP;
    StringAttr optSourceProcess;
    StringAttr optMemoryLimit;
    StringAttr optPriority;
    StringAttr optComment;
    StringAttr optName;
    unsigned optMsToWait;
    unsigned optTimeLimit;
    unsigned optWarnTimeLimit;
    bool optActivate;
    bool optNoReload;
    bool optOverwrite;
    bool optDontCopyFiles;
};

class EclCmdQueriesConfig : public EclCmdCommon
{
public:
    EclCmdQueriesConfig() : optNoReload(false), optMsToWait(10000)
    {
        optTimeLimit = (unsigned) -1;
        optWarnTimeLimit = (unsigned) -1;
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
                if (optTargetCluster.isEmpty())
                    optTargetCluster.set(arg);
                else if (optQueryId.isEmpty())
                    optQueryId.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return false;
                }
                continue;
            }
            if (iter.matchFlag(optNoReload, ECLOPT_NORELOAD))
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
            if (EclCmdCommon::matchCommandLineOption(iter, true)!=EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optTargetCluster.isEmpty() || optQueryId.isEmpty())
        {
            fputs("Target and QueryId must both be specified.\n\n", stderr);
            return false;
        }
        if (optMemoryLimit.length() && !isValidMemoryValue(optMemoryLimit))
        {
            fprintf(stderr, "invalid --memoryLimit value of %s.\n\n", optMemoryLimit.get());
            return false;
        }
        if (optPriority.length() && !isValidPriorityValue(optPriority))
        {
            fprintf(stderr, "invalid --priority value of %s.\n\n", optPriority.get());
            return false;
        }
        return true;
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQueryConfigRequest> req = client->createWUQueryConfigRequest();
        req->setTarget(optTargetCluster.get());
        req->setQueryId(optQueryId.get());
        req->setWait(optMsToWait);
        req->setNoReload(optNoReload);

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

        Owned<IClientWUQueryConfigResponse> resp = client->WUQueryConfig(req);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        IArrayOf<IConstWUQueryConfigResult> &results = resp->getResults();
        if (results.length())
        {
            fputs("configured:\n", stdout);
            ForEachItemIn(i, results)
                fprintf(stdout, "   %s\n", results.item(i).getQueryId());
        }
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'queries config' command updates query configuration values.\n"
            "\n"
            "ecl queries config <target> <queryid> [options]\n"
            "\n"
            " Options:\n"
            "   <target>               Name of target queryset containing query\n"
            "   <queryid>              Id of the query to configure\n"
            "   --no-reload            Do not request a reload of the (roxie) cluster\n"
            "   --wait=<ms>            Max time to wait in milliseconds\n"
            "   --timeLimit=<sec>      Value to set for query timeLimit configuration\n"
            "   --warnTimeLimit=<sec>  Value to set for query warnTimeLimit configuration\n"
            "   --memoryLimit=<mem>    Value to set for query memoryLimit configuration\n"
            "                          format <mem> as 500000B, 550K, 100M, 10G, 1T etc.\n"
            "   --priority=<val>       Set the priority for this query. Value can be LOW,\n"
            "                          HIGH, SLA, NONE. NONE will clear current setting.\n"
            "   --comment=<string>     Set the comment associated with this query\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optTargetCluster;
    StringAttr optQueryId;
    StringAttr optMemoryLimit;
    StringAttr optPriority;
    StringAttr optComment;
    unsigned optMsToWait;
    unsigned optTimeLimit;
    unsigned optWarnTimeLimit;
    bool optNoReload;
};

IEclCommand *createEclQueriesCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    if (strieq(cmdname, "list"))
        return new EclCmdQueriesList();
    if (strieq(cmdname, "config"))
        return new EclCmdQueriesConfig();
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
            "      config       update query settings\n"
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
