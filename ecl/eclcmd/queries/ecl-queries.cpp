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
            return false;

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
                for (const char *ch = temp.str(); *ch; ch++)
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
                        fprintf(stderr, "Unrecognized --show flag = %c\n", *ch);
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
                fputs("--show and --inactive should not be used together.\n", stderr);
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
        line.append("  ").append(queryid);
        if (line.length() < 34)
            line.appendN(34 - line.length(), ' ');
        line.append(' ').append(query.getWuid());
        if (query.getComment())
        {
            if (line.length() < 51)
                line.appendN(51 - line.length(), ' ');
            line.append(' ').append(query.getComment());
        }
        fputs(line.append('\n').str(), stdout);
        StringBuffer metaTags;
        if (!query.getTimeLimit_isNull())
            metaTags.append("timeLimit=").append(query.getTimeLimit());
        if (!query.getWarnTimeLimit_isNull())
        {
            if (metaTags.length())
                metaTags.append(", ");
            metaTags.append("warnTimeLimit=").append(query.getWarnTimeLimit());
        }
        if (query.getPriority())
        {
            if (metaTags.length())
                metaTags.append(", ");
            metaTags.append("priority=").append(query.getPriority());
        }
        if (query.getMemoryLimit())
        {
            if (metaTags.length())
                metaTags.append(", ");
            metaTags.append("memLimit=").append(query.getMemoryLimit());
        }
        if (query.getSnapshot())
        {
            if (metaTags.length())
                metaTags.append(", ");
            metaTags.append("snapshot=").append(query.getSnapshot());
        }
        if (metaTags.length())
        {
            fputs("          [", stdout);
            fputs(metaTags.str(), stdout);
            fputs("]\n\n", stdout);
        }
    }

    void outputQueryset(IConstWUQuerySetDetail &qs)
    {
        ActiveQueryMap queryMap(qs);
        if (qs.getQuerySetName())
            fprintf(stdout, "\nTarget: %s\n", qs.getQuerySetName());
        fputs("\n", stdout);
        fputs("Flags Query Id                     WUID             Comment\n", stdout);
        fputs("----- ---------------------------- ---------------- ------------\n", stdout);

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

class EclCmdQueryFiles : public EclCmdCommon
{
public:
    EclCmdQueryFiles()
    {
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return false;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                if (optTarget.isEmpty())
                    optTarget.set(arg);
                else if (optQuery.isEmpty())
                    optQuery.set(arg);
                else
                {
                    fprintf(stderr, "\n%s option not recognized\n", arg);
                    return false;
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
        if (optTarget.isEmpty())
        {
            fputs("Target must be specified.\n", stderr);
            return false;
        }
        if (optTarget.isEmpty())
        {
            fputs("Query must be specified.\n", stderr);
            return false;
        }
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQueryFilesRequest> req = client->createWUQueryFilesRequest();
        req->setTarget(optTarget.get());
        req->setQueryId(optQuery.get());

        Owned<IClientWUQueryFilesResponse> resp = client->WUQueryFiles(req);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else
        {
            IArrayOf<IConstFileUsedByQuery> &files = resp->getFiles();
            if (!files.length())
                fputs("No files used.\n", stdout);
            else
                fputs("Files used:\n", stdout);
            ForEachItemIn(i, files)
            {
                IConstFileUsedByQuery &file = files.item(i);
                StringBuffer line("  ");
                line.append(file.getFileName()).append(", ");
                line.append(file.getFileSize()).append(" bytes, ");
                line.append(file.getNumberOfParts()).append(" part(s)\n");
                fputs(line, stdout);
            }
            fputs("\n", stdout);
        }
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'queries files' command displays a list of the files currently in use by\n"
            "the given query.\n"
            "\n"
            "ecl queries files <target> <query>\n\n"
            " Options:\n"
            "   <target>               Name of target cluster the query is published on\n"
            "   <query>                Name of the query to get a list of files in use by\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optTarget;
    StringAttr optQuery;
};

class EclCmdQueriesCopy : public EclCmdCommon
{
public:
    EclCmdQueriesCopy() : optActivate(false), optNoReload(false), optMsToWait(10000), optDontCopyFiles(false), optOverwrite(false), optAllowForeign(false)
    {
        optTimeLimit = (unsigned) -1;
        optWarnTimeLimit = (unsigned) -1;
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return false;

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
            if (iter.matchFlag(optAllowForeign, ECLOPT_ALLOW_FOREIGN))
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
            fputs("source and target must both be specified.\n", stderr);
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
        req->setAllowForeignFiles(optAllowForeign);

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
            fprintf(stdout, "%s/%s\n\n", optTargetQuerySet.str(), resp->getQueryId());
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
            "   --no-files             Do not copy DFS file information for referenced files\n"
            "   --daliip=<ip>          Remote Dali DFS to use for copying file information\n"
            "                          (only required if remote environment version < 3.8)\n"
            "   --source-process       Process cluster to copy files from\n"
            "   -A, --activate         Activate the new query\n"
            "   --no-reload            Do not request a reload of the (roxie) cluster\n"
            "   -O, --overwrite        Overwrite existing files\n"
            "   --allow-foreign        Do not fail if foreign files are used in query (roxie)\n"
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
    bool optAllowForeign;
};

class EclCmdQueriesCopyQueryset : public EclCmdCommon
{
public:
    EclCmdQueriesCopyQueryset() : optCloneActiveState(false), optAllQueries(false), optDontCopyFiles(false), optOverwrite(false), optAllowForeign(false)
    {
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return false;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                if (optSourceQuerySet.isEmpty())
                    optSourceQuerySet.set(arg);
                else if (optDestQuerySet.isEmpty())
                    optDestQuerySet.set(arg);
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
            if (iter.matchFlag(optCloneActiveState, ECLOPT_CLONE_ACTIVE_STATE))
                continue;
            if (iter.matchFlag(optDontCopyFiles, ECLOPT_DONT_COPY_FILES))
                continue;
            if (iter.matchFlag(optAllQueries, ECLOPT_ALL))
                continue;
            if (iter.matchFlag(optAllowForeign, ECLOPT_ALLOW_FOREIGN))
                continue;
            if (iter.matchFlag(optOverwrite, ECLOPT_OVERWRITE)||iter.matchFlag(optOverwrite, ECLOPT_OVERWRITE_S))
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
        if (optSourceQuerySet.isEmpty() || optDestQuerySet.isEmpty())
        {
            fputs("source and destination querysets must both be specified.\n", stderr);
            return false;
        }
        return true;
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUCopyQuerySetRequest> req = client->createWUCopyQuerySetRequest();
        req->setActiveOnly(!optAllQueries);
        req->setSource(optSourceQuerySet.get());
        req->setTarget(optDestQuerySet.get());
        req->setDfsServer(optDaliIP.get());
        req->setSourceProcess(optSourceProcess);
        req->setCloneActiveState(optCloneActiveState);
        req->setOverwriteDfs(optOverwrite);
        req->setCopyFiles(!optDontCopyFiles);
        req->setAllowForeignFiles(optAllowForeign);

        Owned<IClientWUCopyQuerySetResponse> resp = client->WUCopyQuerySet(req);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        StringArray &copied = resp->getCopiedQueries();
        fputs("Queries copied:\n", stdout);
        if (!copied.length())
            fputs("  none\n\n", stdout);
        else
        {
            ForEachItemIn(i, copied)
                fprintf(stdout, "  %s\n", copied.item(i));
            fputs("\n", stdout);
        }
        StringArray &existing = resp->getExistingQueries();
        fputs("Queries already on destination target:\n", stdout);
        if (!existing.length())
            fputs("  none\n\n", stdout);
        else
        {
            ForEachItemIn(i, existing)
                fprintf(stdout, "  %s\n", existing.item(i));
            fputs("\n", stdout);
        }
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'queries copy-set' command copies a set of queries from one target to another.\n"
            "\n"
            "By default only active queries will be copied.  Use --all to copy all queries.\n"
            "\n"
            "ecl queries copy-set <source_target> <destination_target> [--clone-active-state]\n"

            "ecl queries copy-set roxie1 roxie2\n"
            "ecl queries copy-set //ip:port/roxie1 roxie2 --clone-active-state\n"
            "\n"
            " Options:\n"
            "   <source_target>        Name of local (or path to remote) target cluster to"
            "                          copy queries from\n"
            "   <destination_target>   Target cluster to copy queries to\n"
            "   --all                  Copy both active and inactive queries\n"
            "   --no-files             Do not copy DFS file information for referenced files\n"
            "   --daliip=<ip>          Remote Dali DFS to use for copying file information\n"
            "   --source-process       Process cluster to copy files from\n"
            "   --clone-active-state   Make copied queries active if active on source\n"
            "   -O, --overwrite        Overwrite existing DFS file information\n"
            "   --allow-foreign        Do not fail if foreign files are used in query (roxie)\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optSourceQuerySet;
    StringAttr optDestQuerySet;
    StringAttr optDaliIP;
    StringAttr optSourceProcess;
    bool optCloneActiveState;
    bool optOverwrite;
    bool optDontCopyFiles;
    bool optAllowForeign;
    bool optAllQueries;
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
            return false;

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
            fputs("Target and QueryId must both be specified.\n", stderr);
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
    if (strieq(cmdname, "files"))
        return new EclCmdQueryFiles();
    if (strieq(cmdname, "config"))
        return new EclCmdQueriesConfig();
    if (strieq(cmdname, "copy"))
        return new EclCmdQueriesCopy();
    if (strieq(cmdname, "copy-set"))
        return new EclCmdQueriesCopyQueryset();
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
            "      list         list queries on target cluster(s)\n"
            "      files        list the files currently used by a query\n"
            "      config       update query settings\n"
            "      copy         copy a query from one target cluster to another\n"
            "      copy-set     copy queries from one target cluster to another\n"
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
