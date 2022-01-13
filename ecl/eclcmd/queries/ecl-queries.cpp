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

#include "ws_workunits.hpp"

#include "eclcmd.hpp"
#include "eclcmd_common.hpp"
#include "eclcmd_core.hpp"
#include "workunit.hpp"

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
    EclCmdQueriesList() : flags(0), optInactive(false), optCheckAllNodes(false)
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
                optTargetCluster.set(arg);
                continue;
            }
            if (iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED)||iter.matchOption(optTargetCluster, ECLOPT_CLUSTER_DEPRECATED_S))
                continue;
            if (iter.matchOption(optTargetCluster, ECLOPT_TARGET)||iter.matchOption(optTargetCluster, ECLOPT_TARGET_S))
                continue;
            if (iter.matchFlag(optInactive, ECLOPT_INACTIVE))
                continue;
            if (iter.matchFlag(optCheckAllNodes, ECLOPT_CHECK_ALL_NODES))
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
                        return EclCmdOptionNoMatch;
                    }
                }
                continue;
            }
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
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
        if (!optCheckAllNodes)
            extractEclCmdOption(optCheckAllNodes, globals, ECLOPT_CHECK_ALL_NODES_ENV, ECLOPT_CHECK_ALL_NODES_INI, false);
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
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        req->setQuerySetName(optTargetCluster.get());
        req->setClusterName(optTargetCluster.get());
        req->setFilterType("All");
        req->setCheckAllNodes(optCheckAllNodes);

        Owned<IClientWUMultiQuerySetDetailsResponse> resp = client->WUMultiQuerysetDetails(req);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret == 0)
        {
            IArrayOf<IConstWUQuerySetDetail> &querysets = resp->getQuerysets();
            ForEachItemIn(i, querysets)
                outputQueryset(querysets.item(i));
        }
        return ret;
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
            "   --check-all-nodes      Check query status on all nodes in the process cluster\n"
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
    bool optCheckAllNodes;
};

class EclCmdQueryFiles : public EclCmdCommon
{
public:
    EclCmdQueryFiles()
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
                if (optTarget.isEmpty())
                    optTarget.set(arg);
                else if (optQuery.isEmpty())
                    optQuery.set(arg);
                else
                {
                    fprintf(stderr, "\n%s option not recognized\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
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
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        req->setTarget(optTarget.get());
        req->setQueryId(optQuery.get());

        Owned<IClientWUQueryFilesResponse> resp = client->WUQueryFiles(req);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret == 0)
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
        return ret;
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
    EclCmdQueriesCopy() : optActivate(false), optNoReload(false), optMsToWait(10000), optDontCopyFiles(false), optOverwrite(false), optAllowForeign(false),
        optUpdateSuperfiles(false), optUpdateCloneFrom(false), optDontAppendCluster(false)
    {
        optTimeLimit = (unsigned) -1;
        optWarnTimeLimit = (unsigned) -1;
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
                if (optSourceQueryPath.isEmpty())
                    optSourceQueryPath.set(arg);
                else if (optTargetCluster.isEmpty())
                    optTargetCluster.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchOption(optDaliIP, ECLOPT_DALIIP))
                continue;
            if (iter.matchOption(optSourceProcess, ECLOPT_SOURCE_PROCESS))
                continue;

            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE)||iter.matchFlag(optActivate, ECLOPT_ACTIVATE_S))
                continue;
            if (iter.matchFlag(optSourceSSL, ECLOPT_SOURCE_SSL))
                continue;
            if (iter.matchFlag(optSourceNoSSL, ECLOPT_SOURCE_NO_SSL))
                continue;
            if (iter.matchFlag(optSuspendPrevious, ECLOPT_SUSPEND_PREVIOUS)||iter.matchFlag(optSuspendPrevious, ECLOPT_SUSPEND_PREVIOUS_S))
                continue;
            if (iter.matchFlag(optDeletePrevious, ECLOPT_DELETE_PREVIOUS)||iter.matchFlag(optDeletePrevious, ECLOPT_DELETE_PREVIOUS_S))
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
            if (iter.matchFlag(optUpdateSuperfiles, ECLOPT_UPDATE_SUPER_FILES))
                continue;
            if (iter.matchFlag(optUpdateCloneFrom, ECLOPT_UPDATE_CLONE_FROM))
                continue;
            if (iter.matchFlag(optDontAppendCluster, ECLOPT_DONT_APPEND_CLUSTER))
                continue;
            if (iter.matchFlag(optUpdateSuperfiles, ECLOPT_UPDATE_SUPER_FILES))
                continue;
            if (iter.matchFlag(optUpdateCloneFrom, ECLOPT_UPDATE_CLONE_FROM))
                continue;
            if (iter.matchFlag(optDontAppendCluster, ECLOPT_DONT_APPEND_CLUSTER))
                continue;
            if (iter.matchOption(optName, ECLOPT_NAME)||iter.matchOption(optName, ECLOPT_NAME_S))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
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
        if (!optActivate && (optSuspendPrevious || optDeletePrevious))
        {
            fputs("invalid --suspend-prev and --delete-prev require --activate.\n", stderr);
            return false;
        }
        if (optSuspendPrevious && optDeletePrevious)
        {
            fputs("invalid --suspend-prev and --delete-prev are mutually exclusive options.\n", stderr);
            return false;
        }

        return true;
    }

    inline bool useSSLForSource()
    {
        if (optSourceSSL)
            return true;
        if (optSourceNoSSL)
            return false;
        return optSSL; //default to whether we use SSL to call ESP
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQuerySetCopyQueryRequest> req = client->createWUQuerysetCopyQueryRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        req->setSource(optSourceQueryPath.get());
        req->setTarget(optTargetCluster.get());
        req->setCluster(optTargetCluster.get());
        req->setDaliServer(optDaliIP.get());
        req->setSourceProcess(optSourceProcess);
        if (optDeletePrevious)
            req->setActivate(CWUQueryActivationMode_ActivateDeletePrevious);
        else if (optSuspendPrevious)
            req->setActivate(CWUQueryActivationMode_ActivateSuspendPrevious);
        else
            req->setActivate(optActivate ? CWUQueryActivationMode_Activate : CWUQueryActivationMode_NoActivate);

        req->setOverwrite(optOverwrite);
        req->setUpdateSuperFiles(optUpdateSuperfiles);
        req->setUpdateCloneFrom(optUpdateCloneFrom);
        req->setAppendCluster(!optDontAppendCluster);
        req->setDontCopyFiles(optDontCopyFiles);
        req->setWait(optMsToWait);
        req->setNoReload(optNoReload);
        req->setAllowForeignFiles(optAllowForeign);
        req->setIncludeFileErrors(true);

        //default to same tcp/tls as our ESP connection, but can be changed using --source-ssl or --source-no-ssl
        req->setSourceSSL(useSSLForSource());

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
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (outputQueryFileCopyErrors(resp->getFileErrors()))
            ret = 1;

        if (resp->getQueryId() && *resp->getQueryId())
            fprintf(stdout, "%s/%s\n\n", optTargetQuerySet.str(), resp->getQueryId());
        return ret;
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
            "   --source-ssl           Use SSL when connecting to source (default if --ssl is used)\n"
            "   --source-no-ssl        Do not use SSL when connecting to source (default if --ssl is NOT used)\n"
            "   --no-files             Do not copy DFS file information for referenced files\n"
            "   --daliip=<ip>          Remote Dali DFS to use for copying file information\n"
            "                          (only required if remote environment version < 3.8)\n"
            "   --source-process       Process cluster to copy files from\n"
            "   -A, --activate         Activate the new query\n"
            "   -sp, --suspend-prev    Suspend previously active query\n"
            "   -dp, --delete-prev     Delete previously active query\n"
            "   --no-reload            Do not request a reload of the (roxie) cluster\n"
            "   -O, --overwrite        Completely replace existing DFS file information (dangerous)\n"
            "   --update-super-files   Update local DFS super-files if remote DALI has changed\n"
            "   --update-clone-from    Update local clone from location if remote DALI has changed\n"
            "   --dont-append-cluster  Only use to avoid locking issues due to adding cluster to file\n"
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
    bool optSuspendPrevious = false;
    bool optDeletePrevious = false;
    bool optNoReload;
    bool optOverwrite;
    bool optUpdateSuperfiles;
    bool optUpdateCloneFrom;
    bool optDontAppendCluster; //Undesirable but here temporarily because DALI may have locking issues
    bool optDontCopyFiles;
    bool optAllowForeign;
    bool optSourceSSL = false; //user explicitly turning on SSL for accessing the remote source location (ssl defaults to use SSL if we are hitting ESP via SSL)
    bool optSourceNoSSL = false; //user explicitly turning OFF SSL for accessing the remote source location (ssl defaults to not use SSL if we are not hitting ESP via SSL)
};

class EclCmdQueriesCopyQueryset : public EclCmdCommon
{
public:
    EclCmdQueriesCopyQueryset() : optCloneActiveState(false), optAllQueries(false), optDontCopyFiles(false), optOverwrite(false), optAllowForeign(false),
        optUpdateSuperfiles(false), optUpdateCloneFrom(false), optDontAppendCluster(false)
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
                if (optSourceQuerySet.isEmpty())
                    optSourceQuerySet.set(arg);
                else if (optDestQuerySet.isEmpty())
                    optDestQuerySet.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchFlag(optSourceSSL, ECLOPT_SOURCE_SSL))
                continue;
            if (iter.matchFlag(optSourceNoSSL, ECLOPT_SOURCE_NO_SSL))
                continue;
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
            if (iter.matchFlag(optUpdateSuperfiles, ECLOPT_UPDATE_SUPER_FILES))
                continue;
            if (iter.matchFlag(optUpdateCloneFrom, ECLOPT_UPDATE_CLONE_FROM))
                continue;
            if (iter.matchFlag(optDontAppendCluster, ECLOPT_DONT_APPEND_CLUSTER))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
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

    inline bool useSSLForSource()
    {
        if (optSourceSSL)
            return true;
        if (optSourceNoSSL)
            return false;
        return optSSL; //default to whether we use SSL to call ESP
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUCopyQuerySetRequest> req = client->createWUCopyQuerySetRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        req->setActiveOnly(!optAllQueries);
        req->setSource(optSourceQuerySet.get());
        req->setTarget(optDestQuerySet.get());
        req->setDfsServer(optDaliIP.get());
        req->setSourceProcess(optSourceProcess);
        req->setCloneActiveState(optCloneActiveState);
        req->setOverwriteDfs(optOverwrite);
        req->setUpdateSuperFiles(optUpdateSuperfiles);
        req->setUpdateCloneFrom(optUpdateCloneFrom);
        req->setAppendCluster(!optDontAppendCluster);
        req->setCopyFiles(!optDontCopyFiles);
        req->setAllowForeignFiles(optAllowForeign);
        req->setIncludeFileErrors(true);

        //default to same tcp/tls as our ESP connection, but can be changed using --source-ssl or --source-no-ssl
        req->setSourceSSL(useSSLForSource());

        Owned<IClientWUCopyQuerySetResponse> resp = client->WUCopyQuerySet(req);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (outputQueryFileCopyErrors(resp->getFileErrors()))
            ret = 1;

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
        return ret;
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
            "   --source-ssl           Use SSL when connecting to source (default if --ssl is used)\n"
            "   --source-no-ssl        Do not use SSL when connecting to source (default if --ssl is NOT used)\n"
            "   --all                  Copy both active and inactive queries\n"
            "   --no-files             Do not copy DFS file information for referenced files\n"
            "   --daliip=<ip>          Remote Dali DFS to use for copying file information\n"
            "   --source-process       Process cluster to copy files from\n"
            "   --clone-active-state   Make copied queries active if active on source\n"
            "   -O, --overwrite        Completely replace existing DFS file information (dangerous)\n"
            "   --update-super-files   Update local DFS super-files if remote DALI has changed\n"
            "   --update-clone-from    Update local clone from location if remote DALI has changed\n"
            "   --dont-append-cluster  Only use to avoid locking issues due to adding cluster to file\n"
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
    bool optUpdateSuperfiles;
    bool optUpdateCloneFrom;
    bool optDontAppendCluster; //Undesirable but here temporarily because DALI may have locking issues
    bool optDontCopyFiles;
    bool optAllowForeign;
    bool optAllQueries;
    bool optSourceSSL = false; //user explicitly turning on SSL for accessing the remote source location (ssl defaults to use SSL if we are hitting ESP via SSL)
    bool optSourceNoSSL = false; //user explicitly turning OFF SSL for accessing the remote source location (ssl defaults to not use SSL if we are not hitting ESP via SSL)
};

class EclCmdQueriesConfig : public EclCmdCommon
{
public:
    EclCmdQueriesConfig() : optNoReload(false), optMsToWait(10000)
    {
        optTimeLimit = (unsigned) -1;
        optWarnTimeLimit = (unsigned) -1;
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
                if (optTargetCluster.isEmpty())
                    optTargetCluster.set(arg);
                else if (optQueryId.isEmpty())
                    optQueryId.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
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
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
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
        setCmdRequestTimeouts(req->rpc(), optMsToWait, optWaitConnectMs, optWaitReadSec);

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
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        IArrayOf<IConstWUQueryConfigResult> &results = resp->getResults();
        if (results.length())
        {
            fputs("configured:\n", stdout);
            ForEachItemIn(i, results)
                fprintf(stdout, "   %s\n", results.item(i).getQueryId());
        }
        return ret;
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

class EclCmdQueriesRecreate : public EclCmdCommon
{
public:
    EclCmdQueriesRecreate()
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
                if (optTarget.isEmpty())
                    optTarget.set(arg);
                else if (optQueryId.isEmpty())
                    optQueryId.set(arg);
                else if (optDestTarget.isEmpty())
                    optDestTarget.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
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
            if (iter.matchFlag(optNoPublish, ECLOPT_NOPUBLISH))
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
            if (iter.matchOption(optResultLimit, ECLOPT_RESULT_LIMIT))
                continue;
            if (matchVariableOption(iter, 'f', debugValues, true))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optTarget.isEmpty())
        {
            fputs("Target not specified.\n", stderr);
            return false;
        }
        if (optQueryId.isEmpty())
        {
            fputs("Query not specified.\n", stderr);
            return false;
        }
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
        if (optVerbose)
            fprintf(stdout, "\nRecreating %s/%s\n", optTarget.str(), optQueryId.str());

        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this); //upload_ disables maxRequestEntityLength
        Owned<IClientWURecreateQueryRequest> req = client->createWURecreateQueryRequest();
        setCmdRequestTimeouts(req->rpc(), optMsToWait, optWaitConnectMs, optWaitReadSec);

        if (optDeletePrevious)
            req->setActivate(CWUQueryActivationMode_ActivateDeletePrevious);
        else if (optSuspendPrevious)
            req->setActivate(CWUQueryActivationMode_ActivateSuspendPrevious);
        else
            req->setActivate(optNoActivate ? CWUQueryActivationMode_NoActivate : CWUQueryActivationMode_Activate);

        req->setTarget(optTarget);
        req->setDestTarget(optDestTarget);
        req->setQueryId(optQueryId);
        req->setRemoteDali(optDaliIP);
        req->setSourceProcess(optSourceProcess);
        req->setWait(optMsToWait);
        req->setNoReload(optNoReload);
        req->setRepublish(!optNoPublish);
        req->setDontCopyFiles(optDontCopyFiles);
        req->setAllowForeignFiles(optAllowForeign);
        req->setUpdateDfs(optUpdateDfs);
        req->setUpdateSuperFiles(optUpdateSuperfiles);
        req->setUpdateCloneFrom(optUpdateCloneFrom);
        req->setAppendCluster(!optDontAppendCluster);
        req->setIncludeFileErrors(true);
        req->setDebugValues(debugValues);

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

        Owned<IClientWURecreateQueryResponse> resp = client->WURecreateQuery(req);
        const char *wuid = resp->getWuid();
        if (wuid && *wuid)
            fprintf(stdout, "\nWorkunit: %s\n", wuid);
        const char *id = resp->getQueryId();
        if (id && *id)
        {
            const char *qs = resp->getQuerySet();
            fprintf(stdout, "\nPublished: %s/%s\n", qs ? qs : "", resp->getQueryId());
        }
        if (resp->getReloadFailed())
            fputs("\nAdded to Target, but request to reload queries on cluster failed\n", stderr);

        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (outputQueryFileCopyErrors(resp->getFileErrors()))
            ret = 1;

        return ret;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'queries recreate' command recompiles a query into a new workunit and republishes\n"
            "the new workunit.  This is usefull when upgrading to a new ECL compiler and you\n"
            "want to recompile a query with the exact same source.\n"
            "\n"
            "The ECL archive must be available within the workunit of the query.\n"
            "\n"
            "ecl queries recreate <target> <query> [options]\n\n"
            "ecl queries recreate <target> <query> <destination-target> [options]\n\n"
            "   <target>               the target the query you wish to recreate is in\n"
            "   <query>                the query ID of the query you wish to recreate\n"
            "   <destination-target>   the target you want to move the new query to\n"
            "                          (if different from the source target)\n"
            " Options:\n"
            "   -A, --activate         Activate query when published (default)\n"
            "   --limit=<limit>        Sets the result limit for the query, defaults to 100\n"
            "   -sp, --suspend-prev    Suspend previously active query\n"
            "   -dp, --delete-prev     Delete previously active query\n"
            "   -A-, --no-activate     Do not activate query when published\n"
            "   --no-publish           Create a recompiled workunit, but do not publish it\n"
            "   --no-reload            Do not request a reload of the (roxie) cluster\n"
            "   --no-files             Do not copy DFS file information for referenced files\n"
            "   --allow-foreign        Do not fail if foreign files are used in query (roxie)\n"
            "   --daliip=<IP>          The IP of the DALI to be used to locate remote files\n"
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
        EclCmdCommon::usage();
    }
private:
    StringAttr optTarget;
    StringAttr optDestTarget;
    StringAttr optQueryId;
    StringAttr optDaliIP;
    StringAttr optSourceProcess;
    StringAttr optMemoryLimit;
    StringAttr optPriority;
    StringAttr optComment;
    IArrayOf<IEspNamedValue> debugValues;
    unsigned optMsToWait = (unsigned) -1;
    unsigned optTimeLimit = (unsigned) -1;
    unsigned optWarnTimeLimit = (unsigned) -1;
    unsigned optResultLimit = (unsigned) -1;
    bool optNoActivate = false;
    bool activateSet = false;
    bool optNoReload = false;
    bool optNoPublish = false;
    bool optDontCopyFiles = false;
    bool optSuspendPrevious = false;
    bool optDeletePrevious = false;
    bool optAllowForeign = false;
    bool optUpdateDfs = false;
    bool optUpdateSuperfiles = false;
    bool optUpdateCloneFrom = false;
    bool optDontAppendCluster = false; //Undesirable but here temporarily because DALI may have locking issues
};

class EclCmdQueriesExport : public EclCmdCommon
{
public:
    EclCmdQueriesExport()
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
                if (optTarget.isEmpty())
                    optTarget.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchOption(optFilename, ECLOPT_OUTPUT) || iter.matchOption(optFilename, ECLOPT_OUTPUT_S))
                continue;
            if (iter.matchFlag(optActiveOnly, ECLOPT_ACTIVE_ONLY))
                continue;
            if (iter.matchFlag(optProtect, ECLOPT_PROTECT))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optTarget.isEmpty())
        {
            fputs("Target must be specified.\n", stderr);
            return false;
        }
        if (optFilename.isEmpty())
        {
            StringBuffer name("./queryset_backup_");
            name.append(optTarget);
            if (optActiveOnly)
                name.append("_activeonly_");
            CDateTime dt;
            dt.setNow();
            dt.getString(name, true);
            name.replace(':', '_').append(".xml");
            optFilename.set(name);
        }
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }

    void saveAsFile(const char *s, const char *filepath)
    {
        if (!s || !*s)
            return;

        Owned<IFile> file = createIFile(filepath);
        Owned<IFileIO> io = file->open(IFOcreaterw);

        fprintf(stdout, "\nWriting to file %s\n", file->queryFilename());

        if (io.get())
            io->write(0, strlen(s), s);
        else
            fprintf(stderr, "\nFailed to create file %s\n", file->queryFilename());
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQuerysetExportRequest> req = client->createWUQuerysetExportRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        req->setTarget(optTarget);
        req->setActiveOnly(optActiveOnly);
        req->setCompress(true);
        req->setProtect(optProtect);

        Owned<IClientWUQuerysetExportResponse> resp = client->WUQuerysetExport(req);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret == 0)
        {
            if (!resp->getData().length())
            {
                fprintf(stderr, "\nEmpty Queryset returned\n");
                return 1;
            }
            MemoryBuffer decompressed;
            fastLZDecompressToBuffer(decompressed, const_cast<MemoryBuffer &>(resp->getData())); //unfortunate need for const_cast
            if (!decompressed.length())
            {
                fprintf(stderr, "\nError decompressing response\n");
                return 1;
            }
            if (optFilename.length())
                saveAsFile(decompressed.toByteArray(), optFilename);
            else
            {
                decompressed.append('\0');
                fputs(decompressed.toByteArray(), stdout); //for piping
                fputs("\n", stdout);
            }
        }
        return ret;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'queries export' command saves backup information about a given queryset.\n"
            "\n"
            "ecl queries export <target> [options]\n\n"
            " Options:\n"
            "   <target>               Name of target cluster to export from\n"
            "   -O,--output=<file>     Filename to save exported backup information to (optional)\n"
            "   --active-only          Only include active queries in the exported queryset\n"
            "   --protect              Protect the workunits for the included queries\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optTarget;
    StringAttr optFilename;
    bool optActiveOnly = false;
    bool optProtect = false;
};

class EclCmdQueriesImport : public EclCmdCommon
{
public:
    EclCmdQueriesImport()
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
                if (optDestQuerySet.isEmpty())
                    optDestQuerySet.set(arg);
                else if (optFilename.isEmpty())
                    optFilename.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchOption(optQueries, ECLOPT_QUERIES))
                continue;
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
            if (iter.matchFlag(optReplace, ECLOPT_REPLACE))
                continue;
            if (iter.matchFlag(optAllowForeign, ECLOPT_ALLOW_FOREIGN))
                continue;
            if (iter.matchFlag(optOverwrite, ECLOPT_OVERWRITE)||iter.matchFlag(optOverwrite, ECLOPT_OVERWRITE_S))
                continue;
            if (iter.matchFlag(optUpdateSuperfiles, ECLOPT_UPDATE_SUPER_FILES))
                continue;
            if (iter.matchFlag(optUpdateCloneFrom, ECLOPT_UPDATE_CLONE_FROM))
                continue;
            if (iter.matchFlag(optDontAppendCluster, ECLOPT_DONT_APPEND_CLUSTER))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optFilename.isEmpty() || optDestQuerySet.isEmpty())
        {
            fputs("Target and file name must both be specified.\n", stderr);
            return false;
        }
        content.loadFile(optFilename, false);
        return true;
    }

    virtual int processCMD()
    {
        Owned<IClientWsWorkunits> client = createCmdClient(WsWorkunits, *this);
        Owned<IClientWUQuerysetImportRequest> req = client->createWUQuerysetImportRequest();
        setCmdRequestTimeouts(req->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        MemoryBuffer compressed;
        fastLZCompressToBuffer(compressed, content.length()+1, content.str());
        req->setCompressed(true);
        req->setData(compressed);

        req->setReplace(optReplace);
        req->setActiveOnly(!optAllQueries);
        req->setQueryMask(optQueries);
        req->setTarget(optDestQuerySet);
        req->setDfsServer(optDaliIP);
        req->setSourceProcess(optSourceProcess);
        req->setActivation(optCloneActiveState ? CQuerysetImportActivation_ImportedActive : CQuerysetImportActivation_None);
        req->setOverwriteDfs(optOverwrite);
        req->setUpdateSuperFiles(optUpdateSuperfiles);
        req->setUpdateCloneFrom(optUpdateCloneFrom);
        req->setAppendCluster(!optDontAppendCluster);
        req->setCopyFiles(!optDontCopyFiles);
        req->setAllowForeignFiles(optAllowForeign);
        req->setIncludeFileErrors(true);

        Owned<IClientWUQuerysetImportResponse> resp = client->WUQuerysetImport(req);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (outputQueryFileCopyErrors(resp->getFileErrors()))
            ret = 1;

        StringArray &imported = resp->getImportedQueries();
        fputs("Queries Imported:\n", stdout);
        if (!imported.length())
            fputs("  none\n\n", stdout);
        else
        {
            ForEachItemIn(i, imported)
                fprintf(stdout, "  %s\n", imported.item(i));
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
        StringArray &missing = resp->getMissingWuids();
        fputs("Missing workunits:\n", stdout);
        if (!missing.length())
            fputs("  none\n\n", stdout);
        else
        {
            ForEachItemIn(i, missing)
                fprintf(stdout, "  %s\n", missing.item(i));
            fputs("\n", stdout);
        }
        return ret;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
            "\n"
            "The 'queries import' command imports the contents of a queryset exported to disk.\n"
            "\n"
            "By default only active queries will be imported.  Use --all to import all queries.\n"
            "\n"
            "ecl queries import <target> <file> [--clone-active-state][--replace]\n"

            "ecl queries import roxie1 queryset.xml\n"
            "\n"
            " Options:\n"
            "   <target>               Target cluster to import queries to\n"
            "   --all                  Copy both active and inactive queries\n"
            "   --replace              Replace entire existing queryset\n"
            "   --queries              Filter query ids to select for import\n"
            "   --no-files             Do not copy DFS file information for referenced files\n"
            "   --daliip=<ip>          Remote Dali DFS to use for copying file information\n"
            "   --source-process       Process cluster to copy files from\n"
            "   --clone-active-state   Make copied queries active if active on source\n"
            "   -O, --overwrite        Completely replace existing DFS file information (dangerous)\n"
            "   --update-super-files   Update local DFS super-files if remote DALI has changed\n"
            "   --update-clone-from    Update local clone from location if remote DALI has changed\n"
            "   --dont-append-cluster  Only use to avoid locking issues due to adding cluster to file\n"
            "   --allow-foreign        Do not fail if foreign files are used in query (roxie)\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringBuffer content;
    StringAttr optFilename;
    StringAttr optQueries;
    StringAttr optDestQuerySet;
    StringAttr optDaliIP;
    StringAttr optSourceProcess;
    bool optReplace = false;
    bool optCloneActiveState = false;
    bool optOverwrite = false;
    bool optUpdateSuperfiles = false;
    bool optUpdateCloneFrom = false;
    bool optDontAppendCluster = false; //Undesirable but here temporarily because DALI may have locking issues
    bool optDontCopyFiles = false;
    bool optAllowForeign = false;
    bool optAllQueries = false;
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
    if (strieq(cmdname, "recreate"))
        return new EclCmdQueriesRecreate();
    if (strieq(cmdname, "export"))
        return new EclCmdQueriesExport();
    if (strieq(cmdname, "import"))
        return new EclCmdQueriesImport();
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
            "      recreate     recompiles query into a new workunit\n"
            "      export       export queryset information for backup\n"
            "      import       import queryset information from backup file\n"
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
