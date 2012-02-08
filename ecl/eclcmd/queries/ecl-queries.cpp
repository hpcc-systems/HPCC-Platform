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
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER) || iter.matchOption(optCluster, ECLOPT_CLUSTER_S))
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
        if (flags)
        {
            if (isActive && !(flags & QUERYLIST_SHOW_ACTIVE))
                return;
            if (query.getSuspended() && !(flags & QUERYLIST_SHOW_SUSPENDED))
                return;
            if (!isActive && !query.getSuspended() &&  !(flags & QUERYLIST_SHOW_UNFLAGGED))
                return;
        }
        VStringBuffer line("  %c%c  ", query.getSuspended() ? 'S' : ' ', isActive ? 'A' : ' ');
        line.append(queryid);
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
        req->setClusterName(optCluster.get());
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
        fprintf(stdout,"\nUsage:\n\n"
            "ecl queries list [<queryset>][--cluster=<cluster>][--show=<flags>]\n\n"
            " Options:\n"
            "   <queryset>             name of queryset to get list of queries for\n"
            "   -cl, --cluster=<name>  name of cluster to get list of published queries for\n"
            "   --show=<flags>         show only queries with matching flags\n"
            " Flags:\n"
            "   A                      query is active\n"
            "   S                      query is suspended in queryset\n"
//not yet   "   X                      query is suspended on selected cluster\n"
            "   U                      query with no flags set\n"
            " Common Options:\n"
        );
        EclCmdCommon::usage();
    }
private:
    StringAttr optCluster;
    StringAttr optQuerySet;
    unsigned flags;
};

IEclCommand *createEclQueriesCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    if (strieq(cmdname, "list"))
        return new EclCmdQueriesList();
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
