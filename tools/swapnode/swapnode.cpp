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

#include "platform.h"
#include "thirdparty.h"

#include "jlib.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "jprop.hpp"
#include "jmisc.hpp"
#include "jargv.hpp"

#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dalienv.hpp"
#include "rmtfile.hpp"
#include "rmtsmtp.hpp"

#include "dautils.hpp" 
#include "workunit.hpp"

#include "swapnodelib.hpp"

struct DaliClient
{
    DaliClient(const char* daliserver): serverGroup(createIGroup(daliserver, DALI_SERVER_PORT))
    {
        if (!serverGroup)
            throw MakeStringException(0, "Could not instantiate IGroup");

        if (!initClientProcess(serverGroup,DCR_Util))
            throw MakeStringException(0, "Could not initializing client process");
        setPasswordsFromSDS();
    }
    ~DaliClient()
    {
        clearPasswordsFromSDS();
        closedownClientProcess();
    }
    Owned<IGroup> serverGroup;
};

void usage()
{
    fprintf(stderr,"Usage: swapnode swap <dali> <thor-cluster> <oldip> <newip>\n");
    fprintf(stderr,"   or: swapnode auto [-dryrun] <dali> <thor-cluster>\n");
    fprintf(stderr,"   or: swapnode reset <dali> <thor-cluster>                           -- resets group used by <thor-cluster> to match environment\n");
    fprintf(stderr,"   or: swapnode resetspares <dali> <thor-cluster>                     -- resets spare group used by <thor-cluster> to match environment\n");
    fprintf(stderr,"   or: swapnode addspares <dali> <thor-cluster> <spares>              -- adds <spares> to spare group used by <thor-cluster>\n");
    fprintf(stderr,"   or: swapnode removespares <dali> <thor-cluster> <spares>           -- removes <spares> to spare group used by <thor-cluster>\n");
    fprintf(stderr,"   or: swapnode history <dali> <thor-cluster> [<days>]                -- list swap history \n");
    fprintf(stderr,"   or: swapnode history <dali> <thor-cluster> [<days>] 2> outfile.csv -- save swap history \n");
    fprintf(stderr,"   or: swapnode resethistory <dali> <thor-cluster>                    -- reset swap history \n");
    fprintf(stderr,"   or: swapnode swapped <dali> <thor-cluster> [<days>]                -- list currently swapped nodes\n");
    fprintf(stderr,"   or: swapnode email <dali> <thor-cluster> [swapped|history]         -- tests email\n");
    fprintf(stderr,"NB: if '-dryrun' specified after 'auto' then only displays what *would* be swapped\n");
    exit(1);
}

#define SDS_LOCK_TIMEOUT 30000

int main(int argc, const char *argv[])
{
    InitModuleObjects();

    int ret = 0;
    bool dryRun = false;
    bool offline = false;
    StringAttr daliServer, envPath;

    enum CmdType { cmd_none, cmd_swap, cmd_auto, cmd_history, cmd_email, cmd_swapped, cmd_reset, cmd_resetspares, cmd_addspares, cmd_removespares, cmd_resethistory };
    CmdType cmd = cmd_none;

    ArgvIterator iter(argc, argv);

    try
    {
        bool stop=false;
        StringArray params;
        for (; !ret&&!iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if ('-' == *arg)
            {
                bool value;
                if (iter.matchFlag(value, "-dryrun"))
                    dryRun = value;
                else if (iter.matchFlag(value, "-offline"))
                    offline = value;
                else
                {
                    PROGLOG("Unknown option");
                    ret = 2;
                    usage();
                    break;
                }
            }
            else
            {
                switch (cmd)
                {
                    case cmd_none:
                        if (strieq("swap", arg))
                            cmd = cmd_swap;
                        else if (strieq("auto", arg))
                            cmd = cmd_auto;
                        else if (strieq("history", arg))
                            cmd = cmd_history;
                        else if (strieq("email", arg))
                            cmd = cmd_email;
                        else if (strieq("swapped", arg))
                            cmd = cmd_swapped;
                        else if (strieq("reset", arg))
                            cmd = cmd_reset;
                        else if (strieq("resetspares", arg))
                            cmd = cmd_resetspares;
                        else if (strieq("addspares", arg))
                            cmd = cmd_addspares;
                        else if (strieq("removespares", arg))
                            cmd = cmd_removespares;
                        else if (strieq("resethistory", arg))
                            cmd = cmd_resethistory;
                        else
                        {
                            PROGLOG("Unknown command");
                            usage();
                            ret = 2;
                        }
                        break;
                    default:
                        params.append(iter.query());
                        break;
                }
            }
        }
        unsigned requiredParams=UINT_MAX;
        switch (cmd)
        {
            case cmd_swap:
                requiredParams = 4;
                break;
            case cmd_addspares:
            case cmd_removespares:
                requiredParams = 3;
                break;
            case cmd_auto:
            case cmd_history:
            case cmd_email:
            case cmd_swapped:
            case cmd_reset:
            case cmd_resetspares:
            case cmd_resethistory:
                requiredParams = 2;
                break;
        }
        if (params.ordinality() < requiredParams)
        {
            usage();
            ret = 2;
        }
        else
        {
            StringAttr daliServer = params.item(0);
            StringAttr clusterName = params.item(1);

            DaliClient dclient(daliServer);
            StringBuffer logname;
            splitFilename(argv[0], NULL, NULL, &logname, NULL);
            addFileTimestamp(logname, true);
            logname.append(".log");
            StringBuffer lf;
            openLogFile(lf, logname.str(),0,false,true);
            queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);

            Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
            IPropertyTree *environment = conn->queryRoot();
            StringBuffer xpath("Software/ThorCluster[@name=\"");
            xpath.append(clusterName).append("\"]");
            IPropertyTree *cluster = environment->queryPropTree(xpath.str());
            if (!cluster)
            {
                PROGLOG("Unknown cluster: %s", clusterName.get());
                ret = 3;
            }
            if (!ret)
            {
                Owned<IPropertyTree> options = createPTreeFromIPT(cluster);
                conn.clear();
                if (options&&options->getPropBool("@enableSysLog",true))
                    UseSysLogForOperatorMessages();

                switch (cmd)
                {
                    case cmd_auto:
                    {
                        if (!autoSwapNode(clusterName, dryRun))
                            ret = 3;
                        break;
                    }
                    case cmd_swap:
                    {
                        const char *oldip=params.item(2);
                        const char *newip=params.item(3);
                        if (!swapNode(clusterName, oldip, newip))
                            ret = 3;
                        break;
                    }
                    case cmd_history:
                    case cmd_swapped:
                    case cmd_email:
                    {
                        unsigned days = params.isItem(2) ? atoi(params.item(2)) : 0; // for history or swapped
                        switch (cmd)
                        {
                            case cmd_history:
                                swapNodeHistory(clusterName, days, NULL);
                                break;
                            case cmd_swapped:
                                swappedList(clusterName, days, NULL);
                                break;
                            case cmd_email:
                            {
                                bool sendSwapped = false;
                                bool sendHistory = false;
                                if (params.isItem(2))
                                {
                                    if (strieq("swapped", params.item(2)))
                                        sendSwapped = true;
                                    else if (strieq("history", params.item(2)))
                                        sendHistory = true;
                                }
                                emailSwap(clusterName, NULL, true, sendSwapped, sendHistory);
                                break;
                            }
                        }
                        break;
                    }
                    case cmd_reset:
                    case cmd_resetspares:
                    {
                        StringBuffer response;
                        if (!resetClusterGroup(clusterName, "ThorCluster", cmd==cmd_resetspares, response))
                        {
                            WARNLOG("%s", response.str());
                            ret = 3;
                        }
                        break;
                    }
                    case cmd_addspares:
                    case cmd_removespares:
                    {
                        SocketEndpointArray allEps;
                        unsigned p=2;
                        do
                        {
                            const char *ipOrRange = params.item(p);
                            SocketEndpointArray epa;
                            epa.fromText(ipOrRange, 0);
                            ForEachItemIn(e, epa)
                                allEps.append(epa.item(e));
                            p++;
                        }
                        while (p<params.ordinality());
                        StringBuffer response;
                        bool res;
                        if (cmd == cmd_addspares)
                            res = addClusterSpares(clusterName, "ThorCluster", allEps, response);
                        else
                            res = removeClusterSpares(clusterName, "ThorCluster", allEps, response);
                        if (!res)
                        {
                            WARNLOG("%s", response.str());
                            ret = 3;
                        }
                        break;
                    }
                    case cmd_resethistory:
                    {
                        Owned<IRemoteConnection> conn = querySDS().connect("/SwapNode", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
                        if (conn)
                        {
                            StringBuffer groupName;
                            getClusterGroupName(*options, groupName);
                            VStringBuffer xpath("Thor[@group=\"%s\"]", groupName.str());
                            if (conn->queryRoot()->removeProp(xpath.str()))
                                PROGLOG("SwapNode info for cluster %s removed", clusterName.get());
                            else
                                PROGLOG("SwapNode info for cluster %s not found", clusterName.get());
                        }
                        break;
                    }
                }
            }
        }
        UseSysLogForOperatorMessages(false);
    }
    catch (IException *e) {
        EXCLOG(e,"SWAPNODE");
        e->Release();
        ret = -1;
    }

    ExitModuleObjects();
    return ret;
}
