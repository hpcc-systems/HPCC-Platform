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

#include "ws_smc.hpp"

#include "eclcmd.hpp"
#include "eclcmd_common.hpp"
#include "eclcmd_core.hpp"

#define INIFILE "ecl.ini"
#define SYSTEMCONFDIR CONFIG_DIR
#define DEFAULTINIFILE "ecl.ini"
#define SYSTEMCONFFILE ENV_CONF_FILE

//=========================================================================================

class EclCmdRoxieAttach : public EclCmdCommon
{
public:
    EclCmdRoxieAttach(bool _attach) : optMsToWait(10000), attach(_attach)
    {
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                if (optProcess.isEmpty())
                    optProcess.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return false;
                }
                continue;
            }
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
        if (optProcess.isEmpty())
        {
            fputs("process cluster must be specified.\n\n", stderr);
            return false;
        }
        return true;
    }

    virtual int processCMD()
    {
        Owned<IClientWsSMC> client = createWsSMCClient();
        VStringBuffer url("http://%s:%s/WsSMC", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);

        Owned<IClientRoxieControlCmdRequest> req = client->createRoxieControlCmdRequest();
        req->setWait(optMsToWait);
        req->setProcessCluster(optProcess);
        req->setCommand(attach ? CRoxieControlCmd_ATTACH : CRoxieControlCmd_DETACH);

        Owned<IClientRoxieControlCmdResponse> resp = client->RoxieControlCmd(req);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        IArrayOf<IConstRoxieControlEndpointInfo> &endpoints = resp->getEndpoints();
        bool failed = false;
        ForEachItemIn(i, endpoints)
        {
            IConstRoxieControlEndpointInfo &ep = endpoints.item(i);
            if (!ep.getStatus() || !strieq(ep.getStatus(), "ok"))
            {
                if (!failed)
                    failed = true;
                fprintf(stderr, "    %s - %s\n", ep.getAddress(), ep.getStatus() ? ep.getStatus() : "Unknown");
            }
            else if (optVerbose)
                fprintf(stdout, "    %s - %s\n", ep.getAddress(), ep.getStatus());
        }
        if (failed)
            fprintf(stderr, "\nOne or more endpoints did not report status 'ok'\n");
        return 0;
    }
    virtual void usage()
    {
        if (attach)
            fputs("\nUsage:\n"
                "\n"
                "The 'roxie attach' command (re)attaches a roxie process cluster to its"
                "DALI allowing changes to the environment or contents of its assigned\n"
                "querysets to take effect.\n"
                "\n"
                "ecl roxie attatch <process_cluster>\n"
                " Options:\n"
                "   <process_cluster>      the roxie process cluster to attach\n",
                stdout);
        else
            fputs("\nUsage:\n"
                "\n"
                "The 'roxie detach' command detaches a roxie process cluster from DALI\n"
                "preventing changes to the environment or contents of its assigned\n"
                "querysets from taking effect.\n"
                "\n"
                "ecl roxie detatch <process_cluster>\n"
                " Options:\n"
                "   <process_cluster>      the roxie process cluster to detach\n",
                stdout);

        fputs("\n"
            "   --wait=<ms>            Max time to wait in milliseconds\n"
            " Common Options:\n",
            stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optProcess;
    unsigned optMsToWait;
    bool attach;
};

IEclCommand *createEclRoxieCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    if (strieq(cmdname, "attach"))
        return new EclCmdRoxieAttach(true);
    if (strieq(cmdname, "detach"))
        return new EclCmdRoxieAttach(false);
    return NULL;
}

//=========================================================================================

class EclRoxieCMDShell : public EclCMDShell
{
public:
    EclRoxieCMDShell(int argc, const char *argv[], EclCommandFactory _factory, const char *_version)
        : EclCMDShell(argc, argv, _factory, _version)
    {
    }

    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl roixe <command> [command options]\n\n"
            "   Queries Commands:\n"
            "      attach         (re)attach a roxie cluster from dali\n"
            "      detach         detach a roxie cluster from dali\n"
        );
    }
};

static int doMain(int argc, const char *argv[])
{
    EclRoxieCMDShell processor(argc, argv, createEclRoxieCommand, BUILD_TAG);
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
