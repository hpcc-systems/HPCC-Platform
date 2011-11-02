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
            switch (EclCmdCommon::matchCommandLineOption(iter))
            {
                case EclCmdOptionNoMatch:
                    fprintf(stderr, "\n%s option not recognized\n", arg);
                    return false;
                case EclCmdOptionCompletion:
                    return false;
                case EclCmdOptionMatch:
                    break;
            }
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
        StringBuffer s;
        if (optObj.type==eclObjTypeUnknown)
            fprintf(stderr, "\nCan't determine content type of argument %s\n", optObj.value.sget());
        else if (optObj.type==eclObjSource)
            fprintf(stderr, "\nDirect deployment of ECL source is not yet supported\n");
        else if (optObj.type==eclObjWuid || optObj.type==eclObjQueryId)
            fprintf(stderr, "\nRemote objects already deployed %s\n", optObj.getDescription(s).str());
        else
        {
            fprintf(stdout, "\nDeploying %s\n", optObj.getDescription(s).str());
            Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
            VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
            client->addServiceUrl(url.str());
            if (optUsername.length())
                client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);
            Owned<IClientWUDeployWorkunitRequest> req = client->createWUDeployWorkunitRequest();
            switch (optObj.type)
            {
                case eclObjArchive:
                {
                    req->setObjType("archive");
                    break;
                }
                case eclObjSharedObject:
                {
                    req->setObjType("shared_object");
                    break;
                }
            }
            MemoryBuffer mb;
            Owned<IFile> file = createIFile(optObj.value.sget());
            Owned<IFileIO> io = file->open(IFOread);
            read(io, 0, (size32_t)file->size(), mb);
            if (optName.length())
                req->setName(optName.get());
            if (optCluster.length())
                req->setCluster(optCluster.get());
            req->setFileName(optObj.value.sget());
            req->setObject(mb);
            Owned<IClientWUDeployWorkunitResponse> resp = client->WUDeployWorkunit(req);
            if (resp->getExceptions().ordinality())
                outputMultiExceptions(resp->getExceptions());
            const char *wuid = resp->getWorkunit().getWuid();
            if (wuid && *wuid)
            {
                fprintf(stdout, "\nDeployed\nwuid: %s\nstate: %s\n", wuid, resp->getWorkunit().getState());
            }
        }
        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl deploy --cluster=<cluster> --name=<name> <archive>\n"
            "ecl deploy [--cluster=<cluster>] [--name=<name>] <so|dll>\n\n"
            "   Options:\n"
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
    EclCmdPublish() : optActivate(false), activateSet(false)
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
                if (optWuid.length())
                {
                    fprintf(stderr, "\nmultiple targets (%s and %s) not currently supported\n", optWuid.sget(), arg);
                    return false;
                }
                optWuid.set(arg);
                continue;
            }
            if (iter.matchOption(optWuid, ECLOPT_WUID))
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
            switch (EclCmdCommon::matchCommandLineOption(iter))
            {
                case EclCmdOptionNoMatch:
                    fprintf(stderr, "\n%s option not recognized\n", arg);
                    return false;
                case EclCmdOptionCompletion:
                    return false;
                case EclCmdOptionMatch:
                    break;
            }
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (!activateSet)
            extractEclCmdOption(optActivate, globals, ECLOPT_ACTIVATE_ENV, ECLOPT_ACTIVATE_INI, false);
        if (optWuid.isEmpty())
        {
            fprintf(stderr, "\nMust specify a WUID to publish\n");
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        StringBuffer s;
        if (!optWuid.length())
        {
            fprintf(stderr, "\nError: wuid parameter required\n");
            return 1;
        }

        fprintf(stdout, "\nPublishing %s\n", optWuid.get());
        Owned<IClientWsWorkunits> client = createWsWorkunitsClient();
        VStringBuffer url("http://%s:%s/WsWorkunits", optServer.sget(), optPort.sget());
        client->addServiceUrl(url.str());
        if (optUsername.length())
            client->setUsernameToken(optUsername.get(), optPassword.sget(), NULL);

        Owned<IClientWUPublishWorkunitRequest> req = client->createWUPublishWorkunitRequest();
        req->setWuid(optWuid.get());
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
            "ecl publish [--cluster=<cluster>][--name=<name>][--activate] <wuid>\n\n"
            "   Options:\n"
            "      --cluster=<cluster>  cluster to publish workunit to\n"
            "                           (defaults to cluster defined inside workunit)\n"
            "      --name=<name>        query name to use for published workunit\n"
            "      --activate           activates query when published\n"
        );
        EclCmdCommon::usage();
    }
private:
    StringAttr optCluster;
    StringAttr optWuid;
    StringAttr optName;
    bool optActivate;
    bool activateSet;
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
    return NULL;
}
