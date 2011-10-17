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

        bool boolValue;
        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                optObj.set(arg);
                break;
            }
            else if (EclCmdCommon::matchCommandLineOption(iter))
                continue;
            else if (iter.matchOption(optCluster, ECLOPT_CLUSTER))
                continue;
            else if (iter.matchOption(optName, ECLOPT_NAME))
                continue;
            else if (iter.matchFlag(boolValue, ECLOPT_VERSION))
            {
                fprintf(stdout, "\necl delploy version %s\n\n", BUILD_TAG);
                return false;
            }
        }
        return true;
    }
    virtual void finalizeOptions(IProperties *globals)
    {
        EclCmdCommon::finalizeOptions(globals);
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
            "ecl deploy [options][<archive>|<eclfile>|<so>|<dll>]\n\n"
            "   Options:\n"
            "      --cluster=<cluster>  cluster to associate workunit with\n"
            "      --name=<name>        workunit job name\n\n"
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

        bool boolValue;
        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                optWuid.set(arg);
                break;
            }
            else if (EclCmdCommon::matchCommandLineOption(iter))
                continue;
            else if (iter.matchOption(optWuid, ECLOPT_WUID))
                continue;
            else if (iter.matchOption(optName, ECLOPT_NAME))
                continue;
            else if (iter.matchOption(optCluster, ECLOPT_CLUSTER))
                continue;
            else if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE))
            {
                activateSet=true;
                continue;
            }
            else if (iter.matchFlag(boolValue, ECLOPT_VERSION))
            {
                fprintf(stdout, "\necl publish version %s\n\n", BUILD_TAG);
                return false;
            }
        }
        return true;
    }
    virtual void finalizeOptions(IProperties *globals)
    {
        EclCmdCommon::finalizeOptions(globals);
        if (!activateSet)
            extractOption(optActivate, globals, ECLOPT_ACTIVATE_ENV, ECLOPT_ACTIVATE_INI, false);
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
            "ecl publish [options][<wuid>]\n\n"
            "   Options:\n"
            "      --cluster=<cluster>  cluster to publish workunit to\n"
            "                           (defaults to cluster defined inside workunit)\n"
            "      --name=<name>        query name to use for published workunit\n"
            "      --wuid=<wuid>        workunit id to publish\n"
            "      --activate           activates query when published\n\n"
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
