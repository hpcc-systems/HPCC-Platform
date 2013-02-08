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

#include "ws_packageprocess_esp.ipp"

#include "eclcmd.hpp"
#include "eclcmd_common.hpp"
#include "eclcmd_core.hpp"

//=========================================================================================

class EclCmdPackageActivate : public EclCmdCommon
{
public:
    EclCmdPackageActivate()
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
                if (!optTarget.length())
                    optTarget.set(arg);
                else if (!optPackageMap.length())
                    optPackageMap.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
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
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optTarget.isEmpty())
        {
            fprintf(stdout, "\n ... Missing target name\n\n");
            usage();
            return false;
        }
        if (optPackageMap.isEmpty())
        {
            fprintf(stdout, "\n ... Missing package map name\n\n");
            usage();
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientActivatePackageRequest> request = packageProcessClient->createActivatePackageRequest();
        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);

        Owned<IClientActivatePackageResponse> resp = packageProcessClient->ActivatePackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'activate' command will deactivate the currently activate packagemap \n"
                    "and make the specified packagemap the one that is used.\n"
                    "\n"
                    "ecl packagemap activate <target> <packagemap>\n"
                    " Options:\n"
                    "   <target>               name of target containing package map to activate\n"
                    "   <packagemap>           packagemap to activate\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:

    StringAttr optTarget;
    StringAttr optPackageMap;
};

class EclCmdPackageDeActivate : public EclCmdCommon
{
public:
    EclCmdPackageDeActivate()
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
                if (!optTarget.length())
                    optTarget.set(arg);
                else if (!optPackageMap.length())
                    optPackageMap.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
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
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optTarget.isEmpty())
        {
            fprintf(stdout, "\n ... Missing target name\n\n");
            usage();
            return false;
        }
        if (optPackageMap.isEmpty())
        {
            fprintf(stdout, "\n ... Missing package map name\n\n");
            usage();
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientDeActivatePackageRequest> request = packageProcessClient->createDeActivatePackageRequest();
        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);

        Owned<IClientDeActivatePackageResponse> resp = packageProcessClient->DeActivatePackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'deactivate' command will deactivate the currently activate packagemap \n"
                    "\n"
                    "ecl packagemap deactivate <target> <packagemap>\n"
                    " Options:\n"
                    "   <target>               name of target containing package map to activate\n"
                    "   <packagemap>           packagemap to activate\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:

    StringAttr optTarget;
    StringAttr optPackageMap;
};

class EclCmdPackageList : public EclCmdCommon
{
public:
    EclCmdPackageList()
    {
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
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
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientListPackageRequest> request = packageProcessClient->createListPackageRequest();
        request->setTarget(optTarget);
        request->setProcess("*");

        Owned<IClientListPackageResponse> resp = packageProcessClient->ListPackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else
        {
            IArrayOf<IConstPackageListMapData> &pkgMapInfo = resp->getPkgListMapData();
            unsigned int num = pkgMapInfo.ordinality();

            for (unsigned i=0; i<num; i++)
            {
                IConstPackageListMapData& req = pkgMapInfo.item(i);
                printf("\nPackage Name = %s  active = %d\n", req.getId(), req.getActive());
                IArrayOf<IConstPackageListData> &pkgInfo = req.getPkgListData();

                unsigned int numPkgs = pkgInfo.ordinality();
                for (unsigned int j = 0; j <numPkgs; j++)
                {
                    IConstPackageListData& req = pkgInfo.item(j);
                    const char *id = req.getId();
                    const char *queries = req.getQueries();
                    if (queries && *queries)
                        printf("\t\tid = %s  queries = %s\n", id, queries);
                    else
                        printf("\t\tid = %s\n", id);
                }
            }
        }
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'list' command will list package map information for the target cluster \n"
                    "\n"
                    "ecl packagemap list <target> \n"
                    " Options:\n"
                    "   <target>               name of target containing package map to use when retrieve list of package maps\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:

    StringAttr optTarget;
};

class EclCmdPackageInfo: public EclCmdCommon
{
public:
    EclCmdPackageInfo()
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
                if (optTarget.isEmpty())
                    optTarget.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
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
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientGetPackageRequest> request = packageProcessClient->createGetPackageRequest();
        request->setTarget(optTarget);
        request->setProcess("*");

        Owned<IClientGetPackageResponse> resp = packageProcessClient->GetPackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else
            printf("%s", resp->getInfo());
        return 0;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'info' command will return the contents of the active package map information for the target cluster \n"
                    "\n"
                    "ecl packagemap info <target> \n"
                    " Options:\n"
                    "   <target>               name of the target to use when retrieving active package map information\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:

    StringAttr optTarget;
};

class EclCmdPackageDelete : public EclCmdCommon
{
public:
    EclCmdPackageDelete()
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
                if (!optTarget.length())
                    optTarget.set(arg);
                else if (!optPackageMap.length())
                    optPackageMap.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
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
        if (!EclCmdCommon::finalizeOptions(globals))
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optPackageMap.isEmpty())
            err.append("\n ... Missing package map name\n\n");
        else if (optTarget.isEmpty())
            err.append("\n ... Specify a target cluster name\n\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            usage();
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        fprintf(stdout, "\n ... deleting package map %s now\n\n", optPackageMap.sget());

        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientDeletePackageRequest> request = packageProcessClient->createDeletePackageRequest();
        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);

        Owned<IClientDeletePackageResponse> resp = packageProcessClient->DeletePackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'delete' command will delete the package map from the target cluster \n"
                    "\n"
                    "ecl packagemap delete <target> <packagemap>\n"
                    " Options:\n"
                    "   <target>               name of the target to use \n"
                    "   <packagemap>           name of the package map to delete",
                    stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optPackageMap;
    StringAttr optTarget;
};

class EclCmdPackageAdd : public EclCmdCommon
{
public:
    EclCmdPackageAdd() : optActivate(false), optOverWrite(false)
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
                if (optTarget.isEmpty())
                    optTarget.set(arg);
                else if (optFileName.isEmpty())
                    optFileName.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return false;
                }
                continue;
            }
            if (iter.matchOption(optDaliIP, ECLOPT_DALIIP))
                continue;
            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE)||iter.matchFlag(optActivate, ECLOPT_ACTIVATE_S))
                continue;
            if (iter.matchFlag(optOverWrite, ECLOPT_OVERWRITE)||iter.matchFlag(optOverWrite, ECLOPT_OVERWRITE_S))
                continue;
            if (EclCmdCommon::matchCommandLineOption(iter, true)!=EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optFileName.isEmpty())
            err.append("\n ... Missing package file name\n\n");
        else if (optTarget.isEmpty())
            err.append("\n ... Specify a cluster name\n\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            usage();
            return false;
        }

        if (optProcess.isEmpty())
            optProcess.set("*");

        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        StringBuffer pkgInfo;
        pkgInfo.loadFile(optFileName);

        fprintf(stdout, "\n ... adding package map %s now\n\n", optFileName.sget());

        Owned<IClientAddPackageRequest> request = packageProcessClient->createAddPackageRequest();
        request->setActivate(optActivate);
        request->setInfo(pkgInfo);
        request->setTarget(optTarget);
        request->setPackageMap(optFileName);
        request->setProcess(optProcess);
        request->setDaliIp(optDaliIP);
        request->setOverWrite(optOverWrite);

        Owned<IClientAddPackageResponse> resp = packageProcessClient->AddPackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'add' command will add the package map information to dali \n"
                    "\n"
                    "ecl packagemap add [options] <target> <filename>\n"
                    " Options:\n"
                    "   -O, --overwrite             overwrite existing information\n"
                    "   -A, --activate              activate the package information\n"
                    "   --daliip=<ip>               ip of the remote dali to use for logical file lookups"
// NOT-YET          "  --packageprocessname         if not set use this package process name for all clusters"
                    "   <target>                    name of target to use when adding package map information\n"
                    "   <filename>                  name of file containing package map information\n",
                    stdout);

        EclCmdCommon::usage();
    }
private:
    StringAttr optFileName;
    StringAttr optTarget;
    StringAttr optProcess;
    bool optActivate;
    bool optOverWrite;
    StringBuffer pkgInfo;
    StringAttr optDaliIP;
};


IEclCommand *createPackageSubCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    if (strieq(cmdname, "add"))
        return new EclCmdPackageAdd();
    if (strieq(cmdname, "delete"))
        return new EclCmdPackageDelete();
    if (strieq(cmdname, "activate"))
        return new EclCmdPackageActivate();
    if (strieq(cmdname, "deactivate"))
        return new EclCmdPackageDeActivate();
    if (strieq(cmdname, "info"))
        return new EclCmdPackageInfo();
    if (strieq(cmdname, "list"))
        return new EclCmdPackageList();
    return NULL;
}

//=========================================================================================

class PackageCMDShell : public EclCMDShell
{
public:
    PackageCMDShell(int argc, const char *argv[], EclCommandFactory _factory, const char *_version)
        : EclCMDShell(argc, argv, _factory, _version)
    {
    }

    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl packagemap <command> [command options]\n\n"
            "   packagemap Commands:\n"
            "      add          add a package map to the environment\n"
            "      delete       delete a package map\n"
            "      activate     activate a package map\n"
            "      deactivate   deactivate a package map (package map will not get loaded)\n"
            "      list         list loaded package map names\n"
            "      info         return active package map information\n"
        );
    }
};

static int doMain(int argc, const char *argv[])
{
    PackageCMDShell processor(argc, argv, createPackageSubCommand, BUILD_TAG);
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
