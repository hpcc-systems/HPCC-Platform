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

IClientWsPackageProcess *getWsPackageSoapService(const char *server, const char *port, const char *username, const char *password)
{
    if(server == NULL)
        throw MakeStringException(-1, "Server url not specified");

    VStringBuffer url("http://%s:%s/WsPackageProcess", server, port);

    IClientWsPackageProcess *packageProcessClient = createWsPackageProcessClient();
    packageProcessClient->addServiceUrl(url.str());
    packageProcessClient->setUsernameToken(username, password, NULL);

    return packageProcessClient;
}


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
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);

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
                    "ecl package activate <target> <packagemap>\n"
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
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);

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
                    "ecl package deactivate <target> <packagemap>\n"
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
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optTarget.isEmpty())
        {
            fprintf(stderr, "\nTarget cluster must be specified\n");
            usage();
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);

        Owned<IClientListPackageRequest> request = packageProcessClient->createListPackageRequest();
        request->setTarget(optTarget);

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
                const char *id = req.getId();
                printf("\nPackage Name = %s\n", id);
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
                    "ecl package list <target> \n"
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
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        if (optTarget.isEmpty())
        {
            fprintf(stderr, "\nTarget cluster must be specified\n");
            usage();
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);

        Owned<IClientGetPackageRequest> request = packageProcessClient->createGetPackageRequest();
        request->setTarget(optTarget);

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
                    "ecl package info <target> \n"
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
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);

        fprintf(stdout, "\n ... deleting package map %s now\n\n", optPackageMap.sget());

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
                    "ecl package delete <target> <packagemap>\n"
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
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);
        StringBuffer pkgInfo;
        pkgInfo.loadFile(optFileName);

        fprintf(stdout, "\n ... adding package map %s now\n\n", optFileName.sget());

        Owned<IClientAddPackageRequest> request = packageProcessClient->createAddPackageRequest();
        request->setActivate(optActivate);
        request->setInfo(pkgInfo);
        request->setTarget(optTarget);
        request->setPackageMap(optFileName);
        request->setProcess(optProcess);

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
                    "ecl package add [options] <target> <filename>\n"
                    " Options:\n"
                    "   -O, --overwrite             overwrite existing information\n"
                    "   -A, --activate              activate the package information\n"
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

};



class EclCmdPackageCopyFiles : public EclCmdCommon
{
public:
    EclCmdPackageCopyFiles() :optOverWrite (false)
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
                if (optFileName.isEmpty())
                    optFileName.set(arg);
                else
                {
                    fprintf(stderr, "\nargument is already defined %s\n", arg);
                    return false;
                }
                continue;
            }
            if (iter.matchOption(optDaliIp, ECLOPT_DALIIP))
                continue;
            if (iter.matchFlag(optOverWrite, ECLOPT_OVERWRITE))
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
            err.append("\n ... Specify a process name\n\n");

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
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);
        StringBuffer pkgInfo;
        pkgInfo.loadFile(optFileName);

        fprintf(stdout, "\n ... looking up files in package to see what needs copying\n\n");

        Owned<IClientCopyFilesRequest> request = packageProcessClient->createCopyFilesRequest();
        request->setInfo(pkgInfo);
        request->setTarget(optTarget);
        request->setPackageName(optFileName);
        request->setOverWrite(optOverWrite);
        if (!optDaliIp.isEmpty())
            request->setDaliIp(optDaliIp.get());

        Owned<IClientCopyFilesResponse> resp = packageProcessClient->CopyFiles(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'copyFiles' command will copy any file listed in the package that is not currently \n"
                    "known on the cluster.  This will NOT load the package information \n"
                    "\n"
                    "ecl package copyFiles [options] <target> <filename>\n"
                    " Options:\n"
                    "   -O, --overwrite             overwrite existing information\n"
                    "  --daliip=<daliip>            ip of the source dali to use for file lookups\n"
                    "   <target>                    name of target to use when adding package information\n"
                    "   <filename>                  name of file containing package information\n",
                    stdout);

        EclCmdCommon::usage();
    }
private:
    StringAttr optFileName;
    StringAttr optTarget;
    StringAttr optDaliIp;
    StringBuffer pkgInfo;
    bool optOverWrite;
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
    if (strieq(cmdname, "copyFiles"))
        return new EclCmdPackageCopyFiles();
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
            "ecl package <command> [command options]\n\n"
            "   package Commands:\n"
            "      add          add a package map to the environment\n"
            "      copyFiles    copy missing data files to the appropriate cluster\n"
            "      delete       delete a packag emap\n"
            "      activate     activate a package map\n"
            "      deactivate   deactivate a package map (package map will not get loaded)\n"
            "      list         list loaded package map names\n"
            "      info         return active package map information for a cluster\n"
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
