/*##############################################################################
Copyright (C) 2011 HPCC Systems.
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

#define ECLOPT_PACKAGEMAP "--packagemap"
#define ECLOPT_OVERWRITE "--overwrite"
#define ECLOPT_PACKAGE "--packagename"
#define ECLOPT_DALIIP "--daliip"
#define ECLOPT_PROCESS "--process"

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
                optQuerySetName.set(arg);
                return true;
            }
            if (iter.matchOption(optPackageMap, ECLOPT_PACKAGEMAP))
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
        if (optQuerySetName.isEmpty())
        {
            fprintf(stdout, "\n ... Missing query set name\n\n");
            usage();
            return false;
        }

        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);

        Owned<IClientActivatePackageRequest> request = packageProcessClient->createActivatePackageRequest();
        request->setPackageName(optQuerySetName);
        request->setPackageMapName(optPackageMap);

        Owned<IClientActivatePackageResponse> resp = packageProcessClient->ActivatePackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl package activate [options] [<querySetName>]\n\n"
            "   Options:\n"
            "      --packagemap=<packagemap>        name of packagemap to update\n"
        );
        EclCmdCommon::usage();
    }
private:

    StringAttr optQuerySetName;
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
                optQuerySetName.set(arg);
                break;
            }
            if (iter.matchOption(optPackageMap, ECLOPT_PACKAGEMAP))
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
        if (optQuerySetName.isEmpty())
        {
            fprintf(stdout, "\n ... Missing query set name\n\n");
            usage();
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);

        Owned<IClientDeActivatePackageRequest> request = packageProcessClient->createDeActivatePackageRequest();
        request->setPackageName(optQuerySetName);
        request->setPackageMapName(optPackageMap);

        Owned<IClientDeActivatePackageResponse> resp = packageProcessClient->DeActivatePackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl package deactivate [options] [<querySetName>]\n\n"
            "   Options:\n"
            "      --packagemap=<packagemap>        name of packagemap to update\n"
        );
        EclCmdCommon::usage();
    }
private:

    StringAttr optQuerySetName;
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
                optCluster.set(arg);
                return true;
            }
            if (iter.matchOption(optCluster, ECLOPT_CLUSTER))
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
        if (optCluster.isEmpty())
        {
            fprintf(stderr, "\nCluster must be specified\n");
            usage();
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);

        Owned<IClientListPackageRequest> request = packageProcessClient->createListPackageRequest();
        request->setCluster(optCluster);

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
        fprintf(stdout,"\nUsage:\n\n"
            "ecl package list [options] \n\n"
            "   Options:\n"
            "      [--cluster<cluster>   name of cluster to retrieve package information.  Defaults to all package information stored in dali\n"
        );
        EclCmdCommon::usage();
    }
private:

    StringAttr optCluster;
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
                optPkgName.set(arg);
                return true;
            }
            else if (iter.matchOption(optCluster, ECLOPT_CLUSTER))
                continue;
            else if (iter.matchOption(optPkgName, ECLOPT_PACKAGE))
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
        if (!optPkgName.isEmpty() && !optCluster.isEmpty())
            err.append("\n ... Specify either a cluster name of a package name, but NOT both\n\n");
        else if (optPkgName.isEmpty() && optCluster.isEmpty())
            err.append("\n ... Specify either a cluster name of a package name\n\n");

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

        Owned<IClientGetPackageRequest> request = packageProcessClient->createGetPackageRequest();
        request->setPackageName(optPkgName);
        request->setCluster(optCluster);

        Owned<IClientGetPackageResponse> resp = packageProcessClient->GetPackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else
            printf("%s", resp->getInfo());
        return 0;
    }
    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl package info [options] [<packageName>]\n\n"
            "   Options:\n"
            "      [--cluster=<cluster> | --packageName=<packageName>]  specify either a cluster name or a pacakge name to retrieve information\n"
        );
        EclCmdCommon::usage();
    }
private:

    StringAttr optPkgName;
    StringAttr optCluster;
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
                optFileName.set(arg);
                return true;
            }
            if (iter.matchOption(optQuerySet, ECLOPT_QUERYSET))
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
        else if (optQuerySet.isEmpty())
            err.append("\n ... Specify either a cluster name of a package name\n\n");

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

        fprintf(stdout, "\n ... deleting package %s now\n\n", optFileName.sget());

        Owned<IClientDeletePackageRequest> request = packageProcessClient->createDeletePackageRequest();
        request->setQuerySet(optQuerySet);
        request->setPackageName(optFileName);

        Owned<IClientDeletePackageResponse> resp = packageProcessClient->DeletePackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }

    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl package delete [options] [<filename>]\n\n"
            "   Options:\n"
            "      --queryset=<queryset>        name of queryset to associate the information\n"
        );
        EclCmdCommon::usage();
    }
private:
    StringAttr optFileName;
    StringAttr optQuerySet;
};

class EclCmdPackageAdd : public EclCmdCommon
{
public:
    EclCmdPackageAdd() : optActivate(true), optOverWrite(true)
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
                optFileName.set(arg);
                return true;
            }
            if (iter.matchOption(optQuerySet, ECLOPT_QUERYSET))
                continue;
            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE))
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
        else if (optQuerySet.isEmpty())
            err.append("\n ... Specify either a cluster name of a package name\n\n");

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

        fprintf(stdout, "\n ... adding package %s now\n\n", optFileName.sget());

        Owned<IClientAddPackageRequest> request = packageProcessClient->createAddPackageRequest();
        request->setActivate(optActivate);
        request->setInfo(pkgInfo);
        request->setQuerySet(optQuerySet);
        request->setPackageName(optFileName);
        request->setOverWrite(optOverWrite);

        Owned<IClientAddPackageResponse> resp = packageProcessClient->AddPackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        return 0;
    }

    virtual void usage()
    {
        fprintf(stdout,"\nUsage:\n\n"
            "ecl package add [options] [<filename>]\n\n"
            "   Options:\n"
            "      --queryset=<queryset>        name of queryset to associate the information\n"
            "      --overwritename=<true/false> overwrite existing information - defaults to true\n"
            "      --activate=<true/false>      activate the package information - defaults to true\n"
        );
        EclCmdCommon::usage();
    }
private:
    StringAttr optFileName;
    StringAttr optQuerySet;
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
                optFileName.set(arg);
                return true;
            }
            if (iter.matchOption(optProcess, ECLOPT_PROCESS))
                continue;
            if (iter.matchFlag(optDaliIp, ECLOPT_DALIIP))
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
        else if (optProcess.isEmpty())
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
        request->setProcess(optProcess);
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
        fprintf(stdout,"\nUsage:\n\n"
            "ecl package copyFiles [options] [<filename>]\n\n"
            "   Options:\n"
            "      --process=<processcluster>    name of the process cluster to copy files\n"
            "      --overwrite=<true/false>      overwrite data file if it already exists on the target cluster. defaults to false\n"
            "      --daliip=<daliip>            ip of the source dali to use for file lookups\n"
        );
        EclCmdCommon::usage();
    }
private:
    StringAttr optFileName;
    StringAttr optProcess;
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
            "   Package Commands:\n"
            "      add          add a package to the environment\n"
            "      copyFiles    copy missing data files to the appropriate cluster\n"
            "      delete       delete a package\n"
            "      activate     activate a package\n"
            "      deactivate   deactivate a package (package will not get loaded)\n"
            "      list         list loaded package names\n"
            "      info         return active package information for a cluster\n"
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
