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
    EclCmdPackageActivate() : optGlobalScope(false)
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
                if (!optTarget.length())
                    optTarget.set(arg);
                else if (!optPackageMap.length())
                    optPackageMap.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
            fprintf(stdout, "\n ... Missing target name\n");
            return false;
        }
        if (optPackageMap.isEmpty())
        {
            fprintf(stdout, "\n ... Missing package map name\n");
            return false;
        }
        if (optProcess.isEmpty())
            optProcess.set("*");

        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientActivatePackageRequest> request = packageProcessClient->createActivatePackageRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);
        request->setProcess(optProcess);
        request->setGlobalScope(optGlobalScope);

        Owned<IClientActivatePackageResponse> resp = packageProcessClient->ActivatePackage(request);
        return outputMultiExceptionsEx(resp->getExceptions());
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
                    "   <target>               Name of target containing package map to activate\n"
                    "   <packagemap>           Packagemap to activate\n"
                    "   --global-scope         The specified packagemap can be shared across multiple targets\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:

    StringAttr optTarget;
    StringAttr optPackageMap;
    StringAttr optProcess;
    bool optGlobalScope;
};

class EclCmdPackageDeActivate : public EclCmdCommon
{
public:
    EclCmdPackageDeActivate() : optGlobalScope(false)
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
                if (!optTarget.length())
                    optTarget.set(arg);
                else if (!optPackageMap.length())
                    optPackageMap.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
            fprintf(stdout, "\n ... Missing target name\n");
            return false;
        }
        if (optPackageMap.isEmpty())
        {
            fprintf(stdout, "\n ... Missing package map name\n");
            return false;
        }
        if (optProcess.isEmpty())
            optProcess.set("*");

        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientDeActivatePackageRequest> request = packageProcessClient->createDeActivatePackageRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);
        request->setProcess(optProcess);
        request->setGlobalScope(optGlobalScope);

        Owned<IClientDeActivatePackageResponse> resp = packageProcessClient->DeActivatePackage(request);
        return outputMultiExceptionsEx(resp->getExceptions());
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'deactivate' command will deactivate the currently activate packagemap \n"
                    "\n"
                    "ecl packagemap deactivate <target> <packagemap>\n"
                    " Options:\n"
                    "   <target>               Name of target containing package map to activate\n"
                    "   <packagemap>           Packagemap to activate\n"
                    "   --global-scope         The specified packagemap can be shared across multiple targets\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:

    StringAttr optTarget;
    StringAttr optPackageMap;
    StringAttr optProcess;
    bool optGlobalScope;
};

class EclCmdPackageList : public EclCmdCommon
{
public:
    EclCmdPackageList()
    {
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
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
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientListPackageRequest> request = packageProcessClient->createListPackageRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setProcess("*");

        Owned<IClientListPackageResponse> resp = packageProcessClient->ListPackage(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret == 0)
        {
            IArrayOf<IConstPackageListMapData> &pkgMapInfo = resp->getPkgListMapData();
            unsigned int num = pkgMapInfo.ordinality();

            for (unsigned i=0; i<num; i++)
            {
                IConstPackageListMapData& req = pkgMapInfo.item(i);
                printf("\nPackage Id = %s  active = %d\n", req.getId(), req.getActive());

                IArrayOf<IConstPackageListData> &pkgInfo = req.getPkgListData();
                unsigned int numPkgs = pkgInfo.ordinality();
                for (unsigned int j = 0; j <numPkgs; j++)
                {
                    IConstPackageListData& req = pkgInfo.item(j);
                    const char *id = req.getId();
                    const char *queries = req.getQueries();
                    if (queries && *queries)
                        printf("\t\tname = %s  queries = %s\n", id, queries);
                    else
                        printf("\t\tname = %s\n", id);
                }
            }
        }
        return ret;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'list' command will list package map information for the target cluster \n"
                    "\n"
                    "ecl packagemap list <target> \n"
                    " Options:\n"
                    "   <target>               Name of target containing package map to use when retrieving list of package maps\n",
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
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientGetPackageRequest> request = packageProcessClient->createGetPackageRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setProcess("*");

        Owned<IClientGetPackageResponse> resp = packageProcessClient->GetPackage(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret == 0)
            printf("%s", resp->getInfo());
        return ret;
    }
    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'info' command will return the contents of the active package map information for the target cluster \n"
                    "\n"
                    "ecl packagemap info <target> \n"
                    " Options:\n"
                    "   <target>               Name of the target to use when retrieving active package map information\n",
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
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
            return EclCmdOptionNoMatch;

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
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optPackageMap.isEmpty())
            err.append("\n ... Missing package map name\n");
        else if (optTarget.isEmpty())
            err.append("\n ... Specify a target cluster name\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            return false;
        }

        if (optProcess.isEmpty())
            optProcess.set("*");
        return true;
    }
    virtual int processCMD()
    {
        fprintf(stdout, "\n ... deleting package map %s now\n\n", optPackageMap.str());

        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientDeletePackageRequest> request = packageProcessClient->createDeletePackageRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);
        request->setProcess(optProcess);
        request->setGlobalScope(optGlobalScope);

        Owned<IClientDeletePackageResponse> resp = packageProcessClient->DeletePackage(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret == 0)
            printf("Successfully deleted package %s\n", optPackageMap.get());
        return ret;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'delete' command will delete the package map from the target cluster \n"
                    "\n"
                    "ecl packagemap delete <target> <packagemap>\n"
                    " Options:\n"
                    "   <target>               Name of the target to use \n"
                    "   <packagemap>           Name of the package map to delete\n"
                    "   --global-scope         The specified packagemap is sharable across multiple targets\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optPackageMap;
    StringAttr optTarget;
    StringAttr optProcess;
    bool optGlobalScope;
};

class EclCmdPackageAdd : public EclCmdCommon
{
public:
    EclCmdPackageAdd() : optActivate(false), optOverWrite(false), optGlobalScope(false), optAllowForeign(false), optPreloadAll(false),
        optUpdateSuperfiles(false), optUpdateCloneFrom(false), optDontAppendCluster(false), optReplacePackagemap(false)
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
                else if (optFileName.isEmpty())
                    optFileName.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchOption(optPackageMapId, ECLOPT_PMID))
                continue;
            if (iter.matchOption(optDaliIP, ECLOPT_DALIIP))
                continue;
            if (iter.matchOption(optSourceProcess, ECLOPT_SOURCE_PROCESS))
                continue;
            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE)||iter.matchFlag(optActivate, ECLOPT_ACTIVATE_S))
                continue;
            if (iter.matchFlag(optOverWrite, ECLOPT_OVERWRITE)||iter.matchFlag(optOverWrite, ECLOPT_OVERWRITE_S))
                continue;
            if (iter.matchFlag(optReplacePackagemap, ECLOPT_REPLACE))
                continue;
            if (iter.matchFlag(optUpdateSuperfiles, ECLOPT_UPDATE_SUPER_FILES))
                continue;
            if (iter.matchFlag(optUpdateCloneFrom, ECLOPT_UPDATE_CLONE_FROM))
                continue;
            if (iter.matchFlag(optDontAppendCluster, ECLOPT_DONT_APPEND_CLUSTER))
                continue;
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
                continue;
            if (iter.matchFlag(optAllowForeign, ECLOPT_ALLOW_FOREIGN))
                continue;
            if (iter.matchFlag(optPreloadAll, ECLOPT_PRELOAD_ALL_PACKAGES))
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
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optFileName.isEmpty())
            err.append("\n ... Missing package file name\n");
        else if (optTarget.isEmpty())
            err.append("\n ... Specify a cluster name\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            return false;
        }

        if (optProcess.isEmpty())
            optProcess.set("*");

        if (optPackageMapId.isEmpty())
        {
            StringBuffer name;
            splitFilename(optFileName.get(), NULL, NULL, &name, &name);
            optPackageMapId.set(name.str());
        }
        optPackageMapId.toLowerCase();
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        StringBuffer pkgInfo;
        pkgInfo.loadFile(optFileName);

        fprintf(stdout, "\n ... adding package map %s now\n\n", optFileName.str());

        Owned<IClientAddPackageRequest> request = packageProcessClient->createAddPackageRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setActivate(optActivate);
        request->setInfo(pkgInfo);
        request->setTarget(optTarget);
        request->setPackageMap(optPackageMapId);
        request->setProcess(optProcess);
        request->setDaliIp(optDaliIP);
        request->setOverWrite(optOverWrite);
        request->setGlobalScope(optGlobalScope);
        request->setSourceProcess(optSourceProcess);
        request->setAllowForeignFiles(optAllowForeign);
        request->setPreloadAllPackages(optPreloadAll);
        request->setReplacePackageMap(optReplacePackagemap);
        request->setUpdateSuperFiles(optUpdateSuperfiles);
        request->setUpdateCloneFrom(optUpdateCloneFrom);
        request->setAppendCluster(!optDontAppendCluster);

        Owned<IClientAddPackageResponse> resp = packageProcessClient->AddPackage(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());

        StringArray &notFound = resp->getFilesNotFound();
        if (notFound.length())
        {
            fputs("\nFiles defined in package but not found in DFS:\n", stderr);
            ForEachItemIn(i, notFound)
                fprintf(stderr, "  %s\n", notFound.item(i));
            fputs("\n", stderr);
        }

        return ret;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'add' command will add the package map information to dali \n"
                    "\n"
                    "ecl packagemap add [options] <target> <filename>\n"
                    "   <target>                 Name of target to use when adding package map information\n"
                    "   <filename>               Name of file containing package map information\n"
                    " Options:\n"
                    "   -O, --overwrite          Replace existing packagemap and file information (dangerous)\n"
                    "   -A, --activate           Activate the package information\n"
                    "   --daliip=<ip>            IP of the remote dali to use for logical file lookups\n"
                    "   --pmid                   Identifier of package map - defaults to filename if not specified\n"
                    "   --global-scope           The specified packagemap can be shared across multiple targets\n"
                    "   --source-process         Process cluster to copy files from\n"
                    "   --allow-foreign          Do not fail if foreign files are used in packagemap\n"
                    "   --preload-all            Set preload files option for all packages\n"
                    "   --replace                Replace existing packagmap"
                    "   --update-super-files     Update local DFS super-files if remote DALI has changed\n"
                    "   --update-clone-from      Update local clone from location if remote DALI has changed\n"
                    "   --dont-append-cluster    Only use to avoid locking issues due to adding cluster to file\n",
                    stdout);

        EclCmdCommon::usage();
    }
private:
    StringBuffer pkgInfo;
    StringAttr optFileName;
    StringAttr optTarget;
    StringAttr optProcess;
    StringAttr optDaliIP;
    StringAttr optPackageMapId;
    StringAttr optSourceProcess;
    bool optActivate;
    bool optOverWrite;
    bool optReplacePackagemap;
    bool optUpdateSuperfiles;
    bool optUpdateCloneFrom;
    bool optDontAppendCluster; //Undesirable but here temporarily because DALI may have locking issues
    bool optGlobalScope;
    bool optAllowForeign;
    bool optPreloadAll;
};

class EclCmdPackageMapCopy : public EclCmdCommon
{
public:
    EclCmdPackageMapCopy()
    {
    }
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter) override
    {
        if (iter.done())
            return EclCmdOptionNoMatch;

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg!='-')
            {
                if (optSrcPath.isEmpty())
                    optSrcPath.set(arg);
                else if (optTarget.isEmpty())
                    optTarget.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchOption(optDaliIP, ECLOPT_DALIIP))
                continue;
            if (iter.matchOption(optPMID, ECLOPT_PMID))
                continue;
            if (iter.matchOption(optSourceProcess, ECLOPT_SOURCE_PROCESS))
                continue;
            if (iter.matchFlag(optActivate, ECLOPT_ACTIVATE)||iter.matchFlag(optActivate, ECLOPT_ACTIVATE_S))
                continue;
            if (iter.matchFlag(optReplacePackagemap, ECLOPT_REPLACE))
                continue;
            if (iter.matchFlag(optUpdateSuperfiles, ECLOPT_UPDATE_SUPER_FILES))
                continue;
            if (iter.matchFlag(optUpdateCloneFrom, ECLOPT_UPDATE_CLONE_FROM))
                continue;
            if (iter.matchFlag(optDontAppendCluster, ECLOPT_DONT_APPEND_CLUSTER))
                continue;
            if (iter.matchFlag(optPreloadAll, ECLOPT_PRELOAD_ALL_PACKAGES))
                continue;
            eclCmdOptionMatchIndicator ind = EclCmdCommon::matchCommandLineOption(iter, true);
            if (ind != EclCmdOptionMatch)
                return ind;
        }
        return EclCmdOptionMatch;
    }
    virtual bool finalizeOptions(IProperties *globals) override
    {
        if (!EclCmdCommon::finalizeOptions(globals))
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optSrcPath.isEmpty())
            err.append("\n ... Missing path to source packagemap\n");
        else if (optTarget.isEmpty())
            err.append("\n ... Specify a target cluster\n");

        if (err.length())
        {
            fputs(err.str(), stderr);
            return false;
        }

        return true;
    }
    virtual int processCMD() override
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);

        fprintf(stdout, "\n ... copy package map %s to %s\n\n", optSrcPath.str(), optTarget.str());

        Owned<IClientCopyPackageMapRequest> request = packageProcessClient->createCopyPackageMapRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setSourcePath(optSrcPath);
        request->setTarget(optTarget);
        request->setProcess("*");
        request->setPMID(optPMID);
        request->setActivate(optActivate);
        request->setDaliIp(optDaliIP);
        request->setSourceProcess(optSourceProcess);
        request->setPreloadAllPackages(optPreloadAll);
        request->setReplacePackageMap(optReplacePackagemap);
        request->setUpdateSuperFiles(optUpdateSuperfiles);
        request->setUpdateCloneFrom(optUpdateCloneFrom);
        request->setAppendCluster(!optDontAppendCluster);

        Owned<IClientCopyPackageMapResponse> resp = packageProcessClient->CopyPackageMap(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());

        StringArray &notFound = resp->getFilesNotFound();
        if (notFound.length())
        {
            fputs("\nFiles defined in package but not found in DFS:\n", stderr);
            ForEachItemIn(i, notFound)
                fprintf(stderr, "  %s\n", notFound.item(i));
            fputs("\n", stderr);
        }

        return ret;
    }

    virtual void usage() override
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'copy' command will copy a package map from one target to another \n"
                    "\n"
                    "ecl packagemap copy <path> <target>\n"
                    "   <path>                 Path to the source packagemap to copy\n"
                    "                          The following formats are supported:\n"
                    "                            remote PackageMap - //IP:PORT/Target/PackageMapId\n"
                    "                            local PackageMap - target/PackageMapId\n"
                    "   <target>               Name of target to copy the packagemap to\n"
                    " Options:\n"
                    "   -A, --activate         Activate the package information\n"
                    "   --daliip=<ip>          IP of the remote dali to use for logical file lookups\n"
                    "   --pmid                 Identifier of package map - defaults to source PMID\n"
                    "   --source-process       Process cluster to copy files from\n"
                    "   --preload-all          Set preload files option for all packages\n"
                    "   --replace              Replace existing packagmap\n"
                    "   --update-super-files   Update local DFS super-files if remote DALI has changed\n"
                    "   --update-clone-from    Update local clone from location if remote DALI has changed\n"
                    "   --dont-append-cluster  Only use to avoid locking issues due to adding cluster to file\n",
                    stdout);

        EclCmdCommon::usage();
    }
private:
    StringAttr optSrcPath;
    StringAttr optTarget;
    StringAttr optPMID;
    StringAttr optDaliIP;
    StringAttr optSourceProcess;
    bool optActivate = false;
    bool optReplacePackagemap = false;
    bool optUpdateSuperfiles = false;
    bool optUpdateCloneFrom = false;
    bool optDontAppendCluster = false; //Undesirable but here temporarily because DALI may have locking issues
    bool optPreloadAll = false;
};

class EclCmdPackageValidate : public EclCmdCommon
{
public:
    EclCmdPackageValidate() : optValidateActive(false), optCheckDFS(false), optGlobalScope(false)
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
                else if (optFileName.isEmpty())
                    optFileName.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchFlag(optValidateActive, ECLOPT_ACTIVE))
                continue;
            if (iter.matchFlag(optCheckDFS, ECLOPT_CHECK_DFS))
                continue;
            if (iter.matchOption(optPMID, ECLOPT_PMID) || iter.matchOption(optPMID, ECLOPT_PMID_S))
                continue;
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
                continue;
            if (iter.matchFlag(optIgnoreWarnings, ECLOPT_IGNORE_WARNINGS))
                continue;
            if (iter.matchFlag(optIgnoreOptionalFiles, ECLOPT_IGNORE_OPTIONAL))
                continue;
            StringAttr queryIds;
            if (iter.matchOption(queryIds, ECLOPT_QUERYID))
            {
                optQueryIds.appendList(queryIds.get(), ",");
                continue;
            }
            if (iter.matchOption(queryIds, ECLOPT_IGNORE_QUERIES))
            {
                optIgnoreQueries.appendList(queryIds.get(), ",");
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
        if (!EclCmdCommon::finalizeOptions(globals))
        {
            usage();
            return false;
        }
        StringBuffer err;
        int pcount=0;
        if (optFileName.length())
            pcount++;
        if (optPMID.length())
            pcount++;
        if (optValidateActive)
            pcount++;
        if (pcount==0)
            err.append("\n ... Package file name, --pmid, or --active required\n");
        else if (pcount > 1)
            err.append("\n ... Package file name, --pmid, and --active are mutually exclusive\n");
        if (optTarget.isEmpty())
            err.append("\n ... Specify a cluster name\n\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            return false;
        }
        return true;
    }

    void outputLfnCategoryTree(IPropertyTree &querynode, const char *name)
    {
        StringBuffer lcname(name);
        IPropertyTree *catnode = querynode.queryPropTree(lcname.toLowerCase());
        if (!catnode)
            return;
        fprintf(stderr, "        [%s]\n", name);

        Owned<IPropertyTreeIterator> lfnnodes = catnode->getElements("*");
        ForEach(*lfnnodes)
            fprintf(stderr, "          %s\n", lfnnodes->query().queryName());
    }
    void outputLfnTree(IPropertyTree *lfntree)
    {
        Owned<IPropertyTreeIterator> querynodes = lfntree->getElements("*");
        ForEach(*querynodes)
        {
            IPropertyTree &querynode = querynodes->query();
            fprintf(stderr, "     --%s\n", querynode.queryName());
            outputLfnCategoryTree(querynode, "Compulsory");
            outputLfnCategoryTree(querynode, "Required");
            outputLfnCategoryTree(querynode, "Optional");
        }
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);
        Owned<IClientValidatePackageRequest> request = packageProcessClient->createValidatePackageRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        if (optFileName.length())
        {
            StringBuffer pkgInfo;
            pkgInfo.loadFile(optFileName);
            fprintf(stdout, "\nvalidating packagemap file %s\n\n", optFileName.str());
            request->setInfo(pkgInfo);
        }

        request->setActive(optValidateActive);
        request->setPMID(optPMID);
        request->setTarget(optTarget);
        request->setQueriesToVerify(optQueryIds);
        request->setQueriesToIgnore(optIgnoreQueries);
        request->setCheckDFS(optCheckDFS);
        request->setGlobalScope(optGlobalScope);
        request->setIgnoreWarnings(optIgnoreWarnings);
        request->setIgnoreOptionalFiles(optIgnoreOptionalFiles);

        bool validateMessages = false;
        Owned<IClientValidatePackageResponse> resp = packageProcessClient->ValidatePackage(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret != 0)
            validateMessages = true;
        const char *pmid = resp->getPMID();
        if (!isEmptyString(pmid))
        { //server version < 1.04
            processValidatePackageResponse(nullptr, nullptr, resp->getErrors(), resp->getWarnings(),
                resp->getQueries().getUnmatched(), resp->getPackages().getUnmatched(), resp->getFiles(), validateMessages);
        }
        else
        {
            IArrayOf<IConstValidateResult> &results = resp->getResults();
            ForEachItemIn(i, results)
            {
                IConstValidateResult &result = results.item(i);
                processValidatePackageResponse(result.getTarget(), result.getPMID(), result.getErrors(), result.getWarnings(),
                    result.getQueries().getUnmatched(), result.getPackages().getUnmatched(), result.getFiles(), validateMessages);
            }
        }

        if (!validateMessages)
            fputs("   Validation was successful\n", stdout);

        return ret;
    }

    bool processValidatePackageResponse(const char *target, const char *pmid, StringArray &errors, StringArray &warnings,
        StringArray &unmatchedQueries, StringArray &unusedPackages, IConstValidatePackageFiles &files,
        bool &validateMessages)
    {
        if (!isEmptyString(target) && !isEmptyString(pmid))
            fprintf(stderr, "   Target: %s, PMID: %s :\n", target, pmid);

        if (errors.ordinality()>0)
        {
            validateMessages = true;
            fputs("   Validation Failed!\n", stderr);
            fputs("   Error(s):\n", stderr);
            ForEachItemIn(i, errors)
                fprintf(stderr, "      %s\n", errors.item(i));
        }
        if (warnings.ordinality()>0)
        {
            validateMessages = true;
            fputs("   Warning(s):\n", stderr);
            ForEachItemIn(i, warnings)
                fprintf(stderr, "      %s\n", warnings.item(i));
        }
        if (unmatchedQueries.ordinality()>0)
        {
            validateMessages = true;
            fputs("\n   Queries without matching package:\n", stderr);
            ForEachItemIn(i, unmatchedQueries)
                fprintf(stderr, "      %s\n", unmatchedQueries.item(i));
        }
        if (unusedPackages.ordinality()>0)
        {
            validateMessages = true;
            fputs("\n   Packages without matching queries:\n", stderr);
            ForEachItemIn(i, unusedPackages)
                fprintf(stderr, "      %s\n", unusedPackages.item(i));
        }
        StringArray &unusedFiles = files.getUnmatched();
        if (unusedFiles.ordinality()>0)
        {
            validateMessages = true;
            fputs("\n   Query files without matching package definitions:\n", stderr);
            Owned<IPropertyTree> filetree = createPTree();
            ForEachItemIn(i, unusedFiles)
            {
                StringArray info;
                info.appendList(unusedFiles.item(i), "/");
                if (info.length()>=2)
                {
                    IPropertyTree *querynode = ensurePTree(filetree, info.item(0));
                    if (querynode)
                    {
                        StringBuffer category((info.length()>=3) ? info.item(2) : "required");
                        IPropertyTree *cat = ensurePTree(querynode, category.toLowerCase());
                        if (cat)
                            ensurePTree(cat, info.item(1));
                    }
                }
            }
            outputLfnTree(filetree);
        }

        StringArray &notInDFS = files.getNotInDFS();
        if (notInDFS.ordinality()>0)
        {
            validateMessages = true;
            fputs("\n   Packagemap SubFiles not found in DFS:\n", stderr);
            ForEachItemIn(i, notInDFS)
                fprintf(stderr, "      %s\n", notInDFS.item(i));
        }

        if (!isEmptyString(target) && !isEmptyString(pmid))
            fprintf(stderr, "\n   Target: %s, PMID: %s done\n\n", target, pmid);
        return validateMessages;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'validate' command will checkout the contents of the package map file \n"
                    "\n"
                    "ecl packagemap validate <target> <filename>\n"
                    " Options:\n"
                    "   <target>                    Name of target to use when validating package map information\n"
                    "   <filename>                  Name of file containing package map information\n"
                    "   --active                    Validate the active packagemap\n"
                    "   --check-dfs                 Verify that subfiles exist in DFS\n"
                    "   -pm, --pmid                 Identifier of packagemap to validate\n"
                    "   --queryid                   Query to verify against packagemap, multiple queries can be\n"
                    "                               specified using a comma separated list, or by using --queryid\n"
                    "                               more than once. Default is all queries in the target queryset\n"
                    "   --ignore-queries            Queries to exclude from verification, multiple queries can be\n"
                    "                               specified using wildcards, a comma separated list, or by using\n"
                    "                               --ignore-queries more than once.\n"
                    "   --ignore-optional           Doesn't warn when optional files are not defined in packagemap.\n"
                    "   --ignore-warnings           Doesn't output general packagemap warnings.\n"
                    "   --global-scope              The specified packagemap can be shared across multiple targets\n",
                    stdout);

        EclCmdCommon::usage();
    }
private:
    StringArray optQueryIds;
    StringArray optIgnoreQueries;
    StringAttr optFileName;
    StringAttr optTarget;
    StringAttr optPMID;
    bool optValidateActive;
    bool optCheckDFS;
    bool optGlobalScope;
    bool optIgnoreWarnings = false;
    bool optIgnoreOptionalFiles = false;
};

class EclCmdPackageQueryFiles : public EclCmdCommon
{
public:
    EclCmdPackageQueryFiles() : optGlobalScope(false)
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
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchOption(optPMID, ECLOPT_PMID) || iter.matchOption(optPMID, ECLOPT_PMID_S))
                continue;
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optTarget.isEmpty())
            err.append("\n ... A target cluster must be specified\n");
        if (optQueryId.isEmpty())
            err.append("\n ... A query must be specified\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            return false;
        }
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = getWsPackageSoapService(optServer, optPort, optUsername, optPassword);
        Owned<IClientGetQueryFileMappingRequest> request = packageProcessClient->createGetQueryFileMappingRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setQueryName(optQueryId);
        request->setPMID(optPMID);
        request->setGlobalScope(optGlobalScope);

        Owned<IClientGetQueryFileMappingResponse> resp = packageProcessClient->GetQueryFileMapping(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());

        StringArray &unmappedFiles = resp->getUnmappedFiles();
        if (!unmappedFiles.ordinality())
            fputs("No undefined files found.\n", stderr);
        else
        {
            fputs("Files not defined in PackageMap:\n", stderr);
            ForEachItemIn(i, unmappedFiles)
                fprintf(stderr, "  %s\n", unmappedFiles.item(i));
        }
        IArrayOf<IConstSuperFile> &superFiles = resp->getSuperFiles();
        if (!superFiles.ordinality())
            fputs("\nNo matching SuperFiles found in PackageMap.\n", stderr);
        else
        {
            fputs("\nSuperFiles defined in PackageMap:\n", stderr);
            ForEachItemIn(i, superFiles)
            {
                IConstSuperFile &super = superFiles.item(i);
                fprintf(stderr, "  %s\n", super.getName());
                StringArray &subfiles = super.getSubFiles();
                if (subfiles.ordinality()>0)
                {
                    ForEachItemIn(sbi, subfiles)
                        fprintf(stderr, "   > %s\n", subfiles.item(sbi));
                }
            }
        }

        return ret;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'query-files' command will list the files referenced by a query, showing if/how they\n"
                    "are mapped as SuperFiles in the active packagemap.  --pmid option allows an inactive\n"
                    "packagemap to be used instead.\n"
                    "\n"
                    "ecl packagemap query-files <target> <queryid>\n"
                    " Options:\n"
                    "   <target>                    Name of target to use when validating package map information\n"
                    "   <queryid>                   Name of query to get file mappings for\n"
                    "   -pm, --pmid                 Optional id of packagemap to validate, defaults to active\n"
                    "   --global-scope              The specified packagemap can be shared across multiple targets\n",
                    stdout);

        EclCmdCommon::usage();
    }
private:
    StringAttr optTarget;
    StringAttr optQueryId;
    StringAttr optPMID;
    bool optGlobalScope;
};


class EclCmdPackageAddPart : public EclCmdCommon
{
public:
    EclCmdPackageAddPart() : optDeletePrevious(false), optGlobalScope(false), optAllowForeign(false), optPreloadAll(false), optUpdateSuperfiles(false), optUpdateCloneFrom(false), optDontAppendCluster(false)
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
                else if (optPMID.isEmpty())
                    optPMID.set(arg);
                else if (optFileName.isEmpty())
                    optFileName.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchOption(optPartName, ECLOPT_PART_NAME))
                continue;
            if (iter.matchOption(optDaliIP, ECLOPT_DALIIP))
                continue;
            if (iter.matchOption(optSourceProcess, ECLOPT_SOURCE_PROCESS))
                continue;
            if (iter.matchFlag(optDeletePrevious, ECLOPT_DELETE_PREVIOUS))
                continue;
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
                continue;
            if (iter.matchFlag(optAllowForeign, ECLOPT_ALLOW_FOREIGN))
                continue;
            if (iter.matchFlag(optPreloadAll, ECLOPT_PRELOAD_ALL_PACKAGES))
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
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optFileName.isEmpty())
            err.append("\n ... Missing package file name\n");
        else if (optTarget.isEmpty())
            err.append("\n ... Specify a cluster name\n");
        else if (optPMID.isEmpty())
            err.append("\n ... Specify a packagemap name\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            return false;
        }

        if (optPartName.isEmpty())
        {
            StringBuffer name;
            splitFilename(optFileName.get(), NULL, NULL, &name, &name);
            optPartName.set(name.str());
        }
        optPMID.toLowerCase();
        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        StringBuffer content;
        content.loadFile(optFileName);

        fprintf(stdout, "\n ... adding packagemap %s part %s from file %s\n\n", optPMID.get(), optPartName.get(), optFileName.get());

        Owned<IClientAddPartToPackageMapRequest> request = packageProcessClient->createAddPartToPackageMapRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setPackageMap(optPMID);
        request->setPartName(optPartName);
        request->setContent(content);
        request->setDeletePrevious(optDeletePrevious);
        request->setDaliIp(optDaliIP);
        request->setGlobalScope(optGlobalScope);
        request->setSourceProcess(optSourceProcess);
        request->setAllowForeignFiles(optAllowForeign);
        request->setPreloadAllPackages(optPreloadAll);
        request->setUpdateSuperFiles(optUpdateSuperfiles);
        request->setUpdateCloneFrom(optUpdateCloneFrom);
        request->setAppendCluster(!optDontAppendCluster);

        Owned<IClientAddPartToPackageMapResponse> resp = packageProcessClient->AddPartToPackageMap(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());

        StringArray &notFound = resp->getFilesNotFound();
        if (notFound.length())
        {
            fputs("\nFiles defined in packagemap part but not found in DFS:\n", stderr);
            ForEachItemIn(i, notFound)
                fprintf(stderr, "  %s\n", notFound.item(i));
            fputs("\n", stderr);
        }

        return ret;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'add-part' command will add the packagemap part to an existing packagemap\n"
                    "\n"
                    "ecl packagemap add-part [options] <target> <pmid> <filename>\n"
                    "   <target>                    Name of target to use when adding packagemap part\n"
                    "   <pmid>                      Identifier of packagemap to add the part to\n"
                    "   <filename>                  Name of file containing packagemap part content\n"
                    " Options:\n"
                    "   --part-name                 Name of part being added (defaults to filename)\n"
                    "   --delete-prev               Replace an existing part with matching name\n"
                    "   --daliip=<ip>               IP of the remote dali to use for logical file lookups\n"
                    "   --global-scope              The specified packagemap is shared across multiple targets\n"
                    "   --source-process=<value>    Process cluster to copy files from\n"
                    "   --allow-foreign             Do not fail if foreign files are used in packagemap\n"
                    "   --preload-all               Set preload files option for all packages\n"
                    "   --update-super-files        Update local DFS super-files if remote DALI has changed\n"
                    "   --update-clone-from         Update local clone from location if remote DALI has changed\n"
                    "   --dont-append-cluster       Only use to avoid locking issues due to adding cluster to file\n",
                    stdout);

        EclCmdCommon::usage();
    }
private:
    StringAttr optPMID;
    StringAttr optTarget;
    StringAttr optDaliIP;
    StringAttr optSourceProcess;
    StringAttr optPartName;
    StringAttr optFileName;
    bool optDeletePrevious;
    bool optGlobalScope;
    bool optAllowForeign;
    bool optPreloadAll;
    bool optUpdateSuperfiles;
    bool optUpdateCloneFrom;
    bool optDontAppendCluster;
};

class EclCmdPackageRemovePart : public EclCmdCommon
{
public:
    EclCmdPackageRemovePart()
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
                if (!optTarget.length())
                    optTarget.set(arg);
                else if (!optPMID.length())
                    optPMID.set(arg);
                else if (!optPartName.length())
                    optPartName.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optPMID.isEmpty())
            err.append("\n ... Missing package map name\n");
        else if (optPartName.isEmpty())
            err.append("\n ... Missing part name\n");
        else if (optTarget.isEmpty())
            err.append("\n ... Specify a target cluster name\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            return false;
        }

        return true;
    }
    virtual int processCMD()
    {
        fprintf(stdout, "\n ... removing part %s from packagemap %s\n\n", optPartName.get(), optPMID.get());

        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientRemovePartFromPackageMapRequest> request = packageProcessClient->createRemovePartFromPackageMapRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setPackageMap(optPMID);
        request->setGlobalScope(optGlobalScope);
        request->setPartName(optPartName);

        Owned<IClientRemovePartFromPackageMapResponse> resp = packageProcessClient->RemovePartFromPackageMap(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret == 0)
            printf("Successfully removed part %s from package %s\n", optPartName.get(), optPMID.get());
        return ret;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'remove-part' command will remove the given part from the given packagemap\n"
                    "\n"
                    "ecl packagemap remove-part <target> <packagemap> <partname>\n"
                    "   <target>               Name of the target to use \n"
                    "   <packagemap>           Name of the package map containing the part\n"
                    "   <partname>             Name of the part to remove\n"
                    " Options:\n"
                    "   --global-scope         The specified packagemap is sharable across multiple targets\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optTarget;
    StringAttr optPMID;
    StringAttr optPartName;
    bool optGlobalScope;
};

class EclCmdPackageGetPart: public EclCmdCommon
{
public:
    EclCmdPackageGetPart()
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
                if (!optTarget.length())
                    optTarget.set(arg);
                else if (!optPMID.length())
                    optPMID.set(arg);
                else if (!optPartName.length())
                    optPartName.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return EclCmdOptionNoMatch;
                }
                continue;
            }
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
        {
            usage();
            return false;
        }
        StringBuffer err;
        if (optPMID.isEmpty())
            err.append("\n ... Missing package map name\n");
        else if (optPartName.isEmpty())
            err.append("\n ... Missing part name\n");
        else if (optTarget.isEmpty())
            err.append("\n ... Specify a target cluster name\n");

        if (err.length())
        {
            fprintf(stdout, "%s", err.str());
            return false;
        }

        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientGetPartFromPackageMapRequest> request = packageProcessClient->createGetPartFromPackageMapRequest();
        setCmdRequestTimeouts(request->rpc(), 0, optWaitConnectMs, optWaitReadSec);

        request->setTarget(optTarget);
        request->setPackageMap(optPMID);
        request->setGlobalScope(optGlobalScope);
        request->setPartName(optPartName);

        Owned<IClientGetPartFromPackageMapResponse> resp = packageProcessClient->GetPartFromPackageMap(request);
        int ret = outputMultiExceptionsEx(resp->getExceptions());
        if (ret == 0)
            printf("%s", resp->getContent());
        return ret;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n"
                    "\n"
                    "The 'get-part' command will fetch the given part from the given packagemap\n"
                    "\n"
                    "ecl packagemap get-part <target> <packagemap> <partname>\n"
                    "   <target>               Name of the target to use \n"
                    "   <packagemap>           Name of the package map containing the part\n"
                    "   <partname>             Name of the part to get\n"
                    " Options:\n"
                    "   --global-scope         The specified packagemap is sharable across multiple targets\n",
                    stdout);
        EclCmdCommon::usage();
    }
private:
    StringAttr optTarget;
    StringAttr optPMID;
    StringAttr optPartName;
    bool optGlobalScope;
};

IEclCommand *createPackageSubCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    if (strieq(cmdname, "add"))
        return new EclCmdPackageAdd();
    if (strieq(cmdname, "copy"))
        return new EclCmdPackageMapCopy();
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
    if (strieq(cmdname, "validate"))
        return new EclCmdPackageValidate();
    if (strieq(cmdname, "query-files"))
        return new EclCmdPackageQueryFiles();
    if (strieq(cmdname, "add-part"))
        return new EclCmdPackageAddPart();
    if (strieq(cmdname, "remove-part"))
        return new EclCmdPackageRemovePart();
    if (strieq(cmdname, "get-part"))
        return new EclCmdPackageGetPart();
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
            "      add          Add a package map to the environment\n"
            "      copy         Copy a package map from one target to another\n"
            "      delete       Delete a package map\n"
            "      activate     Activate a package map\n"
            "      deactivate   Deactivate a package map (package map will not get loaded)\n"
            "      list         List loaded package map names\n"
            "      info         Return active package map information\n"
            "      validate     Validate information in the package map file \n"
            "      query-files  Show files used by a query and if/how they are mapped\n"
            "      add-part     Add additional packagemap content to an existing packagemap\n"
            "      get-part     Get the content of a packagemap part from a packagemap\n"
            "      remove-part  Remove a packagemap part from a packagemap\n"
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
