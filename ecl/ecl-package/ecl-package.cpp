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
    EclCmdPackageActivate() : optGlobalScope(false)
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
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
        if (optProcess.isEmpty())
            optProcess.set("*");

        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientActivatePackageRequest> request = packageProcessClient->createActivatePackageRequest();
        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);
        request->setProcess(optProcess);
        request->setGlobalScope(optGlobalScope);

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
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
        if (optProcess.isEmpty())
            optProcess.set("*");

        return true;
    }
    virtual int processCMD()
    {
        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientDeActivatePackageRequest> request = packageProcessClient->createDeActivatePackageRequest();
        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);
        request->setProcess(optProcess);
        request->setGlobalScope(optGlobalScope);

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
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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

        if (optProcess.isEmpty())
            optProcess.set("*");
        return true;
    }
    virtual int processCMD()
    {
        fprintf(stdout, "\n ... deleting package map %s now\n\n", optPackageMap.sget());

        Owned<IClientWsPackageProcess> packageProcessClient = createCmdClient(WsPackageProcess, *this);
        Owned<IClientDeletePackageRequest> request = packageProcessClient->createDeletePackageRequest();
        request->setTarget(optTarget);
        request->setPackageMap(optPackageMap);
        request->setProcess(optProcess);
        request->setGlobalScope(optGlobalScope);

        Owned<IClientDeletePackageResponse> resp = packageProcessClient->DeletePackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());
        else
            printf("Successfully deleted package %s\n", optPackageMap.get());

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
    EclCmdPackageAdd() : optActivate(false), optOverWrite(false), optGlobalScope(false)
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
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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

        fprintf(stdout, "\n ... adding package map %s now\n\n", optFileName.sget());

        Owned<IClientAddPackageRequest> request = packageProcessClient->createAddPackageRequest();
        request->setActivate(optActivate);
        request->setInfo(pkgInfo);
        request->setTarget(optTarget);
        request->setPackageMap(optPackageMapId);
        request->setProcess(optProcess);
        request->setDaliIp(optDaliIP);
        request->setOverWrite(optOverWrite);
        request->setGlobalScope(optGlobalScope);
        request->setSourceProcess(optSourceProcess);

        Owned<IClientAddPackageResponse> resp = packageProcessClient->AddPackage(request);
        if (resp->getExceptions().ordinality())
            outputMultiExceptions(resp->getExceptions());

        StringArray &notFound = resp->getFilesNotFound();
        if (notFound.length())
        {
            fputs("\nFiles defined in package but not found in DFS:\n", stderr);
            ForEachItemIn(i, notFound)
                fprintf(stderr, "  %s\n", notFound.item(i));
            fputs("\n", stderr);
        }

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
                    "   -O, --overwrite             Overwrite existing information\n"
                    "   -A, --activate              Activate the package information\n"
                    "   --daliip=<location>         format cluster@ip/prefix\n"
                    "                               ip is the address of the remote dali to use for logical file lookups\n"
                    "                               cluster (optional) is the process cluster to copy files from\n"
                    "                               prefix (optional) will be added to the file name for lookup on\n"
                    "                               the source dali, but will not be added to the local file name\n"
                    "   --pmid                      Identifier of package map - defaults to filename if not specified\n"
                    "   --global-scope              The specified packagemap can be shared across multiple targets\n"
                    "   --source-process            Process cluster to copy files from\n"
                    "   <target>                    Name of target to use when adding package map information\n"
                    "   <filename>                  Name of file containing package map information\n",
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
    bool optGlobalScope;
};

class EclCmdPackageValidate : public EclCmdCommon
{
public:
    EclCmdPackageValidate() : optValidateActive(false), optCheckDFS(false), optGlobalScope(false)
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
            if (iter.matchFlag(optValidateActive, ECLOPT_ACTIVE))
                continue;
            if (iter.matchFlag(optCheckDFS, ECLOPT_CHECK_DFS))
                continue;
            if (iter.matchOption(optPMID, ECLOPT_PMID) || iter.matchOption(optPMID, ECLOPT_PMID_S))
                continue;
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
                continue;
            StringAttr queryIds;
            if (iter.matchOption(queryIds, ECLOPT_QUERYID))
            {
                optQueryIds.appendList(queryIds.get(), ",");
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
        int pcount=0;
        if (optFileName.length())
            pcount++;
        if (optPMID.length())
            pcount++;
        if (optValidateActive)
            pcount++;
        if (pcount==0)
            err.append("\n ... Package file name, --pmid, or --active required\n\n");
        else if (pcount > 1)
            err.append("\n ... Package file name, --pmid, and --active are mutually exclusive\n\n");
        if (optTarget.isEmpty())
            err.append("\n ... Specify a cluster name\n\n");

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
        Owned<IClientValidatePackageRequest> request = packageProcessClient->createValidatePackageRequest();

        if (optFileName.length())
        {
            StringBuffer pkgInfo;
            pkgInfo.loadFile(optFileName);
            fprintf(stdout, "\nvalidating packagemap file %s\n\n", optFileName.sget());
            request->setInfo(pkgInfo);
        }

        request->setActive(optValidateActive);
        request->setPMID(optPMID);
        request->setTarget(optTarget);
        request->setQueriesToVerify(optQueryIds);
        request->setCheckDFS(optCheckDFS);
        request->setGlobalScope(optGlobalScope);

        bool validateMessages = false;
        Owned<IClientValidatePackageResponse> resp = packageProcessClient->ValidatePackage(request);
        if (resp->getExceptions().ordinality()>0)
        {
            validateMessages = true;
            outputMultiExceptions(resp->getExceptions());
        }
        StringArray &errors = resp->getErrors();
        if (errors.ordinality()>0)
        {
            validateMessages = true;
            fputs("   Error(s):\n", stderr);
            ForEachItemIn(i, errors)
                fprintf(stderr, "      %s\n", errors.item(i));
        }
        StringArray &warnings = resp->getWarnings();
        if (warnings.ordinality()>0)
        {
            validateMessages = true;
            fputs("   Warning(s):\n", stderr);
            ForEachItemIn(i, warnings)
                fprintf(stderr, "      %s\n", warnings.item(i));
        }
        StringArray &unmatchedQueries = resp->getQueries().getUnmatched();
        if (unmatchedQueries.ordinality()>0)
        {
            validateMessages = true;
            fputs("\n   Queries without matching package:\n", stderr);
            ForEachItemIn(i, unmatchedQueries)
                fprintf(stderr, "      %s\n", unmatchedQueries.item(i));
        }
        StringArray &unusedPackages = resp->getPackages().getUnmatched();
        if (unusedPackages.ordinality()>0)
        {
            validateMessages = true;
            fputs("\n   Packages without matching queries:\n", stderr);
            ForEachItemIn(i, unusedPackages)
                fprintf(stderr, "      %s\n", unusedPackages.item(i));
        }
        StringArray &unusedFiles = resp->getFiles().getUnmatched();
        if (unusedFiles.ordinality()>0)
        {
            fputs("\n   Files without matching package definitions:\n", stderr);
            ForEachItemIn(i, unusedFiles)
                fprintf(stderr, "      %s\n", unusedFiles.item(i));
        }

        StringArray &notInDFS = resp->getFiles().getNotInDFS();
        if (notInDFS.ordinality()>0)
        {
            fputs("\n   Packagemap SubFiles not found in DFS:\n", stderr);
            ForEachItemIn(i, notInDFS)
                fprintf(stderr, "      %s\n", notInDFS.item(i));
        }

        if (!validateMessages)
            fputs("   Validation was successful\n", stdout);

        return 0;
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
                    "   --global-scope              The specified packagemap can be shared across multiple targets\n",
                    stdout);

        EclCmdCommon::usage();
    }
private:
    StringArray optQueryIds;
    StringAttr optFileName;
    StringAttr optTarget;
    StringAttr optPMID;
    bool optValidateActive;
    bool optCheckDFS;
    bool optGlobalScope;
};

class EclCmdPackageQueryFiles : public EclCmdCommon
{
public:
    EclCmdPackageQueryFiles() : optGlobalScope(false)
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
                else if (optQueryId.isEmpty())
                    optQueryId.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return false;
                }
                continue;
            }
            if (iter.matchOption(optPMID, ECLOPT_PMID) || iter.matchOption(optPMID, ECLOPT_PMID_S))
                continue;
            if (iter.matchFlag(optGlobalScope, ECLOPT_GLOBAL_SCOPE))
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
        if (optTarget.isEmpty())
            err.append("\n ... A target cluster must be specified\n\n");
        if (optQueryId.isEmpty())
            err.append("\n ... A query must be specified\n\n");

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
        Owned<IClientGetQueryFileMappingRequest> request = packageProcessClient->createGetQueryFileMappingRequest();

        request->setTarget(optTarget);
        request->setQueryName(optQueryId);
        request->setPMID(optPMID);
        request->setGlobalScope(optGlobalScope);

        Owned<IClientGetQueryFileMappingResponse> resp = packageProcessClient->GetQueryFileMapping(request);
        if (resp->getExceptions().ordinality()>0)
            outputMultiExceptions(resp->getExceptions());

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

        return 0;
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
    if (strieq(cmdname, "validate"))
        return new EclCmdPackageValidate();
    if (strieq(cmdname, "query-files"))
        return new EclCmdPackageQueryFiles();
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
            "      delete       Delete a package map\n"
            "      activate     Activate a package map\n"
            "      deactivate   Deactivate a package map (package map will not get loaded)\n"
            "      list         List loaded package map names\n"
            "      info         Return active package map information\n"
            "      validate     Validate information in the package map file \n"
            "      query-files  Show files used by a query and if/how they are mapped\n"
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
