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
#include "jprop.hpp"

#include "eclcmd.hpp"
#include "eclcmd_core.hpp"

#define INIFILE "ecl.ini"
#define DEFAULTINIFILE "ecl.ini"

//=========================================================================================

#ifdef _WIN32
#include "process.h"
#else
#include <pwd.h>
#endif

int EclCMDShell::callExternal(ArgvIterator &iter)
{
    const char *argv[100];
    StringBuffer cmdstr("ecl-");
    cmdstr.append(cmd.str());
    int i=0;
    argv[i++]=cmdstr.str();
    if (optHelp)
        argv[i++]="help";
    for (; !iter.done(); iter.next())
        argv[i++]=iter.query();
    argv[i]=NULL;
//TODO - add common routine or use existing in jlib
    // First try in same dir as the ecl executable
    StringBuffer local;
    splitFilename(queryCurrentProcessPath(), &local, &local, NULL, NULL);
    local.append(cmdstr);
    errno = 0;
#ifdef _WIN32
    if (_spawnvp(_P_WAIT, local.str(), const_cast<char **>(argv))==-1)
        return 0;
    // If not found, try the path
    if (errno!=ENOENT || _spawnvp(_P_WAIT, cmdstr.str(), const_cast<char **>(argv))==-1)
#else
    if (execvp(local.str(), const_cast<char **>(argv))!=-1)
        return 0;
    // If not found, try the path
    if (errno!=ENOENT || execvp(cmdstr.str(), const_cast<char **>(argv))==-1)
#endif
    {
        switch(errno)
        {
        case ENOENT:
            fprintf(stderr, "ecl '%s' command not found\n", cmd.str());
            return 1;
        default:
            fprintf(stderr, "ecl '%s' command error %d\n", cmd.str(), errno);
            return 1;
        }
    }
    return 0;
}

int EclCMDShell::processCMD(ArgvIterator &iter)
{
    Owned<IEclCommand> c = factory(cmd.get());
    if (!c)
    {
        if (cmd.length())
        {
            if (runExternals)
                return callExternal(iter);
            fprintf(stderr, "ecl '%s' command not found\n", cmd.str());
        }
        usage();
        return 1;
    }
    if (optHelp)
    {
        c->usage();
        return 0;
    }
    switch (c->parseCommandLineOptions(iter))
    {
    case EclCmdOptionNoMatch:
        c->usage(); //fall through
    case EclCmdOptionCompletion:
        return 0;
    }
    if (!c->finalizeOptions(globals))
    {
        c->usage();
        return 0;
    }

    return c->processCMD();
}

void EclCMDShell::finalizeOptions(IProperties *globals)
{
}

int EclCMDShell::run()
{
    try
    {
        if (!parseCommandLineOptions(args))
            return 1;

        if (!optIniFilename)
        {
            StringBuffer fn;
            if (checkFileExists(INIFILE))
                optIniFilename.set(INIFILE);
            else if (getHomeDir(fn) && checkFileExists(addPathSepChar(fn).append(INIFILE)))
                optIniFilename.set(fn);
            else if (fn.set(hpccBuildInfo.configDir).append(PATHSEPSTR).append(DEFAULTINIFILE))
                optIniFilename.set(fn);
        }

        globals.setown(createProperties(optIniFilename, true));
        finalizeOptions(globals);

        return processCMD(args);
    }
    catch (IException *E)
    {
        StringBuffer m;
        fputs(E->errorMessage(m.newline()).newline().str(), stderr);
        E->Release();
        return 2;
    }
#ifndef _DEBUG
    catch (...)
    {
        IERRLOG("Unexpected exception\n");
        return 4;
    }
#endif
    return 0;
}

//=========================================================================================

bool EclCMDShell::parseCommandLineOptions(ArgvIterator &iter)
{
    if (iter.done())
    {
        usage();
        return false;
    }

    bool boolValue;
    for (; !iter.done(); iter.next())
    {
        const char * arg = iter.query();
        if (iter.matchFlag(optHelp, ECLARG_HELP) || iter.matchFlag(optHelp, ECLOPT_HELP)) //users expect --help to work too
            continue;
        if (*arg!='-')
        {
            cmd.set(arg);
            iter.next();
            break;
        }
        if (iter.matchFlag(boolValue, "--version"))
        {
            fprintf(stdout, "%s\n", hpccBuildInfo.buildTag);
            return false;
        }
        StringAttr tempArg;
        if (iter.matchOption(tempArg, "-brk"))
        {
#if defined(_WIN32) && defined(_DEBUG)
            unsigned id = atoi(tempArg.str());
            if (id == 0)
                DebugBreak();
            else
                _CrtSetBreakAlloc(id);
#endif
        }
    }
    return true;
}

//=========================================================================================

void EclCMDShell::usage()
{
    fprintf(stdout,"\nUsage:\n"
        "    ecl [--version] <command> [<args>]\n\n"
           "Commonly used commands:\n"
           "   deploy           create a workunit from an ecl file, archive, or dll\n"
           "   publish          add a workunit to a query set\n"
           "   unpublish        remove a query from a query set\n"
           "   run              run the given ecl file, archive, dll, wuid, or query\n"
           "   results          retrieve the results for an existing workunit\n"
           "   activate         activate a published query\n"
           "   deactivate       deactivate the given query alias name\n"
           "   queries          show or manipulate queries and querysets\n"
           "   packagemap       manage HPCC packagemaps\n"
           "   bundle           manage ECL bundles\n"
           "   roxie            commands specific to roxie clusters\n"
           "   abort            abort workunit(s) for WUID or job name\n"
           "   status           show workunit(s) current status for WUID or job name\n"
           "   getname          provide job name from WUID\n"
           "   getwuid          provide WUID from job name\n"
           "   zapgen           provide ZAP file from WUID\n"
           "   sign             add digital signature to an ecl file or a text file\n"
           "   listkeyuid       lists all the key user IDs that can be used in the sign command\n"
           "   url-secret-name  Generate a secret name from a url for automatic url mapping\n"
           "\nRun 'ecl help <command>' for more information on a specific command\n\n"
    );
}
