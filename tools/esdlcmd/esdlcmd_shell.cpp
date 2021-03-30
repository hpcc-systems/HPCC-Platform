/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#include "esdlcmd.hpp"
#include "esdlcmd_core.hpp"


//=========================================================================================

#ifdef _WIN32
#include "process.h"
#endif

int EsdlCMDShell::callExternal(ArgvIterator &iter)
{
    const char *argv[100];
    StringBuffer cmdstr("esdl-");
    cmdstr.append(cmd.str());
    int i=0;
    argv[i++]=cmdstr.str();
    if (optHelp)
        argv[i++]="--help";
    for (; !iter.done(); iter.next())
        argv[i++]=iter.query();
    argv[i]=NULL;
//TODO - add common routine or use existing in jlib
#ifdef _WIN32
    if (_spawnvp(_P_WAIT, cmdstr.str(), const_cast<char **>(argv))==-1)
#else
    // First try in same dir as the esdl executable
    StringBuffer local;
    splitFilename(queryCurrentProcessPath(), &local, &local, NULL, NULL);
    local.append(cmdstr);
    if (execvp(local.str(), const_cast<char **>(argv))!=-1)
        return 0;
    // If not found, try the path
    if (errno!=ENOENT || execvp(cmdstr.str(), const_cast<char **>(argv))==-1)
#endif
    {
        switch(errno)
        {
        case ENOENT:
            fprintf(stderr, "esdl '%s' command not found\n", cmd.str());
            usage();
            return 1;
        default:
            fprintf(stderr, "esdl '%s' command error %d\n", cmd.str(), errno);
            return 1;
        }
    }
    return 0;
}

int EsdlCMDShell::processCMD(ArgvIterator &iter)
{
    Owned<IEsdlCommand> c = factory(cmd.get());
    if (!c)
    {
        if (cmd.length())
        {
            if (runExternals)
                return callExternal(iter);
            fprintf(stderr, "esdl '%s' command not found\n", cmd.str());
        }
        usage();
        return 1;
    }
    if (optHelp)
    {
        c->usage();
        return 0;
    }
    if (!c->parseCommandLineOptions(iter))
        return 0;

    if (!c->finalizeOptions(globals))
        return 0;

    return c->processCMD();
}

void EsdlCMDShell::finalizeOptions(IProperties *globals)
{
}

int EsdlCMDShell::run()
{
    try
    {
        if (!parseCommandLineOptions(args))
            return 1;

        finalizeOptions(globals);

        return processCMD(args);
    }
    catch (IException *E)
    {
        StringBuffer m;
        fputs(E->errorMessage(m).newline().str(), stderr);
        E->Release();
        return 2;
    }
#ifndef _DEBUG
    catch (...)
    {
        ERRLOG("Unexpected exception\n");
        return 4;
    }
#endif
    return 0;
}

//=========================================================================================

bool EsdlCMDShell::parseCommandLineOptions(ArgvIterator &iter)
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
        if (iter.matchFlag(optHelp, ESDLARG_HELP) || iter.matchFlag(optHelp, ESDLOPT_HELP)) //users expect --help to work too
            continue;
        if (*arg!='-')
        {
            cmd.set(arg);
            iter.next();
            break;
        }
        if (iter.matchFlag(boolValue, ESDLOPT_VERSION))
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

void EsdlCMDShell::usage()
{
    fprintf(stdout,"\nUsage:\n"
        "    esdl <command> [<args>]\n\n"
           "Commonly used commands:\n"
           "   xml               Generate XML from ESDL definition.\n"
           "   ecl               Generate ECL from ESDL definition.\n"
           "   xsd               Generate XSD from ESDL definition.\n"
           "   wsdl              Generate WSDL from ESDL definition.\n"
           "   java              Generate Java code from ESDL definition.\n"
           "   cpp               Generate C++ code from ESDL definition.\n"
           "   publish           Publish ESDL Definition for ESP use.\n"
           "   list-definitions  List all ESDL definitions.\n"
           "   get-definition    Get ESDL definition.\n"
           "   delete            Delete ESDL Definition.\n"
           "   bind-service      Configure ESDL based service on target ESP (with existing ESP Binding).\n"
           "   list-bindings     List all ESDL bindings.\n"
           "   unbind-service    Remove ESDL based service binding on target ESP.\n"
           "   bind-method       Configure method associated with existing ESDL binding.\n"
           "   unbind-method     Remove method associated with existing ESDL binding.\n"
           "   get-binding       Get ESDL binding.\n"
           "   bind-log-transform  	Configure log transform associated with existing ESDL binding.\n"
           "   unbind-log-transform	Remove log transform associated with existing ESDL binding.\n"
           "   monitor           Generate ECL code for result monitoring / differencing\n"
           "   monitor-template  Generate a template for use with 'monitor' command\n"
           ""
           "\nRun 'esdl help <command>' for more information on a specific command\n"
           "\nRun 'esdl --version' to get version information\n\n"
    );
}
