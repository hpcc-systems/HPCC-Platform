/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#ifndef ESDLCMD_HPP
#define ESDLCMD_HPP

#include "esdlcmd_common.hpp"

class EsdlCMDShell
{
public:
    EsdlCMDShell(int argc, const char *argv[], EsdlCommandFactory _factory, const char *_version, bool _runExternals=false)
        : args(argc, argv), factory(_factory), version(_version), optHelp(false), runExternals(_runExternals)
    {
        splitFilename(argv[0], NULL, NULL, &name, NULL);
    }

    bool parseCommandLineOptions(ArgvIterator &iter);
    void finalizeOptions(IProperties *globals);
    int processCMD(ArgvIterator &iter);
    int callExternal(ArgvIterator &iter);
    int run();

    virtual void usage();

protected:
    ArgvIterator args;
    Owned<IProperties> globals;
    EsdlCommandFactory factory;
    StringBuffer name;
    StringAttr cmd;
    bool runExternals;

    StringAttr version;

    bool optHelp;
    StringAttr optIniFilename;
};

#endif /* ESDLCMD_HPP */
