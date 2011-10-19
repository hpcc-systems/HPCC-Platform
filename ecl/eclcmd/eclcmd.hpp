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

#ifndef ECLCMD_HPP
#define ECLCMD_HPP

#include "eclcmd_common.hpp"

class EclCMDShell
{
public:
    EclCMDShell(int argc, const char *argv[], EclCommandFactory _factory, const char *_version, bool _runExternals=false)
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
    EclCommandFactory factory;
    StringBuffer name;
    StringAttr cmd;
    bool runExternals;

    StringAttr version;

    bool optHelp;
    StringAttr optIniFilename;
};

#endif
