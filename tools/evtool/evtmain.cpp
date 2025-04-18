/*##############################################################################

    Copyright (C) 2024 HPCC Systems®.

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

#include "jlib.hpp"
#include "jiface.hpp"
#include "evtool.h"
#include "jstring.hpp"
#include "jutil.hpp"
#include <functional>
#include <map>
#include <string>

static void usage(const char* tool, const CmdMap& commands, IBufferedSerialOutputStream& out)
{
    const char* argv[] = { tool, nullptr };
    for (const CmdMap::value_type& c : commands)
    {
        Owned<IEvToolCommand> cmd = c.second();
        argv[1] = c.first.c_str();
        cmd->usage(2, argv, 1, out);
        out.put(1, "\n");
    }
}

int main(int argc, const char* argv[])
{
    COnScopeExit cleanup(cleanupConsole);

    InitModuleObjects();

    CmdMap commands{
        { "dump", createDumpCommand },
        { "sim", createSimCommand },
        { "index", createIndexCommand },
    };
    if (argc < 2)
    {
        usage(argv[0], commands, consoleOut());
        return 0;
    }
    CmdMap::const_iterator it = commands.find(argv[1]);
    if (commands.end() == it)
    {
        StringBuffer err;
        err << "unknown command: " << argv[1] << "\n\n";
        consoleErr().put(err.length(), err.str());
        usage(argv[0], commands, consoleErr());
        return 1;
    }
    Owned<IEvToolCommand> cmd = it->second();
    try
    {
        return cmd->dispatch(argc, argv, 1);
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        e->Release();
        msg << '\n';
        consoleErr().put(msg.length(), msg.str());
        return 1;
    }
}
