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
#include <functional>
#include <iostream>
#include <map>
#include <string>

using CmdCreator = std::function<IEvToolCommand*()>;
using CmdMap = std::map<std::string, CmdCreator>;

static void usage(const char* tool, const CmdMap& commands, std::ostream& out)
{
    const char* argv[] = { tool, nullptr };
    for (const CmdMap::value_type& c : commands)
    {
        Owned<IEvToolCommand> cmd = c.second();
        argv[1] = c.first.c_str();
        cmd->usage(2, argv, 1, out);
    }
}

int main(int argc, const char* argv[])
{
    InitModuleObjects();

    CmdMap commands{
        { "dump", createDumpCommand },
    };
    if (argc < 2)
    {
        usage(argv[0], commands, std::cout);
        return 0;
    }
    CmdMap::const_iterator it = commands.find(argv[1]);
    if (commands.end() == it)
    {
        std::cerr << "unknown command: " << argv[1] << std::endl << std::endl;
        usage(argv[0], commands, std::cerr);
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
        std::cerr << msg.str() << std::endl;
        return 1;
    }
}
