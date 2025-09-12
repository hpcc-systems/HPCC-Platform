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

int main(int argc, const char* argv[])
{
    COnScopeExit cleanup(cleanupConsole);

    InitModuleObjects();

    CEvtCommandGroup evtool({
        { "dump", createDumpCommand },
        { "save-as", createSaveAsCommand },
        { "sim", createSimCommand },
        { "index", createIndexCommand },
    });
    try
    {
        evtool.dispatch(argc, argv, 0);
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
    return 0;
}
