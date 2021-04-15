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

#include "jlog.hpp"
#include "jfile.hpp"
#include "jargv.hpp"

#include "esdlcmd.hpp"
#include "esdlcmd_common.hpp"
#include "esdlcmd_core.hpp"


//=========================================================================================

static int doMain(int argc, const char *argv[])
{
    EsdlCMDShell processor(argc, argv, createCoreEsdlCommand, hpccBuildInfo.buildTag, true);
    return processor.run();
}

int main(int argc, const char *argv[])
{
    InitModuleObjects();
    queryStderrLogMsgHandler()->setMessageFields(0);
    unsigned exitCode = doMain(argc, argv);
    releaseAtoms();
    return exitCode;
}
