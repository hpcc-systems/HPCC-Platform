/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
 */

#include "jliball.hpp"
#include "archive.hpp"

const char* version = "1.0";

void usage()
{
    printf("Tankfile version %s.\n\nUsage:\n", version);
    printf("  tankfile action=[archive] [<options>]\n\n");
    printf("options:\n");
    printf("    server=<[http|https]://host:port> : log server url\n");
    printf("    user=<username>\n");
    printf("    password=<password>\n");
    printf("    group=<log-agent-group-name>\n");
    printf("    dir=<directory-name> : directory to archive the tank files\n");
}

bool getInput(int argc, const char* argv[], IProperties* input)
{
    for (unsigned i = 1; i < argc; i++)
    {
        const char* arg = argv[i];
        if (strchr(arg, '='))
            input->loadProp(arg);
        else if (strieq(argv[i], "-v") || strieq(argv[i], "-version"))
        {
            printf("Tankfile version %s\n", version);
            return false;
        }
        else if (strieq(arg, "-h") || strieq(arg, "-?"))
        {
            usage();
            return false;
        }
        else
        {
            printf("Error: unknown command parameter: %s\n", argv[i]);
            usage();
            return false;
        }
    }
    return true;
}

bool processRequest(IProperties* input)
{
    const char* action = input->queryProp("action");
    if (isEmptyString(action))
    {
        printf("Error: 'action' not specified\n");
        return false;
    }

    printf("Tankfile version %s: %s", version, action);
    if (strieq(action, "archive"))
    {
        archiveTankFiles(input);
    }
    else
    {
        printf("Error: unknown 'action': %s\n", action);
        return false;
    }

    return true;
}

int main(int argc, const char** argv)
{
    InitModuleObjects();

    Owned<IProperties> input = createProperties(true);
    if (!getInput(argc, argv, input))
    {
        releaseAtoms();
        return 0;
    }

    try
    {
        processRequest(input);
    }
    catch(IException *excpt)
    {
        StringBuffer errMsg;
        printf("Exception: %d:%s\n", excpt->errorCode(), excpt->errorMessage(errMsg).str());
    }
    catch(...)
    {
        printf("Unknown exception\n");
    }
    releaseAtoms();

    return 0;
}
