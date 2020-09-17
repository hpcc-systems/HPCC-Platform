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

#include "platform.h"
#include "ccd.hpp"

static void roxie_server_usage()
{
    printf("\nRoxie Server: Starts the Roxie service or executes a one-off query.\n");
    printf("\troxie [options below]\n");

    // Not documenting use of internal options: selftest, restarts, enableSysLog and host
    printf("\nService:\n");
#ifndef _CONTAINERIZED
    printf("  --daemon|-d <instanceName>: Run daemon as instance\n");
#endif
    printf("  --port=[integer]          : Network port (default 9876)\n");
    printf("One-off query:\n");
    printf("  --loadWorkunit=[so/dll]   : Load and execute named query dll\n");
    printf("  --workunit=wuid           : Load and execute named workunit\n");
    printf("\n");

    // If changing these, please change ccdmain.cpp's roxie_common_usage() as well
    printf("Generic:\n");
    printf("  --daliServers=[host1,...] : List of Dali servers to use\n");
    printf("  --tracelevel=[integer]    : Amount of information to dump on logs\n");
    printf("  --stdlog=[boolean]        : Standard log format (based on tracelevel)\n");
    printf("  --logfile=[filename]      : Outputs to logfile, rather than stdout\n");
    printf("  --help|-h                 : This message\n");
    printf("\n");
}

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
roxie:
  allFilesDynamic: false
  localSlave: true
  numChannels: 1
  queueNames: roxie.roxie
  logging:
    detail: 100
)!!";

int main(int argc, const char *argv[])
{
    for (unsigned i=0; i<(unsigned)argc; i++)
    {
        if (stricmp(argv[i], "--help")==0 ||
            stricmp(argv[i], "-h")==0)
        {
            roxie_server_usage();
            return EXIT_SUCCESS;
        }
    }
    return roxie_main(argc, argv, defaultYaml);
}
