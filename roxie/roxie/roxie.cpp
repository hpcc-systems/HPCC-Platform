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
    printf("\t--daemon <instance>\t: sets up daemon and writes out pidfile\n");
    printf("\t--topology=[XML-file]\t: Reads Roxie topology (default RoxieTopology.xml)\n");
    printf("\t--port=[integer]\t\t: Network port (default 9876)\n");
    printf("\nOne-off query:\n");
    printf("\t--loadWorkunit=[so|dll]\t: Load and execute shared library\n");
    printf("\t-[xml|csv|raw]\t\t: Output format (default ascii)\n");
    printf("\n");

    // If changing these, please change ccdmain.cpp's roxie_common_usage() as well
    printf("Generic:\n");
    printf("\t--daliServers=[host1,...]\t: List of Dali servers to use\n");
    printf("\t--tracelevel=[integer]\t: Amount of information to dump on logs\n");
    printf("\t--stdlog=[boolean]\t: Standard log format (based on tracelevel)\n");
    printf("\t--logfile\t: Outputs to logfile, rather than stdout\n");
    printf("\t--help|-h\t: This message\n");
    printf("\n");
}

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
    return start_query(argc, argv);
}
