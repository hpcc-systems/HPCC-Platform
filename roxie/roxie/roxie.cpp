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

#include "platform.h"
#include "ccd.hpp"

static void roxie_server_usage()
{
    printf("\nRoxie Server: Starts the Roxie service or executes a one-off query.\n");
    printf("\troxie [options below]\n");

    // Not documenting use of internal options: selftest, restarts, enableSysLog and host
    printf("\nService:\n");
    printf("\t--topology=[XML-file]\t: Reads Roxie topology (deafult RoxieTopology.xml)\n");
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
    printf("\t--logfile=[format]\t: Outputs to logfile, rather than stdout\n");
    printf("\t--help|-h\t: This message\n");
    printf("\n");
}

int main(int argc, const char *argv[])
{
    for (unsigned i=0; i<argc; i++)
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
