/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "esdl2xml.hpp"

char** gArgv = NULL;
int gArgc = 0;

//------------------------------------------------------
// usage
void static usage(const char* programName)
{
    printf("\nESDL Compiler\n\n");
    printf("Usage:        %s [options] filename.ecm [<outdir>]\n", programName);
    printf("Output:       (srcdir|<outdir>)/filename.xml\n\n");

    puts("Available options:");
    puts(" -r|--recursive: process all includes");
    puts(" -v|--verbose:   display information");
    puts(" -?/-h/--help:   show this usage page");
    exit(1);
}

void parseCommandLine(int argc, char* argv[], Esdl2Esxdl * cmd)
{
    gArgv = new char*[argc+1];
    gArgc = 0;

    // parse options
    for (int i=0; i<argc; i++)
    {
        if (*argv[i]=='-')
        {
            if (stricmp(argv[i], "-?")==0 || stricmp(argv[i], "-h")==0 || stricmp(argv[i], "--help")==0)
            {
                usage(argv[0]);
            }
            else if (stricmp(argv[i], "-r")==0 || stricmp(argv[i], "--recursive")==0)
            {
                cmd->setRecursive(true);
            }
            else if (stricmp(argv[i], "-v")==0 || stricmp(argv[i], "--verbose")==0)
            {
                cmd->setVerbose(true);
            }
            else
            {
                fprintf(stdout, "Unknown option: %s\n", argv[i]);
                usage(argv[0]);
            }
        }
        else
            gArgv[gArgc++] = argv[i];
    }
    gArgv[gArgc] = NULL;

    if (gArgc<2 || gArgc>4)
        usage(argv[0]);
}

//------------------------------------------------------
// main

int main(int argc, char* argv[])
{
    Owned<Esdl2Esxdl> cmd = new Esdl2Esxdl();
    parseCommandLine(argc, argv, cmd.get());
    cmd->transform(gArgv[1], (char*)gArgv[2]);

    delete [] gArgv;
    return 0;
}

// end
//------------------------------------------------------
