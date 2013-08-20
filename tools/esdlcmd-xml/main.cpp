/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "esdlcomp.h"

int gArgc;
char** gArgv = NULL;

//------------------------------------------------------
// usage

void usage(const char* programName)
{
    printf("ESDL Compiler\n\n");
    printf("Usage:  %s [options] filename.scm [<outdir>]\n\n", programName);
    printf("Output:       filename.xml\n");

    puts("Available options:");
    puts(" -?/-h: show this usage page");
    exit(1);
}

//------------------------------------------------------
// parse comamnd line: put non-option params into gArgv

void parseCommandLine(int argc, char* argv[])
{
    gArgv = new char*[argc+1];
    gArgc = 0;

    // parse options
    for (int i=0; i<argc; i++)
    {
        if (*argv[i]=='-')
        {
            if (stricmp(argv[i], "-?")==0 || stricmp(argv[i], "-h")==0)
                usage(argv[0]);

            else
            {
                printf("Unknown option: %s\n", argv[i]);
                exit(1);
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
    parseCommandLine(argc, argv);

    char* sourcefile = gArgv[1];
    char* outdir = (gArgc>=3)?(char*)gArgv[2]:(char*)"";

    ESDLcompiler hc(sourcefile, outdir);
    hc.Process();

    delete[] gArgv;

    return 0;
}

// end
//------------------------------------------------------
