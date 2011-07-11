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
