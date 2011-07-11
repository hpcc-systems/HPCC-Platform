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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hidlcomp.h"

int gArgc;
char** gArgv = NULL;

//------------------------------------------------------
// usage

void usage(const char* programName)
{
    printf("HIDL Compiler Version %s\n\n",HIDLVER);
    printf("Usage:  %s [options] filename.<hid|scm|ecm|ncm> [<outdir>]\n\n", programName);
    printf("Output:       filename.cpp\n");
    printf("              filename.hpp\n");
#if 0
    printf("              filename.xsv\n");
    printf(" (the xsv file is an example server implementation template)\n");
#endif
    puts("Available options:");
    puts(" -?/-h: show this usage page");
    puts(" -esp:  the input file is an ESP *.ecm file");
    puts(" -espng:  the input file is an ESP *.ncm file and supports X protocal");
    exit(1);
}

//------------------------------------------------------
// parse comamnd line: put non-option params into gArgv

void parseCommandLine(int argc, char* argv[])
{
    gArgv = new char*[argc+1];
    gArgc = 0;
    const char* option = NULL;

    // parse options
    for (int i=0; i<argc; i++)
    {
        if (*argv[i]=='-')
        {
            if (stricmp(argv[i], "-esp")==0 || stricmp(argv[i], "-espng")==0)
                option = argv[i];
            else if (stricmp(argv[i], "-?")==0 || stricmp(argv[i], "-h")==0)
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


    if (option)
    {
        if (stricmp(option, "-esp")==0)
            isESP=true;
        else if (stricmp(option, "-espng")==0)
            isESP=isESPng=true;
    }
    else
    {
        const char* p = strrchr(gArgv[1], '.');
        if (p && p!=gArgv[1] && strlen(++p)==3)
        {
            strcpy(srcFileExt, p);

            if (0 == strcmp(p, "ecm"))
                isESP = true;
            else if (0 == strcmp(p, "ncm"))
                isESP = isESPng = true;
            else
                if (0 != strcmp(p, "scm")) //not *.scm
                    isSCM = false;
        }
        else
        {
            strcpy(srcFileExt, "hid");
            isSCM = false;
        }
    }
}

//------------------------------------------------------
// main

int main(int argc, char* argv[])
{
#ifdef _DEBUG
    //initLeakCheck(false);
#endif

    parseCommandLine(argc, argv);
    
    char* sourcefile = gArgv[1];
    char* outdir = (gArgc>=3)?(char*)gArgv[2]:(char*)"";
    
    HIDLcompiler hc(sourcefile, outdir);
    hc.Process();

    delete[] gArgv;
    if (esp_def_export_tag)
        free(esp_def_export_tag);

    return 0;
}

// end
//------------------------------------------------------
