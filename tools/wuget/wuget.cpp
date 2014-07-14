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

#include "jlib.hpp"
#include "thorplugin.hpp"
#include "workunit.hpp"

void usage(const char *progname)
{
    fprintf(stderr, "%s - extract workunit information from query dll.\nUsage: %s filename\n", progname, progname);
}

int main(int argc, char **argv)
{
    InitModuleObjects();
    bool doManifestInfo = false;
    bool doWorkunit = false;
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0]=='-')
        {
            if (strcmp(argv[i], "-m")==0)
                doManifestInfo = true;
            else if (strcmp(argv[i], "-w")==0)
                doWorkunit = true;
            else
            {
                usage(argv[0]);
                return 1;
            }
        }
    }
    int filesSeen = 0;
    int errors = 0;
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0]!='-')
        {
            filesSeen++;
            if (doWorkunit || !doManifestInfo)
            {
                StringBuffer xml;
                if (getWorkunitXMLFromFile(argv[i], xml))
                {
                    Owned<ILocalWorkUnit> wu = createLocalWorkUnit();
                    wu->loadXML(xml);
                    exportWorkUnitToXML(wu, xml.clear(), true, false);
                    printf("%s\n", xml.str());
                }
                else
                {
                    fprintf(stderr, "Could not load workunit from %s\n", argv[i]);
                    errors++;
                }
            }
            if (doManifestInfo)
            {
                StringBuffer xml;
                if (getManifestXMLFromFile(argv[i], xml))
                {
                    printf("%s\n", xml.str());
                }
                else
                {
                    fprintf(stderr, "Could not load manifest from %s\n", argv[i]);
                    errors++;
                }
            }
        }
    }
    if (!filesSeen)
    {
        fprintf(stderr, "No files specified\n");
        errors++;
    }
    return errors ? 1 : 0;
}
