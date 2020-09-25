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

#include "jlib.hpp"
#include "jfile.hpp"
#include "thorplugin.hpp"
#include "workunit.hpp"

void usage(const char *progname)
{
    printf("%s - extract workunit information from query dll.\nUsage: %s [-m] [-w] filename\n", progname, progname);
    printf("Options:\n");
    printf("  -m     Display manifest information\n");
    printf("  -w     Display embedded workunit\n");
    printf("  -a     Display embedded archive\n");
    printf("/nIf no options specified, -w is assumed\n");
}

int main(int argc, char **argv)
{
    InitModuleObjects();
    bool doManifestInfo = false;
    bool doWorkunit = false;
    bool doArchive = false;
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0]=='-')
        {
            if (strcmp(argv[i], "-m")==0)
                doManifestInfo = true;
            else if (strcmp(argv[i], "-w")==0)
                doWorkunit = true;
            else if (strcmp(argv[i], "-a")==0)
                doArchive = true;
            else
            {
                usage(argv[0]);
                return 1;
            }
        }
    }
    if (!doArchive && !doManifestInfo && !doWorkunit)
        doWorkunit = true;
    int filesSeen = 0;
    int errors = 0;
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0]!='-')
        {
            try
            {
                filesSeen++;
                if (doWorkunit || doArchive)
                {
                    const char *filename = argv[i];
                    const char *ext = pathExtension(filename);
                    StringBuffer xml;
                    if (strisame(ext, ".xml"))
                        xml.loadFile(filename, false);
                    else
                        getWorkunitXMLFromFile(filename, xml);
                    if (xml.length())
                    {
                        Owned<ILocalWorkUnit> wu = createLocalWorkUnit(xml);
                        if (doArchive && getArchiveXMLFromFile(filename, xml.clear()))
                        {
                            Owned<IWUQuery> q = wu->updateQuery();
                            q->setQueryText(xml);
                        }
                        if (doWorkunit)
                        {
                            exportWorkUnitToXML(wu, xml.clear(), true, false, true);
                            printf("%s\n", xml.str());
                        }
                        if (doArchive)
                        {
                            Owned<IConstWUQuery> query = wu->getQuery();
                            if (query)
                            {
                                SCMStringBuffer text;
                                query->getQueryText(text);
                                printf("%s\n", text.s.str());
                            }
                            else
                                printf("No archive found\n");
                        }
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
            catch (IException *E)
            {
                StringBuffer msg;
                fprintf(stderr, "Exception %d: %s processing argument %s\n", E->errorCode(), E->errorMessage(msg).str(), argv[i]);
                errors++;
                E->Release();
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
