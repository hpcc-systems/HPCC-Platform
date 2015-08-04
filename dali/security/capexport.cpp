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
#include "jlib.hpp"
#include "jfile.hpp"
#include "mpcomm.hpp"

#include "daclient.hpp"
#include "dasds.hpp"

#include "dacaplib.hpp"

void usage()
{
    printf("USAGE: CapExport <output-filename> <system-id> <server-password> <commands>\n");
    printf("where commands are:\n");
    printf("                     THORWIDTH <width>\n");
    printf("                or:  DALINODE <mac-address>\n");
    printf("                or:  DALINODE <local-ip>\n");
    printf("\n");
    printf("e.g. capexport update1.xml XYZ001 QyTTzAB ^\n");
    printf("               THORWIDTH 600 DALINODE 10.150.28.12\n");
}

int main(int argc, char* argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    if (argc<6)
    {
        usage();
        return 0;
    }

    try
    {
        const char *filename = argv[1];
        Owned<IDaliCapabilityCreator> cc = createDaliCapabilityCreator();
        cc->setSystemID(argv[2]);
        cc->setServerPassword(argv[3]);
        for (unsigned i=4;i<argc;i++) {
            const char *cmd = argv[i++];
            if (i==argc)
                break;
            const char *param = argv[i];
            if (stricmp(cmd,"THORWIDTH")==0) {
                cc->setLimit(DCR_ThorSlave,atoi(param));
            }
            else if (stricmp(cmd,"DALINODE")==0) {
                StringBuffer mac;
                if (strchr(param,'.')) { // must be ip
                    IpAddress ip;
                    ip.set(param);
                    if (!getMAC(ip,mac)) {
                        printf("ERROR: could mot get MAC address for %s\n",param);
                        return 1;
                    }
                }
                else
                    mac.append(param);
                cc->addCapability(DCR_DaliServer,mac.str());
            }
            else {
                printf("ERROR: unknown command %s\n",cmd);
                return 1;
            }
        }
        StringBuffer results;
        cc->save(results);
        Owned<IFile> ifile = createIFile(filename);
        Owned<IFileIO> ifileio = ifile->open(IFOcreate);
        ifileio->write(0,results.length(),results.str());
        printf("Dali Capabilities sucessfully exported to %s\n", filename);
    }
    catch (IException *e)
    {
        EXCLOG(e);
        e->Release();
    }

    releaseAtoms();
    return 0;
}

