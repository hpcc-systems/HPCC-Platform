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

