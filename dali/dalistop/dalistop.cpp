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
#include "jmisc.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"


int main(int argc, char* argv[])
{
    InitModuleObjects();
    try {
        if (argc<2) {
            printf("usage: dalistop <server_ip:port> [/nowait]\n");
            printf("eg:  dalistop .                          -- stop dali server running locally\n");
            printf("     dalistop eq0001016                  -- stop dali server running remotely\n");
        }
        else {
            SocketEndpoint ep;
            ep.set(argv[1],DALI_SERVER_PORT);
            bool nowait = false;
            if (argc>=3)
                nowait = stricmp(argv[2],"/nowait")==0;
            printf("Stopping Dali Server on %s\n",argv[1]);
            startMPServer(0);
            Owned<IGroup> group = createIGroup(1,&ep); 
            Owned<ICommunicator> comm = createCommunicator(group);
            CMessageBuffer mb;
            int fn=-1;
            mb.append(fn);
            if (comm->verifyConnection(0,2000)) {
                comm->send(mb,0,MPTAG_DALI_COVEN_REQUEST,MP_ASYNC_SEND);
                if (nowait)
                    Sleep(1000);
                else
                    while (comm->verifyConnection(0,1000)) {
                        PROGLOG("Waiting for Dali Server to stop....");
                        Sleep(5000);
                    }
            }
            else
                PROGLOG("Dali not responding");
            stopMPServer();
        }
    }
    catch (IException *e) {
        pexception("Exception",e);
        stopMPServer();
    }
    releaseAtoms();
    return 0;
}

