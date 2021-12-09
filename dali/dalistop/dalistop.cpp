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
#include "jmisc.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"


int main(int argc, const char* argv[])
{
    InitModuleObjects();
    int exitCode = 1;
    try
    {
        unsigned port = 0;
        const char *server = nullptr;
        if (argc<2)
        {
            // with no args, use port from daliconfig if present (used by init scripts)
            Owned<IFile> daliConfigFile = createIFile("daliconf.xml");
            if (daliConfigFile->exists())
            {
                Owned<IPropertyTree> daliConfig = createPTree(*daliConfigFile, ipt_caseInsensitive);
                port = daliConfig->getPropInt("@port", DALI_SERVER_PORT);
                server = ".";
            }
            else
            {
                printf("usage: dalistop <server_ip:port> [/nowait]\n");
                printf("eg:  dalistop .                          -- stop dali server running locally\n");
                printf("     dalistop eq0001016                  -- stop dali server running remotely\n");
            }
        }
        else
        {
            server = argv[1];
            port = DALI_SERVER_PORT;
        }

        if (server)
        {
            SocketEndpoint ep;
            ep.set(server, port);
            bool nowait = false;
            if (argc>=3)
                nowait = stricmp(argv[2],"/nowait")==0;
            printf("Stopping Dali Server on %s, port=%u\n", server, port);

            Owned<IGroup> group = createIGroup(1,&ep); 
            initClientProcess(group, DCR_DaliStop);
            Owned<ICommunicator> comm = createCommunicator(group);
            CMessageBuffer mb;
            int fn=-1;
            mb.append(fn);
            if (comm->verifyConnection(0,2000))
            {
                comm->send(mb,0,MPTAG_DALI_COVEN_REQUEST,MP_ASYNC_SEND);
                if (nowait)
                {
                    Sleep(1000);
                    exitCode = 0;
                }
                else
                {
                    // verifyConnection() has a min conn timeout of 10s
                    // use recv() instead to check for socket closed ...
                    try
                    {
                        while (!comm->recv(mb,0,MPTAG_DALI_COVEN_REQUEST,nullptr,5000))
                        {
                            printf("Waiting for Dali Server to stop....\n");
                        }
                        exitCode = 0;
                    }
                    catch (IMP_Exception *e)
                    {
                        if (e->errorCode() == MPERR_link_closed)
                            exitCode = 0;
                        e->Release();
                    }
                }
            }
            else
                fprintf(stderr, "Dali not responding\n");
            stopMPServer();
        }
    }
    catch (IException *e)
    {
        pexception("Exception",e);
        stopMPServer();
    }
    releaseAtoms();
    return exitCode;
}
