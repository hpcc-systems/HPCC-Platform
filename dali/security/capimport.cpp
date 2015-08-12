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


void usage(const char *exe)
{
    printf("USAGE: capimport <xml-filename>        -- must be run in dali server directory\n");
    printf("   or: capimport <dali-ip> <filename>  -- full update\n");
}

int main(int argc, char* argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    if (argc<2)
    {
        usage(argv[0]);
        return 0;
    }

    if (argc<3)
    {
        unsigned err=importDaliCapabilityXML_basic(argv[1]);
        if (err) 
            printf("Dali Capability Import FAILED: error(%d)\n",err);
        else
            printf("Dali Capability Import succeeded");
        releaseAtoms();
        return err;
    }

    SocketEndpoint ep;
    ep.set(argv[1],DALI_SERVER_PORT);
    SocketEndpointArray epa;
    epa.append(ep);

    Owned<IGroup> group = createIGroup(epa); 
    if (!initClientProcess(group))
    {
        Sleep(1000);
        PROGLOG("Failed to connect to dali");
        return 0;
    }

    const char *filename = argv[argc-1];
    try
    {
        OwnedIFile ifile = createIFile(filename);
        OwnedIFileIO ifileio = ifile->open(IFOread);

        MemoryBuffer mb;
        size32_t sz = ifile->size();
        if (sz > FIXED_HT_SIZE*FIXED_KEY_SIZE)
            throw MakeStringException(0, "Capabilities binary is too large");
        if (sz % FIXED_KEY_SIZE != 0)
            throw MakeStringException(0, "Capabilities binary wrong format");
        ifileio->read(0, sz, mb.reserveTruncate(sz));
        querySessionManager().importCapabilities(mb);
        PROGLOG("Capabilities imported");
    }
    catch (IException *e)
    {
        EXCLOG(e);
        e->Release();
    }

    closedownClientProcess();
    releaseAtoms();
    return 0;
}

