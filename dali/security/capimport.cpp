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

