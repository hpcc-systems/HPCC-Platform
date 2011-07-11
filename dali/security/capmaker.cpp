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

void usage(const char *exe)
{
    printf("USAGE: %s <import filename> [export_filename]       OR\n", exe);
    printf("USAGE: %s -local <usage> <system> <export_filename>\n", exe);
}

static unsigned csvread(IIOStream &stream, char *dst, unsigned max, bool stopEOL=false)
{
    char *p = dst;
    char c;
    loop
    { 
        if (!stream.read(1, &c))
            return 0;
        else
        {
            if (stopEOL && ('\n' == c || '\r' == c)) return 0;
            if (!isspace(c)) break;
        }
    }
    do
    {
        if (',' == c || '\n' == c || '\r' == c) { *p = '\0'; break; }
        else if (!isspace(c) && (p-dst)<max-1)
            *p++ = c;
    }
    while (stream.read(1, &c));
    return p-dst;
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

    CCapabilityExporter exporter(CLIENT_ENCRYPT_KEY);
    try
    {
        StringBuffer exportFname;
        if ('-' == *argv[1])
        {
            if (0!=stricmp(argv[1]+1, "local") || argc<5)
            {
                usage(argv[0]);
                return 0;           
            }
            DaliClientRole roleType = CSystemCapability::decodeRole(argv[2]);
            if (DCR_Unknown == roleType)
                throw MakeStringException(0, "Unknown role: %s", argv[2]);
            const char *system = argv[3];

            CSystemCapability sc(roleType, system);
            StringBuffer s;
            dumpMAC(sc, s);
            PROGLOG("MAC detected: %s", s.str());
            dumpCPUSN(sc, s.clear());
            PROGLOG("CPU SN detected: %s", s.str());
            exportFname.append(argv[4]);
            OwnedIFile ifile = createIFile(exportFname.str());
            OwnedIFileIO ifileio = ifile->open(IFOcreate);
            OwnedIFileIOStream stream = createBufferedIOStream(ifileio);
            exporter.export(*stream);
        }
        else
        {
            const char *filename = argv[1];
            if (argc>2) exportFname.append(argv[2]);
            else
            {
                char *dot = strchr(filename, '.');
                if (!dot || (0 == stricmp("scd", dot+1)))
                    exportFname.append(filename);
                else
                    exportFname.append(dot-filename, filename);
                exportFname.append(".scd");
            }

            OwnedIFile ifile = createIFile(filename);
            OwnedIFileIO ifileio = ifile->open(IFOread);
            OwnedIFileIOStream stream = createBufferedIOStream(ifileio);

            char system[9], role[51], addr[18], cpusn[26];
            PROGLOG("Reading capabilities");
            while (csvread(*stream, system, sizeof(system)))
            {
                DaliClientRole roleType;
                if (!csvread(*stream, role, sizeof(role))) throw MakeStringException(0, "Invalid format (role)");
                else
                {
                    roleType = CSystemCapability::decodeRole(role);
                    if (DCR_Unknown == roleType)
                        throw MakeStringException(0, "Unknown role: %s", role);
                }
                unsigned addrLength = csvread(*stream, addr, sizeof(addr));
                if (!addrLength)
                    throw MakeStringException(0, "Invalid format address)");
                unsigned cpuSNLength = csvread(*stream, cpusn, sizeof(cpusn), true);
                exporter.add(roleType, system, addr, cpuSNLength?cpusn:NULL);
            }

            ifile.setown(createIFile(exportFname.str()));
            ifileio.setown(ifile->open(IFOcreate));
            stream.setown(createBufferedIOStream(ifileio));

            exporter.export(*stream);
        }
        PROGLOG("Capabilities exported to %s", exportFname.str());
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

