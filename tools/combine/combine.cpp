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

#ifdef _WIN32
#define _WIN32_WINNT 0x0400
#include <windows.h>
#endif

#include "platform.h"
#include "jlib.hpp"
#include "jio.hpp"
#include "jfile.hpp"
#include "jexcept.hpp"
#include "jhtree.hpp"
#include "jsocket.hpp"
#include "jlog.hpp"


void usage()
{
    printf("\nCOMBINE <srcpaths> <targetpath> [<options>]\n");
    printf("Options:\n");
    printf("  -header:<text>        header text\n");
    printf("  -glue:<text>          file separator text\n");
    printf("  -footer:<text>        footer text\n");
    printf("  -prefix:<cmdlist>     file prefix\n");
    printf("  -r                    process sub directories\n");
    exit(2);
}

bool isWild(const char *path)
{
    if (strchr(path,'?')||strchr(path,'*'))
        return true;
    return false;
}

#define BUFFER_SIZE 0x100000

void appendFile(IFileIO *srcio,IFileIOStream *out,bool sub)
{
    static byte buf[BUFFER_SIZE];
    Owned<IFileIOStream> srcstrm = createIOStream(srcio);
    loop {
        size32_t rd = srcstrm->read(BUFFER_SIZE,buf);
        if (!rd)
            break;
        out->write(rd,buf);
    }
}




bool addFilename(StringAttrArray &names,const char *name,bool sub)
{
    if (isWild(name)) {
        StringBuffer dir;
        const char *tail = splitDirTail(name,dir);
        if (isWild(dir.str())) {
            printf("Directory %s - cannot be wild!\n",dir.str());
            return false;
        }
        Owned<IFile> dirf = createIFile(dir.str());
        Owned<IDirectoryIterator> iter = dirf->directoryFiles(tail,sub,false);
        StringBuffer subname;
        ForEach(*iter) {
            subname.clear().append(iter->query().queryFilename());
            names.append(*new StringAttrItem(subname.str()));
        }
    }
    else 
        names.append(*new StringAttrItem(name));
    return true;
}

static void genPrefix(MemoryBuffer &out,const char *prefix,const char *filename,unsigned __int64 length)
{
    out.setEndian(__LITTLE_ENDIAN);

    const char * s = prefix;
    while (s&&*s) {
        StringAttr command;
        const char * comma = strchr(s, ',');
        if (comma) {
            command.set(s, comma-s);
            s = comma+1;
        }
        else {
            command.set(s);
            s = NULL;
        }
        command.toUpperCase();
        if (memcmp(command, "FILENAME", 8) == 0) {
            StringBuffer tmp;
            const char *tail = splitDirTail(filename,tmp);
            tmp.clear().append(tail);
            if (command[8] == ':') {
                unsigned maxLen = atoi(command+9);
                tmp.padTo(maxLen);
                out.append(maxLen, tmp.str());
            }
            else {
                out.append((unsigned)tmp.length());
                out.append(tmp.length(), tmp.str());
            }
        }
        else if ((memcmp(command, "FILESIZE", 8) == 0) || (command.length() == 2))
        {
            const char * format = command;
            if (memcmp(format, "FILESIZE", 8) == 0) {
                if (format[8] == ':')
                    format = format+9;
                else
                    format = "L4";
            }

            bool bigEndian;
            char c = format[0];
            if (c == 'B')
                bigEndian = true;
            else if (c == 'L')
                bigEndian = false;
            else
                throw MakeStringException(-1,"Invalid prefix format %s", format);
            c = format[1];
            if ((c <= '0') || (c > '8'))
                throw MakeStringException(-1,"Invalid prefix format %s", format);

            unsigned l = (c - '0');
            unsigned __int64 value = length;
            byte temp[8];
            for (unsigned i=0; i<l; i++) {
                temp[i] = (byte)value;
                value >>= 8;
            }
            if (value)
                throw MakeStringException(-1,"Prefix too small");
            if (bigEndian)
            {
                byte temp2[8];
                _cpyrevn(&temp2, &temp, l);
                out.append(l, &temp2);
            }
            else
                out.append(l, &temp);
        }
        else
            throw MakeStringException(-1,"Invalid prefix format %s", command.get());
    }
}

int main(int argc, const char *argv[])
{
    InitModuleObjects();

    UnsignedArray srcargs;
    unsigned dstarg = 0;
    StringBuffer header;
    StringBuffer footer;
    StringBuffer glue;
    StringBuffer prefix;
    bool sub = false;
    for (unsigned ai=1;ai<argc;ai++) {
        const char *arg = argv[ai];
        if (arg[0]=='-')
        {
            if (memicmp(arg+1,"header:",7)==0)
                header.clear().append(arg+8);
            else if (memicmp(arg+1,"footer:",7)==0)
                footer.clear().append(arg+8);
            else if (memicmp(arg+1,"glue:",5)==0)
                glue.clear().append(arg+6);
            else if (memicmp(arg+1,"prefix:",7)==0)
                prefix.clear().append(arg+8);
            else if (stricmp(arg+1,"r")==0)
                sub = true;
        }
        else {
            if (dstarg)
                srcargs.append(dstarg);
            dstarg = ai;
        }
    }
    if (!dstarg||!srcargs.ordinality()) 
        usage();
    try {
        if (isWild(argv[dstarg])) {
            printf("ERROR Target %s cannot be wild!\n",argv[dstarg]);
            return 1;
        }
        Owned<IFile> dst = createIFile(argv[dstarg]);
        if (!dst||dst->exists()) {
            printf("ERROR Target %s already exists\n",argv[dstarg]);
            return 1;
        }
        Owned<IFileIO> dstio = dst->open(IFOcreate);
        if (!dstio) {
            printf("ERROR Could not open Target %s\n",argv[dstarg]);
            return 1;
        }
        Owned<IFileIOStream> dststrm = createIOStream(dstio);
        if (header.length()) 
            dststrm->write(header.length(),header.str());
        StringAttrArray srcnames;
        ForEachItemIn(i,srcargs) 
            if (!addFilename(srcnames,argv[srcargs.item(i)],sub))
                return 1;
        MemoryBuffer pref;
        ForEachItemIn(j,srcnames) {
            Owned<IFile> src = createIFile(srcnames.item(j).text.get());
            Owned<IFileIO> srcio = src?src->open(IFOread):NULL;
            if (!srcio) {
                printf("ERROR Could not open Source %s\n",srcnames.item(j).text.get());
                return 1;
            }
            if (j&&glue.length()) 
                dststrm->write(glue.length(),glue.str());
            if (prefix.length()) {
                genPrefix(pref.clear(),prefix.str(),srcnames.item(j).text.get(),srcio->size());
                if (pref.length())
                    dststrm->write(pref.length(),pref.toByteArray());
            }
            appendFile(srcio,dststrm,sub);
        }
        if (footer.length()) 
            dststrm->write(footer.length(),footer.str());
    }
    catch (IException * e) {
        pexception("COMBINE: ",e);
        e->Release();
    }
    releaseAtoms();
    return 0;
}
