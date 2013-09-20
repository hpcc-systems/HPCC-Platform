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


#include "platform.h"
/* Overloaded and redefined operators */
#include "jlib.hpp"
#include "jiface.hpp"
#include "jfile.hpp"
#include "jlzw.hpp"
#include "jio.hpp"
#include "jflz.hpp"

void doexit(int err)
{
    releaseAtoms();
    exit(err);
}

void usage(bool isHelp)
{
    printf("Usage:\n");
    printf("   copyexp <file>                     -- returns compress type\n");
    printf("   copyexp <file> <destination>       -- copies file to destination\n");
    printf("                                         (expanding as needed)\n");
    printf("   copyexp -z <file> <dest>           -- compresses file (LZW)\n");
    printf("   copyexp -f <file> <dest>           -- compresses file (FastLZ)\n");
    printf("   copyexp -r <recsz> <file> <dest>   -- compresses file (RowDif)\n");
    printf("   copyexp -fs <file> <dest>          -- compresses file (FastLZ stream)\n");
    printf("           -s                         -- timing stats\n");
    doexit(isHelp ? 0 : 2);
}

#define BUFFERSIZE (0x100000)

void printCompDetails(const char *fname,IFileIO *baseio,ICompressedFileIO *cmpio,IFileIOStream *flzstrm)
{
    const char *method = "Unknown Method";
    if (flzstrm)
        method = "FLZSTREAM";
    else {
        switch (cmpio->method()) {
        case COMPRESS_METHOD_ROWDIF:  method = "ROWDIF"; break;
        case COMPRESS_METHOD_LZW:     method = "LZW"; break;
        case COMPRESS_METHOD_FASTLZ:  method = "FASTLZ"; break;
        }
    }
    printf("%s: is %s compressed, size= %"I64F"d, expanded= %"I64F"d",fname,method,baseio->size(),flzstrm?flzstrm->size():cmpio->size());
    if (!flzstrm&&cmpio->recordSize())
        printf(", record size = %d",cmpio->recordSize());
    printf("\n");
}

static const char *formatTime(unsigned t,StringBuffer &str)
{
    str.clear();
    if (t>100000)
        str.appendf("%ds",t/1000);
    else if (t>100000)
        str.appendf("%dms",t);
    return str.str();

}

static const char *formatTimeU(unsigned t,StringBuffer &str)
{
    str.clear();
    if (t>100000000)
        str.appendf("%ds",t/1000000);
    else if (t>100000)
        str.appendf("%dms",t/1000);
    else
        str.appendf("%dus",t);
    return str.str();

}

static void printStats(offset_t filesize,unsigned start,unsigned startu)
{
    StringBuffer tmp;
    unsigned elapsed = msTick()-start;
    unsigned elapsedu = usTick()-startu;
    if (!elapsedu)
        elapsedu = 1;
    if (elapsed<1000)
        printf("%"I64F"d bytes copied, at %.2f MB/s in %s\n",filesize,((((double)filesize)/(1024*1024))/elapsedu)*1000000,formatTimeU(elapsedu,tmp));
    else
        printf("%"I64F"d bytes copied, at %.2f MB/s in %s\n",filesize,((((double)filesize)/(1024*1024))/elapsed)*1000,formatTime(elapsed*1000,tmp));
}

int copyExpanded(const char *from, const char *to, bool stats)
{
    Owned<IFile> srcfile = createIFile(from);
    Owned<IFileIO> srcio = srcfile->open(IFOread);
    if (!srcio) {
        printf("ERROR: could not open '%s' for read\n",from);
        doexit(3);
    }
    Owned<ICompressedFileIO> cmpio = createCompressedFileReader(srcio);
    Owned<IFileIOStream>  flzstrm = cmpio?NULL:createFastLZStreamRead(srcio);
    int ret = 0;
    if (cmpio||flzstrm) 
        printCompDetails(from,srcio,cmpio,flzstrm);
    else {
        ret = 1;
        printf("%s is not compressed, size= %"I64F"d\n",from,srcio->size());
    }
    if (!to||!*to)
        return ret;
    Owned<IFile> dstfile = createIFile(to);
    StringBuffer fulldst;
    if (dstfile->isDirectory()==foundYes) {
        dstfile.clear();
        addPathSepChar(fulldst.append(to)).append(pathTail(from));
        to = fulldst.str();
        dstfile.setown(createIFile(to));
    }

    if (dstfile->exists()) {
        printf("ERROR: file '%s' already exists\n",to);
        doexit(4);
    }
    unsigned start = 0;
    unsigned startu = 0;
    if (stats) {
         start = msTick();
         startu = usTick();
    }
    Owned<IFileIO> dstio = dstfile->open(IFOcreate);
    if (!dstio) {
        printf("ERROR: could not open '%s' for write\n",to);
        doexit(5);
    }
#ifdef __linux__
    // this is not really needed in windows - if it is we will have to
    // test the file extension - .exe, .bat

    struct stat info;
    if (stat(from, &info) == 0)  // cannot fail - exception would have been thrown above
        dstfile->setCreateFlags(info.st_mode&(S_IRUSR|S_IRGRP|S_IROTH|S_IWUSR|S_IWGRP|S_IWOTH|S_IXUSR|S_IXGRP|S_IXOTH));
#endif
    MemoryAttr mb;
    void * buffer = mb.allocate(BUFFERSIZE);

    offset_t offset = 0;
    try
    {
        loop {
            size32_t got = cmpio.get()?cmpio->read(offset,BUFFERSIZE, buffer):
                (flzstrm?flzstrm->read(BUFFERSIZE, buffer):
                    srcio->read(offset, BUFFERSIZE, buffer));
            if (got == 0)
                break;
            dstio->write(offset, got, buffer);
            offset += got;
        }
    }
    catch (IException *e)
    {
        // try to delete partial copy
        dstio.clear();
        try {
            dstfile->remove();
        }
        catch (IException *e2) {
            StringBuffer s;
            pexception(s.clear().append("Removing partial copy file: ").append(to).str(),e2);
            e2->Release();
        }
        throw e;
    }
    dstio.clear();
    if (stats) 
        printStats(offset,start,startu);
    CDateTime createTime, modifiedTime;
    if (srcfile->getTime(&createTime, &modifiedTime, NULL))
        dstfile->setTime(&createTime, &modifiedTime, NULL);
    printf("copied %s to %s%s\n",from,to,cmpio.get()?" expanding":"");
    return 0;
}


void copyCompress(const char *from, const char *to, size32_t rowsize, bool fast, bool flzstrm, bool stats)
{
    Owned<IFile> srcfile = createIFile(from);
    Owned<IFileIO> baseio = srcfile->open(IFOread);
    if (!baseio) {
        printf("ERROR: could not open '%s' for read\n",from);
        doexit(3);
    }
    Owned<ICompressedFileIO> cmpio = createCompressedFileReader(baseio);
    Owned<IFileIOStream>  flzstrmsrc = cmpio?NULL:createFastLZStreamRead(baseio);
    bool plaincopy = false;
    IFileIO *srcio = NULL;
    if (cmpio) {
        srcio = cmpio;
        if (rowsize&&(cmpio->recordSize()==rowsize))
            plaincopy = true;
        else if (!rowsize) {
            if (fast&&(cmpio->method()==COMPRESS_METHOD_FASTLZ))
                plaincopy = true;
            else if (!fast&&(cmpio->method()==COMPRESS_METHOD_LZW))
                plaincopy = true;
        }
    }
    else if (flzstrmsrc) {
        if (flzstrm)
            plaincopy = true;
    }
    else
        srcio = baseio; 
    if (plaincopy) {
        cmpio.clear();
        srcio = baseio.get(); 
    }
    Owned<IFile> dstfile = createIFile(to);
    StringBuffer fulldst;
    if (dstfile->isDirectory()==foundYes) {
        dstfile.clear();
        addPathSepChar(fulldst.append(to)).append(pathTail(from));
        to = fulldst.str();
        dstfile.setown(createIFile(to));
    }

    if (dstfile->exists()) {
        printf("ERROR: file '%s' already exists\n",to);
        doexit(4);
    }
    unsigned start = 0;
    unsigned startu = 0;
    if (stats) {
         start = msTick();
         startu = usTick();
    }
    Owned<IFileIO> dstio;
    Owned<IFileIOStream>  flzstrmdst;
    if (plaincopy||flzstrm) {
        dstio.setown(dstfile->open(IFOcreate));
        if (dstio&&!plaincopy)
            flzstrmdst.setown(createFastLZStreamWrite(dstio));
    }
    else 
        dstio.setown(createCompressedFileWriter(dstfile,rowsize,false,true,NULL,fast));

    if (!dstio) {
        printf("ERROR: could not open '%s' for write\n",to);
        doexit(5);
    }
#ifdef __linux__
    // this is not really needed in windows - if it is we will have to
    // test the file extension - .exe, .bat

    struct stat info;
    if (stat(from, &info) == 0)  // cannot fail - exception would have been thrown above
        dstfile->setCreateFlags(info.st_mode&(S_IRUSR|S_IRGRP|S_IROTH|S_IWUSR|S_IWGRP|S_IWOTH|S_IXUSR|S_IXGRP|S_IXOTH));
#endif
    MemoryAttr mb;
    void * buffer = mb.allocate(BUFFERSIZE);

    offset_t offset = 0;
    try
    {
        loop {
            size32_t got = cmpio.get()?cmpio->read(offset, BUFFERSIZE, buffer):srcio->read(offset, BUFFERSIZE, buffer);
            if (got == 0)
                break;
            if (flzstrmdst)
                flzstrmdst->write(got,buffer);
            else
                dstio->write(offset, got, buffer);
            offset += got;
        }
    }
    catch (IException *e)
    {
        // try to delete partial copy
        dstio.clear();
        try {
            dstfile->remove();
        }
        catch (IException *e2) {
            StringBuffer s;
            pexception(s.clear().append("Removing partial copy file: ").append(to).str(),e2);
            e2->Release();
        }
        throw e;
    }
    flzstrmdst.clear();
    dstio.clear();
    if (stats) 
        printStats(offset,start,startu);
    CDateTime createTime, modifiedTime;
    if (srcfile->getTime(&createTime, &modifiedTime, NULL))
        dstfile->setTime(&createTime, &modifiedTime, NULL);
    printf("copied %s to %s%s\n",from,to,plaincopy?"":" compressing");
    { // print details 
        dstio.setown(dstfile->open(IFOread));
        if (dstio) {
            Owned<ICompressedFileIO> cmpio = createCompressedFileReader(dstio);
            Owned<IFileIOStream>  flzstrm = cmpio?NULL:createFastLZStreamRead(dstio);
            if (cmpio||flzstrm) 
                printCompDetails(to,dstio,cmpio,flzstrm);
            else 
                printf("destination %s not compressed\n",to);
        }
        else
            printf("destination %s could not be read\n",to);
    }
}




int main(int argc, char * const * argv)
{
    InitModuleObjects();
    int ret = 0;
    try
    {
        bool test=false;
        unsigned arg = 1;
        StringBuffer fname1;
        StringBuffer fname2;
        bool lzw = false;
        bool fast = false;
        bool flzstrm = false;
        bool stats = false;
        size32_t rowsz = 0;
        for (int a = 1; a<argc; a++) {
            const char *arg = argv[a];
            if (arg[0]=='-') {
                if(strcmp(arg, "-t") == 0)
                    test = true;
                else if(strcmp(arg, "-?") == 0)
                    usage(true);
                else if(strcmp(arg, "-h") == 0)
                    usage(true);
                else if(strcmp(arg, "-z") == 0) {
                    lzw = true;
                    continue;
                }
                else if(strcmp(arg, "-s") == 0) {
                    stats = true;
                    continue;
                }
                else if(strcmp(arg, "-f") == 0) {
                    fast = true;
                    continue;
                }
                else if(strcmp(arg, "-fs") == 0) {
                    flzstrm = true;
                    continue;
                }
                else if(strcmp(arg, "-r") == 0) {
                    if (a+1<argc) {
                        rowsz = atoi(argv[a+1]);
                        if (rowsz) {
                            a++;
                            continue;
                        }
                    }
                    usage(false);
                }
                else {
                    printf("ERROR unexpected parameter '%s'",arg);
                    usage(false);
                }
            }
            if (fname1.length()) {
                if (test||fname2.length()) {
                    printf("ERROR unexpected parameter '%s'",arg);
                    usage(false);
                }
                fname2.append(arg);
            }
            else
                fname1.append(arg);
        }
        if (!fname1.length())
            usage(true);
        if (!fast&&!lzw&&!rowsz&&!flzstrm)
            copyExpanded(fname1.str(),fname2.str(),stats);
        else
            copyCompress(fname1.str(),fname2.str(),rowsz,fast,flzstrm,stats);
    }
    catch(IException * e)
    {
        pexception("copyexp: ",e);
        e->Release();
        ret = 99;
    }
    releaseAtoms();
    return ret;
}


