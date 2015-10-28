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

#ifdef _WIN32
#define _WIN32_WINNT 0x0400
#include <windows.h>
#endif
#include "platform.h"
#include "thirdparty.h"

#include "jlib.hpp"
#include "jhtree.hpp"
#include "jio.hpp" 
#include "jstring.hpp"
#include "jfile.hpp"
#include "jexcept.hpp"
#include "jsocket.hpp"
#include "jlog.hpp"
#include "rmtfile.hpp"

#define USE_JLOG

extern bool outputPartsFiles(const char *daliserver,const char *cluster,const char *outdir,StringBuffer &errstr,bool verbose);
extern void applyPartsFile(IFileIO *in,void (* applyfn)(const char *,const char *));


static AtomRefTable *ignoreExt = NULL;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    ignoreExt = new AtomRefTable(true);
    return true;
}
MODULE_EXIT()
{
    ::Release(ignoreExt);
}

#define BUFSIZE 0x10000
#define MINCOMPRESS_THRESHOLD 0x8000
static StringAttr unixmirror("/mnt/mirror");

static bool checkMode = false;
static bool silent = false;
static bool verbose = false;
static bool compressAll = false;
static bool noCheckSlaveCount = false;
static bool inexactDateMatch = false;

#ifdef USE_JLOG // and why not?
#define println PROGLOG
#define printerr ERRLOG
#else
static void println(const char *format, ...) __attribute__((format(printf, 1, 2)))
{
    va_list x;
    va_start(x, format);
    vfprintf(stdout,format, x);
    fprintf(stdout,"\n");
    fflush(stdout);
    va_end(x);
}

static void printerr(const char *format, ...) __attribute__((format(printf, 1, 2)))
{
    va_list x;
    va_start(x, format);
    fprintf(stderr,"ERROR: ");
    vfprintf(stderr,format, x);
    fprintf(stderr,"\n");
    fflush(stderr);
    va_end(x);
}
#endif

static bool shouldCompressFile(const char *name)
{
    if (compressAll)
        return true;
    OwnedIFile file = createIFile(name);
    bool iskey = false;
    unsigned __int64 filesize = file->size();
    if (filesize < MINCOMPRESS_THRESHOLD)
    {
        if (verbose)
            println("File %s is too small to compress", name);
        return false;
    }
    return !isCompressedIndex(name);
}


static bool CopySingleFile(IFile *srcfile,IFile *dstfile, bool compress, bool suppressnotfounderrs)
{
    const char *source = srcfile->queryFilename();
    const char *target = dstfile->queryFilename();
#ifdef _WIN32
    if (compress && shouldCompressFile(source))
    {
        if (!silent)
            println("Copy %s to %s with compress", source, target);
        if (!checkMode)
        {
            HANDLE hTarget=::CreateFile(target,GENERIC_READ|GENERIC_WRITE,0,NULL,CREATE_NEW,0,NULL);
            USHORT compression=COMPRESSION_FORMAT_DEFAULT;
            DWORD bytes;
            if(::DeviceIoControl(hTarget, FSCTL_SET_COMPRESSION, &compression, sizeof(compression), NULL, 0, &bytes, NULL))
            {
                HANDLE hSource=::CreateFile(source,GENERIC_READ,0,NULL,OPEN_EXISTING,0,NULL);
                void *buf = malloc(BUFSIZE);
                loop
                {
                    DWORD read;
                    if (!::ReadFile(hSource, buf, BUFSIZE, &read, NULL))
                        throw makeOsExceptionV(GetLastError(), "Failed to read file %s", source);
                    if (read)
                    {
                        DWORD wrote;
                        if (!::WriteFile(hTarget, buf, read, &wrote, NULL))
                            throw makeOsExceptionV(GetLastError(), "Failed to write file %s", target);
                        assertex(wrote==read);
                    }
                    else
                        break;
                }
                FILETIME c, a, w;
                ::GetFileTime(hSource, &c, &a, &w);
                ::SetFileTime(hTarget, &c, &a, &w);
                ::CloseHandle(hSource);
                ::CloseHandle(hTarget);
                return true;
            }
            DWORD err=::GetLastError();
            ::CloseHandle(hTarget);
        }
        return checkMode;
    }
#endif
    if (!silent)
        println("Copy %s to %s", source, target);
    if(checkMode)
        return false;
    try {
        recursiveCreateDirectoryForFile(target);    // maybe should only do if fails
        dstfile->remove();
        srcfile->copyTo(dstfile,0x100000,NULL,true);
    }
    catch (IException *e) {
        if (suppressnotfounderrs) {
            if (srcfile&&!srcfile->exists()) { // its gone!
                if (verbose)
                    printerr("File %s no longer exists", source);
                e->Release();
                return true;
            }
        }
        StringBuffer msg("CopyFile(");
        msg.append(source).append(',').append(target).append("): ");
        e->errorMessage(msg);
        printerr("%s",msg.str());
        e->Release();
        return false;
    }
    return true;
}

void syncFile(const char *src, const char *dst)
{
    // from must exist otherwise ignore
    Owned<IFile> srcfile = createIFile(src);
    bool isdir;
    CDateTime srcdt;
    offset_t srcsz;
    if (srcfile->getInfo(isdir,srcsz,srcdt)) { // ignore if not there
        if (isdir)
            printerr("src file %s is directory, ignoring copy", src);
        else {
            Owned<IFile> dstfile = createIFile(dst);
            CDateTime dstdt;
            offset_t dstsz;
            if (dstfile->getInfo(isdir,dstsz,dstdt)) { // check if there
                if (isdir) {
                    printerr("dst file %s is directory, ignoring copy", dst);
                    return;
                }
                if ((srcsz==dstsz)&&srcdt.equals(dstdt,!inexactDateMatch))
                    return;
            }
            CopySingleFile(srcfile,dstfile, false, true);
        }
    }
}

static void usage()
{
    printf("\nBACKUPNODE sourcepath targetpath [options]\n");
    printf("   Copies and optionally compresses files from source to target\n\n");
    printf("BACKUPNODE -X <data-dir-path> -T slaveno numslaves myip backupip\n");
    printf("   Thor node backup mode - syncs named paths with adjacent drive\n\n");
    printf("BACKUPNODE -W slavesfile dir\n");
    printf("   Waits for .ERR files in the specified directory then concatenates into a log file\n\n");
    printf("BACKUPNODE -O daliip cluster outdir\n");
    printf("   generates data files in outdir containing all files to be checked (*.DAT) \n\n");
    printf("Options:\n");
    printf("  -A - compression options apply to all files (normally excludes small files and all keys)\n");
    printf("  -B - use /mnt/mirror for replicate target\n");
    printf("  -C - compress files on target (including existing files)\n");
    printf("  -D - overwrite existing files if size/date mismatch\n");
    printf("  -E - set compression state of existing files\n");
    printf("  -F <file> - use option XML file\n");
    printf("  -I <ext> - ignore files that have specified extension\n");
    printf("  -M - ignore sub-second differences when comparing file dates\n");
    printf("  -N - Include files even if slave count does not match filename\n");
    printf("  -Q - quiet mode: only errors are reported\n");
    printf("  -V - verbose mode\n");
    printf("  -Y - report what would have been copied/compressed but do nothing\n");
    printf("  -S - snmp enabled\n");
    printf("  -X <dir> - read part lists (%%n.DAT) from and write %%n.ERR to specified dir\n");
    exit(2);
}

static bool different(IFile &target, IFile &source)
{
    CDateTime tmt, smt;
    if (target.size() != source.size())
        return true;
    target.getTime(NULL, &tmt, NULL);
    if (inexactDateMatch)
    {
        unsigned hour, min, sec, nanosec;
        tmt.getTime(hour, min, sec, nanosec);
        tmt.setTime(hour, min, sec, 0);
    }
    source.getTime(NULL, &smt, NULL);
    if (inexactDateMatch)
    {
        unsigned hour, min, sec, nanosec;
        smt.getTime(hour, min, sec, nanosec);
        smt.setTime(hour, min, sec, 0);
    }
    return tmt.compare(smt) != 0;
}

static bool includeFile(IFile &file, unsigned numSlaves)
{
    StringBuffer ext;
    splitFilename(file.queryFilename(), NULL, NULL, NULL, &ext);
    const char *_ext = ext.length()?ext.str()+1:"";
    if (ignoreExt->find(*_ext))
        return false;
    if (!numSlaves || noCheckSlaveCount)
        return true;
    const char *partcount = strstr(ext.str(), "_of_");
    if (partcount)
    {
        unsigned clusterSize = atoi(partcount+4);
        return clusterSize==numSlaves || clusterSize==numSlaves+1;
    }
    else
        return false;
}

static void CopyDirectory(const char *source, const char *target, unsigned numSlaves, bool compress, bool sourceIsMaster)
{
    if (verbose)
        println("Copy directory %s to %s", source, target);
    bool first = true;
    Owned<IDirectoryIterator> dir = createDirectoryIterator(source, "*");
    ForEach (*dir)
    {
        IFile &sourceFile = dir->query();
        if (sourceFile.isFile())
        {
            if (includeFile(sourceFile, numSlaves))
            {
                StringBuffer targetname(target);
                targetname.append(PATHSEPCHAR);
                dir->getName(targetname);
                OwnedIFile destFile = createIFile(targetname.str());
                if ((destFile->size()==-1) || (sourceIsMaster && different(*destFile, sourceFile)))
                {
                    if (first && !checkMode)
                    {
                        if (!recursiveCreateDirectory(target)) {
                            throw MakeStringException(-1,"Cannot create directory %s",target);
                        }
                        first = false;
                    }
                    if (!CopySingleFile(&sourceFile, destFile, compress, true))
                        printerr("File %s copy to %s failed", sourceFile.queryFilename(), destFile->queryFilename());
                }
                else if (verbose)
                {
                    println("File %s already exists", destFile->queryFilename());
                }
            }
            else if (verbose)
                println("Skipping file %s (cluster size mismatch)", sourceFile.queryFilename());
        }
        else if (sourceFile.isDirectory())
        {
            StringBuffer newSource(source);
            StringBuffer newTarget(target);
            newSource.append(PATHSEPCHAR);
            newTarget.append(PATHSEPCHAR);
            dir->getName(newSource);
            dir->getName(newTarget);
            CopyDirectory(newSource.str(), newTarget.str(), numSlaves, compress, sourceIsMaster);
        }
    }
    if (verbose)
        println("Copied directory %s to %s", source, target);
}

static void CompressDirectory(const char *target, unsigned numSlaves, bool compress)
{
#ifdef _WIN32
    if (verbose)
        println("%s directory %s", compress ? "Compress" : "Decompress", target);
    Owned<IDirectoryIterator> dir = createDirectoryIterator(target, "*");
    ForEach (*dir)
    {
        IFile &targetFile = dir->query();
        if (targetFile.isFile())
        {
            if (includeFile(targetFile, numSlaves))
            {
                // Quick test to see if it's a key file.
                bool compressThis = compress && shouldCompressFile(targetFile.queryFilename());
                DWORD attr=::GetFileAttributes(targetFile.queryFilename());
                if (attr==-1)
                    printerr("Could not read compression state of %s: error %x", targetFile.queryFilename(), ::GetLastError());
                else
                {
                    bool compressed = (attr & FILE_ATTRIBUTE_COMPRESSED) != 0;
                    if (compressed != compressThis)
                    {
                        if (!silent)
                        {
                            if (compressThis)
                                println("Compress %s before %" I64F "d", targetFile.queryFilename(), targetFile.size());
                            else
                                println("Decompress %s before %" I64F "d", targetFile.queryFilename(), targetFile.compressedSize());
                        }
                        if (!checkMode)
                            targetFile.setCompression(compressThis);
                        if (!silent)
                        {
                            if (compressThis)
                            {
                                if (checkMode)
                                    println("");  // size after not known
                                else
                                    println("after %" I64F "d", targetFile.compressedSize());
                            }
                            else
                                println("after %" I64F "d", targetFile.size());
                        }
                    }
                }
            }
        }
        else if (targetFile.isDirectory())
        {
            StringBuffer newTarget(target);
            newTarget.append(PATHSEPCHAR);
            dir->getName(newTarget);
            CompressDirectory(newTarget.str(), numSlaves, compress);
        }
    }
    if (verbose)
        println("%s directory %s", compress ? "Compressed" : "Decompressed", target);
#endif
}


#define MAX_SLAVES 1000
static StringAttr slaveIP[MAX_SLAVES+1];
static unsigned numSlaves;

static void loadSlaves(const char *slavesName)
{
    FILE *slavesFile  = fopen(slavesName, "rt");
    if( !slavesFile)
    {
        printerr("failed to open slaves file %s", slavesName);
        throw MakeStringException(MSGAUD_operator, 0, "failed to open slaves file %s", slavesName);
    }
    char inbuf[1000];
    numSlaves = 0;
    while (fgets( inbuf, sizeof(inbuf), slavesFile))
    {
        char *hash = strchr(inbuf, '#');
        if (hash)
            *hash = 0;
        char *finger = inbuf;
        loop
        {
            while (isspace(*finger))
                finger++;
            char *start = finger;
            while (*finger && !isspace(*finger))
                finger++;
            if (finger > start)
                slaveIP[numSlaves ++].set(start, finger - start);
            else
                break;
            if (numSlaves > MAX_SLAVES)
            {
                printerr("Too many slaves - invalid slaves file %s?", slavesName);
                throw MakeStringException(MSGAUD_operator, 0, "Too many slaves - invalid slaves file %s?", slavesName);
            }
        }
    }
    fclose(slavesFile);
    slaveIP[numSlaves].set(slaveIP[0].get());
}

static void waitSlaves(const char *dir,unsigned num,StringAttr *slaves)
{
    unsigned start=msTick();
    unsigned last=0;
    bool *done = (bool *)calloc(num,sizeof(bool));
    unsigned ndone = 0;
    unsigned errors = 0;
    StringBuffer name;
    while (ndone<num) {
        unsigned startndone = ndone;
        for (unsigned i=0;i<num;i++) {
            if (!done[i]) {
                addPathSepChar(name.clear().append(dir)).append(i+1).append(".ERR");
                if (checkFileExists(name.str())) {
                    done[i] = true;
                    ndone++;
                    for (unsigned attempt=0;attempt<10;attempt++) {
                        try {
                            Owned<IFile> file = createIFile(name.str());
                            Owned<IFileIO> fio = file->open(IFOread);
                            if (fio) {
                                size32_t sz = (size32_t)fio->size();
                                if (sz) {
                                    StringBuffer s;
                                    fio->read(0,sz,s.reserve(sz));
                                    println("%s: %s",slaves[i].get(),s.str());
                                    errors++;
                                }
                                else {
                                    try {
                                        fio.clear();
                                        file->remove(); 
                                    }
                                    catch (IException *e) {
                                        StringBuffer msg("waitSlaves.1: ");
                                        e->errorMessage(msg);
                                        println("%s",msg.str());
                                        e->Release();
                                    }
                                    println("%s: DONE",slaves[i].get());
                                }
                                break;
                            }
                        }
                        catch (IException *e) {
                            if (attempt==9) {
                                StringBuffer msg("waitSlaves.2: ");
                                e->errorMessage(msg);
                                println("%s",msg.str());
                            }
                            e->Release();
                        }
                        Sleep(5000);
                    }
                }
            }
        }
        if (startndone==ndone) {
            Sleep(5000);
        }
        unsigned t = (msTick()-start)/(5*1000*60);
        if (t!=last) {
            last = t;
            println("Running: %d minutes taken, %d slave%s complete of %d",t*5,ndone,(ndone==1)?"":"s",num);
            if (num-ndone<10) {
                StringBuffer waiting;
                for (unsigned j=0;j<num;j++) {
                    if (!done[j]) {
                        if (waiting.length())
                            waiting.append(',');
                        waiting.append(slaves[j]);
                    }
                }
                println("Waiting for %s",waiting.str());
            }
        }
    }
    unsigned t2 = (msTick()-start)/1000;
    println("Completed in %dm %ds with %d error%s",t2/60,t2%60,errors,(errors==1)?"":"s");
    free(done);
}





int main(int argc, const char *argv[])
{
    InitModuleObjects();
    int retValue = 0;

    bool compress = false;
    bool compressExisting = false;
    bool overwriteDifferent = false;
    bool thorMode = false;
    bool waitMode = false;
    bool forceSlaveIP = false;
    bool snmpEnabled = false;
    bool useMirrorMount = false;
    bool outputMode = false;
    StringAttr errdatdir;
    StringArray args;
    unsigned slaveNum = 0;
    unsigned argNo = 1;
    while ((int)argNo<argc)
    {
        const char *arg = argv[argNo++];
        if (arg[0]=='-')
        {
            while (arg)
            {
                switch (toupper(arg[1]))
                {
                case 'A':
                    compressAll = true;
                    break;
                case 'B':
                    useMirrorMount = true;
                    break;
                case 'C':
                    compress = true;
                    println("NOTE - executing in check mode. No files will compressed or copied");
                    break;
                case 'D':
                    overwriteDifferent = true;
                    break;
                case 'E':
                    compressExisting = true;
                    break;
                case 'F':
                    forceSlaveIP = true;
                    break;
                case 'I':
                {
                    if ((int)argNo<argc)
                        ignoreExt->queryCreate(argv[argNo++]);
                    break;
                }
                case 'M':
                    inexactDateMatch = true;
                    break;
                case 'N':
                    noCheckSlaveCount = true;
                    break;
                case 'O':
                    outputMode = true;
                    break;
                case 'Q':
                    if (verbose)
                        println("Silent and verbose specified - silent will be ignored");
                    else
                        silent = true;
                    break;
                case 'S':
                    snmpEnabled = true;
                    break;
                case 'T':
                    thorMode = true;
                    break;
                case 'V':
                    if (silent)
                    {
                        println("Silent and verbose specified - silent will be ignored");
                        silent = false;
                    }
                    verbose = true;
                    break;
                case 'W':
                    waitMode = true;
                    break;
                case 'X':
                    if ((int)argNo<argc)
                        errdatdir.set(argv[argNo++]);
                    break;
                case 'Y':
                    checkMode = true;
                    break;
                default:
                    usage();
                    break;
                }
                if (arg[2]=='/' || arg[2]=='-')
                    arg += 2;
                else if (arg[2])
                    usage();
                else
                    arg = NULL;
            }
        }
        else
            args.append(arg);
    }

    if (args.ordinality()<2)
        usage();
    StringBuffer erroutstr;
    try
    {
        if (thorMode)
        { 
            if (args.ordinality()<4 || 0 == errdatdir.length())
                usage();
            slaveNum = atoi(args.item(0));
            numSlaves = atoi(args.item(1));
            const char *myIp = args.item(2);
            const char *backupIp = args.item(3);

            setDaliServixSocketCaching(true); 
            if (!slaveNum || slaveNum>numSlaves)
            {
                printerr("'%s' is not a valid slave number (range is 1 to %d)", args.item(1), numSlaves);
                throw MakeStringException(-1, "'%s' is not a valid slave number (range is 1 to %d)", args.item(1), numSlaves);
            }
            if (!forceSlaveIP)
            {
                IpAddress myip;
                GetHostIp(myip);
                IpAddress myipfromSlaves(myIp);
                if (!myip.ipequals(myipfromSlaves))
                {
                    StringBuffer ips1, ips2;
                    myipfromSlaves.getIpText(ips1);
                    myip.getIpText(ips2);
                    printerr("IP address %d in slaves file %s does not match this machine %s", slaveNum, ips1.str(), ips2.str());
                    throw MakeStringException(-1, "IP address %d in slaves file %s does not match this machine %s", slaveNum, ips1.str(), ips2.str());
                }
            }
            StringBuffer datafile(errdatdir);
            addPathSepChar(datafile).append(slaveNum).append(".DAT");
            Owned<IFile> file = createIFile(datafile.str());
            Owned<IFileIO> fio;
            // add a slight stagger
            Sleep(slaveNum*200);
            for (unsigned attempt=0;attempt<10;attempt++) {
                try {
                    fio.setown(file->open(IFOread));
                    if (fio)
                        break;
                }
                catch (IException *e) {
                    if (attempt==9) {
                        StringBuffer msg;
                        e->errorMessage(msg);
                        printerr("%s",msg.str());
                    }
                    e->Release();
                }
                Sleep(5000);
            }
            if (fio)
                applyPartsFile(fio,syncFile);
            else {
                printerr("Could not read file %s",datafile.str());
                throw MakeStringException(-1, "Could not read file %s",datafile.str());
            }
        }
        else if (waitMode) {
            loadSlaves(args.item(0));
#ifndef _WIN32
            struct sigaction act;   // ignore break (from parent)
            sigset_t blockset;
            sigemptyset(&blockset);
            act.sa_mask = blockset;
            act.sa_handler = SIG_IGN;
            act.sa_flags = 0;
            sigaction(SIGINT, &act, NULL);
#endif
            waitSlaves(args.item(1),numSlaves,slaveIP);
        }
        else if (outputMode) {
            if (args.ordinality()<3)
                usage();
            else {
                if (!silent)
                    println("Creating part lists, please wait...");
                StringBuffer errstr;
                if (!outputPartsFiles(args.item(0),args.item(1),args.item(2),errstr,verbose))
                    throw MakeStringExceptionDirect(-1, errstr.str());
            }
        }
        else
        {
            const char *source = args.item(0);
            const char *target = args.item(1);
            if (compressExisting)
                CompressDirectory(target, 0, compress);
            CopyDirectory(source, target, 0, compress, overwriteDifferent);
        }

        if (checkMode)
            println("NOTE - executing in check mode. No files were compressed or copied");

        if(!silent)
            println("backupnode finished");

    }
    catch(IException *E)
    {
        E->errorMessage(erroutstr);
        printerr("%s",erroutstr.str());
        E->Release();
        retValue = 2;
    }

    if (errdatdir.length()&&slaveNum) {
        StringBuffer errfilename(errdatdir);
        addPathSepChar(errfilename).append(slaveNum).append(".ERR");
        Owned<IFile> file = createIFile(errfilename.str());
        for (unsigned attempt=0;attempt<10;attempt++) {
            try {
                Owned<IFileIO> fio = file->open(IFOcreate);
                if (fio) {
                    if (erroutstr.length()) {
                        if (erroutstr.charAt(erroutstr.length()-1)!='\n')
                            erroutstr.append('\n');
                        fio->write(0,erroutstr.length(),erroutstr.str());
                    }
                    releaseAtoms();
                    return retValue;
                }
            }
            catch (IException *e) {
                if (attempt==9) {
                    StringBuffer msg;
                    e->errorMessage(msg);
                    printerr("%s",msg.str());
                }
                e->Release();
            }
            Sleep(5000);
        }
        printerr("Could not write to %s",errfilename.str());
    }

    releaseAtoms();
    return retValue;
}
