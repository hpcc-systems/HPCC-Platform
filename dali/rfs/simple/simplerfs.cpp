// Trivial example of RFS

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#ifndef _CRT_SECURE_CPP_OVERLOAD_STANDARD_NAMES
#define _CRT_SECURE_CPP_OVERLOAD_STANDARD_NAMES 1
#undef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS 1
#endif
#undef UNICODE
#include <windows.h>
#include <io.h>
#include <sys/utime.h>
#define S_ISDIR(m) (((m)&_S_IFDIR)!=0)
#define S_ISREG(m) (((m)&_S_IFREG)!=0)

#define ALLOW_WINDOWS_SERVICE

#else
#include <unistd.h>
#include <utime.h>
#include <dirent.h>

#define _strdup strdup
#define _O_RDONLY O_RDONLY
#define _O_WRONLY O_WRONLY
#define _O_RDWR O_RDWR
#define _O_CREAT O_CREAT
#define _O_TRUNC O_TRUNC
#define _O_BINARY (0)
#define _open ::open
#define _read ::read
#define _write ::write
#define _lseek ::lseek
#define _close ::close
#define _unlink unlink
#define _utimbuf utimbuf
#define _utime utime
#endif

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <time.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "rfs.h"

static const char * safestrcpy(char *dst,const char *src,size_t max)
{
    const char *ret = dst;
    if (dst&&src&&max) {
        while (--max&&*src)
            *(dst++)=*(src++);
        *dst = 0;
    }
    return ret;
}



class CSimpleRFSconn: public RFS_ConnectionBase
{
    // NB this class relies on single threaded nature of RFS
    RFS_ServerBase &base;
    int handle;
    char *filename;
public:
    CSimpleRFSconn(RFS_ServerBase &_base)
        : base(_base)
    {
        handle = -1;
        filename = NULL;
    }
    ~CSimpleRFSconn()
    {
        close();
    }

    int open(const char *_filename,byte mode, byte share)
    {
        int openflags;
        switch (mode&RFS_OPEN_MODE_MASK) {
        case RFS_OPEN_MODE_CREATE:
            openflags = _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY;
            break;
        case RFS_OPEN_MODE_READ:
            openflags = _O_RDONLY | _O_BINARY;
            break;
        case RFS_OPEN_MODE_WRITE:
            openflags = _O_WRONLY | _O_CREAT | _O_BINARY;
            break;
        case RFS_OPEN_MODE_CREATERW:
            openflags = _O_RDWR | _O_CREAT | _O_TRUNC | _O_BINARY;
            break;
        case RFS_OPEN_MODE_READWRITE:
            openflags = _O_RDWR | _O_CREAT | _O_BINARY;
            break;
        default:
            return EACCES;
        }
        int rights = 0;
#ifdef _WIN32
        rights = _S_IREAD|_S_IWRITE; // don't support linux style rights
#else
        switch (share) {
        case RFS_SHARING_NONE:
            rights = S_IRUSR|S_IWUSR;
        case RFS_SHARING_EXEC:
            rights = S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;
            break;
        case RFS_SHARING_ALL:
            rights = S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH; // bit excessive
            break;
        }
#endif
        handle = _open(_filename,openflags,rights);
        if (handle==-1)
            return errno;
        free(filename);
        filename = _strdup(_filename);
        return 0;
    }

    void read(rfs_fpos_t pos, size_t len, size_t &outlen, void *out)
    {
        long ret = (long)_lseek(handle,(long)pos,SEEK_SET);
        outlen = 0;
        if (ret!=pos)
            base.throwError(errno,"read.1");
        int rd = _read(handle,out,len);
        if (rd==-1)
            base.throwError(errno,"read.2");
        outlen = (size_t)rd;
    }

    rfs_fpos_t size()
    {
        long savedPos = (long)_lseek(handle,0,SEEK_CUR);
        if (savedPos == -1)
            base.throwError(errno,"size.1");
        long length = (long)_lseek(handle,0,SEEK_END);
        if (length==-1)
            base.throwError(errno,"size.2");
        if ((long)_lseek(handle,savedPos,SEEK_SET)!=savedPos)
            base.throwError(errno,"size.3");
        return (rfs_fpos_t)length;
    }

    void close()
    {
        if (handle!=-1)
            _close(handle); // could check error here
        handle = -1;
        free(filename);
        filename = NULL;
    }

    void write(rfs_fpos_t pos, size_t len, const void *in)
    {
        long ret = (long)_lseek(handle,(long)pos,SEEK_SET);
        if (ret!=pos)
            base.throwError(errno,"write.1");
        int wr = _write(handle,in,len);
        if (wr==-1)
            base.throwError(errno,"write.2");
        if ((size_t)wr!=len)  // disk full
            base.throwError(ENOSPC,"write.3");
    }

};

#ifdef _WIN32
#define PATHSEPCHAR '\\'
#else
#define PATHSEPCHAR '/'
#endif


class cSimpleDir
{
    RFS_SimpleString path;
    char *tail;
    size_t pathhead;
    int level;
    char *mask;
    bool includedirs;
    bool recursive;
    bool first;


#ifdef _WIN32
    typedef HANDLE _Handle;

    time_t FileTimeToUnixTime(LPFILETIME pft)
    {
        return (time_t)((*(__int64 *)pft-116444736000000000i64)/10000000i64);
    }
#else

    DIR *handle;
    typedef DIR * _Handle;
#define INVALID_HANDLE_VALUE ((DIR *)NULL)
#endif

    _Handle *handles;


    bool WildMatchN ( const char *src, size_t srclen, size_t srcidx, const char *pat, size_t patlen, size_t patidx, bool nocase)
    {
        char next_char;
        while (1) {
            if (patidx == patlen)
                return (srcidx == srclen);
            next_char = pat[patidx++];
            if (next_char == '?') {
                if (srcidx == srclen)
                    return false;
                srcidx++;
            }
            else if (next_char != '*') {
                if (nocase) {
                    if ((srcidx == srclen) ||
                        (toupper(src[srcidx])!=toupper(next_char)))
                        return false;
                }
                else
                    if ((srcidx == srclen) || (src[srcidx]!=next_char))
                        return false;
                    srcidx++;
            }
            else {
                while (1)
                {
                    if (patidx == patlen)
                        return true;
                    if (pat[patidx] != '*')
                        break;
                    patidx++;
                }
                while (srcidx < srclen) {
                    if (WildMatchN(src,srclen,srcidx,
                        pat, patlen, patidx,nocase))
                        return true;
                    srcidx++;
                }
                return false;
            }
        }
        return false;
    }

    bool WildMatch(const char *src, const char *pat, bool nocase)
    {
        size_t srclen = strlen(src);
        size_t patlen = strlen(pat);
        if (pat[0]=='*') {
            // common case optimization
            int i = patlen;
            int j = srclen;
            while (--i>0) {
                if (pat[i]=='*') goto Normal;
                if (j--==0) return false;
                if (nocase) {
                    if ((toupper(pat[i])!=toupper(src[j]))&&(pat[i]!='?'))
                        return false;
                }
                else
                    if ((pat[i]!=src[j])&&(pat[i]!='?'))
                        return false;
            }
            return true;
        }
Normal:
        return WildMatchN(src,srclen,0,pat,patlen,0,nocase);
    }


public:

    cSimpleDir(const char *dirname, const char *_mask, bool _recursive, bool _includedirs)
    {
        path.appends(dirname);
        if (path.lastChar()!='\\')
            path.appendc('\\');
        pathhead = path.length();
        level = 0;
        mask = _mask?_strdup(_mask):NULL;
        includedirs = _includedirs;
        recursive = _recursive;
        first = true;
        handles = (_Handle *)malloc(sizeof(_Handle));
        tail = NULL;
    }

    ~cSimpleDir()
    {
        while ((level>=0)&&(handles[level] != INVALID_HANDLE_VALUE))    {
#ifdef _WIN32
            FindClose(handles[level--]);
#else
            closedir(handles[level--]);
#endif
        }
        free(handles);
        free(mask);
        free(tail);
    }


    void next(size_t maxfilenamesize, char *outfilename, bool &isdir, rfs_fpos_t &filesizeout, time_t &outmodifiedtime)
    {
#ifdef _WIN32
        WIN32_FIND_DATA info;
        bool nocase = true;
#else
        struct dirent *entry;
        bool nocase = false;
#endif
        while (1) {
            *outfilename = 0;
            if (level<0)
                return;
            if (first) {
                first = false;
                handles = (_Handle *)realloc(handles,sizeof(_Handle)*(level+1));
#ifdef _WIN32
                path.appendc('*');
                handles[level] = FindFirstFile(path.str(), &info);
                path.decLength();
#else
                handles[level] = opendir(path.str());
                if (handles[level])
                    entry = readdir(handles[level]);  // don't need _r here
#endif
            }
            else {
#ifdef _WIN32
                if (!FindNextFile(handles[level], &info)) {
                    FindClose(handles[level]);
#else
                entry = readdir(handles[level]);  // don't need _r here
                if (!entry) {
                    closedir(handles[level]);
#endif
                    handles[level] = INVALID_HANDLE_VALUE;
                }
            }
            if (handles[level]!=INVALID_HANDLE_VALUE) {
                free(tail);
#ifdef _WIN32
                tail = _strdup(info.cFileName);
                isdir = ((info.dwFileAttributes&FILE_ATTRIBUTE_DIRECTORY)!=0);
#else
                tail = _strdup(entry->d_name);
#endif
                size_t hs = path.length()-pathhead;
                size_t ts = strlen(tail);
                if (hs>=maxfilenamesize)
                    hs = maxfilenamesize-1;
                if (hs+ts>=maxfilenamesize)
                    ts = maxfilenamesize-1-hs;
                memcpy(outfilename,path.str()+pathhead,hs);
                memcpy(outfilename+hs,tail,ts);
                outfilename[hs+ts] = 0;
#ifndef _WIN32
                struct stat info;
                if (stat(outfilename, &info) != 0)  // will follow link
                    continue;
                isdir = S_ISDIR(info.st_mode);
#endif

                if ((strcmp(tail,".")==0)||(strcmp(tail,"..")==0))
                    continue;
                bool matched = (mask&&*mask)?WildMatch(tail,mask,nocase):false;
                if (!matched&&(!recursive||!isdir))
                    continue;
                if (isdir&&!recursive&&!includedirs)
                    continue;
                if (isdir) {
                    if (recursive) {
                        // add name
                        path.appends(tail);
                        if (path.lastChar()!='\\')
                            path.appendc('\\');
                        first = true;
                        level++;
                    }
                    if (!includedirs||!matched)
                        continue;
                    filesizeout = (rfs_fpos_t)0;
                }
                else {
#ifdef _WIN32
                    LARGE_INTEGER x;
                    x.LowPart = info.nFileSizeLow;
                    x.HighPart = info.nFileSizeHigh;
                    filesizeout = (rfs_fpos_t)x.QuadPart;
                }
                outmodifiedtime = FileTimeToUnixTime(&info.ftLastWriteTime);
#else
                    filesizeout = info.st_size;
                }
                outmodifiedtime = info.st_mtime;
#endif
                break;
            }
            level--;
            if (level<0)
                return;
            if (path.lastChar()=='\\')
                path.decLength();
            while (path.length()) {
                if (path.lastChar()=='\\')
                    break;
                path.decLength();

            }
        }
    }

};


class CSimpleRFS: public RFS_ServerBase
{

#ifdef _WIN32

    bool WindowsCreateDirectory(const char * path)
    {
        if (CreateDirectory(path, NULL))
            return true;
        DWORD err = GetLastError();
        if ((err==ERROR_FILE_NOT_FOUND) || (err==ERROR_PATH_NOT_FOUND) || (err==ERROR_FILE_EXISTS) || (err==ERROR_CANNOT_MAKE))
            return false;
        if (err==ERROR_ALREADY_EXISTS) {
            DWORD attr = GetFileAttributes(path);
            if ((attr != -1)&&( attr & FILE_ATTRIBUTE_DIRECTORY))
                return true;
        }
        return false;
    }

    #else


    bool LinuxCreateDirectory(const char * path)
    {
        if (!path)
            return false;
        if (mkdir(path,S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH)==0)
            return true;
        else
        {
            if (EEXIST == errno)
            {
                struct stat info;
                if (stat(path, &info) != 0)
                    return false;
                return S_ISDIR(info.st_mode);
            }
        }
        return false;
    }


    #endif


public:

    RFS_ConnectionBase * open(const char *name, byte mode, byte share)
    {
        CSimpleRFSconn *conn = new CSimpleRFSconn(*this);
        int err = conn->open(name,mode,share);
        if (err) {
            delete conn;
            conn = NULL;
            if (err==ENOENT)
                return false;
            throwError(err,"open");
        }
        return conn;
    }

    void existFile(const char *filename, bool &existsout)
    {
        struct stat info;
        existsout = (stat(filename, &info) == 0);
    }
    void removeFile(const char *filename)
    {
        bool isdir;
        isDir(filename,isdir);
        if (isdir) {
#ifdef _WIN32
            // no rmdir?
            if (RemoveDirectory(filename)==0)
                return;
            DWORD err = GetLastError();
            if ( (err==ERROR_FILE_NOT_FOUND) || (err==ERROR_PATH_NOT_FOUND) )
                return;
            throwError((int)err,"removeFile"); // shouldn't really pass win error here
#else
            if (rmdir(filename) == 0)
                return;
#endif
        }
        else {
            if (_unlink(filename) == 0)
                return;
        }
        if (ENOENT!=errno)
            throwError(errno,"removeFile");
    }
    void renameFile(const char *fromname,const char *toname)
    {
        if (::rename(fromname, toname))
            throwError(errno,"rename");
    }
    void getFileTime(const char *filename, time_t &outaccessedtime, time_t &outcreatedtime, time_t &outmodifiedtime)
    {
        struct stat info;
        if (stat(filename, &info) == 0) {
            outaccessedtime =info.st_atime;
            outcreatedtime = info.st_ctime;
            outmodifiedtime = info.st_mtime;
        }
    }
    void setFileTime(const char *filename, time_t *inaccessedtime, time_t *increatedtime, time_t *inmodifiedtime) // params NULL if not to be set
    {
        // only supports accessed and modified currently
        struct _utimbuf am;
        if (!inaccessedtime||!inmodifiedtime) {
            struct stat info;
            if (stat(filename, &info) != 0)
                return;
            am.actime = info.st_atime;
            am.modtime = info.st_mtime;
        }
        if (inaccessedtime)
            am.actime   = *inaccessedtime;
        if (inmodifiedtime)
            am.modtime  = *inmodifiedtime;
        _utime(filename, &am);

    }
    void isFile(const char *filename, bool &outisfile)
    {
        outisfile = false;
        struct stat info;
        if (stat(filename, &info) == 0)
            outisfile = S_ISREG(info.st_mode);
    }
    void isDir(const char *filename, bool &outisdir)
    {
        outisdir = false;
        struct stat info;
        if (stat(filename, &info) == 0)
            outisdir = S_ISDIR(info.st_mode);
    }

    void isReadOnly(const char *filename, bool &outisreadonly)
    {
        //TBD
    }
    void setReadOnly(const char *filename, bool readonly)
    {
        // TBD
    }



    void createDir(const char *name,bool &createdout)
    {
        if (!name) {
            createdout = false;
            return;
        }
        createdout = true;
        size_t l = strlen(name);
        if (l==0)
            return;
        if ((name[0]==PATHSEPCHAR)&&((l==1)||((name[1]==PATHSEPCHAR)&&!strchr(name+2,PATHSEPCHAR))))
            return;
#ifdef _WIN32
        if (name[1]==':') {
            if ((l==2)||((l==3)&&(name[2]=='\\')))
                return;
        }
#endif
        bool isdir;
        isDir(name,isdir);
        if (isdir)
            return;
#ifdef _WIN32
        if (WindowsCreateDirectory(name))
#else
        if (LinuxCreateDirectory(name))
#endif
            return;
        RFS_SimpleString parent(name);
        if (parent.lastChar()==PATHSEPCHAR)
            parent.decLength();
        while (parent.length()&&(parent.lastChar()!=PATHSEPCHAR))
            parent.decLength();
        if (parent.length()<=1)
            return;
        createDir(parent.str(),createdout);
        if (!createdout)
            return;
#ifdef _WIN32
        createdout = WindowsCreateDirectory(name);
#else
        createdout = LinuxCreateDirectory(name);
#endif
    }


    void openDir(const char *dirname, const char *mask, bool recursive, bool includedirs, void *&outhandle)
    {
        outhandle = new cSimpleDir(dirname,mask,recursive,includedirs);
    }
    void nextDirEntry(void *handle, size_t maxfilenamesize, char *outfilename, bool &isdir, rfs_fpos_t &filesizeout, time_t &outmodifiedtime)  // empty return for filename marks end
    {
        outfilename[0] = 0;
        if (handle)
            ((cSimpleDir *)handle)->next(maxfilenamesize,outfilename,isdir,filesizeout,outmodifiedtime);
    }
    void closeDir(void *handle)
    {
        delete ((cSimpleDir *)handle);
    }

    void getVersion(size_t programnamemax, char *programname, short &version)
    {
        safestrcpy(programname,"SimpleRFS",programnamemax);
        version = 1;
    }

    int run()
    {
        return RFS_ServerBase::run();
    }

#ifdef ALLOW_WINDOWS_SERVICE
    void getServiceName(size_t maxname, char *outname, size_t maxdisplayname, char *outdisplayname)
    {
        safestrcpy(outname,"SimpleRFS",maxname);
        safestrcpy(outdisplayname,"RFS example",maxdisplayname);
    }

    int serviceInit(int argc, const char **argv, bool &outmulti)
    {
        // My initializtion here
        outmulti = false;
        return 0;
    }

#endif


};

void usage()
{
    printf("Usage: simplerfs { <option> }\n");
    printf("Options:\n");
    printf("  --port=<port>\n");    // --port handled by RFS_ServerBase
}

int main(int argc, const char **argv)
{
    CSimpleRFS server;
    if (!server.init(argc,argv)) // this must be called first
        return 1;
    // my initialization here (NB Windows services won't reach here - use serviceInit for initialization code for these)
    return server.run();
}
