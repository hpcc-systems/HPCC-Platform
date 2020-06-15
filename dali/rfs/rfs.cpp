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
#define WIN32_LEAN_AND_MEAN
#ifndef _CRT_SECURE_CPP_OVERLOAD_STANDARD_NAMES
#define _CRT_SECURE_CPP_OVERLOAD_STANDARD_NAMES 1
#undef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS 1
#endif
#undef UNICODE
#include <windows.h>
#include <io.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <signal.h>
#define getpid ::GetCurrentProcessId
#else
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stddef.h>
#include <errno.h>
#include <stdarg.h>
#define _strdup strdup
#define closesocket close
#include <sys/stat.h>
#define _O_WRONLY O_WRONLY
#define _open ::open
#define _write ::write
#define _close ::close
#define _S_IREAD    S_IRUSR | S_IRGRP | S_IROTH
#define _S_IWRITE   S_IWUSR | S_IWGRP | S_IWOTH
#define _S_IEXEC    S_IXUSR | S_IXGRP | S_IXOTH
#define _O_CREAT O_CREAT
#define _O_APPEND O_APPEND
#define _vsnprintf vsnprintf

#endif

#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include <time.h>
#include <malloc.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>


#include "rfs.h"



#ifdef _WIN32
#define socklen_t int
#define T_SOCKET SOCKET
#define ETIMEDOUT WSAETIMEDOUT
#define ECONNREFUSED WSAECONNREFUSED
#define BADSOCKERR(err) ((err==WSAEBADF)||(err==WSAENOTSOCK))
#define SEND_FLAGS 0
#define ERRNO() WSAGetLastError()
#define EADDRINUSE WSAEADDRINUSE
#define EINTRCALL WSAEINTR
#define ECONNRESET WSAECONNRESET
#define ECONNABORTED WSAECONNABORTED
#define ENOTCONN WSAENOTCONN
#define EWOULDBLOCK WSAEWOULDBLOCK
#define EINPROGRESS WSAEINPROGRESS
#define ENOTSOCK WSAENOTSOCK
static int strncasecmp (const char *s1, const char *s2, size_t len)
{
    int ret = 0;
    if (len)
        while ((ret = tolower((unsigned char)*s1)-tolower((unsigned char)*s2)) == 0) {
            s2++;
            if (*s1++ == '\0' || --len == 0)
                break;
        }
    return ret;
}
#else
#define T_SOCKET int
#define SEND_FLAGS (MSG_NOSIGNAL)
#define BADSOCKERR(err) ((err==EBADF)||(err==ENOTSOCK))
#define INVALID_SOCKET -1
#define ERRNO() (errno)
#define EINTRCALL EINTR

static int _memicmp (const void *s1, const void *s2, size_t len)
{
    const unsigned char *b1 = (const unsigned char *)s1;
    const unsigned char *b2 = (const unsigned char *)s2;
    int ret = 0;
    while (len&&((ret = tolower(*b1)-tolower(*b2)) == 0)) {
        b1++;
        b2++;
        len--;
    }
    return ret;
}
#define _stricmp   strcasecmp

#endif

#define MAX_BLOCK_SIZE (100*1048576) // 100MB though actually should never be anywhere near

typedef byte RFS_RemoteFileCommandType;
enum {
    RFCopenIO,
    RFCcloseIO,
    RFCread,
    RFCwrite,
    RFCsize,
    RFCexists,
    RFCremove,
    RFCrename,
    RFCgetver,
    RFCisfile,
    RFCisdirectory,
    RFCisreadonly,
    RFCsetreadonly,
    RFCgettime,
    RFCsettime,
    RFCcreatedir,
    RFCgetdir,
    RFCstop,        // not supported
    RFCexec,        // legacy cmd removed
    RFCdummy1,      // legacy placeholder
    RFCredeploy,    // not supported
    RFCgetcrc,
#ifdef TBD      // The follwing may be implemented depending if required
    RFCmove,
    RFCsetsize,
    RFCextractblobelements,
    RFCcopy,
    RFCappend,
    RFCmonitordir,
    RFCsettrace,
    RFCgetinfo,
    RFCfirewall,    // not supported
    RFCunlock,
    RFCunlockreply,
    RFCinvalid,
    RFCcopysection,
    RFCtreecopy,
    RFCtreecopytmp,
    RFCsetthrottle,
    RFCsetthrottle2,
    RFCsetfileperms,
#endif
    RFCmax,
    RFCnone = 255
    };


#define RFSERR_SysError                         7999
#define RFSERR_InvalidCommand                   8000
#define RFSERR_NullFileIOHandle                 8001
#define RFSERR_InvalidFileIOHandle              8002
#define RFSERR_TimeoutFileIOHandle              8003
#define RFSERR_OpenFailed                       8004
#define RFSERR_ReadFailed                       8005
#define RFSERR_WriteFailed                      8006
#define RFSERR_RenameFailed                     8007
#define RFSERR_ExistsFailed                     8009
#define RFSERR_RemoveFailed                     8010
#define RFSERR_CloseFailed                      8011
#define RFSERR_IsFileFailed                     8012
#define RFSERR_IsDirectoryFailed                8013
#define RFSERR_IsReadOnlyFailed                 8014
#define RFSERR_SetReadOnlyFailed                8015
#define RFSERR_GetTimeFailed                    8016
#define RFSERR_SetTimeFailed                    8017
#define RFSERR_CreateDirFailed                  8018
#define RFSERR_GetDirFailed                     8019
#define RFSERR_GetCrcFailed                     8020
#define RFSERR_MoveFailed                       8021
#define RFSERR_ExtractBlobElementsFailed        8022
#define RFSERR_CopyFailed                       8023
#define RFSERR_AppendFailed                     8024
#define RFSERR_AuthenticateFailed               8025
#define RFSERR_CopySectionFailed                8026
#define RFSERR_TreeCopyFailed                   8027
#define RFSERR_SizeFailed                       8051
#define RFSERR_GetVerFailed                     8052


struct mapCmdToErr_t { RFS_RemoteFileCommandType cmd; int err; } mapCmdToErr[] =
{
  { RFCopenIO,      RFSERR_OpenFailed },
  { RFCcloseIO,     RFSERR_CloseFailed },
  { RFCread,        RFSERR_ReadFailed },
  { RFCwrite,       RFSERR_WriteFailed },
  { RFCsize,        RFSERR_SizeFailed },
  { RFCexists,      RFSERR_ExistsFailed },
  { RFCremove,      RFSERR_RemoveFailed },
  { RFCrename,      RFSERR_RenameFailed },
  { RFCgetver,      RFSERR_GetVerFailed },
  { RFCisfile,      RFSERR_IsFileFailed },
  { RFCisdirectory, RFSERR_IsDirectoryFailed },
  { RFCisreadonly,  RFSERR_IsReadOnlyFailed },
  { RFCsetreadonly, RFSERR_SetReadOnlyFailed },
  { RFCgettime,     RFSERR_GetTimeFailed },
  { RFCsettime,     RFSERR_SetTimeFailed },
  { RFCcreatedir,   RFSERR_CreateDirFailed },
  { RFCgetdir,      RFSERR_GetDirFailed },
  { RFCgetcrc,      RFSERR_GetCrcFailed },
  { RFCmax,         RFSERR_InvalidCommand }
};

struct mapErrs_t { int val; const char *str; } mapErrs[] =
{
    { RFSERR_InvalidCommand                     ,"RFSERR_InvalidCommand"},
    { RFSERR_NullFileIOHandle                   ,"RFSERR_NullFileIOHandle"},
    { RFSERR_InvalidFileIOHandle                ,"RFSERR_InvalidFileIOHandle"},
    { RFSERR_TimeoutFileIOHandle                ,"RFSERR_TimeoutFileIOHandle"},
    { RFSERR_OpenFailed                         ,"RFSERR_OpenFailed"},
    { RFSERR_ReadFailed                         ,"RFSERR_ReadFailed"},
    { RFSERR_WriteFailed                        ,"RFSERR_WriteFailed"},
    { RFSERR_RenameFailed                       ,"RFSERR_RenameFailed"},
    { RFSERR_ExistsFailed                       ,"RFSERR_ExistsFailed"},
    { RFSERR_RemoveFailed                       ,"RFSERR_RemoveFailed"},
    { RFSERR_CloseFailed                        ,"RFSERR_CloseFailed"},
    { RFSERR_IsFileFailed                       ,"RFSERR_IsFileFailed"},
    { RFSERR_IsDirectoryFailed                  ,"RFSERR_IsDirectoryFailed"},
    { RFSERR_IsReadOnlyFailed                   ,"RFSERR_IsReadOnlyFailed"},
    { RFSERR_SetReadOnlyFailed                  ,"RFSERR_SetReadOnlyFailed"},
    { RFSERR_GetTimeFailed                      ,"RFSERR_GetTimeFailed"},
    { RFSERR_SetTimeFailed                      ,"RFSERR_SetTimeFailed"},
    { RFSERR_CreateDirFailed                    ,"RFSERR_CreateDirFailed"},
    { RFSERR_GetDirFailed                       ,"RFSERR_GetDirFailed"},
    { RFSERR_GetCrcFailed                       ,"RFSERR_GetCrcFailed"},
    { RFSERR_MoveFailed                         ,"RFSERR_MoveFailed"},
    { RFSERR_ExtractBlobElementsFailed          ,"RFSERR_ExtractBlobElementsFailed"},
    { RFSERR_CopyFailed                         ,"RFSERR_CopyFailed"},
    { RFSERR_AppendFailed                       ,"RFSERR_AppendFailed"},
    { RFSERR_AuthenticateFailed                 ,"RFSERR_AuthenticateFailed"},
    { RFSERR_CopySectionFailed                  ,"RFSERR_CopySectionFailed"},
    { RFSERR_TreeCopyFailed                     ,"RFSERR_TreeCopyFailed"},
    { RFSERR_SizeFailed                         ,"RFSERR_SizeFailed"},
    { RFSERR_GetVerFailed                       ,"RFSERR_GetVerFailed"},
    { 0, NULL }
};

#define RFEnoerror      0U

enum class fileBool { foundNo = false, foundYes = true, notFound = 2 };

#ifdef _WIN32
class win_socket_library
{
public:
    win_socket_library() { init(); }
    bool init()
    {
        WSADATA wsa;
        if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
            if (WSAStartup(MAKEWORD(1, 1), &wsa) != 0) {
                MessageBoxA(NULL,"Failed to initialize windows sockets","JLib Socket Error",MB_OK);
                exit(0);
            }
        }
        return true;
    }
    ~win_socket_library()
    {
        WSACleanup();
    }
};

#endif

#ifndef _WIN32
static char *_itoa(unsigned long n, char *str, int b, bool sign)
{
    char *s = str;

    if (sign)
        n = -n;

    do
    {
        byte d = n % b;
        *(s++) = d+((d<10)?'0':('a'-10));
    }
    while ((n /= b) > 0);
    if (sign)
        *(s++) = '-';
    *s = '\0';

    // reverse
    char *s2 = str;
    s--;
    while (s2<s)
    {
        char tc = *s2;
        *(s2++) = *s;
        *(s--) = tc;
    }

    return str;
}
char *_itoa(int n, char *str, int b)
{
    return _itoa(n, str, b, (n<0));
}
char *_ltoa(long n, char *str, int b)
{
    return _itoa(n, str, b, (n<0));
}
char *_ultoa(unsigned long n, char *str, int b)
{
    return _itoa(n, str, b, false);
}

#endif

void getLogTime(char *res)
{
#ifdef _WIN32
  SYSTEMTIME st;
  GetLocalTime(&st);
  sprintf(res,"%02u/%02u/%04u %02u:%02u:%02u:%03u ", st.wDay,st.wMonth,st.wYear,st.wHour,st.wMinute,st.wSecond,st.wMilliseconds);
#else
  time_t st;
  time(&st);
  struct tm *_tm = localtime(&st);
  sprintf(res,"%02u/%02u/%04u %02u:%02u:%02u ", _tm->tm_mday,_tm->tm_mon+1,_tm->tm_year+1900,_tm->tm_hour,_tm->tm_min,_tm->tm_sec);
#endif
}




inline  void _cpyrevn(void * _tgt, const void * _src, unsigned len) {
    char * tgt = (char *)_tgt; const char * src = (const char *)_src+len;
    for (;len;len--) {
        *tgt++ = *--src;
    }
}

inline  void _rev4(char *b) { char t=b[0]; b[0]=b[3]; b[3]=t; t=b[1]; b[1]=b[2]; b[2]=t; }

inline void BECONV(unsigned &v)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    _rev4((char *)&v);
#else
#endif
}

struct RFS_context
{
    unsigned short port;
    T_SOCKET listensock;
    bool running;
    int argc;
    const char **argv;
    char *logname;
    int debug;
    RFS_RemoteFileCommandType cmd;
#ifdef _WIN32
    win_socket_library *ws32_lib;
    static class RFS_WindowsService *RFS_Service;
#else
    bool daemon;
#endif
};

void logError(RFS_ServerBase *base, const char *str,int err,unsigned extra=0) // used for comms failures
{
    const char *errs;
    char es[1024];
#ifdef _WIN32
    if (err>1000) {
        LPVOID lpMsgBuf=NULL;
        FormatMessageA( FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM, NULL,
            err, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR) &lpMsgBuf,0,NULL);
        if (lpMsgBuf) {
            //wsprintf(buf, "%d: %s", dw, lpMsgBuf);
            strncpy(es,(char *)lpMsgBuf,sizeof(es)-1);
            LocalFree( lpMsgBuf );
            errs = es;
        }
        else
            errs = strerror(err);
    }
    else
#endif
        errs = err?strerror(err):"";
    if (!base||!base->logFilename()) {
        if (!err&&!extra)
            fprintf(stderr,"ERROR: %s\n",str,err,extra,errs);
        else if (extra)
            fprintf(stderr,"ERROR: %s (%d,%d) %s\n",str,err,extra,errs);
        else
            fprintf(stderr,"ERROR: %s (%d) %s\n",str,err,errs);
        fflush(stderr);
    }
    else {
        if (!err&&!extra)
            base->log("ERROR: %s",str,err,extra,errs);
        else if (extra)
            base->log("ERROR: %s (%d,%d) %s",str,err,extra,errs);
        else
            base->log("ERROR: %s (%d) %s",str,err,errs);
    }
}

static inline byte decode_32c(char c)
{
    byte b = (byte)c;
    if (b>=97) {
        if (b<=122)
            return b-97;
    }
    else if ((b>=50)&&(b<=55))
        return b-24;
    return 0;
}


class RFS_MemoryBuffer  // NB big endian only!
{
    byte *  buffer;
    size_t  curLen;
    size_t  readPos;
    size_t  maxLen;
public:


    inline RFS_MemoryBuffer()
    {
        curLen = 0;
        readPos = 0;
        maxLen = 0;
        buffer = NULL;
    }

    inline ~RFS_MemoryBuffer()
    {
        free(buffer);
    }

    void swapWith(RFS_MemoryBuffer &other)
    {
        byte *tb = buffer;
        buffer = other.buffer;
        other.buffer = tb;
        size_t t = curLen;
        curLen = other.curLen;
        other.curLen = t;
        t = readPos;
        readPos = other.readPos;
        other.readPos = t;
        t = maxLen;
        curLen = other.maxLen;
        other.maxLen = t;
    }


    inline size_t length() { return curLen; }
    inline size_t curPos() { return readPos; }
    inline size_t maxLength() { return maxLen; }

    inline void setLength(size_t len)
    {
        assert (len<=maxLen);
        curLen = len;
    }


    inline void *reserve(size_t sz)
    {
        if (!buffer) {
            maxLen = sz+1024;
            buffer = (byte *)malloc(maxLen);
        }
        else if (sz>maxLen-curLen) {
            do {
                maxLen += maxLen;
            } while (sz>maxLen-curLen);
            buffer = (byte *)realloc(buffer,maxLen);
        }
        byte *ret = buffer+curLen;
        curLen+=sz;
        return ret;
    }

    inline RFS_MemoryBuffer & appendBigEndian(size_t len, const void * value)
    {
#if __BYTE_ORDER == __LITTLE_ENDIAN
        _cpyrevn(reserve(len), value, len);
#else
        memcpy(reserve(len), value, len);
#endif
        return *this;
    }


    inline RFS_MemoryBuffer &  append(rfs_fpos_t value)
    {
        return appendBigEndian(sizeof(value),&value);
    }

    RFS_MemoryBuffer &  append(unsigned value)
    {
        return appendBigEndian(sizeof(value),&value);
    }

    inline RFS_MemoryBuffer &  append(int value)
    {
        return appendBigEndian(sizeof(value),&value);
    }

    inline RFS_MemoryBuffer &  append(short value)
    {
        return appendBigEndian(sizeof(value),&value);
    }

    inline RFS_MemoryBuffer &  append(byte value)
    {
        memcpy(reserve(sizeof(value)),&value,sizeof(value));
        return *this;
    }

    inline RFS_MemoryBuffer &  append(char value)
    {
        memcpy(reserve(sizeof(value)),&value,sizeof(value));
        return *this;
    }

    inline RFS_MemoryBuffer &  append(bool value)
    {
        memcpy(reserve(sizeof(value)),&value,1);
        return *this;
    }

    RFS_MemoryBuffer &   append(size_t sz,const char *s)
    {
        memcpy(reserve(sz),s,sz);
        return *this;
    }

    RFS_MemoryBuffer &   append(const char *s)
    {
        if (!s)
            return append((byte)0);
        return append(strlen(s)+1,s);
    }

    RFS_MemoryBuffer & readEndian(size_t len, void * value)
    {
    #if __BYTE_ORDER == __LITTLE_ENDIAN
        _cpyrevn(value, buffer + readPos, len);
    #else
        memcpy(value, buffer + readPos, len);
    #endif
        readPos += len;
        return *this;
    }


    void read(byte &b)
    {
        memcpy(&b,buffer+readPos++,1);
    }

    void read(rfs_fpos_t &i)
    {
        readEndian(sizeof(i),&i);
    }

    void read(unsigned &i)
    {
        readEndian(sizeof(i),&i);
    }

    void read(int &i)
    {
        readEndian(sizeof(i),&i);
    }

    void read(short &i)
    {
        readEndian(sizeof(i),&i);
    }

    void read(bool &_b)
    {
        byte b;
        memcpy(&b,buffer+readPos,sizeof(b));
        readPos+=sizeof(b);
        _b = b!=0;
    }

    size_t readsize() // NB size sent as 32bit unsigned
    {
        unsigned ret;
        read(ret);
        return (size_t)ret;
    }

    char *readStr()
    {
        size_t l = strlen((char *)buffer+readPos)+1;
        char *ret = (char *)malloc(l);
        memcpy(ret,buffer+readPos,l);
        readPos+=l;
        return ret;
    }

    const byte *readBlock(size_t sz)
    {
        assert (sz<=maxLen-readPos);
        byte *ret = buffer+readPos;
        readPos+=sz;
        return ret;
    }

    inline RFS_MemoryBuffer & reset(size_t pos=0)
    {
        readPos = pos;
        return *this;
    }

    inline RFS_MemoryBuffer & clear(size_t max=0)
    {
        curLen = 0;
        readPos = 0;
        free(buffer);
        buffer = NULL;
        if (max)
            buffer = (byte *)malloc(max);
        maxLen = max;
        return *this;
    }

    inline byte *toByteArray()
    {
        return buffer;
    }

    byte *detach()
    {
        byte *ret = buffer;
        curLen = 0;
        readPos = 0;
        maxLen = 0;
        buffer = NULL;
        return ret;
    }

    void base32_Decode(const char *bi,RFS_MemoryBuffer &out)
    {
        while (1) {
            byte b[8];
            for (unsigned i=0;i<8;i++)
                b[i] =  decode_32c(*(bi++));
            byte o;
            o = ((b[0] & 0x1f) << 3) | ((b[1] & 0x1c) >> 2);
            if (!o) return;
            out.append(o);
            o = ((b[1] & 0x03) << 6) | ((b[2] & 0x1f) << 1) | ((b[3] & 0x10) >> 4);
            if (!o) return;
            out.append(o);
            o = ((b[3] & 0x0f) << 4) | ((b[4] & 0x1e) >> 1);
            if (!o) return;
            out.append(o);
            o = ((b[4] & 0x01) << 7) | ((b[5] & 0x1f) << 2) | ((b[6] & 0x18) >> 3);
            if (!o) return;
            out.append(o);
            o = ((b[6] & 0x07) << 5) | (b[7] & 0x1f);
            if (!o) return;
            out.append(o);
        }
    }

    void appendUtf8(unsigned c)
    {
        if (c < 0x80)
            append((char)c);
        else if (c < 0x800)
        {
            append((char)(0xC0 | c>>6));
            append((char)(0x80 | c & 0x3F));
        }
        else if (c < 0x10000)
        {
            append((char) (0xE0 | c>>12));
            append((char) (0x80 | c>>6 & 0x3F));
            append((char) (0x80 | c & 0x3F));
        }
        else if (c < 0x200000)
        {
            append((char) (0xF0 | c>>18));
            append((char) (0x80 | c>>12 & 0x3F));
            append((char) (0x80 | c>>6 & 0x3F));
            append((char) (0x80 | c & 0x3F));
        }
        else if (c < 0x4000000)
        {
            append((char) (0xF8 | c>>24));
            append((char) (0x80 | c>>18 & 0x3F));
            append((char) (0x80 | c>>12 & 0x3F));
            append((char) (0x80 | c>>6 & 0x3F));
            append((char) (0x80 | c & 0x3F));
        }
        else if (c < 0x80000000)
        {
            append((char) (0xFC | c>>30));
            append((char) (0x80 | c>>24 & 0x3F));
            append((char) (0x80 | c>>18 & 0x3F));
            append((char) (0x80 | c>>12 & 0x3F));
            append((char) (0x80 | c>>6 & 0x3F));
            append((char) (0x80 | c & 0x3F));
        }
    }


    void appendXML(const char *x, size_t len)
    {
        if (!x)
            return;
        if ((unsigned)-1 == len)
            len = strlen(x);
        const char *end = x+len;
        while (x<end && *x) {
            if ('&' == *x) {
                switch (*(x+1)) {
                case 'a':
                case 'A': {
                        switch (*(x+2)) {
                        case 'm':
                        case 'M': {
                                char c1 = *(x+3);
                                if (('p' == c1 || 'P' == c1) && ';' == *(x+4)) {
                                    x += 5;
                                    append('&');
                                    continue;
                                }
                                break;
                            }
                        case 'p':
                        case 'P': {
                                char c1 = *(x+3);
                                char c2 = *(x+4);
                                if (('o' == c1 || 'O' == c1) && ('s' == c2 || 'S' == c2) && ';' == *(x+5)) {
                                    x += 6;
                                    append('\'');
                                    continue;
                                }
                                break;
                            }
                        }
                        break;
                    }
                case 'l':
                case 'L': {
                        char c1 = *(x+2);
                        if (('t' == c1 || 'T' == c1) && ';' == *(x+3)) {
                            x += 4;
                            append('<');
                            continue;
                        }
                        break;
                    }
                case 'g':
                case 'G': {
                        char c1 = *(x+2);
                        if (('t' == c1 || 'T' == c1) && ';' == *(x+3)) {
                            x += 4;
                            append('>');
                            continue;
                        }
                        break;
                    }
                case 'q':
                case 'Q': {
                        char c1 = *(x+2);
                        char c2 = *(x+3);
                        char c3 = *(x+4);
                        if (('u' == c1 || 'U' == c1) && ('o' == c2 || 'O' == c2) && ('t' == c3 || 'T' == c3) && ';' == *(x+5)) {
                            x += 6;
                            append('"');
                            continue;
                        }
                        break;
                    }
                case 'n':
                case 'N': {
                        char c1 = *(x+2);
                        char c2 = *(x+3);
                        char c3 = *(x+4);
                        if (('b' == c1 || 'B' == c1) && ('s' == c2 || 'S' == c2) && ('p' == c3 || 'P' == c3) && ';' == *(x+5)) {
                            x += 6;
                            appendUtf8(0xa0);
                            continue;
                        }
                        break;
                    }
                default: {
                        x++;
                        if (*x == '#') {
                            x++;
                            bool hex;
                            if (*x == 'x' || *x == 'X') { // strictly not sure about X.
                                hex = true;
                                x++;
                            }
                            else
                                hex = false;
                            char *endptr;
                            unsigned long val = 0;
                            if (hex)
                                val = strtoul(x,&endptr,16);
                            else
                                val = strtoul(x,&endptr,10);
                            if (x!=endptr && *endptr == ';')
                                appendUtf8((unsigned)val);
                            x = endptr+1;
                            continue;
                        }
                        else {
                            if ('\0' == *x)
                                --x;
                            else {
                                bool error = false;
                                append('&');
                            }
                        }
                        break;
                    }
                }
            }
            append(*(x++));
        }
    }



    const char * readFileName(RFS_MemoryBuffer &out)
    {
        size_t t = out.length();
        char *s = (char *)buffer+readPos;
        size_t l = strlen(s);
        if ((l>=3)&&((memcmp(s,"/$/",3)==0)||(memcmp(s,"\\$\\",3)==0)))
            base32_Decode(s+3,out);
        else
            out.append(l,s);
        out.append((byte)0);
        readPos+=l+1;
        return (char *)out.toByteArray()+t;
    }

};


struct RFS_DateTime
{
    RFS_DateTime()
    {
        year = 0;
        month = 0;
        day = 0;
        hour = 0;
        min = 0;
        sec = 0;
        nanosec = 0;
    }

    void deserialize(RFS_MemoryBuffer &src)
    {
        src.read(year);
        src.read(month);
        src.read(day);
        src.read(hour);
        src.read(min);
        src.read(sec);
        src.read(nanosec);
    }

    void serialize(RFS_MemoryBuffer &dst) const
    {
        dst.append(year).append(month).append(day).append(hour).append(min).append(sec).append(nanosec);
    }

    void setDate(unsigned _year, unsigned _month, unsigned _day)
    {
        year = _year;
        month = _month;
        day = _day;
    }

    void setTime(unsigned _hour, unsigned _min, unsigned _sec, unsigned _nanosec)
    {
        hour = _hour;
        min = _min;
        sec = _sec;
        nanosec = _nanosec;
    }

    void getDate(int & _year, int & _month, int & _day) const
    {
        _year = year;
        _month = month;
        _day = day;
    }

    void getTime(int & _hour, int & _min, int & _sec, int & _nanosec) const
    {
        _hour = hour;
        _min = min;
        _sec = sec;
        _nanosec = nanosec;
    }

    void set(time_t time)
    {
        struct tm tm_r;
        struct tm * gmt;
#ifdef _WIN32
#if _MSC_VER < 1400             // VC6
        tm_r = *localtime(&time);
#else
        localtime_s(&tm_r,&time);
#endif
        gmt = &tm_r;
#else
        gmt = localtime_r(&time,&tm_r);
#endif
        setDate(gmt->tm_year + 1900, gmt->tm_mon + 1, gmt->tm_mday);
        setTime(gmt->tm_hour, gmt->tm_min, gmt->tm_sec, 0);
    }

    time_t get()
    {
        int unused;
        struct tm ttm;
        getDate(ttm.tm_year, ttm.tm_mon, ttm.tm_mday);
        getTime(ttm.tm_hour, ttm.tm_min, ttm.tm_sec, unused);
        ttm.tm_isdst = -1;

        if(ttm.tm_year >= 1900)
            ttm.tm_year -= 1900;

        ttm.tm_mon -= 1;

        time_t time = mktime(&ttm);
        if (time == (time_t)-1)
            time = 0;
        return time;
    }


protected:
    short    year;
    byte     month;
    byte     day;
    byte     hour;
    byte     min;
    byte     sec;
    unsigned nanosec;
};

class RFS_Exception
{
public:
    char *str;
    int rfserr;
    int err;
    bool fatal;
    RFS_Exception() { str = NULL; rfserr = 0; err = 0; fatal = false; }
    RFS_Exception(int _rfserr, int _err,const char *msg,bool _fatal)
    {
        rfserr = _rfserr;
        err = _err;
        fatal = _fatal;
        char es[1024];
        const char *ename = NULL;
        if (rfserr==RFSERR_SysError) {
#ifdef _WIN32
            if (err>1000) {
                LPVOID lpMsgBuf=NULL;
                FormatMessageA( FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM, NULL,
                    err, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR) &lpMsgBuf,0,NULL);
                if (lpMsgBuf) {
                    //wsprintf(buf, "%d: %s", dw, lpMsgBuf);
                    strncpy(es,(char *)lpMsgBuf,sizeof(es)-1);
                    LocalFree( lpMsgBuf );
                    ename = es;
                }
                else
                    ename = strerror(err);
            }
            else
#endif
                ename = strerror(err);
        }
        else {
            for (unsigned i=0;;i++) {
                ename = mapErrs[i].str;
                if (!ename||(mapErrs[i].val==rfserr))
                    break;
            }
            if (!ename) {
                _itoa(rfserr,es,10);
                ename = es;
            }
        }
        size_t msgsz = msg?strlen(msg):0;
        msgsz += strlen(ename)+32;
        str = (char *)malloc(msgsz);
        sprintf(str,"ERROR: %s(%d) '%s'",ename,err,msg?msg:"");
    }

    ~RFS_Exception() { free(str); }

};

static unsigned long crc_32_tab[] = { /* CRC polynomial 0xedb88320 */
0x00000000L, 0x77073096L, 0xee0e612cL, 0x990951baL, 0x076dc419L, 0x706af48fL,
0xe963a535L, 0x9e6495a3L, 0x0edb8832L, 0x79dcb8a4L, 0xe0d5e91eL, 0x97d2d988L,
0x09b64c2bL, 0x7eb17cbdL, 0xe7b82d07L, 0x90bf1d91L, 0x1db71064L, 0x6ab020f2L,
0xf3b97148L, 0x84be41deL, 0x1adad47dL, 0x6ddde4ebL, 0xf4d4b551L, 0x83d385c7L,
0x136c9856L, 0x646ba8c0L, 0xfd62f97aL, 0x8a65c9ecL, 0x14015c4fL, 0x63066cd9L,
0xfa0f3d63L, 0x8d080df5L, 0x3b6e20c8L, 0x4c69105eL, 0xd56041e4L, 0xa2677172L,
0x3c03e4d1L, 0x4b04d447L, 0xd20d85fdL, 0xa50ab56bL, 0x35b5a8faL, 0x42b2986cL,
0xdbbbc9d6L, 0xacbcf940L, 0x32d86ce3L, 0x45df5c75L, 0xdcd60dcfL, 0xabd13d59L,
0x26d930acL, 0x51de003aL, 0xc8d75180L, 0xbfd06116L, 0x21b4f4b5L, 0x56b3c423L,
0xcfba9599L, 0xb8bda50fL, 0x2802b89eL, 0x5f058808L, 0xc60cd9b2L, 0xb10be924L,
0x2f6f7c87L, 0x58684c11L, 0xc1611dabL, 0xb6662d3dL, 0x76dc4190L, 0x01db7106L,
0x98d220bcL, 0xefd5102aL, 0x71b18589L, 0x06b6b51fL, 0x9fbfe4a5L, 0xe8b8d433L,
0x7807c9a2L, 0x0f00f934L, 0x9609a88eL, 0xe10e9818L, 0x7f6a0dbbL, 0x086d3d2dL,
0x91646c97L, 0xe6635c01L, 0x6b6b51f4L, 0x1c6c6162L, 0x856530d8L, 0xf262004eL,
0x6c0695edL, 0x1b01a57bL, 0x8208f4c1L, 0xf50fc457L, 0x65b0d9c6L, 0x12b7e950L,
0x8bbeb8eaL, 0xfcb9887cL, 0x62dd1ddfL, 0x15da2d49L, 0x8cd37cf3L, 0xfbd44c65L,
0x4db26158L, 0x3ab551ceL, 0xa3bc0074L, 0xd4bb30e2L, 0x4adfa541L, 0x3dd895d7L,
0xa4d1c46dL, 0xd3d6f4fbL, 0x4369e96aL, 0x346ed9fcL, 0xad678846L, 0xda60b8d0L,
0x44042d73L, 0x33031de5L, 0xaa0a4c5fL, 0xdd0d7cc9L, 0x5005713cL, 0x270241aaL,
0xbe0b1010L, 0xc90c2086L, 0x5768b525L, 0x206f85b3L, 0xb966d409L, 0xce61e49fL,
0x5edef90eL, 0x29d9c998L, 0xb0d09822L, 0xc7d7a8b4L, 0x59b33d17L, 0x2eb40d81L,
0xb7bd5c3bL, 0xc0ba6cadL, 0xedb88320L, 0x9abfb3b6L, 0x03b6e20cL, 0x74b1d29aL,
0xead54739L, 0x9dd277afL, 0x04db2615L, 0x73dc1683L, 0xe3630b12L, 0x94643b84L,
0x0d6d6a3eL, 0x7a6a5aa8L, 0xe40ecf0bL, 0x9309ff9dL, 0x0a00ae27L, 0x7d079eb1L,
0xf00f9344L, 0x8708a3d2L, 0x1e01f268L, 0x6906c2feL, 0xf762575dL, 0x806567cbL,
0x196c3671L, 0x6e6b06e7L, 0xfed41b76L, 0x89d32be0L, 0x10da7a5aL, 0x67dd4accL,
0xf9b9df6fL, 0x8ebeeff9L, 0x17b7be43L, 0x60b08ed5L, 0xd6d6a3e8L, 0xa1d1937eL,
0x38d8c2c4L, 0x4fdff252L, 0xd1bb67f1L, 0xa6bc5767L, 0x3fb506ddL, 0x48b2364bL,
0xd80d2bdaL, 0xaf0a1b4cL, 0x36034af6L, 0x41047a60L, 0xdf60efc3L, 0xa867df55L,
0x316e8eefL, 0x4669be79L, 0xcb61b38cL, 0xbc66831aL, 0x256fd2a0L, 0x5268e236L,
0xcc0c7795L, 0xbb0b4703L, 0x220216b9L, 0x5505262fL, 0xc5ba3bbeL, 0xb2bd0b28L,
0x2bb45a92L, 0x5cb36a04L, 0xc2d7ffa7L, 0xb5d0cf31L, 0x2cd99e8bL, 0x5bdeae1dL,
0x9b64c2b0L, 0xec63f226L, 0x756aa39cL, 0x026d930aL, 0x9c0906a9L, 0xeb0e363fL,
0x72076785L, 0x05005713L, 0x95bf4a82L, 0xe2b87a14L, 0x7bb12baeL, 0x0cb61b38L,
0x92d28e9bL, 0xe5d5be0dL, 0x7cdcefb7L, 0x0bdbdf21L, 0x86d3d2d4L, 0xf1d4e242L,
0x68ddb3f8L, 0x1fda836eL, 0x81be16cdL, 0xf6b9265bL, 0x6fb077e1L, 0x18b74777L,
0x88085ae6L, 0xff0f6a70L, 0x66063bcaL, 0x11010b5cL, 0x8f659effL, 0xf862ae69L,
0x616bffd3L, 0x166ccf45L, 0xa00ae278L, 0xd70dd2eeL, 0x4e048354L, 0x3903b3c2L,
0xa7672661L, 0xd06016f7L, 0x4969474dL, 0x3e6e77dbL, 0xaed16a4aL, 0xd9d65adcL,
0x40df0b66L, 0x37d83bf0L, 0xa9bcae53L, 0xdebb9ec5L, 0x47b2cf7fL, 0x30b5ffe9L,
0xbdbdf21cL, 0xcabac28aL, 0x53b39330L, 0x24b4a3a6L, 0xbad03605L, 0xcdd70693L,
0x54de5729L, 0x23d967bfL, 0xb3667a2eL, 0xc4614ab8L, 0x5d681b02L, 0x2a6f2b94L,
0xb40bbe37L, 0xc30c8ea1L, 0x5a05df1bL, 0x2d02ef8dL
};

#define UPDC32(octet, crc) (crc_32_tab[((crc) ^ (octet)) & 0xff] ^ ((crc) >> 8))

unsigned long crc32(const char *buf, unsigned len, unsigned long crc)
{
    byte    c;
    while(len >= 12)
    {
        c = *buf++; crc = UPDC32(c,crc);
        c = *buf++; crc = UPDC32(c,crc);
        c = *buf++; crc = UPDC32(c,crc);
        c = *buf++; crc = UPDC32(c,crc);
        len -= 4;
    }

    switch (len)
    {
    case 11: c = *buf++; crc = UPDC32(c,crc);
    case 10: c = *buf++; crc = UPDC32(c,crc);
    case 9: c = *buf++; crc = UPDC32(c,crc);
    case 8: c = *buf++; crc = UPDC32(c,crc);
    case 7: c = *buf++; crc = UPDC32(c,crc);
    case 6: c = *buf++; crc = UPDC32(c,crc);
    case 5: c = *buf++; crc = UPDC32(c,crc);
    case 4: c = *buf++; crc = UPDC32(c,crc);
    case 3: c = *buf++; crc = UPDC32(c,crc);
    case 2: c = *buf++; crc = UPDC32(c,crc);
    case 1: c = *buf++; crc = UPDC32(c,crc);
    }

    return(crc);
}


static size_t do_recv(int sock, void *buf, size_t len, int &err)
{
    do {
        int sz = (int)recv(sock,(char *)buf,len,0);
        if (sz>=0) {
            err = 0;
            return (size_t)sz;
        }
        err = ERRNO();
    } while (err==EINTRCALL);
    return (size_t)-1;
}

bool sendBuffer(RFS_ServerBase *base,T_SOCKET socket, RFS_MemoryBuffer & src)
{
    unsigned length = src.length() - sizeof(unsigned);
    byte * buffer = src.toByteArray();
    memcpy(buffer, &length, sizeof(unsigned));
    BECONV(*(unsigned *)buffer);
    size_t remaining = src.length();
    while(1) {
        int ret = send(socket, (const char *)buffer, remaining,0);
#ifdef _TRACE_RW
        LogF("SEND(%d)",remaining);
#endif
        if (ret<0) {
            int err = ERRNO();
            if (err!=EINTRCALL) {
                logError(base,"sendBuffer",err);   // cant return to caller!
                return false;
            }
            ret = 0;
        }
        remaining -= ret;
        if (remaining==0)
            break;
        buffer += ret;
    }
    return true;
}

class RFS_ConnectionList
{
    unsigned maxconn;
    RFS_ConnectionBase **conn;
    static int nextsalt;
    int salt;
public:
    RFS_ConnectionList()
    {
        salt = nextsalt;
        nextsalt += 1000;
        maxconn = 16;
        conn = (RFS_ConnectionBase **)calloc(sizeof(RFS_ConnectionBase *),maxconn);
    }

    ~RFS_ConnectionList()
    {
        while (maxconn--)
            delete conn[maxconn];
        free(conn);
    }

    RFS_ConnectionBase *find(int handle)
    {
        handle-=salt;
        if ((unsigned)handle>=maxconn)
            return NULL;
        return conn[handle];
    }

    int add(RFS_ConnectionBase *c)
    {
        unsigned i;
        for (i=0;;i++) {
            if (i==maxconn) {
                maxconn+=16;
                conn = (RFS_ConnectionBase **)realloc(conn,sizeof(RFS_ConnectionBase *)*maxconn);
                memset(conn+i,0,16*sizeof(RFS_ConnectionBase *));
                break;
            }
            if (!conn[i])
                break;
        }
        conn[i] = c;
        return i+salt;
    }

    bool remove(RFS_ConnectionBase *c)
    {
        unsigned i;
        for (i=0;i<maxconn;i++)
            if (conn[i]==c) {
                conn[i] = NULL;
                return true;
            }
        return false;
    }

    void swapWith(RFS_ConnectionList &other)
    {
        unsigned tmpmaxconn = maxconn;
        maxconn = other.maxconn;
        other.maxconn = tmpmaxconn;
        RFS_ConnectionBase **tmpconn = conn;
        conn = other.conn;
        other.conn = tmpconn;
        int tmpsalt = salt;
        salt = other.salt;
        other.salt = tmpsalt;
    }
};

int RFS_ConnectionList::nextsalt = 1000;

static bool processCommand(RFS_ServerBase &base, RFS_context &context, RFS_ConnectionList &connlist, T_SOCKET sock,RFS_MemoryBuffer &in)
{
    RFS_MemoryBuffer out;
    out.append((unsigned)0); // for length
    RFS_MemoryBuffer tmp;
    RFS_MemoryBuffer tmp2;
    RFS_RemoteFileCommandType cmd;
    try {
        in.read(cmd);
        context.cmd = cmd;
        switch (cmd) {
        case RFCopenIO: {
                const char * name = in.readFileName(tmp);
                byte mode;
                in.read(mode);
                byte share;
                in.read(share);
                int handleout = -1;
                RFS_ConnectionBase *conn = base.open(name,mode,share);
                if (conn)
                    handleout = connlist.add(conn);
                out.append((unsigned)RFEnoerror).append(handleout);
            }
            break;
        case RFCcloseIO: {
                int handle;
                in.read(handle);
                RFS_ConnectionBase *conn = connlist.find(handle);
                if (conn) {
                    connlist.remove(conn);
                    conn->close();
                    delete conn;        // should delete even if close raises error?
                }
                else if (handle!=-1)
                    logError(&base,"Unexpected handle (close)",0,(unsigned)handle);
                out.append((unsigned)RFEnoerror);
            }
            break;
        case RFCread: {
                int handle;
                in.read(handle);
                rfs_fpos_t pos;
                in.read(pos);
                size_t len = in.readsize();
                // mabe loop here if big?
                size_t posOfErr = out.length();
                out.append((unsigned)RFEnoerror);
                size_t newlen =  out.length()+sizeof(unsigned);
                unsigned *outlen = (unsigned *)out.reserve(len+sizeof(unsigned));
                *outlen = 0;
                RFS_ConnectionBase *conn = connlist.find(handle);
                if (conn) {
                    size_t outl = 0;
                    conn->read(pos,len,outl,outlen+1);
                    *outlen = (unsigned)outl;
                }
                else if (handle!=-1)
                    logError(&base,"Unexpected handle (read)",0,(unsigned)handle);
                newlen += *outlen;
                BECONV(*outlen);
                out.setLength(newlen);
            }
            break;
        case RFCwrite: {
                int handle;
                in.read(handle);
                rfs_fpos_t pos;
                in.read(pos);
                size_t len = in.readsize();

                RFS_ConnectionBase *conn = connlist.find(handle);
                if (conn)
                    conn->write(pos,len,in.readBlock(len));  // maybe loop here if big?
                else if (handle!=-1) {
                    logError(&base,"Unexpected handle (write)",0,(unsigned)handle);
                    len = 0;
                }
                out.append((unsigned)RFEnoerror).append((unsigned)len);
            }
            break;
        case RFCsize: {
                int handle;
                in.read(handle);
                rfs_fpos_t sizeout=(rfs_fpos_t)-1;
                RFS_ConnectionBase *conn = connlist.find(handle);
                if (conn)
                    sizeout = conn->size();
                else if (handle!=-1)
                    logError(&base,"Unexpected handle (size)",0,(unsigned)handle);
                out.append((unsigned)RFEnoerror).append(sizeout);
            }
            break;
        case RFCexists: {
                const char * name = in.readFileName(tmp);
                bool existsout = false;
                base.existFile(name,existsout);
                out.append((unsigned)RFEnoerror).append(existsout);
            }
            break;
        case RFCremove: {
                const char * name = in.readFileName(tmp);
                base.removeFile(name);
                out.append((unsigned)RFEnoerror).append((bool)true);
            }
            break;
        case RFCrename: {
                const char * name = in.readFileName(tmp);
                const char * name2 = in.readFileName(tmp2);
                base.renameFile(name,name2);
                out.append((unsigned)RFEnoerror);
            }
            break;
        case RFCgetver: {
                char programname[256];
                short version;
                base.getVersion(sizeof(programname)-1,programname,version);
                programname[255] = 0;
                char ver[1024];
                if (programname[0])
                    strcpy(ver,programname);
                else
                    strcpy(ver,context.argv[0]);
                strcat(ver,
#ifdef _WIN32
                " - Windows"
#else
                " - Linux"
#endif
                );
                if (in.length()-in.curPos()>sizeof(unsigned))
                    out.append((unsigned)RFEnoerror).append(ver);
                else
                    out.append((unsigned)0x10000+version).append(ver);
            }
            break;
        case RFCisfile:
        case RFCisdirectory:
        case RFCisreadonly: {
                const char * name = in.readFileName(tmp);
                bool existsout = false;
                base.existFile(name,existsout);
                unsigned ret = (unsigned)fileBool::notFound;
                if (existsout) {
                    if (cmd==RFCisfile)
                        base.isFile(name,existsout);
                    else if (cmd==RFCisdirectory)
                        base.isDir(name,existsout);
                    else
                        base.isReadOnly(name,existsout);
                    if (existsout)
                        ret = (unsigned)fileBool::foundYes;
                    else
                        ret = (unsigned)fileBool::foundNo;
                }
                out.append((unsigned)RFEnoerror).append(ret);
            }
            break;
        case RFCgettime: {
                const char * name = in.readFileName(tmp);
                bool existsout = false;
                base.existFile(name,existsout);
                if (existsout) {
                    RFS_DateTime createTime;
                    RFS_DateTime modifiedTime;
                    RFS_DateTime accessedTime;
                    time_t created = 0;
                    time_t accessed = 0;
                    time_t modified = 0;
                    base.getFileTime(name,accessed,created,modified);
                    createTime.set(created);
                    modifiedTime.set(modified);
                    accessedTime.set(modified);
                    out.append((unsigned)RFEnoerror).append((bool)true);
                    createTime.serialize(out);
                    modifiedTime.serialize(out);
                    accessedTime.serialize(out);
                }
                else
                    out.append((unsigned)RFEnoerror).append((bool)false);
            }
            break;
        case RFCsettime: {
                const char * name = in.readFileName(tmp);
                bool existsout = false;
                base.existFile(name,existsout);
                if (existsout) {
                    bool creategot;
                    RFS_DateTime createTime;
                    bool modifiedgot;
                    RFS_DateTime modifiedTime;
                    bool accessedgot;
                    RFS_DateTime accessedTime;
                    time_t created = 0;
                    time_t accessed = 0;
                    time_t modified = 0;
                    in.read(creategot);
                    if (creategot) {
                        createTime.deserialize(in);
                        created = createTime.get();
                    }
                    in.read(modifiedgot);
                    if (modifiedgot) {
                        modifiedTime.deserialize(in);
                        modified = modifiedTime.get();
                    }
                    in.read(accessedgot);
                    if (accessedgot) {
                        accessedTime.deserialize(in);
                        accessed = accessedTime.get();
                    }
                    base.setFileTime(name,accessedgot?&accessed:NULL,creategot?&created:NULL,modifiedgot?&modified:NULL);
                    out.append((unsigned)RFEnoerror).append((bool)true);
                }
                else
                    out.append((unsigned)RFEnoerror).append((bool)false);
            }
            break;
        case RFCcreatedir: {
                const char * name = in.readFileName(tmp);
                bool createdout = false;
                base.createDir(name,createdout);
                out.append((unsigned)RFEnoerror).append(createdout);
            }
            break;
        case RFCgetdir: {
                const char * name = in.readFileName(tmp);   // dir
                const char * name2 = in.readFileName(tmp2);  // mask        -- need readFilename here?
                bool includedir;
                bool sub;
                in.read(includedir);
                in.read(sub);
                void *handle;
                base.openDir(name,name2,sub,includedir,handle);
                char fn[1024];
                byte b = 1;
                out.append((unsigned)RFEnoerror);
                while (1) {
                    fn[0] = 0;
                    time_t modified;
                    bool isdir = false;
                    rfs_fpos_t filesize = 0;
                    base.nextDirEntry(handle,sizeof(fn)-1,fn,isdir,filesize,modified);
                    if (!fn[0])
                        break;
                    out.append(b);
                    out.append(isdir);
                    if (isdir)
                        filesize = 0;
                    out.append(filesize);
                    RFS_DateTime dt;
                    dt.set(modified);
                    dt.serialize(out);
                    out.append(fn);
                }
                b = 0;
                out.append(b);
            }
            break;
        case RFCgetcrc: {
                const char * name = in.readFileName(tmp);
                int handle = 0;
                class cH
                {
                public:
                    RFS_ConnectionBase *conn;
                    cH() { conn = NULL; }
                    ~cH() { delete conn; }
                } ch;
                ch.conn = base.open(name,RFS_OPEN_MODE_READ,RFS_SHARING_NONE);
                if (!ch.conn)
                    base.throwError(2,name);
                class cBuf
                {
                public:
                    byte * b;
                    size_t size;
                    cBuf() {  size = 0x100000; b = (byte *)malloc(size); }
                    ~cBuf() { free(b); }
                } buf;
                rfs_fpos_t pos=0;
                unsigned long crc=~0;
                while (1) {
                    size_t sz = 0;
                    ch.conn->read(pos,buf.size,sz,buf.b);
                    if (sz==0)
                        break;
                    crc = crc32((const char *)buf.b,sz,crc);
                    pos += sz;
                }
            }
            break;
        default: {
                char errormsg[256];
                strcpy(errormsg,"RFSERR_InvalidCommand:");
                _itoa(cmd,&errormsg[strlen(errormsg)],10);
                out.clear().append(RFSERR_InvalidCommand).append(errormsg);

            }
        }
    }
    catch (RFS_Exception *e)
    {
        if (e->fatal)
            throw;
        out.clear().append(e->rfserr).append(e->str);
        RFS_SimpleString errs("RFS_Exception: ");
        struct sockaddr_in *nameptr;
        struct sockaddr_in name;
        socklen_t namelen = sizeof(name);
        nameptr = &name;
        if(getpeername(sock,(struct sockaddr*)&name, &namelen)>=0) {
            errs.appends(inet_ntoa(nameptr->sin_addr));
            errs.appendc(' ');
        }
        errs.appends(e->str);
        logError(&base,errs.str(),0);
        delete e;
    }

    context.cmd = RFCnone;
    return sendBuffer(&base,sock,out);
}



RFS_ServerBase::~RFS_ServerBase()
{
    if (context) {
        setLogFilename(NULL);
        while (context->argc--)
            free((char *)context->argv[context->argc]);
        free(context->argv);
        if (context->listensock!=INVALID_SOCKET)
#ifdef _WIN32
            ::closesocket(context->listensock);
#else
            ::close(context->listensock);
#endif
    }
#ifdef _WIN32
    delete context->ws32_lib;
#endif

    delete context;
}

#ifdef _WIN32

// Service initialization
//==============================================================================


class RFS_WindowsService
{

    RFS_ServerBase &parent;
    char servicename[256];
    char displayname[256];

    SERVICE_STATUS ServiceStatus;
    SERVICE_STATUS_HANDLE hStatus;

    // Control handler function
    void doControlHandler(DWORD request)
    {
        switch(request) {
            case SERVICE_CONTROL_STOP:
                ServiceStatus.dwWin32ExitCode = 0;
                ServiceStatus.dwCurrentState  = SERVICE_STOPPED;
                break;
            case SERVICE_CONTROL_SHUTDOWN:
                ServiceStatus.dwWin32ExitCode = 0;
                ServiceStatus.dwCurrentState  = SERVICE_STOPPED;
                break;
            case SERVICE_CONTROL_INTERROGATE:
                break;
            default:
                break;
        }
        // Report current status
        SetServiceStatus (hStatus,  &ServiceStatus);
        return;
    }


    void doServiceMain(int argc, char** argv)
    {
        ServiceStatus.dwServiceType        = SERVICE_WIN32;
        ServiceStatus.dwCurrentState       = SERVICE_START_PENDING;
        ServiceStatus.dwControlsAccepted   = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
        ServiceStatus.dwWin32ExitCode      = 0;
        ServiceStatus.dwServiceSpecificExitCode = 0;
        ServiceStatus.dwCheckPoint         = 0;
        ServiceStatus.dwWaitHint           = 0;

        parent.getServiceName(sizeof(servicename),servicename,sizeof(displayname),displayname);
        hStatus = RegisterServiceCtrlHandlerA(servicename, (LPHANDLER_FUNCTION)ControlHandler);
        if (hStatus == (SERVICE_STATUS_HANDLE)0)  {
            // Registering Control Handler failed
            logError(&parent,"RegisterServiceCtrlHandler",GetLastError());
            return;
        }
        bool multi=false;
        if (!parent.queryContext()||(parent.serviceInit(parent.queryContext()->argc,parent.queryContext()->argv,multi)!=0)) {
            // Initialization failed
            ServiceStatus.dwCurrentState       = SERVICE_STOPPED;
            ServiceStatus.dwWin32ExitCode      = -1;
            SetServiceStatus(hStatus, &ServiceStatus);
            return;
        }
        ServiceStatus.dwCurrentState = SERVICE_RUNNING;
        SetServiceStatus (hStatus, &ServiceStatus);
        // Autentication gets params from HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\XXXX\Parameters
        // Not yet implemented (clients do not currently require authentication)
        parent.run(multi,parent.logFilename());
    }


public:
    RFS_WindowsService(RFS_ServerBase &_parent)
        : parent(_parent)
    {
    }


    void start()
    {
        parent.getServiceName(sizeof(servicename),servicename,sizeof(displayname),displayname);
        SERVICE_TABLE_ENTRYA ServiceTable[2];
        ServiceTable[0].lpServiceName = servicename;
        ServiceTable[0].lpServiceProc = (LPSERVICE_MAIN_FUNCTIONA)ServiceMain;

        ServiceTable[1].lpServiceName = NULL;
        ServiceTable[1].lpServiceProc = NULL;
        // Start the control dispatcher thread for our service
        if (!StartServiceCtrlDispatcherA(ServiceTable)) {
            logError(&parent,"StartServiceCtrlDispatcher",GetLastError());
        }
    }

    void stop()
    {

        ServiceStatus.dwCurrentState       = SERVICE_STOPPED;
        ServiceStatus.dwCheckPoint         = 0;
        ServiceStatus.dwWaitHint           = 0;
        ServiceStatus.dwWin32ExitCode      = 0;
        ServiceStatus.dwServiceSpecificExitCode = 0;
        SetServiceStatus(hStatus, &ServiceStatus);
    }

    // Control handler function
    static void ControlHandler(DWORD request)
    {
        RFS_context::RFS_Service->doControlHandler(request) ;
    }


    static void ServiceMain(int argc, char** argv)
    {
        RFS_context::RFS_Service->doServiceMain(argc, argv);
    }

    bool running() { return ServiceStatus.dwCurrentState == SERVICE_RUNNING; }

};



bool installService(const char *servicename,const char *servicedisplayname,const char *dependancies,int argc, const char **argv)
{
    DWORD err = ERROR_SUCCESS;
    char path[512];
    if (GetModuleFileNameA( NULL, path, sizeof(path) )) {
        SC_HANDLE hSCM = OpenSCManager( NULL, NULL, SC_MANAGER_ALL_ACCESS);  // full access rights
        if (hSCM) {
            size_t sz = strlen(path)+12;    // calc max size for command line
            for (int i0=0;i0<argc;i0++) {
                if (argv[i0])
                    sz += strlen(argv[i0])+1;
            }
            char *fullpath = (char *)malloc(sz);
            strcpy(fullpath,path);
            strcat(fullpath," --service");
            for (int i=0;i<argc;i++) {
                strcat(fullpath," ");
                strcat(fullpath,argv[i]);
            }
            SC_HANDLE hService = CreateServiceA(
                hSCM, servicename, servicedisplayname,
                SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS,
                SERVICE_AUTO_START, SERVICE_ERROR_NORMAL,
                fullpath,NULL,NULL,dependancies,NULL,NULL);
            if (hService) {
                Sleep(1000);
                StartService(hService,0,0);
                CloseServiceHandle(hService);
            }
            else
                err = GetLastError();
            free(fullpath);
            CloseServiceHandle(hSCM);       }
        else
            err = GetLastError();
    }
    else
        err = GetLastError();
    if (err!=ERROR_SUCCESS) {
        logError(NULL,"Install failed",err);
        return false;
    }
    return true;
}



bool uninstallService(const char *servicename,const char *servicedisplayname)
{
    DWORD err = ERROR_SUCCESS;
    SC_HANDLE hSCM = OpenSCManager( NULL, NULL, SC_MANAGER_ALL_ACCESS);  // full access rights
    if (hSCM) {
        SC_HANDLE hService = OpenServiceA(hSCM, servicename, SERVICE_STOP|SERVICE_QUERY_STATUS);
        if (hService) {
            // try to stop the service
            SERVICE_STATUS ss;
            if ( ControlService( hService, SERVICE_CONTROL_STOP, &ss ) ) {
                Sleep( 1000 );

                while( QueryServiceStatus( hService, &ss ) ) {
                    if ( ss.dwCurrentState != SERVICE_STOP_PENDING )
                        break;
                    Sleep( 1000 );
                }

                if ( ss.dwCurrentState != SERVICE_STOPPED )
                    logError(NULL,"Failed to stop",0);
            }
            CloseServiceHandle(hService);
        }
        hService = OpenServiceA(hSCM, servicename, DELETE);
        if (hService) {
            // now remove the service
            if (!DeleteService(hService))
                err = GetLastError();
            CloseServiceHandle(hService);
        }
        else
            err = GetLastError();
        CloseServiceHandle(hSCM);
    }
    else
        err = GetLastError();
    if (err!=ERROR_SUCCESS) {
        logError(NULL,"Uninstall failed",err);
        return false;
    }
    return true;
}

class RFS_WindowsService *RFS_context::RFS_Service = NULL;


#else // linux

// Daemon initialization
//==============================================================================

static int makeDaemon()
{
    pid_t   pid, sid;
    pid = fork();
    if (pid < 0) {
        logError(NULL,"fork failed",errno);
        return(EXIT_FAILURE);
    }
    if (pid > 0)
        exit(EXIT_SUCCESS);


    if ((sid = setsid()) < 0) {
        logError(NULL,"setsid failed",errno);
        return(EXIT_FAILURE);
    }

    umask(0);

    freopen("/dev/null", "r", stdin);
    freopen("/dev/null", "w", stdout);
    freopen("/dev/null", "w", stderr);

    pid = fork();                           // To prevent zombies
    if (pid < 0) {
        logError(NULL,"fork failed(2)",errno);
        return(EXIT_FAILURE);
    }
    if (pid > 0)
        exit(EXIT_SUCCESS);

    return(EXIT_SUCCESS);

}

#endif




int RFS_ServerBase::run(bool multi, const char *logname)
{

    assert(!multi);     // multi TBD
    assert(context);
    assert(context->port);
    context->cmd = RFCnone;

    short ver = 0;
    char *progname = (char *)malloc(1024);
    char *hostname = (char *)malloc(1024);
    progname[0] = 0;
    hostname[0] = 0;
    getVersion(1024,progname, ver);
#ifdef _WIN32
    if (gethostname(hostname, 1024)!=0) {
        int err = ERRNO();
        if ((err==WSANOTINITIALISED)&&!context->ws32_lib)
            context->ws32_lib = new win_socket_library;
        gethostname(hostname, 1024);
    }
#else
    gethostname(hostname, 1024);
#endif
    setLogFilename(logname);
    log("Opening DIFS Server %s on %s port %d", progname, hostname, (int)context->port);
    log("Version: %d", (int)ver);
    log("PID: %d", (int)getpid());
    free(progname);
    free(hostname);

    unsigned maxsocks = 16;
    unsigned numsocks = 0;
    T_SOCKET *socks = new T_SOCKET[maxsocks];
    RFS_MemoryBuffer *inbuf = new RFS_MemoryBuffer[maxsocks];
    RFS_ConnectionList *conns = new RFS_ConnectionList[maxsocks];
    try {
        context->listensock = ::socket(AF_INET, SOCK_STREAM, 0);
        if (context->listensock == INVALID_SOCKET)
            throwError(ERRNO(),"::socket",true);
#ifndef _WIN32
        {
            int on = 1;
            setsockopt( context->listensock, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));
        }
#endif
        socks[numsocks++] = context->listensock;
        struct sockaddr_in u;
        memset(&u, 0, sizeof(u));
        u.sin_family = AF_INET;
        u.sin_addr.s_addr = htonl(INADDR_ANY);
        u.sin_port = htons(context->port);
        if (::bind(context->listensock, (struct sockaddr *)&u, sizeof(u)) == -1)
            throwError(ERRNO(),"::bind",true);
        if (::listen(context->listensock, 16) == -1)
            throwError(ERRNO(),"::listen",true);

        time_t lastpoll = time(NULL);
        unsigned idlecount = 0;
        context->running = true;
        while (context->running) {
            struct timeval tv;
            tv.tv_sec = 1;
            tv.tv_usec = 0;

            int fdmax = 0;
            fd_set fds;
            FD_ZERO(&fds);
            for (unsigned i=0;i<numsocks;i++) { // this can be expensive but hopefully not large here
                if (socks[i]!=INVALID_SOCKET) {
                    FD_SET(socks[i],&fds);
                    if (fdmax<(int)socks[i])
                        fdmax = socks[i];
                }
            }
            int rc = ::select(fdmax+1, &fds, NULL, NULL, &tv);
            if (rc > 0) {
                // select suceeded
                unsigned i = 0;
                while (context->running&&(i<numsocks)) {
                    T_SOCKET sock = socks[i];
                    if (sock!=INVALID_SOCKET) {
                        if ((FD_ISSET(sock,&fds))) {
                            if (i==0) {  // connection
                                int newsock = ::accept(context->listensock, NULL, NULL);
                                if (newsock==-1) {
                                    throwError(ERRNO(),"::accept",true);
                                }
                                unsigned j;
                                for (j=0;j<numsocks;j++) {
                                    if (socks[j]==INVALID_SOCKET) {
                                        socks[j] = newsock;
                                        break;
                                    }
                                }
                                if (j==numsocks) {
                                    if (numsocks>=maxsocks) {
                                        maxsocks += 16;
                                        T_SOCKET *old = socks;
                                        socks = NULL;
                                        socks = new T_SOCKET[maxsocks];
                                        memcpy(socks,old,sizeof(T_SOCKET)*numsocks);
                                        delete [] old;
                                        RFS_MemoryBuffer *oldbuf = inbuf;
                                        inbuf = NULL;
                                        inbuf = new RFS_MemoryBuffer[maxsocks];
                                        unsigned j;
                                        for (j=0;j<maxsocks-16;j++)
                                            inbuf[j].swapWith(oldbuf[j]);
                                        delete [] oldbuf;
                                        RFS_ConnectionList *oldcl = conns;
                                        conns = NULL;
                                        conns = new RFS_ConnectionList[maxsocks];
                                        for (j=0;j<maxsocks-16;j++)
                                            conns[j].swapWith(oldcl[j]);
                                        delete [] oldcl;
                                    }
                                    socks[numsocks++] = newsock;
                                }
                            }
                            else { // command
                                RFS_MemoryBuffer &in = inbuf[i];
                                RFS_ConnectionList &connlist = conns[i];
                                int err;
                                if (!in.maxLength()) {
                                    // read size
                                    unsigned blocksize;
                                    size_t ret = do_recv(sock, (char *)&blocksize, sizeof(blocksize),err);
                                    if (ret!=sizeof(blocksize)) {
                                        if (ret!=0)
                                            logError(this,"recv failed",ERRNO(),(unsigned)ret);
                                        blocksize = 0;
                                    }
                                    BECONV(blocksize);
                                    if (blocksize>MAX_BLOCK_SIZE) {
                                        logError(this,"recv failed block too big",ERRNO(),(unsigned)blocksize);
                                        blocksize = 0;
                                    }
                                    if (blocksize==0) {
                                        for (unsigned j=0;j<numsocks;j++) {
                                            if (socks[i]==sock) {
                                                socks[i] = INVALID_SOCKET;
                                                break;
                                            }
                                            ::closesocket(sock);
                                        }
                                        sock = INVALID_SOCKET;
                                    }
                                    in.clear(blocksize);
                                }
                                if (sock != INVALID_SOCKET) {
                                    // read as much as have
#ifdef _WIN32
                                    u_long avail;
                                    if (ioctlsocket(sock, FIONREAD, &avail)!=0)
#else
                                    int avail;
                                    if (ioctl(sock, FIONREAD, &avail)!=0)
#endif
                                    {
                                        logError(this,"ioctl failed",ERRNO());
                                        avail = 0;
                                    }
                                    if (avail) {
                                        if (avail>in.maxLength()-in.length()) // protocol error?
                                            avail = in.maxLength()-in.length();
                                        size_t ret = do_recv(sock, (char *)in.reserve(avail), avail,err);
                                        if (ret!=avail) {
                                            char ln[255];
                                            sprintf(ln,"recv failed expected %d, got %d",avail,ret);
                                            logError(this,ln,ERRNO());
                                        }
                                    }
                                    if (in.maxLength()==in.length()) {
                                        if (!processCommand(*this,*context,connlist,sock,in)) {
                                            if (socks[i]==sock) {
                                                socks[i] = INVALID_SOCKET;
                                                break;
                                            }
                                            ::closesocket(sock);
                                        }
                                        in.clear();
                                    }
                                }
                            }
                        }
                    }
                    i++;
                }
            }
            else if (rc<0) {
                int err = ERRNO();
                if (err==EINTRCALL)
                    continue;
                throwError(ERRNO(),"::select",true);
            }
            time_t t = time(NULL);
            if (t-lastpoll>0) {
                if (rc>0)
                    idlecount = 0;
                else
                    idlecount += (unsigned)(t-lastpoll);
                lastpoll = t;
                poll();
            }
#ifdef _WIN32
            if (context->RFS_Service&&!context->RFS_Service->running())
                context->running = false;
#endif
        }
    }
    catch (RFS_Exception *e)
    {
        logError(this,e->str,0);
        delete e;
    }
    catch (...) {
        logError(this,"Unknown exception raised",0);
    }
    delete [] conns;
    delete [] socks;
    delete [] inbuf;
    return 0;
}

void RFS_ServerBase::stop()
{
    context->running = false;
}

void RFS_ServerBase::throwError(int err, const char *errstr, bool fatal)
{
    int rfserr = RFSERR_SysError;
    for (unsigned i=0;;i++)
        if (mapCmdToErr[i].cmd == context->cmd) {
            rfserr = mapCmdToErr[i].err;
            break;
        }
        else if (mapCmdToErr[i].cmd == RFCmax)
            break;
    throw new RFS_Exception(rfserr, err, errstr, fatal);
}


bool RFS_ServerBase::init(int &argc, const char **argv)
{
    assert(context==NULL);  // must be called once at start
    assert(argc);
    assert(argv);
    context = new RFS_context;
    context->port = 0;
    context->running = false;
    context->listensock = INVALID_SOCKET;
    context->cmd = RFCnone;
    context->logname = NULL;
    context->debug = 0;
#ifdef _WIN32
    context->ws32_lib = NULL;
    context->RFS_Service = NULL;
#else
    context->daemon = false;
#endif
    context->argc = 1;
    context->argv = (const char **)malloc(sizeof(const char *));
    context->argv[0] = _strdup(argv[0]);

    int fp = 1;
    // first param can be service/daemon
    if (argc>1) {
#ifdef _WIN32
        if (_stricmp(argv[1],"--install")==0) {
            char servicename[256];
            char displayname[256];
            getServiceName(sizeof(servicename),servicename,sizeof(displayname),displayname);
            if (installService(servicename,displayname,NULL,argc-2,argv+2)) {
                printf("Service %s installed",displayname);
                return 0;
            }
            return false;
        }
        if (_stricmp(argv[1],"--remove")==0) {
            char servicename[256];
            char displayname[256];
            getServiceName(sizeof(servicename)-1,servicename,sizeof(displayname)-1,displayname);
            if (uninstallService(servicename,displayname)) {
                printf("Service %s uninstalled",displayname);
                return 0;
            }
            return false;
        }
#else
        if (_stricmp(argv[1],"--daemon=")==0) {
            context->daemon = true;
        }
#endif
    }
    // other params
    for (int i=fp;i<argc;i++) {
        const char *arg = argv[i];
        if (!arg)
            break;
        bool consumed = false;
        if (*arg=='-') {
            if (strncasecmp(arg+1,"-port=",6)==0) {
                context->port = atoi(arg+7);
                consumed = true;
            }
            if (strncasecmp(arg+1,"-debug=",7)==0) {
                context->debug = atoi(arg+8);
                consumed = true;
            }
#ifdef _WIN32
            if (_stricmp(arg+1,"-service")==0) {
                context->RFS_Service = new RFS_WindowsService(*this);
                consumed = true;
            }
#endif
        }
        if (consumed) {
            for (int j=i+1;j<argc;j++)
                argv[j-1] = argv[j];
            argc--;
            i--;
        }
        else {
            context->argc++;
            context->argv = (const char **)realloc(context->argv,sizeof(const char *)*context->argc);
            context->argv[context->argc-1] = _strdup(arg);
        }
    }
    if (!context->port) {
        context->port = getDefaultPort();
        if (!context->port)
            return false;
    }
#ifdef _WIN32
    if (context->RFS_Service) {
        context->RFS_Service->start();
        return false;
    }
#endif
    return true;
}

void RFS_ServerBase::setLogFilename(const char *filename)
{
    free(context->logname);
    if (!filename) {
        context->logname = NULL;
        return;
    }
    char *deflogname=NULL;
    if (!*filename) {
        const char * ts = context->argv[0];
        const char * te = ts;
        const char * ex = NULL;
#ifdef _WIN32
        char tmp[256];
        if (context->RFS_Service) {
            char displayname[256];
            strcpy(tmp,"c:\\");
            getServiceName(sizeof(tmp)-4,tmp+3,sizeof(displayname)-1,displayname);
            ts = tmp;
            te = ts+strlen(ts);
        }
#endif
        while (*te) {
            if ((*te=='/')||(*te=='\\')) {
                if (te[1])
                    ts = te+1;
            }
            else if (*te=='.')
                ex = te;
            te++;
        }
        if (!ex)
            ex = te;
        deflogname = (char *)malloc(strlen(ts)+32);
        time_t st;
        time(&st);
        struct tm *_tm = localtime(&st);
        int l = (int)(ex-ts);
        sprintf(deflogname,"%.*s.%02u_%02u_%02u_%02u_%02u_%02u.log", (int)(ex-ts), ts, _tm->tm_mon+1,_tm->tm_mday,_tm->tm_year%100,_tm->tm_hour,_tm->tm_min,_tm->tm_sec);
        filename = deflogname;
    }
    context->logname = (char *)malloc(512);
#ifdef _WIN32
    char *filepart;
    GetFullPathNameA(filename, 512, context->logname, &filepart);
#else
    realpath(filename,context->logname);
#endif
    free(deflogname);
}

void RFS_ServerBase::log(const char *fmt, ...)
{
    if (context->logname==NULL)
        return;
    size_t sz = 1024;
    va_list ap;
    char *s = (char *)malloc(sz);
    while (s) {
        va_start(ap, fmt);
        int ret = _vsnprintf (s, sz, fmt, ap);
        va_end(ap);
        if ((ret >= 0)&&(ret<(int)sz)) {
            sz = ret;
            break;
        }
        if (ret >= 0)
            sz = ret+1;
        else
            sz *= 2;
        void *n = realloc(s,sz);
        if (n)
            free(s);
        s = (char *)n;
    }
    if (s) {
        char timeStamp[32];
        time_t tNow;
        time(&tNow);

#ifdef WIN32
        strftime(timeStamp, 32, "%Y-%m-%d %H:%M:%S ", localtime(&tNow)); 
#else
        struct tm ltNow;
        localtime_r(&tNow, &ltNow);
        strftime(timeStamp, 32, "%Y-%m-%d %H:%M:%S ", &ltNow); 
#endif
        RFS_SimpleString out(timeStamp);
        out.appends(s);
        out.appendc('\n');
        int logfile = _open(context->logname,_O_WRONLY | _O_CREAT | _O_APPEND ,
            _S_IREAD|_S_IWRITE // not quite sure about this
        );
        if (logfile>=0) {
            if (_write(logfile,out.str(),out.length())>0) {
                free(s);
                _close(logfile);
                return;
            }
            _close(logfile);
        }
        static bool firsterr = true;
        if (firsterr) {
            firsterr = false;
            logError(this, context->logname ,errno);
        }
        fprintf(stderr,"%.*s",out.length(),out.str());
        free(s);
    }
}



int RFS_ServerBase::debugLevel() { return context?context->debug:0; }

const char * RFS_ServerBase::logFilename()
{
    return context?context->logname:NULL;
}

unsigned short RFS_ServerBase::getDefaultPort()
{
    logError(NULL,"port not specified  (e.g. --port=7080)",0);
    return 0;
}

void RFS_ServerBase::getServiceName(size_t maxname, char *outname,
                     size_t maxdisplayname, char *outdisplayname)
{
    logError(NULL,"Service installation not allowed",0);
    outname[0] = 0;
    outdisplayname[0] = 0;
}

int RFS_ServerBase::serviceInit(int argc, const char **argv,
                            bool &outmulti)
{
    return 0;
}

RFS_context *RFS_ServerBase::queryContext()
{
    return context;
}

RFS_CSVwriter::RFS_CSVwriter()
    : out(NULL,0x10000)
{
}

void RFS_CSVwriter::rewrite()
{
    out.clear();
}


void RFS_CSVwriter::putField(size_t fldsize,void *data)
{
    out.appendc('\'');
    char *s = (char *)data;
    while (fldsize--) {
        char c = *(s++);
        if (c=='\'')
            out.appendc(c);
        out.appendc(c);
    }
    out.appendc('\'');
    out.appendc(',');
}

void RFS_CSVwriter::putRow()
{
    if (out.length())
        out.data()[out.length()-1] = '\n';
}

void RFS_CSVwriter::consume(size_t sz)
{
    // this could be done better (copies a bit too much!)
    assert(sz<=out.length());
    if (sz>out.length())
        sz = out.length();
    memmove(out.data(),out.data()+sz,out.length()-sz);
    out.setLength(out.length()-sz);
}


RFS_CSVreader::RFS_CSVreader()
    : fld(NULL,0x10000)
{
    str = NULL;
    end = NULL;
}


void RFS_CSVreader::reset(size_t sz,const void *data)
{
    str = (char *)data;
    end = str + (str?sz:0);
    fld.clear();
}

bool RFS_CSVreader::nextRow()
{
    while (str!=end) {
        if (*str!='\n')
            return true;
        str++;
    }
    return false;
}

const char * RFS_CSVreader::getField(size_t &fldsize)
{
    if ((str==end) || (*str=='\n')) {
        fldsize = 0;
        return NULL;
    }
    fld.clear();
    char quote = 0;
    if (*str=='\'')
        quote = *(str++);
    while (str!=end) {
        char c = *(str++);
        if (c==quote) {
            if ((str==end)||(*str!=quote))
                break;
            str++;
        } else if (!quote&&(c==','))
            break;
        fld.appendc(c);
    }
    if (*str==',')
        str++;
    fldsize = fld.length();
    return fld.str();
}

RFS_SimpleString::RFS_SimpleString(const char *inits,size_t initsz)
{
    inc = initsz;
    if (inc<0x1000)
        inc = 0x1000;
    max = initsz;
    if (inits) {
        size_t sz = strlen(inits);
        if (sz>=max)
            max = sz+1;
        base = (char *)malloc(max);
        memcpy(base,inits,sz);
        end = base+sz;
    }
    else {
        base = (char *)malloc(max);
        end = base;
    }
}

void RFS_SimpleString::appendc(char c)
{
    size_t l = end-base;
    if (l+1>=max) {
        max += 0x1000;
        base = (char *)realloc(base,max);
        end = base+l;
    }
    *(end++) = c;
}

void RFS_SimpleString::appends(const char *s)
{
    while (*s)
        appendc(*(s++));
}


void RFS_SimpleString::trim()
{
    while ((end!=base)&&(isspace((unsigned char)*(end-1))))
        end--;
}

