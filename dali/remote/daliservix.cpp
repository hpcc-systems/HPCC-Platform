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

// Dali Unix Server
//----------------

// to compile:
// Solaris:  gcc daliservix.cpp -o daliservix -lsocket -lnsl
// or for x86: /usr/sfw/bin/gcc daliservix.cpp -o daliservix -lposix4 -lsocket -lstdc++ -lnsl
// Linux:    gcc daliservix.cpp -o daliservix


#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <errno.h>
#include <utime.h>
#include <dirent.h>
#include <ctype.h>
#include <signal.h>

static const char *VERSTRINGBE= "DS V1.6 - Solaris";
static const char *VERSTRINGLE= "DS V1.6 - Solaris X86";
#define VERNUM 16

#define WATCHDOG_ALARM_TIME  (24*60*60)

//#define _TRACE
//#define _TRACE_RW
//-------------------------------------------------------------------------------------
#ifndef size32_t  // solaris hasn't defined
#define size32_t unsigned
#endif
//------------------------------------------------------------------------------------
#define BUFFER_READS
#define BUFFER_WRITES

typedef unsigned char byte;
typedef long long __int64;

enum class fileBool { foundNo = false, foundYes = true, notFound = 2 };

const int endiancheck = 1;
#define is_bigendian() ((*(const char*)&endiancheck) == 0)

#define VERSTRING (is_bigendian()?VERSTRINGBE:VERSTRINGLE)

#define READ_BUFFER_SIZE (10*1048576)    // 10MB
#define WRITE_BUFFER_SIZE (10*1048576)   // 10MB
#define MAX_BLOCK_HEADER (READ_BUFFER_SIZE+64)
void usage()
{
    printf("usage:  daliservix [ <port> <send-buffer-size-kb> <recv-buffer-size-kb> ]\n\n");
    printf("Default port is 7100\n");
    printf("Version %d:  %s\n\n",VERNUM,VERSTRING);
}

static sem_t *logsem;

void Log(const char *s)
{
    char timeStamp[32];
    time_t tNow;
    time(&tNow);
    unsigned tpid = getpid();
    struct tm ltNow;
    localtime_r(&tNow, &ltNow);
    strftime(timeStamp, 32, "%m/%d/%y %H:%M:%S ", &ltNow);
    sem_wait(logsem);
    fprintf(stderr,"%s PID=%04x - %s\n",timeStamp,getpid(),s);
    sem_post(logsem);
}

void LogF(const char *fmt, ...) __attribute__((format(printf, 1, 2)))
{
    static char logbuf[1024*16];
    va_list args;
    va_start( args, fmt);
    if (vsnprintf(logbuf,sizeof(logbuf)-2,fmt,args)<0)
        logbuf[sizeof(logbuf)-3] = 0;
    Log(logbuf);
    va_end( args );
}


const char *findTail(const char *path)
{
    if (!path)
        return NULL;
    const char *tail=path;
    const char *s = path;
    while (*s)
        if (*(s++)=='/')
            tail = s;
    return tail;
}

char * makePath(char *path,const char *dir,const char *tail)
{
    path[0] = 0;
    unsigned l = 0;
    if (dir&&(tail[0]!='/')) {
        strcpy(path,dir);
        l = strlen(path);
        if (l && (path[l-1]!='/'))
            path[l++] = '/';
    }
    strcpy(path+l,tail);
    return path;
}

static bool WildMatchN ( const char *src, int srclen, int srcidx,
                    const char *pat, int patlen, int patidx,int nocase)
{
  char next_char;
  for (;;) {
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
      if (patidx == patlen)
         return true;
      while (srcidx < srclen) {
        if (WildMatchN(src,srclen,srcidx,
                     pat, patlen, patidx,nocase))
           return true;
        srcidx++;
      }
      return false;
    }
  }
}

bool WildMatch(const char *src, int srclen, const char *pat, int patlen,bool nocase)
{
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

bool WildMatch(const char *src, const char *pat, bool nocase)
{
    return WildMatch(src,strlen(src),pat,strlen(pat),nocase);
}

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
    unsigned char    c;
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


void processCommand(int socket);

extern "C" void sighup_callback(int signo)
{
    LogF("SIGHUP(%d) received, stopping",signo);
    exit(0);
}


static int activity;

extern "C" void sigalarm_callback(int signo)
{
    if (signo!=0) {
        LogF("SIGALRM(%d) received",signo);
        if (activity==0) {
            LogF("No activity since last alarm, aborting process");
            exit(0);
        }
    }
    activity = 0;
    signal(SIGALRM, sigalarm_callback);
    alarm(WATCHDOG_ALARM_TIME);
}


static size32_t do_pread(int fd, void *buf, size32_t count, off_t offset, int &err)
{
    if (++activity==0)
      activity++;
    do {
        int sz = (int)pread(fd,buf,count,offset);
        if (sz>=0) {
            err = 0;
            return (size32_t)sz;
        }
        err = errno;
    } while (err==EINTR);
    return (size32_t)-1;
}

static size32_t do_pwrite(int fd, const void *buf, size32_t len, off_t offset)
{
    if (++activity==0)
      activity++;
    int err;
    do {
        int sz = (int)pwrite(fd,buf,len,offset);
        if (sz>=0) {
            if (sz!=len) {
                LogF("pwrite out of space");
            }
            return (size32_t)sz;
        }
        err = errno;
    } while (err==EINTR);
    LogF("pwrite errno = %d",errno);
    return (size32_t)-1;
}

static size32_t do_recv(int sock, void *buf, size_t len, int flags, int &err)
{
    if (++activity==0)
      activity++;
    do {
        int sz = (int)recv(sock,buf,len,flags);
        if (sz>=0) {
            err = 0;
            return (size32_t)sz;
        }
        err = errno;
    } while (err==EINTR);
    return (size32_t)-1;
}


int server(int port,unsigned sendbufsize,unsigned recvbufsize)
{

    fprintf(stderr, "%s\n", VERSTRING);
    fprintf(stderr, "Opening Dali server on port %d\n", port);


    struct protoent *proto;
    if ( ( proto = getprotobyname("tcp")) == NULL) {
        perror("Could not get protocol number for TCP");
        return -1;
    }


    int sockfd;
    if ( (sockfd = socket(AF_INET, SOCK_STREAM,  proto->p_proto)) < 0) {
        perror("Could not obtain a socket");
        return -1;
    }

    struct sockaddr_in servsock;
    memset( (char *)&servsock, 0, sizeof(servsock));
    servsock.sin_family = AF_INET;
    servsock.sin_addr.s_addr = htonl(INADDR_ANY);
    servsock.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr *)&servsock, sizeof(servsock)) < 0) {
        perror("Could not bind local socket\n");
        return -1;
    }

    sem_unlink("/DALISERVIX_LOGSEM");
    logsem = sem_open("/DALISERVIX_LOGSEM", O_CREAT, S_IRWXG  , 1); // |S_IRWXO|S_IRWXU
    if (logsem == SEM_FAILED)
        perror("sem_open logsem");

    LogF("Opening Dali server on port %d", (int)port);

    listen(sockfd, 5);

    while (1) {
        struct sockaddr_in clientsock;
        socklen_t clilen = sizeof(clientsock);
        int newsockfd = accept(sockfd, (struct sockaddr *) &clientsock, &clilen); // blocks here
#ifdef _TRACE
        LogF("accept returned sockfd %d", (int)newsockfd);
#endif
        if (newsockfd < 0) {
            perror("accept error");
            return -1;
        }

        int childpid;
        if ( (childpid = fork()) < 0) {
            perror("fork failed");
            return -1;
        }


        if (childpid == 0) {    // the child
            close(sockfd);
            if ((childpid = fork()) < 0) {        // forking twice to avoid zombies:
                perror("fork 2 failed\n");
                return -1;
            }
            else if (childpid > 0)
                exit(0);
            //sleep(1);
            if (sendbufsize)
                setsockopt(newsockfd, SOL_SOCKET, SO_SNDBUF, (char *) &sendbufsize, sizeof(sendbufsize));
            if (recvbufsize)
                setsockopt(newsockfd, SOL_SOCKET, SO_RCVBUF, (char *) &recvbufsize, sizeof(recvbufsize));
            signal(SIGHUP, sighup_callback);
            sigalarm_callback(0); // initialize


            processCommand(newsockfd);
            close(newsockfd);
#ifdef _TRACE
            LogF("child with sockfd %d closing", (int)newsockfd);
#endif
            exit(0);
        }
        close(newsockfd);
        int testpid;
        if ((testpid = waitpid(childpid, NULL, 0)) != childpid) {
            perror("waitpid error");
            return -1;
        }
    }
}



int main(int argc, char **argv)
{
    assert(sizeof(bool)==sizeof(byte));
    int port;
    unsigned sendbufsize = 0;
    unsigned recvbufsize = 0;
    if (argc == 1)
      port = 7100;
    else {
        port = atoi(argv[1]);
        if (port==0) {
            usage();
            exit(-1);
        }
        sendbufsize = (argc>2)?(atoi(argv[2])*1024):0;
        recvbufsize = (argc>3)?(atoi(argv[3])*1024):0;
    }
    server(port,sendbufsize,recvbufsize);
    exit(0);
}

//================================================================================================================


#define MAXHANDLES 100


inline  void _cpyrevn(void * _tgt, const void * _src, unsigned len)
{
    char * tgt = (char *)_tgt; const char * src = (const char *)_src+len;
    for (;len;len--) {
        *tgt++ = *--src;
    }
}


inline  void _rev4(char *b) { char t=b[0]; b[0]=b[3]; b[3]=t; t=b[1]; b[1]=b[2]; b[2]=t; }

inline void BECONV(unsigned &v)
{
    if (!is_bigendian())
        _rev4((char *)&v);
}


inline void be_memcpy(void * _tgt, const void * _src, unsigned len)
{
    if (is_bigendian())
        memcpy(_tgt, _src, len);
    else
        _cpyrevn(_tgt, _src, len);
}

class CMemoryBuffer
{
    byte *  buffer;
    size32_t  curLen;
    size32_t  readPos;
    size32_t  maxLen;
public:


    CMemoryBuffer()
    {
        curLen = 0;
        readPos = 0;
        maxLen = 1024;
        buffer = (byte *)malloc(maxLen);
    }

    ~CMemoryBuffer()
    {
        free(buffer);
    }

    size32_t length() { return curLen; }
    size32_t curPos() { return readPos; }
    void setLength(size32_t len)
    {
        assert (len<=maxLen);
        curLen = len;
    }

    inline CMemoryBuffer & appendBigEndian(size_t len, const void * value)
    {
        be_memcpy(reserve(len), value, len);
        return *this;
    }

    inline void readBigEndian(size_t len, void * value)
    {
        be_memcpy(value, readBlock(len), len);
    }

    void *reserve(size32_t sz)
    {
        if (sz>maxLen-curLen) {
            do {
                maxLen += maxLen;
            } while (sz>maxLen-curLen);
            buffer = (byte *)realloc(buffer,maxLen);
        }
        byte *ret = buffer+curLen;
        curLen+=sz;
        return ret;
    }

    CMemoryBuffer &  append(fpos_t value)
    {
        return appendBigEndian(sizeof(value),&value);
    }

    CMemoryBuffer &  append(unsigned value)
    {
        return appendBigEndian(sizeof(value),&value);
    }

    CMemoryBuffer &  append(int value)
    {
        return appendBigEndian(sizeof(value),&value);
    }

    CMemoryBuffer &  append(short value)
    {
        return appendBigEndian(sizeof(value),&value);
    }

    CMemoryBuffer &  append(byte value)
    {
        memcpy(reserve(sizeof(value)),&value,sizeof(value));
        return *this;
    }

    CMemoryBuffer &  append(bool value)
    {
        memcpy(reserve(sizeof(value)),&value,1);
        return *this;
    }

    CMemoryBuffer &   append(const char *s)
    {
        if (!s)
            append("");
        else {
            size32_t l=strlen(s)+1;
            memcpy(reserve(l),s,l);
        }
        return *this;
    }



    void read(byte &b)
    {
        memcpy(&b,buffer+readPos++,1);
    }

    void read(fpos_t &i)
    {
        readBigEndian(sizeof(i),&i);
    }

    void read(unsigned &i)
    {
        readBigEndian(sizeof(i),&i);
    }

    void read(int &i)
    {
        readBigEndian(sizeof(i),&i);
    }

    void read(short &i)
    {
        readBigEndian(sizeof(i),&i);
    }

    void read(bool &_b)
    {
        byte b;
        memcpy(&b,buffer+readPos,sizeof(b));
        readPos+=sizeof(b);
        _b = (bool)b;
    }

    char *readStr()
    {
        size32_t l = strlen((char *)buffer+readPos)+1;
        char *ret = (char *)malloc(l);
        memcpy(ret,buffer+readPos,l);
        readPos+=l;
        return ret;
    }

    const byte *readBlock(size32_t sz)
    {
        assert (sz<=maxLen-readPos);
        byte *ret = buffer+readPos;
        readPos+=sz;
        return ret;
    }

    CMemoryBuffer & reset(size32_t pos=0) { readPos = pos; return *this; }
    CMemoryBuffer & clear() { curLen = 0; readPos = 0; return *this; }

    byte *toByteArray()
    {
        return buffer;
    }

    byte *detach()
    {
        byte *ret = buffer;
        curLen = 0;
        readPos = 0;
        maxLen = 1024;
        buffer = (byte *)malloc(maxLen);
        return ret;
    }


};


struct CDateTime
{
    CDateTime()
    {
        year = 0;
        month = 0;
        day = 0;
        hour = 0;
        min = 0;
        sec = 0;
        nanosec = 0;
    }

    void deserialize(CMemoryBuffer &src)
    {
        src.read(year);
        src.read(month);
        src.read(day);
        src.read(hour);
        src.read(min);
        src.read(sec);
        src.read(nanosec);
    }

    void serialize(CMemoryBuffer &dst) const
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

protected:
    short    year;
    byte     month;
    byte     day;
    byte     hour;
    byte     min;
    byte     sec;
    unsigned nanosec;
};

void timetToIDateTime(CDateTime * target, time_t time)
{
    if (target)
    {
        struct tm tm_r;
        struct tm * gmt = localtime_r(&time,&tm_r);
        //struct tm * gmt = gmtime(&time);
        target->setDate(gmt->tm_year + 1900, gmt->tm_mon + 1, gmt->tm_mday);
        target->setTime(gmt->tm_hour, gmt->tm_min, gmt->tm_sec, 0);
    }
}

time_t timetFromIDateTime(const CDateTime * source)
{
    if (source == NULL)
        return (time_t) 0;

    int bluff;
    struct tm ttm;
    source->getDate(ttm.tm_year, ttm.tm_mon, ttm.tm_mday);
    source->getTime(ttm.tm_hour, ttm.tm_min, ttm.tm_sec, bluff);
    ttm.tm_isdst = -1;

    if(ttm.tm_year >= 1900)
        ttm.tm_year -= 1900;

    ttm.tm_mon -= 1;

    time_t time = mktime(&ttm);
    if (time == (time_t)-1)
        time = 0;
    return time;
}

bool checkDirExists(const char * filename)
{
    struct stat info;
    if (stat(filename, &info) != 0)
        return false;
    return ((info.st_mode&S_IFMT)==S_IFDIR);
}


static bool recursiveCreateDirectory(const char * path)
{
    if (!path || !path[0])
        return false;
    if (checkDirExists(path))
        return false;
    if (mkdir(path,S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH)==0)
        return true; // mode compatible with linux setting

    const char * cur = path;
    if (*cur=='/')
        cur++;
    const char * last = NULL;
    while (*cur) {
        if ((*cur=='/') && cur[1])
            last = cur;
        cur++;
    }
    if (!last)
        return false;
    unsigned len = last-path;
    char *parent = (char *)malloc(len+1);
    memcpy(parent,path,len);
    parent[len] = 0;
    if (mkdir(path,0)!=0) {
        free(parent);
        return false;
    }
    free(parent);
    return true;
}




#define throwError3(e,v,s) { LogF("ERROR: %s(%d) '%s'",#e,v,s?s:""); \
                             char msg[512]; \
                             sprintf(msg,"ERROR: %s(%d) '%s'",#e,v,s?s:""); \
                             reply.append(e); reply.append(msg); }

#define throwError(e)   { LogF("ERROR: %s",#e); reply.append(e).append(#e); }
#define throwError2(e,v)    { LogF("ERROR: %s(%d)",#e,v); \
                             char msg[512]; \
                             sprintf(msg,"ERROR: %s(%d)",#e,v); \
                             reply.append(e); reply.append(msg); }



void sendMemoryBuffer(int socket, CMemoryBuffer & src)
{
    unsigned length = src.length() - sizeof(unsigned);
    char * buffer = (char *)src.toByteArray();
    be_memcpy(buffer, &length, sizeof(unsigned));
    int remaining = (int)src.length();
    while(1) {
        int ret = send(socket, buffer, remaining,0);
#ifdef _TRACE_RW
        LogF("SEND(%d)",remaining);
#endif
        if (ret<0) {
           perror("send failed");
           exit(0);
        }
        remaining -= ret;
        if (remaining==0)
            break;
        buffer += ret;
    }
}

bool receiveMemoryBuffer(int socket, CMemoryBuffer & tgt)
{
    unsigned oldTgtPos = tgt.length();
    unsigned gotLength;
    int err;
    size32_t ret = do_recv(socket, (char *)&gotLength, sizeof(gotLength),MSG_WAITALL, err);
    BECONV(gotLength);
    if (ret!=sizeof(gotLength)) {
        if (err&&(err!=ECONNRESET)) {
            LogF("recv(2) failed %d",err);
            exit(0);
        }
        close(socket);
        return false;
    }
    if (gotLength>MAX_BLOCK_HEADER) {
       LogF("invalid block length %d",gotLength);
       return false;
    }
    byte * replyBuff = (byte *)tgt.reserve(gotLength);
    ret = do_recv(socket, (char *)replyBuff, gotLength,MSG_WAITALL, err);
#ifdef _TRACE_RW
    LogF("RECV(%d)",gotLength+sizeof(unsigned));
#endif
    if (err!=0) {
       LogF("recv(3) failed, gotLength= %d, replyBuff = %d, err = %d",gotLength,(int)replyBuff, err);
       exit(0);
    }
    if (ret==0)
        return false;
    tgt.reset(oldTgtPos);
    return true;
}


//---------------------------------------------------------------------------

typedef enum { IFOcreate, IFOread, IFOwrite, IFOreadwrite, IFOcreaterw } IFOmode;               // modes for open
typedef enum { IFSHnone, IFSHread, IFSHwrite } IFSHmode;            // sharing options.

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
//
    RFCmove,

    RFCmax
    };

#define RFCunlock 31        // not supported but supressed

typedef unsigned char RemoteFileCommandType;

#define ERR_REMOTE_FIRST                        8200
#define ERR_REMOTE_LAST                         8249

#define RFSERR_InvalidCommand                   8200
#define RFSERR_NullFileIOHandle                 8201
#define RFSERR_InvalidFileIOHandle              8202
#define RFSERR_TimeoutFileIOHandle              8203
#define RFSERR_OpenFailed                       8204
#define RFSERR_ReadFailed                       8205
#define RFSERR_WriteFailed                      8206
#define RFSERR_RenameFailed                     8207
#define RFSERR_SetReadOnlyFailed                8208
#define RFSERR_GetDirFailed                     8209
#define RFSERR_MoveFailed                       8210


#define RFEnoerror      0U



class CRemoteFileServer
{
    unsigned numhandles;

protected:
    typedef bool (CRemoteFileServer :: * commandFunc)(CMemoryBuffer & msg, CMemoryBuffer & reply);
public:

    void registerCommand(RemoteFileCommandType cmd, commandFunc handler)    { table[cmd] = handler; }

    CRemoteFileServer()
    {
        numhandles = 0;
        rbuffer = NULL;
        rbufhandle = 0;
        rbufbase = 0;
        rbufsize = 0;
        wbuffer = NULL;
        wbufhandle = 0;
        wbufbase = 0;
        wbufsize = 0;
        RemoteFileCommandType idx;
        for (idx = (RemoteFileCommandType)0; idx < RFCmax; idx++)
            table[idx] = &CRemoteFileServer::cmdUnknown;

        registerCommand(RFCcloseIO, &CRemoteFileServer::cmdCloseFileIO);
        registerCommand(RFCopenIO, &CRemoteFileServer::cmdOpenFileIO);
        registerCommand(RFCread, &CRemoteFileServer::cmdRead);
        registerCommand(RFCsize, &CRemoteFileServer::cmdSize);
        registerCommand(RFCwrite, &CRemoteFileServer::cmdWrite);
        registerCommand(RFCexists, &CRemoteFileServer::cmdExists);
        registerCommand(RFCremove, &CRemoteFileServer::cmdRemove);
        registerCommand(RFCrename, &CRemoteFileServer::cmdRename);
        registerCommand(RFCgetver, &CRemoteFileServer::cmdGetVer);
        registerCommand(RFCisfile, &CRemoteFileServer::cmdIsFile);
        registerCommand(RFCisdirectory, &CRemoteFileServer::cmdIsDir);
        registerCommand(RFCisreadonly, &CRemoteFileServer::cmdIsReadOnly);
        registerCommand(RFCsetreadonly, &CRemoteFileServer::cmdSetReadOnly);
        registerCommand(RFCgettime, &CRemoteFileServer::cmdGetTime);
        registerCommand(RFCsettime, &CRemoteFileServer::cmdSetTime);
        registerCommand(RFCcreatedir, &CRemoteFileServer::cmdCreateDir);
        registerCommand(RFCgetdir, &CRemoteFileServer::cmdGetDir);
        registerCommand(RFCgetcrc, &CRemoteFileServer::cmdGetCrc);
        registerCommand(RFCmove, &CRemoteFileServer::cmdMove);
    }

    ~CRemoteFileServer()
    {
        free(rbuffer);
        free(wbuffer);
        while (numhandles)
          close(handles[--numhandles]);
    }

    //MORE: The file handles should timeout after a while, and accessing an old (invalid handle)
    // should throw a different exception
    bool checkFileIOHandle(CMemoryBuffer &reply, int handle)
    {
        if (handle<=0) {
            throwError(RFSERR_NullFileIOHandle);
            return false;
        }
        unsigned i;
        for (i=0;i<MAXHANDLES;i++)
            if (handles[i]==handle)
                return true;
        throwError(RFSERR_InvalidFileIOHandle);
        return false;
    }


    bool cmdOpenFileIO(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        char *name = msg.readStr();
        byte mode;
        msg.read(mode);
        byte share;
        msg.read(share);  // not used (yet)
        int handle;
        switch (mode)
        {
        case IFOcreate:
            handle = open(name, O_WRONLY|O_TRUNC|O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH );
            break;
        case IFOread:
            handle = open(name, O_RDONLY);
            break;
        case IFOwrite:
            handle = open(name, O_WRONLY);
            break;
        case IFOcreaterw:
            handle = open(name, O_RDWR|O_TRUNC|O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH );
            break;
        case IFOreadwrite:
            handle = open(name, O_RDWR);
            break;
        }
#ifdef _TRACE
        LogF("open file %s, mode %d, handle = %d",name,(int)mode,handle);
#endif
        if ((handle<0)&&(errno==2)) {
            LogF("Could not find file '%s'",name);
            handle = 0;
        }
        if (handle>=0) {
            reply.append(RFEnoerror);
            reply.append(handle);
            if (handle) {
                assert(numhandles<MAXHANDLES);  // TBD
                handles[numhandles++] = handle;
            }
            free(name);
            return true;
        }
        throwError3(RFSERR_OpenFailed,errno,name);
        free(name);
        return false;
    }


    bool cmdCloseFileIO(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        int handle = readFileIOHandle(msg,reply);
        if (handle<0)
            return false;
#ifdef _TRACE
        LogF("close file,  handle = %d",handle);
#endif
        if (wbufhandle==handle) {
            if (!flushwrite()) {
                throwError(RFSERR_WriteFailed);
                return false;
            }
            wbufhandle = NULL;
        }
        if (rbufhandle==handle)
            rbufhandle = NULL;
        close(handle);
        unsigned i;
        for (i=0;i<numhandles;i++)
            if (handles[i]==handle) {
                numhandles--;
                while (i<numhandles)
                    handles[i] = handles[i+1];
            }
        reply.append(RFEnoerror);
        return true;
    }


    size32_t do_buf_read(int handle, void *data, size32_t len, fpos_t pos, int &err)
    {
        size32_t done=0;
        size32_t rd;
        err = 0;
        while (1) {
            if (handle!=rbufhandle) {
                if (rbufhandle!=0)
                    break;
                rbufhandle = handle;
                if (!rbuffer)
                    rbuffer = (char *)malloc(READ_BUFFER_SIZE);
                rd = do_pread(rbufhandle,rbuffer,READ_BUFFER_SIZE,pos,err);
                if (err!=0) {
                    rbufsize = 0;
                    return rd;
                }
                if (rd==0)
                    return done;
                rbufbase = pos;
                rbufsize = rd;
            }
            if (pos<rbufbase)
                break;
            if (pos>=rbufbase+rbufsize)
                rbufhandle = 0;
            else {
                size32_t ofs = (size32_t)(pos-rbufbase);
                size32_t cpy = rbufsize-ofs;
                if (cpy>len)
                    cpy = len;
                memcpy(data,rbuffer+ofs,cpy);
                len -= cpy;
                done += cpy;
                if (len==0)
                    return done;
                data = (char *)data+cpy;
                pos += cpy;
            }
        }
        rd = do_pread(handle,data,len,pos,err);
        if (err!=0)
            return rd;
        return done+rd;
    }

    bool flushwrite()
    {
        if (wbufhandle&&(wbufsize!=0)) {
            clock_t t;
            size32_t numWritten = do_pwrite(wbufhandle, wbuffer, wbufsize, wbufbase);
            wbufbase += wbufsize;
            if (numWritten!=wbufsize) {
                wbufsize = 0;
                return false;
            }
            wbufsize = 0;
        }
        return true;
    }

    size32_t dowrite(int handle, const byte *data, size32_t len, fpos_t pos)
    {
        size32_t done=0;
        while (len) {
            if (handle!=wbufhandle) {
                if (wbufhandle!=0)          // only one handle used per process but in case not
                    return do_pwrite(handle, data, len, pos);
                wbufhandle = handle;
                if (!wbuffer)
                    wbuffer = (char *)malloc(WRITE_BUFFER_SIZE);
                wbufbase = pos;
                wbufsize = 0;
            }
            if ((pos!=wbufbase+wbufsize)||(len+wbufsize>WRITE_BUFFER_SIZE)) {
                if (!flushwrite())
                    return (size32_t)-1;
                wbufbase = pos;
            }
            size32_t tocopy = len;
            if (tocopy>WRITE_BUFFER_SIZE)
                tocopy = WRITE_BUFFER_SIZE;
            len -= tocopy;
            memcpy(wbuffer+wbufsize,data,tocopy);
            data = ((byte *)data)+tocopy;
            wbufsize += tocopy;
            pos += tocopy;
            done += tocopy;
        }
        return done;
    }

    bool cmdRead(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        fpos_t pos;
        size32_t len;
        int handle = readFileIOHandle(msg,reply);
        if (handle<0)
            return false;
        msg.read(pos);
        msg.read(len);

        //arrange it so we read directly into the reply buffer...
        unsigned posOfErr = reply.length();
        reply.append((unsigned)RFEnoerror);
        size32_t numRead;
        unsigned posOfLength = reply.length();
        reply.reserve(sizeof(numRead));
        void * data = reply.reserve(len);
        int err;
#ifdef BUFFER_READS
        numRead = do_buf_read(handle, data, len, pos, err);
#else
        numRead = do_pread(handle, data, len, pos, err);
#endif
#ifdef _TRACE
        LogF("read file,  handle = %d, pos = %lld, toread = %d, read = %d",handle,pos,len,numRead);
#endif
        if (numRead==(size32_t)-1)  {
            reply.setLength(posOfErr);
            throwError2(RFSERR_ReadFailed,err);
            return false;
        }

        be_memcpy((char *)reply.toByteArray()+posOfLength,&numRead,sizeof(numRead));
        reply.setLength(posOfLength+sizeof(numRead)+numRead);
        return true;
    }


    bool cmdSize(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        int handle = readFileIOHandle(msg,reply);
        if (handle<0)
            return false;
#ifdef BUFFER_WRITES
        if (wbufhandle==handle) {
            if (!flushwrite()) {
                throwError(RFSERR_WriteFailed);
                return false;
            }
            wbufhandle = NULL;
        }
#endif
        fpos_t size = lseek(handle,0,SEEK_END); // we don't use seek pos so no need to restore
        reply.append((unsigned)RFEnoerror).append(size);
#ifdef _TRACE
        LogF("size file,  handle = %d, size = %lld",handle,size);
#endif
        return true;
    }


    bool cmdWrite(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        fpos_t pos;
        size32_t len;
        const byte * data;
        int handle = readFileIOHandle(msg,reply);
        if (handle<0)
            return false;
        msg.read(pos);
        msg.read(len);
        data = msg.readBlock(len);

#ifdef BUFFER_WRITES
        size32_t numWritten = dowrite(handle, data, len, pos);
#else
        size32_t numWritten = do_pwrite(handle, data, len, pos);
#endif
#ifdef _TRACE
        LogF("write file,  handle = %d, towrite = %d, written = %d",handle,len,numWritten);
#endif
        if (numWritten==(size32_t)-1) {
            throwError(RFSERR_WriteFailed);
            return false;
        }
        reply.append((unsigned)RFEnoerror).append(numWritten);
        return true;
    }

    bool cmdExists(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        char *filename = msg.readStr();
#ifdef _TRACE
        LogF("exists,  '%s'",filename);
#endif
        struct stat s;
        reply.append((unsigned)RFEnoerror).append((bool)(stat(filename,&s)==0));
        free(filename);
        return true;
    }


    bool cmdRemove(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        char *filename = msg.readStr();
#ifdef _TRACE
        LogF("remove,  '%s'",filename);
#endif
        reply.append((unsigned)RFEnoerror).append((bool)(unlink(filename)==0));
        free(filename);
        return true;
    }

    bool cmdRename(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        char *from = msg.readStr();
        char *to = msg.readStr();

        const char *totail = findTail(to);
        if (totail==to) {   // kludge
            const char *fromtail = findTail(from);
            if (fromtail!=from) {
                unsigned l = fromtail-from;
                char * s = (char *)malloc(strlen(to)+l+1);
                memcpy(s,from,l);
                strcpy(s+l,to);
                free(to);
                to = s;
            }
        }


#ifdef _TRACE
        LogF("rename,  '%s' to '%s'",from,to);
#endif
        if (rename(from,to)!=0) {
            throwError(RFSERR_RenameFailed);
            free(from);
            free(to);
            return false;
        }
        reply.append((unsigned)RFEnoerror);
        free(from);
        free(to);
        return true;
    }


    bool cmdUnknown(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        RemoteFileCommandType cmd;
        msg.reset();
        msg.read(cmd);
        if (cmd!=RFCunlock) {
            throwError2(RFSERR_InvalidCommand, cmd);
        }
        else { // kludge - don't log if unlock
            char msg[512];
            sprintf(msg,"ERROR: RFSERR_InvalidCommand (%d)",cmd);
            reply.append(RFSERR_InvalidCommand); reply.append(msg);
        }
        return false;
    }

    bool cmdGetVer(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        if (msg.length()-msg.curPos()>sizeof(unsigned))
            reply.append((unsigned)RFEnoerror).append(VERSTRING);
        else
            reply.append((unsigned)0x10000+VERNUM).append(VERSTRING);
        return true;
    }


    bool cmdIsFile(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *filename = msg.readStr();
#ifdef _TRACE
        LogF("isFile,  '%s'",filename);
#endif
        struct stat s;
        unsigned ret;
        if (stat(filename, &s) != 0)
            ret = (unsigned)fileBool::notFound;
        else
            ret = (unsigned)(((s.st_mode&S_IFMT)==S_IFREG) ? fileBool::foundYes : fileBool::foundNo);
        reply.append((unsigned)RFEnoerror).append(ret);
        free(filename);
        return true;
    }

    bool cmdIsDir(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *filename = msg.readStr();
#ifdef _TRACE
        LogF("isDir,  '%s'",filename);
#endif
        struct stat s;
        unsigned ret;
        if (stat(filename, &s) != 0)
            ret = (unsigned)fileBool::notFound;
        else
            ret = (unsigned)(((s.st_mode&S_IFMT)==S_IFDIR) ? fileBool::foundYes : fileBool::foundNo);
        reply.append((unsigned)RFEnoerror).append(ret);
        free(filename);
        return true;
    }

    bool cmdIsReadOnly(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *filename = msg.readStr();
#ifdef _TRACE
        LogF("isReadOnly,  '%s'",filename);
#endif
        struct stat s;
        unsigned ret;
        if (stat(filename, &s) != 0)
            ret = (unsigned)fileBool::notFound;
        else
            ret = (s.st_mode & (S_IWUSR|S_IWGRP|S_IWOTH)) ? fileBool::foundNo : fileBool::foundYes;
        // I don't think this is necessarily correct but consistant with linux implementation
        reply.append((unsigned)RFEnoerror).append(ret);
        free(filename);
        return true;
    }

    bool cmdSetReadOnly(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *filename = msg.readStr();
        bool set;
        msg.read(set);
#ifdef _TRACE
        LogF("setReadOnly,  '%s'",filename);
#endif
        struct stat s;
        unsigned ret;
        if (stat(filename, &s) != 0) {
            throwError(RFSERR_SetReadOnlyFailed);
            free(filename);
            return false;
        }
        // not sure correct but consistant with isReadOnly
        if (set)
            s.st_mode &= ~(S_IWUSR|S_IWGRP|S_IWOTH);
        else
            s.st_mode |= (S_IWUSR|S_IWGRP|S_IWOTH);
        chmod(filename, s.st_mode);
        reply.append((unsigned)RFEnoerror);
        free(filename);
        return true;
    }

    bool cmdGetTime(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *filename = msg.readStr();
#ifdef _TRACE
        LogF("getTime,  '%s'",filename);
#endif
        CDateTime createTime;
        CDateTime modifiedTime;
        CDateTime accessedTime;

        struct stat info;
        if (stat(filename, &info) != 0) {
            reply.append((unsigned)RFEnoerror).append((bool)false);
            free(filename);
            return true;
        }
        timetToIDateTime(&accessedTime, info.st_atime);
        timetToIDateTime(&createTime,   info.st_ctime);
        timetToIDateTime(&modifiedTime, info.st_mtime);
        reply.append((unsigned)RFEnoerror).append((bool)true);
        createTime.serialize(reply);
        modifiedTime.serialize(reply);
        accessedTime.serialize(reply);
        free(filename);
        return true;
    }

    bool cmdSetTime(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *filename = msg.readStr();
        bool creategot;
        CDateTime createTime;
        bool modifiedgot;
        CDateTime modifiedTime;
        bool accessedgot;
        CDateTime accessedTime;
        msg.read(creategot);
        if (creategot)
            createTime.deserialize(msg);
        msg.read(modifiedgot);
        if (modifiedgot)
            modifiedTime.deserialize(msg);
        msg.read(accessedgot);
        if (accessedgot)
            accessedTime.deserialize(msg);

#ifdef _TRACE
        LogF("setTime,  '%s' %d",filename);
#endif

        struct utimbuf am;
        if (!accessedgot||!modifiedgot) {
            struct stat info;
            if (stat(filename, &info) != 0) {
                reply.append((unsigned)RFEnoerror).append((bool)false);
                free(filename);
                return true;
            }
            am.actime = info.st_atime;
            am.modtime = info.st_mtime;
        }
        if (accessedgot)
            am.actime   = timetFromIDateTime (&accessedTime);
        if (modifiedgot)
            am.modtime  = timetFromIDateTime (&modifiedTime);
        if(utime(filename, &am)!=0)
            reply.append((unsigned)RFEnoerror).append((bool)false);
        else
            reply.append((unsigned)RFEnoerror).append((bool)true);
        free(filename);
        return true;
    }

    bool cmdCreateDir(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *path = msg.readStr();
#ifdef _TRACE
        LogF("createDir,  '%s'",path);
#endif
        if (*path&&recursiveCreateDirectory(path))
            reply.append((unsigned)RFEnoerror).append((bool)true);
        else
            reply.append((unsigned)RFEnoerror).append((bool)false);
        free(path);
        return true;
    }

    bool cmdGetDir(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *path = msg.readStr();
        char *wildcard = msg.readStr();
        bool includedir;
        bool sub;
        msg.read(includedir);
        msg.read(sub);
#ifdef _TRACE
        LogF("getDir,  '%s' '%s'",path,wildcard);
#endif

        DIR * handle = opendir(path);
        if (!handle) {
            throwError(RFSERR_GetDirFailed);
            free(path);
            free(wildcard);
            return false;
        }
        reply.append((unsigned)RFEnoerror);
        byte b=1;
        struct dirent *entry;
        while(1) {
            entry = readdir(handle);
            // need better checking here?
            if (!entry)
                break;
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            struct stat st;
            char fullname[512];
            if (stat(makePath(fullname,path,entry->d_name), &st) != 0)
                continue;
            bool curisdir = false;
            if ((st.st_mode&S_IFMT)==S_IFDIR) {
                // sub TBD
                if (!includedir)
                    continue;
                curisdir = true;
            }
            if (!*wildcard || WildMatch(entry->d_name, wildcard, false)) {
                reply.append(b);
                reply.append(curisdir);
                fpos_t sz = curisdir?0:st.st_size;
                CDateTime dt;
                memset(&dt,0,sizeof(dt));
                char fullname[512];
                timetToIDateTime(&dt,st.st_mtime);
                reply.append(sz);
                dt.serialize(reply);
                reply.append(entry->d_name);
            }
        }
        closedir(handle);
        b = 0;
        reply.append(b);
        free(path);
        free(wildcard);
    }

    bool cmdGetCrc(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *filename = msg.readStr();
        fpos_t pos=0;
        fpos_t len=0x7fffffffffffffffLL;
        //msg.read(pos);                // pos/len not supported yet
        //msg.read(len);
#ifdef _TRACE
        LogF("getCrc,  '%s' %d %d",filename,from,len);
#endif
        int handle = open(filename, O_RDONLY);
        if (handle<0) {
            throwError3(RFSERR_OpenFailed,errno,filename);
            free(filename);
            return false;
        }
        void *buf = malloc(READ_BUFFER_SIZE);
        unsigned long crc=~0;
        while (len!=0) {
            size32_t sz = (len>READ_BUFFER_SIZE)?READ_BUFFER_SIZE:((size32_t)len);
            int err;
            sz = do_pread(handle,buf,sz,pos,err);
            if (err!=0) {
                throwError3(RFSERR_ReadFailed,err,filename);
                free(filename);
                return false;
            }
            if (sz==0)
                len = 0;
            else
                crc = crc32((const char *)buf,sz,crc);
            len -= sz;
            pos += sz;
        }
        close(handle);
        free(buf);
        reply.append((unsigned)RFEnoerror).append((unsigned)(~crc));
        free(filename);
        return true;
    }


    bool cmdMove(CMemoryBuffer &msg, CMemoryBuffer &reply)
    {
        char *from = msg.readStr();
        char *to = msg.readStr();
#ifdef _TRACE
        LogF("move,  '%s' %s",from,to);
#endif
        int err = rename(from,to);
        if (err<0) {
            throwError3(RFSERR_MoveFailed,errno,from);
            free(from);
            free(to);
            return false;
        }
        reply.append((unsigned)RFEnoerror);
        free(from);
        free(to);
        return true;
    }

    bool dispatchCommand(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        RemoteFileCommandType cmd;
        msg.read(cmd);
        if (cmd < RFCmax)
            return (this->*(table[cmd]))(msg, reply);
        return cmdUnknown(msg,reply);
    }

    void processCommand(int socket)
    {
        char retname[256];
        struct sockaddr_in *nameptr;
        struct sockaddr_in name;
        socklen_t namelen = sizeof(name);
        nameptr = &name;
        if(getpeername(socket,(struct sockaddr*)&name, &namelen)<0) {
            LogF("getpeername failed %d",errno);
        }
        else {
            strncpy(retname,inet_ntoa(nameptr->sin_addr),sizeof(retname));
            LogF("Connected to %s",retname);
        }
        CMemoryBuffer replyBuffer;
        CMemoryBuffer commandBuffer;
        while (receiveMemoryBuffer(socket, commandBuffer.clear())) {
            dispatchCommand(commandBuffer, replyBuffer.clear().append((unsigned)0)); // reserve space for length prefix
            sendMemoryBuffer(socket, replyBuffer);
        }
    }


    int readFileIOHandle(CMemoryBuffer & msg, CMemoryBuffer & reply)
    {
        int handle;
        msg.read(handle);
        if (!checkFileIOHandle(reply,handle))
            return -1;
        return handle;
    }

protected:
    commandFunc         table[RFCmax];
    int                 handles[MAXHANDLES];
    char *              rbuffer;
    int                 rbufhandle;
    fpos_t              rbufbase;
    size32_t                rbufsize;
    char *              wbuffer;
    int                 wbufhandle;
    fpos_t              wbufbase;
    size32_t                wbufsize;
};

void processCommand(int socket)
{
    CRemoteFileServer server;
    server.processCommand(socket);
}


