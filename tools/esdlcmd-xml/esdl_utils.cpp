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

#include <platform.h>

#include "esdl_utils.hpp"

#if defined (__linux__)
#include <dirent.h>
#endif

// borrow from jlib
bool es_checkDirExists(const char * filename)
{
#ifdef _WIN32
    DWORD attr = GetFileAttributes(filename);
    return (attr != (DWORD)-1)&&(attr & FILE_ATTRIBUTE_DIRECTORY);
#else
    struct stat info;
    if (stat(filename, &info) != 0)
        return false;
    return S_ISDIR(info.st_mode);
#endif
}

void es_createDirectory(const char* dir)
{
    if (dir && *dir)
    {
        if (!es_checkDirExists(dir)) {
#ifdef WIN32
            if (mkdir(dir)!=0)
#else
            if (mkdir(dir,0755)!=0)
#endif
            {
                fprintf(stderr,"Create directory %s failed", dir);
                exit(1);
            }
        }
    }
}

int es_createFile(const char* src, const char* ext)
{
    char * path = es_changeext(src,ext);
    //printf("Target: %s\n", path);
    int h = open(path,_O_WRONLY | _O_CREAT | _O_TRUNC | _O_TEXT  , _S_IREAD|_S_IWRITE);
    if (h==-1)
    {
        printf("Could not open file for write: %s (current dir: %s)\n",path,getcwd(NULL,0));
    }
    free(path);
    return h;
}

char * es_gettail(const char *fn)
{
    const char *e=NULL;
    const char *e1=fn;
    while((e1=strchr(e1,'.'))!=NULL)
        e = e1++;
    const char *s=fn;
    const char *s1;
#ifdef _WIN32
    if (*s&&s[1]==':')
        s+=2;
#endif
    for (s1 = s;*s1&&(s1!=e);s1++)
#ifdef _WIN32
        if (*s1=='\\')
#else
            if (*s1=='/')
#endif
                s = s1+1;
            size_t l = s1-s;
            char *ret = (char *)malloc(l+1);
            memcpy(ret,s,l);
            ret[l] = 0;
            return ret;
}

char * es_changeext(const char *fn,const char *ext)
{
    int preext,  l;
    preext = l = strlen(fn);
    char *p;
    for (p=(char*)(fn+preext-1); p>=fn; p--)
    {
        if (*p == '.')
        {
            preext = p-fn;
            break;
        }
    }

    //  char *ret=gettail(fn);
    //  size_t l = strlen(ret);
    //  ret = (char *)realloc(ret,l+strlen(ext)+2);

    char *ret = (char *)malloc(preext+strlen(ext)+2);
    memcpy(ret, fn, preext);

    ret[preext] = '.';
    strcpy(ret+preext+1,ext);
    return ret;
}

char * es_changetail(const char *fn,const char *tail, const char *ext)
{
    int preext,  l;
    preext = l = strlen(fn);
    char *p;
    for (p=(char*)(fn+preext-1); p>=fn; p--)
    {
        if (*p == '.')
        {
            preext = p-fn;
            break;
        }
    }

    char *ret = (char *)malloc(preext+strlen(tail)+strlen(ext)+2);
    memcpy(ret, fn, preext);

    ret[preext] = 0;
    strcat(ret,tail);
    strcat(ret,".");
    strcat(ret,ext);
    return ret;
}

bool es_hasext(const char *fn,const char *ext)
{ // assumes 3 char ext
    const char *s = strstr(fn,ext);
    if (!s)
        return false;
    return (s!=fn)&&(*(s-1)=='.')&&(s[4]==0);
}

int es_createFile(const char* src, const char* tail, const char* ext)
{
    char * path=es_changetail(src,tail,ext);
    int h = open(path,_O_WRONLY | _O_CREAT | _O_TRUNC | _O_TEXT  , _S_IREAD|_S_IWRITE);
    if (h==-1)
    {
        printf("Could not open file for write: %s (current dir: %s)\n",path,getcwd(NULL,0));
    }
    free(path);
    return h;
}

//==============================================================
// class StrBuffer

#include <stdarg.h>

static const char * TheNullStr = "";

#define FIRST_CHUNK_SIZE  8
#define DOUBLE_LIMIT      0x100000          // must be a power of 2
//#define DETACH_GRANULARITY 16

StrBuffer::StrBuffer()
{
    init();
}

StrBuffer::StrBuffer(const char *value)
{
    init();
    append(value);
}

StrBuffer::StrBuffer(unsigned len, const char *value)
{
    init();
    append(len, value);
}

void StrBuffer::_realloc(size32_t newLen)
{
    if (newLen >= maxLen)
    {
        size32_t newMax = maxLen;
        if (newMax == 0)
            newMax = FIRST_CHUNK_SIZE;
        if (newLen > DOUBLE_LIMIT)
        {
            newMax = (newLen + DOUBLE_LIMIT) & ~(DOUBLE_LIMIT-1);
            if (newLen >= newMax)
                throw "StrBuffer::_realloc: Request for memory is too big";
        }
        else
        {
            while (newLen >= newMax)
                newMax += newMax;
        }
        char * newStr;
        if(!newMax || !(newStr=(char *)realloc(buffer, newMax)))
            throw "StrBuffer::_realloc: Failed to realloc memory";

        buffer = newStr;
        maxLen = newMax;
    }
}

StrBuffer & StrBuffer::append(int value)
{
    char temp[12];

    unsigned written = sprintf(temp, "%d", value);
    return append(written, temp);
}

StrBuffer & StrBuffer::append(unsigned int value)
{
    char temp[12];

    unsigned written = sprintf(temp, "%u", value);
    return append(written, temp);
}

StrBuffer & StrBuffer::append(char c)
{
    appendf("%c",c);
    return *this;
}

StrBuffer & StrBuffer::append(double value)
{
    char temp[317];
    unsigned written = sprintf(temp, "%g", value);
    return append(written, temp);
}

StrBuffer & StrBuffer::append(const char * value)
{
    if (value)
    {
        size32_t SourceLen = (size32_t)::strlen(value);

        ensureCapacity(SourceLen);
        memcpy(buffer + curLen, value, SourceLen);
        curLen += SourceLen;
    }
    return *this;
}

StrBuffer & StrBuffer::append(unsigned len, const char * value)
{
    if (len)
    {
        ensureCapacity(len);
        memcpy(buffer + curLen, value, len);
        curLen += len;
    }
    return *this;
}

StrBuffer & StrBuffer::appendf(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    valist_appendf(format, args);
    va_end(args);
    return *this;
}

StrBuffer & StrBuffer::setf(const char *format, ...)
{
    clear();
    va_list args;
    va_start(args, format);
    valist_appendf(format, args);
    va_end(args);
    return *this;
}

StrBuffer & StrBuffer::append(const char * value, int offset, int len)
{
    ensureCapacity(len);
    memcpy(buffer + curLen, value+offset, len);
    curLen += len;
    return *this;
}

StrBuffer & StrBuffer::replace(char oldChar, char newChar)
{
    if (buffer)
    {
        int l = curLen;
        for (int i = 0; i < l; i++)
            if (buffer[i] == oldChar)
         {
                buffer[i] = newChar;
            if (newChar == '\0')
            {
                 curLen = i;
               break;
            }
         }
    }
    return *this;
}

StrBuffer & StrBuffer::valist_appendf(const char *format, va_list args)
{
    const int BUF_SIZE = 1024;
    char  buf[BUF_SIZE];

    int len = _vsnprintf(buf,sizeof(buf),format,args);
    if (len >= 0)
    {
        if (len < BUF_SIZE)
            append(len, buf);
        else
        {
            ensureCapacity(len);
            // no need for _vsnprintf since the buffer is already made big enough
            vsprintf(buffer+curLen,format,args);
            curLen += len;
        }
    }
    else
    {
        size32_t size = BUF_SIZE * 2;
        char* pbuf = (char *)malloc(size);
        while ((len = _vsnprintf(pbuf,size,format,args)) < 0)
        {
            size <<= 1;
            pbuf = (char*)realloc(pbuf,size);
        }
        append(len, pbuf);
        free(pbuf);
    }

    return *this;
}

const char * StrBuffer::str() const
{
    if (buffer)
    {
        buffer[curLen] = '\0';          // There is always room for this null
        return buffer;
    }
    return TheNullStr;
}

void StrBuffer::setLength(unsigned len)
{
    if (len > curLen)
    {
        ensureCapacity(len-curLen);
    }
    curLen = len;
}

VStrBuffer::VStrBuffer(const char* format, ...)
{
    va_list args;
    va_start(args,format);
    valist_appendf(format,args);
    va_end(args);
}

// steal from jlib with some simplification
StrBuffer& encodeXML(const char *x, StrBuffer &ret)
{
    while (true)
    {
        switch(*x)
        {
        case '&':
            ret.append("&amp;");
            break;
        case '<':
            ret.append("&lt;");
            break;
        case '>':
            ret.append("&gt;");
            break;
        case '\"':
            ret.append("&quot;");
            break;
        case '\'':
            ret.append("&apos;");
            break;
        case ' ':
            //ret.append("&#32;");
            ret.append(" ");
            break;
        case '\n':
            ret.append("&#10;");
            break;
        case '\r':
            ret.append("&#13;");
            break;
        case '\t':
            ret.append("&#9;");
            break;
        case '\0':
            return ret;
        default:
            if (*x >= ' ' && ((byte)*x) < 128)
                ret.append(*x);
            else if (*x < ' ' && *x > 0)
                ret.append("&#xe0").append((unsigned int)*(unsigned char *) x).append(';'); // HACK
            else
                ret.append("&#").append((unsigned int)*(unsigned char *) x).append(';');

            break;
        }
        ++x;
    }
    return ret;
}

#ifndef _WIN32

#define _MAX_DRIVE      4
#define _MAX_DIR        256
#define _MAX_FNAME      256
#define _MAX_EXT        256

void _splitpath(const char *path, char *drive, char *dir, char *fname, char *ext)
{
    strncpy(dir, path, (_MAX_DIR-1));

    char* last = strrchr(dir, '.');
    if (last != NULL)
    {
        if (strrchr(last, PATHSEPCHAR)==NULL)
        {
            strncpy(ext, last, (_MAX_EXT-1));
            *last = '\0';
        }
        else
        {
            *ext = '\0';
        }
    }
    else
    {
        *ext = '\0';
    }

    last = strrchr(dir, PATHSEPCHAR);
    if (last != NULL)
    {
        strncpy(fname, ++last, (_MAX_FNAME-1));
        *last = '\0';
    }
    else
    {
        strncpy(fname, dir, (_MAX_FNAME-1));
        *dir = '\0';
    }

    *drive = '\0';
}
#endif

void splitFilename(const char * filename, StrBuffer * drive, StrBuffer * path, StrBuffer * tail, StrBuffer * ext)
{
    char tdrive[_MAX_DRIVE];
    char tdir[_MAX_DIR];
    char ttail[_MAX_FNAME];
    char text[_MAX_EXT];

    ::_splitpath(filename, tdrive, tdir, ttail, text);
    if (drive)
        drive->append(tdrive);
    if (path)
        path->append(tdir);
    if (tail)
        tail->append(ttail);
    if (ext)
        ext->append(text);
}

bool streq(const char* s, const char* t)
{ return strcmp(s,t)==0; }
