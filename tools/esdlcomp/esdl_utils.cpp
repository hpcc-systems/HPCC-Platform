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
                if (!es_checkDirExists(dir))
                {
                   fprintf(stderr,"Create directory %s failed", dir);
                   exit(1);
                }
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

