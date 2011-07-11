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

// Prints OS cached details for files open by a process

#include "platform.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include "ctfile.hpp"

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
char *itoa(int n, char *str, int b)
{
    return _itoa(n, str, b, (n<0));
}

size_t page_size;

inline offset_t pagetoofs(unsigned pg)
{
    return (offset_t)page_size*(offset_t)pg;
}



void nodeStats(int fd,offset_t ofs,unsigned &leaves,unsigned &nonleaves)
{
    _lseeki64(fd, ofs, SEEK_SET);
    NodeHdr hdr;
    if (_read(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        printf("ERROR: Could not read node at %"I64F"x", ofs);
        return;
    }
    if (hdr.leafFlag) 
        leaves++;
    else
        nonleaves++;
}

void printFileCache(const char *fname, unsigned &globtot, unsigned &globfs, bool verbose, bool onlykey, unsigned &totlf, unsigned &totnl)
{

    if (onlykey&&(strstr(fname,"_of_")==NULL))
        return;

    size_t page_index;
    unsigned leaves=0;
    unsigned nonleaves=0;


    int fd = open(fname,O_RDONLY);
    if (fd==-1) {
        int err = errno;
        if (err==ENOENT)
            return;
        printf("ERROR: open %s failed %d\n",fname,err);
        return;
    }
    struct stat file_stat;
    fstat(fd, &file_stat);
    if (!S_ISREG(file_stat.st_mode))
        return;
    

    void * file_mmap = mmap((void *)0, file_stat.st_size, PROT_NONE, MAP_SHARED, fd, 0);
    page_size = getpagesize();
    unsigned fs = (file_stat.st_size+page_size-1)/page_size;
    if (!fs)
        return;
    unsigned char *mincore_vec = (unsigned char *)calloc(1, fs);
    mincore(file_mmap, file_stat.st_size, mincore_vec);
    printf("%s:\n",fname);
    size_t s = 0;
    size_t e = (size_t)-1;
    unsigned tot = 0;
    for (size_t page_index = 0; page_index < fs; page_index++) {
        if (mincore_vec[page_index]&1) {
            if (page_index&&onlykey&&(pagetoofs(page_index)%8192==0))
                nodeStats(fd,pagetoofs(page_index),leaves,nonleaves);
            if (e!=-1) {
                if (page_index!=e+1) {
                    tot += (e-s+1);
                    if (verbose)
                        printf("  0x%"I64F"x-0x%"I64F"x = %"I64F"d\n",pagetoofs(s),pagetoofs(e+1)-1,pagetoofs(e-s+1));
                    s = page_index;
                }
            }
            else
                s = page_index;
            e = page_index;
        }
    }
    if (e!=-1) {
        if (verbose)
            printf("  0x%"I64F"x-0x%"I64F"x = %"I64F"d\n",pagetoofs(s),pagetoofs(e+1)-1,pagetoofs(e-s+1));
        tot += (e-s+1);
    }
    if (onlykey)
        printf("  Cached %"I64F"d of %"I64F"d = %0.2f%  NonLeaves: %u, Leaves: %u\n",pagetoofs(tot),pagetoofs(fs),(double)tot*100.0/(double)fs,nonleaves,leaves);
    else
        printf("  Cached %"I64F"d of %"I64F"d = %0.2f%\n",pagetoofs(tot),pagetoofs(fs),(double)tot*100.0/(double)fs);
    free(mincore_vec);
    munmap(file_mmap, file_stat.st_size);
    close(fd);
    globtot += tot;
    globfs += fs;
    totlf += leaves;
    totnl += nonleaves;
}

void printPidCachedFiles(int pid,bool verbose,bool onlykey)
{
    char path[128];
    char tmp[16];
    strcpy(path,"/proc/");
    strcat(path,itoa(pid,tmp,10));
    strcat(path,"/fd/");
    DIR * handle = ::opendir(path);
    if (!handle) {
        printf("ERROR: opendir %s failed %d\n",path,errno);
        return;
    }
    unsigned leaves = 0;
    unsigned nonleaves = 0;
    unsigned tot = 0;
    unsigned fs = 0;
    size_t pl = strlen(path);
    while (1) {
        struct dirent *entry = readdir(handle);  // don't need _r here 
        if (!entry)
            break;
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        strcat(path,entry->d_name);
        char link[258];
        size_t ll = readlink(path,link,sizeof(link));
        if (ll==(size_t)-1) {
            printf("ERROR readlink failed on %s with %d\n",path,errno);
            break;
        }
        link[ll] = 0;
        path[pl] = 0;
        printFileCache(link,tot,fs,verbose,onlykey,leaves,nonleaves);
    }
    closedir(handle);
    if (onlykey)
        printf("Total cached %"I64F"d of %"I64F"d = %0.2f%  NonLeaves: %u, Leaves: %u\n",pagetoofs(tot),pagetoofs(fs),(double)tot*100.0/(double)fs,nonleaves,leaves);
    else
        printf("Total cached %"I64F"d of %"I64F"d = %0.2f%\n",pagetoofs(tot),pagetoofs(fs),(double)tot*100.0/(double)fs);
}

int main(int argc, const char *argv[])
{
    offset_t nodeAddress = 0;
    if (argc < 2)
    {
        printf("Usage: fcached [ -v | -k ] <pid>\n");
        exit(2);
    }
    int arg=1;
    bool verbose = false;
    bool onlykey = false;
    if ((arg+1<argc)&&(stricmp(argv[arg],"-v")==0)) {
        verbose = true;
        arg++;
    }
    else if ((arg+1<argc)&&(stricmp(argv[arg],"-k")==0)) {
        onlykey = true;
        arg++;
    }
    printPidCachedFiles(atoi(argv[arg]),verbose,onlykey);
    return 0;
}
