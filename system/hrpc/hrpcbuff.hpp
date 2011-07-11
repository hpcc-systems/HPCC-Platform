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

#ifndef HRPCBUFF_HPP
#define HRPCBUFF_HPP

#include "jmisc.hpp" // for endian support
#include "jexcept.hpp" // for assertex

#define BUFFER_RETAIN_LIMIT (0x10000)
// hrpc inline buffer

class HRPCbuffer
{
public:

    HRPCbuffer()
    {
        buff = 0;
        buffhigh = 0;
        bpread = 0;
        bpwrite = 0;
    }

    ~HRPCbuffer()
    {
        free(buff);
    }


    void write(const void *a,size32_t sz)
    {
        size32_t d=bpwrite;
        bpwrite = align(bpwrite+sz);
        if (bpwrite>buffhigh)
            ensure(bpwrite-buffhigh);
        memcpy(buff+d,a,sz);
    }

    void read(void *a,size32_t sz)
    {
#ifdef _DEBUG
        if (bpread+sz>bpwrite) {
            assertex(!"buffer overflow");
        }
#endif
        memcpy(a,buff+bpread,sz);
        bpread = align(bpread+sz);
    }

    char * readstrptr()
    {
        size32_t sz;
        read(&sz,sizeof(sz));
        if (sz==0)
            return NULL;
        _WINREV(sz);
        char *s = buff+bpread;
#ifdef _DEBUG
        if (bpread+sz>bpwrite) {
            assertex(!"buffer overflow");
        }
#endif
        bpread = align(bpread+sz);
        return s;
    }

    char * readstrdup()
    {
        size32_t sz;
        read(&sz,sizeof(sz));
        if (sz==0)
            return NULL;
        _WINREV(sz);
        char *s = buff+bpread;
#ifdef _DEBUG
        if (bpread+sz>bpwrite) {
            assertex(!"buffer overflow");
        }
#endif
        bpread = align(bpread+sz);
        char *ret = (char *)malloc(sz);
        memcpy(ret,s,sz);
        return ret;
    }

    void writestr(const char *s)
    {
        size32_t sz;
        if (s) {
            size32_t l=(size32_t)strlen(s)+1;
            sz = l;
            _WINREV(sz);
            write(&sz,sizeof(sz));
            write(s,l);
        }
        else {
            sz = 0;
            write(&sz,sizeof(sz));
        }
    }

    void rewrite()
    {
        releasewrite(0);
    }
    void reset()
    {
        bpread = 0;
    }
    void sync()
    {
        bpread = bpwrite;
    }
    void * writeptr(size32_t sz)
    {
        size32_t ret=bpwrite;
        bpwrite = align(bpwrite+sz);
#ifdef _DEBUG
        if (bpwrite>buffhigh) {
            assertex(!"space not reserved!");
        }
#endif
        return buff+ret;
    }

    void * writeptr()
    {
        return buff+bpwrite;
    }

    void * readptr(size32_t sz)
    {
        size32_t ret=bpread;
#ifdef _DEBUG
        if (bpread+sz>bpwrite) {
            assertex(!"buffer overflow");
        }
#endif
        bpread = align(bpread+sz);
        return buff+ret;
    }

    void ensure(size32_t sz)
    {
        if (bpwrite+sz>buffhigh) // doesn't need align
            resize(bpwrite+sz);
    }

    size32_t len() // careful as this is aligned len
    {
        return bpwrite-bpread;
    }


    size32_t markread()
    {
        return bpread;
    }
    size32_t markwrite()
    {
        return bpwrite;
    }
    void releaseread(size32_t p)
    {
        bpread = p;
    }
    void releasewrite(size32_t p)
    {
        bpwrite = p;
        bpread = p;
        if ((buffhigh>BUFFER_RETAIN_LIMIT)&&(p<BUFFER_RETAIN_LIMIT)) {
            buffhigh = BUFFER_RETAIN_LIMIT;
            if (p==0) {         // keep to reasonable sizes
                free(buff);
                buff = (char *)malloc(buffhigh);
            }
            else 
                buff = (char *)::realloc(buff,buffhigh);
        }
    }

    void *buffptr(size32_t p)
    {
        return buff+p;
    }

    void shrink()
    {
        free(buff);
        buff = NULL;
    }



protected:
    size32_t align(size32_t sz)
    {
        return (sz+3)&~3;
    }

    void resize(size32_t sz)
    {
        while (buffhigh<sz) {
            if (buffhigh==0)
                buffhigh = 16;
            else if (buffhigh<0x1000)
                buffhigh+=buffhigh;
            else
                buffhigh+=0x1000;
        }
        if (buff)
            buff = (char *)::realloc(buff,buffhigh);
        else
            buff = (char *)::malloc(buffhigh);
#ifdef LOG
        if (!buff) 
            ERRLOG("HRPC parameter out of memory %d,%d",sz,buffhigh);
#endif
        assertex(buff);
    }


    char *  buff;
    size32_t  buffhigh;
    size32_t    bpread;
    size32_t    bpwrite;

};



#endif
