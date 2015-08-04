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


// If anyone wants to try and understand crcs especially combining them, try reading 
// http://www.repairfaq.org/filipg/LINK/F_LINK_IN.html if you want to stand any chance!

#ifndef __JCRC__
#define __JCRC__

#include "platform.h"
#include <stdio.h>
#include "jiface.hpp"
#include "jio.hpp"


jlib_decl unsigned crc32(const char *buf, unsigned len, unsigned crc);
jlib_decl unsigned cxc32(unsigned * buf, unsigned numWords, unsigned cxc32);
jlib_decl unsigned short crc16(const void *buf,size32_t len,unsigned short crc);

class jlib_decl CRC32
{
public:
    CRC32(unsigned crc = ~0U) { reset(crc); }
    inline void reset(unsigned _crc = ~0U) { crc = _crc; }
    inline unsigned get() const { return ~crc; }
    void skip(offset_t length);
    void tally(unsigned len, const void * buf);

protected:
    unsigned crc;
};

class jlib_decl CRC32Merger
{
public:
    CRC32Merger();

    void addChildCRC(offset_t nextLength, unsigned nextCRC, bool fullCRC=false);    // length = length of chunk CRC'd fullCRC=true if seed was ~0 and results was ~'d (e.g. from CRC32 class).
    void clear();
    unsigned get() { return ~crc; }

protected:
    unsigned crc;
};

interface ICrcIOStream : extends IIOStream
{
    virtual unsigned queryCrc() = 0;
};
jlib_decl ICrcIOStream *createCrcPipeStream(IIOStream *stream);

jlib_decl unsigned getFileCRC(const char * name);       // correctly uses ~0 for seed and ~ on result.

//Legacy...
jlib_decl unsigned crc_file(const char * name);     // NB: Does not correctly use 0xffffffff


// Fast inline 16 bit sumcheck
inline unsigned short chksum16(const void *ptr,size32_t sz)
{
    const unsigned short *p = (const unsigned short *)ptr;
    unsigned sum =0;

    while (sz>1) {
        sum += *p++;
        sz -= 2;
    }
    if (sz) 
        sum += *(const byte *)p;
    sum = (sum >> 16) + (sum & 0xffff);       // add in carrys
    sum += (sum >> 16);                       // and again
    return (unsigned short)(~sum);
}



#endif
