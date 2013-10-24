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
#include "jcrc.hpp"
#include "jlib.hpp"
#include "jfile.hpp"

const static unsigned short crc_16_tab[256] = { // x^16+x^15+x^2+1
      0x0000,0xc0c1,0xc181,0x0140,0xc301,0x03c0,0x0280,0xc241,
      0xc601,0x06c0,0x0780,0xc741,0x0500,0xc5c1,0xc481,0x0440,
      0xcc01,0x0cc0,0x0d80,0xcd41,0x0f00,0xcfc1,0xce81,0x0e40,
      0x0a00,0xcac1,0xcb81,0x0b40,0xc901,0x09c0,0x0880,0xc841,
      0xd801,0x18c0,0x1980,0xd941,0x1b00,0xdbc1,0xda81,0x1a40,      
      0x1e00,0xdec1,0xdf81,0x1f40,0xdd01,0x1dc0,0x1c80,0xdc41,
      0x1400,0xd4c1,0xd581,0x1540,0xd701,0x17c0,0x1680,0xd641,
      0xd201,0x12c0,0x1380,0xd341,0x1100,0xd1c1,0xd081,0x1040,
      0xf001,0x30c0,0x3180,0xf141,0x3300,0xf3c1,0xf281,0x3240,
      0x3600,0xf6c1,0xf781,0x3740,0xf501,0x35c0,0x3480,0xf441,
      0x3c00,0xfcc1,0xfd81,0x3d40,0xff01,0x3fc0,0x3e80,0xfe41,
      0xfa01,0x3ac0,0x3b80,0xfb41,0x3900,0xf9c1,0xf881,0x3840,
      0x2800,0xe8c1,0xe981,0x2940,0xeb01,0x2bc0,0x2a80,0xea41,
      0xee01,0x2ec0,0x2f80,0xef41,0x2d00,0xedc1,0xec81,0x2c40,
      0xe401,0x24c0,0x2580,0xe541,0x2700,0xe7c1,0xe681,0x2640,
      0x2200,0xe2c1,0xe381,0x2340,0xe101,0x21c0,0x2080,0xe041,
      0xa001,0x60c0,0x6180,0xa141,0x6300,0xa3c1,0xa281,0x6240,
      0x6600,0xa6c1,0xa781,0x6740,0xa501,0x65c0,0x6480,0xa441,
      0x6c00,0xacc1,0xad81,0x6d40,0xaf01,0x6fc0,0x6e80,0xae41,
      0xaa01,0x6ac0,0x6b80,0xab41,0x6900,0xa9c1,0xa881,0x6840,
      0x7800,0xb8c1,0xb981,0x7940,0xbb01,0x7bc0,0x7a80,0xba41,
      0xbe01,0x7ec0,0x7f80,0xbf41,0x7d00,0xbdc1,0xbc81,0x7c40,
      0xb401,0x74c0,0x7580,0xb541,0x7700,0xb7c1,0xb681,0x7640,
      0x7200,0xb2c1,0xb381,0x7340,0xb101,0x71c0,0x7080,0xb041,
      0x5000,0x90c1,0x9181,0x5140,0x9301,0x53c0,0x5280,0x9241,
      0x9601,0x56c0,0x5780,0x9741,0x5500,0x95c1,0x9481,0x5440,
      0x9c01,0x5cc0,0x5d80,0x9d41,0x5f00,0x9fc1,0x9e81,0x5e40,
      0x5a00,0x9ac1,0x9b81,0x5b40,0x9901,0x59c0,0x5880,0x9841,
      0x8801,0x48c0,0x4980,0x8941,0x4b00,0x8bc1,0x8a81,0x4a40,
      0x4e00,0x8ec1,0x8f81,0x4f40,0x8d01,0x4dc0,0x4c80,0x8c41,
      0x4400,0x84c1,0x8581,0x4540,0x8701,0x47c0,0x4680,0x8641,
      0x8201,0x42c0,0x4380,0x8341,0x4100,0x81c1,0x8081,0x4040
};

unsigned short crc16(const void *buf,size32_t len,unsigned short crc)
{
    byte *p=(byte *)buf;
    byte *e=p+len;
    while (p!=e)
        crc = (crc>>8) ^ crc_16_tab[(crc&0xff) ^ *p++];
    return crc;
}

//---------------------------------------------------------------------------

const static unsigned crc_32_tab[] = { /* CRC polynomial 0xedb88320 */
0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

#define UPDC32(octet, crc) (crc_32_tab[((crc) ^ (octet)) & 0xff] ^ ((crc) >> 8))

unsigned crc32(const char *buf, unsigned len, unsigned crc)
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

#define UPDC32_0(crc) (crc_32_tab[(crc) & 0xff] ^ ((crc) >> 8))

unsigned combineCrc32(unsigned crc1, unsigned numBytes, unsigned crc2)
{
    //Can't think of any other way to improve this...  Uses (a+b)%c==(a+b%c)%c
    if (crc1 == 0)
        return crc2;
    unsigned crc = crc1;
    while (numBytes >= 4)
    {
        //Unroll the loop a few times...
        unsigned crcA = UPDC32_0(crc);
        unsigned crcB = UPDC32_0(crcA);
        unsigned crcC = UPDC32_0(crcB);
        crc  = UPDC32_0(crcC);
        numBytes -= 4;
    }
    while (numBytes--)
        crc = UPDC32_0(crc);
    return crc ^ crc2;
}

unsigned combineCrc32(unsigned crc1, unsigned __int64 numBytes, unsigned crc2)
{
    if (crc1 == 0)
        return crc2;
    const unsigned bigChunk = 0xfffffff0;
    unsigned crc = crc1;
    while (numBytes > bigChunk)
    {
        crc = combineCrc32(crc, bigChunk, 0);
        numBytes -= bigChunk;
    }
    return combineCrc32(crc, (unsigned)numBytes, crc2);
}

//---------------------------------------------------------------------------

//Information for a reflected in, reflected out polynomial...
//if I had the energy I could cover the other cases...
class CRC32info
{
public:
    CRC32info(unsigned _poly);

    unsigned skip(unsigned internalCrc, offset_t bytes) const;
    unsigned tally(size32_t len, const void * data, unsigned crc);

protected:
    unsigned multiplyModPoly(unsigned x, unsigned y) const;

protected:
    enum { maxPower = 63 };
    unsigned poly;
    unsigned scale[maxPower+1];             // polynomials to multiply by to skip mask[x] zeros.
    unsigned __int64 mask[maxPower+1];              // powers of two for easy access
};

CRC32info::CRC32info(unsigned _poly)
{
    poly = _poly;

    //MORE: Should calculate table here, rather than using a static one.

    //Calculate an array of skip sequences for powers of two, so that CRCs can be combined easily
    const unsigned one = 0x80000000;
    scale[0] = crc32("\0", 1, one);
    mask[0] = 1;
    for (unsigned power = 1; power <= maxPower; power++)
    {
        scale[power] = multiplyModPoly(scale[power-1], scale[power-1]);
        mask[power] = mask[power-1] << 1;
    }
}

unsigned CRC32info::multiplyModPoly(unsigned x, unsigned y) const
{
    const unsigned bit0 = 0x80000000;       // NB: reflected out so bits reversed
    const unsigned bit31 = 1;

    unsigned answer = 0;
    for (;x; x <<= 1)
    {
        if (x & bit0)
            answer ^= y;
        
        if (y & bit31)                          // would overflow => subtract(^) poly...
            y = ((y >> 1) ^ poly);
        else
            y >>=1;
    }
    return answer;
}

unsigned getFirstPower(unsigned __int64 value)
{
    unsigned power = 0;
    unsigned temp = (unsigned)value;
    if (temp != value)
    {
        power += 32;
        temp = (unsigned)(value >> 32);
    }
    if (temp >= 0x10000)
    {
        power += 16;
        value >>= 16;
    }
    if (temp >= 0x100)
    {
        power += 8;
        value >>= 8;
    }
    if (temp >= 0x10)
    {
        power += 4;
        value >>= 4;
    }
    if (temp >= 0x4)
    {
        power += 2;
        value >>= 2;
    }
    if (temp >= 0x2)
    {
        power += 1;
        value >>= 1;
    }
    return power;
}

unsigned CRC32info::skip(unsigned icrc, offset_t _bytes) const
{
    unsigned __int64 bytes = _bytes;
    if (bytes == 0) return icrc;

    //Quickly find the first power we need to test...
    unsigned power = getFirstPower(bytes)+1;
    assertex(bytes < mask[power]);

    do
    {
        power--;
        if (bytes >= mask[power])
        {
            icrc = multiplyModPoly(icrc, scale[power]);
            bytes -= mask[power];
        }
    } while (power && bytes);

    return icrc;
}

unsigned CRC32info::tally(size32_t len, const void * data, unsigned crc)
{
    return crc32((const char *)data, len, crc);
}

static CRC32info crc32info(0xedb88320);

//---------------------------------------------------------------------------

void CRC32::skip(offset_t length)
{
    crc = crc32info.skip(crc, length);
}

void CRC32::tally(unsigned len, const void * buf)
{
    crc = crc32((const char *)buf, len, crc);
}

//---------------------------------------------------------------------------

CRC32Merger::CRC32Merger() 
{ 
    crc = ~0;
}

void CRC32Merger::addChildCRC(offset_t nextLength, unsigned nextCRC, bool fullCRC)
{
    if (fullCRC)
        nextCRC = ~nextCRC ^ crc32info.skip(0xffffffff, nextLength);

    crc = crc32info.skip(crc, nextLength) ^ nextCRC;
}

void CRC32Merger::clear()
{
    crc = ~0;
}

//---------------------------------------------------------------------------

// A faster alternative to a CRC - word at a time, and no table lookup.
unsigned cxc32(unsigned * buf, unsigned numWords, unsigned cxc)
{
    unsigned * start = buf;
    unsigned * end = start+numWords;
    while (start != end)
    {
// the code for _rotl is worse than appalling in debug
#if (defined(_DEBUG) || (!defined(_WIN32)))
        cxc = ((cxc >> 19) | (cxc << 13)) ^ *start;
#else
        cxc = _rotl(cxc, 13) ^ *start;
#endif
        start++;
    }
    return cxc;
}


//---------------------------------------------------------------------------



unsigned getFileCRC(const char * name)
{ 
    Owned<IFile> file = createIFile(name);
    return file->getCRC();
}



unsigned crc_file(const char * name)
{ char buffer[1024];
  unsigned crc=0;
  FILE * f=fopen(name,"rb");
  if (f)
  { while(1)
    { unsigned int si=(size32_t)fread(buffer,1,sizeof(buffer),f);
      if (si)
      { crc=crc32(buffer,si,crc);
      } else
      { fclose(f);
        return crc;
      }
    }
  }
  return 0;
}


class CCrcPipeStream : public CInterface, implements ICrcIOStream
{
    CRC32 crc;
    Linked<IIOStream> io;
public:
    IMPLEMENT_IINTERFACE;

    CCrcPipeStream(IIOStream *_io) : io(_io) { }

    virtual unsigned queryCrc() { return crc.get(); }

    virtual void flush() { io->flush(); }
    virtual size32_t read(size32_t len, void * data) { size32_t l = io->read(len, data); crc.tally(l, data); return l; }
    virtual size32_t write(size32_t len, const void * data) { size32_t l = io->write(len, data); crc.tally(l, data); return l; }
};

ICrcIOStream *createCrcPipeStream(IIOStream *stream)
{
    return new CCrcPipeStream(stream);
}



#if 0

//Test conditions and examples of how the classes might be used...
#include <stdio.h>

void t3()
{
    CRC32 crca;
    crca.tally(9,"123456789");
    unsigned x = crca.get();

    CRC32 crcb(~0);
    CRC32 crcc(0);
    crcb.skip(9);
    crcc.tally(9,"123456789");
    unsigned y = ~(~crcb.get() ^ ~crcc.get());

    printf("t3 check %x cf %x\n", x, y);
}

void t3b()
{
    CRC32 crc1(0);
    crc1.tally(9, "123456789");

    CRC32Merger merged;
    merged.addChildCRC(9, ~crc1.get(), false);
    printf("t3b check %x\n", merged.get());
}

void t4()
{
    CRC32 crc1(~0);
    CRC32 crc2(~0);
    CRC32 crc3(~0);
    crc1.skip(5);
    crc1.tally(4, "6789");
    crc2.tally(5, "12345");
    crc2.skip(4);
    crc3.skip(9);
    unsigned val = (crc1.get()^crc2.get()^crc3.get());
    printf("t4 check %x\n", val);
}

void t5()
{
    CRC32 crc1(~0);
    CRC32 crc2(~0);
    crc1.tally(5, "12345");
    crc2.tally(4, "6789");

    CRC32Merger merged;
    merged.addChildCRC(5, crc1.get(), true);
    merged.addChildCRC(4, crc2.get(), true);
    printf("t5 check %x\n", merged.get());
}

void t6()
{
    CRC32 crc1(0);
    CRC32 crc2(0);
    crc1.tally(5, "12345");
    crc2.tally(4, "6789");

    CRC32Merger merged;
    merged.addChildCRC(5, ~crc1.get(), false);
    merged.addChildCRC(4, ~crc2.get(), false);
    printf("t6 check %x\n", merged.get());
}

#endif
