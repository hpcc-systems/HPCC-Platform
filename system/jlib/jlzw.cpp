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


// JLIB LZW compression class 
#include "platform.h"
#include "jmisc.hpp"
#include "jlib.hpp"
#include <time.h>
#include "jfile.hpp"
#include "jencrypt.hpp"
#include "jflz.hpp"

#ifdef _WIN32
#include <io.h>
#endif

#include "jlzw.ipp"

#define COMMITTED ((size32_t)-1)

#define BITS_LO                    9
#define BITS_HI                    15
#define MAX_CODE                   ((1<<BITS_HI)-1)
#define BUMP_CODE                  257
#define FIRST_CODE                 258
#define SAFETY_MARGIN              16 // for 15 bits

#define BITS_PER_UNIT   8
#define BITS_ALWAYS     8
#define ALWAYS_MASK     ((1<<BITS_ALWAYS)-1)

typedef unsigned long   bucket_t;
// typedef long long       lbucket_t;
typedef __int64 lbucket_t;


//#define STATS
//#define TEST
#ifdef _DEBUG
#define ASSERT(a) assertex(a)
#else
#define ASSERT(a) 
#endif

LZWDictionary::LZWDictionary()
{
    curbits = 0;
}

void LZWDictionary::initdict()
{
    nextcode = FIRST_CODE;
    curbits = BITS_LO;
    nextbump = 1<<BITS_LO;
}


bool LZWDictionary::bumpbits()
{
    if (curbits==BITS_HI)
        return false;
    curbits++;
    nextbump = 1<<curbits;
    return true;
}

#ifdef STATS
static unsigned st_tottimems=0;
static unsigned st_maxtime=0;
static int st_maxtime_writes=0;
static int st_totwrites=0;
static int st_totblocks=0;
static int st_totwritten=0; // in K
static int st_totread=0;    // in K
static unsigned st_thistime=0;
static int st_thiswrites=0;
static unsigned st_totbitsize=0;
static unsigned st_totbitsizeuc=0;
#endif


CLZWCompressor::CLZWCompressor(bool _supportbigendian)
{
    outbuf = NULL;
    outlen = 0;
    maxlen = 0;
    bufalloc = 0;
    inuseflag=0xff;
    supportbigendian = _supportbigendian;
    outBufStart = 0;
    outBufMb = NULL;
}

CLZWCompressor::~CLZWCompressor()
{
    if (bufalloc)
        free(outbuf);
#ifdef STATS
    printf("HLZW STATS:\n");
    printf(" st_tottimems = %d\n",st_tottimems);
    printf(" st_maxtime = %d\n",st_maxtime);
    printf(" st_maxtime_writes = %d\n",st_maxtime_writes);
    printf(" st_totwrites = %d\n",st_totwrites);
    printf(" st_totblocks = %d\n",st_totblocks);
    printf(" st_totwritten = %dK\n",st_totwritten); // in K
    printf(" st_totread = %dK\n",st_totread);       // in K
    printf(" st_totbitsize = %d\n",st_totbitsize);
    printf(" st_totbitsizeuc = %d\n",st_totbitsizeuc);
#endif
}

void CLZWCompressor::initdict()
{
    dict.initdict();
    // use inuseflag rather than clearing as this can take a large proportion of the time
    // (e.g. in hozed)
    if (inuseflag==0xff) {
        memset(dictinuse,0,sizeof(dictinuse));
        inuseflag=0;
    }
    inuseflag++;
}


struct ShiftInfo {
    int mask1;
    int shift2;           // NB right shift, not left
    int mask2;
    int padding;          // make it multiple of 4
};


ShiftInfo ShiftArray[BITS_HI-BITS_ALWAYS+1][BITS_PER_UNIT];

static struct __initShiftArray {
    __initShiftArray()
    {
        for (unsigned numBits = BITS_LO; numBits <= BITS_HI; ++numBits) {
            unsigned copyBits = numBits-BITS_ALWAYS;
            unsigned mask = (1<<numBits)-1-ALWAYS_MASK;
            for (unsigned shift = 0; shift < BITS_PER_UNIT; shift++) {
                ShiftInfo & cur = ShiftArray[copyBits][shift];

                if (shift + copyBits <= BITS_PER_UNIT) {
                    cur.mask1 = mask;
                    cur.shift2 = 0;
                    cur.mask2 = 0;
                }
                else {
                    cur.shift2 = BITS_PER_UNIT + BITS_ALWAYS - shift;
                    cur.mask1 = (1<<cur.shift2)-1-ALWAYS_MASK;
                    cur.mask2 = mask - cur.mask1;
                }
            }
        }
    }
} __do_initShiftArray;

#define PUTCODE(code)                                       \
{                                                            \
  unsigned inbits=code;                                      \
  int shift=curShift;                                        \
  int copyBits = dict.curbits - BITS_PER_UNIT;               \
                                                             \
  *(outbytes++) = (unsigned char)(inbits&0xff);              \
  ShiftInfo & cur = ShiftArray[copyBits][shift];             \
  outbitbuf |= (inbits & cur.mask1) >> (BITS_ALWAYS-shift);  \
  shift += copyBits;                                         \
  if (shift >= BITS_PER_UNIT)                                \
  {                                                          \
    shift -= BITS_PER_UNIT;                                  \
    *(outbits++) = outbitbuf;                                \
    if (outbits==outnext) {                                  \
      outbytes = outnext;                                    \
      outbits = outbytes+BITS_ALWAYS;                        \
      outnext += dict.curbits;                               \
      outlen += dict.curbits;                                \
      ASSERT(shift==0);                                      \
    }                                                        \
    outbitbuf = 0;                                           \
    if (shift != 0)                                          \
       outbitbuf = (inbits & cur.mask2) >> cur.shift2;       \
  }                                                          \
  curShift = shift;                                          \
}


#define GETCODE(ret)                                        \
{                                                            \
  int shift=curShift;                                        \
  int copyBits = dict.curbits - BITS_PER_UNIT;               \
                                                             \
  ret = *(inbytes++);                                        \
  ShiftInfo & cur = ShiftArray[copyBits][shift];             \
  ret |= (*inbits << (BITS_ALWAYS-shift)) & cur.mask1;       \
  shift += copyBits;                                         \
  if (shift >= BITS_PER_UNIT)                                \
  {                                                          \
    shift -= BITS_PER_UNIT;                                  \
    inbits++;                                                \
    if (inbits==innext) {                                    \
      inbytes = innext;                                      \
      inbits = inbytes+BITS_ALWAYS;                          \
      innext += dict.curbits;                                \
      ASSERT(shift==0);                                      \
    }                                                        \
    if (shift != 0)                                          \
       ret |= (*inbits << cur.shift2) & cur.mask2;           \
  }                                                          \
  curShift = shift;                                          \
}

void CLZWCompressor::initCommon()
{
    ASSERT(dict.curbits==0);   // check for open called twice with no close
    initdict();
    curcode = -1;
    inlen = 0;
    inlenblk = COMMITTED;
    memset(outbuf,0,sizeof(size32_t));
    outlen = sizeof(size32_t)+dict.curbits;
    outbytes = (unsigned char *)outbuf+sizeof(size32_t);
    outbits = outbytes+8;
    outnext = outbytes+dict.curbits;
    curShift=0; //outmask = 0x80;
    outbitbuf = 0;
}

void CLZWCompressor::flushbuf()
{
    if (outbytes==outnext)
        return;
    *(outbits++) = outbitbuf;
    while (outbits!=outnext) {
        *(outbits++) = 0;
    }
    do {
        *(outbytes++) = 0;
    } while (outbytes+(dict.curbits-8)!=outnext);
}

void CLZWCompressor::ensure(size32_t sz)
{
    dbgassertex(outBufMb);
    size32_t outBytesOffset = outbytes-(byte *)outbuf;
    size32_t outBitsOffset = outbits-(byte *)outbuf;
    size32_t outNextOffset = outnext-(byte *)outbuf;
    outbuf = (byte *)outBufMb->ensureCapacity(sz);
    maxlen = outBufMb->capacity()-SAFETY_MARGIN;
    outbytes = (byte *)outbuf+outBytesOffset;
    outbits = (byte *)outbuf+outBitsOffset;
    outnext = (byte *)outbuf+outNextOffset;
}

void CLZWCompressor::open(MemoryBuffer &mb, size32_t initialSize)
{
    if (bufalloc)
        free(outbuf);
    bufalloc = 0;
    outBufMb = &mb;
    outBufStart = mb.length();
    outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
    maxlen = outBufMb->capacity()-SAFETY_MARGIN;
    initCommon();
}

void CLZWCompressor::open(void *buf,size32_t max)
{
#ifdef STATS
    st_thistime = msTick();
    st_thiswrites=0;
#endif

    if (buf)
    {
        if (bufalloc)
            free(outbuf);
        bufalloc = 0;
        outbuf = buf;
    }
    else if (max>bufalloc)
    {
        if (bufalloc)
            free(outbuf);
        bufalloc = max;
        outbuf = malloc(bufalloc);
    }
    outBufMb = NULL;
    ASSERT(max>SAFETY_MARGIN+sizeof(size32_t)); // minimum required
    maxlen=max-SAFETY_MARGIN;
    initCommon();
}



#define HASHC(code,ch) (((0x01000193*(unsigned)code)^(unsigned char)ch)%LZW_HASH_TABLE_SIZE)

#define BE_MEMCPY4(dst,src)     { if (supportbigendian) _WINCPYREV4(dst,src); else memcpy(dst,src,4); }


size32_t CLZWCompressor::write(const void *buf,size32_t buflen)
{
    if (!buflen)
        return 0;
    if (!dict.curbits)
        return 0;
    unsigned char *in=(unsigned char *)buf;
#ifdef STATS
    st_thiswrites++;
#endif

    size32_t len=buflen;
    if (curcode==-1)
    {
        curcode = *(in++);
        len--;
    }
    while (len--)
    {
        int ch = *(in++);
        int index = HASHC(curcode,ch);
        for (;;)
        {
            if (dictinuse[index]!=inuseflag)
            {
                dictinuse[index] = inuseflag;
                dictcode[index] = dict.nextcode++;
                dict.dictparent[index] = curcode;
                dict.dictchar[index] = (unsigned char) ch;
                PUTCODE(curcode);
                if ((outlen>=maxlen))
                {
                    if (outBufMb)
                        ensure(outlen+0x10000);
                    else
                    {
                        size32_t ret;
                        if (inlenblk==COMMITTED)
                        {
                            ret = in-(unsigned char *)buf-1;
                            inlen += in-(unsigned char *)buf-1;
                        }
                        else
                            ret = 0;
                        close();
                        return ret;
                    }
                }
                if (dict.nextcode == dict.nextbump)
                {
                    PUTCODE(BUMP_CODE);
                    flushbuf();
                    bool eodict = !dict.bumpbits();
                    if (eodict)
                        initdict();
                    outbytes = outnext;
                    outbits = outbytes+8;
                    outnext += dict.curbits;
                    outlen += dict.curbits;
                    curShift=0;//outmask = 0x80;
                    outbitbuf = 0;
                }
                curcode = ch;
                break;
            }
            if (dict.dictparent[index] == curcode &&
                dict.dictchar[index] == (unsigned char)ch)
            {
                curcode = dictcode[index];
                break;
            }
            index--;
            if (index<0)
                index = LZW_HASH_TABLE_SIZE-1;
        }
    }
    inlen += buflen;
    return buflen;
}

void CLZWCompressor::startblock()
{
    inlenblk = inlen;
}

void CLZWCompressor::commitblock()
{
    inlenblk = COMMITTED;
}

void CLZWCompressor::close()
{
    if (dict.curbits)
    {
        PUTCODE(curcode);
        flushbuf();
        dict.curbits = 0;
        if (inlenblk!=COMMITTED)
            inlen = inlenblk; // transaction failed
        inlenblk = COMMITTED;
        BE_MEMCPY4(outbuf,&inlen);
#ifdef STATS
        unsigned t = (msTick()-st_thistime);
        if (t>st_maxtime) {
            st_maxtime = t;
            st_maxtime_writes = st_thiswrites;
        }
        st_tottimems += t;
        st_totwrites += st_thiswrites;
        st_totwritten += (outlen+511)/1024;
        st_totread += (inlen+511)/1024;
        st_totblocks++;
#endif
        if (outBufMb)
        {
            outBufMb->setWritePos(outBufStart+outlen);
            outBufMb = NULL;
        }
    }
}

CLZWExpander::CLZWExpander(bool _supportbigendian)
{
    outbuf = NULL;
    outlen = 0;
    outmax = 0;
    bufalloc = 0;
    supportbigendian = _supportbigendian;
}

CLZWExpander::~CLZWExpander()
{
    if (bufalloc)
        free(outbuf);
}

size32_t CLZWExpander::init(const void *blk)
{
    dict.initdict();
    BE_MEMCPY4(&outlen,blk);
    inbytes=(unsigned char *)blk+sizeof(size32_t);
    inbits=inbytes+8;
    innext=inbytes+dict.curbits;
    curShift=0;
    return outlen;
}

void CLZWExpander::expand(void *buf)
{
    if (!outlen)
        return;
    if (buf) {
        if (bufalloc)
            free(outbuf);
        bufalloc = 0;
        outbuf = (unsigned char *)buf;
    }
    else if (outlen>bufalloc) {
        if (bufalloc)
            free(outbuf);
        bufalloc = outlen;
        outbuf = (unsigned char *)malloc(bufalloc);
        if (!outbuf)
            throw MakeStringException(MSGAUD_operator,0, "Out of memory in LZWExpander::expand, requesting %d bytes", bufalloc);
    }
    unsigned char *out=outbuf;
    unsigned char *outend = out+outlen;
    int oldcode ;
    GETCODE(oldcode);
    int ch=oldcode;
    *(out++)=(unsigned char)ch;
    while (out!=outend) {
        int newcode;
        GETCODE(newcode);
        unsigned char *sp = stack;
        if (newcode >= dict.nextcode) {
            *(sp++) = (unsigned char) ch;
            ch = oldcode;
        }
        else if (newcode == BUMP_CODE) {
            bool eodict = !dict.bumpbits();
            if (eodict) 
                dict.initdict();
            inbytes = innext;
            inbits = inbytes+8;
            innext += dict.curbits;
            curShift=0;
            if (eodict) {
                GETCODE(oldcode);
                ch=oldcode;
                *(out++)=(unsigned char)ch;
            }
            continue;
        }
        else 
            ch = newcode;
        while (ch > 255) {
            *(sp++) = dict.dictchar[ch];
            ch = dict.dictparent[ch];
        }
#ifdef _DEBUG
        assertex(dict.nextcode <= MAX_CODE);
#endif
        dict.dictparent[dict.nextcode] = oldcode;
        dict.dictchar[dict.nextcode++] = (unsigned char) ch;
        oldcode = newcode;
        *(out++) = ch;
        while ((sp!=stack)&&(out!=outend)) {
            *(out++)=(unsigned char)*(--sp);
        }
    }
}


// encoding  
//    0              =   0
//    10             =   1
//    1100           =   2
//    1101           =   3
//    1110bb         =   4-7
//    11110bbbb      =   8-23
//    111110bbbbbbbb =   24-279



#define OUTBIT(b) { if (b) bb|=bm; if (bm==0x80) { outp[l++] = bb; bb=0; bm=1; } else bm<<=1; }

size32_t bitcompress(unsigned *p,int n,void *outb)
{
    int l=0;
    unsigned char *outp=(unsigned char *)outb;
    outp[1] = 0;
    unsigned char bm=1;
    unsigned char bb=0;
    while (n--) {
        unsigned d=*p;

        if (d==0) {  // special 0
            OUTBIT(0);
        }
        else if (--d==0) { // special 1
            OUTBIT(1);
            OUTBIT(0);
        }
        else {
            d--;
            unsigned m;
            unsigned nb=0;
            while (1) {
                if (nb==5) {
                    m = 0x80000000;
                    nb++;
                    break;
                }
                unsigned ntb = 1<<nb;
                m = 1<<ntb;
                nb++;
                if (d<m) {
                    m>>=1;
                    break;
                }
                d-=m;
            }
            OUTBIT(1);
            while (nb--)
                OUTBIT(1);
            OUTBIT(0);
            while (m) {
                OUTBIT(m&d);
                m>>=1;
            }
        }
        p++;
    }

    if (bm!=1) {
        outp[l++] = bb; // flush remaining bits
    }

    return l;
}



#define MAX_BUCKETS 1024

ICompressor *createLZWCompressor(bool _supportbigendian)
{
    return new CLZWCompressor(_supportbigendian);
}

IExpander *createLZWExpander(bool _supportbigendian)
{
    return new CLZWExpander(_supportbigendian);
}



//===========================================================================

/*
RLE
   uses <d1-de>  1-15 repeats of prev char
        <d0> <rept-15>  15-222 repeats of prev char
        <d0> as escape (followed by d0-df)
        <d0> <ff> (at start) - plain row following
        prev char is initialy assumed 0
*/

size32_t RLECompress(void *dst,const void *src,size32_t size) // maximum will write is 2+size
{
    
    if (size==0)
        return 0;
    byte *out=(byte *)dst;
    byte *outmax = out+size;
    const byte *in=(const byte *)src;
    const byte *inmax = in+size;
    byte pc = 0;
    loop {
        byte c = *(in++);
        if (c==pc) {
            byte cnt = 0;
            do {
                cnt++;
                if (in==inmax) {
                    if (cnt<=15)
                        *(out++) = 0xd0+cnt;
                    else {
                        *(out++) = 0xd0;
                        if (out==outmax) 
                            goto Fail;
                        *(out++) = cnt-15;
                    }   
                    return (size32_t)(out-(byte *)dst);
                }
                c = *(in++);
            } while ((c==pc)&&(cnt!=222));
            if (cnt<=15)
                *(out++) = 0xd0+cnt;
            else {
                *(out++) = 0xd0;
                if (out==outmax) 
                    break;  // fail
                *(out++) = cnt-15;
            }   
            if (out==outmax) 
                break;
        }
        if ((c<0xd0)||(c>=0xe0))
            *(out++) = c;
        else {
            *(out++) = 0xd0;
            if (out==outmax)
                break; // fail
            *(out++) = c;
        }
        if (in==inmax)
            return (size32_t)(out-(byte *)dst);
        if (out==outmax)
            break;      // will need at least one more char
        pc = c;
    }
Fail:
    out=(byte *)dst;
    *(out++) = 0xd0;
    *(out++) = 0xff;
    memcpy(out,src,size);
    return size+2;
}


size32_t RLEExpand(void *dst,const void *src,size32_t expsize)
{
    if (expsize==0)
        return 0;
    byte *out=(byte *)dst;
    byte *outmax = out+expsize;
    const byte *in=(const byte *)src;
    byte c = *(in++);
    if ((c==0xd0)&&(*in==0xff)) {
        memcpy(dst,in+1,expsize);
        return expsize+2;
    }
    byte pc = 0;
    loop {
        if ((c<0xd0)||(c>=0xe0)) 
            *(out++) = c;
        else {
            c -= 0xd0;
            if (c==0) {
                c = *(in++);
                if (c>=0xd0) {
                    *(out++) = c;
                    if (c>=0xe0)
                        throw MakeStringException(-1,"Corrupt RLE format");
                    goto Escape;
                }
                c+=15;
            }
            size32_t left = (size32_t)(outmax-out);
            size32_t cnt = c;
            c = pc;
            if (left<cnt)
                cnt = left;
            while (cnt--)
                *(out++) = c;
        }
Escape:
        if (out==outmax)
            break;
        pc = c;
        c = *(in++);
    }
    return (size32_t)(in-(const byte *)src);
}

void appendToBuffer(MemoryBuffer & out, size32_t len, const void * src)
{
    out.append(false);
    out.append(len);
    out.append(len, src);
}

void compressToBuffer(MemoryBuffer & out, size32_t len, const void * src)
{
    unsigned originalLength = out.length();
    out.append(true);
    out.append((size32_t)0);

    if (len >= 32)
    {
        size32_t newSize = len * 4 / 5; // Copy if compresses less than 80% ...
        Owned<ICompressor> compressor = createLZWCompressor();
        void *newData = out.reserve(newSize);
        compressor->open(newData, newSize);
        if (compressor->write(src, len)==len)
        {
            compressor->close();
            size32_t compressedLen = compressor->buflen();
            out.setWritePos(originalLength + sizeof(bool));
            out.append(compressedLen);
            out.setWritePos(originalLength + sizeof(bool) + sizeof(size32_t) + compressedLen);
            return;
        }
    }
    
    // all or don't compress
    out.setWritePos(originalLength);
    appendToBuffer(out, len, src);
}

void decompressToBuffer(MemoryBuffer & out, const void * src)
{
    Owned<IExpander> expander = createLZWExpander();
    unsigned outSize = expander->init(src);
    void * buff = out.reserve(outSize);
    expander->expand(buff);
}


void decompressToBuffer(MemoryBuffer & out, MemoryBuffer & in)
{
    bool compressed;
    size32_t srcLen;
    in.read(compressed).read(srcLen);
    if (compressed)
        decompressToBuffer(out, in.readDirect(srcLen));
    else
        out.append(srcLen, in.readDirect(srcLen));
}

void decompressToAttr(MemoryAttr & out, const void * src)
{
    Owned<IExpander> expander = createLZWExpander();
    unsigned outSize = expander->init(src);
    void * buff = out.allocate(outSize);
    expander->expand(buff);
}

void decompressToBuffer(MemoryAttr & out, MemoryBuffer & in)
{
    bool compressed;
    size32_t srcLen;
    in.read(compressed).read(srcLen);
    if (compressed)
        decompressToAttr(out, in.readDirect(srcLen));
    else
        out.set(srcLen, in.readDirect(srcLen));
}



/*
   Simple Diff compression format is

  <compressed-block> ::= <initial-row> { <compressed-row> }
  <compressed-row>   ::= { <same-count> <diff-count> <diff-bytes> }
  <same-count>       ::= { 255 } <byte>                -- value is sum
  <diff-count>       ::= <byte>
  <diff-bytes>       ::= { <bytes> }

  // note if diff-count is > 255 it will be broken into 255 diff followed by 0 same
  // also need at least 2 bytes same before stops difference block

  thus                 AAAAAA...AAAAAA  [ len 500 ]
  followed by          ADADAD...ADADAD  
  will be saved as     1,255,ADADA..ADADA,0,244,ADADA..ADADA -> 503 bytes 
   
  and                  AAAAAA...AAAAAA  [ len 500 ]
  followed by          AADDAA...AADDAA  
  will be saved as     2,2,DD,2,2,DD...2,2,DD,2              -> 499 bytes

  and                  AAAAAA...AAAAAA  [ len 500 ]
  followed by          AAAAAA...AAAAAA  
  will be saved as     255,245                               -> 2 bytes 

  and                  AAAAAA...AAAAAA  [ len 500 ]
  followed by          ZZZZZZ...ZZZZZZ  
  will be saves as     0,255,ZZ..ZZ,0,245,ZZ..ZZ             -> 504 bytes
    
  // maximum size is of a row is bounded by: rowsize+((rowsize+254)/255)*2;

*/



size32_t DiffCompress(const void *src,void *dst,void *buff,size32_t rs)
{
    const unsigned char *s=(const unsigned char *)src;
    unsigned char *d=(unsigned char *)dst;
    unsigned char *b=(unsigned char *)buff;
    ASSERT(rs);
    size32_t cnt;
    cnt = 0;
    while (*s==*b) {
Loop:
        cnt++;
        rs--;
        if (rs==0) break;
        s++;
        b++;
    }
    while (cnt>=255) {
        *d = 255;
        d++;
        cnt-=255;
    }
    *d = (unsigned char)cnt;
    d++;
    if (rs!=0) {
        unsigned char *dcnt=d;
        d++;
        cnt = 0;
        while(1) {
            cnt++;
            *d = *s;
            d++;
            *b = *s;
            rs--;
            if (rs==0) {
                *dcnt=(unsigned char)cnt;
                break;
            }
            s++;
            b++;
            if (*s==*b) {
                if ((rs>1)&&(s[1]==b[1])) {     // slower but slightly better compression 
                    *dcnt=(unsigned char)cnt;
                    cnt = 0;
                    goto Loop;
                }
            }
            if (cnt==255) {
                *dcnt=(unsigned char)cnt;
                *d = 0;
                d++;
                dcnt = d++;
                cnt = 0;
            }
        }
    }
    return (size32_t)(d-(unsigned char *)dst);
}

size32_t DiffCompress2(const void *src,void *dst,const void *prev,size32_t rs)
{   
    // doesn't update prev
    const unsigned char *s=(const unsigned char *)src;
    unsigned char *d=(unsigned char *)dst;
    const unsigned char *b=(unsigned char *)prev;
    ASSERT(rs);
    size32_t cnt;
    cnt = 0;
    while (*s==*b) {
Loop:
        cnt++;
        rs--;
        if (rs==0) break;
        s++;
        b++;
    }
    while (cnt>=255) {
        *d = 255;
        d++;
        cnt-=255;
    }
    *d = (unsigned char)cnt;
    d++;
    if (rs!=0) {
        unsigned char *dcnt=d;
        d++;
        cnt = 0;
        while(1) {
            cnt++;
            *d = *s;
            d++;
            rs--;
            if (rs==0) {
                *dcnt=(unsigned char)cnt;
                break;
            }
            s++;
            b++;
            if (*s==*b) {
                if ((rs>1)&&(s[1]==b[1])) {     // slower but slightly better compression 
                    *dcnt=(unsigned char)cnt;
                    cnt = 0;
                    goto Loop;
                }
            }
            if (cnt==255) {
                *dcnt=(unsigned char)cnt;
                *d = 0;
                d++;
                dcnt = d++;
                cnt = 0;
            }
        }
    }
    return (size32_t)(d-(unsigned char *)dst);
}



size32_t DiffCompressFirst(const void *src,void *dst,void *buf,size32_t rs)
{
    memcpy(buf,src,rs);
    const unsigned char *s=(const unsigned char *)src;
    unsigned char *d=(unsigned char *)dst;
    *d = 0;
    d++;
    while (rs) {
        unsigned cnt=(rs<=255)?rs:255;
        *d=(unsigned char)cnt;
        d++;
        memcpy(d,s,cnt);
        d += cnt;
        s += cnt;
        *d = 0;
        d++;
        rs -= cnt;
    }
    return (size32_t)(d-(unsigned char *)dst);
}

size32_t DiffCompressedSize(const void *src,size32_t rs)
{
    const unsigned char *s=(const unsigned char *)src;
    unsigned n;
    while (rs) {
        // first comes compressed
        do {
            n = *s;
            s++;
            rs -= n;
        } while (n==255);
        if (rs==0)
            break;
        n = *s;
        s++;
        rs -= n;
        s += n;
    }
    return (size32_t)(s-(const unsigned char *)src);
}



size32_t DiffExpand(const void *src,void *dst,const void *prev,size32_t rs)
{
    unsigned char *s=(unsigned char *)src;
    unsigned char *d=(unsigned char *)dst;
    const unsigned char *b=(const unsigned char *)prev;
    ASSERT(rs);
    while (rs) {
        size32_t cnt = 0;
        size32_t c;
        do {
            c=(size32_t)*s;
            s++;
            cnt += c;
        } while (c==255);
        rs -= cnt;
        while (cnt!=0) {
            *d = *b;
            d++;
            b++;
            cnt--;
        }
        if ((int)rs<=0) {
            if (rs == 0)
                break;
            throw MakeStringException(-1,"Corrupt compressed data(1)");
        }
        cnt=(size32_t)*s;
        s++;
        rs -= cnt;
        b += cnt;
        const unsigned char *e = s+cnt;
        while (s!=e) {
            *d = *s;
            s++;
            d++;
        }
    }
    return (size32_t)(s-(unsigned char *)src);
}

// helper class

class CDiffExpand
{
    byte *s;
    const byte *b;
    size32_t rs;
    enum {
        S_pre_repeat,
        S_repeat,
        S_diff
    } state;
    size32_t cnt;

public:
    inline void init(const void *src,const void *prev,size32_t _rs)
    {
        s=(byte *)src;
        b=(const byte *)prev;
        state = S_pre_repeat;
        rs = _rs;
        cnt = 0;
    }

    inline void skip(size32_t sz)
    {
        if (!sz)
            return;
        while (sz) {
            switch (state) {
            case S_pre_repeat:
                if (!rs)
                    return;
                cnt = 0;
                size32_t c;
                do {
                    c=(size32_t)*s;
                    s++;
                    cnt += c;
                } while (c==255);
                rs -= cnt;
                state = S_repeat;
                // fall through
            case S_repeat:
                if (cnt) {
                    if (sz<=cnt) {
                        cnt -= sz;
                        b += sz;
                        return;
                    }
                    b += cnt;
                    sz-=cnt;
                }
                if ((int)rs<=0) {
                    if (rs == 0)
                        return;
                    throw MakeStringException(-1,"Corrupt compressed data(2)");
                }
                cnt=(size32_t)*s;
                s++;
                rs -= cnt;
                b += cnt;
                state = S_diff;
                // fall through
            case S_diff:
                if (cnt) {
                    if (sz<=cnt) {
                        cnt -= sz;
                        s += sz;
                        return;
                    }
                    s += cnt;
                    sz -= cnt;
                }
                state = S_pre_repeat;
            }
        }
    }

    inline size32_t cpy(void *dst,size32_t sz)
    {
        if (!sz)
            return 0;
        byte *d=(byte *)dst;
        loop {
            switch (state) {
            case S_pre_repeat:
                if (!rs) 
                    return (size32_t)(d-(byte *)dst);
                cnt = 0;
                size32_t c;
                do {
                    c=(size32_t)*s;
                    s++;
                    cnt += c;
                } while (c==255);
                rs -= cnt;
                state = S_repeat;
                // fall through
            case S_repeat:
                if (cnt) {
                    if (cnt>=sz) {
                        memcpy(d,b,sz);
                        b += sz;
                        cnt -= sz;
                        d += sz;
                        return (size32_t)(d-(byte *)dst);
                    }
                    memcpy(d,b,cnt);
                    b += cnt;
                    d += cnt;
                    sz -= cnt;
                }
                if ((int)rs<=0) {
                    if (rs == 0)
                        return (size32_t)(d-(byte *)dst);
                    throw MakeStringException(-1,"Corrupt compressed data(3)");
                }
                cnt=(size32_t)*s;
                s++;
                rs -= cnt;
                b += cnt;
                state = S_diff;
                // fall through
            case S_diff:
                if (cnt) {
                    if (cnt>=sz) {
                        memcpy(d,s,sz);
                        s += sz;
                        cnt -= sz;
                        d += sz;
                        return (size32_t)(d-(byte *)dst);
                    }
                    memcpy(d,s,cnt);
                    s += cnt;
                    d += cnt;
                    sz -= cnt;
                }
                state = S_pre_repeat;
            }
        }
        return 0; // never gets here
    }

    inline int cmp(const void *dst,size32_t sz)
    {
        int ret;
        if (!sz)
            return rs?-1:0;
        const byte *d=(const byte *)dst;
        loop {
            switch (state) {
            case S_pre_repeat:
                if (!rs) 
                    return sz?1:0;
                cnt = 0;
                size32_t c;
                do {
                    c=(size32_t)*s;
                    s++;
                    cnt += c;
                } while (c==255);
                rs -= cnt;
                state = S_repeat;
                // fall through
            case S_repeat:
                if (cnt) {
                    if (cnt>=sz) {
                        ret = memcmp(d,b,sz);
                        b += sz;
                        cnt -= sz;
                        return ret;
                    }
                    ret = memcmp(d,b,cnt);
                    b += cnt;
                    if (ret)
                        return ret;
                    d += cnt;
                    sz -= cnt;
                }
                if ((int)rs<=0) {
                    if (rs == 0)
                        return sz?1:0;
                    throw MakeStringException(-1,"Corrupt compressed data(4)");
                }
                cnt=(size32_t)*s;
                s++;
                rs -= cnt;
                b += cnt;
                state = S_diff;
                // fall through
            case S_diff:
                if (cnt) {
                    if (cnt>=sz) {
                        ret = memcmp(d,s,sz);
                        s += sz;
                        cnt -= sz;
                        return ret;
                    }
                    ret = memcmp(d,s,cnt);
                    s += cnt;
                    if (ret)
                        return ret;
                    d += cnt;
                    sz -= cnt;
                }
                state = S_pre_repeat;
            }
        }
        return 0; // never gets here
    }



};



// =============================================================================

// RDIFF
// format ::=  <expsize> <recsize> <plainrec> { <rowdif> }

class jlib_decl CRDiffCompressor : public CInterface, public ICompressor
{
    size32_t inlen;
    size32_t outlen;
    size32_t bufalloc;
    size32_t remaining;
    void *outbuf;
    unsigned char *out;
    MemoryBuffer *outBufMb;
    size32_t outBufStart;

    size32_t recsize;       // assumed fixed length rows
    // assumes a transaction is a record
    MemoryBuffer transbuf;
    size32_t maxrecsize;  // maximum size diff compress 
    unsigned char *prev;

    void initCommon()
    {
        inlen = 0;
        memset(outbuf, 0, sizeof(size32_t)*2);
        outlen = sizeof(size32_t)*2;
        out = (byte *)outbuf+outlen;
        free(prev);
        prev = NULL;
    }
    inline void ensure(size32_t sz)
    {
        if (NULL == outBufMb)
            throw MakeStringException(-3,"CRDiffCompressor row doesn't fit in buffer!");
        dbgassertex(remaining<sz);
        verifyex(outBufMb->ensureCapacity(outBufMb->capacity()+(sz-remaining)));
        outbuf = ((byte *)outBufMb->bufferBase())+outBufStart;
        out = (byte *)outbuf+outlen;
        remaining = outBufMb->capacity()-outlen;
    }
public:
    IMPLEMENT_IINTERFACE;
    CRDiffCompressor()
    {
        outbuf = NULL;
        outlen = 0;
        maxrecsize = 0;
        recsize = 0;
        bufalloc = 0;
        prev = NULL;
        outBufMb = NULL;
    }

    ~CRDiffCompressor()
    {
        free(prev);
        if (bufalloc)
            free(outbuf);
    }

    void open(MemoryBuffer &mb, size32_t initialSize)
    {
        outBufMb = &mb;
        outBufStart = mb.length();
        outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
        bufalloc = 0;
        initCommon();
        remaining = outBufMb->capacity()-outlen;
    }

    void open(void *buf,size32_t max)
    {
        if (buf)
        {
            if (bufalloc)
                free(outbuf);
            bufalloc = 0;
            outbuf = buf;
        }
        else if (max>bufalloc)
        {
            if (bufalloc)
                free(outbuf);
            bufalloc = max;
            outbuf = malloc(bufalloc);
        }
        outBufMb = NULL;
        ASSERT(max>2+sizeof(size32_t)*2); // minimum required (actually will need enough for recsize so only a guess)
        initCommon();
        remaining = max-outlen;
    }

    void close()
    {
        transbuf.clear();
        memcpy(outbuf,&inlen,sizeof(inlen));        // expanded size
        memcpy((byte *)outbuf+sizeof(inlen),&recsize,sizeof(recsize));
        if (outBufMb)
        {
            outBufMb->setWritePos(outBufStart+outlen);
            outBufMb = NULL;
        }
    }

    inline size32_t maxcompsize(size32_t s) { return s+((s+254)/255)*2; }

    size32_t write(const void *buf,size32_t buflen)
    {
        // assumes a transaction is a row and at least one row fits in
        if (prev)
        {
            if (transbuf.length()==0)
            {
                if (remaining<maxrecsize)  // this is a bit odd because no incremental diffcomp
                {
                    if (NULL == outBufMb)
                        return 0;
                }
            }
            transbuf.append(buflen,buf);
        }
        else // first row
        {
            if (remaining<buflen)
                ensure(buflen);
            memcpy(out,buf,buflen);
            out += buflen;
            outlen += buflen;
        }
        // should inlen be updated here (probably not in transaction mode which is all this supports)
        return buflen;
    }



    void startblock()
    {
        transbuf.clear();
    }

    void commitblock()
    {
        if (prev)
        {
            if (recsize!=transbuf.length())
                throw MakeStringException(-1,"CRDiffCompressor used with variable sized row");
            if (remaining<maxrecsize)
                ensure(maxrecsize-remaining);
            size32_t sz = DiffCompress(transbuf.toByteArray(),out,prev,recsize);
            transbuf.clear();
            out += sz;
            outlen += sz;
            remaining -= sz;
        }
        else
        {
            recsize = outlen-sizeof(size32_t)*2;
            maxrecsize = maxcompsize(recsize);
            prev = (byte *)malloc(recsize);
            memcpy(prev,out-recsize,recsize);
            remaining -= recsize;
        }
        inlen += recsize;
    }


    virtual void *bufptr() { return outbuf;}
    virtual size32_t buflen() { return outlen;}

};


class jlib_decl CRDiffExpander : public CInterface, public IExpander
{
    unsigned char *outbuf;
    size32_t outlen;
    size32_t bufalloc;
    unsigned char *in;
    size32_t recsize;
public:
    IMPLEMENT_IINTERFACE;

    CRDiffExpander()
    {
        outbuf = NULL;
        outlen = 0;
        bufalloc = 0;
        recsize = 0;
    }

    ~CRDiffExpander()
    {
        if (bufalloc)
            free(outbuf);
    }

    size32_t  init(const void *blk) // returns size required
    {
        memcpy(&outlen,blk,sizeof(outlen));
        memcpy(&recsize,(unsigned char *)blk+sizeof(outlen),sizeof(recsize));
        in=(unsigned char *)blk+sizeof(outlen)+sizeof(recsize);
        return outlen;
    }

    void expand(void *buf)
    {
        if (!outlen)
            return;
        if (buf) {
            if (bufalloc)
                free(outbuf);
            bufalloc = 0;
            outbuf = (unsigned char *)buf;
        }
        else if (outlen>bufalloc) {
            if (bufalloc)
                free(outbuf);
            bufalloc = outlen;
            outbuf = (unsigned char *)malloc(bufalloc);
        }
        if (outlen<recsize) 
            throw MakeStringException(-1,"CRDiffExpander: invalid buffer format");
        unsigned char *out=outbuf;
        memcpy(out,in,recsize);
        const unsigned char *prev = out;
        out += recsize;
        in += recsize;
        size_t remaining = outlen-recsize;
        while (remaining) {
            if (remaining<recsize) 
                throw MakeStringException(-2,"CRDiffExpander: invalid buffer format");
            size32_t sz = DiffExpand(in,out,prev,recsize);
            in += sz;
            prev = out;
            out += recsize;
            remaining -= recsize;
        }
    }


    
    virtual void *bufptr() { return outbuf;}
    virtual size32_t   buflen() { return outlen;}
};


ICompressor *createRDiffCompressor()
{
    return new CRDiffCompressor;
}

IExpander *createRDiffExpander()
{
    return new CRDiffExpander;
}


// =============================================================================

// RANDRDIFF
// format ::=  <totsize> <0xffff> <recsize> <firstrlesize> <numrows> { <rowofs> }  <difrecs> <firsrecrle>
// all 16bit except recs

struct RRDheader
{
    unsigned short totsize;
    unsigned short flag;
    unsigned short recsize;
    unsigned short firstrlesize;
    unsigned short numrows;
    unsigned short rowofs[0x3fff];
    inline   unsigned short hsize() { return (5+numrows)*sizeof(short); }
};

#define MIN_RRDHEADER_SIZE (5*sizeof(short))



class jlib_decl CRandRDiffCompressor : public CInterface, public ICompressor
{
    size32_t inlen;
    size32_t bufalloc;
    size32_t max;
    void *outbuf;
    RRDheader *header;
    // assumes a transaction is a record
    MemoryBuffer rowbuf;
    MemoryBuffer diffbuf;
    MemoryBuffer firstrec;
    MemoryAttr firstrle;
    size32_t maxdiffsize;
    size32_t recsize;
    size32_t compsize;
    size32_t outBufStart;
    MemoryBuffer *outBufMb;

    void initCommon()
    {
        header = (RRDheader *)outbuf;
        inlen = 0;
        memset(header,0,MIN_RRDHEADER_SIZE);
        diffbuf.clear();
        firstrec.clear();
        firstrle.clear();
        rowbuf.clear();
    }
public:
    IMPLEMENT_IINTERFACE;
    CRandRDiffCompressor()
    {
        outbuf = NULL;
        header = NULL;
        bufalloc = 0;
        max = 0;
        maxdiffsize = 0;
        recsize = 0;
        outBufStart = 0;
        outBufMb = NULL;
    }
        
    ~CRandRDiffCompressor()
    {
        if (bufalloc)
            free(outbuf);
    }

    void open(MemoryBuffer &mb, size32_t initialSize)
    {
        outBufMb = &mb;
        outBufStart = mb.length();
        outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
        bufalloc = 0;
        initCommon();
    }

    void open(void *buf,size32_t _max)
    {
        max = _max;
        if (buf) {
            if (bufalloc) {
                free(outbuf);
            }
            bufalloc = 0;
            outbuf = buf;
        }
        else if (max>bufalloc) {
            if (bufalloc)
                free(outbuf);
            bufalloc = max;
            outbuf = malloc(bufalloc);
        }
        outBufMb = NULL;
        ASSERT(max>MIN_RRDHEADER_SIZE+sizeof(unsigned short)+3); // hopefully a lot bigger!
        initCommon();
    }

    void close()
    {
        header->rowofs[0] = (unsigned short)diffbuf.length();
        ASSERT((size32_t)(header->totsize+header->firstrlesize)<=max);
        unsigned short hofs = header->hsize();
        ASSERT(header->totsize==hofs+diffbuf.length());
        if (outBufMb)
        {
            outbuf = (byte *)outBufMb->ensureCapacity(header->totsize+header->firstrlesize);
            outBufMb->setWritePos(outBufStart+header->totsize+header->firstrlesize);
            outBufMb = NULL;
        }
        byte *out = (byte *)outbuf+hofs;
        memcpy(out,diffbuf.toByteArray(),diffbuf.length());
        out += diffbuf.length();
        diffbuf.clear();
        memcpy(out,firstrle.bufferBase(),header->firstrlesize);
        header->totsize += header->firstrlesize;
        firstrle.clear();
        firstrec.clear();
        header->flag = 0xffff;
        // adjust offsets
        unsigned i = header->numrows;
        while (i--)
            header->rowofs[i] += hofs;
    }

    inline size32_t maxcompsize(size32_t s) { return s+((s+254)/255)*2; }

    size32_t write(const void *buf,size32_t buflen)
    {
        // assumes a transaction is a row and at least one row fits in
        unsigned nr = header->numrows;
        if (nr) {
            rowbuf.append(buflen,buf);
            if (rowbuf.length()==recsize)   { // because no incremental diffcomp do here
                size32_t sz = diffbuf.length();
                compsize = DiffCompress2(rowbuf.toByteArray(),diffbuf.reserve(maxdiffsize),firstrec.toByteArray(),recsize);
                if (header->totsize+sizeof(short)+compsize+header->firstrlesize>max) {
                    diffbuf.setLength(sz);
                    return 0;
                }
                header->rowofs[nr] = (unsigned short)sz; // will need to adjust later
                diffbuf.setLength(sz+compsize);
            }
        }
        else 
            firstrec.append(buflen,buf);
        return buflen;
    }



    void startblock()
    {
        rowbuf.clear();
    }

    void commitblock()
    {
        unsigned nr = header->numrows;
        if (nr) {
            if (recsize!=rowbuf.length())
                throw MakeStringException(-1,"CRandDiffCompressor used with variable sized row");
            rowbuf.clear();
            header->numrows++;
            header->totsize += (unsigned short)compsize+sizeof(unsigned short);
        }
        else {
            header->numrows = 1;
            header->totsize = header->hsize(); // don't add in rle size yet
            recsize = firstrec.length();
            header->recsize = (unsigned short)recsize;
            maxdiffsize = maxcompsize(recsize);
            size32_t sz = RLECompress(firstrle.allocate(recsize+2),firstrec.toByteArray(),recsize);
            header->firstrlesize = (unsigned short)sz;
        }
        inlen += recsize;
    }


    void *bufptr() { return outbuf;}
    size32_t buflen() { return header->totsize;}

};


class jlib_decl CRandRDiffExpander : public CInterface, public IRandRowExpander
{
    MemoryAttr buf;
    const RRDheader *header;
    size32_t recsize;
    unsigned numrows;
    byte *firstrow;

    inline byte *rowptr(unsigned idx) const { return (byte *)header+header->rowofs[idx]; }

public:
    IMPLEMENT_IINTERFACE;

    CRandRDiffExpander()
    {
        recsize = 0;
        numrows = 0;
        header = NULL;
    }

    ~CRandRDiffExpander()
    {
    }

    bool init(const void *blk,bool copy) 
    {
        // if copy then use new block with first row at end
        header=(const RRDheader *)blk;
        if (header->flag!=0xffff)   // flag
            return false;
        recsize = header->recsize;
        numrows = header->numrows;
        RRDheader *headercopy;
        if (copy) {
            size32_t sz = header->totsize-header->firstrlesize+recsize;
            headercopy = (RRDheader *)buf.allocate(sz);
            memcpy(headercopy,blk,header->totsize-header->firstrlesize);
            firstrow = (byte *)headercopy+headercopy->rowofs[0];
            headercopy->totsize = (unsigned short)sz;
        }
        else
            firstrow = (byte *)buf.allocate(recsize);
        RLEExpand(firstrow,(const byte *)header+header->rowofs[0],recsize);
        if (copy)
            header = headercopy;
        return true; 
    }



    bool expandRow(void *target,unsigned idx) const
    {
        if (idx>=numrows)
            return false;
        if (idx) 
            DiffExpand(rowptr(idx),target,firstrow,recsize);
        else 
            memcpy(target, firstrow, recsize);
        return true;
    }

    size32_t expandRow(void *target,unsigned idx,size32_t ofs,size32_t sz) const
    {
        if ((idx>=numrows)||(ofs>=recsize))
            return 0;
        if (sz>recsize-ofs) 
            sz = recsize-ofs;
        if (idx==0) 
            memcpy(target,firstrow+ofs,sz);
        else if ((ofs==0)&&(sz>=recsize))
            DiffExpand(rowptr(idx),target,firstrow,recsize);
        else {
            CDiffExpand exp;
            exp.init(rowptr(idx),firstrow,recsize);
            exp.skip(ofs);
            exp.cpy(target,sz);
        }
        return sz;
    }
    int cmpRow(const void *target,unsigned idx,size32_t ofs=0,size32_t sz=(size32_t)-1) const
    {
        if ((idx>=numrows)||(ofs>=recsize))
            return -1;
        if (sz>=recsize-ofs) 
            sz = recsize-ofs;
        if (idx==0) 
            return memcmp(target,firstrow+ofs,sz);
        CDiffExpand exp;
        exp.init(rowptr(idx),firstrow,recsize);
        exp.skip(ofs);
        return exp.cmp(target,sz);
    }


    size32_t rowSize() const { return recsize; }
    unsigned numRows() const { return numrows; }

    const byte *firstRow() const { return firstrow; }

};




ICompressor *createRandRDiffCompressor()
{
    return new CRandRDiffCompressor;
}

IRandRowExpander *createRandRDiffExpander()
{
    return new CRandRDiffExpander;
}




// =============================================================================

// Compressed files

typedef enum { ICFcreate, ICFread, ICFappend } ICFmode;

static const __int64 COMPRESSEDFILEFLAG = I64C(0xc0528ce99f10da55);
#define COMPRESSEDFILEBLOCKSIZE (0x10000)
static const __int64 FASTCOMPRESSEDFILEFLAG = I64C(0xc1518de99f10da55);

#pragma pack(push,1)

struct CompressedFileTrailer
{
    unsigned        datacrc;            
    offset_t        expandedSize;
    offset_t        indexPos;       // end of blocks
    size32_t        blockSize;
    size32_t        recordSize;     // 0 is lzw compressed
    __int64        compressedType;
    unsigned        crc;                // must be last
    unsigned numBlocks() { return (unsigned)((indexPos+blockSize-1)/blockSize); }
    unsigned method()
    {
        if (recordSize)
            return COMPRESS_METHOD_ROWDIF;
        if (compressedType==COMPRESSEDFILEFLAG)
            return COMPRESS_METHOD_LZW;
        if (compressedType==FASTCOMPRESSEDFILEFLAG)
            return COMPRESS_METHOD_FASTLZ;
        return 0;
    }

    void setDetails(IPropertyTree &tree)
    {
        tree.setPropInt("@datacrc",datacrc);        
        tree.setPropInt64("@expandedSize",expandedSize);
        tree.setPropInt64("@indexPos",indexPos);
        tree.setPropInt("@blockSize",blockSize);
        tree.setPropInt("@recordSize",recordSize);      // 0 is lzw compressed
        tree.setPropInt64("@compressedType",compressedType);
        tree.setPropInt("@method",method());
        tree.setPropInt("@crc",crc);                
        tree.setPropInt("@numblocks",(unsigned)((indexPos+blockSize-1)/blockSize));             
    }
};

// backward compatibility - will remove at some point
struct WinCompressedFileTrailer
{
    unsigned        datacrc;            
    unsigned        filler1;
    offset_t        expandedSize;
    offset_t        indexPos;       // end of blocks
    size32_t        blockSize;
    size32_t        recordSize;     // 0 is lzw compressed
    __int64         compressedType;
    unsigned        crc;            // must be last
    unsigned        filler2;

    void translate(CompressedFileTrailer &out)
    {
        if (compressedType==COMPRESSEDFILEFLAG) {
            out.datacrc = datacrc;
            out.expandedSize = expandedSize;
            out.indexPos = indexPos;
            out.blockSize = blockSize;
            out.recordSize = recordSize;
            out.compressedType = compressedType;
            out.crc = crc;
        }
        else {
            memcpy(&out,(byte *)this+sizeof(WinCompressedFileTrailer)-sizeof(CompressedFileTrailer),sizeof(CompressedFileTrailer));
        }
    }

};


#pragma pack(pop)


class CCompressedFile : public CInterface, implements ICompressedFileIO
{
    Linked<IFileIO> fileio;
    Linked<IMemoryMappedFile> mmfile;
    CompressedFileTrailer trailer;
    unsigned curblocknum;           
    offset_t curblockpos;           // logical pos (reading only)
    MemoryBuffer curblockbuf;       // expanded buffer when reading
    MemoryAttr compblk;
    byte *compblkptr;
    size32_t compblklen;
    MemoryAttr compbuf;
    MemoryBuffer indexbuf;          // non-empty once index read
    ICFmode mode;
    CriticalSection crit;
    MemoryBuffer overflow;          // where partial row written
    MemoryAttr prevrowbuf; 
    bool setcrc;
    bool writeException;
    Owned<ICompressor> compressor;
    Owned<IExpander> expander;

    unsigned indexNum() { return indexbuf.length()/sizeof(offset_t); }

    unsigned lookupIndex(offset_t pos,offset_t &curpos,size32_t &expsize)
    {
        // NB index starts at block 1 (and has size as last entry)
        const offset_t *index;
        unsigned a = 0;
        unsigned b = indexNum();
        index = (const offset_t *)indexbuf.toByteArray();
        while (b>a) {
            unsigned m = (a+b)/2;
            __int64 dif = (__int64)pos-index[m];
            if (dif==0) {
                b = m+1;
                a = b;
            }
            else if (dif>0) 
                a = m+1;
            else
                b = m;
        }
        curpos=b?index[b-1]:0;
        expsize = (size32_t)(index[b]-curpos);
        return b;
    }


    void getblock(offset_t pos)
    {
        curblockbuf.clear();
        size32_t expsize;
        curblocknum = lookupIndex(pos,curblockpos,expsize);
        size32_t toread = trailer.blockSize;
        offset_t p = (offset_t)curblocknum*toread;
        assertex(p<=trailer.indexPos);
        if (trailer.indexPos-p<(offset_t)toread)
            toread = (size32_t)(trailer.indexPos-p);
        if (!toread) 
            return;
        if (fileio) {
            MemoryAttr comp;
            void *b=comp.allocate(toread);
            size32_t r = fileio->read(p,toread,b);
            assertex(r==toread);
            expand(b,curblockbuf,expsize);
        }
        else { // memory mapped
            assertex((memsize_t)p==p);
            expand(mmfile->base()+(memsize_t)p,curblockbuf,expsize);
        }
    }
    void checkedwrite(offset_t pos, size32_t len, const void * data) 
    {
        size32_t ret = fileio->write(pos,len,data);
        if (ret!=len)
            throw MakeStringException(DISK_FULL_EXCEPTION_CODE,"CCompressedFile::checkedwrite");        
        if (setcrc) 
            trailer.crc = crc32((const char *)data,len,trailer.crc);

    }

    void flush()
    {   
        try
        {
            curblocknum++;
            indexbuf.append((unsigned __int64) trailer.expandedSize-overflow.length());
            offset_t p = ((offset_t)curblocknum)*((offset_t)trailer.blockSize);
            if (trailer.recordSize==0) {
                compressor->close();
                compblklen = compressor->buflen();
            }
            if (compblklen) {
                if (p>trailer.indexPos) { // fill gap
                    MemoryAttr fill;
                    size32_t fl = (size32_t)(p-trailer.indexPos);
                    memset(fill.allocate(fl),0xff,fl);
                    checkedwrite(trailer.indexPos,fl,fill.get());
                }
                checkedwrite(p,compblklen,compblkptr);
                p += compblklen;
                compblklen = 0;
            }
            trailer.indexPos = p;
            if (trailer.recordSize==0) {
                compressor->open(compblkptr, trailer.blockSize);
            }
        }
        catch (IException *e)
        {
            writeException = true;
            EXCLOG(e, "CCompressedFile::flush");
            throw;
        }
    }

    virtual void expand(const void *compbuf,MemoryBuffer &expbuf,size32_t expsize)
    {
        size32_t rs = trailer.recordSize;
        if (rs) { // diff compress
            const byte *src = (const byte *)compbuf;
            byte *dst = (byte *)expbuf.reserve(expsize);
            if (expsize) {
                assertex(expsize>=rs);
                memcpy(dst,src,rs);
                dst += rs;
                src += rs;
                expsize -= rs;
                while (expsize) {
                    assertex(expsize>=rs);
                    src += DiffExpand(src, dst, dst-rs, rs);
                    expsize -= rs;
                    dst += rs;
                }
            }
        }
        else { // lzw 
            assertex(expander.get());
            size32_t exp = expander->init(compbuf);
            if (exp!=expsize) {
                throw MakeStringException(-1,"Compressed file format failure(%d,%d) - Encrypted?",exp,expsize);
            }
            expander->expand(expbuf.reserve(exp));
        }
    }

    bool compressrow(const void *src,size32_t rs)
    {
        bool ret = true;
        if (compblklen==0) {
            memcpy(prevrowbuf.bufferBase(),src,rs);
            memcpy(compblkptr,src,rs);
            compblklen = rs;
        }
        else {
            size32_t len = DiffCompress(src,compblkptr+compblklen,prevrowbuf.bufferBase(),rs);
            if (compblklen+len>trailer.blockSize) 
                ret = false;
            else
                compblklen += len;
        }
        return ret;
    }

    size32_t compress(const void *expbuf,size32_t len)  // iff return!=len then overflowed
    {
        const byte *src = (const byte *)expbuf;
        size32_t rs = trailer.recordSize;
        if (rs) { // diff compress
            if (overflow.length()) {
                assertex(overflow.length()<=rs);
                size32_t left = rs-overflow.length();
                if (left>len)
                    left = len;
                overflow.append(left,expbuf);
                len -= left;
                if (overflow.length()==rs) {
                    if (!compressrow(overflow.toByteArray(),rs)) {  // this is nasty case
                        overflow.setLength(rs-left);
                        return (size32_t)(src-(const byte *)expbuf);
                    }
                    overflow.clear();
                }
                src += left;
            }
            while (len>=rs) {
                if (!compressrow(src,rs))   
                    return (size32_t)(src-(const byte *)expbuf);
                len -= rs;
                src += rs;
            }
            if (len) {
                overflow.append(len,src);
                src += len;
            }
        }
        else 
            src += compressor->write(src, len);
        return (size32_t)(src-(const byte *)expbuf);
    }
public:
    IMPLEMENT_IINTERFACE;

    CCompressedFile(IFileIO *_fileio,IMemoryMappedFile *_mmfile,CompressedFileTrailer &_trailer,ICFmode _mode, bool _setcrc,ICompressor *_compressor,IExpander *_expander, bool fast)
        : fileio(_fileio), mmfile(_mmfile)
    {
        compressor.set(_compressor);
        expander.set(_expander);
        setcrc = _setcrc;
        writeException = false;
        memcpy(&trailer,&_trailer,sizeof(trailer));
        mode = _mode;
        curblockpos = 0;
        curblocknum = (unsigned)-1; // relies on wrap
        if (mode!=ICFread) {
            if (!_fileio&&_mmfile)
                throw MakeStringException(-1,"Compressed Write not supported on memory mapped files");
            if (trailer.recordSize) {
                if ((trailer.recordSize>trailer.blockSize/4) || // just too big
                    (trailer.recordSize<10))                    // or too small
                    trailer.recordSize = 0;
                else
                    prevrowbuf.allocate(trailer.recordSize);
            }
            compblkptr = (byte *)compblk.allocate(trailer.blockSize+trailer.recordSize*2+16); // over estimate!
            compblklen = 0;
            if (trailer.recordSize==0) {
                if (!compressor)
                {
                    if (fast)
                        compressor.setown(createFastLZCompressor());
                    else
                        compressor.setown(createLZWCompressor(true));
                }
                compressor->open(compblkptr, trailer.blockSize);
            }
        }
        if (mode!=ICFcreate) {
            unsigned nb = trailer.numBlocks();
            size32_t toread = sizeof(offset_t)*nb;
            if (fileio) {
                size32_t r = fileio->read(trailer.indexPos,toread,indexbuf.reserveTruncate(toread));
                assertex(r==toread);
            }
            else {
                assertex((memsize_t)trailer.indexPos==trailer.indexPos);
                memcpy(indexbuf.reserveTruncate(toread),mmfile->base()+(memsize_t)trailer.indexPos,toread);
            }
            if (mode==ICFappend) {
                curblocknum = nb-1;
                if (setcrc) {
                    trailer.crc = trailer.datacrc;
                    trailer.datacrc = ~0U;
                }
            }
            if (trailer.recordSize==0) {
                if (!expander) {
                    if (fast)
                        expander.setown(createFastLZExpander());
                    else
                        expander.setown(createLZWExpander(true));
                }
            }
        }
    }
    virtual ~CCompressedFile()
    {
        if (!writeException)
        {
            try { close(); }
            catch (IException *e)
            {
                EXCLOG(e, "~CCompressedFile");
                e->Release();
            }
        }
    }

    virtual offset_t size()                                             
    { 
        CriticalBlock block(crit);
        return trailer.expandedSize;
    }

    virtual size32_t read(offset_t pos, size32_t len, void * data)          
    {
        CriticalBlock block(crit);
        assertex(mode==ICFread);
        size32_t ret=0;
        while (pos<trailer.expandedSize) {
            if ((offset_t)len>trailer.expandedSize-pos)
                len = (size32_t)(trailer.expandedSize-pos);
            if ((pos>=curblockpos)&&(pos<curblockpos+curblockbuf.length())) { // see if in current buffer
                size32_t tocopy = (size32_t)(curblockpos+curblockbuf.length()-pos);
                if (tocopy>len)
                    tocopy = len;
                memcpy(data,curblockbuf.toByteArray()+(pos-curblockpos),tocopy);
                ret += tocopy;
                len -= tocopy;
                data = (byte *)data+tocopy;
                pos += tocopy;
            }
            if (len==0)
                break;
            getblock(pos);
        }
        return ret;
    }
    size32_t write(offset_t pos, size32_t len, const void * data)   
    {
        CriticalBlock block(crit);
        assertex(mode!=ICFread);
        size32_t ret = 0;
        loop {
            if (pos!=trailer.expandedSize)
                throw MakeStringException(-1,"sequential writes only on compressed file");
            size32_t done = compress(data,len);
            trailer.expandedSize += done;
            len -= done;
            ret += done;
            pos += done;
            data = (const byte *)data+done;
            if (len==0)
                break;
            flush();
        }
        return ret;
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind)
    {
        return fileio->getStatistic(kind);
    }

    void setSize(offset_t size) { UNIMPLEMENTED; }
    offset_t appendFile(IFile *file,offset_t pos,offset_t len) { UNIMPLEMENTED; }

    void close()
    {
        CriticalBlock block(crit);
        if (mode!=ICFread) {
            if (overflow.length()) {
                unsigned ol = overflow.length();
                overflow.clear();
                throw MakeStringException(-1,"Partial row written at end of file %d of %d",ol,trailer.recordSize);
            }
            flush();
            trailer.datacrc = trailer.crc;
            if (setcrc) {
                indexbuf.append(sizeof(trailer)-sizeof(trailer.crc),&trailer);
                trailer.crc = crc32((const char *)indexbuf.toByteArray(),
                                indexbuf.length(),trailer.crc);
                indexbuf.append(trailer.crc);
            }
            else {
                trailer.datacrc = 0;
                trailer.crc = ~0U;
                indexbuf.append(sizeof(trailer),&trailer);
            }
            checkedwrite(trailer.indexPos,indexbuf.length(),indexbuf.toByteArray());
            indexbuf.clear();
        }
        mode = ICFread;
        curblockpos = 0;
        curblocknum = (unsigned)-1; // relies on wrap
    }

    unsigned dataCRC()
    {
        if (mode==ICFread)
            return trailer.datacrc;
        return trailer.crc;
    }
    size32_t recordSize()
    {
        return trailer.recordSize;
    }
    size32_t blockSize()
    {
        return trailer.blockSize;
    }
    void setBlockSize(size32_t size)
    {
        trailer.blockSize = size;
    }
    bool readMode()
    {
        return (mode==ICFread);
    }

    unsigned method()
    {
        return trailer.method();
    }


};



        

ICompressedFileIO *createCompressedFileReader(IFileIO *fileio,IExpander *expander)
{
    if (fileio) {
        offset_t fsize = fileio->size();
        if (fsize>=sizeof(WinCompressedFileTrailer)) {  // thats 8 bytes bigger but I think doesn't matter
            WinCompressedFileTrailer wintrailer;
            CompressedFileTrailer trailer;
            if (fileio->read(fsize-sizeof(WinCompressedFileTrailer),sizeof(WinCompressedFileTrailer),&wintrailer)==sizeof(WinCompressedFileTrailer)) {
                wintrailer.translate(trailer);
                if ((trailer.compressedType==COMPRESSEDFILEFLAG)||(trailer.compressedType==FASTCOMPRESSEDFILEFLAG)) {
                    if (expander&&(trailer.recordSize!=0)) {
                        throw MakeStringException(-1, "Compressed file format error(%d), Encrypted?",trailer.recordSize);
                    }
                    CCompressedFile *cfile = new CCompressedFile(fileio,NULL,trailer,ICFread,false,NULL,expander,(trailer.compressedType==FASTCOMPRESSEDFILEFLAG));
                    return cfile;
                }
            }
        }
    }
    return NULL;
}


ICompressedFileIO *createCompressedFileReader(IFile *file,IExpander *expander, bool memorymapped, IFEflags extraFlags)
{
    if (file) {
        if (memorymapped) {
            Owned<IMemoryMappedFile> mmfile = file->openMemoryMapped();
            if (mmfile) {
                offset_t fsize = mmfile->fileSize();
                if (fsize>=sizeof(WinCompressedFileTrailer)) {  // thats 8 bytes bigger but I think doesn't matter
                    WinCompressedFileTrailer wintrailer;
                    CompressedFileTrailer trailer;
                    memcpy(&wintrailer,mmfile->base()+fsize-sizeof(WinCompressedFileTrailer),sizeof(WinCompressedFileTrailer));
                    wintrailer.translate(trailer);
                    if ((trailer.compressedType==COMPRESSEDFILEFLAG)||(trailer.compressedType==FASTCOMPRESSEDFILEFLAG)) {
                        if (expander&&(trailer.recordSize!=0)) {
                            throw MakeStringException(-1, "Compressed file format error(%d), Encrypted?",trailer.recordSize);
                        }
                        CCompressedFile *cfile = new CCompressedFile(NULL,mmfile,trailer,ICFread,false,NULL,expander,(trailer.compressedType==FASTCOMPRESSEDFILEFLAG));
                        return cfile;
                    }
                }
            }
        }
        Owned<IFileIO> fileio = file->open(IFOread, extraFlags);
        if (fileio) 
            return createCompressedFileReader(fileio,expander);
    }
    return NULL;
}




ICompressedFileIO *createCompressedFileWriter(IFileIO *fileio,size32_t recordsize,bool _setcrc,ICompressor *compressor,bool fast)
{
    CompressedFileTrailer trailer;
    offset_t fsize = fileio->size();
    if (fsize) {
        loop {
            if (fsize>=sizeof(WinCompressedFileTrailer)) {  // thats 8 bytes bigger but I think doesn't matter
                WinCompressedFileTrailer wintrailer;
                CompressedFileTrailer trailer;
                if (fileio->read(fsize-sizeof(WinCompressedFileTrailer),sizeof(WinCompressedFileTrailer),&wintrailer)==sizeof(WinCompressedFileTrailer)) {
                    wintrailer.translate(trailer);
                    if ((trailer.compressedType==COMPRESSEDFILEFLAG)||(trailer.compressedType==FASTCOMPRESSEDFILEFLAG)) {
                        if ((recordsize==trailer.recordSize)||!trailer.recordSize)
                            break;
                        throw MakeStringException(-1,"Appending to file with different record size (%d,%d)",recordsize,trailer.recordSize);
                    }
                }
            }
            throw MakeStringException(-1,"Appending to file that is not compressed");
        }
    }
    else {
        memset(&trailer,0,sizeof(trailer));
        trailer.crc = ~0U;
        trailer.compressedType = fast?FASTCOMPRESSEDFILEFLAG:COMPRESSEDFILEFLAG;
        trailer.blockSize = COMPRESSEDFILEBLOCKSIZE;
        trailer.recordSize = recordsize;
    }
    if (compressor)
        trailer.recordSize = 0; // force not row compressed if compressor specified
    CCompressedFile *cfile = new CCompressedFile(fileio,NULL,trailer,fsize?ICFappend:ICFcreate,_setcrc,compressor,NULL,fast);
    return cfile;
}

ICompressedFileIO *createCompressedFileWriter(IFile *file,size32_t recordsize,bool append,bool _setcrc,ICompressor *compressor,bool fast, IFEflags extraFlags)
{
    if (file) {
        if (append&&!file->exists())
            append = false;
        Owned<IFileIO> fileio = file->open(append?IFOreadwrite:IFOcreate, extraFlags);
        if (fileio) 
            return createCompressedFileWriter(fileio,recordsize,_setcrc,compressor,fast);
    }
    return NULL;
}


//===================================================================================



#define AES_PADDING_SIZE 32


class CAESCompressor : public CInterface, implements ICompressor
{
    Owned<ICompressor> comp;    // base compressor
    MemoryBuffer compattr;      // compressed buffer
    MemoryAttr outattr;         // compressed and encrypted (if outblk NULL)
    void *outbuf;               // dest
    size32_t outlen;
    size32_t outmax;
    MemoryAttr key;
    MemoryBuffer *outBufMb;

public:
    IMPLEMENT_IINTERFACE;
    CAESCompressor(const void *_key, unsigned _keylen)
        : key(_keylen,_key)
    {
        comp.setown(createLZWCompressor(true));
        outlen = 0;
        outmax = 0;
        outBufMb = NULL;
    }

    void open(MemoryBuffer &mb, size32_t initialSize)
    {
        outlen = 0;
        outmax = initialSize;
        outbuf = NULL;
        outBufMb = &mb;
        comp->open(compattr, initialSize);
    }

    void open(void *blk,size32_t blksize)
    {
        outlen = 0;
        outmax = blksize;
        if (blk)
            outbuf = blk;
        else
            outbuf = outattr.allocate(blksize);
        outBufMb = NULL;
        size32_t subsz = blksize-AES_PADDING_SIZE-sizeof(size32_t);
        comp->open(compattr.reserveTruncate(subsz),subsz);
    }

    void close()
    {
        comp->close();
        // now encrypt
        MemoryBuffer buf;
        aesEncrypt(key.get(), key.length(), comp->bufptr(), comp->buflen(), buf);
        outlen = buf.length();
        if (outBufMb)
        {
            outmax = sizeof(size32_t)+outlen;
            outbuf = outBufMb->reserveTruncate(outmax);
            outBufMb = NULL;
        }
        memcpy(outbuf,&outlen,sizeof(size32_t));
        outlen += sizeof(size32_t);
        assertex(outlen<=outmax);
        memcpy((byte *)outbuf+sizeof(size32_t),buf.bufferBase(),buf.length());
        outmax = 0;
    }

    size32_t write(const void *buf,size32_t len)
    {
        return comp->write(buf,len);
    }

    void * bufptr()
    {
        assertex(0 == outmax); // i.e. closed
        return outbuf;
    }

    size32_t buflen()
    {
        assertex(0 == outmax); // i.e. closed
        return outlen;
    }

    void startblock()
    {
        comp->startblock();
    }

    void commitblock()
    {
        comp->commitblock();
    }
};

class CAESExpander : public CInterface, implements IExpander
{
    Owned<IExpander> exp;   // base expander
    MemoryBuffer compbuf;
    MemoryAttr key;
public:
    IMPLEMENT_IINTERFACE;
    CAESExpander(const void *_key, unsigned _keylen)
        : key(_keylen,_key)
    {
        exp.setown(createLZWExpander(true));
    }
    size32_t init(const void *blk)
    {
        // first decrypt
        const byte *p = (const byte *)blk;
        size32_t l = *(const size32_t *)p;
        aesDecrypt(key.get(),key.length(),p+sizeof(size32_t),l,compbuf);
        return exp->init(compbuf.bufferBase());         
    }

    void   expand(void *target)
    {
        exp->expand(target);
    }

    virtual void * bufptr()
    {
        return exp->bufptr();
    }

    virtual size32_t buflen()
    {
        return exp->buflen();
    }
};


ICompressor *createAESCompressor(const void *key, unsigned keylen)
{
    return  new CAESCompressor(key,keylen);
}
IExpander *createAESExpander(const void *key, unsigned keylen)
{
    return new CAESExpander(key,keylen);
}

#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (8 - (n))))


inline void padKey32(byte *keyout,size32_t len, const byte *key)
{
    if (len==0) 
        memset(keyout,0xcc,32);
    else if (len<=32) {
        for (unsigned i=0;i<32;i++)
            keyout[i] = (i<len)?key[i%len]:ROTATE_LEFT(key[i%len],i/len);
    }
    else {
        memcpy(keyout,key,32);
        // xor excess rotated
        for (unsigned i=32;i<len;i++) 
            keyout[i%32] ^= ROTATE_LEFT(key[i],(i/8)%8);
    }
}


ICompressor *createAESCompressor256(size32_t len, const void *key)
{
    byte k[32];
    padKey32(k,len,(const byte *)key);
    return  new CAESCompressor(k,32);
}

IExpander *createAESExpander256(size32_t len, const void *key)
{
    byte k[32];
    padKey32(k,len,(const byte *)key);
    return new CAESExpander(k,32);
}



IPropertyTree *getBlockedFileDetails(IFile *file)
{
    Owned<IPropertyTree> tree = createPTree("BlockedFile");
    Owned<IFileIO> fileio = file?file->open(IFOread):NULL;
    if (fileio) {
        offset_t fsize = fileio->size();
        tree->setPropInt64("@size",fsize);
        if (fsize>=sizeof(WinCompressedFileTrailer)) {  // thats 8 bytes bigger but I think doesn't matter
            WinCompressedFileTrailer wintrailer;
            CompressedFileTrailer trailer;
            if (fileio->read(fsize-sizeof(WinCompressedFileTrailer),sizeof(WinCompressedFileTrailer),&wintrailer)==sizeof(WinCompressedFileTrailer)) {
                wintrailer.translate(trailer);
                if ((trailer.compressedType==COMPRESSEDFILEFLAG)||(trailer.compressedType==FASTCOMPRESSEDFILEFLAG)) {
                    trailer.setDetails(*tree);
                    unsigned nb = trailer.numBlocks();
                    MemoryAttr indexbuf;
                    size32_t toread = sizeof(offset_t)*nb;
                    size32_t r = fileio->read(trailer.indexPos,toread,indexbuf.allocate(toread));
                    if (r&&(r==toread)) {
                        offset_t s = 0;
                        const offset_t *index = (const offset_t *)indexbuf.bufferBase();
                        for (unsigned i=0;i<nb;i++) {
                            IPropertyTree * t = tree->addPropTree("Block",createPTree("Block"));
                            t->addPropInt64("@start",s);
                            offset_t p = s;
                            s = index[i];
                            t->addPropInt64("@end",s);
                            t->addPropInt64("@length",s-p);
                        }
                    }
                    return tree.getClear();
                }
            }
        }
    }
    return NULL;
}

class CCompressHandlerArray : public IArrayOf<ICompressHandler>
{
public:
    ICompressHandler *lookup(const char *type) const
    {
        ForEachItemIn(h, *this)
        {
            ICompressHandler &handler = item(h);
            if (0 == stricmp(type, handler.queryType()))
                return &handler;
        }
        return NULL;
    }
} compressors;


bool addCompressorHandler(ICompressHandler *handler)
{
    if (compressors.lookup(handler->queryType()))
    {
        handler->Release();
        return false; // already registered
    }
    compressors.append(* handler);
    return true;
}

bool removeCompressorHandler(ICompressHandler *handler)
{
    return compressors.zap(* handler);
}

Linked<ICompressHandler> defaultCompressor;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    class CCompressHandlerBase : public CInterface, implements ICompressHandler
    {
        StringAttr type;
    public:
        IMPLEMENT_IINTERFACE;
        CCompressHandlerBase(const char *_type) : type(_type) { }
    // ICompressHandler
        virtual const char *queryType() const { return type; }
    };
    class CFLZCompressHandler : public CCompressHandlerBase
    {
    public:
        CFLZCompressHandler() : CCompressHandlerBase("FLZ") { }
        virtual ICompressor *getCompressor(const char *options) { return createFastLZCompressor(); }
        virtual IExpander *getExpander(const char *options) { return createFastLZExpander(); }
    };
    class CAESCompressHandler : public CCompressHandlerBase
    {
    public:
        CAESCompressHandler() : CCompressHandlerBase("AES") { }
        virtual ICompressor *getCompressor(const char *options)
        {
            assertex(options);
            return createAESCompressor(options, strlen(options));
        }
        virtual IExpander *getExpander(const char *options)
        {
            assertex(options);
            return createAESExpander(options, strlen(options));
        }
    };
    class CDiffCompressHandler : public CCompressHandlerBase
    {
    public:
        CDiffCompressHandler() : CCompressHandlerBase("DIFF") { }
        virtual ICompressor *getCompressor(const char *options) { return createRDiffCompressor(); }
        virtual IExpander *getExpander(const char *options) { return createRDiffExpander(); }
    };
    class CLZWCompressHandler : public CCompressHandlerBase
    {
    public:
        CLZWCompressHandler() : CCompressHandlerBase("LZW") { }
        virtual ICompressor *getCompressor(const char *options) { return createLZWCompressor(true); }
        virtual IExpander *getExpander(const char *options) { return createLZWExpander(true); }
    };
    ICompressHandler *flzCompressor = new CFLZCompressHandler();
    addCompressorHandler(flzCompressor);
    addCompressorHandler(new CAESCompressHandler());
    addCompressorHandler(new CDiffCompressHandler());
    addCompressorHandler(new CLZWCompressHandler());
    defaultCompressor.set(flzCompressor);
    return true;
}

ICompressHandler *queryCompressHandler(const char *type)
{
    return compressors.lookup(type);
}

void setDefaultCompressor(const char *type)
{
    ICompressHandler *_defaultCompressor = queryCompressHandler(type);
    if (!_defaultCompressor)
        throw MakeStringException(-1, "setDefaultCompressor: '%s' compressor not registered", type);
    defaultCompressor.set(_defaultCompressor);
}

ICompressHandler *queryDefaultCompressHandler()
{
    return defaultCompressor;
}

ICompressor *getCompressor(const char *type, const char *options)
{
    ICompressHandler *handler = compressors.lookup(type);
    if (handler)
        return handler->getCompressor(options);
    return NULL;
}

IExpander *getExpander(const char *type, const char *options)
{
    ICompressHandler *handler = compressors.lookup(type);
    if (handler)
        return handler->getExpander(options);
    return NULL;
}



//===================================================================================

//#define TEST_ROWDIFF
#ifdef TEST_ROWDIFF

#include "jfile.hpp"

jlib_decl void testDiffComp(unsigned amount)
{
    size32_t sz = 11;

    Owned<IWriteSeqVar> out = createRowCompWriteSeq("test.out", sz);

    { MTIME_SECTION(defaultTimer, "Comp Write");
        int cpies;
        for (cpies=0; cpies<amount; cpies++)
        {
            out->putn("Kate cccc \0A Another \0A Brother ", 3);
            out->putn( "Jake Smith", 1);
            out->putn( "Jake Brown", 1);
            out->putn( "J Smith   ", 1);
            out->putn( "K Smith   ", 1);
            out->putn( "Kate Smith", 1);
            out->putn( "Kate Brown", 1);
            out->putn("Kate aaaa \0Kate bbbb ", 2);
            out->putn("Kate cccc \0A Another \0A Brother ", 3);
            out->putn( "A Brolley ", 1);
        }
    }

    out.clear();


    MemoryBuffer buf;
    char *s = (char *) buf.reserve(sz);

    { MTIME_SECTION(defaultTimer, "Comp read");
        Owned<IReadSeqVar> in = createRowCompReadSeq("test.out", 0, sz);

        count_t a = 0;
        loop
        {
            size32_t tmpSz;
            if (!in->get(sz, s, tmpSz))
                break;
            a++;
//          DBGLOG("Entry: %s", s);
        }
        DBGLOG("read: %d", a);
    }

    { MTIME_SECTION(defaultTimer, "Comp read async std");
        
        Owned<IFile> iFile = createIFile("test.out");
        Owned<IFileAsyncIO> iFileIO = iFile->openAsync(IFOread);
        Owned<IFileIOStream> iFileIOStream = createBufferedAsyncIOStream(iFileIO);
        Owned<IReadSeqVar> in = createRowCompReadSeq(*iFileIOStream, 0, sz);

        count_t a = 0;
        loop
        {
            size32_t tmpSz;
            if (!in->get(sz, s, tmpSz))
                break;
            a++;
//          DBGLOG("Entry: %s", s);
        }
        DBGLOG("async std read: %d", a);
    }

    { MTIME_SECTION(defaultTimer, "Comp read async");
        
        Owned<IReadSeqVar> in = createRowCompReadSeq("test.out", 0, sz, -1, true);

        count_t a = 0;
        loop
        {
            size32_t tmpSz;
            if (!in->get(sz, s, tmpSz))
                break;
            a++;
//          DBGLOG("Entry: %s", s);
        }
        DBGLOG("async read: %d", a);
    }
}
#endif
