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
#include "jlz4.hpp"
#include "jzstd.hpp"
#include "jplane.hpp"

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

static std::atomic<bool> disableZeroSizeCompressedFiles{false};
static std::atomic<bool> initialisedCompressionOptions{false};

static bool allowZeroSizeCompressedFiles()
{
    if (!initialisedCompressionOptions)
    {
        // NB: potentially could be >1 thread here, but that's ok.
        try
        {
            Owned<IPropertyTree> globalConfig = getGlobalConfigSP();
            Owned<IPropertyTree> config = getComponentConfigSP();
            disableZeroSizeCompressedFiles = config->getPropBool("expert/@disableZeroSizeCompressedFiles", globalConfig->getPropBool("expert/@disableZeroSizeCompressedFiles", disableZeroSizeCompressedFiles));
        }
        catch (IException *e) // handle cases where config. not available
        {
            EXCLOG(e, "allowZeroSizeCompressedFiles");
            e->Release();
        }
        initialisedCompressionOptions = true;
    }
    return !disableZeroSizeCompressedFiles.load();
}


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
    inuseflag=0xff;
    supportbigendian = _supportbigendian;
    outBufStart = 0;
    outBufMb = NULL;
}

CLZWCompressor::~CLZWCompressor()
{
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

void CLZWCompressor::open(MemoryBuffer &mb, size32_t initialSize, size32_t fixedRowSize)
{
    allowPartialWrites = false; // buffer is always expanded to fit
    outBufMb = &mb;
    outBufStart = mb.length();
    outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
    maxlen = outBufMb->capacity()-SAFETY_MARGIN;
    initCommon();
}

void CLZWCompressor::open(void *buf,size32_t max, size32_t fixedRowSize, bool _allowPartialWrites)
{
    assertex(buf);
#ifdef STATS
    st_thistime = msTick();
    st_thiswrites=0;
#endif
    originalMax = max;
    allowPartialWrites = _allowPartialWrites;

    outbuf = buf;
    outBufMb = NULL;
    if (max<=SAFETY_MARGIN+sizeof(size32_t)) // minimum required
        throw makeStringException(0, "CLZWCompressor: target buffer too small");
    maxlen=max-SAFETY_MARGIN;
    initCommon();
}



#define HASHC(code,ch) (((fnvPrime32*(unsigned)code)^(unsigned char)ch)%LZW_HASH_TABLE_SIZE)

#define BE_MEMCPY4(dst,src)     { if (supportbigendian) _WINCPYREV4(dst,src); else memcpy(dst,src,4); }


size32_t CLZWCompressor::write(const void *buf,size32_t buflen)
{
    if (!buflen)
        return 0;
    if (!dict.curbits)
        return 0;

    size32_t savedInLen = inlen;
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
                        if (allowPartialWrites)
                        {
                            ret = in-(unsigned char *)buf-1;
                            inlen += in-(unsigned char *)buf-1;
                        }
                        else
                        {
                            inlen = savedInLen;
                            ret = 0;
                        }
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

bool CLZWCompressor::adjustLimit(size32_t newLimit)
{
    assertex(!outBufMb);       // Only supported when a fixed size buffer is provided
    assertex(newLimit <= originalMax);

    if (newLimit < SAFETY_MARGIN + outlen)
        return false;
    maxlen = newLimit - SAFETY_MARGIN;
    return true;
}

void CLZWCompressor::close()
{
    if (dict.curbits)
    {
        PUTCODE(curcode);
        flushbuf();
        dict.curbits = 0;
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


size32_t CExpanderBase::expandFirst(MemoryBuffer & target, const void * src)
{
    size32_t size = init(src);
    void * buffer = target.reserve(size);
    expand(buffer);
    return size;
}

size32_t CExpanderBase::expandNext(MemoryBuffer & target)
{
    return 0;
}

size32_t CExpanderBase::expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src)
{
    throwUnimplemented();
}

bool CExpanderBase::supportsBlockDecompression() const
{
    return false;
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
    for (;;) {
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
    for (;;) {
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

void compressToBuffer(MemoryBuffer & out, size32_t len, const void * src, CompressionMethod method, const char *options)
{
    if (method != COMPRESS_METHOD_NONE && len >= 32)
    {
        ICompressHandler *handler = queryCompressHandler(method);
        if (!handler)
        {
            VStringBuffer s("Unknown compression method %x requested in compressToBuffer", (byte) method);
            throw makeStringException(0, s.str());
        }
        unsigned originalLength = out.length();
        // For back-compatibility, we always store COMPRESS_METHOD_LZW_LITTLE_ENDIAN as 1 as earlier versions stored a boolean here
        // rather than an enum
        // This means that compressToBuffer/decompressToBuffer cannot bs used for rowdiff compression - this is not likely to be an issue
        // Alternative would be a separate enum for compressToBuffer formats, but that seems more likely to cause confusion
        out.append((byte) (method == COMPRESS_METHOD_LZW_LITTLE_ENDIAN ? COMPRESS_METHOD_LZWLEGACY : method));
        out.append((size32_t)0);
        size32_t newSize = len * 4 / 5; // Copy if compresses less than 80% ...
        Owned<ICompressor> compressor = handler->getCompressor(options);
        void *newData = out.reserve(newSize);
        if (compressor->supportsBlockCompression())
        {
            size32_t compressedLen = compressor->compressBlock(newSize, newData, len, src);
            if (compressedLen != 0)
            {
                out.setWritePos(originalLength + sizeof(byte));
                out.append(compressedLen);
                out.setWritePos(originalLength + sizeof(byte) + sizeof(size32_t) + compressedLen);
                return;
            }
        }
        else
        {
            try
            {
                compressor->open(newData, newSize, 0, false);
                if (compressor->write(src, len)==len)
                {
                    compressor->close();
                    size32_t compressedLen = compressor->buflen();
                    out.setWritePos(originalLength + sizeof(byte));
                    out.append(compressedLen);
                    out.setWritePos(originalLength + sizeof(byte) + sizeof(size32_t) + compressedLen);
                    return;
                }
            }
            catch (IException *E)
            {
                E->Release();
            }
        }
        // failed to compress...
        out.setWritePos(originalLength);
    }
    out.append((byte) COMPRESS_METHOD_NONE);
    out.append(len);
    out.append(len, src);
}

void decompressToBuffer(MemoryBuffer & out, MemoryBuffer & in, const char *options)
{
    size32_t srcLen;
    unsigned char _method;
    in.read(_method).read(srcLen);
    CompressionMethod method = (CompressionMethod) _method;
    if (method==COMPRESS_METHOD_NONE)
        out.append(srcLen, in.readDirect(srcLen));
    else
    {
        if (method==COMPRESS_METHOD_LZWLEGACY)
            method = COMPRESS_METHOD_LZW_LITTLE_ENDIAN;    // Back compatibilty
        ICompressHandler *handler = queryCompressHandler(method);
        if (!handler)
        {
            VStringBuffer s("Unknown decompression method %x required in decompressToBuffer", (byte) method);
            throw makeStringException(0, s.str());
        }
        Owned<IExpander> expander = handler->getExpander(options);
        unsigned outSize = expander->init(in.readDirect(srcLen));
        void * buff = out.reserve(outSize);
        expander->expand(buff);
    }
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



size32_t DiffCompress(const void *src,void *dst, void *buff,size32_t rs)
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
        for (;;) {
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
        for (;;) {
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

class jlib_decl CRDiffCompressor : public ICompressor, public CInterface
{
    size32_t inlen;
    size32_t outlen;
    size32_t remaining;
    size32_t originalMax = 0;
    void *outbuf;
    unsigned char *out;
    MemoryBuffer *outBufMb;
    size32_t outBufStart;

    size32_t recsize;       // assumed fixed length rows
    size32_t maxrecsize;  // maximum size diff compress 
    MemoryAttr prev;
    bool allowPartialWrites{true};  // Rows cannot be partial, but can blocks of rows?

    bool isFirstRow() const
    {
        return outlen == sizeof(size32_t)*2;
    }

    void initCommon(size32_t fixedRowSize)
    {
        inlen = 0;
        memset(outbuf, 0, sizeof(size32_t)*2);
        outlen = sizeof(size32_t)*2;
        out = (byte *)outbuf+outlen;
        recsize = fixedRowSize;
        maxrecsize = maxcompsize(recsize);
        prev.allocate(fixedRowSize);
        allowPartialWrites = true;
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
        outBufMb = NULL;
    }

    virtual void open(MemoryBuffer &mb, size32_t initialSize, size32_t fixedRowSize) override
    {
        if (fixedRowSize == 0)
            throw makeStringException(-1, "CRDiffCompressor used with variable sized row");

        allowPartialWrites = false; // buffer is always expanded to fit
        outBufMb = &mb;
        outBufStart = mb.length();
        outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
        initCommon(fixedRowSize);
        remaining = outBufMb->capacity()-outlen;
    }

    virtual void open(void *buf,size32_t max, size32_t fixedRowSize, bool _allowPartialWrites) override
    {
        assertex(buf);
        if (fixedRowSize == 0)
            throw makeStringException(-1, "CRDiffCompressor used with variable sized row");

        originalMax = max;
        allowPartialWrites = _allowPartialWrites;
        outbuf = buf;
        outBufMb = NULL;
        if (max<=2+sizeof(size32_t)*2) // minimum required (actually will need enough for recsize so only a guess)
            throw makeStringException(0, "CRDiffCompressor: target buffer too small");
        initCommon(fixedRowSize);
        remaining = max-outlen;
    }

    virtual void close() override
    {
        memcpy(outbuf,&inlen,sizeof(inlen));        // expanded size
        memcpy((byte *)outbuf+sizeof(inlen),&recsize,sizeof(recsize));
        if (outBufMb)
        {
            outBufMb->setWritePos(outBufStart+outlen);
            outBufMb = NULL;
        }
    }

    virtual bool supportsBlockCompression() const override { return false; }
    virtual bool supportsIncrementalCompression() const override { return true; }

    virtual size32_t compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src) override { return 0; }

    virtual size32_t compressDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src, size32_t * numCompressed) override
    {
        throwUnimplemented();
    }

    virtual bool adjustLimit(size32_t newLimit) override
    {
        assertex(!outBufMb);       // Only supported when a fixed size buffer is provided
        assertex(newLimit <= originalMax);

        if (newLimit < outlen + maxrecsize)
            return false;
        remaining = newLimit - outlen;
        return true;
    }

    inline size32_t maxcompsize(size32_t s) { return s+((s+254)/255)*2; }

    virtual size32_t write(const void *buf,size32_t buflen) override
    {
        const byte * cur = (const byte *)buf;
        size32_t savedOutlen = outlen;
        for (size32_t i =0; i < buflen; i += recsize)
        {
            if (unlikely(i+recsize > buflen))
                throw MakeStringException(-1,"CRDiffCompressor used with variable sized row");

            const byte * row = cur+i;
            if (!isFirstRow())
            {
                if (remaining<maxrecsize)
                {
                    if (!outBufMb)
                    {
                        if (allowPartialWrites)
                            return i;
                        outlen = savedOutlen;
                        inlen -= i;
                        return 0;
                    }

                    ensure(maxrecsize-remaining);
                }
                size32_t sz = DiffCompress(row, out, prev.bufferBase(),recsize);
                out += sz;
                outlen += sz;
                remaining -= sz;
            }
            else
            {
                if (remaining<recsize)
                    ensure(recsize);
                memcpy(out,row,recsize);
                out += recsize;
                outlen += recsize;
                remaining -= recsize;
                memcpy(prev.bufferBase(), row, recsize);
            }
            inlen += recsize;
        }
        return buflen;
    }

    virtual void *bufptr() override { return outbuf;}
    virtual size32_t buflen() override { return outlen;}

    virtual CompressionMethod getCompressionMethod() const override { return COMPRESS_METHOD_ROWDIF; }
};


class jlib_decl CRDiffExpander : public CExpanderBase
{
    unsigned char *outbuf;
    size32_t outlen;
    size32_t bufalloc;
    unsigned char *in;
    size32_t recsize;
public:
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



class jlib_decl CRandRDiffCompressor : public ICompressor, public CInterface
{
    size32_t inlen;
    size32_t max;
    size32_t originalMax = 0;
    void *outbuf;
    RRDheader *header;
    MemoryBuffer diffbuf;
    MemoryBuffer firstrec;
    MemoryAttr firstrle;
    size32_t maxdiffsize;
    size32_t recsize;
    size32_t outBufStart;
    MemoryBuffer *outBufMb;
    bool finished = false;
    bool allowPartialWrites{true};

    void initCommon()
    {
        header = (RRDheader *)outbuf;
        inlen = 0;
        memset(header,0,MIN_RRDHEADER_SIZE);
        diffbuf.clear();
        firstrec.clear();
        firstrle.clear();
        finished = false;
        allowPartialWrites = true;
    }
public:
    IMPLEMENT_IINTERFACE;
    CRandRDiffCompressor()
    {
        outbuf = NULL;
        header = NULL;
        max = 0;
        maxdiffsize = 0;
        recsize = 0;
        outBufStart = 0;
        outBufMb = NULL;
    }
        
    virtual void open(MemoryBuffer &mb, size32_t initialSize, size32_t fixedRowSize) override
    {
        if (fixedRowSize == 0)
            throw makeStringException(-1, "CRandRDiffCompressor used with variable sized row");

        allowPartialWrites = false; // buffer is always expanded to fit
        outBufMb = &mb;
        outBufStart = mb.length();
        outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
        recsize = fixedRowSize;
        initCommon();
    }

    virtual void open(void *buf,size32_t _max, size32_t fixedRowSize, bool _allowPartialWrites) override
    {
        assertex(buf);
        if (fixedRowSize == 0)
            throw makeStringException(-1, "CRandRDiffCompressor used with variable sized row");

        max = _max;
        originalMax = max;
        allowPartialWrites = _allowPartialWrites;
        outbuf = buf;
        outBufMb = NULL;
        recsize = fixedRowSize;
        if (max<=MIN_RRDHEADER_SIZE+sizeof(unsigned short)+3) // hopefully a lot bigger!
            throw makeStringException(0, "CRandRDiffCompressor: target buffer too small");
        initCommon();
    }

    virtual void close() override
    {
        if (finished)
            return;
        finished = true;
        header->rowofs[0] = (unsigned short)diffbuf.length();
        assertex((size32_t)(header->totsize+header->firstrlesize)<=max || max == 0);
        unsigned short hofs = header->hsize();
        assertex(header->totsize==hofs+diffbuf.length());
        if (outBufMb)
        {
            outbuf = (byte *)outBufMb->ensureCapacity(header->totsize+header->firstrlesize);
            outBufMb->setWritePos(outBufStart+header->totsize+header->firstrlesize);
            outBufMb = NULL;
        }
        byte *out = (byte *)outbuf+hofs;
        if (diffbuf.length())
        {
            memcpy(out,diffbuf.toByteArray(),diffbuf.length());
            out += diffbuf.length();
            diffbuf.clear();
        }
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

    virtual bool adjustLimit(size32_t newLimit) override
    {
        assertex(!outBufMb);       // Only supported when a fixed size buffer is provided
        assertex(newLimit <= originalMax);

        if (newLimit < header->totsize+sizeof(short)+header->firstrlesize)
            return false;
        max = newLimit;
        return true;
    }

    virtual bool supportsBlockCompression() const override { return false; }
    virtual bool supportsIncrementalCompression() const override { return true; }

    virtual size32_t compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src) override { return 0; }

    virtual size32_t compressDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src, size32_t * numCompressed) override
    {
        throwUnimplemented();
    }

    inline size32_t maxcompsize(size32_t s) { return s+((s+254)/255)*2; }

    virtual size32_t write(const void *buf,size32_t buflen) override
    {
        const byte * cur = (const byte *)buf;
        unsigned originalRows = header->numrows;
        size32_t originalLength = diffbuf.length();
        size32_t originalTotalSize = header->totsize;
        for (size32_t i=0; i < buflen; i += recsize)
        {
            if (i+recsize > buflen)
                throw makeStringExceptionV(-1,"CRandRDiffCompressor used with variable sized row (%u, %u, %u)", recsize, buflen, buflen - i);

            const byte * row = cur+i;

            unsigned nr = header->numrows;
            if (nr)
            {
                size32_t sz = diffbuf.length();
                size32_t compsize = DiffCompress2(row,diffbuf.reserve(maxdiffsize),firstrec.toByteArray(),recsize);
                if (header->totsize+sizeof(short)+compsize+header->firstrlesize>max)
                {
                    if (allowPartialWrites)
                    {
                        diffbuf.setLength(sz);
                        inlen += i;
                        return i;
                    }
                    else
                    {
                        diffbuf.setLength(originalLength);
                        header->numrows = originalRows;
                        header->totsize = originalTotalSize;
                        return 0;
                    }
                }
                header->rowofs[nr] = (unsigned short)sz; // will need to adjust later
                header->numrows = nr + 1;
                assertex(header->numrows != 0); // Check for overflow
                diffbuf.setLength(sz+compsize);
                header->totsize += (unsigned short)compsize+sizeof(unsigned short);
            }
            else
            {
                firstrec.append(recsize,row);
                header->numrows = 1;
                header->totsize = header->hsize(); // don't add in rle size yet
                header->recsize = (unsigned short)recsize;
                maxdiffsize = maxcompsize(recsize);
                size32_t sz = RLECompress(firstrle.allocate(recsize+2),firstrec.toByteArray(),recsize);
                header->firstrlesize = (unsigned short)sz;
            }
        }
        inlen += buflen;
        // This compression should only be used on relatively small blocks of data because
        // header->totsize or header->numrows are only unsigned shorts.
        // Check they have not wrapped - because that will lead to corrupt output.
        assertex(header->totsize >= originalTotalSize);
        assertex(header->numrows >= originalRows);
        return buflen;
    }

    virtual void *bufptr() override { return outbuf;}
    virtual size32_t buflen() override { return header->totsize;}

    virtual CompressionMethod getCompressionMethod() const override { return COMPRESS_METHOD_RANDROW; }
};


class jlib_decl CRandRDiffExpander : public IRandRowExpander, public CInterface
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

//MORE: These should really be unsigned integers - could modify later.
static const __int64 COMPRESSEDFILEFLAG = I64C(0xc0528ce99f10da55);
#define COMPRESSEDFILEBLOCKSIZE (0x10000)
static const __int64 FASTCOMPRESSEDFILEFLAG = I64C(0xc1518de99f10da55);
static const __int64 LZ4COMPRESSEDFILEFLAG = I64C(0xc1200e0b71321c73);
static const __int64 NEWCOMPRESSEDFILEFLAG = I64C(0x686367C3C3D0C880);
static const __int64 NEWCOMPRESSEDFILEFLAGMAX = NEWCOMPRESSEDFILEFLAG + 0x80;  // Allow full enumeration of compression

#pragma pack(push,1)

struct CompressedFileTrailer
{
    unsigned        datacrc;            
    offset_t        expandedSize;
    offset_t        indexPos;       // end of blocks
    size32_t        blockSize;
    size32_t        recordSize;     // 0 is lzw or fastlz or lz4
    __int64         compressedType;
    unsigned        crc;                // must be last
    unsigned numBlocks() { return (unsigned)((indexPos+blockSize-1)/blockSize); }
    unsigned method()
    {
        if (compressedType==FASTCOMPRESSEDFILEFLAG)
            return COMPRESS_METHOD_FASTLZ;
        if (compressedType==LZ4COMPRESSEDFILEFLAG)
            return COMPRESS_METHOD_LZ4;
        if (compressedType==COMPRESSEDFILEFLAG)
        {
            if (recordSize)
                return COMPRESS_METHOD_ROWDIF;
            else
                return COMPRESS_METHOD_LZW;
        }
        if ((compressedType >= NEWCOMPRESSEDFILEFLAG) && (compressedType < NEWCOMPRESSEDFILEFLAGMAX))
        {
            if (compressedType < NEWCOMPRESSEDFILEFLAG + COMPRESS_METHOD_LAST)
                return (unsigned)(compressedType - NEWCOMPRESSEDFILEFLAG);
            throw makeStringExceptionV(-1, "File has compression type %u, which is not supported by this version", (unsigned)(compressedType - NEWCOMPRESSEDFILEFLAG));
        }
        return COMPRESS_METHOD_NONE;
    }

    void setDetails(IPropertyTree &tree)
    {
        tree.setPropInt("@datacrc",datacrc);        
        tree.setPropInt64("@expandedSize",expandedSize);
        tree.setPropInt64("@indexPos",indexPos);
        tree.setPropInt("@blockSize",blockSize);
        tree.setPropInt("@recordSize",recordSize);      // 0 is lzw or fastlz or lz4
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
    size32_t        recordSize;     // 0 is lzw or fastlz or lz4
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

static size32_t countZeros(size32_t size, const byte * data)
{
    size32_t len;
    for (len = 0; len < size; len++)
    {
        if (data[len])
            break;
    }
    return len;
}

//---------------------------------------------------------------------------------------------------------------------

class CCompressedFileBase : implements ICompressedFileIO, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CCompressedFileBase(IFileIO *_fileio, CompressedFileTrailer &_trailer, unsigned bufferSize)
    : fileio(_fileio), trailer(_trailer)
    {
        //Allow the disk read and write to send multiple blocks in a single operation - to reduce cloud io costs.
        assertex(trailer.blockSize);
        numBlocksToBuffer = 1;
        if (fileio && bufferSize == (size32_t)-1)
        {
            IFile * file = fileio->queryFile();
            if (file)
            {
                const char * filename = file->queryFilename();
                if (filename)
                {
                    Owned<const IStoragePlane> plane = getStoragePlaneFromPath(filename, false);
                    if (plane)
                    {
                        bufferSize = plane->getAttribute(BlockedSequentialIO);
                    }
                    else
                    {
                        if (isContainerized())
                            bufferSize = 0x400000; // Default to 4MB if containerized and no plane - so that dafilesrv default to large reads
                    }

                }
            }
        }
        if (bufferSize && (bufferSize != (size32_t)-1))
            numBlocksToBuffer = bufferSize / trailer.blockSize;
        if (numBlocksToBuffer < 1)
            numBlocksToBuffer = 1;
        sizeIoBuffer = trailer.blockSize*numBlocksToBuffer;
    }

    virtual void setSize(offset_t size) override { UNIMPLEMENTED; }
    virtual unsigned __int64 getStatistic(StatisticKind kind) override
    {
        return fileio->getStatistic(kind);
    }

// CCompressedFile impl.
    virtual size32_t recordSize() override
    {
        return trailer.recordSize;
    }
    virtual size32_t blockSize() override
    {
        return trailer.blockSize;
    }
    virtual unsigned method() override
    {
        return trailer.method();
    }
    virtual IFile * queryFile() const override
    {
        if (fileio)
            return fileio->queryFile();
        return nullptr;
    }

protected:
    Linked<IFileIO> fileio;
    CompressedFileTrailer trailer;
    CriticalSection crit;
    size32_t sizeIoBuffer = 0;
    unsigned numBlocksToBuffer = 1; // default to buffering 1 block
};

class CCompressedFileReader final : public CCompressedFileBase
{
    Linked<IMemoryMappedFile> mmfile;
    MemoryBuffer expandedBuffer;    // buffer that contains the expanded input
    MemoryBuffer iobuffer;          // buffer used for reading
    MemoryBuffer indexbuf;          // non-empty once index read
    Owned<IExpander> expander;
    MemoryAttr compressedInputBlock;

    //Each block in the file consists of a sequence of compressed chunks
    offset_t startBlockExpandedPos = (offset_t)-1;  // The offset of the start of the current compressed block
    offset_t startBlockIOPos = 0;   // The offset in the input of the current compressed block
    size32_t fullBlockSize = 0;     // The size of the current compressed block - which contains multiple chunks

    offset_t startChunkExpandedPos = (offset_t)-1;  // The expanded position of the current chunk
    offset_t nextChunkExpandedPos = (offset_t)-1;   // The expanded position of the next chunk (i.e. end of the current chunk)

    // The reader may read multiple uncompressed blocks at once - save the range currently in memory
    offset_t firstIOOffset = 0;     // First offset in the current IO block
    offset_t lastIOOffset = 0;      // Last offset in the current IO block

    unsigned indexNum() { return indexbuf.length()/sizeof(offset_t); }

    unsigned lookupIndex(offset_t pos,offset_t &curpos,size32_t &expsize)
    {
        // NB index starts at block 1 (and has size as last entry)
        const offset_t *index;
        unsigned a = 0;
        unsigned b = indexNum();
        index = (const offset_t *)indexbuf.toByteArray();
        while (b>a) {
            unsigned m = a+(b-a)/2;
            __int64 dif = (__int64)pos-index[m];
            //Do not optimize exact matches - because if there are zero length blocks this needs
            //to return the block that follows
            if (dif >= 0)
                a = m+1;
            else
                b = m;
        }
        curpos=b?index[b-1]:0;
        expsize = (size32_t)(index[b]-curpos);
        return b;
    }

    void * getCompressedData(offset_t offset)
    {
        if (fileio)
        {
            assertex(offset >= firstIOOffset && offset < lastIOOffset);
            return (byte *)iobuffer.bufferBase() + offset - firstIOOffset;
        }
        else
            return mmfile->base()+offset;
    }

    void getblock(offset_t pos)
    {
        expandedBuffer.clear();

        //If the blocks are being expanded incrementally check if the position is within the current block
        //This test will never be true for row compressed data, or non-incremental decompression
        try
        {
            //Is the file position within the current block of chunks?
            if ((pos >= startBlockExpandedPos) && (pos < startBlockExpandedPos + fullBlockSize))
            {
                //Is the file position in a previous chunk - in which case we need to start decompressing the chunks from
                //the beginning
                if (pos < nextChunkExpandedPos)
                {
                    // Start decompressing again from the beginning of the block
                    // and avoid re-reading the data from disk
                    const void * rawData = getCompressedData(startBlockIOPos);
                    assertex(rawData);

                    nextChunkExpandedPos = startBlockExpandedPos; // update in case an exception is thrown
                    size32_t exp = expander->expandFirst(expandedBuffer, rawData);

                    startChunkExpandedPos = startBlockExpandedPos;
                    nextChunkExpandedPos = startBlockExpandedPos + exp;
                    if (pos < nextChunkExpandedPos)
                        return;

                    expandedBuffer.clear();
                }

                for (;;)
                {
                    size32_t nextSize = expander->expandNext(expandedBuffer);
                    if (nextSize == 0)
                        throw makeStringException(-1, "Unexpected zero length compression block");

                    startChunkExpandedPos = nextChunkExpandedPos;
                    nextChunkExpandedPos = nextChunkExpandedPos+nextSize;
                    if (pos < nextChunkExpandedPos)
                        return;
                }
            }
        }
        catch (IException * e)
        {
            unsigned code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg).appendf(" at uncompressed position %llu block %llu of %llu", nextChunkExpandedPos, startBlockIOPos/trailer.blockSize, trailer.indexPos);
            e->Release();
            throw makeStringException(code, msg.str());
        }

        unsigned curblocknum = lookupIndex(pos, startBlockExpandedPos, fullBlockSize);
        size32_t toread = trailer.blockSize;
        offset_t nextIOOffset = (offset_t)curblocknum*toread;
        assertex(nextIOOffset <= trailer.indexPos);
        if (trailer.indexPos-nextIOOffset < (offset_t)toread)
            toread = (size32_t)(trailer.indexPos - nextIOOffset);
        if (!toread) 
            return;

        if (fileio)
        {
            //Only read from the file if the block has not already been read
            if ((nextIOOffset < firstIOOffset) || (nextIOOffset >= lastIOOffset))
            {
                size32_t sizeRead = fileio->read(nextIOOffset, sizeIoBuffer, iobuffer.bufferBase());
                firstIOOffset = nextIOOffset;
                lastIOOffset = firstIOOffset + sizeRead;

                if (sizeRead < toread)
                    throw makeStringException(-1, "Read past end of IO buffer");
            }
            else
            {
                // We always read whole blocks - so this should be guaranteed to be false
                if (nextIOOffset + toread > lastIOOffset)
                    throw makeStringException(-1, "Read past end of IO buffer");
            }
            startBlockIOPos = nextIOOffset;
        }
        else
        {
            assertex((memsize_t)nextIOOffset == nextIOOffset);
        }

        void * rawData = getCompressedData(nextIOOffset);
        expand(rawData, expandedBuffer, nextIOOffset);
    }

    void expand(const void *compbuf,MemoryBuffer &expbuf, offset_t compressedPos)
    {
        try
        {
            size32_t rs = trailer.recordSize;
            if (rs) { // diff expand
                const byte *src = (const byte *)compbuf;
                size32_t expsize = fullBlockSize;
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
            else { // lzw or fastlz or lz4
                assertex(expander.get());
                size32_t exp = expander->expandFirst(expbuf, compbuf);
                if (exp == 0)
                {
                    unsigned numZeros = countZeros(trailer.blockSize, (const byte *)compbuf);
                    if (numZeros >= 16)
                        throw makeStringExceptionV(-1, "Unexpected zero fill in compressed file at position %llu length %u", compressedPos, numZeros);
                }

                startChunkExpandedPos = startBlockExpandedPos;
                nextChunkExpandedPos = startBlockExpandedPos + exp;
            }
        }
        catch (IException * e)
        {
            unsigned code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg).appendf(" at compressed position %llu of %llu", compressedPos, trailer.indexPos);
            e->Release();
            throw makeStringException(code, msg.str());
        }
    }

public:
    CCompressedFileReader(IFileIO *_fileio, IMemoryMappedFile *_mmfile, CompressedFileTrailer &_trailer, IExpander *_expander, CompressionMethod compMethod, unsigned bufferSize)
        : CCompressedFileBase(_fileio, _trailer, bufferSize), mmfile(_mmfile)
    {
        expander.set(_expander);

        //Allocate an io buffer that can be used for reading - not needed if using memory mapped files
        if (fileio)
            iobuffer.ensureCapacity(sizeIoBuffer);

        // Read the index of blocks from the end of the file
        unsigned nb = trailer.numBlocks();
        size32_t toread = sizeof(offset_t)*nb;
        if (fileio)
        {
            //MORE: This could be avoided if the file is only being read sequentially from the start
            //Therefore could delay until it was actually needed (and save an IO operation)
            size32_t r = fileio->read(trailer.indexPos,toread,indexbuf.reserveTruncate(toread));
            assertex(r==toread);
        }
        else
        {
            assertex((memsize_t)trailer.indexPos==trailer.indexPos);
            memcpy(indexbuf.reserveTruncate(toread),mmfile->base()+(memsize_t)trailer.indexPos,toread);
        }

        //Ensure that an expander is set up
        if (trailer.recordSize==0)
        {
            if (!expander)
            {
                ICompressHandler *handler = queryCompressHandler(compMethod);
                if (unlikely(!handler))
                    throw makeStringExceptionV(-1, "Unsupported compression method %u", compMethod);
                expander.setown(handler->getExpander(nullptr));
            }
        }

        // Preallocate the target for the expanded chunks - this should always be big enough
        expandedBuffer.ensureCapacity(trailer.blockSize);
    }

    virtual ~CCompressedFileReader()
    {
        try { close(); }
        catch (IException *e)
        {
            EXCLOG(e, "~CCompressedFile");
            e->Release();
        }
    }

    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        CriticalBlock block(crit);
        size32_t ret=0;
        while (pos<trailer.expandedSize) {
            if ((offset_t)len>trailer.expandedSize-pos)
                len = (size32_t)(trailer.expandedSize-pos);
            if ((pos>=startChunkExpandedPos)&&(pos<nextChunkExpandedPos)) { // see if in current buffer
                size32_t tocopy = (size32_t)(nextChunkExpandedPos-pos);
                if (tocopy>len)
                    tocopy = len;
                memcpy(data,expandedBuffer.toByteArray()+(pos-startChunkExpandedPos),tocopy);
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

    virtual offset_t size() override
    { 
        CriticalBlock block(crit);
        return trailer.expandedSize;
    }

    virtual size32_t write(offset_t pos, size32_t len, const void * data) override
    {
        throwUnexpected();
    }
    virtual void flush() override
    {
        throwUnexpected();
    }
    virtual void close() override
    {
        CriticalBlock block(crit);
        if (fileio)
            fileio->close();
    }

// CCompressedFile impl.
    virtual unsigned dataCRC() override
    {
        return trailer.datacrc;
    }
};

//---------------------------------------------------------------------------------------------------------------------

class CCompressedFileWriter : public CCompressedFileBase
{
    unsigned curblocknum;
    MemoryBuffer iobuffer;          // buffer used for writing to file
    size32_t compblockoffset;       // What offset within the io buffer is the current compression block
    MemoryBuffer indexbuf;          // non-empty once index read (when appending)
    ICFmode mode;
    bool setcrc;
    bool writeException = false;
    Linked<ICompressor> compressor;
    offset_t lastFlushPos = 0;

    void checkedwrite(offset_t pos, size32_t len, const void * data)
    {
        size32_t ret = fileio->write(pos,len,data);
        if (ret!=len)
            throw makeOsException(DISK_FULL_EXCEPTION_CODE,"CCompressedFile::checkedwrite");
        if (setcrc)
            trailer.crc = crc32((const char *)data,len,trailer.crc);
    }

    size32_t compress(const void *expbuf,size32_t len)
    {
        return compressor->write(expbuf, len);
    }

    byte * getCompressionTargetBuffer()
    {
        return (byte *)iobuffer.bufferBase() + compblockoffset;
    }
public:
    CCompressedFileWriter(IFileIO *_fileio,CompressedFileTrailer &_trailer,ICFmode _mode, bool _setcrc,ICompressor *_compressor, unsigned compMethod, unsigned bufferSize)
        : CCompressedFileBase(_fileio, _trailer, bufferSize), mode(_mode), setcrc(_setcrc), compressor(_compressor)
    {
        curblocknum = (unsigned)-1; // relies on wrap

        assertex(trailer.recordSize == 0);

        //Allocate a single io buffer that can be used for reading or writing.
        iobuffer.ensureCapacity(sizeIoBuffer); // over estimate!

        compblockoffset = 0;
        if (!compressor)
        {
            ICompressHandler * compressorFactory = queryCompressHandler((CompressionMethod)compMethod);
            if (!compressorFactory)
                throw makeStringExceptionV(-1, "Unsupported compression method %u", compMethod);

            compressor.setown(compressorFactory->getCompressor(nullptr));
        }

        compressor->open(getCompressionTargetBuffer(), trailer.blockSize, 0, true);

        if (mode == ICFappend)
        {
            //Read the existing index from the file
            unsigned nb = trailer.numBlocks();
            size32_t toread = sizeof(offset_t)*nb;
            size32_t r = fileio->read(trailer.indexPos,toread,indexbuf.reserveTruncate(toread));
            assertex(r==toread);

            curblocknum = nb-1;
            if (setcrc)
            {
                trailer.crc = trailer.datacrc;
                trailer.datacrc = ~0U;
            }
        }
    }

    virtual ~CCompressedFileWriter()
    {
        if (!writeException)
        {
            try { close(); }
            catch (IException *e)
            {
                EXCLOG(e, "~CCompressedFileWriter");
                e->Release();
            }
        }
    }
    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        throwUnexpected();
    }
    virtual offset_t size() override
    {
        CriticalBlock block(crit);
        return trailer.expandedSize;
    }
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override
    {
        CriticalBlock block(crit);
        assertex(mode!=ICFread);
        size32_t ret = 0;
        for (;;) {
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
            doFlush(true, false);
        }
        return ret;
    }
    virtual void flush() override
    {
        doFlush(false, true);
    }
    void doFlush(bool fillThisBlock, bool forceFlushToDisk)
    {
        try
        {
            if (lastFlushPos == trailer.expandedSize) // nothing written since last flush. NB: only sequential writes supported
            {
                if (forceFlushToDisk && (compblockoffset != 0))
                {
                    checkedwrite(trailer.indexPos-compblockoffset,compblockoffset,(byte *)iobuffer.bufferBase());
                    compblockoffset = 0;
                }
                return;
            }
            curblocknum++;
            indexbuf.append((unsigned __int64) trailer.expandedSize);

            offset_t p = ((offset_t)curblocknum)*((offset_t)trailer.blockSize);
            compressor->close();

            size32_t compblklen = compressor->buflen();
            assertex(compblklen != 0);

            //If adding a block and the previous block was not a full block, fill the gap with 0xff
            //This can occur if a compressed file writer has been opened to append to an existing file.
            if (p>trailer.indexPos)
            {
                //Sanity check that this is the first item written to the file - otherwise the following code will
                //need to checkwrite the data before this block - but it should have been filled already.
                assertex(compblockoffset == 0);

                MemoryAttr fill;
                size32_t fl = (size32_t)(p-trailer.indexPos);
                memset(fill.allocate(fl),0xff,fl);
                checkedwrite(trailer.indexPos,fl,fill.get());
            }
            if (fillThisBlock && (trailer.blockSize != compblklen))
            {
                memset(getCompressionTargetBuffer()+compblklen,0xff,trailer.blockSize-compblklen);
                compblklen = trailer.blockSize;
            }

            //Write to disk if this is the last block, or if there is not enough space in the io buffer for the next block
            bool writeToDisk = forceFlushToDisk || (compblockoffset + compblklen + trailer.blockSize > sizeIoBuffer);
            if (writeToDisk)
            {
                checkedwrite(p-compblockoffset,compblockoffset + compblklen,(byte *)iobuffer.bufferBase());
                compblockoffset = 0;
            }
            else
                compblockoffset += compblklen;

            p += compblklen;
            compblklen = 0;

            trailer.indexPos = p;
            compressor->open(getCompressionTargetBuffer(), trailer.blockSize, 0, true);
            lastFlushPos = trailer.expandedSize;
        }
        catch (IException *e)
        {
            writeException = true;
            EXCLOG(e, "CCompressedFile::flush");
            throw;
        }
    }
    virtual void close() override
    {
        CriticalBlock block(crit);
        if (mode!=ICFread) {
            //Ensure that a subsequent close() does not re-enter this code if an exception is thrown
            mode = ICFread;

            //Avoid writing out a header/footer if the file is empty
            if ((trailer.expandedSize != 0) || !allowZeroSizeCompressedFiles())
            {
                //Backward compatibility - force a single index entry, even if the file is empty
                if (trailer.expandedSize == 0)
                    lastFlushPos = (offset_t)-1;
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
            }

            indexbuf.clear();
            if (fileio)
                fileio->close();
        }
        curblocknum = (unsigned)-1; // relies on wrap
    }

// CCompressedFile impl.
    virtual unsigned dataCRC() override
    {
        return trailer.crc;
    }
};

//---------------------------------------------------------------------------------------------------------------------


static CompressionMethod getCompressedMethod(__int64 compressedType)
{
    if (compressedType == COMPRESSEDFILEFLAG)
        return COMPRESS_METHOD_LZW;
    else if (compressedType == FASTCOMPRESSEDFILEFLAG)
        return COMPRESS_METHOD_FASTLZ;
    else if (compressedType == LZ4COMPRESSEDFILEFLAG)
        return COMPRESS_METHOD_LZ4;
    else if ((compressedType >= NEWCOMPRESSEDFILEFLAG) && (compressedType < NEWCOMPRESSEDFILEFLAGMAX))
        return (CompressionMethod)(compressedType - NEWCOMPRESSEDFILEFLAG);
    return COMPRESS_METHOD_NONE;
}

static bool isCompressedType(__int64 compressedType)
{
    return COMPRESS_METHOD_NONE != getCompressedMethod(compressedType);
}

static bool isCompressedFile(IFileIO *iFileIO, CompressedFileTrailer *trailer=nullptr)
{
    if (iFileIO)
    {
        offset_t fsize = iFileIO->size();
        if (fsize>=sizeof(WinCompressedFileTrailer))  // thats 8 bytes bigger but I think doesn't matter
        {
            WinCompressedFileTrailer wintrailer;
            CompressedFileTrailer _trailer;
            if (!trailer)
                trailer = &_trailer;
            if (iFileIO->read(fsize-sizeof(WinCompressedFileTrailer),sizeof(WinCompressedFileTrailer),&wintrailer)==sizeof(WinCompressedFileTrailer))
            {
                wintrailer.translate(*trailer);
                if (isCompressedType(trailer->compressedType))
                    return true;
            }
        }
        else if ((fsize == 0) && trailer)
        {
            //If the file is empty, but we are expecting a compressed file, fill in the trailer with default information
            memset(trailer,0,sizeof(*trailer));
            trailer->crc = ~0U;
            trailer->compressedType = LZ4COMPRESSEDFILEFLAG;
            trailer->blockSize = LZ4COMPRESSEDFILEBLOCKSIZE;
            trailer->recordSize = 0;
            return true;
        }
    }
    return false;
}



bool isCompressedFile(const char *filename)
{
    Owned<IFile> iFile = createIFile(filename);
    return isCompressedFile(iFile);
}

bool isCompressedFile(IFile *iFile)
{
    Owned<IFileIO> iFileIO = iFile->open(IFOread);
    return isCompressedFile(iFileIO);
}

ICompressedFileIO *createCompressedFileReader(IFileIO *fileio,IExpander *expander, size32_t ioBufferSize)
{
    CompressedFileTrailer trailer;
    if (isCompressedFile(fileio, &trailer))
    {
        if (expander&&(trailer.recordSize!=0))
            throw MakeStringException(-1, "Compressed file format error(%d), Encrypted?",trailer.recordSize);

        //MORE: Revisit the compressed io size when the compressed file supports it (post refactoring)
        CompressionMethod compMethod = getCompressedMethod(trailer.compressedType);
        return new CCompressedFileReader(fileio,NULL,trailer,expander,compMethod, ioBufferSize);
    }
    return nullptr;
}


ICompressedFileIO *createCompressedFileReader(IFile *file,IExpander *expander, size32_t ioBufferSize, bool memorymapped, IFEflags extraFlags)
{
    if (file)
    {
        if (memorymapped)
        {
            Owned<IMemoryMappedFile> mmfile = file->openMemoryMapped();
            if (mmfile)
            {
                offset_t fsize = mmfile->fileSize();
                if (fsize>=sizeof(WinCompressedFileTrailer))  // thats 8 bytes bigger but I think doesn't matter
                {
                    WinCompressedFileTrailer wintrailer;
                    CompressedFileTrailer trailer;
                    memcpy(&wintrailer,mmfile->base()+fsize-sizeof(WinCompressedFileTrailer),sizeof(WinCompressedFileTrailer));
                    wintrailer.translate(trailer);
                    CompressionMethod compMethod = getCompressedMethod(trailer.compressedType);
                    if (compMethod)
                    {
                        if (expander&&(trailer.recordSize!=0))
                            throw MakeStringException(-1, "Compressed file format error(%d), Encrypted?",trailer.recordSize);

                        //MORE: Revisit the compressed io size when the compressed file supports it (post refactoring)
                        return new CCompressedFileReader(NULL,mmfile,trailer,expander,compMethod,ioBufferSize);
                    }
                }
            }
        }
        Owned<IFileIO> fileio = file->open(IFOread, extraFlags);
        if (fileio) 
            return createCompressedFileReader(fileio, expander, ioBufferSize);
    }
    return NULL;
}




ICompressedFileIO *createCompressedFileWriter(IFileIO *fileio, bool append, bool _setcrc,ICompressor *compressor, unsigned compMethod, size32_t compressorBlockSize, size32_t bufferSize)
{
    CompressedFileTrailer trailer;
    offset_t fsize = append ? fileio->size() : 0;
    if (fsize)
    {
        for (;;)
        {
            if (fsize>=sizeof(WinCompressedFileTrailer))  // thats 8 bytes bigger but I think doesn't matter
            {
                WinCompressedFileTrailer wintrailer;
                if (fileio->read(fsize-sizeof(WinCompressedFileTrailer),sizeof(WinCompressedFileTrailer),&wintrailer)==sizeof(WinCompressedFileTrailer)) {
                    wintrailer.translate(trailer);
                    if (trailer.recordSize!=0)
                        throw makeStringException(-1, "Appending to a row compressed file is not supported");
                    compMethod = getCompressedMethod(trailer.compressedType);
                    if (compMethod)
                        break;
                }
            }
            throw MakeStringException(-1,"Appending to file that is not compressed");
        }
    }
    else
    {
        memset(&trailer,0,sizeof(trailer));
        trailer.crc = ~0U;
        if (compMethod == COMPRESS_METHOD_NONE)
            compMethod = COMPRESS_METHOD_LZ4;

        //Preserve the legacy compressedType values for backward compatibility
        if (compMethod == COMPRESS_METHOD_FASTLZ)
        {
            trailer.compressedType = FASTCOMPRESSEDFILEFLAG;
            trailer.blockSize = FASTCOMPRESSEDFILEBLOCKSIZE;
        }
        else if ((compMethod == COMPRESS_METHOD_LZ4) || (compMethod == COMPRESS_METHOD_LZ4HC) || (compMethod == COMPRESS_METHOD_LZ4HC3))
        {
            trailer.compressedType = LZ4COMPRESSEDFILEFLAG;
            trailer.blockSize = LZ4COMPRESSEDFILEBLOCKSIZE;
        }
        else if (compMethod == COMPRESS_METHOD_LZW)
        {
            trailer.compressedType = COMPRESSEDFILEFLAG;
            trailer.blockSize = COMPRESSEDFILEBLOCKSIZE;
        }
        else
        {
            trailer.compressedType = NEWCOMPRESSEDFILEFLAG+compMethod;
            trailer.blockSize = 0x100000;
        }

        trailer.recordSize = 0;
        if (compressorBlockSize)
            trailer.blockSize = compressorBlockSize;
    }

    return new CCompressedFileWriter(fileio,trailer,fsize?ICFappend:ICFcreate,_setcrc,compressor,compMethod,bufferSize);
}

ICompressedFileIO *createCompressedFileWriter(IFile *file,bool append,bool _setcrc,ICompressor *compressor, unsigned _compMethod, size32_t compressorBlockSize, size32_t bufferSize, IFEflags extraFlags)
{
    if (file) {
        if (append&&!file->exists())
            append = false;
        Owned<IFileIO> fileio = file->open(append?IFOreadwrite:IFOcreate, extraFlags);
        if (fileio)
            return createCompressedFileWriter(fileio,append,_setcrc,compressor,_compMethod,compressorBlockSize,bufferSize);
    }
    return NULL;
}


//===================================================================================



#define AES_PADDING_SIZE 32


class CAESCompressor : implements ICompressor, public CInterface
{
    Owned<ICompressor> comp;    // base compressor
    MemoryBuffer compattr;      // compressed buffer
    void *outbuf;               // dest
    size32_t outlen;
    size32_t outmax;
    size32_t originalMax = 0;
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

    virtual void open(MemoryBuffer &mb, size32_t initialSize, size32_t fixedRowSize) override
    {
        outlen = 0;
        outmax = initialSize;
        outbuf = NULL;
        outBufMb = &mb;
        comp->open(compattr, initialSize, fixedRowSize);
    }

    virtual void open(void *blk, size32_t blksize, size32_t fixedRowSize, bool allowPartialWrites) override
    {
        assertex(blk);
        outlen = 0;
        outmax = blksize;
        originalMax = blksize;
        outbuf = blk;
        outBufMb = NULL;
        if (blksize <= AES_PADDING_SIZE+sizeof(size32_t))
            throw makeStringException(0, "CAESCompressor: target buffer too small");
        size32_t subsz = blksize-AES_PADDING_SIZE-sizeof(size32_t);
        comp->open(compattr.reserveTruncate(subsz), subsz, fixedRowSize, allowPartialWrites);
    }

    virtual bool adjustLimit(size32_t newLimit) override
    {
        assertex(newLimit <= originalMax);

        if (!comp->adjustLimit(newLimit-AES_PADDING_SIZE-sizeof(size32_t)))
            return false;
        outmax = newLimit;
        return true;
    }

    virtual bool supportsBlockCompression() const override { return false; }
    virtual bool supportsIncrementalCompression() const override { return true; }

    virtual size32_t compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src) override { return 0; }

    virtual size32_t compressDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src, size32_t * numCompressed) override
    {
        throwUnimplemented();
    }

    virtual void close() override
    {
        if (outmax == 0)
            return;

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

    virtual size32_t write(const void *buf,size32_t len) override
    {
        return comp->write(buf,len);
    }

    virtual void * bufptr() override
    {
        assertex(0 == outmax); // i.e. closed
        return outbuf;
    }

    virtual size32_t buflen() override
    {
        assertex(0 == outmax); // i.e. closed
        return outlen;
    }

    virtual CompressionMethod getCompressionMethod() const override { return (CompressionMethod)(COMPRESS_METHOD_AES | comp->getCompressionMethod()); }
};

class CAESExpander : implements CExpanderBase
{
    Owned<IExpander> exp;   // base expander
    MemoryBuffer compbuf;
    MemoryAttr key;
public:
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
        aesDecrypt(key.get(),key.length(),p+sizeof(size32_t),l,compbuf.clear());
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

#define ROTATE_BYTE_LEFT(x, n) (((x) << (n)) | ((x) >> (8 - (n))))


inline void padKey32(byte *keyout,size32_t len, const byte *key)
{
    if (len==0) 
        memset(keyout,0xcc,32);
    else if (len<=32) {
        for (unsigned i=0;i<32;i++)
            keyout[i] = (i<len)?key[i%len]:ROTATE_BYTE_LEFT(key[i%len],i/len);
    }
    else {
        memcpy(keyout,key,32);
        // xor excess rotated
        for (unsigned i=32;i<len;i++) 
            keyout[i%32] ^= ROTATE_BYTE_LEFT(key[i],(i/8)%8);
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
                if (isCompressedType(trailer.compressedType))
                {
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

class CCompressHandlerArray
{
    IArrayOf<ICompressHandler> registered;    // Owns the relevant handler objects
    ICompressHandler *byMethod[COMPRESS_METHOD_LAST] = { nullptr };
    ICompressHandler *AESbyMethod[COMPRESS_METHOD_LAST] = { nullptr };

public:
    ICompressHandler *lookup(const char *type) const
    {
        ForEachItemIn(h, registered)
        {
            ICompressHandler &handler = registered.item(h);
            if (0 == stricmp(type, handler.queryType()))
                return &handler;
        }
        return NULL;
    }
    ICompressHandler *lookup(CompressionMethod method) const
    {
        if ((method & ~COMPRESS_METHOD_AES) >= COMPRESS_METHOD_LAST)
            return nullptr;
        else if (method & COMPRESS_METHOD_AES)
            return AESbyMethod[method & ~COMPRESS_METHOD_AES];
        else
            return byMethod[method];
    }
    ICompressHandlerIterator *getIterator()
    {
        return new ArrayIIteratorOf<IArrayOf<ICompressHandler>, ICompressHandler, ICompressHandlerIterator>(registered);
    }
    bool addCompressor(ICompressHandler *handler)
    {
        CompressionMethod method = handler->queryMethod();
        if (lookup(method))
        {
            handler->Release();
            return false; // already registered
        }
        registered.append(* handler);
        if ((method & ~COMPRESS_METHOD_AES) < COMPRESS_METHOD_LAST)
        {
            if (method & COMPRESS_METHOD_AES)
                AESbyMethod[method & ~COMPRESS_METHOD_AES] = handler;
            else
                byMethod[method] = handler;
        }
        return true;
    }
    bool removeCompressor(ICompressHandler *handler)
    {
        CompressionMethod method = handler->queryMethod();
        if (registered.zap(* handler))
        {
            if ((method & ~COMPRESS_METHOD_AES) < COMPRESS_METHOD_LAST)
            {
                if (method & COMPRESS_METHOD_AES)
                    AESbyMethod[method & ~COMPRESS_METHOD_AES] = nullptr;
                else
                    byMethod[method] = nullptr;
            }
            return true;
        }
        else
            return false;
    }
} compressors;

typedef IIteratorOf<ICompressHandler> ICompressHandlerIterator;

ICompressHandlerIterator *getCompressHandlerIterator()
{
    return compressors.getIterator();
}

bool addCompressorHandler(ICompressHandler *handler)
{
    return compressors.addCompressor(handler);
}

bool removeCompressorHandler(ICompressHandler *handler)
{
    return compressors.removeCompressor(handler);
}

Linked<ICompressHandler> defaultCompressor;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    class CCompressHandlerBase : implements ICompressHandler, public CInterface
    {
    public:
        IMPLEMENT_IINTERFACE;
    };
    class CFLZCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "FLZ"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_FASTLZ; }
        virtual ICompressor *getCompressor(const char *options) override { return createFastLZCompressor(); }
        virtual IExpander *getExpander(const char *options) override { return createFastLZExpander(); }
    };
    class CLZ4CompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "LZ4"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_LZ4; }
        virtual ICompressor *getCompressor(const char *options) override { return createLZ4Compressor(options, false); }
        virtual IExpander *getExpander(const char *options) override { return createLZ4Expander(); }
    };
    class CLZ4HCCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "LZ4HC"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_LZ4HC; }
        virtual ICompressor *getCompressor(const char *options) override { return createLZ4Compressor(options, true); }
        virtual IExpander *getExpander(const char *options) override { return createLZ4Expander(); }
    };
    class CLZ4HC3CompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "LZ4HC3"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_LZ4HC3; }
        virtual ICompressor *getCompressor(const char *options) override {
            StringBuffer opts(options);
            opts.append(',').append("hclevel=3"); // note extra leading comma will be ignored
            return createLZ4Compressor(opts, true);
        }
        virtual IExpander *getExpander(const char *options) override { return createLZ4Expander(); }
    };
    class CLZ4SCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "LZ4S"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_LZ4S; }
        virtual ICompressor *getCompressor(const char *options) override { return createLZ4StreamCompressor(options, false); }
        virtual IExpander *getExpander(const char *options) override { return createLZ4StreamExpander(); }
    };
    class CLZ4SHCCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "LZ4SHC"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_LZ4SHC; }
        virtual ICompressor *getCompressor(const char *options) override { return createLZ4StreamCompressor(options, true); }
        virtual IExpander *getExpander(const char *options) override { return createLZ4StreamExpander(); }
    };
    class CZStdSCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "ZSTDS"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_ZSTDS; }
        virtual ICompressor *getCompressor(const char *options) override { return createZStdStreamCompressor(options); }
        virtual IExpander *getExpander(const char *options) override { return createZStdStreamExpander(); }
    };
    class CZStdCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "ZSTD"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_ZSTD; }
        virtual ICompressor *getCompressor(const char *options) override { return createZStdCompressor(options); }
        virtual IExpander *getExpander(const char *options) override { return createZStdExpander(); }
    };
    class CAESCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "AES"; }
        virtual CompressionMethod queryMethod() const override { return (CompressionMethod) (COMPRESS_METHOD_AES|COMPRESS_METHOD_LZW); }
        virtual ICompressor *getCompressor(const char *options) override
        {
            assertex(options);
            return createAESCompressor(options, strlen(options));
        }
        virtual IExpander *getExpander(const char *options) override
        {
            assertex(options);
            return createAESExpander(options, strlen(options));
        }
    };
    class CDiffCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "DIFF"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_ROWDIF; }
        virtual ICompressor *getCompressor(const char *options) override { return createRDiffCompressor(); }
        virtual IExpander *getExpander(const char *options) override { return createRDiffExpander(); }
    };
    class CRDiffCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "RDIFF"; }  // Synonym for DIFF
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_ROWDIF; }
        virtual ICompressor *getCompressor(const char *options) override { return createRDiffCompressor(); }
        virtual IExpander *getExpander(const char *options) override { return createRDiffExpander(); }
    };
    class CRandRDiffCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "RANDROW"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_RANDROW; }
        virtual ICompressor *getCompressor(const char *options) override { return createRandRDiffCompressor(); }
        virtual IExpander *getExpander(const char *options) override { UNIMPLEMENTED; } // Expander has a different interface
    };
    class CLZWCompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "LZW"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_LZW; }
        virtual ICompressor *getCompressor(const char *options) override { return createLZWCompressor(true); }
        virtual IExpander *getExpander(const char *options) override { return createLZWExpander(true); }
    };
    class CLZWLECompressHandler : public CCompressHandlerBase
    {
    public:
        virtual const char *queryType() const override { return "LZWLE"; }
        virtual CompressionMethod queryMethod() const override { return COMPRESS_METHOD_LZW_LITTLE_ENDIAN; }
        virtual ICompressor *getCompressor(const char *options) override { return createLZWCompressor(false); }
        virtual IExpander *getExpander(const char *options) override { return createLZWExpander(false); }
    };
    addCompressorHandler(new CLZWLECompressHandler());
    addCompressorHandler(new CLZWCompressHandler());
    addCompressorHandler(new CAESCompressHandler());
    addCompressorHandler(new CDiffCompressHandler());
    addCompressorHandler(new CRDiffCompressHandler());
    addCompressorHandler(new CRandRDiffCompressHandler());
    addCompressorHandler(new CFLZCompressHandler());
    addCompressorHandler(new CLZ4HCCompressHandler());
    addCompressorHandler(new CLZ4HC3CompressHandler());
    ICompressHandler *lz4Compressor = new CLZ4CompressHandler();
    defaultCompressor.set(lz4Compressor);
    addCompressorHandler(lz4Compressor);
    addCompressorHandler(new CLZ4SCompressHandler());
    addCompressorHandler(new CLZ4SHCCompressHandler());
    addCompressorHandler(new CZStdSCompressHandler());
    addCompressorHandler(new CZStdCompressHandler());
    return true;
}

ICompressHandler *queryCompressHandler(const char *type)
{
    return compressors.lookup(type);
}

ICompressHandler *queryCompressHandler(CompressionMethod method)
{
    return compressors.lookup(method);
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



CompressionMethod translateToCompMethod(const char *compStr, CompressionMethod defaultMethod)
{
    CompressionMethod compMethod = defaultMethod;
    if (!isEmptyString(compStr))
    {
        if (strieq("FLZ", compStr))
            compMethod = COMPRESS_METHOD_FASTLZ;
        else if (strieq("LZW", compStr))
            compMethod = COMPRESS_METHOD_LZW;
        else if (strieq("RDIFF", compStr))
            compMethod = COMPRESS_METHOD_ROWDIF;
        else if (strieq("RANDROW", compStr))
            compMethod = COMPRESS_METHOD_RANDROW;
        else if (strieq("LZMA", compStr))
            compMethod = COMPRESS_METHOD_LZMA;
        else if (strieq("LZ4HC", compStr))
            compMethod = COMPRESS_METHOD_LZ4HC;
        else if (strieq("LZ4HC3", compStr))
            compMethod = COMPRESS_METHOD_LZ4HC3;
        else if (strieq("LZ4", compStr))
            compMethod = COMPRESS_METHOD_LZ4;
        else if (strieq("LZ4SHC", compStr))
            compMethod = COMPRESS_METHOD_LZ4SHC;
        else if (strieq("LZ4S", compStr))
            compMethod = COMPRESS_METHOD_LZ4S;
        else if (strieq("ZSTDS", compStr))
            compMethod = COMPRESS_METHOD_ZSTDS;
        else if (strieq("ZSTD", compStr))
            compMethod = COMPRESS_METHOD_ZSTD;
        //else // default is LZ4
    }
    return compMethod;
}

const char *translateFromCompMethod(unsigned compMethod)
{
    switch (compMethod)
    {
        case COMPRESS_METHOD_ROWDIF:
            return "RDIFF";
        case COMPRESS_METHOD_RANDROW:
            return "RANDROW";
        case COMPRESS_METHOD_LZW:
            return "LZW";
        case COMPRESS_METHOD_FASTLZ:
            return "FLZ";
        case COMPRESS_METHOD_LZ4:
            return "LZ4";
        case COMPRESS_METHOD_LZ4HC:
            return "LZ4HC";
        case COMPRESS_METHOD_LZ4HC3:
            return "LZ4HC3";
        case COMPRESS_METHOD_LZ4S:
            return "LZ4S";
        case COMPRESS_METHOD_LZ4SHC:
            return "LZ4SHC";
        case COMPRESS_METHOD_LZMA:
            return "LZMA";
        case COMPRESS_METHOD_ZSTDS:
            return "ZSTDS";
        case COMPRESS_METHOD_ZSTD:
            return "ZSTD";
        default:
            return ""; // none
    }
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
        for (;;)
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
        for (;;)
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
        for (;;)
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
