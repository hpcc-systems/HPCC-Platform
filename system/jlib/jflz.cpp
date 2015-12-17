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


/*  
  FastLZ - lightning-fast lossless compression library

  Copyright (C) 2007 Ariya Hidayat (ariya@kde.org)
  Copyright (C) 2006 Ariya Hidayat (ariya@kde.org)
  Copyright (C) 2005 Ariya Hidayat (ariya@kde.org)

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
*/


#if !defined(FASTLZ_COMPRESSOR) && !defined(FASTLZ_DECOMPRESSOR)

// adapted for jlib
#include "platform.h"

#include "jflz.hpp"

#include "jcrc.hpp"

/*
 * Always check for bound when decompressing.
 * Generally it is best to leave it defined.
 */
#define FASTLZ_SAFE

/*
 * Give hints to the compiler for branch prediction optimization.
 */
#if defined(__GNUC__) && (__GNUC__ > 2)
#define FASTLZ_EXPECT_CONDITIONAL(c)    (__builtin_expect((c), 1))
#define FASTLZ_UNEXPECT_CONDITIONAL(c)  (__builtin_expect((c), 0))
#else
#define FASTLZ_EXPECT_CONDITIONAL(c)    (c)
#define FASTLZ_UNEXPECT_CONDITIONAL(c)  (c)
#endif

/*
 * Use inlined functions for supported systems.
 */
#if defined(__GNUC__) || defined(__DMC__) || defined(__POCC__) || defined(__WATCOMC__) || defined(__SUNPRO_C)
#define FASTLZ_INLINE inline
#elif defined(__BORLANDC__) || defined(_MSC_VER) || defined(__LCC__)
#define FASTLZ_INLINE __inline
#else 
#define FASTLZ_INLINE
#endif

/*
 * Prevent accessing more than 8-bit at once, except on x86 architectures.
 */
#if !defined(FASTLZ_STRICT_ALIGN)
#define FASTLZ_STRICT_ALIGN
#if defined(__i386__) || defined(__386)  /* GNU C, Sun Studio */
#undef FASTLZ_STRICT_ALIGN
#elif defined(__i486__) || defined(__i586__) || defined(__i686__) /* GNU C */
#undef FASTLZ_STRICT_ALIGN
#elif defined(_M_IX86) /* Intel, MSVC */
#undef FASTLZ_STRICT_ALIGN
#elif defined(__386)
#undef FASTLZ_STRICT_ALIGN
#elif defined(_X86_) /* MinGW */
#undef FASTLZ_STRICT_ALIGN
#elif defined(__I86__) /* Digital Mars */
#undef FASTLZ_STRICT_ALIGN
#endif
#endif

/*
 * FIXME: use preprocessor magic to set this on different platforms!
 */
typedef byte           flzuint8;
typedef unsigned short flzuint16;
typedef unsigned int   flzuint32;

/* prototypes */
//int fastlz_compress(const void* input, int length, void* output);
//int fastlz_compress_level(int level, const void* input, int length, void* output);
//int fastlz_decompress(const void* input, int length, void* output, int maxout);

#define MAX_COPY       32
#define MAX_LEN       264  /* 256 + 8 */
#define MAX_DISTANCE 8192

#if !defined(FASTLZ_STRICT_ALIGN)
#define FASTLZ_READU16(p) *((const flzuint16*)(p)) 
#else
#define FASTLZ_READU16(p) ((p)[0] | (p)[1]<<8)
#endif

#define HASH_LOG  13
#define HASH_SIZE (1<< HASH_LOG)
#define HASH_MASK  (HASH_SIZE-1)
#define HASH_FUNCTION(v,p) { v = FASTLZ_READU16(p); v ^= FASTLZ_READU16(p+1)^(v>>(16-HASH_LOG));v &= HASH_MASK; }

typedef const flzuint8* HTAB_T[HASH_SIZE];

#undef FASTLZ_LEVEL
#define FASTLZ_LEVEL 1


#undef FASTLZ_COMPRESSOR
#undef FASTLZ_DECOMPRESSOR
#define FASTLZ_COMPRESSOR fastlz1_compress
#define FASTLZ_DECOMPRESSOR fastlz1_decompress
static FASTLZ_INLINE int FASTLZ_COMPRESSOR(const void* input, int length, void* output, HTAB_T &htab);
static FASTLZ_INLINE int FASTLZ_DECOMPRESSOR(const void* input, int length, void* output, int maxout);
#include "jflz.cpp"

#undef FASTLZ_LEVEL
#define FASTLZ_LEVEL 2

#undef MAX_DISTANCE
#define MAX_DISTANCE 8191
#define MAX_FARDISTANCE (65535+MAX_DISTANCE-1)

#undef FASTLZ_COMPRESSOR
#undef FASTLZ_DECOMPRESSOR
#define FASTLZ_COMPRESSOR fastlz2_compress
#define FASTLZ_DECOMPRESSOR fastlz2_decompress
static FASTLZ_INLINE int FASTLZ_COMPRESSOR(const void* input, int length, void* output, HTAB_T &htab);
static FASTLZ_INLINE int FASTLZ_DECOMPRESSOR(const void* input, int length, void* output, int maxout);
#include "jflz.cpp"

#define FASTLZ__JLIBCOMPRESSOR 1

inline size32_t fastlz_compress(const void* input, size32_t length, void* output, HTAB_T &htab)
{
  /* for short block, choose fastlz1 */
  if(length < 65536)
    return fastlz1_compress(input, length, output, htab);

  /* else... */
  return fastlz2_compress(input, length, output, htab);
}

size32_t fastlz_compress(const void* input, size32_t length, void* output)
{
    MemoryAttr ma;
    HTAB_T *ht = (HTAB_T *)ma.allocate(sizeof(HTAB_T)); // HTAB_T too big for stack really
    return fastlz_compress(input,length,output,*ht);
}

size32_t fastlz_decompress(const void* input, size32_t length, void* output, size32_t maxout)
{
  /* magic identifier for compression level */
  int level = ((*(const flzuint8*)input) >> 5) + 1;

  if(level == 1)
    return fastlz1_decompress(input, length, output, maxout);
  if(level == 2)
    return fastlz2_decompress(input, length, output, maxout);

  /* unknown level, trigger error */
  return 0;
}

int fastlz_compress_level(int level, const void* input, size32_t length, void* output)
{
  MemoryAttr ma;
  HTAB_T *ht = (HTAB_T *)ma.allocate(sizeof(HTAB_T)); // HTAB_T too big for stack really
  if(level == 1)
    return fastlz1_compress(input, length, output, *ht);
  if(level == 2)
    return fastlz2_compress(input, length, output, *ht);

  return 0;
}

#else /* !defined(FASTLZ_COMPRESSOR) && !defined(FASTLZ_DECOMPRESSOR) */

static FASTLZ_INLINE int FASTLZ_COMPRESSOR(const void* input, int length, void* output, HTAB_T &htab)
{
  const flzuint8* ip = (const flzuint8*) input;
  const flzuint8* ip_bound = ip + length - 2;
  const flzuint8* ip_limit = ip + length - 12;
  flzuint8* op = (flzuint8*) output;

  const flzuint8** hslot;
  flzuint32 hval;

  flzuint32 copy;

  /* sanity check */
  if(FASTLZ_UNEXPECT_CONDITIONAL(length < 4))
  {
    if(length)
    {
      /* create literal copy only */
      *op++ = length-1;
      ip_bound++;
      while(ip <= ip_bound)
        *op++ = *ip++;
      return length+1;
    }
    else
      return 0;
  }

  /* initializes hash table */
  for (hslot = htab; hslot < htab + HASH_SIZE; hslot++)
    *hslot = ip;

  /* we start with literal copy */
  copy = 2;
  *op++ = MAX_COPY-1;
  *op++ = *ip++;
  *op++ = *ip++;

  /* main loop */
  while(FASTLZ_EXPECT_CONDITIONAL(ip < ip_limit))
  {
    const flzuint8* ref;
    flzuint32 distance;

    /* minimum match length */
    flzuint32 len = 3;

    /* comparison starting-point */
    const flzuint8* anchor = ip;

    /* check for a run */
#if FASTLZ_LEVEL==2
    if(ip[0] == ip[-1] && FASTLZ_READU16(ip-1)==FASTLZ_READU16(ip+1))
    {
      distance = 1;
      ip += 3;
      ref = anchor - 1 + 3;
      goto match;
    }
#endif

    /* find potential match */
    HASH_FUNCTION(hval,ip);
    hslot = htab + hval;
    ref = htab[hval];

    /* calculate distance to the match */
    distance = anchor - ref;

    /* update hash table */
    *hslot = anchor;

    /* is this a match? check the first 3 bytes */
    if(distance==0 || 
#if FASTLZ_LEVEL==1
    (distance >= MAX_DISTANCE) ||
#else
    (distance >= MAX_FARDISTANCE) ||
#endif
    *ref++ != *ip++ || *ref++!=*ip++ || *ref++!=*ip++)
      goto literal;

#if FASTLZ_LEVEL==2
    /* far, needs at least 5-byte match */
    if(distance >= MAX_DISTANCE)
    {
      if(*ip++ != *ref++ || *ip++!= *ref++) 
        goto literal;
      len += 2;
    }
    
    match:
#endif

    /* last matched byte */
    ip = anchor + len;

    /* distance is biased */
    distance--;

    if(!distance)
    {
      /* zero distance means a run */
      flzuint8 x = ip[-1];
      while(ip < ip_bound)
        if(*ref++ != x) break; else ip++;
    }
    else
    for(;;)
    {
      /* safe because the outer check against ip limit */
      if(*ref++ != *ip++) break;
      if(*ref++ != *ip++) break;
      if(*ref++ != *ip++) break;
      if(*ref++ != *ip++) break;
      if(*ref++ != *ip++) break;
      if(*ref++ != *ip++) break;
      if(*ref++ != *ip++) break;
      if(*ref++ != *ip++) break;
      while(ip < ip_bound)
        if(*ref++ != *ip++) break;
      break;
    }

    /* if we have copied something, adjust the copy count */
    if(copy)
      /* copy is biased, '0' means 1 byte copy */
      *(op-copy-1) = copy-1;
    else
      /* back, to overwrite the copy count */
      op--;

    /* reset literal counter */
    copy = 0;

    /* length is biased, '1' means a match of 3 bytes */
    ip -= 3;
    len = ip - anchor;

    /* encode the match */
#if FASTLZ_LEVEL==2
    if(distance < MAX_DISTANCE)
    {
      if(len < 7)
      {
        *op++ = (len << 5) + (distance >> 8);
        *op++ = (distance & 255);
      }
      else
      {
        *op++ = (7 << 5) + (distance >> 8);
        for(len-=7; len >= 255; len-= 255)
          *op++ = 255;
        *op++ = len;
        *op++ = (distance & 255);
      }
    }
    else
    {
      /* far away, but not yet in the another galaxy... */
      if(len < 7)
      {
        distance -= MAX_DISTANCE;
        *op++ = (len << 5) + 31;
        *op++ = 255;
        *op++ = distance >> 8;
        *op++ = distance & 255;
      }
      else
      {
        distance -= MAX_DISTANCE;
        *op++ = (7 << 5) + 31;
        for(len-=7; len >= 255; len-= 255)
          *op++ = 255;
        *op++ = len;
        *op++ = 255;
        *op++ = distance >> 8;
        *op++ = distance & 255;
      }
    }
#else

    if(FASTLZ_UNEXPECT_CONDITIONAL(len > MAX_LEN-2))
      while(len > MAX_LEN-2)
      {
        *op++ = (7 << 5) + (distance >> 8);
        *op++ = MAX_LEN - 2 - 7 -2; 
        *op++ = (distance & 255);
        len -= MAX_LEN-2;
      }

    if(len < 7)
    {
      *op++ = (len << 5) + (distance >> 8);
      *op++ = (distance & 255);
    }
    else
    {
      *op++ = (7 << 5) + (distance >> 8);
      *op++ = len - 7;
      *op++ = (distance & 255);
    }
#endif

    /* update the hash at match boundary */
    HASH_FUNCTION(hval,ip);
    htab[hval] = ip++;
    HASH_FUNCTION(hval,ip);
    htab[hval] = ip++;

    /* assuming literal copy */
    *op++ = MAX_COPY-1;

    continue;

    literal:
      *op++ = *anchor++;
      ip = anchor;
      copy++;
      if(FASTLZ_UNEXPECT_CONDITIONAL(copy == MAX_COPY))
      {
        copy = 0;
        *op++ = MAX_COPY-1;
      }
  }

  /* left-over as literal copy */
  ip_bound++;
  while(ip <= ip_bound)
  {
    *op++ = *ip++;
    copy++;
    if(copy == MAX_COPY)
    {
      copy = 0;
      *op++ = MAX_COPY-1;
    }
  }

  /* if we have copied something, adjust the copy length */
  if(copy)
    *(op-copy-1) = copy-1;
  else
    op--;

#if FASTLZ_LEVEL==2
  /* marker for fastlz2 */
  *(flzuint8*)output |= (1 << 5);
#endif

  return op - (flzuint8*)output;
}

static FASTLZ_INLINE int FASTLZ_DECOMPRESSOR(const void* input, int length, void* output, int maxout)
{
  const flzuint8* ip = (const flzuint8*) input;
  const flzuint8* ip_limit  = ip + length;
  flzuint8* op = (flzuint8*) output;
  flzuint8* op_limit = op + maxout;
  flzuint32 ctrl = (*ip++) & 31;
  int loopidx = 1;

  do
  {
    const flzuint8* ref = op;
    flzuint32 len = ctrl >> 5;
    flzuint32 ofs = (ctrl & 31) << 8;

    if(ctrl >= 32)
    {
#if FASTLZ_LEVEL==2
      flzuint8 code;
#endif
      len--;
      ref -= ofs;
      if (len == 7-1)
#if FASTLZ_LEVEL==1
        len += *ip++;
      ref -= *ip++;
#else
        do
        {
          code = *ip++;
          len += code;
        } while (code==255);
      code = *ip++;
      ref -= code;

      /* match from 16-bit distance */
      if(FASTLZ_UNEXPECT_CONDITIONAL(code==255))
      if(FASTLZ_EXPECT_CONDITIONAL(ofs==(31 << 8)))
      {
        ofs = (*ip++) << 8;
        ofs += *ip++;
        ref = op - ofs - MAX_DISTANCE;
      }
#endif
      
#ifdef FASTLZ_SAFE
      if (FASTLZ_UNEXPECT_CONDITIONAL(op + len + 3 > op_limit))
        return 0;

      if (FASTLZ_UNEXPECT_CONDITIONAL(ref-1 < (flzuint8 *)output))
        return 0;
#endif

      if(FASTLZ_EXPECT_CONDITIONAL(ip < ip_limit))
        ctrl = *ip++;
      else
        loopidx = 0;

      if(ref == op)
      {
        /* optimize copy for a run */
        flzuint8 b = ref[-1];
        *op++ = b;
        *op++ = b;
        *op++ = b;
        for(; len; --len)
          *op++ = b;
      }
      else
      {
#if !defined(FASTLZ_STRICT_ALIGN)
        const flzuint16* p;
        flzuint16* q;
#endif
        /* copy from reference */
        ref--;
        *op++ = *ref++;
        *op++ = *ref++;
        *op++ = *ref++;

#if !defined(FASTLZ_STRICT_ALIGN)
        /* copy a byte, so that now it's word aligned */
        if(len & 1)
        {
          *op++ = *ref++;
          len--;
        }

        /* copy 16-bit at once */
        q = (flzuint16*) op;
        op += len;
        p = (const flzuint16*) ref;
        for(len>>=1; len > 4; len-=4)
        {
          *q++ = *p++;
          *q++ = *p++;
          *q++ = *p++;
          *q++ = *p++;
        }
        for(; len; --len)
          *q++ = *p++;
#else
        for(; len; --len)
          *op++ = *ref++;
#endif
      }
    }
    else
    {
      ctrl++;
#ifdef FASTLZ_SAFE
      if (FASTLZ_UNEXPECT_CONDITIONAL(op + ctrl > op_limit))
        return 0;
      if (FASTLZ_UNEXPECT_CONDITIONAL(ip + ctrl > ip_limit))
        return 0;
#endif

      *op++ = *ip++; 
      for(--ctrl; ctrl; ctrl--)
        *op++ = *ip++;

      loopidx = FASTLZ_EXPECT_CONDITIONAL(ip < ip_limit);
      if(loopidx)
        ctrl = *ip++;
    }
  }
  while(FASTLZ_EXPECT_CONDITIONAL(loopidx));

  return op - (flzuint8*)output;
}

#undef FASTLZ__JLIBCOMPRESSOR  // avoid being compiled twice!!

#endif /* !defined(FASTLZ_COMPRESSOR) && !defined(FASTLZ_DECOMPRESSOR) */

#if defined(FASTLZ__JLIBCOMPRESSOR)


#define COMMITTED ((size32_t)-1)

/* Format:
    size32_t totalexpsize;
    { size32_t subcmpsize; bytes subcmpdata; }
    size32_t trailsize; bytes traildata;    // unexpanded
*/


class jlib_decl CFastLZCompressor : public CInterface, public ICompressor
{
    HTAB_T ht;
    size32_t blksz;
    size32_t bufalloc;
    MemoryBuffer inma;      // equals blksize len
    MemoryBuffer *outBufMb; // used when dynamic output buffer (when open() used)
    size32_t outBufStart;
    byte *inbuf;
    size32_t inmax;         // remaining
    size32_t inlen;
    size32_t inlenblk;      // set to COMMITTED when so
    bool trailing;
    byte *outbuf;
    size32_t outlen;
    size32_t wrmax;
    size32_t dynamicOutSz;

    inline void setinmax()
    {
        inmax = blksz-outlen-sizeof(size32_t);
        if (inmax<256)
            trailing = true;    // too small to bother compressing
        else {
            trailing = false;
            size32_t slack = inmax/17;
            if (slack<66)
                slack = 66;
            inmax -= slack+sizeof(size32_t);
        }
    }

    inline void flushcommitted()
    {
        // only does non trailing
        if (trailing)
            return;
        size32_t toflush = (inlenblk==COMMITTED)?inlen:inlenblk;
        if (toflush == 0)
            return;
        size32_t outSzRequired = outlen+sizeof(size32_t)*2+toflush+fastlzSlack(toflush);
        if (!dynamicOutSz)
            assertex(outSzRequired<=blksz);
        else
        {
            if (outSzRequired>dynamicOutSz)
            {
                verifyex(outBufMb->ensureCapacity(outBufStart+outSzRequired));
                dynamicOutSz = outBufMb->capacity();
                outbuf = ((byte *)outBufMb->bufferBase()+outBufStart);
            }
        }
        size32_t *cmpsize = (size32_t *)(outbuf+outlen);
        byte *out = (byte *)(cmpsize+1);
        *cmpsize = (size32_t)fastlz_compress(inbuf, (int)toflush, out, ht);
        if (*cmpsize<toflush)
        {
            *(size32_t *)outbuf += toflush;
            outlen += *cmpsize+sizeof(size32_t);
            if (inlenblk==COMMITTED)
                inlen = 0;
            else
            {
                inlen -= inlenblk;
                memmove(inbuf,inbuf+toflush,inlen);
            }
            setinmax();
            return;
        }
        trailing = true;
    }

    void initCommon()
    {
        blksz = inma.capacity();
        *(size32_t *)outbuf = 0;
        outlen = sizeof(size32_t);
        inlen = 0;
        inlenblk = COMMITTED;
        setinmax();
    }
public:
    IMPLEMENT_IINTERFACE;

    CFastLZCompressor()
    {
        outlen = 0;
        outbuf = NULL;      // only set on close
        bufalloc = 0;
        wrmax = 0;          // set at open
        dynamicOutSz = 0;
        outBufMb = NULL;
        outBufStart = 0;
        inbuf = NULL;
    }

    virtual ~CFastLZCompressor()
    {
        if (bufalloc)
            free(outbuf);
    }


    virtual void open(void *buf,size32_t max)
    {
        if (max<1024)
            throw MakeStringException(-1,"CFastLZCompressor::open - block size (%d) not large enough", blksz);
        wrmax = max;
        if (buf)
        {
            if (bufalloc)
                free(outbuf);
            bufalloc = 0;
            outbuf = (byte *)buf;
        }
        else if (max>bufalloc)
        {
            if (bufalloc)
                free(outbuf);
            bufalloc = max;
            outbuf = (byte *)malloc(bufalloc);
        }
        outBufMb = NULL;
        outBufStart = 0;
        dynamicOutSz = 0;
        inbuf = (byte *)inma.ensureCapacity(max);
        initCommon();
    }

    virtual void open(MemoryBuffer &mb, size32_t initialSize)
    {
        if (!initialSize)
            initialSize = 0x100000; // 1MB
        if (initialSize<1024)
            throw MakeStringException(-1,"CFastLZCompressor::open - block size (%d) not large enough", initialSize);
        wrmax = initialSize;
        if (bufalloc)
        {
            free(outbuf);
            bufalloc = 0;
        }
        inbuf = (byte *)inma.ensureCapacity(initialSize);
        outBufMb = &mb;
        outBufStart = mb.length();
        outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
        dynamicOutSz = outBufMb->capacity();
        initCommon();
    }

    virtual void close()
    {
        if (inlenblk!=COMMITTED) {
            inlen = inlenblk; // transaction failed
            inlenblk = COMMITTED;
        }
        flushcommitted();
        size32_t totlen = outlen+sizeof(size32_t)+inlen;
        assertex(blksz>=totlen);
        size32_t *tsize = (size32_t *)(outbuf+outlen);
        *tsize = inlen;
        memcpy(tsize+1,inbuf,inlen);
        outlen = totlen;
        *(size32_t *)outbuf += inlen;
        inbuf = NULL;
        if (outBufMb)
        {
            outBufMb->setWritePos(outBufStart+outlen);
            outBufMb = NULL;
        }
    }


    size32_t write(const void *buf,size32_t len)
    {
        // no more than wrmax per write (unless dynamically sizing)
        size32_t lenb = wrmax;
        byte *b = (byte *)buf;
        size32_t written = 0;
        while (len)
        {
            if (len < lenb)
                lenb = len;
            if (lenb+inlen>inmax)
            {
                if (trailing)
                    return written;
                flushcommitted();
                if (lenb+inlen>inmax)
                {
                    if (outBufMb) // sizing input buffer, but outBufMb!=NULL is condition of whether in use or not
                    {
                        blksz += len > 0x100000 ? len : 0x100000;
                        verifyex(inma.ensureCapacity(blksz));
                        blksz = inma.capacity();
                        inbuf = (byte *)inma.bufferBase();
                        wrmax = blksz;
                        setinmax();
                    }
                    lenb = inmax-inlen;
                    if (len < lenb)
                        lenb = len;
                }
            }
            if (lenb == 0)
                return written;
            memcpy(inbuf+inlen,b,lenb);
            b += lenb;
            inlen += lenb;
            len -= lenb;
            written += lenb;
        }
        return written;
    }

    void *  bufptr() 
    { 
        assertex(!inbuf);  // i.e. closed
        return outbuf;
    }
    size32_t    buflen() 
    { 
        assertex(!inbuf);  // i.e. closed
        return outlen;
    }
    void    startblock()
    {
        inlenblk = inlen;
    }
    void commitblock()
    {
        inlenblk = COMMITTED;
    }


};


class jlib_decl CFastLZExpander : public CInterface, public IExpander
{

    byte *outbuf;
    size32_t outlen;
    size32_t bufalloc;
    const size32_t *in;  

public:
    IMPLEMENT_IINTERFACE;

    CFastLZExpander()
    {
        outbuf = NULL;
        outlen = 0;
        bufalloc = 0;
    }
    ~CFastLZExpander()
    {
        if (bufalloc)
            free(outbuf);

    }

    virtual size32_t  init(const void *blk)
    {
        const size32_t *expsz = (const size32_t *)blk;
        outlen = *expsz;
        in = (expsz+1);
        return outlen;
    }

    virtual void expand(void *buf)
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
                throw MakeStringException(MSGAUD_operator,0, "Out of memory in FastLZExpander::expand, requesting %d bytes", bufalloc);
        }
        size32_t done = 0;
        loop {
            const size32_t szchunk = *in;
            in++;
            if (szchunk+done<outlen) {
                size32_t written = fastlz_decompress(in,szchunk,(byte *)buf+done,outlen-done);
                done += written;
                if (!written||(done>outlen))
                    throw MakeStringException(0, "FastLZExpander - corrupt data(1) %d %d",written,szchunk);
            }
            else {
                if (szchunk+done!=outlen)
                    throw MakeStringException(0, "FastLZExpander - corrupt data(2) %d %d",szchunk,outlen);
                memcpy((byte *)buf+done,in,szchunk);
                break;
            }
            in = (const size32_t *)(((const byte *)in)+szchunk);
        }
    }

    virtual void *bufptr() { return outbuf;}
    virtual size32_t   buflen() { return outlen;}
};

void fastLZCompressToBuffer(MemoryBuffer & out, size32_t len, const void * src)
{
    size32_t outbase = out.length();
    size32_t *sz = (size32_t *)out.reserve(len+fastlzSlack(len)+sizeof(size32_t)*2);
    *sz = len;
    sz++;
    *sz = (len>16)?fastlz_compress(src, (int)len, sz+1):16;
    if (*sz>=len) {
        *sz = len;
        memcpy(sz+1,src,len);
    }
    out.setLength(outbase+*sz+sizeof(size32_t)*2);
}

void fastLZDecompressToBuffer(MemoryBuffer & out, const void * src)
{
    size32_t *sz = (size32_t *)src;
    size32_t expsz = *(sz++);
    size32_t cmpsz = *(sz++);
    void *o = out.reserve(expsz);
    if (cmpsz!=expsz) {
        size32_t written = fastlz_decompress(sz,cmpsz,o,expsz);
        if (written!=expsz)
            throw MakeStringException(0, "fastLZDecompressToBuffer - corrupt data(1) %d %d",written,expsz);
    }
    else
        memcpy(o,sz,expsz);
}

void fastLZDecompressToBuffer(MemoryBuffer & out, MemoryBuffer & in)
{
    size32_t expsz;
    size32_t cmpsz;
    in.read(expsz).read(cmpsz);
    void *o = out.reserve(expsz);
    if (cmpsz!=expsz) {
        size32_t written = fastlz_decompress(in.readDirect(cmpsz),cmpsz,o,expsz);
        if (written!=expsz)
            throw MakeStringException(0, "fastLZDecompressToBuffer - corrupt data(3) %d %d",written,expsz);
    }
    else
        memcpy(o,in.readDirect(cmpsz),expsz);
}

void fastLZDecompressToAttr(MemoryAttr & out, const void * src)
{
    size32_t *sz = (size32_t *)src;
    size32_t expsz = *(sz++);
    size32_t cmpsz = *(sz++);
    void *o = out.allocate(expsz);
    if (cmpsz!=expsz) {
        size32_t written = fastlz_decompress(sz,cmpsz,o,expsz);
        if (written!=expsz)
            throw MakeStringException(0, "fastLZDecompressToBuffer - corrupt data(2) %d %d",written,expsz);
    }
    else
        memcpy(o,sz,expsz);
}

void fastLZDecompressToBuffer(MemoryAttr & out, MemoryBuffer & in)
{
    size32_t expsz;
    size32_t cmpsz;
    in.read(expsz).read(cmpsz);
    void *o = out.allocate(expsz);
    if (cmpsz!=expsz) {
        size32_t written = fastlz_decompress(in.readDirect(cmpsz),cmpsz,o,expsz);
        if (written!=expsz)
            throw MakeStringException(0, "fastLZDecompressToBuffer - corrupt data(4) %d %d",written,expsz);
    }
    else
        memcpy(o,in.readDirect(cmpsz),expsz);
}


ICompressor *createFastLZCompressor()
{
    return new CFastLZCompressor;
}

IExpander *createFastLZExpander()
{
    return new CFastLZExpander;
}

#define FLZ_BUFFER_SIZE (0x100000)

static const __uint64 FLZCOMPRESSEDFILEFLAG = U64C(0xc3518de42f15da57);

struct FlzCompressedFileTrailer
{
    offset_t        zfill1;             // must be first
    offset_t        expandedSize;
    __uint64        compressedType;
    unsigned        zfill2;             // must be last
};


class CFastLZStream : public CInterface, implements IFileIOStream
{
    Linked<IFileIO> baseio;
    offset_t expOffset;     // expanded offset
    offset_t cmpOffset;     // compressed offset in file
    bool reading;
    MemoryAttr ma;
    size32_t bufsize;
    size32_t bufpos;        // reading only
    offset_t expSize;

    bool load()
    {
        bufpos = 0;
        bufsize = 0;
        if (expOffset==expSize)
            return false;
        size32_t sz[2];
        if (baseio->read(cmpOffset,sizeof(size32_t)*2,&sz)!=sizeof(size32_t)*2)
            return false;
        bufsize = sz[0];
        if (!bufsize)
            return false;
        cmpOffset += sizeof(size32_t)*2;
        if (ma.length()<bufsize)
            ma.allocate(bufsize);
        MemoryAttr cmpma;
        byte *cmpbuf = (byte *)cmpma.allocate(sz[1]);
        if (baseio->read(cmpOffset,sz[1],cmpbuf)!=sz[1])
            throw MakeStringException(-1,"CFastLZStream: file corrupt.1");
        if (fastlz_decompress(cmpbuf,sz[1],ma.bufferBase(),bufsize)!=bufsize)
            throw MakeStringException(-1,"CFastLZStream: file corrupt.2");
        cmpOffset += sz[1];
        return true;
    }

    void save()
    {
        if (bufsize) {
            MemoryAttr dstma;
            byte *dst = (byte *)dstma.allocate(sizeof(size32_t)*2+bufsize+fastlzSlack(bufsize));
            size32_t sz = fastlz_compress(ma.get(),bufsize,sizeof(size32_t)*2+dst);
            memcpy(dst,&bufsize,sizeof(size32_t));
            memcpy(dst+sizeof(size32_t),&sz,sizeof(size32_t));
            baseio->write(cmpOffset,sz+sizeof(size32_t)*2,dst);
            cmpOffset += sz+sizeof(size32_t)*2;
        }
        bufsize = 0;
    }


public:
    IMPLEMENT_IINTERFACE;

    CFastLZStream() 
    {
        expOffset = 0;
        cmpOffset = 0;
        reading = true;
        bufpos = 0;
        bufsize = 0;
    }

    ~CFastLZStream()
    {
        flush();
    }

    bool attach(IFileIO *_baseio)
    {
        baseio.set(_baseio);
        expOffset = 0;
        cmpOffset = 0;
        reading = true;
        bufpos = 0;
        bufsize = 0;

        FlzCompressedFileTrailer trailer;
        offset_t filesize = baseio->size();
        if (filesize<sizeof(trailer))
            return false;
        baseio->read(filesize-sizeof(trailer),sizeof(trailer),&trailer);
        expSize = trailer.expandedSize;
        return trailer.compressedType==FLZCOMPRESSEDFILEFLAG;
    }

    void create(IFileIO *_baseio)
    {
        baseio.set(_baseio);
        expOffset = 0;
        cmpOffset = 0;
        reading = false;
        bufpos = 0;
        bufsize = 0;
        ma.allocate(FLZ_BUFFER_SIZE);
        expSize = (offset_t)-1;
    }

    void seek(offset_t pos, IFSmode origin)
    {
        if ((origin==IFScurrent)&&(pos==0))
            return;
        if ((origin==IFSbegin)||(pos!=0))
            throw MakeStringException(-1,"CFastLZStream seek not supported");
        expOffset = 0;
        bufpos = 0;
        bufsize = 0;
    }

    offset_t size()
    {
        return (expSize==(offset_t)-1)?0:expSize;
    }

    offset_t tell()
    {
        return expOffset;
    }


    size32_t read(size32_t len, void * data)
    {
        if (!reading)
            throw MakeStringException(-1,"CFastLZStream read to stream being written");
        size32_t ret=0;
        while (len) {
            size32_t cpy = bufsize-bufpos;
            if (!cpy) {
                if (!load())
                    break;
                cpy = bufsize-bufpos;
            }
            if (cpy>len)
                cpy = len;
            memcpy(data,(const byte *)ma.get()+bufpos,cpy);
            bufpos += cpy;
            len -= cpy;
            ret += cpy;
        }
        expOffset += ret;
        return ret;
    }

    size32_t write(size32_t len, const void * data)
    {
        if (reading)
            throw MakeStringException(-1,"CFastLZStream write to stream being read");
        size32_t ret = len;
        while (len+bufsize>FLZ_BUFFER_SIZE) {
            size32_t cpy = FLZ_BUFFER_SIZE-bufsize;
            memcpy((byte *)ma.bufferBase()+bufsize,data,cpy);
            data = (const byte *)data+cpy;
            len -= cpy;
            bufsize = FLZ_BUFFER_SIZE;
            save();
        }
        memcpy((byte *)ma.bufferBase()+bufsize,data,len);
        bufsize += len;
        expOffset += len;
        return ret;
    }

    void flush()
    {
        if (!reading&&(expSize!=expOffset)) {
            save();
            FlzCompressedFileTrailer trailer;
            memset(&trailer,0,sizeof(trailer));
            trailer.compressedType = FLZCOMPRESSEDFILEFLAG;
            trailer.expandedSize = expOffset;
            baseio->write(cmpOffset,sizeof(trailer),&trailer);
            expSize = expOffset;
        }
    }

};



IFileIOStream *createFastLZStreamRead(IFileIO *base)
{
    Owned<CFastLZStream> strm = new CFastLZStream();
    if (strm->attach(base))
        return strm.getClear();
    return NULL;
}

IFileIOStream *createFastLZStreamWrite(IFileIO *base)
{
    Owned<CFastLZStream> strm = new CFastLZStream();
    strm->create(base);
    return strm.getClear();
}


#endif


