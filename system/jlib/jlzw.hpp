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



#ifndef JLZW_INCL
#define JLZW_INCL

#include "jiface.hpp"
#include "jfile.hpp"
#include <stdio.h>

enum CompressionMethod : byte 
{
    //These values are persisted - they should not be changed
    COMPRESS_METHOD_NONE,
    COMPRESS_METHOD_ROWDIF,
    COMPRESS_METHOD_LZW,
    COMPRESS_METHOD_FASTLZ,
    COMPRESS_METHOD_LZMA,
    COMPRESS_METHOD_LZ4,
    COMPRESS_METHOD_LZ4HC,
    COMPRESS_METHOD_RANDROW,
    COMPRESS_METHOD_LZW_LITTLE_ENDIAN,
    COMPRESS_METHOD_LZ4S,
    COMPRESS_METHOD_LZ4SHC,
    COMPRESS_METHOD_LZ4HC3,
    COMPRESS_METHOD_ZSTDS,
    COMPRESS_METHOD_ZSTD,
    COMPRESS_METHOD_LAST,


    COMPRESS_METHOD_AES = 0x80,
    COMPRESS_METHOD_DEFAULT = 0xFF,
    COMPRESS_METHOD_LZWLEGACY = 1,  // Matches value of boolean 'true' used to indicate LZW little endian compression by legacy compressToBuffer
};


interface jlib_decl ICompressor : public IInterface
{
    virtual void   open(MemoryBuffer &mb, size32_t initialSize, size32_t fixedRowSize) = 0; // variable internally sized buffer
    virtual void   open(void *blk, size32_t blksize, size32_t fixedRowSize, bool allowPartialWrites) = 0;              // fixed size output
    virtual void   close()=0;
    virtual size32_t write(const void *buf,size32_t len)=0;
    virtual size32_t compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src) = 0;
     // Like compressBlock, but adds no internal header.  If numCompressed is not null, compress as much as will fit
    virtual size32_t compressDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src, size32_t * numCompressed) = 0;
                                                                            
    virtual void * bufptr()=0;
    virtual size32_t buflen()=0;

    virtual bool adjustLimit(size32_t newLimit) = 0;    // adjust the maximum size of a fixed size output buffer
    virtual CompressionMethod getCompressionMethod() const = 0;
    virtual bool supportsBlockCompression() const = 0;
    virtual bool supportsIncrementalCompression() const = 0;
};

interface jlib_decl IExpander : public IInterface
{
    virtual size32_t init(const void *blk)=0; // returns size required
    virtual void   expand(void *target)=0;
    virtual void * bufptr()=0;
    virtual size32_t buflen()=0;
    virtual size32_t expandFirst(MemoryBuffer & target, const void * src) = 0;
    virtual size32_t expandNext(MemoryBuffer & target) = 0;
    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) = 0;
    virtual bool supportsBlockDecompression() const = 0;
};


interface jlib_decl IRandRowExpander : public IInterface
{
    static inline bool isRand(const void *blk) { return *((const unsigned short *)blk+1)==0xffff; }
    virtual bool init(const void *blk,bool copy=true)=0;
    virtual size32_t rowSize() const = 0;       
    virtual unsigned numRows() const = 0;
    virtual bool expandRow(void *target,unsigned index) const =0;
    virtual size32_t expandRow(void *target,unsigned index,size32_t ofs,size32_t sz) const =0;
    virtual const byte *firstRow() const = 0;
    virtual int cmpRow(const void *target,unsigned index,size32_t ofs=0,size32_t sz=(size32_t)-1) const =0;
};


class jlib_decl CExpanderBase : public CInterfaceOf<IExpander>
{
public:
    //Provide default implementations
    virtual size32_t expandFirst(MemoryBuffer & target, const void * src) override;
    virtual size32_t expandNext(MemoryBuffer & target) override;
    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) override;
    virtual bool supportsBlockDecompression() const override;
};


extern jlib_decl ICompressor *createLZWCompressor(bool supportbigendian); // bigendiansupport required for cross platform with solaris
extern jlib_decl IExpander *createLZWExpander(bool supportbigendian);

#define RLEMAXOVERHEAD 2
extern jlib_decl size32_t RLECompress(void *dst,const void *src,size32_t size);  // maximum will write is 2+size
extern jlib_decl size32_t RLEExpand(void *dst,const void *src,size32_t expsize); // returns amount read, expsize must be provided


extern jlib_decl size32_t DiffCompressFirst(const void *src,void *dst,void *buff,size32_t rs);  // compress first row (actually make bigger, but in same format as compression)
                                                                                            // buf need not be initialized
extern jlib_decl size32_t DiffCompress(const void *src,void *dst, void *buff,size32_t rs);  // compress subsequent rows (bufs set by previous DiffFirstCompress or DiffCompress
extern jlib_decl size32_t DiffCompress2(const void *src,void *dst,const void *prev,size32_t rs);// compress row (prev not updated)
extern jlib_decl size32_t DiffExpand(const void *src,void *dst,const void *prev,size32_t rs);   // expand row, prev must be passed previous expanded row 
extern jlib_decl size32_t DiffCompressedSize(const void *cmpressedsrc,size32_t rs);             // calculate compressed row size - rs is expanded size
inline size32_t MaxDiffCompressedRowSize (size32_t rowsize) { return rowsize+((rowsize+254)/255)*2; }


extern jlib_decl ICompressor *createRDiffCompressor(); // NB only supports transaction mode one row per transaction and fixed row size
extern jlib_decl IExpander *createRDiffExpander(); // NB only supports transaction mode one row per transaction and fixed row size

extern jlib_decl ICompressor *createRandRDiffCompressor(); // similar to RDiffCompressor except rows can be expanded individually
extern jlib_decl IRandRowExpander *createRandRDiffExpander(); // NB only supports fixed row size


// Helper functions to make it easy to compress/decompress to memorybuffers.
extern jlib_decl void compressToBuffer(MemoryBuffer & out, size32_t len, const void * src, CompressionMethod method=COMPRESS_METHOD_LZW_LITTLE_ENDIAN, const char *options=nullptr);
extern jlib_decl void decompressToBuffer(MemoryBuffer & out, MemoryBuffer & in, const char *options=nullptr);


interface ICompressedFileIO: extends IFileIO
{
    virtual unsigned dataCRC()=0;                   // CRC for data area (note total file CRC equals COMPRESSEDFILECRC)
    virtual size32_t recordSize()=0;                // 0 for lzw/fastlz, otherwise record length for row difference compression
    virtual size32_t blockSize()=0;                 // block size used
    virtual unsigned method()=0;
};

constexpr size32_t useDefaultIoBufferSize = (size32_t)-1;
extern jlib_decl bool isCompressedFile(const char *filename);
extern jlib_decl bool isCompressedFile(IFile *file);
extern jlib_decl ICompressedFileIO *createCompressedFileReader(IFile *file,IExpander *expander, size32_t ioBufferSize, bool memorymapped, IFEflags extraFlags);
extern jlib_decl ICompressedFileIO *createCompressedFileReader(IFileIO *fileio,IExpander *expander, size32_t ioBufferSize);
extern jlib_decl ICompressedFileIO *createCompressedFileWriter(IFileIO *fileio, bool append, bool setcrc=true, ICompressor *compressor=NULL, unsigned compMethod=COMPRESS_METHOD_LZ4, size32_t compressorBlockSize=0, size32_t bufferSize=(size32_t)-1);
extern jlib_decl ICompressedFileIO *createCompressedFileWriter(IFile *file, bool append, bool setcrc, ICompressor *compressor, unsigned compMethod, size32_t compressorBlockSize, size32_t bufferSize, IFEflags extraFlags);

#define COMPRESSEDFILECRC (~0U)

extern jlib_decl ICompressor *createAESCompressor(const void *key, unsigned keylen=0); // keylen>0 must be 16, 24, or 32 Bytes, if keylen=0 key is padded to 32bytes
extern jlib_decl IExpander *createAESExpander(const void *key, unsigned keylen=0);   // keylen>0 must be 16, 24, or 32 Bytes, if keylen=0 key is padded to 32bytes

extern jlib_decl ICompressor *createAESCompressor256(size32_t len, const void *key); // 256bit key
extern jlib_decl IExpander *createAESExpander256(size32_t len, const void *key);     // key is ascii and is padded

interface IPropertyTree;
extern jlib_decl IPropertyTree *getBlockedFileDetails(IFile *file);


interface ICompressHandler : extends IInterface
{
    virtual const char *queryType() const = 0;
    virtual CompressionMethod queryMethod() const = 0;
    virtual ICompressor *getCompressor(const char *options=NULL) = 0;
    virtual IExpander *getExpander(const char *options=NULL) = 0;
};
typedef IIteratorOf<ICompressHandler> ICompressHandlerIterator;
extern jlib_decl ICompressHandlerIterator *getCompressHandlerIterator();
extern jlib_decl void setDefaultCompressor(const char *type);
extern jlib_decl ICompressHandler *queryCompressHandler(const char *type);
extern jlib_decl ICompressHandler *queryCompressHandler(CompressionMethod method);

extern jlib_decl ICompressHandler *queryDefaultCompressHandler();
extern jlib_decl bool addCompressorHandler(ICompressHandler *handler); // returns true if added, false if already registered
extern jlib_decl bool removeCompressorHandler(ICompressHandler *handler); // returns true if present and removed

extern jlib_decl ICompressor *getCompressor(const char *type, const char *options=NULL);
extern jlib_decl IExpander *getExpander(const char *type, const char *options=NULL);

extern jlib_decl CompressionMethod translateToCompMethod(const char *compStr, CompressionMethod defaultMethod = COMPRESS_METHOD_LZ4);
extern jlib_decl const char *translateFromCompMethod(unsigned compMethod);

#define MIN_ROWCOMPRESS_RECSIZE 8
#endif
