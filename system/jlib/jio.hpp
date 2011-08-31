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



#ifndef JIO_INCL
#define JIO_INCL

#include "jiface.hpp"
#include "jarray.hpp"
#include <stdio.h>

typedef count_t findex_t;       //row index in a file.


#ifndef IRECORDSIZE_DEFINED     // also in eclhelper.hpp
#define IRECORDSIZE_DEFINED
interface IRecordSize: public IInterface 
// used to determine record size from record contents
{
    virtual size32_t getRecordSize(const void *rec) = 0;
       //passing NULL to getRecordSize returns size for fixed records and initial size for variable
    virtual size32_t getFixedSize() const = 0;                      
       // returns 0 for variable row size

    inline bool isFixedSize()      const { return getFixedSize()!=0; }
    inline bool isVariableSize()   const { return getFixedSize()==0; }

};
#endif


interface IReadSeq : public IInterface
{
// fixed length record read interface
    virtual void     reset() = 0;
    virtual bool get(void *dst) = 0;
    virtual unsigned getn(void *dst, unsigned numrecs) = 0;
    virtual size32_t getRecordSize() = 0; 
    virtual void stop() = 0; // indicate finished reading
};

interface IWriteSeq : public IInterface
{
// fixed length record write interface
    virtual void flush() = 0;
    virtual void put(const void *dst) = 0;
    virtual void putn(const void *dst, unsigned numrecs) = 0;
    virtual size32_t getRecordSize() = 0;
    virtual offset_t getPosition() = 0;
};

interface ISimpleReadStream : public IInterface
{
    virtual size32_t read(size32_t max_len, void * data) = 0;
};

interface IIOStream : public ISimpleReadStream
{
    virtual void flush() = 0;
    virtual size32_t write(size32_t len, const void * data) = 0;
};


#ifdef  __x86_64__
extern jlib_decl  void writeStringToStream(IIOStream &out, const char *s);
extern jlib_decl  void writeCharsNToStream(IIOStream &out, char c, unsigned cnt);
extern jlib_decl  void writeCharToStream(IIOStream &out, char c);
#else
inline void writeStringToStream(IIOStream &out, const char *s) { out.write((size32_t)strlen(s), s); }
inline void writeCharsNToStream(IIOStream &out, char c, unsigned cnt) { while(cnt--) out.write(1, &c); }
inline void writeCharToStream(IIOStream &out, char c) { out.write(1, &c); }
#endif

extern jlib_decl IIOStream *createBufferedIOStream(IIOStream *io, unsigned _bufsize=(unsigned)-1);


interface IReceiver : public IInterface
{
    virtual bool takeRecord(offset_t pos) = 0;
};

interface IRecordFetchChannel : public IInterface
{
    virtual void fetch(offset_t pos, void *buffer, IReceiver *receiver) = 0;
    virtual void flush() = 0;
    virtual void abort() = 0;
    virtual bool isAborted() = 0;
    virtual bool isImmediate() = 0;
};

interface IRecordFetcher : public IInterface
{
    virtual IRecordFetchChannel *openChannel(bool immediate) = 0;
};

interface IWriteSeqAllocator : public IInterface
{
    virtual IWriteSeq *next(size32_t &num) = 0; 
};

interface IReadSeqAllocator : public IInterface
{
    virtual IReadSeq *next() = 0;
};


extern jlib_decl IReadSeq *createReadSeq(int fh, offset_t _offset, size32_t size, size32_t _bufsize = (size32_t)-1, // bufsize in bytes 
                                         unsigned maxrecs=(unsigned)-1, bool compress=false); // compression is *not* blocked and needs buffer size
extern jlib_decl IWriteSeq *createWriteSeq(int fh, size32_t size, size32_t bufsize = (size32_t)-1,bool compress=false); // compression is *not* blocked and needs buffer size
extern jlib_decl IWriteSeq *createTeeWriteSeq(IWriteSeq *, IWriteSeq *);
extern jlib_decl IWriteSeq *createChainedWriteSeq(IWriteSeqAllocator *iwsa);
extern jlib_decl IReadSeq *createChainedReadSeq(IReadSeqAllocator *irsa);
extern jlib_decl IRecordFetcher *createElevatorFetcher(int fh, size32_t recSize);


extern jlib_decl IRecordSize *createFixedRecordSize(size32_t recsize);
extern jlib_decl IRecordSize *createDeltaRecordSize(IRecordSize * size, int delta);


extern jlib_decl unsigned copySeq(IReadSeq *from,IWriteSeq *to,size32_t bufsize);

extern jlib_decl void setIORetryCount(unsigned _ioRetryCount); // default 0 == off, retries if read op. fails
extern jlib_decl offset_t checked_lseeki64(int handle, offset_t offset, int origin);
extern jlib_decl size32_t checked_write(int handle, const void *buffer, size32_t count);
extern jlib_decl size32_t checked_read(int file, void *buffer, size32_t len);
extern jlib_decl size32_t checked_pread(int file, void *buffer, size32_t len, offset_t pos);

class CachedRecordSize
{
public:
    inline CachedRecordSize(IRecordSize * _rs = NULL) { set(_rs); }

    inline void set(IRecordSize * _rs)
    {
        rs.set(_rs);
        if (_rs)
        {
            initialSize = _rs->getRecordSize(NULL);
            fixedSize = _rs->getFixedSize();
        }
    }

    inline size32_t getInitialSize() const                  { return initialSize; }
    inline size32_t getFixedSize() const                    { return fixedSize; }
    inline size32_t getRecordSize(const void *rec) const    { return fixedSize ? fixedSize : rs->getRecordSize(rec); }
    inline bool isFixedSize() const                         { return (fixedSize != 0); }
    inline operator IRecordSize * () const                  { return rs; }

private:
    Owned<IRecordSize> rs;
    size32_t fixedSize;
    size32_t initialSize;
};


interface IFileIO;
interface IFileIOStream;


#ifndef IROWSTREAM_DEFINED
#define IROWSTREAM_DEFINED
interface IRowStream : extends IInterface 
{
    virtual const void *nextRow()=0;                      // rows returned must be freed
    virtual void stop() = 0;                              // after stop called NULL is returned

    inline const void *ungroupedNextRow() 
    {
        const void *ret = nextRow();
        if (!ret)
            ret = nextRow();
        return ret;
    }
};
#endif

interface IRowWriter: extends IInterface
{
    virtual void putRow(const void *row) = 0; // takes ownership of row
    virtual void flush() = 0;
};

interface IRowLinkCounter: extends IInterface
{
    virtual void linkRow(const void *row)=0;
    virtual void releaseRow(const void *row)=0;
};

interface IRowProvider: extends IRowLinkCounter
{
    virtual const void *nextRow(unsigned idx)=0;
    virtual void stop(unsigned idx)=0;
};


extern jlib_decl IRowStream *createNullRowStream();
extern jlib_decl unsigned copyRowStream(IRowStream *in, IRowWriter *out);
extern jlib_decl unsigned groupedCopyRowStream(IRowStream *in, IRowWriter *out);
extern jlib_decl unsigned ungroupedCopyRowStream(IRowStream *in, IRowWriter *out);
extern jlib_decl IRowStream *createConcatRowStream(unsigned numstreams,IRowStream** streams,bool grouped=false);// simple concat


#endif
