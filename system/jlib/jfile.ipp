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


#ifndef JFILE_IPP
#define JFILE_IPP

#include "jfile.hpp"
#include "jmutex.hpp"
#include "jio.ipp"
#include <atomic>

#ifndef _WIN32
#include <sys/types.h>
#include <dirent.h>
#endif


class jlib_decl CFile : public CInterface, implements IFile
{
    HANDLE openHandle(IFOmode mode, IFSHmode share, bool async, int stdh=-1);
public:
    CFile(const char * _filename);
    IMPLEMENT_IINTERFACE

    virtual bool exists();
    virtual bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime);
    virtual bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime);
    virtual fileBool isDirectory();
    virtual fileBool isFile();
    virtual fileBool isReadOnly();
    virtual IFileIO * open(IFOmode mode, IFEflags extraFlags=IFEnone);
    virtual IFileAsyncIO * openAsync(IFOmode mode);
    virtual IFileIO * openShared(IFOmode mode,IFSHmode shmode,IFEflags extraFlags=IFEnone);
    virtual const char * queryFilename();
    virtual bool remove();
    virtual void rename(const char *newTail);
    virtual void move(const char *newName);
    virtual void setReadOnly(bool ro);
    virtual offset_t size();
    virtual bool setCompression(bool set);
    virtual offset_t compressedSize();

    bool fastCopyFile(CFile &target, size32_t buffersize, ICopyFileProgress *progress); // internal use

    virtual bool createDirectory();
    virtual IDirectoryIterator *directoryFiles(const char *mask=NULL,bool sub=false,bool includedirs=false);
    virtual IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,        // in
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL);

    virtual unsigned getCRC();

    virtual void setCreateFlags(unsigned cflags);
    virtual void setShareMode(IFSHmode shmode);
    virtual bool getInfo(bool &isdir,offset_t &size,CDateTime &modtime);

    virtual void copySection(const RemoteFilename &dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress *progress=NULL, CFflags copyFlags=CFnone);
    // if toOfs is (offset_t)-1 then copies entire file 

    virtual void copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp, CFflags copyFlags=CFnone);

    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs=0, memsize_t len=(memsize_t)-1, bool write=false);
    
    virtual void treeCopyTo(IFile *dest,IpSubNet &subnet,IpAddress &resfrom,bool usetmp,CFflags copyFlags=CFnone)
    {
        // not really for local but simulate
        GetHostIp(resfrom);
        copyTo(dest,DEFAULT_COPY_BLKSIZE,NULL,usetmp,copyFlags);
    }
    
protected:
    StringAttr filename;
    unsigned flags;
};


class jlib_decl CFileIO : public CInterface, implements IFileIO
{
public:
    CFileIO(HANDLE,IFOmode _openmode,IFSHmode _sharemode,IFEflags _extraFlags);
    ~CFileIO();
    IMPLEMENT_IINTERFACE

    virtual size32_t read(offset_t pos, size32_t len, void * data);
    virtual offset_t size();
    virtual size32_t write(offset_t pos, size32_t len, const void * data);
    virtual void setSize(offset_t size);
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len);
    virtual void flush();
    virtual void close();
    virtual unsigned __int64 getStatistic(StatisticKind kind);

    bool create(const char * filename, bool replace);
    bool open(const char * filename);

    HANDLE queryHandle() { return file; } // for debugging


protected:
    CriticalSection     cs;
    HANDLE              file;
    bool                throwOnError;
    IFSHmode            sharemode;
    IFOmode             openmode;
    IFEflags            extraFlags;
    atomic_t            bytesRead;
    atomic_t            bytesWritten;
    std::atomic<cycle_t> ioReadCycles;
    std::atomic<cycle_t> ioWriteCycles;
    std::atomic<__uint64> ioReadBytes;
    std::atomic<__uint64> ioWriteBytes;
    std::atomic<__uint64> ioReads;
    std::atomic<__uint64> ioWrites;
private:
    void setPos(offset_t pos);

};

class jlib_decl CFileRangeIO : public CInterface, implements IFileIO
{
public:
    CFileRangeIO(IFileIO * _io, offset_t _headerSize, offset_t _maxLength);
    IMPLEMENT_IINTERFACE

    virtual size32_t read(offset_t pos, size32_t len, void * data);
    virtual offset_t size();
    virtual size32_t write(offset_t pos, size32_t len, const void * data);
    virtual void setSize(offset_t size) { UNIMPLEMENTED; }
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len) { UNIMPLEMENTED; return 0; }
    virtual void flush() { io->flush(); }
    virtual void close() { io->close(); }
    virtual unsigned __int64 getStatistic(StatisticKind kind) { return io->getStatistic(kind); }

protected:
    Linked<IFileIO>     io;
    offset_t            curOffset;
    offset_t            headerSize;
    offset_t            maxLength;
};


class jlib_decl CFileAsyncIO : public CInterface, implements IFileAsyncIO
{
public:
    CFileAsyncIO(HANDLE,IFSHmode _sharemode);
    ~CFileAsyncIO();
    IMPLEMENT_IINTERFACE

    virtual size32_t read(offset_t pos, size32_t len, void * data);
    virtual offset_t size();
    virtual size32_t write(offset_t pos, size32_t len, const void * data);
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len);
    virtual void flush();
    virtual void close();
    virtual unsigned __int64 getStatistic(StatisticKind kind);

    virtual void setSize(offset_t size);
    virtual IFileAsyncResult *readAsync(offset_t pos, size32_t len, void * data);
    virtual IFileAsyncResult *writeAsync(offset_t pos, size32_t len, const void * data);

    bool create(const char * filename, bool replace);
    bool open(const char * filename);

protected: friend class CFileAsyncResult;

    CriticalSection         cs;
    HANDLE              file;
    bool                    throwOnError;
    IArrayOf<IFileAsyncResult>  results;
    IFSHmode            sharemode;
};


class CFileIOStream : public CInterface, implements IFileIOStream
{
public:
    CFileIOStream(IFileIO * _io);
    IMPLEMENT_IINTERFACE

    virtual void flush();
    virtual size32_t read(size32_t len, void * data);
    virtual void seek(offset_t pos, IFSmode origin);
    virtual offset_t size();
    virtual offset_t tell();
    virtual size32_t write(size32_t len, const void * data);

protected:
    Linked<IFileIO>     io;
    offset_t            curOffset;
};



class jlib_decl CIOStreamReadWriteSeq : public CInterface, public IWriteSeq, public IReadSeq
{
public:
    IMPLEMENT_IINTERFACE;

    CIOStreamReadWriteSeq(IFileIOStream * _stream, offset_t _offset, size32_t _size);

    virtual void put(const void *src);
    virtual void putn(const void *src, unsigned n);
    virtual void flush();
    virtual size32_t getRecordSize() { return size; }
    virtual offset_t getPosition();

    virtual bool get(void *dst);
    virtual unsigned getn(void *dst, unsigned n);
    virtual void reset();
    virtual void stop() {} // no action required

private:
    offset_t offset;
    size32_t    size;
    Linked<IFileIOStream> stream;
};





class jlib_decl DirectBufferI : public CInterface, implements IFileIO
{
public:
    DirectBufferI(unsigned _len, const void * _buffer) { buffLen = _len; buffer = (byte *)_buffer; }

    virtual size32_t read(offset_t pos, size32_t len, void * data);
    virtual offset_t size() { return buffLen; }
    virtual size32_t write(offset_t pos, size32_t len, const void * data);
    virtual void setSize(offset_t size) { UNIMPLEMENTED; }

protected:
    size32_t buffLen;
    byte * buffer;
};

class jlib_decl DirectBufferIO : public DirectBufferI
{
public:
    DirectBufferIO(unsigned _len, void * _buffer) : DirectBufferI(_len, _buffer) {}

    virtual size32_t write(offset_t pos, size32_t len, const void * data);
};

#endif
