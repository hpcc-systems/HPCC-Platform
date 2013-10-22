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

#ifndef DAFTFORMAT_IPP
#define DAFTFORMAT_IPP

#include "filecopy.hpp"
#include "ftbase.ipp"
#include "daft.hpp"
#include "daftformat.hpp"
#include "rmtpass.hpp"

//---------------------------------------------------------------------------

class DALIFT_API CPartitioner : public CInterface, implements IFormatProcessor
{
public:
    CPartitioner();
    IMPLEMENT_IINTERFACE

    virtual void calcPartitions(Semaphore * sem);
    virtual void getResults(PartitionPointArray & partition);
    virtual void setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts);
    virtual void setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool compressedInput, const char *decryptKey);
    virtual void setTarget(IOutputProcessor * _target);
    virtual void setRecordStructurePresent(bool _recordStructurePresent);
    virtual void getRecordStructure(StringBuffer & _recordStructure);

protected:
    virtual void findSplitPoint(offset_t curOffset, PartitionCursor & cursor) = 0;
    virtual bool splitAfterPoint() { return false; }
    virtual void killBuffer() = 0;
    

    void commonCalcPartitions();


protected:
    PartitionPointArray         results;
    unsigned                    whichInput;
    RemoteFilename              inputName;
    StringAttr                  fullPath;
    Linked<IOutputProcessor>    target;

    offset_t                    totalSize;
    offset_t                    thisOffset;
    offset_t                    thisSize;
    unsigned                    thisHeaderSize;
    unsigned                    numParts;
    bool                        partitioning;
};

//---------------------------------------------------------------------------
// Simple processors that do not need to read the source file.

class DALIFT_API CSimplePartitioner : public CPartitioner
{
public:
    virtual void beginTransform(offset_t thisOffset, offset_t thisLength, TransformCursor & cursor) { UNIMPLEMENTED; }
    virtual void endTransform(TransformCursor & cursor) { UNIMPLEMENTED; }
    virtual unsigned transformBlock(offset_t endOffset, TransformCursor & cursor) { UNIMPLEMENTED; }
    virtual crc32_t getInputCRC() { UNIMPLEMENTED; }
    virtual void setInputCRC(crc32_t value) { UNIMPLEMENTED; }
    virtual void killBuffer() { }
};

class DALIFT_API CSimpleFixedPartitioner : public CSimplePartitioner
{
public:
    CSimpleFixedPartitioner(unsigned _recordSize, bool _noTranslation) { recordSize = _recordSize; noTranslation = _noTranslation; }

    virtual void setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts);

protected:
    virtual void findSplitPoint(offset_t curOffset, PartitionCursor & cursor);

protected:
    unsigned                    recordSize;
    bool                        noTranslation;
};


class DALIFT_API CSimpleBlockedPartitioner : public CSimpleFixedPartitioner
{
public:
    CSimpleBlockedPartitioner(bool _noTranslation);
};

/* no simple partitioner yet (probably would need dafilesrv support)

class DALIFT_API CSimpleRECFMvbPartitioner : public CSimplePartitioner
{
public:
    CSimpleRECFMvbPartitioner(bool _noTranslation, bool blocked) { noTranslation = _noTranslation; isBlocked = blocked; }

    virtual void setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts);

protected:
    virtual void findSplitPoint(offset_t curOffset, PartitionCursor & cursor);

protected:
    bool                        noTranslation;
    bool                        isBlocked;
};



*/


//---------------------------------------------------------------------------
// More complex processors that need to read the source file - e.g. because 
// output offset being calculated.


class DALIFT_API CInputBasePartitioner : public CPartitioner
{
public:
    CInputBasePartitioner(unsigned _headerSize, unsigned expectedRecordSize);
    ~CInputBasePartitioner();

    virtual void setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool compressedInput, const char *decryptKey);

    virtual void beginTransform(offset_t thisOffset, offset_t thisLength, TransformCursor & cursor);
    virtual void endTransform(TransformCursor & cursor);
    virtual unsigned transformBlock(offset_t endOffset, TransformCursor & cursor);
    virtual crc32_t getInputCRC() { return inputCRC; }
    virtual void setInputCRC(crc32_t value) { doInputCRC = true; inputCRC = value; }

protected:
    bool ensureBuffered(unsigned required);
    virtual void findSplitPoint(offset_t curOffset, PartitionCursor & cursor);
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer) = 0;
    virtual size32_t getTransformRecordSize(const byte * record, unsigned maxToRead) = 0;
    void seekInput(offset_t offset);
    offset_t tellInput();

    inline byte *bufferBase()  
    { 
        return (byte *)((bufattr.length()!=bufferSize)?bufattr.allocate(bufferSize):bufattr.bufferBase()); 
    }
    virtual void killBuffer()  { bufattr.clear(); }
    virtual void clearBufferOverrun(void) { numOfBufferOverrun = 0; numOfProcessedBytes = 0;}
protected: 
    Owned<IFileIOStream>   inStream;
    MemoryAttr             bufattr;
    size32_t               headerSize;
    size32_t               blockSize;
    size32_t               bufferSize;
    size32_t               numInBuffer;
    size32_t               bufferOffset;
    unsigned               inputCRC;
    bool                   doInputCRC;
    static IFileIOCache    *openfilecache;
    static CriticalSection openfilecachesect;

    unsigned               numOfBufferOverrun;
    unsigned               numOfProcessedBytes;
};


class DALIFT_API CFixedPartitioner : public CInputBasePartitioner
{
public:
    CFixedPartitioner(unsigned _recordSize);

protected:
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer);
    virtual size32_t getTransformRecordSize(const byte * record, unsigned maxToRead);

protected:
    size32_t            recordSize;
};


class DALIFT_API CBlockedPartitioner : public CFixedPartitioner
{
public:
    CBlockedPartitioner();

    virtual void setTarget(IOutputProcessor * _target);
};


class DALIFT_API CRECFMvbPartitioner : public CInputBasePartitioner
{
    bool isBlocked;
protected:
    virtual size32_t getRecordSize(const byte * record, unsigned maxToRead);
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer);
    virtual size32_t getTransformRecordSize(const byte * record, unsigned maxToRead);
public:
    CRECFMvbPartitioner(bool blocked);
    unsigned transformBlock(offset_t endOffset, TransformCursor & cursor);


};


class DALIFT_API CVariablePartitioner : public CInputBasePartitioner
{
public:
    CVariablePartitioner(bool _bigendian);

    virtual void setTarget(IOutputProcessor * _target);

protected:
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer);
    virtual size32_t getTransformRecordSize(const byte * record, unsigned maxToRead);
    size32_t getRecordSize(const byte * record, unsigned maxToRead);

protected:
    typedef unsigned varLenType;
    bool bigendian;
};


class DALIFT_API CCsvPartitioner : public CInputBasePartitioner
{
public:
    CCsvPartitioner(const FileFormat & _format);

    virtual void setTarget(IOutputProcessor * _target);

    virtual void getRecordStructure(StringBuffer & _recordStructure) { _recordStructure = recordStructure; }
    virtual void setRecordStructurePresent( bool _isRecordStructurePresent) {isRecordStructurePresent = _isRecordStructurePresent;}

protected:
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer, bool ateof);
    virtual size32_t getTransformRecordSize(const byte * record, unsigned maxToRead);
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer)
    {
        return getSplitRecordSize(record,maxToRead,processFullBuffer,true);
    }

private:
    void storeFieldName(const char * start, unsigned len);
    
protected:
    enum { NONE=0, SEPARATOR=1, TERMINATOR=2, WHITESPACE=3, QUOTE=4, ESCAPE=5 };
    unsigned        maxElementLength;
    FileFormat      format;
    StringMatcher   matcher;

    bool            isRecordStructurePresent;
    StringBuffer    recordStructure;
    unsigned        fieldCount;
    bool            isFirstRow;
};


class DALIFT_API CCsvQuickPartitioner : public CCsvPartitioner
{
public:
    CCsvQuickPartitioner(const FileFormat & _format, bool _noTranslation) 
        : CCsvPartitioner(_format) 
    { 
        noTranslation = _noTranslation;
        const char * quote = _format.quote.get();  
        if (quote && (*quote == '\0')) { 
            isquoted = false;
        }       
        else // default is quoted
            isquoted = true;
    }

protected:
    virtual void findSplitPoint(offset_t curOffset, PartitionCursor & cursor);
    virtual bool splitAfterPoint() { return true; }

protected:
    bool                        noTranslation;
    bool                        isquoted;
};

//---------------------------------------------------------------------------

class DALIFT_API CUtfPartitioner : public CInputBasePartitioner
{
public:
    CUtfPartitioner(const FileFormat & _format);

    virtual void setTarget(IOutputProcessor * _target);
    
    virtual void getRecordStructure(StringBuffer & _recordStructure) { _recordStructure = recordStructure; }
    virtual void setRecordStructurePresent( bool _isRecordStructurePresent) {isRecordStructurePresent = _isRecordStructurePresent;}

protected:
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer, bool ateof);
    virtual size32_t getTransformRecordSize(const byte * record, unsigned maxToRead);
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer)
    {
        return getSplitRecordSize(record,maxToRead,processFullBuffer,false);
    }
    
private:
    void storeFieldName(const char * start, unsigned len);

protected:
    enum { NONE=0, SEPARATOR=1, TERMINATOR=2, WHITESPACE=3, QUOTE=4, ESCAPE=5 };
    unsigned        maxElementLength;
    FileFormat      format;
    StringMatcher   matcher;
    unsigned        unitSize;
    UtfReader::UtfFormat utfFormat;
    
    bool            isRecordStructurePresent;
    StringBuffer    recordStructure;
    unsigned        fieldCount;
    bool            isFirstRow;
};


class DALIFT_API CUtfQuickPartitioner : public CUtfPartitioner
{
public:
    CUtfQuickPartitioner(const FileFormat & _format, bool _noTranslation) : CUtfPartitioner(_format) { noTranslation = _noTranslation; }

protected:
    virtual void findSplitPoint(offset_t curOffset, PartitionCursor & cursor);
    virtual bool splitAfterPoint() { return true; }

protected:
    bool                        noTranslation;
};

//---------------------------------------------------------------------------

class BufferedDirectReader
{
public:
    BufferedDirectReader(size32_t _blockSize = 0, size32_t _bufferSize = 0);
    ~BufferedDirectReader();

    inline size32_t available()             { return numInBuffer - bufferOffset; }
    size32_t ensure(size32_t len);
    const byte * next()                     { return buffer+bufferOffset; }
    inline void skip(size32_t len)          { bufferOffset += len; }
    void set(IFileIOStream * _stream)       { stream.set(_stream); numInBuffer = 0; bufferOffset = 0; }
    void seek(offset_t pos)                 { stream->seek(pos, IFSbegin); numInBuffer = 0; bufferOffset = 0; }
    offset_t tell()                         { return stream->tell() - available(); }

protected:
    size32_t blockSize;
    size32_t bufferSize;
    size32_t numInBuffer;
    size32_t bufferOffset;
    byte * buffer;
    Linked<IFileIOStream> stream;
};

class DALIFT_API XmlSplitter
{
public:
    XmlSplitter(const FileFormat & format);

    size32_t getRecordSize(const byte * record, unsigned maxToRead, bool throwOnError);
    size32_t getEndOfRecord(const byte * record, unsigned maxToRead);
    offset_t getHeaderLength(BufferedDirectReader & reader);
    offset_t getFooterLength(BufferedDirectReader & reader, offset_t size);
    
    unsigned getMaxElementLength() { return maxElementLength; }

protected:
    enum { NONE, OPENTAG, CLOSETAG, ENDTAG, ENDCLOSETAG, NEWLINE, WHITESPACE };
    unsigned        maxElementLength;
    StringMatcher   matcher;
    unsigned        unitSize;
    UtfReader::UtfFormat utfFormat;
};



class DALIFT_API CXmlPartitioner : public CInputBasePartitioner
{
public:
    CXmlPartitioner(const FileFormat & _format);

    virtual void setTarget(IOutputProcessor * _target);

protected:
    virtual size32_t getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer);
    virtual size32_t getTransformRecordSize(const byte * record, unsigned maxToRead);

protected:
    XmlSplitter     splitter;
    FileFormat      format;
    unsigned        unitSize;
    UtfReader::UtfFormat utfFormat;
};


class DALIFT_API CXmlQuickPartitioner : public CXmlPartitioner
{
public:
    CXmlQuickPartitioner(const FileFormat & _format, bool _noTranslation) : CXmlPartitioner(_format) { noTranslation = _noTranslation; }

protected:
    virtual void findSplitPoint(offset_t curOffset, PartitionCursor & cursor);
    virtual bool splitAfterPoint() { return true; }

protected:
    bool                        noTranslation;
};

//---------------------------------------------------------------------------

class DALIFT_API CRemotePartitioner : public Thread, public IFormatPartitioner
{
public:
    CRemotePartitioner(const SocketEndpoint & _ep, const FileFormat & _srcFormat, const FileFormat & _tgtFormat, const char * _slave, const char *_wuid);
    IMPLEMENT_IINTERFACE

    virtual int  run();

    virtual void calcPartitions(Semaphore * sem);
    virtual void getResults(PartitionPointArray & partition);
    virtual void setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts);
    virtual void setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool compressedInput, const char *decryptKey);
    virtual void setTarget(IOutputProcessor * _target) { UNIMPLEMENTED; }
    virtual void setRecordStructurePresent(bool _recordStructurePresent);
    virtual void getRecordStructure(StringBuffer & _recordStructure);

protected:
    void callRemote();

protected:
    CachedPasswordProvider      passwordProvider;
    SocketEndpoint              ep;
    FileFormat                  srcFormat;
    FileFormat                  tgtFormat;
    PartitionPointArray         results;
    unsigned                    whichInput;
    RemoteFilename              fullPath;
    Semaphore *                 sem;
    StringAttr                  slave;
    StringAttr                  wuid;

    offset_t                    totalSize;
    offset_t                    thisOffset;
    offset_t                    thisSize;
    unsigned                    thisHeaderSize;
    unsigned                    numParts;
    Linked<IException>          error;
    bool                        compressedInput;
    StringAttr                  decryptKey;
};

//---------------------------------------------------------------------------

// Following are used for processing output.  They can also do transformations (e.g., see below)

class DALIFT_API COutputProcessor : public CInterface, implements IOutputProcessor
{
public:
    COutputProcessor();
    IMPLEMENT_IINTERFACE

    virtual void finishOutputOffset() {}
    virtual void finishOutputRecords() {}
    virtual offset_t getOutputOffset();
    virtual void setOutput(offset_t startOffset, IFileIOStream * out = NULL);

protected:
    offset_t                outputOffset;
    OwnedIFileIOStream      out; 
};

class DALIFT_API CFixedOutputProcessor : public COutputProcessor
{
public:
    CFixedOutputProcessor(unsigned _recordSize) { recordSize = _recordSize; }

    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);

protected:
    unsigned                    recordSize;
};


class DALIFT_API CBlockedOutputProcessor : public COutputProcessor
{
public:
    CBlockedOutputProcessor();
    ~CBlockedOutputProcessor();

    virtual void finishOutputOffset();
    virtual void finishOutputRecords();
    virtual void setOutput(offset_t startOffset, IFileIOStream * out = NULL);
    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);

protected:
    void writeNextBlock();

protected:
    size32_t                        outputExtra;
    byte *                      buffer;

};


class DALIFT_API CVariableOutputProcessor : public COutputProcessor
{
public:
    CVariableOutputProcessor(bool _bigendian) { bigendian = _bigendian; }
    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);

protected:
    typedef unsigned varLenType;
    bool bigendian;
};


class DALIFT_API CSimpleOutputProcessor : public COutputProcessor
{
public:
    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);
};

class DALIFT_API CRecordSizeOutputProcessor : public COutputProcessor
{
public:
    CRecordSizeOutputProcessor(IRecordSize * _recordSize);

    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);

protected:
    IRecordSize *               recordSize;
};


class DALIFT_API CUtfOutputProcessor : public COutputProcessor
{
public:
    CUtfOutputProcessor(FileFormatType _type) { utfFormat = getUtfFormatType(_type); }
    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);

protected:
    UtfReader::UtfFormat utfFormat;
    MemoryBuffer transformed;
};


//---------------------------------------------------------------------------
//Special output formats used for transforming to standard output format....

class DALIFT_API COutputProcessorHook : public CInterface, implements IOutputProcessor
{
public:
    COutputProcessorHook(IOutputProcessor * _target) : target(_target) {}
    IMPLEMENT_IINTERFACE

    virtual void finishOutputOffset();
    virtual void finishOutputRecords();
    virtual offset_t getOutputOffset();
    virtual void setOutput(offset_t startOffset, IFileIOStream * out = NULL);

protected:
    Linked<IOutputProcessor>    target;
};

//This removes the length from variable length records passed to it.
class DALIFT_API CVariableProcessorHook : public COutputProcessorHook
{
public:
    CVariableProcessorHook(IOutputProcessor * _target) : COutputProcessorHook(_target) {}

    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);
};

//Should remove the terminator from the records passed to it.  Doesn't do it yet... and probably never will.
class DALIFT_API CCsvProcessorHook : public COutputProcessorHook
{
public:
    CCsvProcessorHook(IOutputProcessor * _target) : COutputProcessorHook(_target) {}

    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);
};

//This passes each child record from a block in turn
class DALIFT_API CBlockedProcessorHook : public COutputProcessorHook
{
public:
    CBlockedProcessorHook(IOutputProcessor * _target) : COutputProcessorHook(_target) {}

    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);
};


class DALIFT_API CUtfProcessorHook : public COutputProcessorHook
{
public:
    CUtfProcessorHook(FileFormatType _type, IOutputProcessor * _target) : COutputProcessorHook(_target) { utfFormat = getUtfFormatType(_type); }

    virtual void outputRecord(size32_t len, const byte * data);
    virtual void updateOutputOffset(size32_t len, const byte * data);

protected:
    UtfReader::UtfFormat utfFormat;
    MemoryBuffer transformed;
};

#endif
