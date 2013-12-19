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

#ifndef FTBASE_IPP
#define FTBASE_IPP

#include "filecopy.hpp"
#include "rmtfile.hpp"
#include "daft.hpp"
#include "ftbase.hpp"

class DALIFT_API PartitionPoint : public CInterface
{
public:
    PartitionPoint(unsigned _whichInput, unsigned _whichOutput, offset_t _startOffset, offset_t _inputlength, offset_t _outputLength);
    PartitionPoint();

    void deserialize(MemoryBuffer & in);
    void display();
    void restore(IPropertyTree * tree);
    void serialize(MemoryBuffer & out);
    void save(IPropertyTree * tree);

protected:
    void clear();

public:
//static calculated
    offset_t        inputOffset;
    offset_t        inputLength;
    offset_t        outputLength;
    offset_t        outputOffset;
    unsigned        whichInput;
    unsigned        whichOutput;

    //Not saved - derived from other information.
    RemoteFilename  inputName;
    RemoteFilename  outputName;
    CDateTime       modifiedTime;
    unsigned        whichSlave;
    MemoryAttr      fixedText;
};
typedef CIArrayOf<PartitionPoint> PartitionPointArray;

struct DALIFT_API OutputProgress : public CInterface
{
public:
    OutputProgress();

    void reset();
    void set(const OutputProgress & other);

    MemoryBuffer & deserializeCore(MemoryBuffer & in);
    MemoryBuffer & deserializeExtra(MemoryBuffer & in, unsigned version);
    void restore(IPropertyTree * tree);
    void save(IPropertyTree * tree);
    MemoryBuffer & serializeCore(MemoryBuffer & out);
    MemoryBuffer & serializeExtra(MemoryBuffer & out, unsigned version);
    void trace();

public:
    enum            { StatusBegin, StatusActive, StatusCopied, StatusRenamed };
    unsigned        whichPartition;
    crc32_t         inputCRC;
    offset_t        inputLength;
    crc32_t         outputCRC;
    offset_t        outputLength;
    CDateTime       resultTime;
    byte            status;
    bool            hasInputCRC;
    bool            hasCompressed;
    offset_t        compressedPartSize;

//Not saved/serialized - should probably be in a Sprayer-only class that contains an outputProgress.
    Owned<IPropertyTree> tree;
};
typedef CIArrayOf<OutputProgress> OutputProgressArray;


class DALIFT_API CrcIOStream : public CInterface, implements IFileIOStream
{
public:
    CrcIOStream(IFileIOStream * _stream, unsigned startCRC = 0);
    IMPLEMENT_IINTERFACE

    virtual void flush();
    virtual size32_t read(size32_t len, void * data);
    virtual void seek(offset_t pos, IFSmode origin);
    virtual offset_t size();
    virtual offset_t tell();
    virtual size32_t write(size32_t len, const void * data);

    unsigned getCRC()               { return crc; }
    void setCRC(unsigned long _crc)     { crc = _crc; }

protected:
    IFileIOStreamAttr   stream;
    unsigned        crc;
};



extern DALIFT_API void displayPartition(PartitionPointArray & partition);
extern DALIFT_API void deserialize(PartitionPointArray & partition, MemoryBuffer & in);
extern DALIFT_API void serialize(PartitionPointArray & partition, MemoryBuffer & out);
extern DALIFT_API void getDfuTempName(RemoteFilename & temp, const RemoteFilename & src);
extern DALIFT_API void renameDfuTempToFinal(const RemoteFilename & realname);
extern DALIFT_API void displayProgress(OutputProgressArray & progress);

extern DALIFT_API const char * getHeaderText(FileFormatType type);
extern DALIFT_API unsigned getHeaderSize(FileFormatType type);

#endif
