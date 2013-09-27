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

#ifndef DAFTFORMAT_HPP
#define DAFTFORMAT_HPP

#include "filecopy.hpp"
#include "daft.hpp"
#include "ftbase.ipp"

#define DEFAULT_STD_BUFFER_SIZE         0x10000
#define EFX_BLOCK_HEADER_TYPE   unsigned
#define EFX_BLOCK_HEADER_SIZE   sizeof(EFX_BLOCK_HEADER_TYPE)
#define VARIABLE_LENGTH_TYPE    unsigned
#define EXPECTED_VARIABLE_LENGTH 512            // NOt very criticial

//---------------------------------------------------------------------------

struct PartitionCursor
{
public:
    PartitionCursor(offset_t _inputOffset)  { inputOffset = nextInputOffset = _inputOffset; outputOffset = 0; }
    
    offset_t        inputOffset;
    offset_t        nextInputOffset;
    offset_t        outputOffset;
};

struct TransformCursor
{
public:
    TransformCursor()                       { inputOffset = 0; }

    offset_t        inputOffset;
};

interface IOutputProcessor;
interface IFormatPartitioner : public IInterface
{
public:
//Analysis
    virtual void calcPartitions(Semaphore * sem) = 0;
    virtual void getResults(PartitionPointArray & partition) = 0;
    virtual void setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts) = 0;
    virtual void setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool compressedInput, const char *decryptKey) = 0;
    virtual void setTarget(IOutputProcessor * _target) = 0;
    virtual void setRecordStructurePresent(bool _recordStructurePresent) = 0;
    virtual void getRecordStructure(StringBuffer & _recordStructure) = 0;
};

interface IFormatProcessor : public IFormatPartitioner
{
    virtual void setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool compressedInput, const char *decryptKey) = 0;
    virtual void setTarget(IOutputProcessor * _target) = 0;

    //Processing.
    virtual void beginTransform(offset_t thisOffset, offset_t thisLength, TransformCursor & cursor) = 0;
    virtual void endTransform(TransformCursor & cursor) = 0;
    virtual crc32_t getInputCRC() = 0;
    virtual void setInputCRC(crc32_t value) = 0;
    virtual unsigned transformBlock(offset_t endOffset, TransformCursor & cursor) = 0;
};

interface IOutputProcessor : public IInterface
{
public:
    virtual offset_t getOutputOffset() = 0;
    virtual void setOutput(offset_t startOffset, IFileIOStream * out = NULL) = 0;

    virtual void updateOutputOffset(size32_t len, const byte * data) = 0;
    virtual void finishOutputOffset() = 0;

    virtual void outputRecord(size32_t len, const byte * data) = 0;
    virtual void finishOutputRecords() = 0;

//Record processing
//  virtual void processRecord(size32_t len, byte * data, IFileIOStream * output);
};

typedef IArrayOf<IFormatProcessor> FormatProcessorArray;
typedef IArrayOf<IFormatPartitioner> FormatPartitionerArray;

extern DALIFT_API IFormatProcessor * createFormatProcessor(const FileFormat & srcFormat, const FileFormat & tgtFormat, bool calcOutput);
extern DALIFT_API IOutputProcessor * createOutputProcessor(const FileFormat & format);

extern DALIFT_API IFormatPartitioner * createFormatPartitioner(const SocketEndpoint & ep, const FileFormat & srcFormat, const FileFormat & tgtFormat, bool calcOutput, const char * slave, const char *wuid);

#endif
