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
