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

#ifndef FTTRANSFORM_IPP
#define FTTRANSFORM_IPP

#include "jptree.hpp"
#include "filecopy.hpp"
#include "fttransform.hpp"
#include "ftbase.ipp"
#include "daft.hpp"
#include "daftformat.hpp"

//---------------------------------------------------------------------------

class CTransformerBase : public CInterface, implements ITransformer
{
public:
    CTransformerBase();
    IMPLEMENT_IINTERFACE

    virtual void beginTransform(IFileIOStream * out);
    virtual void endTransform(IFileIOStream * out);
    virtual bool getInputCRC(crc32_t & value) { return false; }
    virtual bool setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length, bool compressedInput, const char *decryptKey) = 0;
    virtual void setInputCRC(crc32_t value);

protected:
    bool setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length);

protected:
    IFileAttr   inputFile;
    offset_t    startOffset;
    offset_t    maxOffset;
};




class CTransformer : public CTransformerBase
{
public:
    CTransformer(size32_t _bufferSize);
    ~CTransformer();
    IMPLEMENT_IINTERFACE

    virtual bool setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length, bool compressedInput, const char *decryptKey);
    virtual size32_t getBlock(IFileIOStream * out);
    virtual offset_t tell();

protected:
    size32_t read(size32_t maxLength, void * buffer);
    virtual size32_t getN(byte * buffer, size32_t maxLength) = 0;

protected:
    IFileIOAttr input;
    offset_t    cursor;
    size32_t        bufferSize;
    byte *      buffer;
};



//----------------------------------------------------------------------------

//Copying fixed->fixed, don't really care about the record size when copying.

class CNullTransformer : public CTransformer
{
public:
    CNullTransformer(size32_t buffersize);

    virtual size32_t getN(byte * buffer, size32_t maxLength);

    virtual bool getInputCRC(crc32_t & value) { value = inputCRC; return true; }
    virtual void setInputCRC(crc32_t value);

protected:
    bool doInputCRC;
    crc32_t inputCRC;
};


//----------------------------------------------------------------------------

//Copying fixed->fixed, don't really care about the record size when copying.

class CFixedToVarTransformer : public CTransformer
{
public:
    CFixedToVarTransformer(size32_t _recordSize,size32_t buffersize, bool _bigendian);

    virtual size32_t getN(byte * buffer, size32_t maxLength);
    virtual offset_t tell();

protected:
    typedef unsigned varLenType;
    enum { minBlockSize = 32768 };

protected:
    size32_t                recordSize;
    bool                    bigendian;
};

//----------------------------------------------------------------------------

//Copying fixed->fixed, don't really care about the record size when copying.

class CVarToFixedTransformer : public CTransformer
{
public:
    CVarToFixedTransformer(size32_t _recordSize,size32_t buffersize,bool _bigendian);
    ~CVarToFixedTransformer();

    virtual size32_t getN(byte * buffer, size32_t maxLength);
    virtual offset_t tell();

protected:
    typedef unsigned varLenType;
    enum { minBlockSize = 32768 };

protected:
    size32_t                recordSize;
    size32_t                savedSize;
    byte *              savedBuffer;
    bool                    bigendian;
};

//----------------------------------------------------------------------------

class CBlockToVarTransformer : public CTransformer
{
public:
    CBlockToVarTransformer(bool _bigendian);

    virtual size32_t getN(byte * buffer, size32_t maxLength);
    virtual offset_t tell();

protected:
    typedef unsigned blockLenType;

protected:
    blockLenType        nextBlockSize;
    bool bigendian;
};

//----------------------------------------------------------------------------

class CVarToBlockTransformer : public CTransformer
{
public:
    CVarToBlockTransformer(bool _bigendian);
    ~CVarToBlockTransformer();

    virtual size32_t getN(byte * buffer, size32_t maxLength);
    virtual offset_t tell();

protected:
    typedef unsigned blockLenType;
    typedef unsigned varLenType;

protected:
    size32_t                savedSize;
    byte *              savedBuffer;
    bool bigendian;
};

//----------------------------------------------------------------------------

class CGeneralTransformer : public CTransformerBase
{
public:
    CGeneralTransformer(const FileFormat & srcFormat, const FileFormat & tgtFormat);

    virtual void beginTransform(IFileIOStream * out);
    virtual void endTransform(IFileIOStream * out);
    virtual size32_t getBlock(IFileIOStream * out);
    virtual bool getInputCRC(crc32_t & value);
    virtual bool setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length, bool compressedInput, const char *decryptKey);
    virtual void setInputCRC(crc32_t value);
    virtual offset_t tell();

protected:
    Owned<IFormatProcessor> processor;
    Owned<IOutputProcessor> target;
    TransformCursor         cursor;
};

//----------------------------------------------------------------------------

class DALIFT_API TransferServer
{
public:
    TransferServer(ISocket * _masterSocket);

    void deserializeAction(MemoryBuffer & msg, unsigned action);
    bool pull();
    bool push();

protected:
    void appendTransformed(unsigned whichChunk, ITransformer * input);
    unsigned queryLastOutput(unsigned outputIndex);
    void sendProgress(OutputProgress & curProgress);
    void transferChunk(unsigned chunkIndex);
    void wrapOutInCRC(unsigned startCRC);

protected:
    PartitionPointArray     partition;
    OutputProgressArray     progress;
    FileFormat              srcFormat;
    FileFormat              tgtFormat;
    ISocket *               masterSocket;
    Linked<IFileIOStream>   out;
    Linked<CrcIOStream>     crcOut;
    unsigned                lastTick;
    unsigned                updateFrequency;
    offset_t                totalLengthRead;
    offset_t                totalLengthToRead;
    bool                    calcInputCRC;
    bool                    calcOutputCRC;
    bool                    replicate;
    bool                    mirror;
    bool                    isSafeMode;
    unsigned                throttleNicSpeed;
    unsigned                numParallelSlaves;
    bool                    compressedInput;
    bool                    compressOutput;
    bool                    copyCompressed;
    size32_t                transferBufferSize;
    StringAttr              encryptKey;
    StringAttr              decryptKey;
};


ITransformer * createTransformer(IFile * input, offset_t startOffset, offset_t length, const FileFormat & srcFormat, const FileFormat & tgtFormat);


#endif
