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

#include "jliball.hpp"
#include "platform.h"
#include "jlib.hpp"
#include "jio.hpp"
#include "jmutex.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"

#include "fterror.hpp"
#include "dadfs.hpp"
#include "daftcfg.hpp"
#include "daftmc.hpp"
#include "rmtspawn.hpp"
#include "fttransform.ipp"
#include "ftbase.ipp"

#define OPTIMIZE_COMMON_TRANSFORMS

// A couple of options useful for debugging
const unsigned gpfFrequency = 0;
const unsigned blockDelay = 00000;  // time in ms

//----------------------------------------------------------------------------

CTransformerBase::CTransformerBase()
{
    startOffset = 0;
    maxOffset = 0;
}

void CTransformerBase::beginTransform(IFileIOStream * out)
{
}

void CTransformerBase::endTransform(IFileIOStream * out)
{
}

void CTransformerBase::setInputCRC(crc32_t _inputCRC)
{
}

bool CTransformerBase::setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length)
{
    inputFile.setown(createIFile(remoteInputName));
    startOffset = _startOffset;
    maxOffset = _startOffset + _length;
    return true;
}


//----------------------------------------------------------------------------

CTransformer::CTransformer(size32_t _bufferSize)
{
    cursor = 0;
    bufferSize = _bufferSize;
    buffer = new byte[bufferSize];
}

CTransformer::~CTransformer()
{
    delete [] buffer;
}

size32_t CTransformer::read(size32_t maxLength, void * buffer)
{
    if (cursor + maxLength > maxOffset)
        maxLength = (size32_t)(maxOffset - cursor);
    size32_t got = input->read(cursor, maxLength, buffer);
    cursor += got;
    return got;
}

bool CTransformer::setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length, bool compressedInput, const char *decryptKey)
{
    CTransformerBase::setPartition(remoteInputName, _startOffset, _length);
    // could be cache for local, nocache for mirror
    input.setown(inputFile->open(IFOread,IFEnocache));
    if (compressedInput) {                          
        Owned<IExpander> expander;
        if (decryptKey&&*decryptKey) {
            StringBuffer key;
            decrypt(key,decryptKey);
            expander.setown(createAESExpander256(key.length(),key.str()));
        }
        input.setown(createCompressedFileReader(input,expander));   
    }
    cursor = startOffset;
    return (input != NULL);
}


offset_t CTransformer::tell()
{
    return cursor;
}

size32_t CTransformer::getBlock(IFileIOStream * out)
{
    unsigned gotLength = getN(buffer, bufferSize);
    if (gotLength)
        out->write(gotLength, buffer);
    return gotLength;
}

//----------------------------------------------------------------------------

CNullTransformer::CNullTransformer(size32_t buffersize) : CTransformer(buffersize) 
{
    doInputCRC = false;
}


size32_t CNullTransformer::getN(byte * buffer, size32_t maxLength)
{
    unsigned num = read(maxLength, buffer);
    if (doInputCRC)
        inputCRC = crc32((const char *)buffer, num, inputCRC);
    return num;
}

void CNullTransformer::setInputCRC(crc32_t _inputCRC)
{
    doInputCRC = true;
    inputCRC = _inputCRC;
}

//----------------------------------------------------------------------------

CFixedToVarTransformer::CFixedToVarTransformer(size32_t _recordSize,size32_t buffersize, bool _bigendian) 
    : CTransformer(buffersize)
{
    recordSize = _recordSize;
    bigendian = _bigendian;
    assertex(!bigendian); // TBD Var BE
}


//Coded slightly strangely, so that we avoid an extra memcpy() - except for the bytes 
//that don't quite fit in this block.
size32_t CFixedToVarTransformer::getN(byte * buffer, size32_t maxLength)
{
    //Read a block of fixed length records into memory, then add the variable length tags
    //by moving the data within the block.
    const size32_t targetRecordSize = recordSize + sizeof(varLenType);
    size32_t sizeToGet = (maxLength / targetRecordSize) * recordSize;
    size32_t sizeGot = read(sizeToGet, buffer);

    //Now add the varLenType 
    unsigned numGot = sizeGot/recordSize;
    assertex(numGot*recordSize==sizeGot);
    for (unsigned cur=numGot;cur--!=0;)
    {
        byte * curSource = buffer + recordSize * cur;
        byte * curTarget = buffer + targetRecordSize * cur;
        memmove(curTarget + sizeof(varLenType), curSource, recordSize);
        _WINCPYREV(curTarget, &recordSize, sizeof(varLenType));  
    }

    return numGot * targetRecordSize;
}


offset_t CFixedToVarTransformer::tell()
{
    return cursor;
}

//---------------------------------------------------------------------------


CVarToFixedTransformer::CVarToFixedTransformer(unsigned _recordSize,size32_t buffersize, bool _bigendian) 
    : CTransformer(buffersize)
{
    recordSize = _recordSize;
    savedSize = 0;
    savedBuffer = new byte[minBlockSize];
    bigendian = _bigendian;
    assertex(!bigendian); // TBD Var BE
}

CVarToFixedTransformer::~CVarToFixedTransformer()
{
    delete [] savedBuffer;
}


//Read the variable length records into a temporary buffer, and then
//copy across.  Can't avoid the memcpy, or do it in place...
size32_t CVarToFixedTransformer::getN(byte * buffer, size32_t maxLength)
{
    //Fill the savedBuffer
    savedSize += read(minBlockSize-savedSize, savedBuffer+savedSize);

    //Walk through the records copying them to the destination
    assertex(sizeof(varLenType) == 4);
    byte * good = savedBuffer;
    byte * lastGood = savedBuffer + savedSize;
    byte * target = buffer;
    while (good + sizeof(varLenType) <= lastGood)
    {
        varLenType nextLen;
        _WINCPYREV4(&nextLen, good);
        if (good + sizeof(varLenType) + nextLen > lastGood)
            break;
        if (target + recordSize > buffer + maxLength)
            break;

        if (nextLen < recordSize)
        {
            memcpy(target, good + sizeof(varLenType), nextLen);
            memset(target+nextLen, 0, recordSize-nextLen);
        }
        else
            memcpy(target, good + sizeof(varLenType), recordSize);

        good += sizeof(varLenType) + nextLen;
        target += recordSize;
    }

    //Finally shift the extra records down - if there are any
    unsigned numUsed = good - savedBuffer;
    memmove(savedBuffer, good, savedSize-numUsed);
    savedSize -= numUsed;
    return target-buffer;

}

offset_t CVarToFixedTransformer::tell()
{
    return cursor - savedSize;
}

//----------------------------------------------------------------------------

CBlockToVarTransformer::CBlockToVarTransformer(bool _bigendian) 
    : CTransformer(EFX_BLOCK_SIZE)
{
    assertex(sizeof(blockLenType) == 4);
    bigendian = _bigendian;
    assertex(!bigendian); // TBD Var BE
    nextBlockSize = 0;
}

//Coded to read the length of the next block with the previous block of data.
//assumes the padding is pretty small in the record, so don't try and skip it.
size32_t CBlockToVarTransformer::getN(byte * buffer, size32_t maxLength)
{
    assertex(maxLength >= EFX_BLOCK_SIZE);

    size32_t blockSize = nextBlockSize;
    if (!blockSize)
    {
        blockLenType temp;
        size32_t hdrLen = read(sizeof(temp), &temp);
        if (hdrLen == 0)
            return 0;
        assertex(hdrLen == sizeof(blockLenType));
        _WINCPYREV4(&blockSize, &temp);
    }

    size32_t sizeGot = read(EFX_BLOCK_SIZE, buffer);
    if (sizeGot == EFX_BLOCK_SIZE)
        _WINCPYREV4(&nextBlockSize, buffer+EFX_BLOCK_SIZE- sizeof(blockLenType));
    else
        nextBlockSize = 0;

    return blockSize;
}


offset_t CBlockToVarTransformer::tell()
{
    if (nextBlockSize)
        return cursor - sizeof(nextBlockSize);
    return cursor;
}

//----------------------------------------------------------------------------

CVarToBlockTransformer::CVarToBlockTransformer(bool _bigendian) 
    : CTransformer(EFX_BLOCK_SIZE)
{
    savedSize = 0;
    bigendian = _bigendian;
    assertex(!bigendian); // TBD Var BE
    savedBuffer = new byte[EFX_BLOCK_SIZE];
}

CVarToBlockTransformer::~CVarToBlockTransformer()
{
    delete [] savedBuffer;
}

//Coded slightly strangely, so that we avoid an extra memcpy() - except for the bytes 
//that don't quite fit in this block.
size32_t CVarToBlockTransformer::getN(byte * buffer, size32_t maxLength)
{
    size32_t sizeGot = 0;

    byte * startData = buffer + sizeof(blockLenType);
    const unsigned maxDataLength = maxLength - sizeof(blockLenType);
    if (savedSize)
    {
        assertex(savedSize <= maxDataLength);
        size32_t copyLen = savedSize;
        memcpy(startData, savedBuffer, copyLen);
        sizeGot += copyLen;
        savedSize -= copyLen;
    }

    if (maxDataLength != sizeGot)
        sizeGot += read(maxDataLength-sizeGot, startData + sizeGot);

    if (sizeGot == 0)
        return 0;

    //Now work out how many records we've copied.
    byte * good = startData;
    byte * lastGood = startData + sizeGot;
    while (good + sizeof(varLenType) < lastGood)
    {
        varLenType nextLen;
        _WINCPYREV(&nextLen, good, sizeof(nextLen));
        if (good + sizeof(varLenType) + nextLen > lastGood)
            break;
        good += sizeof(varLenType) + nextLen;
    }
    savedSize = sizeGot - (good - startData);
    assertex(savedSize < EFX_BLOCK_SIZE);
    memcpy(savedBuffer, good, savedSize);

    blockLenType blockSize = sizeGot-savedSize;
    _WINCPYREV(buffer, &blockSize, sizeof(blockSize));
    memset(good, 0, EFX_BLOCK_SIZE-blockSize-sizeof(blockSize));
    return EFX_BLOCK_SIZE;
}


offset_t CVarToBlockTransformer::tell()
{
    return cursor - savedSize;
}

//----------------------------------------------------------------------------
CGeneralTransformer::CGeneralTransformer(const FileFormat & srcFormat, const FileFormat & tgtFormat)
{
    processor.setown(createFormatProcessor(srcFormat, tgtFormat, true));
    target.setown(createOutputProcessor(tgtFormat));
    processor->setTarget(target);
}


size32_t CGeneralTransformer::getBlock(IFileIOStream * out)
{
    return processor->transformBlock(maxOffset, cursor);
}

bool CGeneralTransformer::getInputCRC(crc32_t & value)
{
    value = processor->getInputCRC();
    return true;
}

void CGeneralTransformer::setInputCRC(crc32_t _inputCRC)
{
    processor->setInputCRC(_inputCRC);
}

bool CGeneralTransformer::setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length, bool compressedInput, const char *decryptKey)
{
    CTransformerBase::setPartition(remoteInputName, _startOffset, _length);
    processor->setSource(0, remoteInputName, compressedInput, decryptKey);
    return inputFile->exists();
}

void CGeneralTransformer::beginTransform(IFileIOStream * out)
{
    processor->beginTransform(startOffset, maxOffset-startOffset, cursor);
    target->setOutput(out->tell(), out);
}

void CGeneralTransformer::endTransform(IFileIOStream * out)
{
    processor->endTransform(cursor);
}


offset_t CGeneralTransformer::tell()
{
    return cursor.inputOffset;
}

//----------------------------------------------------------------------------

#include "jhtree.hpp"
#include "ctfile.hpp"
#include "keybuild.hpp"
#include "eclhelper_dyn.hpp"
#include "eclhelper_base.hpp"
#include "hqlexpr.hpp"
#include "hqlutil.hpp"
#include "rtldynfield.hpp"

class CIndexTransformer : public CTransformerBase
{
public:
    CIndexTransformer(const char * _targetCompression) : targetCompression(_targetCompression)
    {
    }

    virtual bool setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length, bool compressedInput, const char *decryptKey) override
    {
        return CTransformerBase::setPartition(remoteInputName, _startOffset, _length);
    }

    virtual size32_t getBlock(IFileIOStream * out) override;

    virtual offset_t tell() override
    {
        return sizeRead;
    }

    virtual stat_type getStatistic(StatisticKind kind) override
    {
        //MORE:
        return 0;
    }

protected:
    void rebuildIndex(IFile * in, IFileIOStream * out, const char * outputCompression);

protected:
    StringAttr targetCompression;
    offset_t sizeRead = 0;
};

class TrivialVirtualFieldCallback : public CInterfaceOf<IVirtualFieldCallback>
{
public:
    TrivialVirtualFieldCallback(IKeyManager *_manager) : manager(_manager)
    {
    }
    virtual const char * queryLogicalFilename(const void * row) override
    {
        UNIMPLEMENTED;
    }
    virtual unsigned __int64 getFilePosition(const void * row) override
    {
        UNIMPLEMENTED;
    }
    virtual unsigned __int64 getLocalFilePosition(const void * row) override
    {
        UNIMPLEMENTED;
    }
    virtual const byte * lookupBlob(unsigned __int64 id) override
    {
        size32_t blobSize;
        return manager->loadBlob(id, blobSize, nullptr);
    }
private:
    Linked<IKeyManager> manager;
};


//This code is copied from equivalent code inside dumpkey.cpp, and then simplied/adapted
//A few bugs inn the original implementation have been fixed, both here and in dumpkey.cpp
//Later the code should be refactored to that blocks of nodes can be processsed to allow
//progress reporting - by moving some of the logic into beginTransform/endTransform
void CIndexTransformer::rebuildIndex(IFile * in, IFileIOStream * out, const char * outputCompression)
{
    const char * fieldSelection = nullptr; // could allow projection...
    const char * keyName = in->queryFilename();
    Owned<IFileIO> io = in->open(IFOread);
    if (!io)
        throw MakeStringException(999, "Failed to open file %s", keyName);

    //read with a buffer size of 4MB - for optimal speed, and minimize azure read costs
    Owned <IKeyIndex> index(createKeyIndex(keyName, 0, *io, -1, false, 0x400000));
    size32_t key_size = index->keySize();  // NOTE - in variable size case, this may be 32767 + sizeof(offset_t)
    size32_t keyedSize = index->keyedSize();
    unsigned nodeSize = index->getNodeSize();
    bool isTLK = index->isTopLevelKey();

    Owned<IKeyManager> manager;
    Owned<IPropertyTree> metadata = index->getMetadata();
    Owned<IOutputMetaData> diskmeta;
    Owned<IOutputMetaData> translatedmeta;
    ArrayOf<const RtlFieldInfo *> deleteFields;
    ArrayOf<const RtlFieldInfo *> fields;  // Note - the lifetime of the array needs to extend beyond the lifetime of outmeta. The fields themselves are shared with diskmeta, and do not need to be released.
    Owned<IOutputMetaData> outmeta;
    Owned<const IDynamicTransform> translator;
    RowFilter rowFilter;
    const RtlRecordTypeInfo *outRecType = nullptr;
    if (metadata && metadata->hasProp("_rtlType"))
    {
        MemoryBuffer layoutBin;
        metadata->getPropBin("_rtlType", layoutBin);
        try
        {
            diskmeta.setown(createTypeInfoOutputMetaData(layoutBin, false));
        }
        catch (IException *E)
        {
            EXCLOG(E);
            E->Release();
        }
    }
    if (!diskmeta && metadata && metadata->hasProp("_record_ECL"))
    {
        MultiErrorReceiver errs;
        Owned<IHqlExpression> expr = parseQuery(metadata->queryProp("_record_ECL"), &errs);
        if (errs.errCount() == 0)
        {
            MemoryBuffer layoutBin;
            if (exportBinaryType(layoutBin, expr, true))
                diskmeta.setown(createTypeInfoOutputMetaData(layoutBin, false));
        }
    }
    if (diskmeta)
    {
        const RtlRecord &inrec = diskmeta->queryRecordAccessor(true);
        manager.setown(createLocalKeyManager(inrec, index, nullptr, true, false));
        size32_t minRecSize = 0;
        if (fieldSelection)
        {
            StringArray fieldNames;
            fieldNames.appendList(fieldSelection, ",");
            ForEachItemIn(idx, fieldNames)
            {
                unsigned fieldNum = inrec.getFieldNum(fieldNames.item(idx));
                if (fieldNum == (unsigned) -1)
                    throw MakeStringException(0, "Requested output field '%s' not found", fieldNames.item(idx));
                const RtlFieldInfo *field = inrec.queryOriginalField(fieldNum);
                if (field->type->getType() == type_blob)
                {
                    // We can't just use the original source field in this case (as blobs are only supported in the input)
                    // So instead, create a field in the target with the original type.
                    field = new RtlFieldStrInfo(field->name, field->xpath, field->type->queryChildType());
                    deleteFields.append(field);
                }
                fields.append(field);
                minRecSize += field->type->getMinSize();
            }
            fields.append(nullptr);
            outRecType = new RtlRecordTypeInfo(type_record, minRecSize, fields.getArray(0));
            outmeta.setown(new CDynamicOutputMetaData(*outRecType));
            translator.setown(createRecordTranslator(outmeta->queryRecordAccessor(true), inrec));
        }
        else
        {
            // Copy all fields from the source record
            unsigned numFields = inrec.getNumFields();
            for (unsigned idx = 0; idx < numFields;idx++)
            {
                const RtlFieldInfo *field = inrec.queryOriginalField(idx);
                if (field->type->getType() == type_blob)
                {
                    if (isTLK)
                        continue;  // blob IDs in TLK are not valid
                    // See above - blob field in source needs special treatment
                    field = new RtlFieldStrInfo(field->name, field->xpath, field->type->queryChildType());
                    deleteFields.append(field);
                }
                fields.append(field);
                minRecSize += field->type->getMinSize();
            }
            fields.append(nullptr);
            outmeta.set(diskmeta);
        }

#if 0
        //Could also filter records
        if (filters.ordinality())
        {
            ForEachItemIn(idx, filters)
            {
                const IFieldFilter &thisFilter = rowFilter.addFilter(diskmeta->queryRecordAccessor(true), filters.item(idx));
                unsigned idx = thisFilter.queryFieldIndex();
                const RtlFieldInfo *field = inrec.queryOriginalField(idx);
                if (field->flags & RFTMispayloadfield)
                    throw MakeStringException(0, "Cannot filter on payload field '%s'", field->name);
            }
        }
        rowFilter.createSegmentMonitors(manager);
#endif
    }
    else
    {
        // We don't have record info - fake it? We could pretend it's a single field...
        UNIMPLEMENTED;
        // manager.setown(createLocalKeyManager(fake, index, nullptr));
    }
    manager->finishSegmentMonitors();
    manager->reset();

    Owned<IFileIOStream> outFileStream(createNoSeekIOStream(out));

    unsigned flags = COL_PREFIX | HTREE_FULLSORT_KEY | HTREE_COMPRESSED_KEY | USE_TRAILING_HEADER | TRAILING_HEADER_ONLY;
    if (!outmeta->isFixedSize())
        flags |= HTREE_VARSIZE;
    //if (quickCompressed)
    //    flags |= HTREE_QUICK_COMPRESSED_KEY;
    // MORE - other global options
    bool isVariable = outmeta->isVariableSize();
    size32_t fileposSize = hasTrailingFileposition(outmeta->queryTypeInfo()) ? sizeof(offset_t) : 0;
    size32_t maxDiskRecordSize;
    if (isTLK)
        maxDiskRecordSize = keyedSize;
    else if (isVariable)
        maxDiskRecordSize = KEYBUILD_MAXLENGTH;
    else
        maxDiskRecordSize = outmeta->getFixedSize()-fileposSize;
    const RtlRecord &indexRecord = outmeta->queryRecordAccessor(true);
//    size32_t keyedSize = indexRecord.getFixedOffset(indexRecord.getNumKeyedFields());

    //MORE: Need to rebuild/copy bloom filters
    Owned<IKeyBuilder> keyBuilder = createKeyBuilder(outFileStream, flags, maxDiskRecordSize, nodeSize, keyedSize, 0, nullptr, outputCompression, false, isTLK);


    TrivialVirtualFieldCallback callback(manager);
    size32_t maxSizeSeen = 0;
    while (manager->lookup(true))
    {
        byte const * buffer = manager->queryKeyBuffer();
        size32_t size = manager->queryRowSize();
        unsigned __int64 seq = manager->querySequence();
        if (translator)
        {
            MemoryBuffer buf;
            MemoryBufferBuilder aBuilder(buf, 0);
            size = translator->translate(aBuilder, callback, buffer);
            if (size)
            {
                // MORE - think about fpos
                keyBuilder->processKeyData((const char *) aBuilder.getSelf(), 0, size);
            }
        }
        else
        {
            if (hasTrailingFileposition(outmeta->queryTypeInfo()))
                size -= sizeof(offset_t);
            keyBuilder->processKeyData((const char *) buffer, manager->queryFPos(), size);
            if (size > maxSizeSeen)
                maxSizeSeen = size;
        }
        manager->releaseBlobs();
    }
    if (keyBuilder)
    {
        keyBuilder->finish(metadata, nullptr, maxSizeSeen);
        printf("New key has %" I64F "u leaves, %" I64F "u branches, %" I64F "u duplicates\n", keyBuilder->getStatistic(StNumLeafCacheAdds), keyBuilder->getStatistic(StNumNodeCacheAdds), keyBuilder->getStatistic(StNumDuplicateKeys));
        printf("Original key size: %" I64F "u bytes\n", const_cast<IFileIO *>(index->queryFileIO())->size());
        printf("New key size: %" I64F "u bytes (%" I64F "u bytes written in %" I64F "u writes)\n", outFileStream->size(), outFileStream->getStatistic(StSizeDiskWrite), outFileStream->getStatistic(StNumDiskWrites));
        keyBuilder.clear();
    }
    if (outRecType)
        outRecType->doDelete();

    ForEachItemIn(idx, deleteFields)
    {
        delete deleteFields.item(idx);
    }

    sizeRead = io->size();
}

//The index transform reads an entire index, and outputs the final index as a single block - no recovery etc.
size32_t CIndexTransformer::getBlock(IFileIOStream * out)
{
    rebuildIndex(inputFile, out, targetCompression);
    return 0;
}


//----------------------------------------------------------------------------

ITransformer * createIndexTransformer(const FileFormat & srcFormat, const FileFormat & tgtFormat, const char * keyCompression)
{
    return new CIndexTransformer(keyCompression);
}

ITransformer * createTransformer(const FileFormat & srcFormat, const FileFormat & tgtFormat, size32_t buffersize)
{
    ITransformer * transformer = NULL;

#ifdef OPTIMIZE_COMMON_TRANSFORMS
    if (srcFormat.equals(tgtFormat))
    {
        transformer = new CNullTransformer(buffersize);
    }
    else
    {
        switch (srcFormat.type)
        {
        case FFTfixed:
            switch (tgtFormat.type)
            {
            case FFTvariable:
            case FFTvariablebigendian:
                transformer = new CFixedToVarTransformer(srcFormat.recordSize,buffersize,(tgtFormat.type==FFTvariablebigendian));
                break;
            }
            break;
        case FFTvariable:
        case FFTvariablebigendian:
            switch (tgtFormat.type)
            {
            case FFTfixed:
                transformer = new CVarToFixedTransformer(tgtFormat.recordSize,buffersize,(srcFormat.type==FFTvariablebigendian));
                break;
            case FFTblocked:
                transformer = new CVarToBlockTransformer((srcFormat.type==FFTvariablebigendian));
                break;
            }
            break;
        case FFTblocked:
            switch (tgtFormat.type)
            {
            case FFTvariable:
            case FFTvariablebigendian:
                transformer = new CBlockToVarTransformer((tgtFormat.type==FFTvariablebigendian));
                break;
            }
            break;
        case FFTutf8: case FFTutf8n:
            switch (tgtFormat.type)
            {
            case FFTutf8n: 
            case FFTutf8:
                transformer = new CNullTransformer(buffersize);
                break;
            case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
                break;
            default:
                throwError(DFTERR_BadSrcTgtCombination);
            }
            break;
        case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
            switch (tgtFormat.type)
            {
            case FFTutf8: case FFTutf8n: case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
                break;
            default:
                throwError(DFTERR_BadSrcTgtCombination);
            }
            break;
        case FFTkey:
            throwError(DFTERR_BadSrcTgtCombination);
        }
    }
#endif

    if (!transformer)
        transformer = new CGeneralTransformer(srcFormat, tgtFormat);
//      throwError(DFTERR_BadSrcTgtCombination);
    
    return transformer;
}


//----------------------------------------------------------------------------

TransferServer::TransferServer(ISocket * _masterSocket)
{
    masterSocket = _masterSocket;
    lastTick = msTick();
    updateFrequency = (unsigned int) -1;
    throttleNicSpeed = 0;
    compressedInput = false;
    compressOutput = false;
    transferBufferSize = DEFAULT_STD_BUFFER_SIZE;
    fileUmask = -1;
}

void TransferServer::sendProgress(OutputProgress & curProgress)
{
    MemoryBuffer msg;
    msg.setEndian(__BIG_ENDIAN);
    curProgress.serializeCore(msg.clear().append(false));
    curProgress.serializeExtra(msg, 1);
    curProgress.serializeExtra(msg, 2);
    if (!catchWriteBuffer(masterSocket, msg))
        throwError(RFSERR_TimeoutWaitMaster);

    checkForRemoteAbort(masterSocket);
}

void TransferServer::appendTransformed(unsigned chunkIndex, ITransformer * input)
{
    OutputProgress & curProgress = progress.item(chunkIndex);
    PartitionPoint & curPartition = partition.item(chunkIndex);
    input->beginTransform(out);

    const offset_t startInputOffset = curPartition.inputOffset;
    const offset_t startOutputOffset = curPartition.outputOffset;
    stat_type prevNumWrites =  out->getStatistic(StNumDiskWrites);
    stat_type prevNumReads = input->getStatistic(StNumDiskReads);
    for (;;)
    {
        unsigned gotLength = input->getBlock(out);
        totalLengthRead  += gotLength;

        if (gpfFrequency || !gotLength || ((unsigned)(msTick() - lastTick)) > updateFrequency)
        {
            out->flush();

            lastTick = msTick();

            offset_t outputOffset = out->tell();
            offset_t inputOffset = input->tell();
            if (totalLengthToRead)
                LOG(MCdebugProgress, "Progress: %d%% done. [%" I64F "u]", (unsigned)(totalLengthRead*100/totalLengthToRead), (unsigned __int64)totalLengthRead);

            curProgress.status = (gotLength == 0) ? OutputProgress::StatusCopied : OutputProgress::StatusActive;
            curProgress.inputLength = input->tell()-startInputOffset;
            curProgress.outputLength = out->tell()-startOutputOffset;
            stat_type curNumWrites = out->getStatistic(StNumDiskWrites);
            stat_type curNumReads = input->getStatistic(StNumDiskReads);
            curProgress.numWrites += (curNumWrites - prevNumWrites);
            curProgress.numReads += (curNumReads - prevNumReads);
            prevNumWrites = curNumWrites;
            prevNumReads = curNumReads;
            if (crcOut)
                curProgress.outputCRC = crcOut->getCRC();
            if (calcInputCRC)
                curProgress.hasInputCRC = input->getInputCRC(curProgress.inputCRC);
            sendProgress(curProgress);
        }

        if (!gotLength)
            break;

        if (blockDelay)
            MilliSleep(blockDelay);
        else if (throttleNicSpeed)
        {
            unsigned delay = (unsigned)(((unsigned __int64)gotLength*10*1000*(numParallelSlaves-1))/(throttleNicSpeed * I64C(0x100000)));
            if (delay)
                MilliSleep(delay*(getRandom()%100)/50);
        }
#ifdef _WIN32
        if (gpfFrequency && ((fastRand() % gpfFrequency) == 0))
        {
            LOG(MCdebugInfo, "About to crash....");
            *(char *)0 = 0;
        }
#endif
    }

    input->endTransform(out);
}



void TransferServer::deserializeAction(MemoryBuffer & msg, unsigned action)
{
    SocketEndpoint ep;
    ep.deserialize(msg);
    if (!isContainerized() && !ep.isLocal())
    {
        StringBuffer host, expected;
        queryHostIP().getHostText(host);
        ep.getHostText(expected);
        throwError2(DFTERR_WrongComputer, expected.str(), host.str());
    }

    srcFormat.deserialize(msg);
    tgtFormat.deserialize(msg);
    msg.read(calcInputCRC);
    msg.read(calcOutputCRC);
    deserialize(partition, msg);
    msg.read(numParallelSlaves);
    msg.read(updateFrequency);
    msg.read(copySourceTimeStamp);
    msg.read(mirror);
    msg.read(isSafeMode);

    int adjust = get_cycles_now() % updateFrequency - (updateFrequency/2);
    lastTick = msTick() + adjust;

    StringBuffer localFilename;
    if (action == FTactionpull)
    {
        partition.item(0).outputName.getPath(localFilename);
        LOG(MCdebugProgress, "Process Pull Command: %s", localFilename.str());
    }
    else
    {
        partition.item(0).inputName.getPath(localFilename);
        LOG(MCdebugProgress, "Process Push Command: %s", localFilename.str());
    }
    LOG(MCdebugProgress, "Num Parallel Slaves=%d Adjust=%d/%d", numParallelSlaves, adjust, updateFrequency);
    LOG(MCdebugProgress, "copySourceTimeStamp(%d) mirror(%d) safe(%d) incrc(%d) outcrc(%d)", copySourceTimeStamp, mirror, isSafeMode, calcInputCRC, calcOutputCRC);

    displayPartition(partition);

    unsigned numProgress;
    msg.read(numProgress);
    for (unsigned i = 0; i < numProgress; i++)
    {
        OutputProgress & next = *new OutputProgress;
        next.deserializeCore(msg);
        progress.append(next);
    }
    if (msg.remaining())
        msg.read(throttleNicSpeed);
    if (msg.remaining())
        msg.read(compressedInput).read(compressOutput);
    if (msg.remaining())
        msg.read(copyCompressed);
    if (msg.remaining())
        msg.read(transferBufferSize);
    if (msg.remaining()) 
        msg.read(encryptKey).read(decryptKey);
    if (msg.remaining())
    {
        srcFormat.deserializeExtra(msg, 1);
        tgtFormat.deserializeExtra(msg, 1);
    }

    ForEachItemIn(i1, progress)
        progress.item(i1).deserializeExtra(msg, 1);

    if (msg.remaining())
        msg.read(fileUmask);
    if (msg.remaining())
    {
        ForEachItemIn(i2, progress)
            progress.item(i2).deserializeExtra(msg, 2);
    }
    if (msg.remaining())
        msg.readOpt(keyCompression);

    LOG(MCdebugProgress, "throttle(%d), transferBufferSize(%d)", throttleNicSpeed, transferBufferSize);
    PROGLOG("compressedInput(%d), compressedOutput(%d), copyCompressed(%d)", compressedInput?1:0, compressOutput?1:0, copyCompressed?1:0);
    PROGLOG("encrypt(%d), decrypt(%d)", encryptKey.isEmpty()?0:1, decryptKey.isEmpty()?0:1);
    if (fileUmask != -1)
        PROGLOG("umask(0%o)", fileUmask);
    else
        PROGLOG("umask(default)");

    //---Finished deserializing ---
    displayProgress(progress);

    totalLengthRead = 0;
    totalLengthToRead = 0;
    ForEachItemIn(idx, partition)
        totalLengthToRead += partition.item(idx).inputLength;
}


unsigned TransferServer::queryLastOutput(unsigned outputIndex)
{
    ForEachItemInRev(idx, partition)
        if (partition.item(idx).whichOutput == outputIndex)
            return idx;
    return (unsigned int) -1;
}

void TransferServer::transferChunk(unsigned chunkIndex)
{
    PartitionPoint & curPartition = partition.item(chunkIndex);
    OutputProgress & curProgress = progress.item(chunkIndex);

    StringBuffer targetPath;
    curPartition.outputName.getPath(targetPath);
    LOG(MCdebugProgress, "Begin to transfer chunk %d (offset: %" I64F "d, size: %" I64F "d) to target:'%s' (offset: %" I64F "d, size: %" I64F "d) ",
                        chunkIndex, curPartition.inputOffset, curPartition.inputLength, targetPath.str(), curPartition.outputOffset, curPartition.outputLength);
    const unsigned __int64 startOutOffset = out->tell();
    if (startOutOffset != curPartition.outputOffset+curProgress.outputLength)
        throwError4(DFTERR_OutputOffsetMismatch, out->tell(), curPartition.outputOffset+curProgress.outputLength, "start", chunkIndex);
    
    size32_t fixedTextLength = (size32_t)curPartition.fixedText.length();
    if (fixedTextLength || curPartition.inputName.isNull())
    {
        stat_type prevWrites = out->getStatistic(StNumDiskWrites);
        out->write(fixedTextLength, curPartition.fixedText.get());
        curProgress.status = OutputProgress::StatusCopied;
        curProgress.inputLength = fixedTextLength;
        curProgress.outputLength = fixedTextLength;
        curProgress.numWrites += (out->getStatistic(StNumDiskWrites)-prevWrites);
        if (crcOut)
            curProgress.outputCRC = crcOut->getCRC();
        sendProgress(curProgress);
    }
    else
    {
        Owned<ITransformer> transformer;
        if (keyCompression)
            transformer.setown(createIndexTransformer(srcFormat, tgtFormat, keyCompression));
        else
            transformer.setown(createTransformer(srcFormat, tgtFormat, transferBufferSize));
        if (!transformer->setPartition(curPartition.inputName, 
                                       curPartition.inputOffset+curProgress.inputLength, 
                                       curPartition.inputLength-curProgress.inputLength,
                                       compressedInput,
                                       decryptKey))
        {
            StringBuffer temp;
            throwError1(DFTERR_CouldNotOpenFile, curPartition.inputName.getRemotePath(temp).str());
        }

        if (calcInputCRC)
            transformer->setInputCRC(curProgress.inputCRC);

        appendTransformed(chunkIndex, transformer);
    }

    assertex(out->tell() == curPartition.outputOffset + curProgress.outputLength);
    if (copyCompressed)
    {
        //Now the copy of this chunk is complete, update the progress with the full expected length.
        //Don't do it before otherwise recovery won't work very well.
        curProgress.outputLength = curPartition.outputLength;
    }
    else
    {
        if (curPartition.outputLength && (curProgress.outputLength != curPartition.outputLength))
            throwError4(DFTERR_OutputOffsetMismatch, out->tell(), curPartition.outputOffset+curPartition.outputLength, "end", chunkIndex);
    }
}

bool TransferServer::pull()
{
    unsigned curOutput = (unsigned)-1;
    unsigned start;
    unsigned __int64 curOutputOffset = 0;
    //loop through all partitions - inner loop does a file at a time.
    unsigned numPartitions = partition.ordinality();
    for (start = 0; start < numPartitions; )
    {
        PartitionPoint & startPartition = partition.item(start);
        OutputProgress & startProgress = progress.item(start);

        if (startProgress.status == OutputProgress::StatusBegin)
            break;

        assertex(!compressOutput);
        RemoteFilename localTempFilename;
        getDfuTempName(localTempFilename, startPartition.outputName);
        OwnedIFile outFile = createIFile(localTempFilename);
        OwnedIFileIO outio = outFile->openShared(IFOwrite,IFSHnone);
        unsigned __int64 size = outio ? outio->size() : 0;

        curOutput = startPartition.whichOutput;
        curOutputOffset = getHeaderSize(tgtFormat.type);
        unsigned i;
        for (i = start;i < numPartitions; i++)
        {
            PartitionPoint & curPartition = partition.item(i);
            OutputProgress & curProgress = progress.item(i);

            if (curOutput != curPartition.whichOutput)
                break;
            curPartition.outputOffset = curOutputOffset;
            unsigned __int64 progressOffset = curOutputOffset + curProgress.outputLength;
            if (progressOffset > size)
            {
                LOG(MCwarning, "Recovery information seems to be invalid (%" I64F "d %" I64F "d) start copying from the beginning",
                         size, progressOffset);
                //reset any remaining partitions...
                for (i = start; i < numPartitions; i++)
                    progress.item(i).reset();
                curOutput = (unsigned int) -1;
                goto processedProgress; // break out of both loops
            }
            assertex(curProgress.status != OutputProgress::StatusRenamed);
            if (curProgress.status != OutputProgress::StatusCopied)
            {
                if (out)
                    out->close();
                out.setown(createIOStream(outio));
                out->seek(progressOffset, IFSbegin);
                wrapOutInCRC(curProgress.outputCRC);

                StringBuffer localFilename;
                localTempFilename.getPath(localFilename);
                LOG(MCdebugProgress, "Continue pulling to file: %s from recovery position", localFilename.str());
                start = i;
                goto processedProgress; // break out of both loops
            }
            curOutputOffset += curProgress.outputLength;
        }
        start = i;
    }

processedProgress:

    //Delete any output files before generating the new ones.
    unsigned maxChunk = partition.ordinality();
    if (((start == 0) && !isSafeMode))
    {
        unsigned prevOutput = (unsigned int) -1;
        for (unsigned i = 0; i < maxChunk; i++)
        {
            PartitionPoint & curPartition = partition.item(i);
            if (curPartition.whichOutput != prevOutput)
            {
                OwnedIFile output = createIFile(curPartition.outputName);
                output->remove();
                prevOutput = curPartition.whichOutput;
            }
        }
    }

    for (unsigned idx=start; idx<maxChunk; idx++)
    {
        PartitionPoint & curPartition = partition.item(idx);
        OutputProgress & curProgress = progress.item(idx);

        //Either first non-recovery file, or the target file has changed....
        if (curOutput != curPartition.whichOutput)
        {
            curOutput = curPartition.whichOutput;
            if (curProgress.status == OutputProgress::StatusRenamed)
            {
                LOG(MCdebugProgress, "Renamed file found - must be CRC recovery");
                idx = queryLastOutput(curOutput);
                continue;
            }

            const RemoteFilename & outputFilename = curPartition.outputName;
            const auto & fsProperties = outputFilename.queryFileSystemProperties();
            RemoteFilename localTempFilename;
            if (!fsProperties.canRename)
                localTempFilename.set(outputFilename);
            else
                getDfuTempName(localTempFilename, outputFilename);

            StringBuffer localFilename;
            localTempFilename.getPath(localFilename);
            if (!recursiveCreateDirectoryForFile(localFilename))
                throw makeOsExceptionV(GetLastError(), "Failed to create directory for file: %s", localFilename.str());

            OwnedIFile outFile = createIFile(localFilename.str());
            // if we want spray to not fill page cache use IFEnocache
            OwnedIFileIO outio = outFile->openShared(IFOcreate,IFSHnone,IFEnocache);
            if (!outio)
                throwError1(DFTERR_CouldNotCreateOutput, localFilename.str());
            if (compressOutput) {
                Owned<ICompressor> compressor;
                if (!encryptKey.isEmpty()) {
                    StringBuffer key;
                    decrypt(key,encryptKey);
                    compressor.setown(createAESCompressor256(key.length(),key.str()));
                }
                outio.setown(createCompressedFileWriter(outio, false, 0, true, compressor, COMPRESS_METHOD_LZ4));
            }

            LOG(MCdebugProgress, "Start pulling to file: %s", localFilename.str());

            //Find the last partition entry that refers to the same file.
            if (!compressOutput && fsProperties.preExtendOutput)
            {
                PartitionPoint & lastChunk = partition.item(queryLastOutput(curOutput));
                if (lastChunk.outputLength)
                {
                    char null = 0;
                    offset_t lastOffset = lastChunk.outputOffset+lastChunk.outputLength;
                    stat_type prevWrites = outio->getStatistic(StNumDiskWrites);
                    outio->write(lastOffset-sizeof(null),sizeof(null),&null);
                    curProgress.numWrites += (outio->getStatistic(StNumDiskWrites)-prevWrites);
                    LOG(MCdebugProgress, "Extend length of target file to %" I64F "d", lastOffset);
                }
            }

            if (out)
                out->close();
            out.setown(createIOStream(outio));
            out->seek(0, IFSbegin);
            wrapOutInCRC(0);

            unsigned headerLen = getHeaderSize(tgtFormat.type);
            if (headerLen)
                out->write(headerLen, getHeaderText(tgtFormat.type));
            curOutputOffset = headerLen;
        }
        else if (crcOut && (idx!=start))
            crcOut->setCRC(0);

        curPartition.outputOffset = curOutputOffset;
        transferChunk(idx);
        curOutputOffset += curProgress.outputLength;
    }

    crcOut.clear();
    if (out)
        out->close();
    out.clear();

    //Once the transfers have completed, rename the files, and sync file times
    //if replicating...
    if (!isSafeMode)
    {
        MemoryBuffer msg;
        unsigned prevOutput = (unsigned int) -1;
        for (unsigned i = 0; i < maxChunk; i++)
        {
            PartitionPoint & curPartition = partition.item(i);
            OutputProgress & curProgress = progress.item(i);
            const RemoteFilename & outputFilename = curPartition.outputName;
            const auto & fsProperties = outputFilename.queryFileSystemProperties();
            if (curPartition.whichOutput != prevOutput)
            {
                if (curProgress.status != OutputProgress::StatusRenamed)
                {
                    if (fsProperties.canRename)
                        renameDfuTempToFinal(curPartition.outputName);

                    OwnedIFile output = createIFile(curPartition.outputName);
                    if (fileUmask != -1)
                        output->setFilePermissions(~fileUmask&0666);

                    if (mirror || copySourceTimeStamp)
                    {
                        OwnedIFile input = createIFile(curPartition.inputName);
                        CDateTime modifiedTime;
                        CDateTime createTime;
                        if (input->getTime(&createTime, &modifiedTime, NULL))
                            output->setTime(&createTime, &modifiedTime, NULL);
                    }
                    else if (!curPartition.modifiedTime.isNull())
                    {
                        output->setTime(&curPartition.modifiedTime, &curPartition.modifiedTime, NULL);
                    }
                    else
                        output->getTime(NULL, &curProgress.resultTime, NULL);

                    if (compressOutput)
                    {
                        curProgress.compressedPartSize = output->size();
                        curProgress.hasCompressed = true;
                    }

                    //Notify the master that the file has been renamed - and send the modified time.
                    msg.setEndian(__BIG_ENDIAN);
                    curProgress.status = OutputProgress::StatusRenamed;
                    curProgress.serializeCore(msg.clear().append(false));
                    curProgress.serializeExtra(msg, 1);
                    curProgress.serializeExtra(msg, 2);
                    if (!catchWriteBuffer(masterSocket, msg))
                        throwError(RFSERR_TimeoutWaitMaster);
                }

                prevOutput = curPartition.whichOutput;
            }
        }
    }

    return true;
}

bool TransferServer::push()
{
    //May be multiple sources files, and may not read all the chunks from the source file so opened each time..
    //Slightly inefficent, but not significant because it is likely to be local
    unsigned maxChunk = partition.ordinality();
    for (unsigned idx=0;idx<maxChunk;idx++)
    {
        PartitionPoint & curPartition = partition.item(idx);
        OutputProgress & curProgress = progress.item(idx);
        if (curProgress.status != OutputProgress::StatusCopied)
        {
            RemoteFilename outFilename;
            getDfuTempName(outFilename, curPartition.outputName);

            OwnedIFile output = createIFile(outFilename);
            OwnedIFileIO outio = output->openShared(compressOutput?IFOreadwrite:IFOwrite,IFSHfull);
            if (!outio)
            {
                StringBuffer outputPath;
                outFilename.getRemotePath(outputPath);
                throwError1(DFTERR_CouldNotCreateOutput, outputPath.str());
            }
            if (compressOutput) {
                Owned<ICompressor> compressor;
                if (!encryptKey.isEmpty()) {
                    StringBuffer key;
                    decrypt(key,encryptKey);
                    compressor.setown(createAESCompressor256(key.length(),key.str()));
                }
                outio.setown(createCompressedFileWriter(outio, false, 0, true, compressor, COMPRESS_METHOD_LZ4));
            }

            out.setown(createIOStream(outio));
            if (!compressOutput)
                out->seek(curPartition.outputOffset + curProgress.outputLength, IFSbegin);
            wrapOutInCRC(curProgress.outputCRC);

            transferChunk(idx);
            if (compressOutput)
            {
                //Notify the master that the file compressed and its new size
                curProgress.compressedPartSize = output->size();
                curProgress.hasCompressed = true;
                sendProgress(curProgress);
            }
            crcOut.clear();
            out->close();
            out.clear();
        }
    }

    return true;
}

void TransferServer::wrapOutInCRC(unsigned startCRC)
{
    if (calcOutputCRC)
    {
        crcOut.setown(new CrcIOStream(out, startCRC));
        out.set(crcOut);
    }
}

