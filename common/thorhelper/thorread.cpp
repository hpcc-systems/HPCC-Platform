/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#include "thorfile.hpp"

#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
#include "rtldynfield.hpp"

#include "rmtclient.hpp"
#include "rmtfile.hpp"

#include "thorread.hpp"
#include "rtlcommon.hpp"
#include "thorcommon.hpp"

//---------------------------------------------------------------------------------------------------------------------

constexpr size32_t defaultReadBufferSize = 0x10000;

class DiskRowReader : extends CInterfaceOf<IAllocRowStream>, implements IRawRowStream, implements IDiskRowReader, implements IThorDiskCallback
{
public:
    DiskRowReader(unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options);
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IAllocRowStream>)

    virtual IRawRowStream * queryRawRowStream() override;
    virtual IAllocRowStream * queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator) override;

    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options) override;

// IThorDiskCallback
    virtual offset_t getFilePosition(const void * row) override;
    virtual offset_t getLocalFilePosition(const void * row) override;
    virtual const char * queryLogicalFilename(const void * row) override;
    virtual const byte * lookupBlob(unsigned __int64 id) override { UNIMPLEMENTED; }


protected:
    offset_t getLocalOffset();

protected:
    Owned<ISerialStream> input;
    Owned<IFileIO> inputfileio;
    CThorContiguousRowBuffer inputBuffer;
    Owned<IEngineRowAllocator> outputAllocator;
    Owned<const IDynamicTransform> translator;
    Owned<const IKeyTranslator> keyedTranslator;
    Linked<IOutputMetaData> expectedDiskMeta = nullptr;
    Linked<IOutputMetaData> projectedDiskMeta = nullptr;
    Linked<IOutputMetaData> actualDiskMeta = nullptr;
    unsigned expectedCrc = 0;
    unsigned projectedCrc = 0;
    unsigned actualCrc = 0;
    MemoryBuffer encryptionKey;
    size32_t readBufferSize = defaultReadBufferSize;
    bool grouped = false;
    bool stranded = false;
    bool compressed = false;
    bool blockcompressed = false;
    bool rowcompressed = false;

//The following refer to the current input file:
    offset_t fileBaseOffset = 0;
    StringAttr logicalFilename;
    unsigned filePart = 0;
};


DiskRowReader::DiskRowReader(unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
: expectedCrc(_expectedCrc), expectedDiskMeta(&_expected), projectedCrc(_projectedCrc), projectedDiskMeta(&_projected), actualCrc(_actualCrc), actualDiskMeta(&_actual)
{
    assertex(options);

    //Not sure this should really be being passed in here...
    RecordTranslationMode translationMode = (RecordTranslationMode)options->getPropInt("translationMode", (int)RecordTranslationMode::All);
    //MORE: HPCC-22287 This is too late to be able to do something different if the keyed filters cannot be translated.
    getTranslators(translator, keyedTranslator, "BinaryDiskRowReader", expectedCrc, expectedDiskMeta, actualCrc, actualDiskMeta, projectedCrc, projectedDiskMeta, translationMode);

    //Options contain information that is the same for each file that is being read, and potentially expensive to reconfigure.
    if (options->hasProp("encryptionKey"))
    {
        encryptionKey.resetBuffer();
        options->getPropBin("encryptionKey", encryptionKey);
    }
    readBufferSize = options->getPropInt("readBufferSize", defaultReadBufferSize);
}

IRawRowStream * DiskRowReader::queryRawRowStream()
{
    return this;
}

IAllocRowStream * DiskRowReader::queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator)
{
    outputAllocator.set(_outputAllocator);
    return this;
}

void DiskRowReader::clearInput()
{
    inputBuffer.setStream(nullptr);
    input.clear();
}

bool DiskRowReader::matches(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
{
    if ((expectedCrc != _expectedCrc) || (projectedCrc != _projectedCrc) || (actualCrc != _actualCrc))
        return false;

    //MORE: Check translation mode

    //MORE: Is the previous check sufficient?  If not, once getDaliLayoutInfo is cached the following line could be enabled.
    //if ((expectedDiskMeta != &_expected) || (projectedDiskMeta != &_projected) || (actualDiskMeta != &_actual))
    //    return false;

    if (options->hasProp("encryptionKey"))
    {
        MemoryBuffer tempEncryptionKey;
        options->getPropBin("encryptionKey", tempEncryptionKey);
        if (!encryptionKey.matches(tempEncryptionKey))
            return false;
    }
    if (readBufferSize != options->getPropInt("readBufferSize", defaultReadBufferSize))
        return false;
    return true;
}


bool DiskRowReader::getCursor(MemoryBuffer & cursor)
{
    return true;
}

void DiskRowReader::setCursor(MemoryBuffer & cursor)
{
}

void DiskRowReader::stop()
{
}


// IThorDiskCallback
offset_t DiskRowReader::getFilePosition(const void * row)
{
    return getLocalOffset() + fileBaseOffset;
}

offset_t DiskRowReader::getLocalFilePosition(const void * row)
{
    return makeLocalFposOffset(filePart, getLocalOffset());
}

const char * DiskRowReader::queryLogicalFilename(const void * row)
{
    return logicalFilename;
}

offset_t DiskRowReader::getLocalOffset()
{
    return inputBuffer.tell();
}

//---------------------------------------------------------------------------------------------------------------------

class LocalDiskRowReader : public DiskRowReader
{
public:
    LocalDiskRowReader(unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options);

    virtual bool matches(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options) override;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;

protected:
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter);

protected:
    IConstArrayOf<IFieldFilter> expectedFilter;  // These refer to the expected layout
};


LocalDiskRowReader::LocalDiskRowReader(unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
: DiskRowReader(_expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options)
{
}

bool LocalDiskRowReader::matches(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
{
    if (streamRemote)
        return false;
    return DiskRowReader::matches(format, streamRemote, _expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options);
}


bool LocalDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & _expectedFilter)
{
    assertex(meta);
    grouped = meta->getPropBool("grouped");
    compressed = meta->getPropBool("compressed", false);
    blockcompressed = meta->getPropBool("blockCompressed", false);
    bool forceCompressed = meta->getPropBool("forceCompressed", false);

    logicalFilename.set(_logicalFilename);
    filePart = _partNumber;
    fileBaseOffset = _baseOffset;

    try
    {
        if (!inputFile->exists())
            return false;
    }
    catch (IException *e)
    {
        EXCLOG(e, "DiskReadStage::setInputFile()");
        e->Release();
        return false;
    }

    size32_t dfsRecordSize = meta->getPropInt("dfsRecordSize");
    size32_t fixedDiskRecordSize = actualDiskMeta->getFixedSize();
    if (dfsRecordSize)
    {
        if (fixedDiskRecordSize)
        {
            if (grouped)
                fixedDiskRecordSize++;
            if (!((dfsRecordSize == fixedDiskRecordSize) || (grouped && (dfsRecordSize+1 == fixedDiskRecordSize)))) //last for backwards compatibility, as hthor used to publish @recordSize not including the grouping byte
                throw MakeStringException(0, "Published record size %d for file %s does not match coded record size %d", dfsRecordSize, logicalFilename.str(), fixedDiskRecordSize);

            if (!compressed && forceCompressed && (fixedDiskRecordSize >= MIN_ROWCOMPRESS_RECSIZE))
            {
                StringBuffer msg;
                msg.append("Ignoring compression attribute on file ").append(logicalFilename.str()).append(", which is not published as compressed");
                WARNLOG("%s", msg.str());
                //MORE: No simple way to do this, unless we are passed an engine context:
                //agent.addWuException(msg.str(), WRN_MismatchCompressInfo, SeverityWarning, "hthor");
                compressed = true;
            }
        }
    }
    else
    {
        if (!compressed && forceCompressed)
        {
            if ((fixedDiskRecordSize == 0) || (fixedDiskRecordSize + (grouped?1:0) >= MIN_ROWCOMPRESS_RECSIZE))
                compressed = true;
        }
    }

    rowcompressed = false;
    if (compressed)
    {
        Owned<IExpander> eexp;
        if (encryptionKey.length()!=0)
            eexp.setown(createAESExpander256((size32_t)encryptionKey.length(),encryptionKey.bufferBase()));
        inputfileio.setown(createCompressedFileReader(inputFile,eexp));
        if(!inputfileio && !blockcompressed) //fall back to old decompression, unless dfs marked as new
        {
            inputfileio.setown(inputFile->open(IFOread));
            if(inputfileio)
                rowcompressed = true;
        }
    }
    else
        inputfileio.setown(inputFile->open(IFOread));
    if (!inputfileio)
        return false;

    expectedFilter.clear();
    ForEachItemIn(i, _expectedFilter)
        expectedFilter.append(OLINK(_expectedFilter.item(i)));
    return true;
}

bool LocalDiskRowReader::setInputFile(const char * localFilename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
{
    Owned<IFile> inputFile = createIFile(localFilename);
    return setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, meta, expectedFilter);
}

bool LocalDiskRowReader::setInputFile(const RemoteFilename & filename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
{
    Owned<IFile> inputFile = createIFile(filename);
    return setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, meta, expectedFilter);
}



//---------------------------------------------------------------------------------------------------------------------

class BinaryDiskRowReader : public LocalDiskRowReader
{
public:
    BinaryDiskRowReader(unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options);

    virtual const void *nextRow() override;
    virtual const void *nextRow(size32_t & resultSize) override;
    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options) override;

protected:
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;

    inline bool segMonitorsMatch(const void * buffer)
    {
        if (actualFilter.numFilterFields())
        {
            const RtlRecord &actual = actualDiskMeta->queryRecordAccessor(true);
            unsigned numOffsets = actual.getNumVarFields() + 1;
            size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
            RtlRow row(actual, nullptr, numOffsets, variableOffsets);
            row.setRow(buffer, 0);  // Use lazy offset calculation
            return actualFilter.matches(row);
        }
        else
            return true;
    }

    size32_t getFixedDiskRecordSize();

protected:
    MemoryBuffer tempOutputBuffer;
    MemoryBufferBuilder bufferBuilder;
    ISourceRowPrefetcher * actualRowPrefetcher = nullptr;
    RowFilter actualFilter;               // This refers to the actual disk layout
    bool eogPending = false;
    bool needToTranslate;
};


BinaryDiskRowReader::BinaryDiskRowReader(unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
: LocalDiskRowReader(_expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options), bufferBuilder(tempOutputBuffer, 0)
{
    actualRowPrefetcher = actualDiskMeta->createDiskPrefetcher();
    needToTranslate = (translator && translator->needsTranslate());
}


void BinaryDiskRowReader::clearInput()
{
    DiskRowReader::clearInput();
    eogPending = false;
}

bool BinaryDiskRowReader::matches(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
{
    if (!strieq(format, "thor") && !strieq(format, "flat"))
        return false;
    return LocalDiskRowReader::matches(format, streamRemote, _expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options);
}

bool BinaryDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
{
    if (!LocalDiskRowReader::setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, meta, expectedFilter))
        return false;

    actualFilter.clear();
    actualFilter.appendFilters(expectedFilter);
    if (keyedTranslator)
        keyedTranslator->translate(actualFilter);

    unsigned __int64 filesize = inputfileio->size();
    if (!compressed && getFixedDiskRecordSize() && ((offset_t)-1 != filesize) && (filesize % getFixedDiskRecordSize()) != 0)
    {
        StringBuffer s;
        s.append("File ").append(inputFile->queryFilename()).append(" size is ").append(filesize).append(" which is not a multiple of ").append(getFixedDiskRecordSize());
        throw makeStringException(MSGAUD_user, 1, s.str());
    }

    //MORE: Allow a previously created input stream to be reused to avoid reallocating the buffer
    input.setown(createFileSerialStream(inputfileio, 0, filesize, readBufferSize));

    inputBuffer.setStream(input);
    eogPending = false;
    return true;
}

//Implementation of IAllocRowStream
const void *BinaryDiskRowReader::nextRow()
{
    for (;;)
    {
        //This may return multiple eog in a row with no intervening records - e.g. if all stripped by keyed filter.
        //It is up to the caller to filter duplicates (to avoid the overhead of multiple pieces of code checking)
        //Multiple eogs should also be harmless if the engines switch to this representation.
        if (eogPending)
        {
            eogPending = false;
            return eogRow;
        }

        inputBuffer.finishedRow();
        if (inputBuffer.eos())
            return eofRow;

        //Currently each row in a stranded file contains a flag to indicate if the next is an end of strand.
        //Is there a better way storing this (and combining it with the eog markers)?
        if (stranded)
        {
            bool eosPending;
            inputBuffer.read(eosPending);
            if (eosPending)
                return eosRow;

            //Call finishRow() so it is not included in the row pointer.  This should be special cased in the base class
            inputBuffer.finishedRow();
            if (inputBuffer.eos())
                return eofRow;
        }

        actualRowPrefetcher->readAhead(inputBuffer);
        size32_t sizeRead = inputBuffer.queryRowSize();
        if (grouped)
            inputBuffer.read(eogPending);
        const byte * next = inputBuffer.queryRow();

        if (likely(segMonitorsMatch(next))) // NOTE - keyed fields are checked pre-translation
        {
            if (needToTranslate)
            {
                RtlDynamicRowBuilder builder(outputAllocator);  // MORE: Make this into a member to reduce overhead
                size32_t size = translator->translate(builder, *this, next);
                return builder.finalizeRowClear(size);
            }
            else
            {
                size32_t allocatedSize;
                void * result = outputAllocator->createRow(sizeRead, allocatedSize);
                memcpy(result, next, sizeRead);
                return outputAllocator->finalizeRow(sizeRead, result, allocatedSize);
            }
        }
    }
}


//Implementation of IRawRowStream
const void *BinaryDiskRowReader::nextRow(size32_t & resultSize)
{
    //Similar to above, except the code at the end will translate to a local buffer or return the pointer
    for (;;)
    {
        //This may return multiple eog in a row with no intervening records - e.g. if all stripped by keyed filter.
        //It is up to the caller to filter duplicates (to avoid the overhead of multiple pieces of code checking)
        //Multiple eogs should also be harmless if the engines switch to this representation.
        if (eogPending)
        {
            eogPending = false;
            return eogRow;
        }

        inputBuffer.finishedRow();
        if (inputBuffer.eos())
            return eofRow;

        //Currently each row in a stranded file contains a flag to indicate if the next is an end of strand.
        //Is there a better way storing this (and combining it with the eog markers)?
        if (stranded)
        {
            bool eosPending;
            inputBuffer.read(eosPending);
            if (eosPending)
                return eosRow;

            //Call finishRow() so it is not included in the row pointer.  This should be special cased in the base class
            inputBuffer.finishedRow();
            if (inputBuffer.eos())
                return eofRow;
        }

        actualRowPrefetcher->readAhead(inputBuffer);
        size32_t sizeRead = inputBuffer.queryRowSize();
        if (grouped)
            inputBuffer.read(eogPending);
        const byte * next = inputBuffer.queryRow();

        if (likely(segMonitorsMatch(next))) // NOTE - keyed fields are checked pre-translation
        {
            if (needToTranslate)
            {
                //MORE: optimize the case where fields are lost off the end, and not bother translating - but return the modified size.
                tempOutputBuffer.clear();
                resultSize = translator->translate(bufferBuilder, *this, next);
                const void * ret = bufferBuilder.getSelf();
                bufferBuilder.finishRow(resultSize);
                return ret;
            }
            else
            {
                resultSize = sizeRead;
                return next;
            }
        }
    }
}


//Common to IAllocRowStream and IRawRowStream
bool BinaryDiskRowReader::getCursor(MemoryBuffer & cursor)
{
    //Is the following needed?
    inputBuffer.finishedRow();
    //save position, eog if grouped, and anything else that is required.
    return false;
}

void BinaryDiskRowReader::setCursor(MemoryBuffer & cursor)
{
}

void BinaryDiskRowReader::stop()
{
}


// IDiskRowReader

size32_t BinaryDiskRowReader::getFixedDiskRecordSize()
{
    size32_t fixedDiskRecordSize = actualDiskMeta->getFixedSize();
    if (fixedDiskRecordSize && grouped)
        fixedDiskRecordSize += 1;
    return fixedDiskRecordSize;
}


//---------------------------------------------------------------------------------------------------------------------


class RemoteDiskRowReader : public LocalDiskRowReader
{
public:
    RemoteDiskRowReader(const char * _format, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options);

    virtual const void *nextRow() override;
    virtual const void *nextRow(size32_t & resultSize) override;
    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options) override;

// IDiskRowReader
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;

protected:
    ISourceRowPrefetcher * projectedRowPrefetcher = nullptr;
    StringAttr format;
    RecordTranslationMode translationMode;
    bool eogPending = false;
};


RemoteDiskRowReader::RemoteDiskRowReader(const char * _format, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
: LocalDiskRowReader(_expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options), format(_format)
{
    translationMode = (RecordTranslationMode)options->getPropInt("translationMode", (int)RecordTranslationMode::All);
    projectedRowPrefetcher = projectedDiskMeta->createDiskPrefetcher();
}

void RemoteDiskRowReader::clearInput()
{
    DiskRowReader::clearInput();
    eogPending = false;
}

bool RemoteDiskRowReader::matches(const char * _format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
{
    if (!streamRemote)
        return false;
    if (!strieq(format, _format))
        return false;
    return LocalDiskRowReader::matches(format, streamRemote, _expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options);
}

bool RemoteDiskRowReader::setInputFile(const RemoteFilename & rfilename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilters)
{
    // NB: only binary handles can be remotely processed by dafilesrv at the moment

    // Open a stream from remote file, having passed actual, expected, projected, and filters to it
    SocketEndpoint ep(rfilename.queryEndpoint());
    setDafsEndpointPort(ep);

    StringBuffer localPath;
    rfilename.getLocalPath(localPath);

    RowFilter actualFilter;
    actualFilter.appendFilters(expectedFilters);

    if (keyedTranslator)
        keyedTranslator->translate(actualFilter);

    //MORE: This needs to be passed to this function - either in the meta or another parameter
    unsigned __int64 remoteLimit = 0;
    //MORE: Need to serialize the translation mode..
    Owned<IRemoteFileIO> remoteFileIO = createRemoteFilteredFile(ep, localPath, actualDiskMeta, projectedDiskMeta, actualFilter, compressed, grouped, remoteLimit);
    if (remoteFileIO)
    {
        StringBuffer tmp;
        remoteFileIO->addVirtualFieldMapping("logicalFilename", _logicalFilename);
        remoteFileIO->addVirtualFieldMapping("baseFpos", tmp.clear().append(_baseOffset).str());
        remoteFileIO->addVirtualFieldMapping("partNum", tmp.clear().append(_partNumber).str());

        try
        {
            remoteFileIO->ensureAvailable(); // force open now, because want to failover to other copies or legacy if fails
        }
        catch (IException *e)
        {
    #ifdef _DEBUG
            EXCLOG(e, nullptr);
    #endif
            e->Release();
            return false;
        }

        Owned<IFile> iFile = createIFile(rfilename);

        // remote side does projection/translation/filtering
        inputfileio.setown(remoteFileIO.getClear());
        if (!inputfileio)
            return false;
    }

    //MORE: Allow a previously created input stream to be reused to avoid reallocating the buffer
    input.setown(createFileSerialStream(inputfileio, 0, (offset_t)-1, readBufferSize));

    inputBuffer.setStream(input);
    eogPending = false;
    return true;
}

bool RemoteDiskRowReader::setInputFile(const char * localFilename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
{
    throwUnexpected();
}



//Implementation of IAllocRowStream
const void *RemoteDiskRowReader::nextRow()
{
    for (;;)
    {
        //This may return multiple eog in a row with no intervening records - e.g. if all stripped by keyed filter.
        //It is up to the caller to filter duplicates (to avoid the overhead of multiple pieces of code checking)
        //Multiple eogs should also be harmless if the engines switch to this representation.
        if (eogPending)
        {
            eogPending = false;
            return eogRow;
        }

        inputBuffer.finishedRow();
        if (inputBuffer.eos())
            return eofRow;

        //Currently each row in a stranded file contains a flag to indicate if the next is an end of strand.
        //Is there a better way storing this (and combining it with the eog markers)?
        if (stranded)
        {
            bool eosPending;
            inputBuffer.read(eosPending);
            if (eosPending)
                return eosRow;

            //Call finishRow() so it is not included in the row pointer.  This should be special cased in the base class
            inputBuffer.finishedRow();
            if (inputBuffer.eos())
                return eofRow;
        }

        projectedRowPrefetcher->readAhead(inputBuffer);
        size32_t sizeRead = inputBuffer.queryRowSize();
        if (grouped)
            inputBuffer.read(eogPending);
        const byte * next = inputBuffer.queryRow();

        size32_t allocatedSize;
        void * result = outputAllocator->createRow(sizeRead, allocatedSize);
        memcpy(result, next, sizeRead);
        return outputAllocator->finalizeRow(sizeRead, result, allocatedSize);
    }
}


//Implementation of IRawRowStream
const void *RemoteDiskRowReader::nextRow(size32_t & resultSize)
{
    //Similar to above, except the code at the end will translate to a local buffer or return the pointer
    for (;;)
    {
        //This may return multiple eog in a row with no intervening records - e.g. if all stripped by keyed filter.
        //It is up to the caller to filter duplicates (to avoid the overhead of multiple pieces of code checking)
        //Multiple eogs should also be harmless if the engines switch to this representation.
        if (eogPending)
        {
            eogPending = false;
            return eogRow;
        }

        inputBuffer.finishedRow();
        if (inputBuffer.eos())
            return eofRow;

        //Currently each row in a stranded file contains a flag to indicate if the next is an end of strand.
        //Is there a better way storing this (and combining it with the eog markers)?
        if (stranded)
        {
            bool eosPending;
            inputBuffer.read(eosPending);
            if (eosPending)
                return eosRow;

            //Call finishRow() so it is not included in the row pointer.  This should be special cased in the base class
            inputBuffer.finishedRow();
            if (inputBuffer.eos())
                return eofRow;
        }

        projectedRowPrefetcher->readAhead(inputBuffer);
        size32_t sizeRead = inputBuffer.queryRowSize();
        if (grouped)
            inputBuffer.read(eogPending);
        const byte * next = inputBuffer.queryRow();
        resultSize = sizeRead;
        return next;
    }
}


//Common to IAllocRowStream and IRawRowStream
bool RemoteDiskRowReader::getCursor(MemoryBuffer & cursor)
{
    //Is the following needed?
    inputBuffer.finishedRow();
    //save position, eog if grouped, and anything else that is required.
    return false;
}

void RemoteDiskRowReader::setCursor(MemoryBuffer & cursor)
{
}

void RemoteDiskRowReader::stop()
{
}


///---------------------------------------------------------------------------------------------------------------------


IDiskRowReader * createLocalDiskReader(const char * format, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
{
    if (strieq(format, "thor") || strieq(format, "flat"))
        return new BinaryDiskRowReader(_expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options);

    UNIMPLEMENTED;
}


IDiskRowReader * createRemoteDiskReader(const char * format, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
{
    return new RemoteDiskRowReader(format, _expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options);
}

IDiskRowReader * createDiskReader(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options)
{
    if (streamRemote)
        return createRemoteDiskReader(format, _expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options);
    else
        return createLocalDiskReader(format, _expectedCrc, _expected, _projectedCrc, _projected, _actualCrc, _actual, options);
}
