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
#include "roxiemem.hpp"

#include "rmtclient.hpp"
#include "rmtfile.hpp"

#include "thorread.hpp"
#include "rtlcommon.hpp"
#include "thorcommon.hpp"
#include "csvsplitter.hpp"

//---------------------------------------------------------------------------------------------------------------------

/*
 * A class that implements IDiskReadMapping - which provides all the information representing a translation from actual->expected->projected.
 */
//It might be sensible to have result structure which is (mode, expected, projected) shared by all actual->result mappings
class DiskReadMapping : public CInterfaceOf<IDiskReadMapping>
{
public:
    DiskReadMapping(RecordTranslationMode _mode, const char * _format, unsigned _actualCrc, IOutputMetaData & _actual, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _output, const IPropertyTree * _options)
    : mode(_mode), format(_format), actualCrc(_actualCrc), actualMeta(&_actual), expectedCrc(_expectedCrc), expectedMeta(&_expected), projectedCrc(_projectedCrc), projectedMeta(&_output), options(_options)
    {}

    virtual const char * queryFormat() const override { return format; }
    virtual unsigned getActualCrc() const override { return actualCrc; }
    virtual unsigned getExpectedCrc() const override { return expectedCrc; }
    virtual unsigned getProjectedCrc() const override { return projectedCrc; }
    virtual IOutputMetaData * queryActualMeta() const override { return actualMeta; }
    virtual IOutputMetaData * queryExpectedMeta() const override{ return expectedMeta; }
    virtual IOutputMetaData * queryProjectedMeta() const override{ return projectedMeta; }
    virtual const IPropertyTree * queryOptions() const override { return options; }
    virtual RecordTranslationMode queryTranslationMode() const override { return mode; }

    virtual const IDynamicTransform * queryTranslator() const override
    {
        ensureTranslators();
        return translator;
    }
    virtual const IKeyTranslator *queryKeyedTranslator() const override
    {
        ensureTranslators();
        return keyedTranslator;
    }

    virtual bool matches(const IDiskReadMapping * other) const
    {
        return mode == other->queryTranslationMode() && streq(format, other->queryFormat()) &&
                ((actualCrc && actualCrc == other->getActualCrc()) || (actualMeta == other->queryActualMeta())) &&
                ((expectedCrc && expectedCrc == other->getExpectedCrc()) || (expectedMeta == other->queryExpectedMeta())) &&
                ((projectedCrc && projectedCrc == other->getProjectedCrc()) || (projectedMeta == other->queryProjectedMeta())) &&
                areMatchingPTrees(options, other->queryOptions());
    }

    virtual bool expectedMatchesProjected() const
    {
        return (expectedCrc && (expectedCrc == projectedCrc)) || (expectedMeta == projectedMeta);
    }

protected:
    void ensureTranslators() const;

protected:
    RecordTranslationMode mode;
    mutable std::atomic<bool> checkedTranslators = { false };
    StringAttr format;
    unsigned actualCrc;
    unsigned expectedCrc;
    unsigned projectedCrc;
    Linked<IOutputMetaData> actualMeta;
    Linked<IOutputMetaData> expectedMeta;
    Linked<IOutputMetaData> projectedMeta;
    Linked<const IPropertyTree> options;
    mutable Owned<const IDynamicTransform> translator;
    mutable Owned<const IKeyTranslator> keyedTranslator;
    mutable SpinLock translatorLock; // use a spin lock since almost certainly not going to contend
};

void DiskReadMapping::ensureTranslators() const
{
    if (checkedTranslators.load())
        return;
    SpinBlock block(translatorLock);
    if (checkedTranslators.load())
        return;

    const char * filename = ""; // not known at this point
    IOutputMetaData * sourceMeta = expectedMeta;
    unsigned sourceCrc = expectedCrc;
    if (mode == RecordTranslationMode::AlwaysECL)
    {
        if (actualCrc && expectedCrc && (actualCrc != expectedCrc))
            DBGLOG("Overriding stored record layout reading file %s", filename);
    }
    else
    {
        if (actualCrc && actualMeta)
        {
            sourceMeta = actualMeta;
            sourceCrc = actualCrc;
        }

        if (actualCrc && expectedCrc && (actualCrc != expectedCrc) && (RecordTranslationMode::None == mode))
            throwTranslationError(actualMeta->queryRecordAccessor(true), expectedMeta->queryRecordAccessor(true), filename);
    }

    //This has a very low possibility of Meta crcs accidentally matching, which could lead to a crashes on an untranslated files.
    const RtlRecord & projectedRecord = projectedMeta->queryRecordAccessor(true);
    const RtlRecord & sourceRecord = sourceMeta->queryRecordAccessor(true);
    if (strsame(format, "csv"))
    {
        type_vals format = options->hasProp("ascii") ? type_string : type_utf8;
        translator.setown(createRecordTranslatorViaCallback(projectedRecord, sourceRecord, format));
    }
    else if (strsame(format, "xml"))
    {
        translator.setown(createRecordTranslatorViaCallback(projectedRecord, sourceRecord, type_utf8));
    }
    else
    {
        if ((projectedMeta != sourceMeta) && (projectedCrc != sourceCrc))
            translator.setown(createRecordTranslator(projectedRecord, sourceRecord));
    }

    if (translator)
    {
        DBGLOG("Record layout translator created for %s", filename);
        translator->describe();

        if (!translator->canTranslate())
            throw MakeStringException(0, "Untranslatable record layout mismatch detected for file %s", filename);

        if (translator->needsTranslate())
        {
            if (sourceMeta != expectedMeta)
            {
                Owned<const IKeyTranslator> _keyedTranslator = createKeyTranslator(sourceMeta->queryRecordAccessor(true), expectedMeta->queryRecordAccessor(true));
                //MORE: What happens if the key filters cannot be translated?
                if (_keyedTranslator->needsTranslate())
                    keyedTranslator.swap(_keyedTranslator);
            }
        }
        else
            translator.clear();
    }

    checkedTranslators = true;
}

THORHELPER_API IDiskReadMapping * createDiskReadMapping(RecordTranslationMode mode, const char * format, unsigned actualCrc, IOutputMetaData & actual, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, IOutputMetaData & projected, const IPropertyTree * options)
{
    assertex(expectedCrc);
    assertex(options);
    return new DiskReadMapping(mode, format, actualCrc, actual, expectedCrc, expected, projectedCrc, projected, options);
}

THORHELPER_API IDiskReadMapping * createUnprojectedMapping(IDiskReadMapping * mapping)
{
    return createDiskReadMapping(mapping->queryTranslationMode(), mapping->queryFormat(), mapping->getActualCrc(), *mapping->queryActualMeta(), mapping->getExpectedCrc(), *mapping->queryExpectedMeta(), mapping->getExpectedCrc(), *mapping->queryExpectedMeta(), mapping->queryOptions());
}


//---------------------------------------------------------------------------------------------------------------------

constexpr size32_t defaultReadBufferSize = 0x10000;

/*
 * The base class for reading rows from an external file.  Each activity will have an instance of a disk reader for
 * each actual file format.
 */
class DiskRowReader : public CInterfaceOf<IDiskRowStream>, implements IDiskRowReader, implements IThorDiskCallback
{
public:
    DiskRowReader(IDiskReadMapping * _mapping);
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IDiskRowStream>)

    virtual IDiskRowStream * queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator) override;

    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * format, bool streamRemote, IDiskReadMapping * mapping) override;

// IThorDiskCallback
    virtual offset_t getFilePosition(const void * row) override;
    virtual offset_t getLocalFilePosition(const void * row) override;
    virtual const char * queryLogicalFilename(const void * row) override;
    virtual const byte * lookupBlob(unsigned __int64 id) override { UNIMPLEMENTED; }


protected:
    virtual offset_t getLocalOffset();

protected:
    Owned<ISerialStream> inputStream;
    Owned<IFileIO> inputfileio;
    CThorContiguousRowBuffer inputBuffer;    // more: move to derived classes.
    Owned<IEngineRowAllocator> outputAllocator;
    RtlDynamicRowBuilder allocatedBuilder;
    const IDynamicTransform * translator = nullptr;
    const IKeyTranslator * keyedTranslator = nullptr;
    Linked<IDiskReadMapping> mapping;
    IOutputMetaData * actualDiskMeta = nullptr;
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


DiskRowReader::DiskRowReader(IDiskReadMapping * _mapping)
: mapping(_mapping), actualDiskMeta(_mapping->queryActualMeta()), allocatedBuilder(nullptr)
{
    //Options contain information that is the same for each file that is being read, and potentially expensive to reconfigure.
    translator = mapping->queryTranslator();
    keyedTranslator = mapping->queryKeyedTranslator();
    const IPropertyTree * options = mapping->queryOptions();
    if (options->hasProp("encryptionKey"))
    {
        encryptionKey.resetBuffer();
        options->getPropBin("encryptionKey", encryptionKey);
    }
    readBufferSize = options->getPropInt("readBufferSize", defaultReadBufferSize);
}

IDiskRowStream * DiskRowReader::queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator)
{
    outputAllocator.set(_outputAllocator);
    allocatedBuilder.setAllocator(_outputAllocator);
    return this;
}

void DiskRowReader::clearInput()
{
    inputBuffer.setStream(nullptr);
    inputStream.clear();
}

bool DiskRowReader::matches(const char * format, bool streamRemote, IDiskReadMapping * otherMapping)
{
    if (!mapping->matches(otherMapping))
        return false;

    //MORE: Check translation mode

    //MORE: Is the previous check sufficient?  If not, once getDaliLayoutInfo is cached the following line could be enabled.
    //if ((expectedDiskMeta != &_expected) || (projectedDiskMeta != &_projected) || (actualDiskMeta != &_actual))
    //    return false;

    const IPropertyTree * options = otherMapping->queryOptions();
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

/*
 * base class for reading a local file (or a remote file via the block based IFile interface)
 */
class LocalDiskRowReader : public DiskRowReader
{
public:
    LocalDiskRowReader(IDiskReadMapping * _mapping);

    virtual bool matches(const char * format, bool streamRemote, IDiskReadMapping * otherMapping) override;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;

protected:
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter);
    virtual bool isBinary() const = 0;

protected:
    IConstArrayOf<IFieldFilter> expectedFilter;  // These refer to the expected layout
    MemoryBuffer tempOutputBuffer;
    MemoryBufferBuilder bufferBuilder;
};


LocalDiskRowReader::LocalDiskRowReader(IDiskReadMapping * _mapping)
: DiskRowReader(_mapping), bufferBuilder(tempOutputBuffer, 0)
{
}

bool LocalDiskRowReader::matches(const char * format, bool streamRemote, IDiskReadMapping * otherMapping)
{
    if (streamRemote)
        return false;
    return DiskRowReader::matches(format, streamRemote, otherMapping);
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

    if (isBinary())
    {
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
                    //agent.addWuException(msg.str(), WRN_MismatchCompressInfo, SeverityWarning, MSGAUD_user, "hthor");
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

    unsigned __int64 filesize = inputfileio->size();
    //MORE: Allow a previously created input stream to be reused to avoid reallocating the buffer
    inputStream.setown(createFileSerialStream(inputfileio, 0, filesize, readBufferSize));

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

/*
 * base class for reading a binary local file
 */
class BinaryDiskRowReader : public LocalDiskRowReader
{
public:
    BinaryDiskRowReader(IDiskReadMapping * _mapping);

    virtual const void *nextRow() override;
    virtual const void *nextRow(size32_t & resultSize) override;
    virtual const void * nextRow(MemoryBufferBuilder & builder) override;
    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * format, bool streamRemote, IDiskReadMapping * otherMapping) override;

protected:
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;
    virtual bool isBinary() const { return true; }

    inline bool fieldFilterMatch(const void * buffer)
    {
        if (actualFilter.numFilterFields())
        {
            unsigned numOffsets = actualRecord->getNumVarFields() + 1;
            size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
            RtlRow row(*actualRecord, nullptr, numOffsets, variableOffsets);
            row.setRow(buffer, 0);  // Use lazy offset calculation
            return actualFilter.matches(row);
        }
        else
            return true;
    }

    size32_t getFixedDiskRecordSize();

private:
    template <class PROCESS>
    inline const void * inlineNextRow(PROCESS processor) __attribute__((always_inline));

protected:
    ISourceRowPrefetcher * actualRowPrefetcher = nullptr;
    const RtlRecord  * actualRecord = nullptr;
    RowFilter actualFilter;               // This refers to the actual disk layout
    bool eogPending = false;
    bool needToTranslate;
};


BinaryDiskRowReader::BinaryDiskRowReader(IDiskReadMapping * _mapping)
: LocalDiskRowReader(_mapping)
{
    actualRowPrefetcher = actualDiskMeta->createDiskPrefetcher();
    actualRecord = &actualDiskMeta->queryRecordAccessor(true);
    needToTranslate = (translator && translator->needsTranslate());
}


void BinaryDiskRowReader::clearInput()
{
    LocalDiskRowReader::clearInput();
    eogPending = false;
}

bool BinaryDiskRowReader::matches(const char * format, bool streamRemote, IDiskReadMapping * otherMapping)
{
    if (!strieq(format, "flat"))
        return false;
    return LocalDiskRowReader::matches(format, streamRemote, otherMapping);
}

bool BinaryDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
{
    if (!LocalDiskRowReader::setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, meta, expectedFilter))
        return false;

    actualFilter.clear().appendFilters(expectedFilter);
    if (keyedTranslator)
        keyedTranslator->translate(actualFilter);

    unsigned __int64 filesize = inputfileio->size();
    if (!compressed && getFixedDiskRecordSize() && ((offset_t)-1 != filesize) && (filesize % getFixedDiskRecordSize()) != 0)
    {
        StringBuffer s;
        s.append("File ").append(inputFile->queryFilename()).append(" size is ").append(filesize).append(" which is not a multiple of ").append(getFixedDiskRecordSize());
        throw makeStringException(MSGAUD_user, 1, s.str());
    }

    inputBuffer.setStream(inputStream);
    eogPending = false;
    return true;
}

template <class PROCESS>
const void *BinaryDiskRowReader::inlineNextRow(PROCESS processor)
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

        if (likely(fieldFilterMatch(next))) // NOTE - keyed fields are checked pre-translation
            return processor(sizeRead, next);
    }
}


//Implementation of IAllocRowStream, return a row allocated with roxiemem
const void *BinaryDiskRowReader::nextRow()
{
    return inlineNextRow(
        [this](size32_t sizeRead, const byte * next)
        {
            if (needToTranslate)
            {
                size32_t size = translator->translate(allocatedBuilder.ensureRow(), *this, next);
                return allocatedBuilder.finalizeRowClear(size);
            }
            else
            {
                size32_t allocatedSize;
                void * result = outputAllocator->createRow(sizeRead, allocatedSize);
                memcpy(result, next, sizeRead);
                return (const void *)outputAllocator->finalizeRow(sizeRead, result, allocatedSize);
            }
        }
    );
}


//Similar to above, except the code at the end will translate to a local buffer or return the pointer
const void *BinaryDiskRowReader::nextRow(size32_t & resultSize)
{
    return inlineNextRow(
        [this,&resultSize](size32_t sizeRead, const byte * next)
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
                return (const void *)next;
            }
        }
    );
}

//return a row allocated within a MemoryBufferBuilder
const void *BinaryDiskRowReader::nextRow(MemoryBufferBuilder & builder)
{
    return inlineNextRow(
        [this,&builder](size32_t sizeRead, const byte * next)
        {
            //MORE: optimize the case where fields are lost off the end, and not bother translating - but return the modified size.
            if (needToTranslate)
            {
                size32_t resultSize = translator->translate(builder, *this, next);
                const void * ret = builder.getSelf();
                builder.finishRow(resultSize);
                return ret;
            }
            else
            {
                builder.appendBytes(sizeRead, next);
                return (const void *)(builder.getSelf() - sizeRead);
            }
        }
    );
}




//Common to IAllocRowStream and IRawRowStream
bool BinaryDiskRowReader::getCursor(MemoryBuffer & cursor)
{
    //Is the following needed?
    inputBuffer.finishedRow();

    cursor.append(inputBuffer.tell());
    cursor.append(eogPending);
    return true;
}

void BinaryDiskRowReader::setCursor(MemoryBuffer & cursor)
{
    unsigned __int64 startPos;
    cursor.read(startPos);
    cursor.read(eogPending);

    if (inputBuffer.tell() != startPos)
        inputBuffer.reset(startPos);
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

/*
 * base class for reading a non-binary local file that uses IDynamicFieldValueFetcher to extract the values from
 * the input data file.
 */
class ExternalFormatDiskRowReader : public LocalDiskRowReader
{
public:
    ExternalFormatDiskRowReader(IDiskReadMapping * _mapping) : LocalDiskRowReader(_mapping)
    {
        projectedRecord = &mapping->queryProjectedMeta()->queryRecordAccessor(true);
    }

    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & _expectedFilter) override
    {
        if (!LocalDiskRowReader::setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, meta, _expectedFilter))
            return false;

        projectedFilter.clear().appendFilters(_expectedFilter);

        //If the following is false then needs keyedTranslator code - but mapping from expected to PROJECTED
        assertex(mapping->expectedMatchesProjected() || projectedFilter.numFilterFields() == 0);
        //if (keyedTranslator)
        //    keyedTranslator->translate(projectedFilter);

        return true;
    }

    //Common to IAllocRowStream and IRawRowStream
    virtual bool getCursor(MemoryBuffer & cursor) override
    {
        cursor.append(inputStream->tell());
        return true;
    }

    virtual void setCursor(MemoryBuffer & cursor) override
    {
        unsigned __int64 startPos;
        cursor.read(startPos);
        if (inputStream->tell() != startPos)
            inputStream->reset(startPos);
    }

    virtual offset_t getLocalOffset() override
    {
        return inputStream->tell();
    }

protected:
    virtual bool isBinary() const { return false; }

protected:
    Owned<const IDynamicFieldValueFetcher> fieldFetcher;
    RowFilter projectedFilter;
    const RtlRecord * projectedRecord = nullptr;
};

class CNullNestedRowIterator : public CSimpleInterfaceOf<IDynamicRowIterator>
{
public:
    virtual bool first() override { return false; }
    virtual bool next() override { return false; }
    virtual bool isValid() override { return false; }
    virtual IDynamicFieldValueFetcher &query() override
    {
        throwUnexpected();
    }
};
static CNullNestedRowIterator nullNestedRowIterator;

//---------------------------------------------------------------------------------------------------------------------

/*
 * class for reading a csv local file
 */
class CsvDiskRowReader : public ExternalFormatDiskRowReader
{
private:
    class CFieldFetcher : public CSimpleInterfaceOf<IDynamicFieldValueFetcher>
    {
        CSVSplitter &csvSplitter;
        unsigned numInputFields;
    public:
        CFieldFetcher(CSVSplitter &_csvSplitter, unsigned _numInputFields) : csvSplitter(_csvSplitter), numInputFields(_numInputFields)
        {
        }
        virtual const byte *queryValue(unsigned fieldNum, size_t &sz) const override
        {
            dbgassertex(fieldNum < numInputFields);
            sz = csvSplitter.queryLengths()[fieldNum];
            return csvSplitter.queryData()[fieldNum];
        }
        virtual IDynamicRowIterator *getNestedIterator(unsigned fieldNum) const override
        {
            return LINK(&nullNestedRowIterator);
        }
        virtual size_t getSize(unsigned fieldNum) const override { throwUnexpected(); }
        virtual size32_t getRecordSize() const override { throwUnexpected(); }
    };

public:
    CsvDiskRowReader(IDiskReadMapping * _mapping);

    virtual const void *nextRow() override;
    virtual const void *nextRow(size32_t & resultSize) override;
    virtual const void *nextRow(MemoryBufferBuilder & builder) override;

    virtual void stop() override;

    virtual bool matches(const char * format, bool streamRemote, IDiskReadMapping * otherMapping) override;

protected:
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;

    void processOption(CSVSplitter::MatchItem element, const IPropertyTree & config, const char * option, const char * dft, const char * dft2 = nullptr);

    inline bool fieldFilterMatchProjected(const void * buffer)
    {
        if (projectedFilter.numFilterFields())
        {
            unsigned numOffsets = projectedRecord->getNumVarFields() + 1;
            size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
            RtlRow row(*projectedRecord, nullptr, numOffsets, variableOffsets);
            row.setRow(buffer, 0);  // Use lazy offset calculation
            return projectedFilter.matches(row);
        }
        else
            return true;
    }

    size32_t getFixedDiskRecordSize();

protected:
    constexpr static unsigned defaultMaxCsvRowSizeMB = 10;
    StringBuffer csvQuote, csvSeparate, csvTerminate, csvEscape;
    unsigned __int64 headerLines = 0;
    unsigned __int64 maxRowSize = 0;
    bool preserveWhitespace = false;
    CSVSplitter csvSplitter;
};


CsvDiskRowReader::CsvDiskRowReader(IDiskReadMapping * _mapping)
: ExternalFormatDiskRowReader(_mapping)
{
    const IPropertyTree & config = *mapping->queryOptions();

    maxRowSize = config.getPropInt64("maxRowSize", defaultMaxCsvRowSizeMB) * 1024 * 1024;
    preserveWhitespace = config.getPropBool("preserveWhitespace", false);
    preserveWhitespace = config.getPropBool("notrim", preserveWhitespace);

    const RtlRecord * inputRecord = &mapping->queryActualMeta()->queryRecordAccessor(true);
    unsigned numInputFields = inputRecord->getNumFields();
    csvSplitter.init(numInputFields, maxRowSize, csvQuote, csvSeparate, csvTerminate, csvEscape, preserveWhitespace);

    //MORE: How about options from the file? - test writing with some options and then reading without specifying them
    processOption(CSVSplitter::QUOTE, config, "quote", "\"");
    processOption(CSVSplitter::SEPARATOR, config, "separator", ",");
    processOption(CSVSplitter::TERMINATOR, config, "terminator", "\n", "\r\n");
    if (config.getProp("escape", csvEscape))
        csvSplitter.addEscape(csvEscape);

    headerLines = config.getPropInt64("heading");
    fieldFetcher.setown(new CFieldFetcher(csvSplitter, numInputFields));
}


bool CsvDiskRowReader::matches(const char * format, bool streamRemote, IDiskReadMapping * otherMapping)
{
    if (!strieq(format, "csv"))
        return false;
    return ExternalFormatDiskRowReader::matches(format, streamRemote, otherMapping);
}

void CsvDiskRowReader::processOption(CSVSplitter::MatchItem element, const IPropertyTree & config, const char * option, const char * dft, const char * dft2)
{
    if (config.hasProp(option))
    {
        bool useAscii = mapping->queryOptions()->hasProp("ascii");
        Owned<IPropertyTreeIterator> iter = config.getElements(option);
        ForEach(*iter)
        {
            const char * value = iter->query().queryProp("");
            StringBuffer temp;
            if (value && useAscii)
            {
                char * ascii = rtlUtf8ToVStr(rtlUtf8Length(strlen(value), value), value);
                csvSplitter.addItem(element, ascii);
                free(ascii);
            }
            else
                csvSplitter.addItem(element, value);
        }
    }
    else
    {
        csvSplitter.addItem(element, dft);
        if (dft2)
            csvSplitter.addItem(element, dft2);
    }
}

bool CsvDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & _expectedFilter)
{
    if (!ExternalFormatDiskRowReader::setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, meta, _expectedFilter))
        return false;

    //Skip any header lines..
    for (unsigned __int64 line = 0; line < headerLines; line++)
    {
        size32_t lineLength = csvSplitter.splitLine(inputStream, maxRowSize);
        if (0 == lineLength)
            break;
        inputStream->skip(lineLength);
    }

    return true;
}

//Implementation of IAllocRowStream
const void *CsvDiskRowReader::nextRow()
{
    for (;;) //while (processed < chooseN)
    {
        size32_t lineLength = csvSplitter.splitLine(inputStream, maxRowSize);
        if (!lineLength)
            break;

        size32_t resultSize = translator->translate(allocatedBuilder.ensureRow(), *this, *fieldFetcher);
        inputStream->skip(lineLength);
        roxiemem::OwnedConstRoxieRow result = allocatedBuilder.finalizeRowClear(resultSize);

        if (fieldFilterMatchProjected(result))
            return result.getClear();
    }
    return eofRow;
}


//Implementation of IRawRowStream
const void *CsvDiskRowReader::nextRow(size32_t & resultSize)
{
    for (;;)
    {
        size32_t lineLength = csvSplitter.splitLine(inputStream, maxRowSize);
        if (!lineLength)
            break;

        resultSize = translator->translate(bufferBuilder, *this, *fieldFetcher);
        dbgassertex(resultSize);
        const void *ret = bufferBuilder.getSelf();
        if (fieldFilterMatchProjected(ret))
        {
            bufferBuilder.finishRow(resultSize);
            inputStream->skip(lineLength);
            return ret;
        }
        else
            bufferBuilder.removeBytes(resultSize);
        inputStream->skip(lineLength);
    }
    resultSize = 0;
    return nullptr;
}

const void * CsvDiskRowReader::nextRow(MemoryBufferBuilder & builder)
{
    for (;;)
    {
        size32_t lineLength = csvSplitter.splitLine(inputStream, maxRowSize);
        if (!lineLength)
            break;

        size32_t resultSize = translator->translate(bufferBuilder, *this, *fieldFetcher);
        dbgassertex(resultSize);
        const void *ret = builder.getSelf();
        if (fieldFilterMatchProjected(ret))
        {
            builder.finishRow(resultSize);
            inputStream->skip(lineLength);
            return ret;
        }
        else
            builder.removeBytes(resultSize);
        inputStream->skip(lineLength);
    }
    return nullptr;
}




void CsvDiskRowReader::stop()
{
}


// IDiskRowReader

size32_t CsvDiskRowReader::getFixedDiskRecordSize()
{
    size32_t fixedDiskRecordSize = actualDiskMeta->getFixedSize();
    if (fixedDiskRecordSize && grouped)
        fixedDiskRecordSize += 1;
    return fixedDiskRecordSize;
}


//---------------------------------------------------------------------------------------------------------------------

/*
 * This class is used to project the input rows - for the situations where the disk reader cannot perform
 * all the filtering and projection that is required.
 */
class CompoundProjectRowReader : extends CInterfaceOf<IDiskRowStream>, implements IDiskRowReader
{
    Linked<IDiskReadMapping> mapping;
    Linked<IDiskRowReader> inputReader;
    UnexpectedVirtualFieldCallback unexpectedCallback;
    Owned<const IDynamicTransform> translator;
    MemoryBuffer tempOutputBuffer;
    MemoryBufferBuilder bufferBuilder;
    RtlDynamicRowBuilder allocatedBuilder;
    Linked<IEngineRowAllocator> outputAllocator;
    IDiskRowStream * rawInputStream;
public:
    CompoundProjectRowReader(IDiskRowReader * _input, IDiskReadMapping * _mapping)
    : inputReader(_input), mapping(_mapping), bufferBuilder(tempOutputBuffer, 0), allocatedBuilder(nullptr)
    {
        const RtlRecord &inRecord = mapping->queryExpectedMeta()->queryRecordAccessor(true);
        const RtlRecord &outRecord = mapping->queryProjectedMeta()->queryRecordAccessor(true);
        translator.setown(createRecordTranslator(outRecord, inRecord));
    }
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IDiskRowStream>)

    virtual IDiskRowStream * queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator)
    {
        allocatedBuilder.setAllocator(_outputAllocator);
        outputAllocator.set(_outputAllocator);
        return this;
    }

    virtual bool matches(const char * _format, bool _streamRemote, IDiskReadMapping * _mapping)
    {
        return false;
    }

    virtual void clearInput()
    {
        inputReader->clearInput();
        rawInputStream = nullptr;
    }

    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
    {
        if (inputReader->setInputFile(localFilename, logicalFilename, partNumber, baseOffset, meta, expectedFilter))
        {
            rawInputStream = inputReader->queryAllocatedRowStream(nullptr);
            return true;
        }
        return false;
    }

    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
    {
        if (inputReader->setInputFile(filename, logicalFilename, partNumber, baseOffset, meta, expectedFilter))
        {
            rawInputStream = inputReader->queryAllocatedRowStream(nullptr);
            return true;
        }
        return false;
    }

//interface IRowReader
    virtual bool getCursor(MemoryBuffer & cursor) { return rawInputStream->getCursor(cursor); }
    virtual void setCursor(MemoryBuffer & cursor) { rawInputStream->setCursor(cursor); }
    virtual void stop() { rawInputStream->stop(); }

    virtual const void *nextRow(size32_t & resultSize) override
    {
        size32_t rawInputSize;
        const void * next = rawInputStream->nextRow(rawInputSize);
        if (isSpecialRow(next))
            return next;

        //MORE: optimize the case where fields are lost off the end, and not bother translating - but return the modified size.
        tempOutputBuffer.clear();
        resultSize = translator->translate(bufferBuilder, unexpectedCallback, (const byte *)next);
        const void * ret = bufferBuilder.getSelf();
        bufferBuilder.finishRow(resultSize);
        return ret;
    }

    virtual const void *nextRow() override
    {
        size32_t rawInputSize;
        const void * next = rawInputStream->nextRow(rawInputSize);
        if (isSpecialRow(next))
            return next;

        size32_t size = translator->translate(allocatedBuilder.ensureRow(), unexpectedCallback, (const byte *)next);
        return allocatedBuilder.finalizeRowClear(size);
    }

    virtual const void *nextRow(MemoryBufferBuilder & builder) override
    {
        size32_t rawInputSize;
        const void * next = rawInputStream->nextRow(rawInputSize);
        if (isSpecialRow(next))
            return next;

        //MORE: optimize the case where fields are lost off the end, and not bother translating - but return the modified size.
        size32_t resultSize = translator->translate(builder, unexpectedCallback, (const byte *)next);
        const void * ret = builder.getSelf();
        bufferBuilder.finishRow(resultSize);
        return ret;
    }
};



/*
 * This class is used for formats which may or may not be able to perform all the filtering and projection that an
 * input dataset requires.   Depending on the filter it will add an extra layer of translation if required.
 */
class AlternativeDiskRowReader : public CInterfaceOf<IDiskRowReader>
{
public:
    AlternativeDiskRowReader(IDiskRowReader * projectedReader, IDiskRowReader * expectedReader, IDiskReadMapping * mapping)
    {
        directReader.set(projectedReader);
        compoundReader.setown(new CompoundProjectRowReader(expectedReader, mapping));
    }

    virtual IDiskRowStream * queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator)
    {
        assertex(activeReader);
        return activeReader->queryAllocatedRowStream(_outputAllocator);
    }

    virtual bool matches(const char * _format, bool _streamRemote, IDiskReadMapping * _mapping)
    {
        return directReader->matches(_format, _streamRemote, _mapping);
    }

    //Specify where the raw binary input for a particular file is coming from, together with its actual format.
    //Does this make sense, or should it be passed a filename?  an actual format?
    //Needs to specify a filename rather than a ISerialStream so that the interface is consistent for local and remote
    virtual void clearInput()
    {
        directReader->clearInput();
        compoundReader->clearInput();
        activeReader = nullptr;
    }

    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
    {
        bool useProjected = canFilterDirectly(expectedFilter);
        if (useProjected)
            activeReader = directReader;
        else
            activeReader = compoundReader;
        return activeReader->setInputFile(localFilename, logicalFilename, partNumber, baseOffset, meta, expectedFilter);
    }

    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
    {
        bool useProjected = canFilterDirectly(expectedFilter);
        if (useProjected)
            activeReader = directReader;
        else
            activeReader = compoundReader;
        return activeReader->setInputFile(filename, logicalFilename, partNumber, baseOffset, meta, expectedFilter);
    }

protected:
    bool canFilterDirectly(const FieldFilterArray & expectedFilter)
    {
        if (expectedFilter.ordinality() == 0)
            return true;
        //MORE: Check if all the fields being filtered are in the projected output
        return false;
    }

protected:
    Owned<IDiskRowReader> directReader;
    Owned<IDiskRowReader> compoundReader;
    IDiskRowReader * activeReader = nullptr;
};


//---------------------------------------------------------------------------------------------------------------------


/*
 * This class is used to read files that have been remotely filtered and projected by dafilesrv.
 */

class RemoteDiskRowReader : public DiskRowReader
{
public:
    RemoteDiskRowReader(const char * _format, IDiskReadMapping * _mapping);

    virtual const void *nextRow() override;
    virtual const void *nextRow(size32_t & resultSize) override;
    virtual const void *nextRow(MemoryBufferBuilder & builder) override;
    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * _format, bool _streamRemote, IDiskReadMapping * _mapping) override;

// IDiskRowReader
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) override;

private:
    template <class PROCESS>
    inline const void * inlineNextRow(PROCESS processor) __attribute__((always_inline));

protected:
    ISourceRowPrefetcher * projectedRowPrefetcher = nullptr;
    StringAttr format;
    RecordTranslationMode translationMode;
    bool eogPending = false;
};


RemoteDiskRowReader::RemoteDiskRowReader(const char * _format, IDiskReadMapping * _mapping)
: DiskRowReader(_mapping), format(_format)
{
    translationMode = mapping->queryTranslationMode();
    projectedRowPrefetcher = mapping->queryProjectedMeta()->createDiskPrefetcher();
}

void RemoteDiskRowReader::clearInput()
{
    DiskRowReader::clearInput();
    eogPending = false;
}

bool RemoteDiskRowReader::matches(const char * _format, bool _streamRemote, IDiskReadMapping * _mapping)
{
    if (!_streamRemote)
        return false;
    if (!strieq(format, _format))
        return false;
    return DiskRowReader::matches(_format, _streamRemote, _mapping);
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
    Owned<IRemoteFileIO> remoteFileIO = createRemoteFilteredFile(ep, localPath, actualDiskMeta, mapping->queryProjectedMeta(), actualFilter, compressed, grouped, remoteLimit);
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
    inputStream.setown(createFileSerialStream(inputfileio, 0, (offset_t)-1, readBufferSize));

    inputBuffer.setStream(inputStream);
    eogPending = false;
    return true;
}

bool RemoteDiskRowReader::setInputFile(const char * localFilename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter)
{
    throwUnexpected();
}


template <class PROCESS>
const void *RemoteDiskRowReader::inlineNextRow(PROCESS processor)
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

        return processor(sizeRead, next);
    }
}



//Implementation of IAllocRowStream
const void *RemoteDiskRowReader::nextRow()
{
    return inlineNextRow(
        [this](size32_t sizeRead, const byte * next)
        {
            size32_t allocatedSize;
            void * result = outputAllocator->createRow(sizeRead, allocatedSize);
            memcpy(result, next, sizeRead);
            return outputAllocator->finalizeRow(sizeRead, result, allocatedSize);
        }
    );
}


//Similar to above, except the code at the end will translate to a local buffer or return the pointer
const void *RemoteDiskRowReader::nextRow(size32_t & resultSize)
{
    return inlineNextRow(
        [this,&resultSize](size32_t sizeRead, const byte * next)
        {
            resultSize = sizeRead;
            return next;
        }
    );
}

//Experimental use of lambdas to common up a few function definitions.
const void *RemoteDiskRowReader::nextRow(MemoryBufferBuilder & builder)
{
    return inlineNextRow(
        [this,&builder](size32_t sizeRead, const byte * next)
        {
            builder.appendBytes(sizeRead, next);
            return (const void *)(builder.getSelf() - sizeRead);
        }
    );
}


bool RemoteDiskRowReader::getCursor(MemoryBuffer & cursor)
{
    throwUnexpected();
    return false;
}

void RemoteDiskRowReader::setCursor(MemoryBuffer & cursor)
{
    throwUnexpected();
}

void RemoteDiskRowReader::stop()
{
}


///---------------------------------------------------------------------------------------------------------------------


IDiskRowReader * doCreateLocalDiskReader(const char * format, IDiskReadMapping * _mapping)
{
    if (strieq(format, "flat"))
        return new BinaryDiskRowReader(_mapping);
    if (strieq(format, "csv"))
        return new CsvDiskRowReader(_mapping);

    UNIMPLEMENTED;
}


//3 possible cases
//   no filter
//   filter can be performed on the projected output
//   filter can only be performed on expected -> need to project to expected as a temporary row

IDiskRowReader * createLocalDiskReader(const char * format, IDiskReadMapping * mapping)
{
    Owned<IDiskRowReader> directReader = doCreateLocalDiskReader(format, mapping);
    if (mapping->expectedMatchesProjected() || strieq(format, "flat"))
        return directReader.getClear();

    Owned<IDiskReadMapping> expectedMapping = createUnprojectedMapping(mapping);
    Owned<IDiskRowReader> expectedReader = doCreateLocalDiskReader(format, expectedMapping);
    return new AlternativeDiskRowReader(directReader, expectedReader, mapping);
}


IDiskRowReader * createRemoteDiskReader(const char * format, IDiskReadMapping * _mapping)
{
    return new RemoteDiskRowReader(format, _mapping);
}



IDiskRowReader * createDiskReader(const char * format, bool streamRemote, IDiskReadMapping * _mapping)
{
    if (streamRemote)
        return createRemoteDiskReader(format, _mapping);
    else
        return createLocalDiskReader(format, _mapping);
}


/*

Aims:

- Avoid creating multiple translators for mappings from one format to another - especially subfiles.
  (Since cost of creating the mapping may be quite high.)
- Persist translators from query instance to query instance in roxie.
- Possibly share dynamic meta information between queries (e.g., same file used more than once).
  (since cost and size of creating the informaion isn't trivial - and may have knock on effects to
   allow more translators to be reused.)
- Share disk readers within an activity for all subfiles that have the same format
  (Creating stream readers and other internal allocations can be relatively expensive).
- Reuse disk readers for calls to a child query.  Similar reasons to sharing within an activity.
- It is assumed that projected is always a strict subset of expected


Complications
- IOutputMetaData cannot be shared between queries in roxie because the dll may be unloaded.
- Some filters cannot be converted from expected to actual.
    csv - no filters can be converted
    field mapping - if a field being filtered does not have a 1:1 mapping
  This conflicts with wanting to reuse as much as possible from time to time - e.g. between subqueries
  but the filter might be possible to apply to the projected if all fields are present.

Solutions:
 - Add a flag to IOutputMetaData to indicate the field information is dynamic (and not dependent on a dll)
 - If a filter cannot be translated, first project to expected, and then from expected to projected
 - Allow query<X>RowStream to return different pointers after setInputFile() is called.
 - setFilter() could be implemented as a separate call - it would avoid re-translating for subfiles, but may
   be slightly tricky to track whether it has been called.

 */
