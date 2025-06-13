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
#include "thorxmlread.hpp"

#ifdef _USE_PARQUET
    #include "parquetembed.hpp"
#endif

constexpr size32_t defaultReadBufferSize = 0x100000;

//---------------------------------------------------------------------------------------------------------------------

static IBufferedSerialInputStream * createInputStream(IFileIO * inputfileio, const IPropertyTree * providerOptions)
{
    assertex(providerOptions);
    size32_t readBufferSize = providerOptions->getPropInt("readBufferSize", defaultReadBufferSize);

    //MORE: Add support for passing these values to the function
    offset_t startOffset = 0;
    offset_t length = unknownFileSize;

    //MORE: Is this a good idea?
    if (length == unknownFileSize)
    {
        offset_t filesize = inputfileio->size();
        assertex(startOffset <= filesize);
        length = filesize - startOffset;
    }

    return createFileSerialStream(inputfileio, startOffset, length, readBufferSize);
};


static bool createInputStream(Shared<IBufferedSerialInputStream> & inputStream, Shared<IFileIO> & inputfileio, IFile * inputFile, const IPropertyTree * providerOptions)
{
    assertex(providerOptions);

    bool compressed = providerOptions->getPropBool("@compressed", false);
    bool blockcompressed = providerOptions->getPropBool("@blockCompressed", false);
    bool forceCompressed = providerOptions->getPropBool("@forceCompressed", false);

    MemoryBuffer encryptionKey;
    if (providerOptions->hasProp("encryptionKey"))
        providerOptions->getPropBin("encryptionKey", encryptionKey);

    bool rowcompressed = false;
    try
    {
        if (compressed)
        {
            Owned<IExpander> eexp;
            if (encryptionKey.length()!=0)
                eexp.setown(createAESExpander256((size32_t)encryptionKey.length(), encryptionKey.bufferBase()));
            inputfileio.setown(createCompressedFileReader(inputFile, eexp));
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
    }
    catch (IException *e)
    {
        EXCLOG(e, "createInputStream");
        e->Release();
        return false;
    }

    inputStream.setown(createInputStream(inputfileio, providerOptions));
    return true;
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * A class that implements IRowReadFormatMapping - which provides all the information representing a translation from actual->expected->projected.
 */
//It might be sensible to have result structure which is (mode, expected, projected) shared by all actual->result mappings
class DiskReadMapping : public CInterfaceOf<IRowReadFormatMapping>
{
public:
    DiskReadMapping(RecordTranslationMode _mode, const char * _format, unsigned _actualCrc, IOutputMetaData & _actual, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _output, const IPropertyTree * _formatOptions)
    : mode(_mode), format(_format), actualCrc(_actualCrc), expectedCrc(_expectedCrc), projectedCrc(_projectedCrc), actualMeta(&_actual), expectedMeta(&_expected), projectedMeta(&_output), formatOptions(_formatOptions)
    {}

    virtual const char * queryFormat() const override { return format; }
    virtual unsigned getActualCrc() const override { return actualCrc; }
    virtual unsigned getExpectedCrc() const override { return expectedCrc; }
    virtual unsigned getProjectedCrc() const override { return projectedCrc; }
    virtual IOutputMetaData * queryActualMeta() const override { return actualMeta; }
    virtual IOutputMetaData * queryExpectedMeta() const override{ return expectedMeta; }
    virtual IOutputMetaData * queryProjectedMeta() const override{ return projectedMeta; }
    virtual const IPropertyTree * queryFormatOptions() const override { return formatOptions; }
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

    virtual bool matches(const IRowReadFormatMapping * other) const
    {
        if ((mode != other->queryTranslationMode()) || !streq(format, other->queryFormat()))
            return false;
        //if crc is set, then a matching crc counts as a match, otherwise meta must be identical
        if (((actualCrc && actualCrc == other->getActualCrc()) || (actualMeta == other->queryActualMeta())) &&
            ((expectedCrc && expectedCrc == other->getExpectedCrc()) || (expectedMeta == other->queryExpectedMeta())) &&
            ((projectedCrc && projectedCrc == other->getProjectedCrc()) || (projectedMeta == other->queryProjectedMeta())))
        {
            if (!areMatchingPTrees(formatOptions, other->queryFormatOptions()))
                return false;
            return true;
        }
        return false;
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
    Linked<const IPropertyTree> formatOptions;
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
        type_vals format = formatOptions->hasProp("ascii") ? type_string : type_utf8;
        translator.setown(createRecordTranslatorViaCallback(projectedRecord, sourceRecord, format));
    }
    else if (strsame(format, "xml"))
    {
        translator.setown(createRecordTranslatorViaCallback(projectedRecord, sourceRecord, type_utf8));
    }
    else
    {
        if ((projectedMeta != sourceMeta) && (projectedCrc != sourceCrc))
        {
            //Special case the situation where the output record matches the input record with some virtual fields
            //appended.  This allows alien datatypes or ifblocks in records to also hav virtual file positions/
            if (formatOptions->getPropBool("@cloneAppendVirtuals"))
                translator.setown(createCloneVirtualRecordTranslator(projectedRecord, *sourceMeta));
            else
                translator.setown(createRecordTranslator(projectedRecord, sourceRecord));
        }
    }

    if (translator)
    {
        DBGLOG("Record layout translator created for %s", filename);
        translator->describe();

        if (!translator->canTranslate())
            throw MakeStringException(0, "Untranslatable record layout mismatch detected for file %s", filename);

        if (mode == RecordTranslationMode::PayloadRemoveOnly && translator->hasNewFields())
            throw MakeStringException(0, "Translatable file layout mismatch reading file %s but translation disabled when expected fields are missing from source.", filename);

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

THORHELPER_API IRowReadFormatMapping * createRowReadFormatMapping(RecordTranslationMode mode, const char * format, unsigned actualCrc, IOutputMetaData & actual, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, IOutputMetaData & projected, const IPropertyTree * formatOptions)
{
    assertex(expectedCrc);
    assertex(formatOptions);
    return new DiskReadMapping(mode, format, actualCrc, actual, expectedCrc, expected, projectedCrc, projected, formatOptions);
}

static IRowReadFormatMapping * createUnprojectedMapping(IRowReadFormatMapping * mapping)
{
    return createRowReadFormatMapping(mapping->queryTranslationMode(), mapping->queryFormat(), mapping->getActualCrc(), *mapping->queryActualMeta(), mapping->getExpectedCrc(), *mapping->queryExpectedMeta(), mapping->getExpectedCrc(), *mapping->queryExpectedMeta(), mapping->queryFormatOptions());
}


//---------------------------------------------------------------------------------------------------------------------

/*
 * The base class for reading rows from an external file.  Each activity will have an instance of a disk reader for
 * each actual file format.
 */
class DiskRowReader : public CInterfaceOf<ILogicalRowStream>, implements IDiskRowReader, implements IThorDiskCallback
{
public:
    DiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator);
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<ILogicalRowStream>)

    virtual ILogicalRowStream * queryAllocatedRowStream() override;

    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * format, bool streamRemote, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions) override;

// IThorDiskCallback
    virtual offset_t getFilePosition(const void * row) override;
    virtual offset_t getLocalFilePosition(const void * row) override;
    virtual const char * queryLogicalFilename(const void * row) override;
    virtual const byte * lookupBlob(unsigned __int64 id) override { UNIMPLEMENTED; }


protected:
    virtual offset_t getLocalOffset();

protected:
    Owned<IBufferedSerialInputStream> inputStream;
    Owned<IFileIO> inputfileio;
    CThorContiguousRowBuffer inputBuffer;    // more: move to derived classes.
    Owned<IEngineRowAllocator> outputAllocator;
    RtlDynamicRowBuilder allocatedBuilder;
    const IDynamicTransform * translator = nullptr;
    const IKeyTranslator * keyedTranslator = nullptr;
    Linked<IRowReadFormatMapping> mapping;
    Linked<const IPropertyTree> providerOptions;
    IOutputMetaData * actualDiskMeta = nullptr;
    bool grouped = false;
    bool stranded = false;
    bool compressed = false;

//The following refer to the current input file:
    offset_t fileBaseOffset = 0;
    StringAttr logicalFilename;
    unsigned filePart = 0;
};


DiskRowReader::DiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator)
: allocatedBuilder(nullptr), mapping(_mapping), providerOptions(_providerOptions), actualDiskMeta(_mapping->queryActualMeta())
{
    outputAllocator.set(_optOutputAllocator);
    allocatedBuilder.setAllocator(_optOutputAllocator);

    //Options contain information that is the same for each file that is being read, and potentially expensive to reconfigure.
    translator = mapping->queryTranslator();
    keyedTranslator = mapping->queryKeyedTranslator();

    const IPropertyTree * formatOptions = mapping->queryFormatOptions();
    grouped = formatOptions->getPropBool("@grouped"); // grouping is a feature of how the underlying byte stream is interpreted => format option

    assertex(providerOptions);
    compressed = providerOptions->getPropBool("@compressed", false);
}

ILogicalRowStream * DiskRowReader::queryAllocatedRowStream()
{
    return this;
}

void DiskRowReader::clearInput()
{
    inputBuffer.setStream(nullptr);
    inputStream.clear();
}

bool DiskRowReader::matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions)
{
    if (!mapping->matches(otherMapping))
        return false;

    if (!areMatchingPTrees(providerOptions, otherProviderOptions))
        return false;

    //MORE: Check translation mode

    //MORE: Is the previous check sufficient?  If not, once getDaliLayoutInfo is cached the following line could be enabled.
    //if ((expectedDiskMeta != &_expected) || (projectedDiskMeta != &_projected) || (actualDiskMeta != &_actual))
    //    return false;
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
    LocalDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator);

    virtual bool matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * _otherProviderOptions) override;
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override;

protected:
    IConstArrayOf<IFieldFilter> expectedFilter;  // These refer to the expected layout
    MemoryBuffer tempOutputBuffer;
    MemoryBufferBuilder bufferBuilder;
};


LocalDiskRowReader::LocalDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator)
: DiskRowReader(_mapping, _providerOptions, _optOutputAllocator), bufferBuilder(tempOutputBuffer, 0)
{
}

bool LocalDiskRowReader::matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions)
{
    if (streamRemote)
        return false;
    return DiskRowReader::matches(format, streamRemote, otherMapping, otherProviderOptions);
}

bool LocalDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & _expectedFilter)
{
    bool blockcompressed = providerOptions->getPropBool("@blockCompressed", false);
    bool forceCompressed = providerOptions->getPropBool("@forceCompressed", false);

    logicalFilename.set(_logicalFilename);
    filePart = _partNumber;
    fileBaseOffset = _baseOffset;

    size32_t readBufferSize = providerOptions->getPropInt("readBufferSize", defaultReadBufferSize);
    MemoryBuffer encryptionKey;
    if (providerOptions->hasProp("encryptionKey"))
        providerOptions->getPropBin("encryptionKey", encryptionKey);

    if (!createInputStream(inputStream, inputfileio, inputFile, providerOptions))
        return false;

    expectedFilter.kill();
    ForEachItemIn(i, _expectedFilter)
        expectedFilter.append(OLINK(_expectedFilter.item(i)));
    return true;
}

bool LocalDiskRowReader::setInputFile(const char * localFilename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const FieldFilterArray & expectedFilter)
{
    Owned<IFile> inputFile = createIFile(localFilename);
    return setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, 0, unknownFileSize, expectedFilter);
}

bool LocalDiskRowReader::setInputFile(const RemoteFilename & filename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const FieldFilterArray & expectedFilter)
{
    Owned<IFile> inputFile = createIFile(filename);
    return setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, 0, unknownFileSize, expectedFilter);
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * base class for reading a binary local file
 */
class BinaryDiskRowReader : public LocalDiskRowReader
{
public:
    BinaryDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator);

    virtual const void *nextRow() override;
    virtual const void *prefetchRow(size32_t & resultSize) override;
    virtual const void * nextRow(MemoryBufferBuilder & builder) override;
    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * providerOptions) override;

protected:
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) override;

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
    Owned<ISourceRowPrefetcher> actualRowPrefetcher;
    const RtlRecord  * actualRecord = nullptr;
    RowFilter actualFilter;               // This refers to the actual disk layout
    bool eogPending = false;
    bool needToTranslate;
};


BinaryDiskRowReader::BinaryDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator)
: LocalDiskRowReader(_mapping, _providerOptions, _optOutputAllocator)
{
    actualRowPrefetcher.setown(actualDiskMeta->createDiskPrefetcher());
    actualRecord = &actualDiskMeta->queryRecordAccessor(true);
    needToTranslate = (translator && translator->needsTranslate());

    bool forceCompressed = providerOptions->getPropBool("@forceCompressed", false);

    const IPropertyTree * formatOptions = mapping->queryFormatOptions();
    size32_t dfsRecordSize = formatOptions->getPropInt("@recordSize");
    size32_t fixedDiskRecordSize = actualDiskMeta->getFixedSize();
    if (dfsRecordSize)
    {
        if (fixedDiskRecordSize)
        {
            //Perform a sanity check on the size of a file containing fixed size records
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


void BinaryDiskRowReader::clearInput()
{
    LocalDiskRowReader::clearInput();
    eogPending = false;
}

bool BinaryDiskRowReader::matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions)
{
    if (!strieq(format, "flat"))
        return false;
    return LocalDiskRowReader::matches(format, streamRemote, otherMapping, otherProviderOptions);
}

bool BinaryDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter)
{
    if (!LocalDiskRowReader::setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, startOffset, length, expectedFilter))
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
const void *BinaryDiskRowReader::prefetchRow(size32_t & resultSize)
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
    ExternalFormatDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator) : LocalDiskRowReader(_mapping, _providerOptions, _optOutputAllocator)
    {
        projectedRecord = &mapping->queryProjectedMeta()->queryRecordAccessor(true);
    }

    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & _expectedFilter) override
    {
        if (!LocalDiskRowReader::setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, startOffset, length, _expectedFilter))
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
            inputStream->reset(startPos, UnknownOffset);
    }

    virtual offset_t getLocalOffset() override
    {
        return inputStream->tell();
    }

protected:
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
    CsvDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator);

    virtual const void *nextRow() override;
    virtual const void *prefetchRow(size32_t & resultSize) override;
    virtual const void *nextRow(MemoryBufferBuilder & builder) override;

    virtual void stop() override;

    virtual bool matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions) override;

protected:
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) override;

    void processOption(CSVSplitter::MatchItem element, const IPropertyTree & csvOptions, const char * option, const char * dft, const char * dft2 = nullptr);

protected:
    constexpr static unsigned defaultMaxCsvRowSizeMB = 10;
    StringBuffer csvQuote, csvSeparate, csvTerminate, csvEscape;
    unsigned __int64 headerLines = 0;
    unsigned __int64 maxRowSize = 0;
    bool preserveWhitespace = false;
    CSVSplitter csvSplitter;
};


CsvDiskRowReader::CsvDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator)
: ExternalFormatDiskRowReader(_mapping, _providerOptions, _optOutputAllocator)
{
    const IPropertyTree & csvOptions = *mapping->queryFormatOptions();

    maxRowSize = csvOptions.getPropInt64("maxRowSize", defaultMaxCsvRowSizeMB) * 1024 * 1024;
    preserveWhitespace = csvOptions.getPropBool("preserveWhitespace", false);
    preserveWhitespace = csvOptions.getPropBool("notrim", preserveWhitespace);

    const RtlRecord * inputRecord = &mapping->queryActualMeta()->queryRecordAccessor(true);
    unsigned numInputFields = inputRecord->getNumFields();
    csvSplitter.init(numInputFields, maxRowSize, csvQuote, csvSeparate, csvTerminate, csvEscape, preserveWhitespace);

    //MORE: How about options from the file? - test writing with some options and then reading without specifying them
    processOption(CSVSplitter::QUOTE, csvOptions, "quote", "\"");
    processOption(CSVSplitter::SEPARATOR, csvOptions, "separator", ",");
    processOption(CSVSplitter::TERMINATOR, csvOptions, "terminator", "\n", "\r\n");
    if (csvOptions.getProp("escape", csvEscape))
        csvSplitter.addEscape(csvEscape);

    headerLines = csvOptions.getPropInt64("heading");
    fieldFetcher.setown(new CFieldFetcher(csvSplitter, numInputFields));
}


bool CsvDiskRowReader::matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions)
{
    if (!strieq(format, "csv"))
        return false;
    return ExternalFormatDiskRowReader::matches(format, streamRemote, otherMapping, otherProviderOptions);
}

void CsvDiskRowReader::processOption(CSVSplitter::MatchItem element, const IPropertyTree & csvOptions, const char * option, const char * dft, const char * dft2)
{
    if (csvOptions.hasProp(option))
    {
        bool useAscii = csvOptions.hasProp("ascii");
        Owned<IPropertyTreeIterator> iter = csvOptions.getElements(option);
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

bool CsvDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & _expectedFilter)
{
    if (!ExternalFormatDiskRowReader::setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, startOffset, length, _expectedFilter))
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
const void *CsvDiskRowReader::prefetchRow(size32_t & resultSize)
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

        size32_t resultSize = translator->translate(builder, *this, *fieldFetcher);
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

//---------------------------------------------------------------------------------------------------------------------

class MarkupDiskRowReader : public ExternalFormatDiskRowReader, implements IXMLSelect
{
private:
    // JCSMORE - it would be good if these were cached/reused (anything using fetcher is single threaded)
    class CFieldFetcher : public CSimpleInterfaceOf<IDynamicFieldValueFetcher>
    {
        unsigned numInputFields;
        const RtlRecord &recInfo;
        Linked<IColumnProvider> currentMatch;
        const char **compoundXPaths = nullptr;

        const char *queryCompoundXPath(unsigned fieldNum) const
        {
            if (compoundXPaths && compoundXPaths[fieldNum])
                return compoundXPaths[fieldNum];
            else
                return recInfo.queryXPath(fieldNum);
        }
    public:
        CFieldFetcher(const RtlRecord &_recInfo, IColumnProvider *_currentMatch) : recInfo(_recInfo), currentMatch(_currentMatch)
        {
            numInputFields = recInfo.getNumFields();

            // JCSMORE - should this be done (optionally) when RtlRecord is created?
            for (unsigned fieldNum=0; fieldNum<numInputFields; fieldNum++)
            {
                if (recInfo.queryType(fieldNum)->queryChildType())
                {
                    const char *xpath = recInfo.queryXPath(fieldNum);
                    dbgassertex(xpath);
                    const char *ptr = xpath;
                    char *expandedXPath = nullptr;
                    char *expandedXPathPtr = nullptr;
                    while (true)
                    {
                        if (*ptr == xpathCompoundSeparatorChar)
                        {
                            if (!compoundXPaths)
                            {
                                compoundXPaths = new const char *[numInputFields];
                                memset(compoundXPaths, 0, sizeof(const char *)*numInputFields);
                            }

                            size_t sz = strlen(xpath)+1;
                            expandedXPath = new char[sz];
                            expandedXPathPtr = expandedXPath;
                            if (ptr == xpath) // if leading char, just skip
                                ++ptr;
                            else
                            {
                                size32_t len = ptr-xpath;
                                memcpy(expandedXPath, xpath, len);
                                expandedXPathPtr = expandedXPath + len;
                                *expandedXPathPtr++ = '/';
                                ++ptr;
                            }
                            while (*ptr)
                            {
                                if (*ptr == xpathCompoundSeparatorChar)
                                {
                                    *expandedXPathPtr++ = '/';
                                    ++ptr;
                                }
                                else
                                    *expandedXPathPtr++ = *ptr++;
                            }
                        }
                        else
                            ptr++;
                        if ('\0' == *ptr)
                        {
                            if (expandedXPath)
                            {
                                *expandedXPathPtr = '\0';
                                compoundXPaths[fieldNum] = expandedXPath;
                            }
                            break;
                        }
                    }
                }
            }
        }
        ~CFieldFetcher()
        {
            if (compoundXPaths)
            {
                for (unsigned fieldNum=0; fieldNum<numInputFields; fieldNum++)
                    delete [] compoundXPaths[fieldNum];
                delete [] compoundXPaths;
            }
        }
        void setCurrentMatch(IColumnProvider *_currentMatch)
        {
            currentMatch.set(_currentMatch);
        }
    // IDynamicFieldValueFetcher impl.
        virtual const byte *queryValue(unsigned fieldNum, size_t &sz) const override
        {
            dbgassertex(fieldNum < numInputFields);
            dbgassertex(currentMatch);

            size32_t rawSz;
            const char *ret = currentMatch->readRaw(recInfo.queryXPath(fieldNum), rawSz);
            sz = rawSz;
            return (const byte *)ret;
        }
        virtual IDynamicRowIterator *getNestedIterator(unsigned fieldNum) const override
        {
            dbgassertex(fieldNum < numInputFields);
            dbgassertex(currentMatch);

            const RtlRecord *nested = recInfo.queryNested(fieldNum);
            if (!nested)
                return nullptr;

            class CIterator : public CSimpleInterfaceOf<IDynamicRowIterator>
            {
                XmlChildIterator xmlIter;
                Linked<IDynamicFieldValueFetcher> curFieldValueFetcher;
                Linked<IColumnProvider> parentMatch;
                const RtlRecord &nestedRecInfo;
            public:
                CIterator(const RtlRecord &_nestedRecInfo, IColumnProvider *_parentMatch, const char *xpath) : parentMatch(_parentMatch), nestedRecInfo(_nestedRecInfo)
                {
                    xmlIter.initOwn(parentMatch->getChildIterator(xpath));
                }
                virtual bool first() override
                {
                    IColumnProvider *child = xmlIter.first();
                    if (!child)
                    {
                        curFieldValueFetcher.clear();
                        return false;
                    }
                    curFieldValueFetcher.setown(new CFieldFetcher(nestedRecInfo, child));

                    return true;
                }
                virtual bool next() override
                {
                    IColumnProvider *child = xmlIter.next();
                    if (!child)
                    {
                        curFieldValueFetcher.clear();
                        return false;
                    }
                    curFieldValueFetcher.setown(new CFieldFetcher(nestedRecInfo, child));
                    return true;
                }
                virtual bool isValid() override
                {
                    return nullptr != curFieldValueFetcher.get();
                }
                virtual IDynamicFieldValueFetcher &query() override
                {
                    assertex(curFieldValueFetcher);
                    return *curFieldValueFetcher;
                }
            };
            // JCSMORE - it would be good if these were cached/reused (can I assume anything using parent fetcher is single threaded?)
            return new CIterator(*nested, currentMatch, queryCompoundXPath(fieldNum));
        }
        virtual size_t getSize(unsigned fieldNum) const override { throwUnexpected(); }
        virtual size32_t getRecordSize() const override { throwUnexpected(); }
    };

public:
    IMPLEMENT_IINTERFACE_USING(ExternalFormatDiskRowReader);
    MarkupDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator, ThorActivityKind _kind);

    virtual const void *nextRow() override;
    virtual const void *prefetchRow(size32_t & resultSize) override;
    virtual const void *nextRow(MemoryBufferBuilder & builder) override;

    virtual void stop() override;
    virtual bool matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions) override;
    // IXMLSelect impl.
    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset) { lastMatch.set(&entry); }

    bool checkOpen();
    IColumnProvider *queryMatch() const { return lastMatch; }

protected:
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) override;

protected:
    StringBuffer xpath;
    StringBuffer rowTag;

    ThorActivityKind kind;
    IXmlToRowTransformer *xmlTransformer = nullptr;
    Linked<IColumnProvider> lastMatch;
    Owned<IXMLParse> xmlParser;

    bool noRoot = false;
    bool useXmlContents = false;

    const RtlRecord *record = nullptr;
    bool opened = false;
};

MarkupDiskRowReader::MarkupDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator, ThorActivityKind _kind)
: ExternalFormatDiskRowReader(_mapping, _providerOptions, _optOutputAllocator), kind(_kind)
{
    const IPropertyTree & markupOptions = *mapping->queryFormatOptions();

    markupOptions.getProp("ActivityOptions/rowTag", rowTag);
    noRoot = markupOptions.getPropBool("noRoot");

    record = &actualDiskMeta->queryRecordAccessor(true);
}

bool MarkupDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & _expectedFilter)
{
    return ExternalFormatDiskRowReader::setInputFile(inputFile, _logicalFilename, _partNumber, _baseOffset, startOffset, length, _expectedFilter);
}

//Implementation of IAllocRowStream
const void *MarkupDiskRowReader::nextRow()
{
    checkOpen();
    while (xmlParser->next())
    {
        if (lastMatch)
        {
            RtlDynamicRowBuilder builder(outputAllocator);
            ((CFieldFetcher *)fieldFetcher.get())->setCurrentMatch(lastMatch);
            size32_t sizeRead = translator->translate(builder, *this, *fieldFetcher);
            dbgassertex(sizeRead);
            lastMatch.clear();
            roxiemem::OwnedConstRoxieRow next = builder.finalizeRowClear(sizeRead);

            if (fieldFilterMatchProjected(next))
                return next.getClear();
        }
    }
    return eofRow;
}

//Implementation of IRawRowStream
const void *MarkupDiskRowReader::prefetchRow(size32_t & resultSize)
{
    tempOutputBuffer.clear();
    const void * next = nextRow(bufferBuilder);
    resultSize = tempOutputBuffer.length();
    return next;
}

const void * MarkupDiskRowReader::nextRow(MemoryBufferBuilder & builder)
{
    checkOpen();
    while (xmlParser->next())
    {
        if (lastMatch)
        {
            ((CFieldFetcher *)fieldFetcher.get())->setCurrentMatch(lastMatch);
            size32_t resultSize = translator->translate(builder, *this, *fieldFetcher);
            dbgassertex(resultSize);
            lastMatch.clear();
            const void *ret = builder.getSelf();

            if (fieldFilterMatchProjected(ret))
            {
                builder.finishRow(resultSize);
                return ret;
            }
            else
                builder.removeBytes(resultSize);
        }
    }
    return eofRow;
}

void MarkupDiskRowReader::stop()
{
    opened = false;
}

bool MarkupDiskRowReader::matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions)
{
    return ExternalFormatDiskRowReader::matches(format, streamRemote, otherMapping, otherProviderOptions);
}

bool MarkupDiskRowReader::checkOpen()
{
    class CSimpleStream : public CSimpleInterfaceOf<ISimpleReadStream>
    {
        Linked<IBufferedSerialInputStream> stream;
    public:
        CSimpleStream(IBufferedSerialInputStream *_stream) : stream(_stream)
        {
        }
    // ISimpleReadStream impl.
        virtual size32_t read(size32_t max_len, void * data) override
        {
            size32_t got;
            const void *res = stream->peek(max_len, got);
            if (got)
            {
                if (got>max_len)
                    got = max_len;
                memcpy(data, res, got);
                stream->skip(got);
            }
            return got;
        }
    };

    if (!opened)
    {
        Owned<ISimpleReadStream> simpleStream = new CSimpleStream(inputStream);
        if (kind==TAKjsonread)
            xmlParser.setown(createJSONParse(*simpleStream, xpath, *this, noRoot?ptr_noRoot:ptr_none, useXmlContents));
        else
            xmlParser.setown(createXMLParse(*simpleStream, xpath, *this, noRoot?ptr_noRoot:ptr_none, useXmlContents));

        if (!fieldFetcher)
            fieldFetcher.setown(new CFieldFetcher(*record, nullptr));

        opened = true;
        return true;
    }
    return false;
}


class XmlDiskRowReader : public MarkupDiskRowReader
{
public:
    XmlDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator)
    : MarkupDiskRowReader(_mapping, _providerOptions, _optOutputAllocator, TAKxmlread)
    {
        const IPropertyTree & xmlOptions = *mapping->queryFormatOptions();

        if (rowTag.isEmpty()) // no override
            xmlOptions.getProp("xpath", xpath);
        else
        {
            xpath.set("/Dataset/");
            xpath.append(rowTag);
        }
    }

    bool matches(const char * format, bool streamRemote, IRowReadFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions)
    {
        if (!strieq(format, "xml"))
            return false;
        return MarkupDiskRowReader::matches(format, streamRemote, otherMapping, otherProviderOptions);
    }
};

//---------------------------------------------------------------------------------------------------------------------

/*
 * This class is used to project the input rows - for the situations where the disk reader cannot perform
 * all the filtering and projection that is required.
 */
class CompoundProjectRowReader : extends CInterfaceOf<ILogicalRowStream>, implements IDiskRowReader
{
    Linked<IRowReadFormatMapping> mapping;
    Linked<IDiskRowReader> inputReader;
    UnexpectedVirtualFieldCallback unexpectedCallback;
    Owned<const IDynamicTransform> translator;
    MemoryBuffer tempOutputBuffer;
    MemoryBufferBuilder bufferBuilder;
    RtlDynamicRowBuilder allocatedBuilder;
    Linked<IEngineRowAllocator> outputAllocator;
    ILogicalRowStream * rawInputStream = nullptr;
public:
    CompoundProjectRowReader(IDiskRowReader * _input, IRowReadFormatMapping * _mapping)
    : mapping(_mapping), inputReader(_input), bufferBuilder(tempOutputBuffer, 0), allocatedBuilder(nullptr)
    {
        const RtlRecord &inRecord = mapping->queryExpectedMeta()->queryRecordAccessor(true);
        const RtlRecord &outRecord = mapping->queryProjectedMeta()->queryRecordAccessor(true);
        translator.setown(createRecordTranslator(outRecord, inRecord));
        rawInputStream = inputReader->queryAllocatedRowStream();
    }
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<ILogicalRowStream>)

    virtual ILogicalRowStream * queryAllocatedRowStream() override
    {
        return this;
    }

    virtual bool matches(const char * _format, bool _streamRemote, IRowReadFormatMapping * _mapping, const IPropertyTree * otherProviderOptions) override
    {
        return false;
    }

    virtual void clearInput() override
    {
        inputReader->clearInput();
    }

    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override
    {
        return inputReader->setInputFile(localFilename, logicalFilename, partNumber, baseOffset, expectedFilter);
    }

    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override
    {
        return inputReader->setInputFile(filename, logicalFilename, partNumber, baseOffset, expectedFilter);
    }

    virtual bool setInputFile(IFile * inputFile, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) override
    {
        return inputReader->setInputFile(inputFile, logicalFilename, partNumber, baseOffset, startOffset, length, expectedFilter);
    }

//interface IRowReader
    virtual bool getCursor(MemoryBuffer & cursor) override { return rawInputStream->getCursor(cursor); }
    virtual void setCursor(MemoryBuffer & cursor) override { rawInputStream->setCursor(cursor); }
    virtual void stop() override { rawInputStream->stop(); }

    virtual const void *prefetchRow(size32_t & resultSize) override
    {
        size32_t rawInputSize;
        const void * next = rawInputStream->prefetchRow(rawInputSize);
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
        const void * next = rawInputStream->prefetchRow(rawInputSize);
        if (isSpecialRow(next))
            return next;

        size32_t size = translator->translate(allocatedBuilder.ensureRow(), unexpectedCallback, (const byte *)next);
        return allocatedBuilder.finalizeRowClear(size);
    }

    virtual const void *nextRow(MemoryBufferBuilder & builder) override
    {
        size32_t rawInputSize;
        const void * next = rawInputStream->prefetchRow(rawInputSize);
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
    AlternativeDiskRowReader(IDiskRowReader * projectedReader, IDiskRowReader * expectedReader, IRowReadFormatMapping * mapping)
    {
        directReader.set(projectedReader);
        compoundReader.setown(new CompoundProjectRowReader(expectedReader, mapping));
    }

    virtual ILogicalRowStream * queryAllocatedRowStream() override
    {
        assertex(activeReader);
        return activeReader->queryAllocatedRowStream();
    }

    virtual bool matches(const char * _format, bool _streamRemote, IRowReadFormatMapping * _mapping, const IPropertyTree * otherProviderOptions) override
    {
        return directReader->matches(_format, _streamRemote, _mapping, otherProviderOptions);
    }

    //Specify where the raw binary input for a particular file is coming from, together with its actual format.
    //Does this make sense, or should it be passed a filename?  an actual format?
    //Needs to specify a filename rather than a IBufferedSerialInputStream so that the interface is consistent for local and remote
    virtual void clearInput() override
    {
        directReader->clearInput();
        compoundReader->clearInput();
        activeReader = nullptr;
    }

    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override
    {
        bool useProjected = canFilterDirectly(expectedFilter);
        if (useProjected)
            activeReader = directReader;
        else
            activeReader = compoundReader;
        return activeReader->setInputFile(localFilename, logicalFilename, partNumber, baseOffset, expectedFilter);
    }

    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override
    {
        bool useProjected = canFilterDirectly(expectedFilter);
        if (useProjected)
            activeReader = directReader;
        else
            activeReader = compoundReader;
        return activeReader->setInputFile(filename, logicalFilename, partNumber, baseOffset, expectedFilter);
    }

    virtual bool setInputFile(IFile * inputFile, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) override
    {
        bool useProjected = canFilterDirectly(expectedFilter);
        if (useProjected)
            activeReader = directReader;
        else
            activeReader = compoundReader;
        return activeReader->setInputFile(inputFile, logicalFilename, partNumber, baseOffset, startOffset, length, expectedFilter);
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
#ifdef _USE_PARQUET
class CParquetActivityContext : public IThorActivityContext
{
public:
    CParquetActivityContext(bool _local, unsigned _numWorkers, unsigned _curWorker)
    : workers(_local ? 1 : _numWorkers), curWorker(_local ? 0 : _curWorker), local(_local)
    {
        assertex(curWorker < workers);
    }

    virtual bool isLocal() const override { return local; };
    virtual unsigned numSlaves() const override { return workers; };
    virtual unsigned numStrands() const override { return 1; };
    virtual unsigned querySlave() const override { return curWorker; };
    virtual unsigned queryStrand() const override { return 0; }; // 0 based 0..numStrands-1
protected:
    unsigned workers;
    unsigned curWorker;
    bool local;
};

/*
 * Base class for reading a Parquet local file
 */
class ParquetDiskRowReader : public ExternalFormatDiskRowReader
{
public:
    ParquetDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator);
    ~ParquetDiskRowReader();
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<ILogicalRowStream>)

    virtual const void * nextRow() override;
    virtual const void * prefetchRow(size32_t & resultSize) override;
    virtual const void * nextRow(MemoryBufferBuilder & builder) override;
    virtual bool getCursor(MemoryBuffer & cursor) override { return parquetFileReader->getCursor(cursor); }
    virtual void setCursor(MemoryBuffer & cursor) override { parquetFileReader->setCursor(cursor); }
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * _format, bool _streamRemote, IRowReadFormatMapping * _mapping, const IPropertyTree * otherProviderOptions) override;

// IDiskRowReader
    virtual bool setInputFile(IFile * inputFile, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override;

protected:
    parquetembed::ParquetReader * parquetFileReader = nullptr;
    CParquetActivityContext * parquetActivityCtx = nullptr;
};

ParquetDiskRowReader::ParquetDiskRowReader(IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator)
 : ExternalFormatDiskRowReader(_mapping, _providerOptions, _optOutputAllocator), parquetActivityCtx(new CParquetActivityContext(true, 1, 0))
{
}

ParquetDiskRowReader::~ParquetDiskRowReader()
{
    if (parquetFileReader)
    {
        delete parquetFileReader;
    }

    if (parquetActivityCtx)
    {
        delete parquetActivityCtx;
    }
}

// Returns rows to the engine for the next stage in the processing
const void * ParquetDiskRowReader::nextRow()
{
    while (parquetFileReader->shouldRead())
    {
        parquetembed::TableColumns * table = nullptr;
        auto index = parquetFileReader->next(table);

        if (table && !table->empty())
        {
            parquetembed::ParquetRowBuilder pRowBuilder(table, index);

            RtlDynamicRowBuilder rowBuilder(outputAllocator);
            const RtlTypeInfo * typeInfo = outputAllocator->queryOutputMeta()->queryTypeInfo();
            assertex(typeInfo);
            RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
            size32_t sizeRead = typeInfo->build(rowBuilder, 0, &dummyField, pRowBuilder);
            roxiemem::OwnedConstRoxieRow next = rowBuilder.finalizeRowClear(sizeRead);
            return next.getClear();
        }
    }
    return eofRow;
}

// Returns temporary rows for filtering/counting etc.
// Row is built in temporary buffer and reused.
const void * ParquetDiskRowReader::prefetchRow(size32_t & resultSize)
{
    tempOutputBuffer.clear();
    const void * next = nextRow(bufferBuilder);
    resultSize = tempOutputBuffer.length();
    return next;
}

// Returns rows to any output buffer in dafilesrv
const void * ParquetDiskRowReader::nextRow(MemoryBufferBuilder & builder)
{
    while (parquetFileReader->shouldRead())
    {
        parquetembed::TableColumns * table = nullptr;
        auto index = parquetFileReader->next(table);

        if (table && !table->empty())
        {
            parquetembed::ParquetRowBuilder pRowBuilder(table, index);

            const RtlTypeInfo * typeInfo = outputAllocator->queryOutputMeta()->queryTypeInfo();
            assertex(typeInfo);
            RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
            size32_t resultSize = typeInfo->build(builder, 0, &dummyField, pRowBuilder);
            const void * next = builder.getSelf();
            builder.finishRow(resultSize);
            return next;
        }
    }
    return nullptr;
}

void ParquetDiskRowReader::stop()
{
}

void ParquetDiskRowReader::clearInput()
{
}

bool ParquetDiskRowReader::matches(const char * _format, bool _streamRemote, IRowReadFormatMapping * _mapping, const IPropertyTree * otherProviderOptions)
{
    if (!strieq(_format, PARQUET_FILE_TYPE_NAME))
        return false;
    return ExternalFormatDiskRowReader::matches(_format, _streamRemote, _mapping, otherProviderOptions);
}

bool ParquetDiskRowReader::setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter)
{
    DBGLOG(0, "Opening File: %s", localFilename);
    parquetFileReader = new parquetembed::ParquetReader("read", localFilename, 50000, nullptr, parquetActivityCtx, mapping->queryExpectedMeta()->queryTypeInfo());
    auto st = parquetFileReader->processReadFile();
    if (!st.ok())
        throw MakeStringException(0, "%s: %s.", st.CodeAsString().c_str(), st.message().c_str());
    return true;
}

bool ParquetDiskRowReader::setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter)
{
    throwUnexpected();
}
bool ParquetDiskRowReader::setInputFile(IFile * inputFile, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter)
{
    assertex(startOffset == 0);
    assertex(length == unknownFileSize);
    return setInputFile(inputFile->queryFilename(), logicalFilename, partNumber, baseOffset, expectedFilter);
}
#endif
//---------------------------------------------------------------------------------------------------------------------


/*
 * This class is used to read files that have been remotely filtered and projected by dafilesrv.
 */

class RemoteDiskRowReader : public DiskRowReader
{
public:
    RemoteDiskRowReader(const char * _format, IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator);

    virtual const void *nextRow() override;
    virtual const void *prefetchRow(size32_t & resultSize) override;
    virtual const void *nextRow(MemoryBufferBuilder & builder) override;
    virtual bool getCursor(MemoryBuffer & cursor) override;
    virtual void setCursor(MemoryBuffer & cursor) override;
    virtual void stop() override;

    virtual void clearInput() override;
    virtual bool matches(const char * _format, bool _streamRemote, IRowReadFormatMapping * _mapping, const IPropertyTree * otherProviderOptions) override;

// IDiskRowReader
    virtual bool setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) override;

private:
    template <class PROCESS>
    inline const void * inlineNextRow(PROCESS processor) __attribute__((always_inline));

protected:
    ISourceRowPrefetcher * projectedRowPrefetcher = nullptr;
    StringAttr format;
    RecordTranslationMode translationMode;
    bool eogPending = false;
};


RemoteDiskRowReader::RemoteDiskRowReader(const char * _format, IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator)
: DiskRowReader(_mapping, _providerOptions, _optOutputAllocator), format(_format)
{
    translationMode = mapping->queryTranslationMode();
    projectedRowPrefetcher = mapping->queryProjectedMeta()->createDiskPrefetcher();
}

void RemoteDiskRowReader::clearInput()
{
    DiskRowReader::clearInput();
    eogPending = false;
}

bool RemoteDiskRowReader::matches(const char * _format, bool _streamRemote, IRowReadFormatMapping * _mapping, const IPropertyTree * otherProviderOptions)
{
    if (!_streamRemote)
        return false;
    if (!strieq(format, _format))
        return false;
    return DiskRowReader::matches(_format, _streamRemote, _mapping, otherProviderOptions);
}

bool RemoteDiskRowReader::setInputFile(const RemoteFilename & rfilename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const FieldFilterArray & expectedFilters)
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

    //MORE: This should be refactored, so that there is an RemoteIFile that creates an IRemoteFileIO when it is opened
    //that may allow much of the filename logic to be commoned up.  Needs more thought.
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
    size32_t readBufferSize = providerOptions->getPropInt("readBufferSize", defaultReadBufferSize);
    inputStream.setown(createFileSerialStream(inputfileio, 0, (offset_t)-1, readBufferSize));

    inputBuffer.setStream(inputStream);
    eogPending = false;
    return true;
}

bool RemoteDiskRowReader::setInputFile(const char * localFilename, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, const FieldFilterArray & expectedFilter)
{
    throwUnexpected();
}

bool RemoteDiskRowReader::setInputFile(IFile * inputFile, const char * _logicalFilename, unsigned _partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter)
{
    UNIMPLEMENTED;
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
const void *RemoteDiskRowReader::prefetchRow(size32_t & resultSize)
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

// Lookup to map the names of file types/formats to their object constructors;
// map will be initialized within MODULE_INIT
static std::map<std::string, std::function<DiskRowReader*(IRowReadFormatMapping*, const IPropertyTree *, IEngineRowAllocator *)>> genericFileTypeMap;


// format is assumed to be lowercase
IDiskRowReader * doCreateLocalDiskReader(const char * format, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions, IEngineRowAllocator * optOutputAllocator)
{
    auto foundReader = genericFileTypeMap.find(format);

    if (foundReader != genericFileTypeMap.end() && foundReader->second)
        return foundReader->second(mapping, providerOptions, optOutputAllocator);

    UNIMPLEMENTED;
}


//3 possible cases
//   no filter
//   filter can be performed on the projected output
//   filter can only be performed on expected -> need to project to expected as a temporary row

IDiskRowReader * createLocalDiskReader(const char * format, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions, IEngineRowAllocator * optOutputAllocator)
{
    Owned<IDiskRowReader> directReader = doCreateLocalDiskReader(format, mapping, providerOptions, optOutputAllocator);
    if (mapping->expectedMatchesProjected() || strieq(format, "flat"))
        return directReader.getClear();

    Owned<IRowReadFormatMapping> expectedMapping = createUnprojectedMapping(mapping);
    Owned<IDiskRowReader> expectedReader = doCreateLocalDiskReader(format, expectedMapping, providerOptions, optOutputAllocator);
    return new AlternativeDiskRowReader(directReader, expectedReader, mapping);
}


IDiskRowReader * createRemoteDiskReader(const char * format, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions, IEngineRowAllocator * optOutputAllocator)
{
    return new RemoteDiskRowReader(format, mapping, providerOptions, optOutputAllocator);
}



IDiskRowReader * createDiskReader(const char * format, bool streamRemote, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions, IEngineRowAllocator * optOutputAllocator)
{
    if (streamRemote)
        return createRemoteDiskReader(format, mapping, providerOptions, optOutputAllocator);
    else
        return createLocalDiskReader(format, mapping, providerOptions, optOutputAllocator);
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    // All pluggable file types that use the generic disk reader
    // should be defined here; the key is the lowecase name of the format,
    // as will be used in ECL, and the value should be a lambda
    // that creates the appropriate disk row reader object
    genericFileTypeMap.emplace("flat", [](IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator) { return new BinaryDiskRowReader(_mapping, _providerOptions, _optOutputAllocator); });
    genericFileTypeMap.emplace("csv", [](IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator) { return new CsvDiskRowReader(_mapping, _providerOptions, _optOutputAllocator); });
    genericFileTypeMap.emplace("xml", [](IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator) { return new XmlDiskRowReader(_mapping, _providerOptions, _optOutputAllocator); });
#ifdef _USE_PARQUET
    genericFileTypeMap.emplace(PARQUET_FILE_TYPE_NAME, [](IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator) { return new ParquetDiskRowReader(_mapping, _providerOptions, _optOutputAllocator); });
#else
    genericFileTypeMap.emplace(PARQUET_FILE_TYPE_NAME, [](IRowReadFormatMapping * _mapping, const IPropertyTree * _providerOptions, IEngineRowAllocator * _optOutputAllocator) { return nullptr; });
#endif

    // Stuff the file type names that were just instantiated into a list;
    // list will be accessed by the ECL compiler to validate the names
    // at compile time
    for (auto iter = genericFileTypeMap.begin(); iter != genericFileTypeMap.end(); iter++)
        addAvailableGenericFileTypeName(iter->first.c_str());

    return true;
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
