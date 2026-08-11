/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
#include "rtlformat.hpp"
#include "roxiemem.hpp"

#include "rmtclient.hpp"
#include "rmtfile.hpp"

#include "thorwrite.hpp"
#include "rtlcommon.hpp"
#include "thorcommon.hpp"
#include "csvsplitter.hpp"
#include "thorxmlread.hpp"

//---------------------------------------------------------------------------------------------------------------------

IBufferedSerialOutputStream * createBufferedOutputStream(IFileIO * io, const IPropertyTree * providerOptions)
{
    assertex(providerOptions);

    bool compressed = providerOptions->getPropBool("@compressed", false);
    const char * compression = providerOptions->queryProp("@compression");
    CompressionMethod compressionMethod = translateToCompMethod(compression, compressed ? COMPRESS_METHOD_LZ4 : COMPRESS_METHOD_NONE);
    bool sequentialAccess = providerOptions->getPropBool("@sequentialAccess", false);

    MemoryBuffer encryptionKey;
    Owned<ICompressor> encryptor;
    if (providerOptions->hasProp("encryptionKey"))
    {
        providerOptions->getPropBin("encryptionKey", encryptionKey);
        encryptor.setown(createAESCompressor256((size32_t)encryptionKey.length(), encryptionKey.bufferBase()));
        compressionMethod = COMPRESS_METHOD_AES;
    }

    bool append = providerOptions->getPropBool("@extend", false);
    Linked<IFileIO> outputFileIO = io;
    unsigned delayNs = providerOptions->getPropInt("@delayNs", 0);
    if (delayNs)
        outputFileIO.setown(createDelayedFileIO(outputFileIO, delayNs));

    size32_t ioBufferSize = providerOptions->getPropInt("@sizeIoBuffer", oneMB);
    size32_t streamBufferSize = ioBufferSize;
    Owned<ISerialOutputStream> fileStream;
    try
    {
        if (sequentialAccess)
        {
            Owned<ICompressor> compressor = getCompressor(compression ? compression : "lz4");
            offset_t offset = append ? outputFileIO->size() : 0;
            Owned<ISerialOutputStream> rawFileStream = createSerialOutputStream(outputFileIO, offset);
            Owned<IBufferedSerialOutputStream> bufferedStream = createBufferedOutputStream(rawFileStream, ioBufferSize);
            fileStream.setown(createCompressingOutputStream(bufferedStream, compressor));
            streamBufferSize = oneMB;
        }
        else if (compressionMethod != COMPRESS_METHOD_NONE)
        {
            size32_t compBlockSize = providerOptions->getPropInt("@sizeCompressBlock", 0);      // 0 means use the default

            //This will throw an exception if appending and the file does not appear to be compressed
            Owned<ICompressedFileIO> compressedIO = createCompressedFileWriter(outputFileIO, append, false, encryptor, compressionMethod, compBlockSize, ioBufferSize);
            assertex(compressedIO);

            //If we are reading from a compressed file, then only buffer the block size, not the io size (which may be 4MB)
            streamBufferSize = compressedIO->blockSize();
            fileStream.set(compressedIO->queryOutputStream());
        }
        else
        {
            //Now wrap the IFileIO in a stream interface
            offset_t offset = append ? outputFileIO->size() : 0;
            fileStream.setown(createSerialOutputStream(outputFileIO, offset));
        }
    }
    catch (IException *e)
    {
        EXCLOG(e, "createOutputStream");
        e->Release();
        return nullptr;
    }

    // Create a buffer around the file stream.  Non zero means use a separate thread for writing.
    // In the future the count may be the number of extra buffers to use.
    unsigned threading = providerOptions->getPropInt("@threading", 0);
    return createBufferedOutputStream(fileStream, streamBufferSize, threading);
}


// Create an output stream and and output io for a given output file.
bool createBufferedOutputStream(Shared<IBufferedSerialOutputStream> & outputStream, Shared<IFileIO> & outputFileIO, IFile * outputFile, const IPropertyTree * providerOptions)
{
    IFOmode mode = providerOptions->getPropBool("@extend", false) ? IFOreadwrite : IFOcreate;
    outputFileIO.setown(outputFile->open(mode));
    if (!outputFileIO)
        return false;

    outputStream.setown(createBufferedOutputStream(outputFileIO, providerOptions));
    return outputStream != nullptr;
}


//---------------------------------------------------------------------------------------------------------------------

/*
 * A class that implements IRowWriteFormatMapping - which provides all the information representing a translation 
 * from projected->expected->actual.
 */
class DiskWriteMapping : public CInterfaceOf<IRowWriteFormatMapping>
{
public:
    DiskWriteMapping(RecordTranslationMode _mode, const char * _format, IOutputMetaData & _projected, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, const IPropertyTree * _formatOptions)
    : mode(_mode), format(_format), expectedCrc(_expectedCrc), projectedCrc(_projectedCrc), projectedMeta(&_projected), expectedMeta(&_expected), formatOptions(_formatOptions)
    {}

    virtual const char * queryFormat() const override { return format; }
    virtual unsigned getExpectedCrc() const override { return expectedCrc; }
    virtual unsigned getProjectedCrc() const override { return projectedCrc; }
    virtual IOutputMetaData * queryProjectedMeta() const override { return projectedMeta; }
    virtual IOutputMetaData * queryExpectedMeta() const override { return expectedMeta; }
    virtual const IPropertyTree * queryFormatOptions() const override { return formatOptions; }
    virtual RecordTranslationMode queryTranslationMode() const override { return mode; }

    virtual bool matches(const IRowWriteFormatMapping * other) const
    {
        if ((mode != other->queryTranslationMode()) || !streq(format, other->queryFormat()))
            return false;
        bool expectedMatches = (expectedCrc && expectedCrc == other->getExpectedCrc()) || (expectedMeta == other->queryExpectedMeta());
        bool projectedMatches = (projectedCrc && projectedCrc == other->getProjectedCrc()) || (projectedMeta == other->queryProjectedMeta());
        return expectedMatches && projectedMatches && areMatchingPTrees(formatOptions, other->queryFormatOptions());
    }

protected:
    RecordTranslationMode mode;
    StringAttr format;
    unsigned expectedCrc;
    unsigned projectedCrc;
    Linked<IOutputMetaData> projectedMeta;
    Linked<IOutputMetaData> expectedMeta;
    Linked<const IPropertyTree> formatOptions;
};

IRowWriteFormatMapping * createRowWriteFormatMapping(RecordTranslationMode mode, const char * format, IOutputMetaData & projected, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, const IPropertyTree * formatOptions)
{
    assertex(formatOptions);
    return new DiskWriteMapping(mode, format, projected, expectedCrc, expected, projectedCrc, formatOptions);
}

void getDefaultWritePlane(StringBuffer & plane, unsigned helperFlags)
{
    //NB: This can only access TDX flags because it is called from readers and writers
    if (helperFlags & TDXjobtemp)
        getDefaultJobTempPlane(plane);
    else if (helperFlags & TDXtemporary)
        getDefaultSpillPlane(plane);
    else
        getDefaultStoragePlane(plane);
}

//--------------------------------------------------------------------------------------------------------------------
// IDiskRowWriter implementations
//--------------------------------------------------------------------------------------------------------------------

// Base class holding the mapping and providerOptions shared by all concrete writers.
class DiskRowWriter : public CInterfaceOf<IDiskRowWriter>
{
public:
    DiskRowWriter(IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions, IRowInterfaces * _rowIf)
    : mapping(_mapping), providerOptions(_providerOptions), rowIf(_rowIf)
    {
    }

    virtual bool matches(const IRowWriteFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions) const override
    {
        return mapping->matches(otherMapping) && areMatchingPTrees(providerOptions, otherProviderOptions);
    }

protected:
    Linked<IRowWriteFormatMapping> mapping;
    Linked<const IPropertyTree> providerOptions;
    Linked<IRowInterfaces> rowIf;
};

//--------------------------------------------------------------------------------------------------------------------
// Format registry and factory
//--------------------------------------------------------------------------------------------------------------------

// map from lowercase format name -> factory lambda; populated in MODULE_INIT.
static std::map<std::string, std::function<IDiskRowWriter*(IRowWriteFormatMapping*, const IPropertyTree*, IRowInterfaces*)>> genericWriterTypeMap;

static void ensureCompatibleWriteLayout(const IRowWriteFormatMapping & mapping)
{
    if (mapping.queryExpectedMeta() == mapping.queryProjectedMeta())
        return;

    unsigned expectedCrc = mapping.getExpectedCrc();
    unsigned projectedCrc = mapping.getProjectedCrc();
    if (expectedCrc && projectedCrc && expectedCrc == projectedCrc)
        return;

    throwStringExceptionV(0, "createDiskWriter: expected and projected layouts differ; generic disk write layout translation is not supported");
}

IDiskRowWriter * createDiskWriter(const char * format, IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions, IRowInterfaces * rowIf)
{
    assertex(format && mapping && providerOptions && rowIf);
    ensureCompatibleWriteLayout(*mapping);

    // Format names are registered in lowercase; normalise so callers can pass any case.
    StringBuffer lcFormat(format);
    lcFormat.toLowerCase();
    auto it = genericWriterTypeMap.find(lcFormat.str());
    if (it == genericWriterTypeMap.end() || !it->second)
        throwStringExceptionV(0, "createDiskWriter: unknown or disabled format '%s'", format);
    return it->second(mapping, providerOptions, rowIf);
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

//--------------------------------------------------------------------------------------------------------------------
// Unit tests
//--------------------------------------------------------------------------------------------------------------------

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

struct TestRecordMetadata
{
    TestRecordMetadata()
        : intType(type_int, sizeof(unsigned __int64)), idField("id", nullptr, &intType), fields{&idField, nullptr}, recordType(type_record, sizeof(unsigned __int64), fields), meta(recordType)
    {
    }

    const RtlIntTypeInfo intType;
    const RtlFieldInfo idField;
    const RtlFieldInfo * const fields[2];
    const RtlRecordTypeInfo recordType;
    CDynamicOutputMetaData meta;
};

class DiskRowWriterTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DiskRowWriterTests);
        CPPUNIT_TEST(testUnknownFormatThrows);
        CPPUNIT_TEST(testLayoutTranslationThrows);
        CPPUNIT_TEST(testEquivalentLayoutCrcAccepted);
        CPPUNIT_TEST(testProjectedLayoutMismatchDoesNotMatch);
    CPPUNIT_TEST_SUITE_END();

    // Build a mapping for the given format.  A zero CRC forces matches() to compare on the meta
    // pointer, and the (non-null) formatOptions satisfies the createRowWriteFormatMapping assert.
    IRowWriteFormatMapping * createMapping(const char * format, IOutputMetaData & meta, const IPropertyTree * formatOptions)
    {
        return createRowWriteFormatMapping(RecordTranslationMode::None, format, meta, 0, meta, 0, formatOptions);
    }

public:
    // An unknown / unregistered format name must be rejected by the factory.
    void testUnknownFormatThrows()
    {
        TestRecordMetadata record;
        Owned<IPropertyTree> formatOptions = createPTree("formatOptions");
        Owned<IPropertyTree> providerOptions = createPTree("providerOptions");
        Owned<IRowWriteFormatMapping> mapping = createMapping("flat", record.meta, formatOptions);
        Owned<IRowInterfaces> rowIf = createRowInterfaces(&record.meta, 0, roxiemem::RHFnone, nullptr);

        bool threw = false;
        try
        {
            Owned<IDiskRowWriter> writer = createDiskWriter("no_such_format", mapping, providerOptions, rowIf);
        }
        catch (IException * e)
        {
            e->Release();
            threw = true;
        }
        CPPUNIT_ASSERT(threw);
    }

    void testLayoutTranslationThrows()
    {
        TestRecordMetadata expectedRecord;
        TestRecordMetadata projectedRecord;
        Owned<IPropertyTree> formatOptions = createPTree("formatOptions");
        Owned<IPropertyTree> providerOptions = createPTree("providerOptions");
        Owned<IRowWriteFormatMapping> mapping = createRowWriteFormatMapping(RecordTranslationMode::None, "flat", projectedRecord.meta, 0x22222222, expectedRecord.meta, 0x11111111, formatOptions);
        Owned<IRowInterfaces> rowIf = createRowInterfaces(&expectedRecord.meta, 0, roxiemem::RHFnone, nullptr);

        bool threw = false;
        StringBuffer msg;
        try
        {
            Owned<IDiskRowWriter> writer = createDiskWriter("flat", mapping, providerOptions, rowIf);
        }
        catch (IException * e)
        {
            e->errorMessage(msg);
            e->Release();
            threw = true;
        }
        CPPUNIT_ASSERT(threw);
        CPPUNIT_ASSERT(strstr(msg.str(), "layout translation is not supported") != nullptr);
    }

    void testEquivalentLayoutCrcAccepted()
    {
        TestRecordMetadata expectedRecord;
        TestRecordMetadata projectedRecord;
        Owned<IPropertyTree> formatOptions = createPTree("formatOptions");
        Owned<IRowWriteFormatMapping> mapping = createRowWriteFormatMapping(RecordTranslationMode::None, "flat", projectedRecord.meta, 0x11111111, expectedRecord.meta, 0x11111111, formatOptions);

        ensureCompatibleWriteLayout(*mapping);
    }

    void testProjectedLayoutMismatchDoesNotMatch()
    {
        TestRecordMetadata expectedRecord;
        TestRecordMetadata firstProjectedRecord;
        TestRecordMetadata secondProjectedRecord;
        Owned<IPropertyTree> formatOptions = createPTree("formatOptions");
        Owned<IRowWriteFormatMapping> firstMapping = createRowWriteFormatMapping(RecordTranslationMode::None, "flat", firstProjectedRecord.meta, 0, expectedRecord.meta, 0, formatOptions);
        Owned<IRowWriteFormatMapping> secondMapping = createRowWriteFormatMapping(RecordTranslationMode::None, "flat", secondProjectedRecord.meta, 0, expectedRecord.meta, 0, formatOptions);

        CPPUNIT_ASSERT(!firstMapping->matches(secondMapping));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(DiskRowWriterTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(DiskRowWriterTests, "DiskRowWriterTests");

#endif // _USE_CPPUNIT
