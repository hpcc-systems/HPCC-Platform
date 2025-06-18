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
#include "jiface.hpp"

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

#include "thorwrite.hpp"
#include "rtlcommon.hpp"
#include "csvsplitter.hpp"
#include "thorxmlread.hpp"

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
        if ((expectedCrc && expectedCrc == other->getExpectedCrc()) || (expectedMeta == other->queryExpectedMeta()))
        {
            if (!areMatchingPTrees(formatOptions, other->queryFormatOptions()))
                return false;
            return true;
        }
        return false;
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

THORHELPER_API IRowWriteFormatMapping * createRowWriteFormatMapping(RecordTranslationMode mode, const char * format, IOutputMetaData & projected, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, const IPropertyTree * formatOptions)
{
    assertex(formatOptions);
    return new DiskWriteMapping(mode, format, projected, expectedCrc, expected, projectedCrc, formatOptions);
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * A helper function to create an output stream for writing data to a file.
 */
static IBufferedSerialOutputStream * createOutputStream(IFileIO * outputfileio, const IPropertyTree * providerOptions, offset_t startOffset = 0)
{
    ISerialOutputStream * serialOutput = createSerialOutputStream(outputfileio);
    
    // Note: The startOffset parameter is provided for future extend mode support.
    // Currently, CFileSerialOutputStream (created by createSerialOutputStream) always 
    // starts with nextOffset = 0. To properly support extend mode, we would need:
    // 1. A modified createSerialOutputStream() that accepts a starting offset, OR
    // 2. A custom CFileSerialOutputStream implementation that initializes nextOffset to startOffset, OR  
    // 3. Use IFileIOStream with seek() capabilities instead of ISerialOutputStream
    //
    // For now, extend mode positioning relies on the caller handling file positioning
    // at a higher level, or ensuring the IFileIO is opened with the correct mode (IFOwrite)
    // that naturally appends to existing files on most file systems.
    if (startOffset > 0)
    {
        // TODO: Implement proper positioning for extend mode
        // This would require modifying the HPCC stream architecture or 
        // creating a custom positioned output stream
    }
    
    size32_t blockWriteSize = providerOptions->getPropInt("writeBufferSize", 0x100000);
    bool threaded = providerOptions->getPropBool("@threaded", false);
    
    if (threaded)
        return createThreadedBufferedOutputStream(serialOutput, blockWriteSize);
    else
        return createBufferedOutputStream(serialOutput, blockWriteSize);
}

static bool createOutputStream(Shared<IBufferedSerialOutputStream> & outputStream, Shared<IFileIO> & outputfileio, IFile * outputFile, const IPropertyTree * providerOptions)
{
    offset_t startOffset = 0;
    
    try
    {
        bool overwrite = providerOptions->getPropBool("@overwrite", false);
        bool extend = providerOptions->getPropBool("@extend", false);
        
        IFOmode mode = overwrite ? IFOcreaterw : (extend ? IFOwrite : IFOcreate);
        outputfileio.setown(outputFile->open(mode));
        if (!outputfileio)
            return false;
            
        if (extend)
        {
            offset_t fileSize = outputFile->size();
            if (fileSize != unknownFileSize)
            {
                // EXTEND MODE POSITIONING LIMITATION:
                // For extend mode, we should position the output stream at the end of the file.
                // However, the current HPCC stream architecture has limitations:
                //
                // 1. IFileIO uses position-based write(offset, len, data) but CFileSerialOutputStream
                //    always starts with nextOffset = 0 
                // 2. createSerialOutputStream() doesn't accept a starting offset parameter
                // 3. ISerialOutputStream doesn't have a seek() method
                //
                // CURRENT BEHAVIOR: 
                // - File is opened with IFOwrite mode which should append on most file systems
                // - However, the SerialOutputStream may overwrite from position 0
                //
                // PROPER SOLUTIONS (for future implementation):
                // 1. Modify createSerialOutputStream() to accept startOffset parameter
                // 2. Create CPositionedFileSerialOutputStream that initializes nextOffset = fileSize  
                // 3. Use IFileIOStream with seek(fileSize, IFSbegin) instead of ISerialOutputStream
                // 4. Create a wrapper stream that adjusts all write offsets by +fileSize
                //
                // For now, extend mode may not work correctly - this is a known limitation.
                startOffset = fileSize;  // Pass to createOutputStream for documentation
            }
        }
    }
    catch (IException *e)
    {
        EXCLOG(e, "createOutputStream");
        e->Release();
        return false;
    }

    outputStream.setown(createOutputStream(outputfileio, providerOptions, startOffset));
    return true;
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * The base class for writing rows to an external file. Each activity will have an instance of a disk writer for
 * each output file format.
 */
class DiskRowWriter : public CInterfaceOf<IDiskRowWriter>
{
public:
    DiskRowWriter(const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions);
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IDiskRowWriter>)

    virtual bool matches(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions) override;
    virtual void clearOutput() override;
    virtual offset_t getPosition() override;
    virtual unsigned __int64 getStatistic(StatisticKind kind) override;

protected:
    virtual void ensureOutputReady();

protected:
    Owned<IBufferedSerialOutputStream> outputStream;
    Owned<IFileIO> outputfileio;
    Linked<const IRowWriteFormatMapping> mapping;
    Linked<const IPropertyTree> providerOptions;
    IOutputMetaData * expectedMeta = nullptr;
    IOutputMetaData * projectedMeta = nullptr;
    bool grouped = false;
    bool compressed = false;
    StringAttr logicalFilename;
    offset_t currentPosition = 0;
    unsigned __int64 numRecords = 0;
};

DiskRowWriter::DiskRowWriter(const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions)
: mapping(_mapping), providerOptions(_providerOptions)
{
    expectedMeta = _mapping->queryExpectedMeta();
    projectedMeta = _mapping->queryProjectedMeta();

    const IPropertyTree * formatOptions = mapping->queryFormatOptions();
    grouped = formatOptions->getPropBool("@grouped", false);

    assertex(providerOptions);
    compressed = providerOptions->getPropBool("@compressed", false);
}

bool DiskRowWriter::matches(const char * format, const IRowWriteFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions)
{
    if (!mapping->matches(otherMapping))
        return false;

    if (!areMatchingPTrees(providerOptions, otherProviderOptions))
        return false;

    return true;
}

void DiskRowWriter::clearOutput()
{
    outputStream.clear();
    outputfileio.clear();
}

void DiskRowWriter::ensureOutputReady()
{
    if (!outputStream)
        throw MakeStringException(99, "Output file not set for disk writer");
}

offset_t DiskRowWriter::getPosition()
{
    return currentPosition;
}

unsigned __int64 DiskRowWriter::getStatistic(StatisticKind kind)
{
    switch (kind)
    {
        case StNumRowsProcessed:
            return numRecords;
        default:
            return 0;
    }
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * base class for writing to a local file (or a remote file via the block based IFile interface)
 */
class LocalDiskRowWriter : public DiskRowWriter
{
public:
    LocalDiskRowWriter(const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions);

    virtual bool matches(const char * format, const IRowWriteFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions) override;
    virtual bool setOutputFile(IFile * file, offset_t pos, size32_t recordSize, bool extend) override;
    virtual bool setOutputFile(const char * filename, offset_t pos, size32_t recordSize, bool extend) override;
    virtual bool setOutputFile(const RemoteFilename & filename, offset_t pos, size32_t recordSize, bool extend) override;
    virtual void flush() override;
    virtual void close() override;

protected:
    bool outputFileSet = false;
};

LocalDiskRowWriter::LocalDiskRowWriter(const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions)
: DiskRowWriter(_mapping, _providerOptions)
{
}

bool LocalDiskRowWriter::matches(const char * format, const IRowWriteFormatMapping * otherMapping, const IPropertyTree * otherProviderOptions)
{
    return DiskRowWriter::matches(format, otherMapping, otherProviderOptions);
}

bool LocalDiskRowWriter::setOutputFile(IFile * file, offset_t pos, size32_t recordSize, bool extend)
{
    if (!createOutputStream(outputStream, outputfileio, file, providerOptions))
        return false;
        
    if (pos != 0)
    {
        // POSITIONING SUPPORT:
        // The 'pos' parameter specifies where to start writing in the file.
        // To implement this properly, we need to position the output stream.
        //
        // Current approach uses ISerialOutputStream which doesn't support seeking.
        // Better approach: Use IFileIOStream which has seek() capability.
        //
        // Implementation options:
        // 1. Replace ISerialOutputStream with IFileIOStream (requires architectural changes)
        // 2. Create a wrapper that handles positioning
        // 3. Use IFileIO's position-based write() calls directly
        //
        // For now, we document the limitation and throw an error for unsupported positioning
        
        throw makeStringExceptionV(-1, "LocalDiskRowWriter::setOutputFile: Non-zero starting position (%lld) not currently supported.", pos);
    }
        
    outputFileSet = true;
    return true;
}

bool LocalDiskRowWriter::setOutputFile(const char * filename, offset_t pos, size32_t recordSize, bool extend)
{
    Owned<IFile> outputFile = createIFile(filename);
    return setOutputFile(outputFile, pos, recordSize, extend);
}

bool LocalDiskRowWriter::setOutputFile(const RemoteFilename & filename, offset_t pos, size32_t recordSize, bool extend)
{
    Owned<IFile> outputFile = createIFile(filename);
    return setOutputFile(outputFile, pos, recordSize, extend);
}

void LocalDiskRowWriter::flush()
{
    if (outputStream)
        outputStream->flush();
}

void LocalDiskRowWriter::close()
{
    if (outputStream)
    {
        outputStream->flush();
        outputStream.clear();
    }
    if (outputfileio)
        outputfileio.clear();
    outputFileSet = false;
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * base class for writing binary files
 */
class BinaryDiskRowWriter : public LocalDiskRowWriter
{
public:
    BinaryDiskRowWriter(const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions);

    virtual bool matches(const char * format, const IRowWriteFormatMapping * otherMapping, const IPropertyTree * providerOptions) override;
    virtual void write(const void *row) override;
    virtual void writeGrouped(const void *row) override;
    virtual offset_t getPosition() override;
    virtual unsigned __int64 getStatistic(StatisticKind kind) override;

protected:
    void writeRow(const void *row, bool isGrouped);

protected:
    Owned<IOutputRowSerializer> serializer;
    MemoryBuffer outputBuffer;
};

BinaryDiskRowWriter::BinaryDiskRowWriter(const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions)
: LocalDiskRowWriter(_mapping, _providerOptions)
{
    // Create the serializer for binary output
    serializer.setown(expectedMeta->createDiskSerializer(nullptr, 0));
}

bool BinaryDiskRowWriter::matches(const char * format, const IRowWriteFormatMapping * otherMapping, const IPropertyTree * providerOptions)
{
    if (!streq(format, "flat"))
        return false;
    return LocalDiskRowWriter::matches(format, otherMapping, providerOptions);
}

void BinaryDiskRowWriter::write(const void *row)
{
    writeRow(row, false);
}

void BinaryDiskRowWriter::writeGrouped(const void *row)
{
    writeRow(row, true);
}

void BinaryDiskRowWriter::writeRow(const void *row, bool isGrouped)
{
    ensureOutputReady();
    
    outputBuffer.clear();
    CMemoryRowSerializer target(outputBuffer);
    serializer->serialize(target, (const byte *)row);
    
    size32_t dataSize = outputBuffer.length();
    if (isGrouped && grouped)
    {
        // Add group marker if needed
        byte groupMarker = 0xFF;
        outputStream->put(sizeof(groupMarker), &groupMarker);
        currentPosition += sizeof(groupMarker);
    }
    
    outputStream->put(dataSize, outputBuffer.toByteArray());
    currentPosition += dataSize;
    numRecords++;
}

offset_t BinaryDiskRowWriter::getPosition()
{
    return currentPosition;
}

unsigned __int64 BinaryDiskRowWriter::getStatistic(StatisticKind kind)
{
    switch (kind)
    {
        case StNumRowsProcessed:
            return numRecords;
        default:
            return 0;
    }
}

//---------------------------------------------------------------------------------------------------------------------

// Global map of file format names to writer factory functions
static std::map<std::string, std::function<IDiskRowWriter*(const IRowWriteFormatMapping*, const IPropertyTree *)>> genericFileTypeWriterMap;

// format is assumed to be lowercase
IDiskRowWriter * doCreateLocalDiskWriter(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions)
{
    auto foundWriter = genericFileTypeWriterMap.find(format);

    if (foundWriter != genericFileTypeWriterMap.end() && foundWriter->second)
        return foundWriter->second(mapping, providerOptions);

    UNIMPLEMENTED;
}

IDiskRowWriter * createLocalDiskWriter(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions)
{
    return doCreateLocalDiskWriter(format, mapping, providerOptions);
}

IDiskRowWriter * createRemoteDiskWriter(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions)
{
    // For now, remote writing uses the same implementation as local
    // In the future, this could be enhanced for true remote streaming
    return createLocalDiskWriter(format, mapping, providerOptions);
}

IDiskRowWriter * createDiskWriter(const char * format, bool streamRemote, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions)
{
    if (streamRemote)
        return createRemoteDiskWriter(format, mapping, providerOptions);
    else
        return createLocalDiskWriter(format, mapping, providerOptions);
}

// Module initialization to register available file format writers
MODULE_INIT(INIT_PRIORITY_STANDARD + 1)  // Initialize after readers
{
    // All pluggable file types that use the generic disk writer
    // should be defined here; the key is the lowercase name of the format,
    // as will be used in ECL, and the value should be a function
    // that creates the appropriate disk row writer object
    genericFileTypeWriterMap.emplace("flat", [](const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions) { return new BinaryDiskRowWriter(_mapping, _providerOptions); });
    
    // genericFileTypeWriterMap.emplace("csv", [](const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions) { return new CsvDiskRowWriter(_mapping, _providerOptions); });
    // genericFileTypeWriterMap.emplace("xml", [](const IRowWriteFormatMapping * _mapping, const IPropertyTree * _providerOptions) { return new XmlDiskRowWriter(_mapping, _providerOptions); });

    // Stuff the file type names that were just instantiated into a list;
    // list will be accessed by the ECL compiler to validate the names
    // at compile time
    for (auto iter = genericFileTypeWriterMap.begin(); iter != genericFileTypeWriterMap.end(); iter++)
        addAvailableGenericFileTypeName(iter->first.c_str());

    return true;
}
