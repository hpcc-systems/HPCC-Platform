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
    Linked<IFileIO> outputfileio = io;
    unsigned delayNs = providerOptions->getPropInt("@delayNs", 0);
    if (delayNs)
        outputfileio.setown(createDelayedFileIO(outputfileio, delayNs));

    size32_t ioBufferSize = providerOptions->getPropInt("@sizeIoBuffer", oneMB);
    size32_t streamBufferSize = ioBufferSize;
    try
    {
        if (sequentialAccess)
        {
            Owned<ICompressor> compressor = getCompressor(compression ? compression : "lz4");
            offset_t offset = append ? outputfileio->size() : 0;
            Owned<ISerialOutputStream> fileStream = createSerialOutputStream(outputfileio, offset);
            Owned<IBufferedSerialOutputStream> bufferedStream = createBufferedOutputStream(fileStream, ioBufferSize);
            Owned<ISerialOutputStream> compressed = createCompressingOutputStream(bufferedStream, compressor);
            return createBufferedOutputStream(compressed, oneMB);
        }

        if (compressionMethod != COMPRESS_METHOD_NONE)
        {
            //If the input file is empty return a dummy stream, otherwise create a decompressed reader
            size32_t compBlockSize = 0; // i.e. default
            size32_t blockedIoSize = -1; // i.e. default
            Owned<ICompressedFileIO> compressedIO = createCompressedFileWriter(outputfileio, append, false, encryptor, compressionMethod, compBlockSize, blockedIoSize);

            //MORE: The compressed file reader should provide a IBufferedSerialInputStream interface - which would avoid
            //the need for extra buffering.

            //MORE: This should throw an exception if the file does not appear to be compressed
            if (!outputfileio)
                return nullptr;

            //If we are reading from a compressed file, then only buffer 1MB, not the io size (which may be 4MB)
            //In the future compressedFileReader will directly implement the buffering
            streamBufferSize = compressedIO->blockSize();
            outputfileio.setown(compressedIO.getClear());
        }
    }
    catch (IException *e)
    {
        EXCLOG(e, "createOutputStream");
        e->Release();
        return nullptr;
    }

    //Now wrap the IFileIO in a stream interface
    offset_t offset = append ? outputfileio->size() : 0;
    Owned<ISerialOutputStream> fileStream = createSerialOutputStream(outputfileio, offset);

    // Create a buffer around the file stream
    // MORE: This should support threaded reading, but that appears to not yet be implemented....
    unsigned threading = providerOptions->getPropInt("@threading", 0);
    return createBufferedOutputStream(fileStream, streamBufferSize, threading);
}


// Create an input stream and and input io for a given input file.
bool createBufferedOutputStream(Shared<IBufferedSerialOutputStream> & outputStream, Shared<IFileIO> & outputfileio, IFile * outputFile, const IPropertyTree * providerOptions)
{
    IFOmode mode = providerOptions->getPropBool("@extend", false) ? IFOreadwrite : IFOcreate;
    outputfileio.setown(outputFile->open(mode));
    if (!outputfileio)
        return false;

    outputStream.setown(createBufferedOutputStream(outputfileio, providerOptions));
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
