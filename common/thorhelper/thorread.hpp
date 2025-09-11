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

#ifndef __THORREAD_HPP_
#define __THORREAD_HPP_

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
#endif

#include "jrowstream.hpp"
#include "rtlkey.hpp"
#include "rtldynfield.hpp"

#define PARQUET_FILE_TYPE_NAME "parquet"

// IRowReadFormatMapping interface represents the mapping when reading a stream from an external source.
//
//  @actualMeta - the format obtained from the meta infromation (e.g. dali)
//  @expectedMeta - the format that is specified in the ECL
//  @projectedMeta - the format of the rows to be streamed out.
//  @formatOptions - what options are applied to the format (e.g. csv separator)
//
// if expectedMeta->querySerializedMeta() != projectedMeta then the transformation will lose
// fields from the dataset as it is written.

interface IRowReadFormatMapping : public IInterface
{
public:
    // Accessor functions to provide the basic information from the disk read
    virtual const char * queryFormat() const = 0;
    virtual unsigned getActualCrc() const = 0;
    virtual unsigned getExpectedCrc() const = 0;
    virtual unsigned getProjectedCrc() const = 0;
    virtual IOutputMetaData * queryActualMeta() const = 0;
    virtual IOutputMetaData * queryExpectedMeta() const = 0;
    virtual IOutputMetaData * queryProjectedMeta() const = 0;
    virtual const IPropertyTree * queryFormatOptions() const = 0;
    virtual RecordTranslationMode queryTranslationMode() const = 0;

    virtual bool matches(const IRowReadFormatMapping * other) const = 0;
    virtual bool expectedMatchesProjected() const = 0;

    virtual const IDynamicTransform * queryTranslator() const = 0; // translates from actual to projected - null if no translation needed
    virtual const IKeyTranslator *queryKeyedTranslator() const = 0; // translates from expected to actual
};

THORHELPER_API IRowReadFormatMapping * createRowReadFormatMapping(RecordTranslationMode mode, const char * format, unsigned actualCrc, IOutputMetaData & actual, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, IOutputMetaData & projected, const IPropertyTree * formatOptions);

// Format options are mapped from the ecl to the property tree without turning them into attributes.
// So CSV(header(1))  would have getPropInt64("header") == 1
//
// Any other format properties that correspond to user options are passed similarly.
//
// Internal format options (e.g. whether the rows are grouped) are passed as attributes.
//
// User defined provider options (when implemented) will be passed as key value pairs - not attributes
//
// Internal provider options (e.g. readBufferSize) are passed as attributes, except for binary valuues (e.g. encryptionKeys)

interface IHThorGenericDiskReadBaseArg;
interface IHThorGenericDiskWriteArg;
interface IStoragePlane;
interface IDistributedFile;
class THORHELPER_API FileAccessOptions
{
public:
    FileAccessOptions();
    explicit FileAccessOptions(const FileAccessOptions & original); // clone - ready for subsequent modification

    bool isCompressed() const;
    void setCompression(bool enable, const char * method);

    void updateFromFile(IDistributedFile * file);
    void updateFromGraphNode(const IPropertyTree * node);
    void updateFromReadHelper(IHThorGenericDiskReadBaseArg & helper);
    void updateFromStoragePlane(const IStoragePlane * storagePlane, IFOmode mode);
    void updateFromStoragePlane(const char * storagePlaneName, IFOmode mode);

    void updateFromWriteHelper(IHThorGenericDiskWriteArg & helper, const char * defaultStoragePlaneName);

//MORE: These members should probably be made private, and accessor methods added for extracting values from the format/provider options.
//      or (better) the logic for setting properties for publishing in dali should become a member function.
public:
    StringAttr format;
    RecordTranslationMode recordTranslationMode = RecordTranslationMode::Unspecified;
    Owned<IPropertyTree> formatOptions;
    Owned<IPropertyTree> providerOptions;
    Owned<IOutputMetaData> actualDiskMeta;
    unsigned formatCrc = 0;
};

THORHELPER_API void updatePlaneFromHelper(StringBuffer & plane, IHThorDiskWriteArg & helper);

//--------------------------------------------------------------------------------------------------------------------

typedef IConstArrayOf<IFieldFilter> FieldFilterArray;

// A IProviderRowReader provides the interface that is used to send queries to an external provider and return
// a stream of records.  It is not yet implemented, and the interface is likely to change.
//
// Here to provide an idea of how it will be accessed in the future.
interface IProviderRowReader : extends IInterface
{
    // Create a filtered set of records - keyed joins will call this from multiple threads.
    // outputAllocator can be null if allocating nextRow() is not used.
    virtual ILogicalRowStream * createRowStream(IEngineRowAllocator * optOutputAllocator, const FieldFilterArray & expectedFilter) = 0;
};


// The IRowReader interface is used to read a stream of rows from an external source.
// It is used to process a single logical file at a time.
//
// The class is responsible for ensuring that the input file is appropriately buffered and decompressed by examining
// options in the providerOptions.
//
// The row allocator is provided to the constructor - if it is null then calls to stream->nextRow() will throw an exception
//
interface IRowReader : extends IInterface
{
public:
    // get the interface for reading streams of row.  outputAllocator can be null if allocating next is not used.
    virtual ILogicalRowStream * queryAllocatedRowStream() = 0;
};

interface ITranslator;
interface IDiskRowReader : extends IRowReader
{
public:
    virtual bool matches(const char * format, bool streamRemote, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions) = 0;

    //Specify where the raw binary input for a particular file is coming from, together with its actual format.
    //Does this make sense, or should it be passed a filename?  an actual format?
    //Needs to specify a filename rather than a IBufferedSerialInputStream so that the interface is consistent for local and remote
    virtual void clearInput() = 0;

    //MORE: It may be better to only have the first of these functions and have the other two functions as global functions that wrap this function
    virtual bool setInputFile(IFile * inputFile, const char * logicalFilename, unsigned partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const FieldFilterArray & expectedFilter) = 0;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) = 0;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const FieldFilterArray & expectedFilter) = 0;
};

//Create a row reader for a thor binary file.  The expected, projected, actual and options never change.  The file providing the data can change.
extern THORHELPER_API IDiskRowReader * createLocalDiskReader(const char * format, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions, IEngineRowAllocator * optOutputAllocator);
extern THORHELPER_API IDiskRowReader * createRemoteDiskReader(const char * format, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions, IEngineRowAllocator * optOutputAllocator);
extern THORHELPER_API IDiskRowReader * createDiskReader(const char * format, bool streamRemote, IRowReadFormatMapping * mapping, const IPropertyTree * providerOptions, IEngineRowAllocator * optOutputAllocator);

//MORE: These should probably move into jlib
extern THORHELPER_API IBufferedSerialInputStream * createBufferedInputStream(IFileIO * io, const IPropertyTree * providerOptions);
extern THORHELPER_API bool createBufferedInputStream(Shared<IBufferedSerialInputStream> & inputStream, Shared<IFileIO> & inputfileio, IFile * inputFile, const IPropertyTree * providerOptions);

#endif
