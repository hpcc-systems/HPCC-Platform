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

#define PARQUET_FILE_TYPE_NAME "parquet"

// IReadFormatMapping interface represents the mapping when reading a stream from an external source.
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

// IWriteFormatMapping interface represents the mapping when outputting a stream to a destination.
//
//  @expectedMeta - the format that rows have in memory (rename?)
//  @projectedMeta - the format that should be written to disk.
//  @formatOptions - which options are applied to the format
//
// if expectedMeta->querySerializedMeta() != projectedMeta then the transformation will lose
// fields from the dataset as it is written.  Reordering may be supported later, but fields
// will never be added.
interface IRowWriteFormatMapping : public IInterface
{
public:
    virtual unsigned getExpectedCrc() const = 0;
    virtual unsigned getProjectedCrc() const = 0;
    virtual IOutputMetaData * queryExpectedMeta() const = 0;
    virtual IOutputMetaData * queryProjectedMeta() const = 0;
    virtual RecordTranslationMode queryTranslationMode() const = 0;
    virtual bool matches(const IRowWriteFormatMapping * other) const = 0;
};
THORHELPER_API IRowWriteFormatMapping * createDiskReadOutputMapping(RecordTranslationMode mode, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, IOutputMetaData & projected);


//--------------------------------------------------------------------------------------------------------------------

typedef IConstArrayOf<IFieldFilter> FieldFilterArray;
interface IRowReader : extends IInterface
{
public:
    // get the interface for reading streams of row.  outputAllocator can be null if allocating next is not used.
    virtual IDiskRowStream * queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator) = 0;
};

interface ITranslator;
interface IDiskRowReader : extends IRowReader
{
public:
    virtual bool matches(const char * format, bool streamRemote, IRowReadFormatMapping * mapping) = 0;

    //Specify where the raw binary input for a particular file is coming from, together with its actual format.
    //Does this make sense, or should it be passed a filename?  an actual format?
    //Needs to specify a filename rather than a IBufferedSerialInputStream so that the interface is consistent for local and remote
    virtual void clearInput() = 0;

    //MORE: It may be better to only have the first of these functions and have the other two functions as global functions that wrap this function
    virtual bool setInputFile(IFile * inputFile, const char * logicalFilename, unsigned partNumber, offset_t _baseOffset, offset_t startOffset, offset_t length, const IPropertyTree * providerOptions, const FieldFilterArray & expectedFilter) = 0;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * providerOptions, const FieldFilterArray & expectedFilter) = 0;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * providerOptions, const FieldFilterArray & expectedFilter) = 0;
};

//Create a row reader for a thor binary file.  The expected, projected, actual and options never change.  The file providing the data can change.
extern THORHELPER_API IDiskRowReader * createLocalDiskReader(const char * format, IRowReadFormatMapping * mapping);
extern THORHELPER_API IDiskRowReader * createRemoteDiskReader(const char * format, IRowReadFormatMapping * mapping);
extern THORHELPER_API IDiskRowReader * createDiskReader(const char * format, bool streamRemote, IRowReadFormatMapping * mapping);

#endif
