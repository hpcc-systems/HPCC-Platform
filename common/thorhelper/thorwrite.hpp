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

#ifndef __THORWRITE_HPP_
#define __THORWRITE_HPP_

#ifdef THORHELPER_EXPORTS
  #define THORHELPER_API DECL_EXPORT
#else
  #define THORHELPER_API DECL_IMPORT
#endif

#include "jiface.hpp"
#include "jfile.hpp"
#include "jrowstream.hpp"
#include "jstatcodes.h"
#include "rtlkey.hpp"
#include "rtldynfield.hpp"
#include "thorcommon.hpp"

class RemoteFilename;
interface IPropertyTree;
interface IOutputMetaData;

//--------------------------------------------------------------------------------------------------------------------

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
    virtual const char * queryFormat() const = 0;
    virtual unsigned getExpectedCrc() const = 0;
    virtual unsigned getProjectedCrc() const = 0;
    virtual IOutputMetaData * queryExpectedMeta() const = 0;
    virtual IOutputMetaData * queryProjectedMeta() const = 0;
    virtual const IPropertyTree * queryFormatOptions() const = 0;
    virtual RecordTranslationMode queryTranslationMode() const = 0;
    virtual bool matches(const IRowWriteFormatMapping * other) const = 0;
};
THORHELPER_API IRowWriteFormatMapping * createRowWriteFormatMapping(RecordTranslationMode mode, const char * format, IOutputMetaData & projected, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, const IPropertyTree * formatOptions);

//--------------------------------------------------------------------------------------------------------------------

// The IDiskRowWriter interface is used to write a stream of rows to an external source.
// It is used to process a single logical file at a time.
//
// The class is responsible for ensuring that the output file is appropriately buffered and compressed by examining
// options in the providerOptions.
//
// CURRENT LIMITATIONS:
// - The 'pos' parameter in setOutputFile() methods is NOT currently supported (will throw exception if non-zero)
// - Only writing from the beginning of files or in extend mode is supported
// - Arbitrary file positioning requires architectural changes to use IFileIOStream instead of ISerialOutputStream
interface IDiskRowWriter : extends IInterface
{
public:
    virtual bool matches(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions) = 0;
    
    virtual void clearOutput() = 0;
    virtual bool setOutputFile(IFile * file, offset_t pos, size32_t recordSize, bool extend) = 0;
    virtual bool setOutputFile(const char * filename, offset_t pos, size32_t recordSize, bool extend) = 0;
    virtual bool setOutputFile(const RemoteFilename & filename, offset_t pos, size32_t recordSize, bool extend) = 0;

    virtual void write(const void *row) = 0;
    virtual void writeGrouped(const void *row) = 0;
    virtual void flush() = 0;
    virtual void close() = 0;
    
    // Statistics and position tracking
    virtual offset_t getPosition() = 0;
    virtual unsigned __int64 getStatistic(StatisticKind kind) = 0;
};

//Create a row writer for a thor binary file.
extern THORHELPER_API IDiskRowWriter * createLocalDiskWriter(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions);
extern THORHELPER_API IDiskRowWriter * createRemoteDiskWriter(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions);
extern THORHELPER_API IDiskRowWriter * createDiskWriter(const char * format, bool streamRemote, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions);

//--------------------------------------------------------------------------------------------------------------------

// Adapter interface to bridge IDiskRowWriter with ILogicalRowWriter interface
// This allows generic disk writers to be used with existing hthor infrastructure
interface IDiskRowWriterAdapter : extends ILogicalRowWriter
{
public:
    virtual bool setOutputFile(IFile * file, offset_t pos, size32_t recordSize, bool extend) = 0;
    virtual bool setOutputFile(const char * filename, offset_t pos, size32_t recordSize, bool extend) = 0;
    virtual bool setOutputFile(const RemoteFilename & filename, offset_t pos, size32_t recordSize, bool extend) = 0;
};

// Create an adapter that wraps an IDiskRowWriter to make it compatible with ILogicalRowWriter
extern THORHELPER_API IDiskRowWriterAdapter * createDiskRowWriterAdapter(IDiskRowWriter * diskWriter);

//--------------------------------------------------------------------------------------------------------------------

// A IProviderRowWriter provides the interface that is used to send data to an external provider.
// It is not yet implemented, and the interface is likely to change.
//
// Here to provide an idea of how it will be accessed in the future.
interface IProviderRowWriter : extends IInterface
{
public:
    virtual void write(const void *row) = 0;
    virtual void flush() = 0;
    virtual void close() = 0;
};

//--------------------------------------------------------------------------------------------------------------------

#endif // __THORWRITE_HPP_
