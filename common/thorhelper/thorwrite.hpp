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
#include "rtlkey.hpp"
#include "rtldynfield.hpp"

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
};

//Create a row writer for a thor binary file.
extern THORHELPER_API IDiskRowWriter * createLocalDiskWriter(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions);
extern THORHELPER_API IDiskRowWriter * createRemoteDiskWriter(const char * format, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions);
extern THORHELPER_API IDiskRowWriter * createDiskWriter(const char * format, bool streamRemote, const IRowWriteFormatMapping * mapping, const IPropertyTree * providerOptions);

#endif // __THORWRITE_HPP_
