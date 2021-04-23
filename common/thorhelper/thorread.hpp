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

//--- Classes and interfaces for reading instances of files
//The following is constant for the life of a disk read activity
interface IDiskReadOutputMapping : public IInterface
{
public:
    virtual unsigned getExpectedCrc() const = 0;
    virtual unsigned getProjectedCrc() const = 0;
    virtual IOutputMetaData * queryExpectedMeta() const = 0;
    virtual IOutputMetaData * queryProjectedMeta() const = 0;
    virtual RecordTranslationMode queryTranslationMode() const = 0;
    virtual bool matches(const IDiskReadOutputMapping * other) const = 0;
};
THORHELPER_API IDiskReadOutputMapping * createDiskReadOutputMapping(RecordTranslationMode mode, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, IOutputMetaData & projected);

interface IDiskReadMapping : public IInterface
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
    virtual const IPropertyTree * queryFileOptions() const = 0;
    virtual RecordTranslationMode queryTranslationMode() const = 0;

    virtual bool matches(const IDiskReadMapping * other) const = 0;
    virtual bool expectedMatchesProjected() const = 0;

    virtual const IDynamicTransform * queryTranslator() const = 0; // translates from actual to projected - null if no translation needed
    virtual const IKeyTranslator *queryKeyedTranslator() const = 0; // translates from expected to actual
};

THORHELPER_API IDiskReadMapping * createDiskReadMapping(RecordTranslationMode mode, const char * format, unsigned actualCrc, IOutputMetaData & actual, unsigned expectedCrc, IOutputMetaData & expected, unsigned projectedCrc, IOutputMetaData & projected, const IPropertyTree * fileOptions);


typedef IConstArrayOf<IFieldFilter> FieldFilterArray;
interface IRowReader : extends IInterface
{
public:
    // get the interface for reading streams of row.  outputAllocator can be null if allocating next is not used.
    virtual IDiskRowStream * queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator) = 0;
};

interface ITranslator;
class CLogicalFileSlice;
interface IDiskRowReader : extends IRowReader
{
public:
    virtual bool matches(const char * format, bool streamRemote, IDiskReadMapping * mapping) = 0;

    //Specify where the raw binary input for a particular file is coming from, together with its actual format.
    //Does this make sense, or should it be passed a filename?  an actual format?
    //Needs to specify a filename rather than a ISerialStream so that the interface is consistent for local and remote
    virtual void clearInput() = 0;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * inputOptions, const FieldFilterArray & expectedFilter) = 0;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * inputOptions, const FieldFilterArray & expectedFilter) = 0;
    virtual bool setInputFile(const CLogicalFileSlice & slice, const FieldFilterArray & expectedFilter, unsigned copy) = 0;
};

//Create a row reader for a thor binary file.  The expected, projected, actual and options never change.  The file providing the data can change.
extern THORHELPER_API IDiskRowReader * createLocalDiskReader(const char * format, IDiskReadMapping * mapping);
extern THORHELPER_API IDiskRowReader * createRemoteDiskReader(const char * format, IDiskReadMapping * mapping);
extern THORHELPER_API IDiskRowReader * createDiskReader(const char * format, bool streamRemote, IDiskReadMapping * mapping);

#endif
