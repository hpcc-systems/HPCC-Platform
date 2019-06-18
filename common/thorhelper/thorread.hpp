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

typedef IConstArrayOf<IFieldFilter> FieldFilterArray;
interface IRowReader : extends IInterface
{
public:
    virtual IRawRowStream * queryRawRowStream() = 0;
    virtual IAllocRowStream * queryAllocatedRowStream(IEngineRowAllocator * _outputAllocator) = 0;
};

interface ITranslator;
interface IDiskRowReader : extends IRowReader
{
public:
    virtual bool matches(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options) = 0;

    //Specify where the raw binary input for a particular file is coming from, together with its actual format.
    //Does this make sense, or should it be passed a filename?  an actual format?
    //Needs to specify a filename rather than a ISerialStream so that the interface is consistent for local and remote
    virtual void clearInput() = 0;
    virtual bool setInputFile(const char * localFilename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) = 0;
    virtual bool setInputFile(const RemoteFilename & filename, const char * logicalFilename, unsigned partNumber, offset_t baseOffset, const IPropertyTree * meta, const FieldFilterArray & expectedFilter) = 0;
};

//MORE: These functions have too many parameters - should probably move them into something like the following?:
class TranslateOptions
{
    IOutputMetaData * expected;
    IOutputMetaData * projected;
    IOutputMetaData * actual;
    unsigned expectedCrc;
    unsigned projectedCrc;
    unsigned actualCrc;
};

//Create a row reader for a thor binary file.  The expected, projected, actual and options never change.  The file providing the data can change.
extern THORHELPER_API IDiskRowReader * createLocalDiskReader(const char * format, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options);
extern THORHELPER_API IDiskRowReader * createRemoteDiskReader(const char * format, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options);
extern THORHELPER_API IDiskRowReader * createDiskReader(const char * format, bool streamRemote, unsigned _expectedCrc, IOutputMetaData & _expected, unsigned _projectedCrc, IOutputMetaData & _projected, unsigned _actualCrc, IOutputMetaData & _actual, const IPropertyTree * options);

#endif
