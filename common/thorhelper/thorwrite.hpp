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

#include "jrowstream.hpp"
#include "rtlkey.hpp"
#include "thorread.hpp"

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

THORHELPER_API void getDefaultWritePlane(StringBuffer & plane, unsigned helperFlags);

#endif // __THORWRITE_HPP_
