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

#ifndef _CCDKEY_INCL
#define _CCDKEY_INCL
#include "rtlkey.hpp"
#include "eclhelper.hpp"
#include "jfile.hpp"
#include "rtlcommon.hpp"
#include "rtlnewkey.hpp"

interface IFileIOArray;
interface ITranslatorSet;

typedef IArrayOf<IKeySegmentMonitor> SegMonitorArray;

/**
 * IDirectStreamReader is used by CSV/XML readers. They bypass the record translation of the associated
 * IDirectReader (this remains TBD at this point)
 *
 */
interface IDirectStreamReader : extends ISerialStream, extends ISimpleReadStream
{
    virtual unsigned queryFilePart() const = 0; // used by CSV
    virtual unsigned __int64 makeFilePositionLocal(offset_t pos) = 0; // used by XML
};

/**
 * IDirectReader is used by Roxie disk activities when the whole file needs to be scanned.
 * There are in-memory and on-disk implementations, and ones that use indexes to seek directly to
 * matching rows. Translated rows are returned.
 *
 */
interface IDirectReader : extends IThorDiskCallback
{
    virtual IDirectStreamReader *queryDirectStreamReader() = 0;
    virtual const byte *nextRow() = 0;
    virtual void finishedRow() = 0;
    virtual void serializeCursorPos(MemoryBuffer &mb) const = 0;
    virtual bool isKeyed() const = 0;
};

class ScoredRowFilter : public RowFilter
{
public:
    unsigned scoreKey(const UnsignedArray &sortFields) const;
    unsigned getMaxScore() const;
};

interface IInMemoryIndexManager : extends IInterface
{
    virtual void load(IFileIOArray *, IOutputMetaData *preloadLayout, bool preload) = 0;
    virtual bool IsShared() const = 0;
    virtual IDirectReader *selectKey(ScoredRowFilter &filter, const ITranslatorSet *translators) const = 0;
    virtual IDirectReader *selectKey(const char *sig, ScoredRowFilter &filter, const ITranslatorSet *translators) const = 0;
    virtual IDirectReader *createReader(const RowFilter &postFilter, bool _grouped, offset_t readPos, unsigned partNo, unsigned numParts, const ITranslatorSet *translators) const = 0;
    virtual void setKeyInfo(IPropertyTree &indexInfo) = 0;
};

extern IInMemoryIndexManager *createInMemoryIndexManager(const RtlRecord &recInfo, bool isOpt, const char *fileName);

#endif
