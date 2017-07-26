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

interface IFileIOArray;
typedef IArrayOf<IKeySegmentMonitor> SegMonitorArray;

interface IDirectReader : public ISerialStream
{
    virtual IThorDiskCallback *queryThorDiskCallback() = 0;
    virtual ISimpleReadStream *querySimpleStream() = 0;
    virtual unsigned queryFilePart() const = 0;
    virtual unsigned __int64 makeFilePositionLocal(offset_t pos) = 0;
};

interface IInMemoryIndexCursor : public IThorDiskCallback, public IIndexReadContext
{
    virtual void reset() = 0;
    virtual bool selectKey() = 0;
    virtual const void *nextMatch() = 0;
    virtual bool isFiltered(const void *row) = 0;
    virtual void serializeCursorPos(MemoryBuffer &mb) const = 0;
    virtual void deserializeCursorPos(MemoryBuffer &mb) = 0;
};

interface IInMemoryIndexManager : extends IInterface
{
    virtual void load(IFileIOArray *, IRecordSize *, bool preload, int numKeys) = 0;
    virtual bool IsShared() const = 0;
    virtual IInMemoryIndexCursor *createCursor(const RtlRecord &recInfo) = 0;
    virtual IDirectReader *createReader(offset_t readPos, unsigned partNo, unsigned numParts) = 0;
    virtual void getTrackedInfo(const char *id, StringBuffer &xml) = 0;
    virtual void setKeyInfo(IPropertyTree &indexInfo) = 0;
};

extern IInMemoryIndexManager *createInMemoryIndexManager(bool isOpt, const char *fileName);
extern void reportInMemoryIndexStatistics(StringBuffer &reply, const char *filename, unsigned count);
extern IInMemoryIndexManager *getEmptyIndexManager();

#endif
