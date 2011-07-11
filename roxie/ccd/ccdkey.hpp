/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    virtual IInMemoryIndexCursor *createCursor() = 0;
    virtual IDirectReader *createReader(offset_t readPos, unsigned partNo, unsigned numParts) = 0;
    virtual void getTrackedInfo(const char *id, StringBuffer &xml) = 0;
    virtual void setKeyInfo(IPropertyTree &indexInfo) = 0;
};

extern IInMemoryIndexManager *createInMemoryIndexManager(bool isOpt, const char *fileName);
extern void reportInMemoryIndexStatistics(StringBuffer &reply, const char *filename, unsigned count);
extern IInMemoryIndexManager *getEmptyIndexManager();

#endif
