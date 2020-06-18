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

#include "jliball.hpp"
#include "eclrtl_imp.hpp"

#include "hqlexpr.hpp"
#include "fileview.hpp"
#include "fvresultset.ipp"
#include "fvidxsource.ipp"
#include "fverror.hpp"
#include "dasess.hpp"
#include "rtlrecord.hpp"
#include "rtldynfield.hpp"

#define MAX_FILE_READ_FAIL_COUNT 3

//cloned from hthor - a candidate for commoning up.
static IKeyIndex *openKeyFile(IDistributedFilePart *keyFile)
{
    unsigned failcount = 0;
    unsigned numCopies = keyFile->numCopies();
    assertex(numCopies);
    for (unsigned copy=0; copy < numCopies && failcount < MAX_FILE_READ_FAIL_COUNT; copy++)
    {
        RemoteFilename rfn;
        try
        {
            OwnedIFile ifile = createIFile(keyFile->getFilename(rfn,copy));
            unsigned __int64 thissize = ifile->size();
            if (thissize != -1)
            {
                StringBuffer remotePath;
                rfn.getPath(remotePath);
                unsigned crc = 0;
                keyFile->getCrc(crc);
                return createKeyIndex(remotePath.str(), crc, false, false);
            }
        }
        catch (IException *E)
        {
            EXCLOG(E, "While opening index file");
            E->Release();
            failcount++;
        }
    }
    RemoteFilename rfn;
    StringBuffer url;
    keyFile->getFilename(rfn).getRemotePath(url);
    throw MakeStringException(1001, "Could not open key file at %s%s", url.str(), (numCopies > 1) ? " or any alternate location." : ".");
}

//---------------------------------------------------------------------------

#define MIN_CACHED_ROWS         150
#define MAX_CACHED_ROWS         200

IndexPageCache::IndexPageCache()
{
    firstRow = 0;
    offsetDelta = 0;
    offsets.append(0);
    saved.ensureCapacity(0x10000);
}

void IndexPageCache::addRow(__int64 row, size32_t len, const void * data)
{
    if (row != firstRow + numRowsCached())
    {
        firstRow = row;
        offsetDelta = 0;
        offsets.kill();
        offsets.append(0);
        saved.setWritePos(0);
    }
    else if (numRowsCached() >= MAX_CACHED_ROWS)
    {
        unsigned numToRemove = numRowsCached() - MIN_CACHED_ROWS;
        __int64 newDelta = offsets.item(numToRemove);
        size32_t sizeLost = (size32_t)(newDelta-offsetDelta);
        //copy the cached rows
        byte * base = (byte *)saved.bufferBase();
        memmove(base, base+sizeLost, saved.length()-sizeLost);
        saved.setWritePos(saved.length()-sizeLost);
        offsets.removen(0, numToRemove);
        firstRow += numToRemove;
        offsetDelta = newDelta;
    }

    assertex(row == firstRow + numRowsCached());
    assertex(offsets.tos() == saved.length() + offsetDelta);
    saved.append(len, data);
    offsets.append(saved.length() + offsetDelta);
}

bool IndexPageCache::getRow(__int64 row, size32_t & len, const void * & data)
{
    if (row < firstRow || row >= firstRow + numRowsCached())
        return false;

    unsigned __int64 startOffset = offsets.item((unsigned)(row-firstRow));
    unsigned __int64 endOffset = offsets.item((unsigned)(row-firstRow+1));
    len = (size32_t)(endOffset - startOffset);
    data = saved.toByteArray() + (unsigned)(startOffset - offsetDelta);
    return true;
}


//---------------------------------------------------------------------------

//Could probably cope with unknown record, just displaying as binary.

IndexDataSource::IndexDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char* _username, const char* _password)
{
    logicalName.set(_logicalName);
    diskRecord.set(_diskRecord);
    deserializer.setown(createRtlFieldTypeDeserializer());
    diskRecordMeta.setown(new CDynamicOutputMetaData(* static_cast<const RtlRecordTypeInfo *>(queryRtlType(*deserializer.get(), diskRecord))));
    Owned<IUserDescriptor> udesc;
    if(_username != NULL && *_username != '\0')
    {
        udesc.setown(createUserDescriptor());
        udesc->set(_username, _password);
    }

    df.setown(queryDistributedFileDirectory().lookup(logicalName, udesc.get(),false,false,false,nullptr,defaultPrivilegedUser));
    filtered = false;
}


IndexDataSource::IndexDataSource(IndexDataSource * _other)
{
    logicalName.set(_other->logicalName);
    diskRecord.set(_other->diskRecord);
    deserializer.set(_other->deserializer);
    diskRecordMeta.set(_other->diskRecordMeta);
    df.set(_other->df);
    original.set(_other);       // stop any work units etc. being unloaded.
    diskMeta.set(_other->diskMeta);     // optimization - would be handled by init anyway
    filtered = false;
    //MORE: What else needs cloning/initializing?
}


IFvDataSource * IndexDataSource::cloneForFilter()
{
    Owned<IndexDataSource> ret = new IndexDataSource(this);
    if (ret->init())
        return ret.getClear();
    return NULL;
}

bool IndexDataSource::init()
{
    if (!df)
        return false;

    numParts = df->numParts();
    singlePart = (numParts == 1);
    ignoreSkippedRows = true;               // better if skipping to a particular point
    StringBuffer partName;
    Owned<IDistributedFilePart> kf = df->getPart(numParts-1);
    tlk.setown(openKeyFile(kf));
    if (!tlk)
        return false;

    IPropertyTree & properties = df->queryAttributes();
    //Need to assign the transformed record to meta
    if (!diskMeta)
        diskMeta.setown(new DataSourceMetaData(diskRecord, 0, true, false, tlk->keyedSize()));

    if (!returnedMeta)
    {
        returnedMeta.set(diskMeta);
        returnedRecordSize.set(returnedMeta);
    }
    if (!transformedMeta)
        transformedMeta.set(returnedMeta);

    if (properties.hasProp("@recordCount"))
        totalRows = properties.getPropInt64("@recordCount");
    else
        totalRows = UNKNOWN_NUM_ROWS;       // more: could probably count them

    isLocal = properties.hasProp("@local");

    diskMeta->extractKeyedInfo(keyedOffsets, keyedTypes);
    ForEachItemIn(i, keyedTypes)
    {
        IStringSet * set = createRtlStringSet(fieldSize(i));
        set->addAll();
        values.append(*set);
    }

    diskMeta->patchIndexFileposition(); // Now returned as a bigendian field on the end of the row

    //Default cursor if no filter is applied
    applyFilter();
    return true;
}

__int64 IndexDataSource::numRows(bool force)
{
    if (!filtered)
        return totalRows;

    //If leading component isn't filtered, then this can take a very long time...
    if (!force && values.item(0).isFullSet())
        return UNKNOWN_NUM_ROWS;

    __int64 total = 0;
    ForEachItemIn(i, matchingParts)
    {
        manager->setKey(NULL);
        curPart.clear();
        if (singlePart)
            curPart.set(tlk);
        else
        {
            Owned<IDistributedFilePart> kf = df->getPart(matchingParts.item(i));
            curPart.setown(openKeyFile(kf));
            if (!curPart)
            {
                total = UNKNOWN_NUM_ROWS;
                break;
            }
        }

        manager->setKey(curPart);
        manager->reset();
        total += manager->getCount();
    }

    manager->setKey(NULL);
    curPart.clear();
    resetCursor();
    return total;
}


bool IndexDataSource::getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset)
{
    if (cache.getRow(row, length, data))
        return true;

    if (row < 0)
        return false;

    if ((unsigned __int64)row < nextRowToRead)
        resetCursor();

    MemoryBuffer temp;
    while ((unsigned __int64)row >= nextRowToRead)
    {
        bool saveRow = !ignoreSkippedRows || (row == nextRowToRead);
        if (!getNextRow(temp.clear(), saveRow))
            return false;
        if (saveRow)
            cache.addRow(nextRowToRead, temp.length(), temp.toByteArray());
        nextRowToRead++;
    }
    return cache.getRow(row, length, data);
}


bool IndexDataSource::getNextRow(MemoryBuffer & out, bool extractRow)
{
    bool nextPart = !matchingParts.isItem((unsigned)curPartIndex);
    for (;;)
    {
        if (nextPart)
        {
            if ((curPartIndex != -1) && ((unsigned)curPartIndex >= matchingParts.ordinality()))
                return false;
            manager->setKey(NULL);
            curPart.clear();
            if ((unsigned)++curPartIndex >= matchingParts.ordinality())
                return false;
            if (singlePart)
                curPart.set(tlk);
            else
            {
                Owned<IDistributedFilePart> kf = df->getPart(matchingParts.item(curPartIndex));
                curPart.setown(openKeyFile(kf));
                if (!curPart)
                    return false;
            }
            manager->setKey(curPart);
            manager->reset();
        }
        
        if (manager->lookup(true))
        {
            if (extractRow)
            {
                if (false)
                {
                    //MORE: Allow a transformer to cope with blobs etc.
                }
                else
                {
                    const byte * thisRow = manager->queryKeyBuffer();
                    unsigned thisSize = diskMeta->getRecordSize(thisRow);
                    void * temp = out.reserve(thisSize);
                    memcpy(temp, thisRow, thisSize);
                }
            }
            return true;
        }

        nextPart = true;
    }
}

void IndexDataSource::resetCursor()
{
    curPartIndex = -1;
    nextRowToRead = 0;
}

bool IndexDataSource::addFilter(unsigned column, unsigned matchLen, unsigned sizeData, const void * data)
{
    if (!values.isItem(column))
        return false;
    unsigned curSize = fieldSize(column);
    IStringSet & set = values.item(column);
    if (set.isFullSet())
        set.reset();

    ITypeInfo & cur = keyedTypes.item(column);
    rtlDataAttr tempLow, tempHigh;
    unsigned keyedSize = cur.getSize();
    byte * temp = (byte *)alloca(keyedSize);
    const void * low = data;
    const void * high = data;
    type_t tc = cur.getTypeCode();
    switch (tc)
    {
    case type_int:
    case type_swapint:
        {
            assertex(sizeData == curSize);
            // values are already converted to bigendian and correctly biased
            break;
        }
    case type_varstring:
        //should cast from string to varstring
        break;
    case type_string:
    case type_data:
        {
            const char * inbuff = (const char *)data;
            if (matchLen != FullStringMatch)
            {
                unsigned lenLow, lenHigh;
                rtlCreateRangeLow(lenLow, tempLow.refstr(), curSize, matchLen, sizeData, inbuff);
                rtlCreateRangeHigh(lenHigh, tempHigh.refstr(), curSize, matchLen, sizeData, inbuff);
                low = tempLow.getdata();
                high = tempHigh.getdata();
            }
            else
            {
                if (tc == type_string)
                {
                    //may need to cast from ascii to ebcidic
                    rtlStrToStr(curSize, temp, sizeData, data);
                }
                else
                    rtlDataToData(curSize, temp, sizeData, data);
                low = high = temp;
            }
            break;
        }
    case type_qstring:
        {
            const char * inbuff = (const char *)data;
            unsigned lenData = rtlQStrLength(sizeData);
            unsigned lenField = cur.getStringLen();
            if (matchLen != FullStringMatch)
            {
                unsigned lenLow, lenHigh;
                rtlCreateQStrRangeLow(lenLow, tempLow.refstr(), lenField, matchLen, lenData, inbuff);
                rtlCreateQStrRangeHigh(lenHigh, tempHigh.refstr(), lenField, matchLen, lenData, inbuff);
                low = tempLow.getdata();
                high = tempHigh.getdata();
            }
            else
            {
                rtlQStrToQStr(lenField, (char *)temp, lenData, (const char *)data);
                low = high = temp;
            }
            break;
        }
    case type_unicode:
        {
            const char * inbuff = (const char *)data;
            unsigned lenData = sizeData / sizeof(UChar);
            unsigned lenField = cur.getStringLen();
            if (matchLen != FullStringMatch)
            {
                unsigned lenLow, lenHigh;
                rtlCreateUnicodeRangeLow(lenLow, tempLow.refustr(), lenField, matchLen, lenData, (const UChar *)inbuff);
                rtlCreateUnicodeRangeHigh(lenHigh, tempHigh.refustr(), lenField, matchLen, lenData, (const UChar *)inbuff);
                low = tempLow.getdata();
                high = tempHigh.getdata();
            }
            else
            {
                rtlUnicodeToUnicode(lenField, (UChar *)temp, lenData, (const UChar *)data);
                low = high = temp;
            }
            break;
        }
    default:
        assertex(sizeData == curSize);
        break;
    }
    set.addRange(low, high);
    filtered = true;
    return true;
}

void IndexDataSource::applyFilter()
{
    manager.setown(createLocalKeyManager(diskRecordMeta->queryRecordAccessor(true), tlk, NULL, false, false));
    ForEachItemIn(i, values)
    {
        IStringSet & cur = values.item(i);
        bool extend = true; // almost certainly better
        manager->append(createKeySegmentMonitor(extend, LINK(&cur), i, keyedOffsets.item(i), fieldSize(i)));
    }
    manager->finishSegmentMonitors();

    //Now work out which parts are affected.
    matchingParts.kill();
    if (singlePart)
        matchingParts.append(0);
    else if (isLocal)
    {
        for (unsigned i = 0; i < numParts; i++)
            matchingParts.append(i);
    }
    else
    {
        manager->reset();
        while (manager->lookup(false))
        {
            offset_t node = extractFpos(manager);
            if (node)
                matchingParts.append((unsigned)(node-1));
        }
    }
    resetCursor();
}

