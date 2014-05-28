/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "eclrtl.hpp"

#include "hqlexpr.hpp"
#include "hqlthql.hpp"
#include "fvresultset.ipp"
#include "fileview.hpp"
#include "fvdisksource.ipp"
#include "fvwugen.hpp"
#include "fverror.hpp"
#include "dasess.hpp"

#define DEFAULT_MAX_CSV_SIZE    0x10000  // 64k

PhysicalFileInfo::PhysicalFileInfo()
{
    cachedPart = (unsigned)-1;
    totalSize = 0;
}


offset_t getPartSize(IDistributedFilePart & part, unsigned copy)
{
    try
    {
        RemoteFilename rfn;
        Owned<IFile> in = createIFile(part.getFilename(rfn,copy));
        return in->size();
    }
    catch (IException * e)
    {
        e->Release();
    }
    return (offset_t) -1;
}

void PhysicalFileInfo::init(IDistributedFile * _df)
{
    df.set(_df);
    totalSize = 0;
    Owned<IDistributedFilePartIterator> iter = df->getIterator();
    ForEach(*iter)
    {
        IDistributedFilePart & cur = iter->query();

        offset_t partSize = cur.getFileSize(true, false);
        if (partSize == -1)
            partSize = getPartSize(cur, 0);
        if (partSize == -1)
            partSize = getPartSize(cur, 1);
        if (partSize == -1)
            partSize = 0x100000;        // force an error when the part is opened.
        partSizes.append(partSize);
        totalSize += partSize;
    }
}


offset_t PhysicalFileInfo::getOptimizedOffset(offset_t offset, unsigned copyLength)
{
    offset_t newOffset = 0;
    ForEachItemIn(idx, partSizes)
    {
        offset_t curSize = partSizes.item(idx);
        if (offset < curSize)
            return newOffset + ((offset) / copyLength) * copyLength;
        newOffset += curSize;
        offset -= curSize;
    }
    return newOffset;
}

bool PhysicalFileInfo::readData(MemoryBuffer & out, __int64 startOffset, size32_t copyLength)
{
    CriticalBlock procedure(cs);

    offset_t chunkOffset = startOffset;
    unsigned numParts = partSizes.ordinality();
    unsigned part;
    offset_t curPartLength;
    if (isLocalFpos(startOffset))
    {
        part = getLocalFposPart(startOffset);
        chunkOffset = getLocalFposOffset(startOffset);
        if (part >= numParts)
            return false;
        curPartLength = partSizes.item(part);
    }
    else
    {
        for (part = 0; part < numParts; part++)
        {
            curPartLength = partSizes.item(part);
            if (chunkOffset < curPartLength)
                break;
            chunkOffset -= curPartLength;
        }
    }

    if (part == numParts)
        return false;

    bool isLast = false;
    if (chunkOffset + copyLength >= curPartLength)
    {
        copyLength = (size32_t)(curPartLength - chunkOffset);
        isLast = true;
    }

    if (part != cachedPart)
    {
        cachedPart = (unsigned)-1;
        cachedFile.clear();
        cachedIO.clear();

        Owned<IDistributedFilePart> dfp = df->getPart(part);
        try
        { 
            RemoteFilename rfn;
            cachedFile.setown(createIFile(dfp->getFilename(rfn)));
            cachedIO.setown(cachedFile->open(IFOread));
        }
        catch (IException * e)
        {
            e->Release();
        }
        if (!cachedIO)
        {
            RemoteFilename rfn;
            cachedFile.setown(createIFile(dfp->getFilename(rfn,1)));
            cachedIO.setown(cachedFile->open(IFOread));
            if (!cachedIO)
            {
                StringBuffer str;
                throwError1(FVERR_FailedOpenFile, dfp->getPartName(str).str());
                return false;
            }
        }
        if (df->isCompressed())
        {
            cachedIO.setown(createCompressedFileReader(cachedIO));
            if (!cachedIO)
            {
                StringBuffer str;
                throwError1(FVERR_FailedOpenCompressedFile, dfp->getPartName(str).str());
                return false;
            }
        }

        cachedPart = part;
    }

    char * data = (char *)out.clear().reserve(copyLength);
    unsigned numGot = cachedIO->read(chunkOffset, copyLength, data);
    out.setLength(numGot);
    return isLast;
}


void PhysicalFileInfo::close()
{
    cachedPart = (unsigned)-1;
    cachedFile.clear();
    cachedIO.clear();
}

//---------------------------------------------------------------------------

DiskDataSource::DiskDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char* _username, const char* _password)
{
    logicalName.set(_logicalName);
    diskRecord.set(_diskRecord);

    Owned<IUserDescriptor> udesc;
    if(_username != NULL && *_username != '\0')
    {
        udesc.setown(createUserDescriptor());
        udesc->set(_username, _password);
    }

    df.setown(queryDistributedFileDirectory().lookup(logicalName, udesc.get()));
}


//---------------------------------------------------------------------------

DirectDiskDataSource::DirectDiskDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char* _username, const char* _password) : DiskDataSource(_logicalName, _diskRecord, _username, _password)
{
}


bool DirectDiskDataSource::init()
{
    if (!df)
        return false;

    IPropertyTree & properties = df->queryAttributes();
    const char * kind = properties.queryProp("@kind");
    bool isGrouped =properties.getPropBool("@grouped");
    if (kind && (stricmp(kind, "key") == 0))
        throwError1(FVERR_CannotViewKey, logicalName.get());

    //Need to assign the transformed record to meta
    diskMeta.setown(new DataSourceMetaData(diskRecord, 0, true, isGrouped, 0));
    if (!returnedMeta)
    {
        returnedMeta.set(diskMeta);
        returnedRecordSize.set(returnedMeta);
    }
    if (!transformedMeta)
        transformedMeta.set(returnedMeta);

    addFileposition();
    physical.init(df);
    if (diskMeta->isFixedSize())
    {
        if (diskMeta->fixedSize() == 0)
            throwError1(FVERR_ZeroSizeRecord, logicalName.get());
        totalRows = physical.totalSize / diskMeta->fixedSize();
    }
    else if (properties.hasProp("@recordCount"))
        totalRows = properties.getPropInt64("@recordCount");
    else
        totalRows = UNKNOWN_NUM_ROWS;

    readBlockSize = 4 * diskMeta->getRecordSize(NULL);
    if (readBlockSize < DISK_BLOCK_SIZE) readBlockSize = DISK_BLOCK_SIZE;
    return true;
}


bool DirectDiskDataSource::fetchRowData(MemoryBuffer & out, __int64 offset)
{
    physical.readData(out, offset, returnedMeta->getMaxRecordSize());
    if (out.length() == 0)
        return false;
    size32_t actualLength = returnedMeta->getRecordSize(out.toByteArray());
    if (actualLength > readBlockSize)
        throwError(FVERR_RowTooLarge);

    out.setLength(actualLength);
    return true;
}

size32_t DirectDiskDataSource::getCopyLength()
{
    size32_t copyLength = readBlockSize;
    if (returnedMeta->isFixedSize())
    {
        unsigned fixedSize = returnedMeta->fixedSize();
        copyLength = (copyLength / fixedSize) * fixedSize;
    }
    return copyLength;
}


void DirectDiskDataSource::improveLocation(__int64 row, RowLocation & location)
{
    if (!returnedMeta->isFixedSize())
        return;

    //Align the row so the chunks don't overlap....
    unsigned fixedSize = returnedMeta->fixedSize();
    size32_t copyLength = getCopyLength();
    location.bestOffset = physical.getOptimizedOffset(row * fixedSize, copyLength);
    location.bestRow = location.bestOffset / fixedSize;
}


bool DirectDiskDataSource::loadBlock(__int64 startRow, offset_t startOffset)
{
    size32_t copyLength = getCopyLength();
    MemoryBuffer temp;
    bool isLast = physical.readData(temp, startOffset, copyLength);
    if (temp.length() == 0)
        return false;

    RowBlock * rows;
    if (returnedMeta->isFixedSize())
        rows = new FixedRowBlock(temp, startRow, startOffset, returnedMeta->fixedSize());
    else
        rows = new VariableRowBlock(temp, startRow, startOffset, returnedRecordSize, isLast);
    cache.addRowsOwn(rows);
    return true;
}

void DirectDiskDataSource::onClose()    
{ 
    DiskDataSource::onClose();
    if (openCount == 0)
        physical.close();
}

//---------------------------------------------------------------------------

UtfReader::UtfFormat getFormat(const char * format)
{
    if (memicmp(format, "utf", 3) == 0)
    {
        const char * tail = format + 3;
        if (*tail == '-')
            tail++;
        if (stricmp(tail, "8N")==0)
            return UtfReader::Utf8;
        else if (stricmp(tail, "16BE")==0)
            return UtfReader::Utf16be;
        else if (stricmp(tail, "16LE")==0)
            return UtfReader::Utf16le;
        else if (stricmp(tail, "32BE")==0)
            return UtfReader::Utf32be;
        else if (stricmp(tail, "32LE")==0)
            return UtfReader::Utf32le;
        else
            throwError1(FVERR_UnknownUTFFormat, format);
    }
    return UtfReader::Utf8;
}

enum { NONE=0, SEPARATOR=1, TERMINATOR=2, WHITESPACE=3, QUOTE=4, ESCAPE=5 };

void CsvRecordSize::init(IDistributedFile * df)
{
    IPropertyTree * props = &df->queryAttributes();
    UtfReader::UtfFormat utfType = getFormat(props->queryProp("@format"));
    switch (utfType)
    {
    case UtfReader::Utf16be:
    case UtfReader::Utf16le:
        unitSize = 2;
        break;
    case UtfReader::Utf32be:
    case UtfReader::Utf32le:
        unitSize = 4;
        break;
    default:
        unitSize = 1;
    }
    maxRecordSize = props->getPropInt("@maxRecordSize", DEFAULT_MAX_CSV_SIZE);
    const char * terminate = props->queryProp("@csvTerminate");
    addUtfActionList(matcher, terminate ? terminate : "\\n,\\r\\n", TERMINATOR, NULL, utfType);

    const char * separate = props->queryProp("@csvSeparate");
    addUtfActionList(matcher, separate ? separate : "\\,", SEPARATOR, NULL, utfType);

    const char * quote = props->queryProp("@csvQuote");
    addUtfActionList(matcher, quote ? quote : "\"", QUOTE, NULL, utfType);

    const char * escape = props->queryProp("@csvEscape");
    addUtfActionList(matcher, escape, ESCAPE, NULL, utfType);

    addUtfActionList(matcher, " ",  WHITESPACE, NULL, utfType);
    addUtfActionList(matcher, "\t",  WHITESPACE, NULL, utfType);

}

size32_t CsvRecordSize::getRecordLength(size32_t maxLength, const void * start, bool includeTerminator)
{
    //If we need more complicated processing...
    unsigned quote = 0;
    unsigned quoteToStrip = 0;
    const byte * cur = (const byte *)start;
    const byte * end = (const byte *)start + maxLength;
    const byte * firstGood = cur;
    const byte * lastGood = cur;
    bool lastEscape = false;

    while (cur != end)
    {
        unsigned matchLen;
        unsigned match = matcher.getMatch(end-cur, (const char *)cur, matchLen);
        switch (match & 255)
        {
        case NONE:
            cur += unitSize;            // matchLen == 0;
            lastGood = cur;
            break;
        case WHITESPACE:
            //Skip leading whitespace
            if (quote)
                lastGood = cur+matchLen;
            else if (cur == firstGood)
            {
                firstGood = cur+matchLen;
                lastGood = cur+matchLen;
            }
            break;
        case SEPARATOR:
            // Quoted separator
            if (quote == 0)
            {
                lastEscape = false;
                quoteToStrip = 0;
                firstGood = cur + matchLen;
            }
            lastGood = cur+matchLen;
            break;
        case TERMINATOR:
            if (quote == 0) // Is this a good idea? Means a mismatched quote is not fixed by EOL
            {
                if (includeTerminator)
                    return cur + matchLen - (const byte *)start;

                return cur - (const byte *)start;
            }
            lastGood = cur+matchLen;
            break;
        case QUOTE:
            // Quoted quote
            if (quote == 0)
            {
                if (cur == firstGood)
                {
                    quote = match;
                    firstGood = cur+matchLen;
                }
                lastGood = cur+matchLen;
            }
            else
            {
                if (quote == match)
                {
                    const byte * next = cur + matchLen;
                    //Check for double quotes
                    if ((next != end))
                    {
                        unsigned nextMatchLen;
                        unsigned nextMatch = matcher.getMatch((size32_t)(end-next), (const char *)next, nextMatchLen);
                        if (nextMatch == quote)
                        {
                            quoteToStrip = quote;
                            matchLen += nextMatchLen;
                            lastGood = cur+matchLen;
                        }
                        else
                            quote = 0;
                    }
                    else
                        quote = 0;
                }
                else
                    lastGood = cur+matchLen;
            }
            break;
        case ESCAPE:
            lastEscape = true;
            lastGood = cur+matchLen;
            // If this escape is at the end, proceed to field range
            if (lastGood == end)
                break;

            // Skip escape and ignore the next match
            cur += matchLen;
            match = matcher.getMatch((size32_t)(end-cur), (const char *)cur, matchLen);
            if ((match & 255) == NONE)
                matchLen = unitSize;
            lastGood += matchLen;
            break;
        }

        cur += matchLen;
    }

    return end - (const byte *)start;

}

size32_t CsvRecordSize::getRecordSize(const void * start)
{
    if (!start) return maxRecordSize;
    return getRecordLength(maxRecordSize, start, true);

}

size32_t CsvRecordSize::getRecordSize(unsigned maxLength, const void * start)
{
    if (!start) return maxRecordSize;
    return getRecordLength(maxLength, start, true);

}

size32_t CsvRecordSize::getFixedSize() const
{
    return 0; // is variable
}


size32_t CsvRecordSize::getMinRecordSize() const
{
    return unitSize;
}

DirectCsvDiskDataSource::DirectCsvDiskDataSource(IDistributedFile * _df, const char * _format)
{
    df.set(_df);
    isUnicode = (memicmp(_format, "utf", 3) == 0);
    utfFormat = getFormat(_format);
    returnedMeta.setown(new DataSourceMetaData(isUnicode ? type_unicode : type_string));
    returnedRecordSize.set(&recordSizer);
    transformedMeta.set(returnedMeta);
    addFileposition();
    IPropertyTree & properties = df->queryAttributes();
    if (properties.hasProp("@recordCount"))
        totalRows = properties.getPropInt64("@recordCount");
}

bool DirectCsvDiskDataSource::init()
{
    physical.init(df);
    recordSizer.init(df);

    readBlockSize = 4 * recordSizer.getRecordSize(NULL);
    if (readBlockSize < DISK_BLOCK_SIZE) readBlockSize = DISK_BLOCK_SIZE;
    return true;
}


void DirectCsvDiskDataSource::copyRow(MemoryBuffer & out, size32_t length, const void * data)
{
    if (isUnicode)
    {
        unsigned offsetOfLength = out.length();
        out.append(length);
        convertUtf(out, UtfReader::Utf16le, length, data, utfFormat);
        unsigned savedLength = out.length();
        out.setWritePos(offsetOfLength);
        out.append((unsigned) (savedLength - offsetOfLength - sizeof(unsigned))/2);
        out.setWritePos(savedLength);
    }
    else
    {
        out.append(length);
        out.append(length, data);
    }
}

bool DirectCsvDiskDataSource::fetchRowData(MemoryBuffer & out, __int64 offset)
{
    MemoryBuffer temp;
    physical.readData(temp, offset, recordSizer.getRecordSize(NULL));
    if (temp.length() == 0)
        return false;
    unsigned realLength = recordSizer.getRecordSize(temp.length(), temp.toByteArray());
    copyRow(out, realLength, temp.toByteArray());
    return true;
}


bool DirectCsvDiskDataSource::loadBlock(__int64 startRow, offset_t startOffset)
{
    size32_t copyLength = readBlockSize;
    MemoryBuffer temp;
    bool isLast = physical.readData(temp, startOffset, copyLength);
    if (temp.length() == 0)
        return false;

    RowBlock * rows = new VariableRowBlock(temp, startRow, startOffset, &recordSizer, isLast);
    cache.addRowsOwn(rows);
    return true;
}


bool DirectCsvDiskDataSource::getRow(MemoryBuffer & out, __int64 row)
{
    size32_t length;
    const void * data;
    unsigned __int64 offset = 0;
    if (getRowData(row, length, data, offset))
    {
        //strip the end of line terminator from the length...
        length = recordSizer.getRecordLength(length, data, false);
        copyRow(out, length, data);
        out.append(offset);
        return true;
    }
    return false;
}


//---------------------------------------------------------------------------

WorkunitDiskDataSource::WorkunitDiskDataSource(const char * _logicalName, IConstWUResult * _wuResult, const char * _wuid, const char * _username, const char * _password) : DirectDiskDataSource(_logicalName, NULL, _username, _password)
{
    wuid.set(_wuid);
    wuResult.set(_wuResult);
}


bool WorkunitDiskDataSource::init()
{
    if (!setReturnedInfoFromResult())
        return false;
    diskRecord.set(returnedRecord);
    return DirectDiskDataSource::init();
}


//---------------------------------------------------------------------------

TranslatedDiskDataSource::TranslatedDiskDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char * _cluster, const char * _username, const char * _password)
{
    logicalName.set(_logicalName);
    diskRecord.set(_diskRecord);
    cluster.set(_cluster);
    username.set(_username);
    password.set(_password);
    openCount = 0;
}

TranslatedDiskDataSource::~TranslatedDiskDataSource()
{
    if (helperWuid)
    {
        directSource.clear();
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        factory->deleteWorkUnit(helperWuid);
    }
}

bool TranslatedDiskDataSource::createHelperWU()
{
    OwnedHqlExpr browseWUcode = buildDiskOutputEcl(logicalName, diskRecord);
    if (!browseWUcode)
        return false;

    // MORE: Where should we get these parameters from ????
    StringAttr application("fileViewer");
    StringAttr customerid("viewer");

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> workunit = factory->createWorkUnit(NULL, application, username);
    workunit->setUser(username);
    workunit->setClusterName(cluster);  
    workunit->setCustomerId(customerid);
    workunit->setAction(WUActionCompile);

    StringBuffer jobName;
    jobName.append("FileView_for_").append(logicalName);
    workunit->setJobName(jobName.str());

    StringBuffer eclText;
    toECL(browseWUcode, eclText, true);
    Owned<IWUQuery> query = workunit->updateQuery();
    query->setQueryText(eclText.str());
    query->setQueryName(jobName.str());

    workunit->setCompareMode(CompareModeOff);
    StringAttrAdaptor xxx(helperWuid); workunit->getWuid(xxx);
    return true;
}


bool TranslatedDiskDataSource::init()
{
    if (!createHelperWU() || !compileHelperWU())
        return false;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(helperWuid, false);
    Owned<IConstWUResult> dataResult = wu->getResultBySequence(0);
    directSource.setown(new WorkunitDiskDataSource(logicalName, dataResult, helperWuid, username.get(), password.get()));
    return directSource->init();
}


bool TranslatedDiskDataSource::compileHelperWU()
{
    submitWorkUnit(helperWuid, username, password);
    return waitForWorkUnitToCompile(helperWuid);
}

//---------------------------------------------------------------------------

IndirectDiskDataSource::IndirectDiskDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char * _cluster, const char * _username, const char * _password) : DiskDataSource(_logicalName, _diskRecord, _username, _password)
{
    cluster.set(_cluster);
    username.set(_username);
    password.set(_password);
    extraFieldsSize = sizeof(offset_t) + sizeof(unsigned short);
    totalSize = 0;
}

IndirectDiskDataSource::~IndirectDiskDataSource()
{
    if (browseWuid)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        factory->deleteWorkUnit(browseWuid);
    }
}

bool IndirectDiskDataSource::createBrowseWU()
{
    OwnedHqlExpr browseWUcode = buildDiskFileViewerEcl(logicalName, diskRecord);
    if (!browseWUcode)
        return false;
    returnedRecord.set(browseWUcode->queryChild(0)->queryRecord());

    // MORE: Where should we get these parameters from ????
    StringAttr application("fileViewer");
    StringAttr owner("fileViewer");
    StringAttr customerid("viewer");

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> workunit = factory->createWorkUnit(NULL, application, owner);
    workunit->setUser(owner);
    workunit->setClusterName(cluster);  
    workunit->setCustomerId(customerid);

    StringBuffer jobName;
    jobName.append("FileView_for_").append(logicalName);
    workunit->setJobName(jobName.str());

    StringBuffer eclText;
    toECL(browseWUcode, eclText, true);
    Owned<IWUQuery> query = workunit->updateQuery();
    query->setQueryText(eclText.str());
    query->setQueryName(jobName.str());

    workunit->setCompareMode(CompareModeOff);
    StringAttrAdaptor xxx(browseWuid); workunit->getWuid(xxx);
    return true;
}


bool IndirectDiskDataSource::init()
{
    if (!df)
        return false;

    if (!createBrowseWU())
        return false;

    //Need to assign the transformed record to meta
    bool isGrouped = false;     // more not sure this is strictly true...
    returnedMeta.setown(new DataSourceMetaData(returnedRecord, 2, true, isGrouped, 0));
    transformedMeta.set(returnedMeta);
    diskMeta.setown(new DataSourceMetaData(diskRecord, 0, true, isGrouped, 0));

    totalSize = df->getFileSize(true,false);

    if (diskMeta->isFixedSize())
        totalRows = totalSize / diskMeta->fixedSize();
    else
        totalRows = UNKNOWN_NUM_ROWS;
    return true;
}


bool IndirectDiskDataSource::loadBlock(__int64 startRow, offset_t startOffset)
{
    MemoryBuffer temp;

    //enter scope....>
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> wu = factory->updateWorkUnit(browseWuid);
        Owned<IWUResult> lower = wu->updateVariableByName(LOWER_LIMIT_ID);
        lower->setResultInt(startOffset);
        lower->setResultStatus(ResultStatusSupplied);

        Owned<IWUResult> dataResult = wu->updateResultBySequence(0);
        dataResult->setResultRaw(0, NULL, ResultFormatRaw);
        dataResult->setResultStatus(ResultStatusUndefined);
        wu->clearExceptions();
        if (wu->getState() != WUStateUnknown)
            wu->setState(WUStateCompiled);

        //Owned<IWUResult> count = wu->updateVariableByName(RECORD_LIMIT_ID);
        //count->setResultInt64(fetchSize);
    }

    //Resubmit the query...
    submitWorkUnit(browseWuid, username, password);
    WUState finalState = waitForWorkUnitToComplete(browseWuid, -1, true);
    if(!((finalState == WUStateCompleted) || (finalState == WUStateWait)))
        return false;

    //Now extract the results...
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(browseWuid, false);
    Owned<IConstWUResult> dataResult = wu->getResultBySequence(0);
    MemoryBuffer2IDataVal xxx(temp); dataResult->getResultRaw(xxx, NULL, NULL);

    if (temp.length() == 0)
        return false;

    RowBlock * rows;
    if (returnedMeta->isFixedSize())
        rows = new FilePosFixedRowBlock(temp, startRow, startOffset, returnedMeta->fixedSize());
    else
        rows = new FilePosVariableRowBlock(temp, startRow, startOffset, returnedMeta, true);
    cache.addRowsOwn(rows);
    return true;
}
