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

#ifndef FVDISKSOURCE_IPP
#define FVDISKSOURCE_IPP

#include "junicode.hpp"
#include "fvdatasource.hpp"
#include "dllserver.hpp"
#include "hqlexpr.hpp"
#include "eclhelper.hpp"

#include "fvsource.ipp"
#include "dadfs.hpp"


class PhysicalFileInfo
{
public:
    PhysicalFileInfo();
    
    void close();
    offset_t getOptimizedOffset(offset_t offset, unsigned copyLength);
    void init(IDistributedFile * _df);
    bool readData(MemoryBuffer & out, __int64 offset, size32_t length);

public:
    Owned<IDistributedFile> df;

    CriticalSection cs;
    unsigned __int64 totalSize;
    unsigned cachedPart;
    OwnedIFile cachedFile;
    OwnedIFileIO cachedIO;
    Int64Array partSizes;
};

class DiskDataSource : public PagedDataSource
{
public:
    DiskDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char* _username, const char* _password);

protected:
    StringAttr logicalName;
    Owned<DataSourceMetaData> diskMeta;
    HqlExprAttr diskRecord;
    Owned<IDistributedFile> df;
};


class DirectDiskDataSource : public DiskDataSource
{
public:
    DirectDiskDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char* _username, const char* _password);

    virtual bool init();
    virtual void onClose();
    virtual bool fetchRowData(MemoryBuffer & out, __int64 offset);
    virtual bool isIndex() { return false; }

protected:
    size32_t getCopyLength();
    virtual bool loadBlock(__int64 startRow, offset_t startOffset);
    void improveLocation(__int64 row, RowLocation & location);

protected:
    PhysicalFileInfo physical;
    size32_t readBlockSize;
};


class CsvRecordSize : public CInterface, implements IRecordSizeEx
{
public:
    IMPLEMENT_IINTERFACE

    void init(IDistributedFile * df);

    virtual size32_t getRecordSize(const void *rec);
    virtual size32_t getRecordSize(unsigned maxLength, const void *rec);
    virtual size32_t getFixedSize() const;
    virtual size32_t getMinRecordSize() const;

    size32_t getRecordLength(size32_t maxLength, const void * start, bool includeTerminator);


protected:
    StringMatcher matcher;
    size32_t unitSize;
    size32_t maxRecordSize;
};
    
class DirectCsvDiskDataSource  : public PagedDataSource
{
public:
    DirectCsvDiskDataSource(IDistributedFile * _df, const char * _format);

    virtual bool init();
    virtual bool isIndex() { return false; }
    virtual bool fetchRowData(MemoryBuffer & out, __int64 offset);
    virtual bool loadBlock(__int64 startRow, offset_t startOffset);

    virtual bool getRow(MemoryBuffer & out, __int64 row);

protected:
    void copyRow(MemoryBuffer & out, size32_t length, const void * data);

protected:
    Owned<IDistributedFile> df;
    bool isUnicode;
    UtfReader::UtfFormat utfFormat;
    PhysicalFileInfo physical;
    CsvRecordSize recordSizer;
    size32_t readBlockSize;
};

class WorkunitDiskDataSource : public DirectDiskDataSource
{
public:
    WorkunitDiskDataSource(const char * _logicalName, IConstWUResult * _wuResult, const char * _wuid, const char * _username, const char * _password);

    virtual bool init();
};


class TranslatedDiskDataSource : public ADataSource
{
public:
    TranslatedDiskDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char * _cluster, const char * _username, const char * _password);
    ~TranslatedDiskDataSource();

    virtual bool init();
    virtual IFvDataSourceMetaData * queryMetaData()         { return directSource->queryMetaData(); }
    virtual __int64 numRows(bool force = false)             { return directSource->numRows(force); }
    virtual bool fetchRow(MemoryBuffer & out, __int64 offset) { return directSource->fetchRow(out, offset); }
    virtual bool fetchRawRow(MemoryBuffer & out, __int64 offset) { return directSource->fetchRawRow(out, offset); }
    virtual bool getRow(MemoryBuffer & out, __int64 row)    { return directSource->getRow(out, row); }
    virtual bool getRawRow(MemoryBuffer & out, __int64 row) { return directSource->getRawRow(out, row); }
    virtual bool isIndex() { return false; }
    virtual void onClose()  { openCount--; }
    virtual void onOpen()   { openCount++; }

protected:
    bool createHelperWU();
    bool compileHelperWU();

protected:
    StringAttr helperWuid;
    StringAttr logicalName;
    StringAttr cluster;
    StringAttr username;
    StringAttr password;
    HqlExprAttr diskRecord;
    Owned<ADataSource> directSource;
    unsigned openCount;
};



class IndirectDiskDataSource : public DiskDataSource
{
public:
    IndirectDiskDataSource(const char * _logicalName, IHqlExpression * _diskRecord, const char * _cluster, const char * _username, const char * _password);
    ~IndirectDiskDataSource();

    virtual bool init();

protected:
    bool createBrowseWU();
    virtual bool loadBlock(__int64 startRow, offset_t startOffset);

protected:
    StringAttr browseWuid;
    StringAttr queue;
    StringAttr cluster;
    StringAttr username;
    StringAttr password;
    unsigned __int64 totalSize;
};


#endif
