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

#ifndef FVIDXSOURCE_IPP
#define FVIDXSOURCE_IPP

#include "junicode.hpp"
#include "fvdatasource.hpp"
#include "dllserver.hpp"
#include "hqlexpr.hpp"
#include "eclhelper.hpp"

#include "fvsource.ipp"
#include "dadfs.hpp"
#include "rtlkey.hpp"
#include "jhtree.hpp"

//---------------------------------------------------------------------------


//An initial simple implementation
class IndexPageCache
{
public:
    IndexPageCache();

    void addRow(__int64 row, size32_t len, const void * data);
    bool getRow(__int64 row, size32_t & len, const void * & data);

    inline unsigned numRowsCached()               { return offsets.ordinality()-1; }

protected:
    MemoryBuffer saved;
    UInt64Array offsets;
    __int64 firstRow;
    unsigned __int64 offsetDelta;
};

class IndexDataSource : public FVDataSource
{
public:
    IndexDataSource(const char * _logicalName, IHqlExpression * _indexRecord, const char* _username, const char* _password);
    IndexDataSource(IndexDataSource * _other);

    virtual void applyFilter();
    virtual IFvDataSource * cloneForFilter();
    virtual __int64 numRows(bool force = false);
    virtual bool addFilter(unsigned offset, unsigned matchLen, unsigned len, const void * data);

    virtual bool init();
    virtual bool fetchRowData(MemoryBuffer & out, __int64 offset)                   { return false; }
    virtual bool getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset);
    virtual bool isIndex() { return true; }

protected:
    inline unsigned fieldSize(unsigned column)      { return keyedOffsets.item(column+1) - keyedOffsets.item(column); }
    bool getNextRow(MemoryBuffer & out, bool extractRow);
    void resetCursor();
    void translateKeyedValues(byte * row);

protected:
    StringAttr logicalName;
    IArrayOf<IKeySegmentMonitor> segMonitors;
    IArrayOf<IStringSet> values;
    Owned<DataSourceMetaData> diskMeta;
    HqlExprAttr diskRecord;
    Owned<IRtlFieldTypeDeserializer> deserializer;
    Owned<IOutputMetaData> diskRecordMeta;
    Owned<IDistributedFile> df;
    Linked<FVDataSource> original;
    unsigned __int64 totalRows;
    unsigned __int64 nextRowToRead;
    unsigned keyedSize;
    UnsignedArray keyedOffsets;
    TypeInfoArray keyedTypes;
    Owned<IKeyIndex> tlk;
    Owned<IKeyManager> manager;
    Owned<IKeyIndex> curPart;
    int curPartIndex;
    UnsignedArray matchingParts;
    unsigned numParts;
    bool singlePart;
    bool filtered;
    bool isLocal;
    bool ignoreSkippedRows;
    IndexPageCache cache;
};


#endif
