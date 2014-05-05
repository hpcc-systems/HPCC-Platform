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

#include "platform.h"
#include "jliball.hpp"
#include "rtlbcd.hpp"
#include "workunit.hpp"
#include "seclib.hpp"
#include "eclrtl.hpp"

#include "fvresultset.ipp"

#include "fileview.hpp"
#include "fverror.hpp"
#include "fvdatasource.hpp"
#include "fvwusource.ipp"
#include "fvresultset.ipp"
#include "fvdisksource.ipp"
#include "fvidxsource.ipp"
#include "fvrelate.ipp"
#include "dasess.hpp"

#include "thorxmlwrite.hpp"
#include "eclhelper.hpp"

#define DEFAULT_FETCH_SIZE 100
#define FILEVIEW_VERSION 1
#define MAX_SORT_ELEMENTS 1000
//#define PAGELOADED_WORKUNITS
#define MAX_FILTER_ELEMENTS 20000
#define MAX_SKIP_ELEMENTS 1000
#define MAX_SKIP_TIME 10000             // 10 seconds, no match then give up

CResultSetMetaData * nullMeta;
ITypeInfo * filePositionType;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    nullMeta = new CResultSetMetaData(NULL, false);
    filePositionType = makeIntType(8, false);
    return true;
}
MODULE_EXIT()
{
    filePositionType->Release();
    nullMeta->Release();
}

//---------------------------------------------------------------------------

IFvDataSource * createDataSource(IConstWUResult * wuResult, const char * wuid, const char * username, const char * password)
{
    Owned<ADataSource> ds;
    SCMStringBuffer tempFilename;
    wuResult->getResultFilename(tempFilename);
    __int64 rowLimit = wuResult->getResultRowLimit();
    if (tempFilename.length())
        ds.setown(new WorkunitDiskDataSource(tempFilename.str(), wuResult, wuid, username, password));
    else if (rowLimit == -2)
        assertex(!"Delayed queries not yet supported");
    else if ((rowLimit == 0) && (wuResult->getResultTotalRowCount() == 0))
        ds.setown(new NullDataSource);
#ifdef PAGELOADED_WORKUNITS
    else if (wuResult->getResultDataSize() < PAGED_WU_LIMIT)
        ds.setown(new FullWorkUnitDataSource(wuResult, wuid));
    else
        ds.setown(new PagedWorkUnitDataSource(wuResult, wuid));
#else
    else
        ds.setown(new FullWorkUnitDataSource(wuResult, wuid));
#endif

    if (ds && ds->init())
        return ds.getClear();
    return NULL;
}

IFvDataSource * createFileDataSource(const char * logicalName, const char * cluster, const char * username, const char * password)
{
    Owned<IUserDescriptor> udesc;
    if(username != NULL && *username != '\0')
    {
        udesc.setown(createUserDescriptor());
        udesc->set(username, password);
    }

    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, udesc.get());
    if (!df)
        throwError1(FVERR_CouldNotResolveX, logicalName);
    return createFileDataSource(df, logicalName, cluster, username, password);
}

IFvDataSource * createFileDataSource(IDistributedFile * df, const char * logicalName, const char * cluster, const char * username, const char * password)
{
    bool blocked;
    if (df->isCompressed(&blocked) && !blocked)
        throwError1(FVERR_CompressedFile, logicalName);

    IPropertyTree & properties = df->queryAttributes();
    const char * format = properties.queryProp("@format");
    if (format && (stricmp(format,"csv")==0 || memicmp(format, "utf", 3) == 0))
    {
        Owned<ADataSource> ds = new DirectCsvDiskDataSource(df, format);
        if (ds && ds->init())
            return ds.getClear();
        return NULL;
    }
    const char * recordEcl = properties.queryProp("ECL");
    OwnedHqlExpr diskRecord;
    if (recordEcl)
    {
        diskRecord.setown(parseQuery(recordEcl));

        if (!diskRecord)
            throwError1(FVERR_BadRecordDesc, logicalName);
    }
    else
    {
        size32_t len = (size32_t)properties.getPropInt("@recordSize", 0);
        if (len)
        {
            VStringBuffer recordText("{ string%u contents };", len);
            diskRecord.setown(parseQuery(recordText));
        }

        if (!diskRecord)
            throwError1(FVERR_NoRecordDescription, logicalName);
    }

    Owned<ADataSource> ds;
    try
    {
        const char * kind = properties.queryProp("@kind");
        if (kind && (stricmp(kind, "key") == 0))
        {
            if (isSimplifiedRecord(diskRecord, true))
                ds.setown(new IndexDataSource(logicalName, diskRecord, username, password));
            else
                throwError1(FVERR_ViewComplexKey, logicalName);
        }
        else if (isSimplifiedRecord(diskRecord, false))
            ds.setown(new DirectDiskDataSource(logicalName, diskRecord, username, password));
        else if (cluster)
            ds.setown(new TranslatedDiskDataSource(logicalName, diskRecord, cluster, username, password));
        else
            throwError1(FVERR_NeedClusterToBrowseX, logicalName);
    }
    catch (IException * e)
    {
        ds.setown(new FailureDataSource(diskRecord, e, false, 0));
        e->Release();
    }

    if (ds && ds->init())
        return ds.getClear();
    return NULL;
}

//---------------------------------------------------------------------------

static __int64 getIntBias(unsigned size)
{
    return I64C(1) << (size * 8 - 1);
}

static __int64 getIntFromSwapInt(ITypeInfo & type, const void * cur, bool isMappedIndexField);

static __int64 getIntFromInt(ITypeInfo & type, const void * cur, bool isMappedIndexField)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
    if (isMappedIndexField) return getIntFromSwapInt(type, cur, isMappedIndexField);
#endif

    unsigned size=type.getSize();
    bool isSigned = type.isSigned();
    if (isSigned && !isMappedIndexField)
    {
        switch (size)
        {
        case 1: return *((signed char *)cur);
        case 2: return *((short *)cur);
        case 3: return rtlReadInt3(cur);
        case 4: return *((int *)cur);
        case 5: return rtlReadInt5(cur);
        case 6: return rtlReadInt6(cur);
        case 7: return rtlReadInt7(cur);
        case 8: return *((__int64 *)cur);
        }
    }
    else
    {
        unsigned __int64 result;
        switch (size)
        {
        case 1: result = *((unsigned char *)cur); break;
        case 2: result = *((unsigned short *)cur); break;
        case 3: result = rtlReadUInt3(cur); break;
        case 4: result = *((unsigned int *)cur); break;
        case 5: result = rtlReadUInt5(cur); break;
        case 6: result = rtlReadUInt6(cur); break;
        case 7: result = rtlReadUInt7(cur); break;
        case 8: result = *((unsigned __int64 *)cur); break;
        default: UNIMPLEMENTED;
        }
        if (isSigned && isMappedIndexField)
            result -= getIntBias(size);
        return result;
    }
    UNIMPLEMENTED;
}

static __int64 getIntFromSwapInt(ITypeInfo & type, const void * cur, bool isMappedIndexField)
{
#if __BYTE_ORDER != __LITTLE_ENDIAN
    if (insideIndex) return getIntFromInt(type, cur, isMappedIndexField);
#endif
    unsigned size = type.getSize();
    bool isSigned = type.isSigned();
    if (isSigned && !isMappedIndexField)
    {
        switch (size)
        {
        case 1: return *((signed char *)cur);
        case 2: return rtlRevInt2(cur);
        case 3: return rtlRevInt3(cur);
        case 4: return rtlRevInt4(cur);
        case 5: return rtlRevInt5(cur);
        case 6: return rtlRevInt6(cur);
        case 7: return rtlRevInt7(cur);
        case 8: return rtlRevInt8(cur);
        }
    }
    else
    {
        unsigned __int64 result;
        switch (size)
        {
        case 1: result = *((unsigned char *)cur); break;
        case 2: result = rtlRevUInt2(cur); break;
        case 3: result = rtlRevUInt3(cur); break;
        case 4: result = rtlRevUInt4(cur); break;
        case 5: result = rtlRevUInt5(cur); break;
        case 6: result = rtlRevUInt6(cur); break;
        case 7: result = rtlRevUInt7(cur); break;
        case 8: result = rtlRevUInt8(cur); break;
        default:
            throwUnexpected();
        }
        if (isSigned && isMappedIndexField)
            result -= getIntBias(size);
        return result;
    }
    UNIMPLEMENTED;
}

//---------------------------------------------------------------------------

CResultSetMetaData::CResultSetMetaData(IFvDataSourceMetaData * _meta, bool _useXPath) 
{ 
    meta = _meta; 
    fixedSize = true;
    alwaysUseXPath = _useXPath;
    unsigned max = meta ? meta->numColumns() : 0;
    for (unsigned idx = 0; idx < max; idx++)
    {
        ITypeInfo * type = meta->queryType(idx);
        CResultSetColumnInfo * column = new CResultSetColumnInfo;
        column->type = type;
        column->flag = meta->queryFieldFlags(idx);
        if (type->getSize() == UNKNOWN_LENGTH)
            fixedSize = false;
        switch (type->getTypeCode())
        {
        case type_void:
        case type_boolean:
        case type_int:
        case type_swapint:
        case type_decimal:
        case type_real:
        case type_data:
        case type_string:
        case type_varstring:
        case type_qstring:
        case type_unicode:
        case type_varunicode:
        case type_utf8:
        case type_packedint:
            column->childMeta.set(nullMeta);
            break;
        case type_set:
        case type_table:
        case type_groupedtable:
            column->childMeta.setown(new CResultSetMetaData(meta->queryChildMeta(idx), _useXPath));
            break;
        default:
            UNIMPLEMENTED;
        }
        columns.append(*column);
    }
}


CResultSetMetaData::CResultSetMetaData(const CResultSetMetaData & _other)
{
    meta = _other.meta;
    alwaysUseXPath = _other.alwaysUseXPath;
    ForEachItemIn(i, _other.columns)
        columns.append(OLINK(_other.columns.item(i)));
    fixedSize = _other.fixedSize;
}


void CResultSetMetaData::calcFieldOffsets(const byte * data, unsigned * offsets) const
{
    unsigned curOffset = 0;
    ForEachItemIn(idx, columns)
    {
        ITypeInfo & type = *columns.item(idx).type;
        unsigned size = type.getSize();
        if (size == UNKNOWN_LENGTH)
        {
            const byte * cur = data + curOffset;
            switch (type.getTypeCode())
            {
            case type_data:
            case type_string:
            case type_table:
            case type_groupedtable:
                size = *((unsigned *)cur) + sizeof(unsigned);
                break;
            case type_set:
                size = *((unsigned *)(cur + sizeof(bool))) + sizeof(unsigned) + sizeof(bool);
                break;
            case type_qstring:
                size = rtlQStrSize(*((unsigned *)cur)) + sizeof(unsigned);
                break;
            case type_unicode:
                size = *((unsigned *)cur)*sizeof(UChar) + sizeof(unsigned);
                break;
            case type_utf8:
                size = sizeof(unsigned) + rtlUtf8Size(*(unsigned *)cur, cur+sizeof(unsigned));
                break;
            case type_varstring:
                size = strlen((char *)cur)+1;
                break;
            case type_varunicode:
                size = (rtlUnicodeStrlen((UChar *)cur)+1)*sizeof(UChar);
                break;
            case type_packedint:
                size = rtlGetPackedSize(cur);
                break;
            default:
                UNIMPLEMENTED;
            }
        }
        offsets[idx] = curOffset;
        curOffset += size;
    }
    offsets[columns.ordinality()] = curOffset;
}


IResultSetMetaData * CResultSetMetaData::getChildMeta(int column) const
{
    if (columns.isItem(column))
        return LINK(columns.item(column).childMeta);
    return NULL;
}

int CResultSetMetaData::getColumnCount() const
{
    return columns.ordinality();
}

DisplayType CResultSetMetaData::getColumnDisplayType(int columnIndex) const
{
    CResultSetColumnInfo & curColumn = columns.item(columnIndex);
    unsigned flag = curColumn.flag;
    switch (flag)
    {
    case FVFFbeginif:
        return TypeBeginIfBlock;
    case FVFFendif:
        return TypeEndIfBlock;
    case FVFFbeginrecord:
        return TypeBeginRecord;
    case FVFFendrecord:
        return TypeEndRecord;
    }

    ITypeInfo & type = *curColumn.type;
    switch (type.getTypeCode())
    {
    case type_boolean:
        return TypeBoolean;
    case type_int:
    case type_swapint:
    case type_packedint:
        if (type.isSigned())
            return TypeInteger;
        return TypeUnsignedInteger;
    case type_decimal:
    case type_real:
        return TypeReal;
    case type_qstring:
    case type_string:
    case type_varstring:
        return TypeString;
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        return TypeUnicode;
    case type_data:
        return TypeData;
    case type_set:
        return TypeSet;
    case type_table:
    case type_groupedtable:
        return TypeDataset;
    }
    UNIMPLEMENTED;  // Should have been translated to one of the above by this point...
    return TypeUnknown;
}


IStringVal & CResultSetMetaData::getColumnLabel(IStringVal & s, int column) const
{
    assertex(columns.isItem(column));
    s.set(meta->queryName(column));
    return s;
}


IStringVal & CResultSetMetaData::getColumnEclType(IStringVal & s, int column) const
{
    assertex(columns.isItem(column));
    StringBuffer str;
    s.set(columns.item(column).type->getECLType(str).str());
    return s;
}


IStringVal & CResultSetMetaData::getColumnXmlType(IStringVal & s, int column) const
{
    //This really doesn't make any sense - only makes sense to get the entire schema because of the user-defined types
    UNIMPLEMENTED;
}


bool CResultSetMetaData::isSigned(int column) const
{
    assertex(columns.isItem(column));
    return columns.item(column).type->isSigned();
}


bool CResultSetMetaData::isEBCDIC(int column) const
{
    assertex(columns.isItem(column));
    ICharsetInfo * charset = columns.item(column).type->queryCharset();
    return (charset && charset->queryName() == ebcdicAtom);
}


bool CResultSetMetaData::isBigEndian(int column) const
{
    assertex(columns.isItem(column));
    ITypeInfo * type = columns.item(column).type;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return (type->getTypeCode() == type_swapint);
#else
    return (type->getTypeCode() != type_swapint);
#endif
}


unsigned CResultSetMetaData::getColumnRawType(int column) const
{
    assertex(columns.isItem(column));
    return getClarionResultType(columns.item(column).type);
}


unsigned CResultSetMetaData::getColumnRawSize(int column) const
{
    assertex(columns.isItem(column));
    unsigned size = columns.item(column).type->getSize();
    return (size == UNKNOWN_LENGTH) ? 0 : size;
}


unsigned CResultSetMetaData::getNumKeyedColumns() const
{
    return meta->numKeyedColumns();
}


IStringVal & CResultSetMetaData::getNaturalColumnLabel(IStringVal & s, int columnIndex) const
{
    assertex(columns.isItem(columnIndex));
    CResultSetColumnInfo & column = columns.item(columnIndex);
    s.set(column.naturalName);
    return s;
}

bool CResultSetMetaData::isVirtual(int columnIndex) const
{
    assertex(columns.isItem(columnIndex));
    CResultSetColumnInfo & column = columns.item(columnIndex);
    return (column.flag == FVFFvirtual);
}


bool CResultSetMetaData::hasGetTranslation(int columnIndex) const
{
    assertex(columns.isItem(columnIndex));
    CResultSetColumnInfo & column = columns.item(columnIndex);
    return (column.getTransforms.ordinality() != 0);
}

bool CResultSetMetaData::hasSetTranslation(int columnIndex) const
{
    assertex(columns.isItem(columnIndex));
    CResultSetColumnInfo & column = columns.item(columnIndex);
    return (column.setTransforms.ordinality() != 0);
}

static bool findSize(int size, IntArray &sizes)
{
    ForEachItemIn(idx, sizes)
    {
        if (sizes.item(idx)==size)
            return true;
    }
    return false;
}


unsigned CResultSetMetaData::queryColumnIndex(unsigned firstField, const char * fieldName) const
{
    //  DonKeep track of record depth, so we don't select fields from nested records..
    unsigned recordDepth = 0;       
    unsigned max = columns.ordinality();
    for (unsigned idx =firstField; idx < max; idx++)
    {
        CResultSetColumnInfo & column = columns.item(idx);
        unsigned flag = column.flag;
        const char * name = meta->queryName(idx);
        if ((recordDepth == 0) && (name && stricmp(name, fieldName) == 0))
            return idx;
        switch (flag)
        {
        case FVFFbeginrecord:
            recordDepth++;
            break;
        case FVFFendrecord:
            if (recordDepth == 0)
                return NotFound;
            recordDepth--;
            break;
        }
    }
    return NotFound;
}

ITypeInfo * containsSingleSimpleFieldBlankXPath(IResultSetMetaData * meta)
{
    if (meta->getColumnCount() != 1)
        return NULL;

    CResultSetMetaData * castMeta = static_cast<CResultSetMetaData *>(meta);
    const char * xpath = castMeta->queryXPath(0);
    if (xpath && (*xpath == 0))
    {
        return castMeta->queryType(0);
    }
    return NULL;
}

void fvSplitXPath(const char *xpath, StringBuffer &s, const char *&name, const char **childname=NULL)
{
    if (!xpath)
        return;
    const char * slash = strchr(xpath, '/');
    if (!slash)
    {
        name = xpath;
        if (childname)
            *childname = NULL;
    }
    else
    {
        if (!childname || strchr(slash+1, '/')) //output ignores xpaths that are too deep
            return;
        name = s.clear().append(slash-xpath, xpath).str();
        *childname = slash+1;
    }
}

void CResultSetMetaData::getXmlSchema(ISchemaBuilder & builder, bool useXPath) const
{
    StringBuffer xname;
    ForEachItemIn(idx, columns)
    {
        CResultSetColumnInfo & column = columns.item(idx);
        unsigned flag = column.flag;
        const char * name = meta->queryName(idx);
        const char * childname = NULL;

        switch (flag)
        {
        case FVFFbeginif:
            builder.beginIfBlock();
            break;
        case FVFFendif:
            builder.endIfBlock();
            break;
        case FVFFbeginrecord:
            if (useXPath)
                fvSplitXPath(meta->queryXPath(idx), xname, name);
            builder.beginRecord(name, meta->mixedContent(idx), NULL);
            break;
        case FVFFendrecord:
            if (useXPath)
                fvSplitXPath(meta->queryXPath(idx), xname, name);
            builder.endRecord(name);
            break;
        case FVFFdataset:
            {
                childname = "Row";
                if (useXPath)
                    fvSplitXPath(meta->queryXPath(idx), xname, name, &childname);
                ITypeInfo * singleFieldType = (useXPath && name && *name && childname && *childname) ? containsSingleSimpleFieldBlankXPath(column.childMeta.get()) : NULL;
                if (!singleFieldType || !builder.addSingleFieldDataset(name, childname, *singleFieldType))
                {
                    const CResultSetMetaData *childMeta = static_cast<const CResultSetMetaData *>(column.childMeta.get());
                    if (builder.beginDataset(name, childname, childMeta->meta->mixedContent(), NULL))
                    {
                        childMeta->getXmlSchema(builder, useXPath);
                    }
                    builder.endDataset(name, childname);
                }
                break;
            }
        default:
            {
                ITypeInfo & type = *column.type;
                if (type.getTypeCode() == type_set)
                {
                    childname = "Item";
                    if (useXPath)
                        fvSplitXPath(meta->queryXPath(idx), xname, name, &childname);
                    builder.addSetField(name, childname, type);
                }
                else
                {
                    if (useXPath)
                        fvSplitXPath(meta->queryXPath(idx), xname, name);
                    builder.addField(name, type);
                }
                break;
            }
        }
    }
}

IStringVal & CResultSetMetaData::getXmlSchema(IStringVal & str, bool addHeader) const
{
    XmlSchemaBuilder builder(addHeader);
    getXmlSchema(builder, alwaysUseXPath);
    builder.getXml(str);
    return str;
}

IStringVal & CResultSetMetaData::getXmlXPathSchema(IStringVal & str, bool addHeader) const
{
    XmlSchemaBuilder builder(addHeader);
    getXmlSchema(builder, true);
    builder.getXml(str);
    return str;
}

//---------------------------------------------------------------------------

IResultSetCursor * CResultSetBase::createCursor()
{
    return doCreateCursor();
}

IResultSetCursor * CResultSetBase::createCursor(IDataVal & savedCursor)
{
    MemoryBuffer buffer;
    buffer.append(savedCursor.length(), savedCursor.data());

    byte version;
    unsigned column;
    bool desc;
    buffer.read(version);
    buffer.read(column);
    if (column == (unsigned)-1)
        return new CResultSetCursor(getMeta(), this, buffer);
    buffer.read(desc);
    return new CResultSetSortedCursor(getMeta(), this, column, desc, buffer);
}


IFilteredResultSet * CResultSetBase::createFiltered()
{
    return new CFilteredResultSetBuilder(this);
}

IResultSetCursor * CResultSetBase::createSortedCursor(unsigned column, bool descend)
{
    if (getNumRows() > MAX_SORT_ELEMENTS)
        return NULL;

    return new CResultSetSortedCursor(getMeta(), this, column, descend);
}

CResultSetCursor * CResultSetBase::doCreateCursor()
{
    return new CResultSetCursor(getMeta(), this);
}


//---------------------------------------------------------------------------

CResultSet::CResultSet(IFvDataSource * _dataSource, bool _useXPath) : meta(_dataSource->queryMetaData(), _useXPath) 
{ 
    dataSource.set(_dataSource); 
    if (dataSource->isIndex())
        calcMappedFields();
}

IExtendedNewResultSet * CResultSet::cloneForFilter()
{ 
    Owned<IFvDataSource> clonedDataSource = dataSource->cloneForFilter(); 
    if (clonedDataSource)
        return new CResultSet(clonedDataSource, meta.alwaysUseXPath);
    return NULL;
}

void CResultSet::calcMappedFields()
{
    //Work out which fields within an index record are mapped.  It should be any numeric fields,
    //but not those within ifblocks or nested child records.... and not the fileposition
    unsigned max = getMetaData().getColumnCount();
    unsigned nesting = 0;
    for(unsigned i = 0; i < max; i++)
    {
        unsigned flag = meta.queryFlags(i);
        bool mapped = false;
        switch (flag)
        {
        case FVFFbeginif:
        case FVFFbeginrecord:
            nesting++;
            break;
        case FVFFendif:
        case FVFFendrecord:
            nesting--;
            break;
        default:
            if ((nesting == 0) && (i != max -1))
            {
                ITypeInfo * type = meta.queryType(i);
                switch (type->getTypeCode())
                {
                case type_int:
                case type_swapint:
                    mapped = true;
                    break;
                }
            }
            break;
        }
        mappedFields.append(mapped);
    }
}

int CResultSet::findColumn(const char * columnName) const
{
    SCMStringBuffer s;
    for(int i = 0; i < getMetaData().getColumnCount(); i++)
    {
        s.clear();
        if(!stricmp(columnName, getMetaData().getColumnLabel(s, i).str()))
            return i;
    }
    return -1;
}


const IResultSetMetaData & CResultSet::getMetaData() const
{
    return meta;
}


__int64 CResultSet::getNumRows() const
{
    return dataSource->numRows();
}


void CResultSet::setColumnMapping(IDistributedFile * df)
{
    StringBuffer mappingText;
    df->getColumnMapping(mappingText);
    if (mappingText.length())
    {
        FieldTransformInfoArray mappings;
        parseFileColumnMapping(mappings, mappingText.str(), meta);
        ForEachItemIn(i, mappings)
        {
            FieldTransformInfo & cur = mappings.item(i);
            CResultSetColumnInfo & column = meta.columns.item(cur.column);
            appendArray(column.getTransforms, cur.getTransforms);
            appendArray(column.setTransforms, cur.setTransforms);
            column.naturalName.setown(cur.naturalName.detach());
        }
    }
}

bool CResultSet::supportsRandomSeek() const
{
    return meta.meta->supportsRandomSeek();
}

bool CResultSet::fetch(MemoryBuffer & out, __int64 offset)
{
    CriticalBlock procedure(cs);
    return dataSource->fetchRow(out, offset);
}

bool CResultSet::fetchRaw(MemoryBuffer & out, __int64 offset)
{
    CriticalBlock procedure(cs);
    return dataSource->fetchRawRow(out, offset);
}

bool CResultSet::getRow(MemoryBuffer & out, __int64 row)
{
    CriticalBlock procedure(cs);
    return dataSource->getRow(out, row);
}

bool CResultSet::getRawRow(MemoryBuffer & out, __int64 row)
{
    CriticalBlock procedure(cs);
    return dataSource->getRawRow(out, row);
}

bool CResultSet::isMappedIndexField(unsigned columnIndex)
{
    return mappedFields.isItem(columnIndex) && mappedFields.item(columnIndex);
}


void CResultSet::serialize(MemoryBuffer & out)
{
    out.append((byte)FILEVIEW_VERSION); // current version
}

//---------------------------------------------------------------------------

CResultSetCursor::CResultSetCursor(const CResultSetMetaData & _meta, IExtendedNewResultSet * _resultSet) : meta(_meta)
{ 
    init(_resultSet); 
    absolute(BEFORE_FIRST_ROW);
}

CResultSetCursor::CResultSetCursor(const CResultSetMetaData & _meta, IExtendedNewResultSet * _resultSet, MemoryBuffer & buffer) : meta(_meta)
{ 
    init(_resultSet); 

    buffer.read(curRow);
    absolute(curRow);
}

CResultSetCursor::~CResultSetCursor()
{
    resultSet->onClose();
    delete [] offsets;
}


void CResultSetCursor::init(IExtendedNewResultSet * _resultSet)
{
    resultSet.set(_resultSet); 
    offsets = new unsigned[meta.getColumnCount()+1];
    if (meta.isFixedSize())
        meta.calcFieldOffsets(NULL, offsets);
    resultSet->onOpen();
}

bool CResultSetCursor::absolute(__int64 row)
{
    curRow = row;
    curRowData.clear();
    if ((row >= 0) && resultSet->getRow(curRowData, curRow))
    {
        if (!meta.isFixedSize())
            meta.calcFieldOffsets((const byte *)curRowData.toByteArray(), offsets);
        return true;
    }
    return false;
}


void CResultSetCursor::afterLast()
{
    absolute(getNumRows()+1);
    curRowData.clear();
}


void CResultSetCursor::beforeFirst()
{
    absolute(BEFORE_FIRST_ROW);
    curRowData.clear();
}


bool CResultSetCursor::fetch(__int64 offset)
{
    curRow = AFTER_LAST_ROW;
    curRowData.clear();
    if (resultSet->fetch(curRowData, offset))
    {
        if (!meta.isFixedSize())
            meta.calcFieldOffsets((const byte *)curRowData.toByteArray(), offsets);
        return true;
    }
    return false;
}


bool CResultSetCursor::first()
{
    return absolute(0);
}


static unsigned getLength(ITypeInfo & type, const byte * & cursor)
{
    unsigned len = type.getStringLen();
    if (len != UNKNOWN_LENGTH)
        return len;
    len = *(unsigned *)cursor;
    cursor += sizeof(unsigned);
    return len;
}

bool CResultSetCursor::getBoolean(int columnIndex)
{
    if (!isValid()) return false;

    const byte * cur = getColumn(columnIndex);

    ITypeInfo & type = *meta.columns.item(columnIndex).type;
    unsigned size = type.getSize();
    unsigned len = UNKNOWN_LENGTH; // error value
    switch (type.getTypeCode())
    {
    case type_void:
    case type_set:
    case type_table:
    case type_groupedtable:
        return false;
    case type_boolean:
        return *((byte *)cur) != 0;
    case type_int:
    case type_swapint:
        switch (size)
        {
        case 1:
            return *((byte *)cur) != 0;
        case 2:
            return *((short *)cur) != 0;
        case 4:
            return *((int *)cur) != 0;
        case 8:
            return *((__int64 *)cur) != 0;
        }
        break;
    case type_packedint:
        if (type.isSigned())
            return rtlGetPackedSigned(cur) != 0;
        else
            return rtlGetPackedUnsigned(cur) != 0;
    case type_decimal:
        if (type.isSigned())
            return Dec2Bool(size, cur);
        return UDec2Bool(size, cur);
    case type_real:
        if (size == 4)
            return *((float *)cur) != 0;
        return *((double *)cur) != 0;
    case type_string:
        len = getLength(type, cur);
        return rtlStrToBool(len, (const char *)cur);
    case type_unicode:
        len = getLength(type, cur);
        return rtlUnicodeToBool(len, (UChar const *)cur);
    case type_varstring:
        return rtlVStrToBool((const char *)cur);
    case type_varunicode:
        return rtlUnicodeToBool(rtlUnicodeStrlen((UChar const *)cur), (UChar const *)cur);
    case type_utf8:
        len = getLength(type, cur);
        return rtlUtf8ToBool(len, (const char *)cur);
    case type_qstring:
        len = getLength(type, cur);
        return rtlQStrToBool(len, (const char *)cur);
    case type_data:
        len = getLength(type, cur);
        return rtlDataToBool(len, cur);
    }
    UNIMPLEMENTED;
    return true;
}


IDataVal & CResultSetCursor::getBytes(IDataVal &d, int columnIndex)
{
    if (isValid())
        d.setLen(getColumn(columnIndex), offsets[columnIndex+1]-offsets[columnIndex]);
    else
        d.setLen(NULL, 0);
    return d;
}


IResultSetCursor * CResultSetCursor::getChildren(int columnIndex) const
{
    if (!isValid()) return NULL;
    ITypeInfo & type = *meta.columns.item(columnIndex).type;
    const byte * cur = getColumn(columnIndex);
    switch (type.getTypeCode())
    {
    case type_set:
        cur += sizeof(bool);
        break;
    case type_table:
    case type_groupedtable:
        break;
    default:
        return NULL;
    }

    unsigned len = *(unsigned *)cur;
    const byte * data = cur + sizeof(unsigned);
    Owned<IFvDataSource> childData = meta.meta->createChildDataSource(columnIndex, len, data);
    Owned<CResultSet> nestedResult = new CResultSet(childData, meta.alwaysUseXPath);
    return nestedResult->createCursor();
}

xdouble CResultSetCursor::getDouble(int columnIndex)
{
    if (!isValid()) return 0.0;
    const byte * cur = getColumn(columnIndex);

    ITypeInfo & type = *meta.columns.item(columnIndex).type;
    unsigned size = type.getSize();
    unsigned len = UNKNOWN_LENGTH; // error value
    switch (type.getTypeCode())
    {
    case type_void:
    case type_set:
    case type_table:
    case type_groupedtable:
        return 0;
    case type_boolean:
        return *((byte *)cur) != 0;
    case type_int:
        if (type.isSigned())
            return (xdouble)getIntFromInt(type, cur, isMappedIndexField(columnIndex));
        return (xdouble)(__int64)getIntFromInt(type, cur, isMappedIndexField(columnIndex));
    case type_swapint:
        if (type.isSigned())
            return (xdouble)getIntFromSwapInt(type, cur, isMappedIndexField(columnIndex));
        return (xdouble)(__int64)getIntFromSwapInt(type, cur, isMappedIndexField(columnIndex));
    case type_packedint:
        if (type.isSigned())
            return (xdouble)rtlGetPackedSigned(cur);
        else
            return (xdouble)rtlGetPackedUnsigned(cur);
    case type_decimal:
        {
            DecLock();
            if (type.isSigned())
                DecPushDecimal(cur, type.getSize(), type.getPrecision());
            else
                DecPushUDecimal(cur, type.getSize(), type.getPrecision());
            xdouble ret = DecPopReal();
            DecUnlock();
            return ret;
        }
    case type_real:
        if (size == 4)
            return *((float *)cur);
        return *((double *)cur);
    case type_data:
    case type_string:
        len = getLength(type, cur);
        return rtlStrToReal(len, (const char *)cur);
    case type_qstring:
        {
            len = getLength(type, cur);
            unsigned newSize;
            char * newStr;
            rtlQStrToStrX(newSize, newStr, len, (const char *)cur);
            double ret = rtlStrToReal(newSize, newStr);
            rtlFree(newStr);
            return ret;
        }
    case type_unicode:
        len = getLength(type, cur);
        return rtlUnicodeToReal(len, (UChar const *)cur);
    case type_utf8:
        len = getLength(type, cur);
        return rtlUtf8ToReal(len, (const char *)cur);
    case type_varstring:
        return rtlVStrToReal((const char *)cur);
    case type_varunicode:
        return rtlUnicodeToReal(rtlUnicodeStrlen((UChar const *)cur), (UChar const *)cur);
    }
    UNIMPLEMENTED;
    return 0.0;
}


int CResultSetCursor::getFetchSize() const
{
    return DEFAULT_FETCH_SIZE;
}


__int64 CResultSetCursor::getInt(int columnIndex)
{
    if (!isValid()) return 0;
    const byte * cur = getColumn(columnIndex);

    ITypeInfo & type = *meta.columns.item(columnIndex).type;
    unsigned size = type.getSize();
    unsigned len = UNKNOWN_LENGTH;
    switch (type.getTypeCode())
    {
    case type_void:
    case type_set:
    case type_table:
    case type_groupedtable:
        return 0;
    case type_boolean:
        return *((byte *)cur) != 0;
    case type_int:
        return getIntFromInt(type, cur, isMappedIndexField(columnIndex));
    case type_swapint:
        return getIntFromSwapInt(type, cur, isMappedIndexField(columnIndex));
    case type_packedint:
        if (type.isSigned())
            return rtlGetPackedSigned(cur);
        else
            return rtlGetPackedUnsigned(cur);
    case type_decimal:
        {
            DecLock();
            if (type.isSigned())
                DecPushDecimal(cur, type.getSize(), type.getPrecision());
            else
                DecPushUDecimal(cur, type.getSize(), type.getPrecision());
            __int64 ret = DecPopInt64();
            DecUnlock();
            return ret;
        }
    case type_real:
        if (size == 4)
            return (__int64) *((float *)cur);
        return (__int64) *((double *)cur);
    case type_data:
    case type_string:
        len = getLength(type, cur);
        return rtlStrToInt8(len, (const char *)cur);
    case type_qstring:
        {
            unsigned newSize;
            char * newStr;
            len = getLength(type, cur);
            rtlQStrToStrX(newSize, newStr, len, (const char *)cur);
            __int64 ret = rtlStrToInt8(newSize, newStr);
            rtlFree(newStr);
            return ret;
        }
    case type_unicode:
        len = getLength(type, cur);
        return rtlUnicodeToInt8(len, (UChar const *)cur);
    case type_utf8:
        len = getLength(type, cur);
        return rtlUtf8ToInt(len, (const char *)cur);
    case type_varstring:
        return rtlVStrToInt8((const char *)cur);
    case type_varunicode:
        return rtlUnicodeToInt8(rtlUnicodeStrlen((UChar const *)cur), (UChar const *)cur);
    }
    UNIMPLEMENTED;
    return 0;
}


bool CResultSetCursor::getIsAll(int columnIndex) const
{
    if (!isValid()) return false;
    ITypeInfo & type = *meta.columns.item(columnIndex).type;
    if (type.getTypeCode() != type_set)
        return false;

    const byte * cur = getColumn(columnIndex);
    return *(bool *)cur;
}


int CResultSetCursor::getType()
{
    if (getNumRows() != UNKNOWN_NUM_ROWS)
        return TYPE_SCROLL_INSENSITIVE;
    return TYPE_FORWARD_ONLY;
}


const IResultSetMetaData & CResultSetCursor::getMetaData() const
{
    return meta;
}


__int64 CResultSetCursor::getCurRow() const
{
    return curRow;
}

__int64 CResultSetCursor::getNumRows() const
{
    return resultSet->getNumRows();
}


IDataVal & CResultSetCursor::getRaw(IDataVal &d, int columnIndex)
{
    //MORE: This should work on the raw data!
    return getBytes(d, columnIndex);
}

IDataVal & CResultSetCursor::getRawRow(IDataVal &d)
{
    MemoryBuffer temp;
    if (resultSet->getRawRow(temp, curRow))
        d.setLen(temp.toByteArray(), temp.length());
    return d;
}

__int64 CResultSetCursor::translateRow(__int64 row) const
{
    return row;
}

IStringVal & CResultSetCursor::getString(IStringVal & ret, int columnIndex)
{
    if (!isValid())
    {
        ret.set("");
        return ret;
    }
    const byte * cur = getColumn(columnIndex);
    unsigned resultLen;
    char * resultStr = NULL;

    ITypeInfo & type = *meta.columns.item(columnIndex).type;
    unsigned size = type.getSize();
    unsigned len;
    switch (type.getTypeCode())
    {
    case type_void:
    case type_set:
    case type_table:
    case type_groupedtable:
        ret.set("");
        break;
    case type_boolean:
        if (*((byte *)cur) != 0)
            ret.set("1");
        else
            ret.set("");
        break;
    case type_int:
        {
            __int64 value = getIntFromInt(type, cur, isMappedIndexField(columnIndex));
            if (type.isSigned())
                rtlInt8ToStrX(resultLen, resultStr, value);
            else
                rtlUInt8ToStrX(resultLen, resultStr, (unsigned __int64) value);
            ret.setLen(resultStr, resultLen);
            break;
        }
    case type_swapint:
        {
            __int64 value = getIntFromSwapInt(type, cur, isMappedIndexField(columnIndex));
            if (type.isSigned())
                rtlInt8ToStrX(resultLen, resultStr, value);
            else
                rtlUInt8ToStrX(resultLen, resultStr, (unsigned __int64) value);
            ret.setLen(resultStr, resultLen);
            break;
        }
    case type_packedint:
        {
            if (type.isSigned())
                rtlInt8ToStrX(resultLen, resultStr, rtlGetPackedSigned(cur));
            else
                rtlUInt8ToStrX(resultLen, resultStr, rtlGetPackedUnsigned(cur));
            ret.setLen(resultStr, resultLen);
            break;
        }
    case type_decimal:
        {
            DecLock();
            if (type.isSigned())
                DecPushDecimal(cur, type.getSize(), type.getPrecision());
            else
                DecPushUDecimal(cur, type.getSize(), type.getPrecision());
            DecPopStringX(resultLen, resultStr);
            DecUnlock();
            ret.setLen(resultStr, resultLen);
            return ret;
        }
    case type_real:
        if (size == 4)
            rtlRealToStrX(resultLen, resultStr, *(float *)cur);
        else
            rtlRealToStrX(resultLen, resultStr, *(double *)cur);
        ret.setLen(resultStr, resultLen);
        break;
    case type_qstring:
        len = getLength(type, cur);
        rtlQStrToStrX(resultLen, resultStr, len, (const char *)cur);
        ret.setLen(resultStr, resultLen);
        break;
    case type_data:
    case type_string:
        len = getLength(type, cur);
        ret.setLen((const char *)cur, len);
        break;
    case type_unicode:
        len = getLength(type, cur);
        rtlUnicodeToStrX(resultLen, resultStr, len, (UChar const *)cur);
        ret.setLen(resultStr, resultLen);
        break;
    case type_utf8:
        len = getLength(type, cur);
        rtlUtf8ToStrX(resultLen, resultStr, len, (const char *)cur);
        ret.setLen(resultStr, resultLen);
        break;
    case type_varstring:
        ret.set((const char *)cur);
        break;
    case type_varunicode:
        rtlUnicodeToStrX(resultLen, resultStr, rtlUnicodeStrlen((UChar const *)cur), (UChar const *)cur);
        ret.setLen(resultStr, resultLen);
        break;
    default:
        UNIMPLEMENTED;
    }
    free(resultStr);
    return ret;
}


IStringVal & CResultSetCursor::getDisplayText(IStringVal &ret, int columnIndex)
{
    if (!isValid())
    {
        ret.set("");
        return ret;
    }

    CResultSetColumnInfo & column = meta.columns.item(columnIndex);
    unsigned flags = column.flag;
    switch (flags)
    {
    case FVFFbeginif:
    case FVFFendif:
    case FVFFbeginrecord:
    case FVFFendrecord:
    case FVFFdataset:
    case FVFFset:
        ret.set("");
        return ret;
    }

    const byte * cur = getColumn(columnIndex);
    unsigned resultLen;
    char * resultStr = NULL;

    ITypeInfo & type = *column.type;
    unsigned size = type.getSize();
    unsigned len = UNKNOWN_LENGTH;
    switch (type.getTypeCode())
    {
    case type_boolean:
        if (*((byte *)cur) != 0)
            ret.set("true");
        else
            ret.set("false");
        break;
    case type_int:
        {
            __int64 value = getIntFromInt(type, cur, isMappedIndexField(columnIndex));
            if (type.isSigned())
                rtlInt8ToStrX(resultLen, resultStr, value);
            else
                rtlUInt8ToStrX(resultLen, resultStr, (unsigned __int64) value);
            ret.setLen(resultStr, resultLen);
            break;
        }
    case type_swapint:
        {
            __int64 value = getIntFromSwapInt(type, cur, isMappedIndexField(columnIndex));
            if (type.isSigned())
                rtlInt8ToStrX(resultLen, resultStr, value);
            else
                rtlUInt8ToStrX(resultLen, resultStr, (unsigned __int64) value);
            ret.setLen(resultStr, resultLen);
            break;
        }
    case type_packedint:
        {
            if (type.isSigned())
                rtlInt8ToStrX(resultLen, resultStr, rtlGetPackedSigned(cur));
            else
                rtlUInt8ToStrX(resultLen, resultStr, rtlGetPackedUnsigned(cur));
            ret.setLen(resultStr, resultLen);
            break;
        }
    case type_decimal:
        {
            DecLock();
            if (type.isSigned())
                DecPushDecimal(cur, type.getSize(), type.getPrecision());
            else
                DecPushUDecimal(cur, type.getSize(), type.getPrecision());
            DecPopStringX(resultLen, resultStr);
            DecUnlock();
            ret.setLen(resultStr, resultLen);
            return ret;
        }
    case type_real:
        if (size == 4)
            rtlRealToStrX(resultLen, resultStr, *(float *)cur);
        else
            rtlRealToStrX(resultLen, resultStr, *(double *)cur);
        ret.setLen(resultStr, resultLen);
        break;
    case type_qstring:
        len = getLength(type, cur);
        rtlQStrToStrX(resultLen, resultStr, len, (const char *)cur);
        ret.setLen(resultStr, resultLen);
        break;
    case type_data:
        {
            len = getLength(type, cur);
            StringBuffer temp;
            while (len--)
                temp.appendhex(*cur++, true);
            ret.setLen(temp.str(), temp.length());
            break;
        }
    case type_string:
        {
            len = getLength(type, cur);
            rtlStrToUtf8X(resultLen, resultStr, len , (const char *)cur);
            ret.setLen(resultStr, rtlUtf8Size(resultLen, resultStr));
            break;
        }
    case type_unicode:
        len = getLength(type, cur);
        rtlUnicodeToUtf8X(resultLen, resultStr, len, (UChar const *)cur);
        ret.setLen(resultStr, rtlUtf8Size(resultLen, resultStr));
        break;
    case type_utf8:
        len = getLength(type, cur);
        ret.setLen((const char *)cur, rtlUtf8Size(len, cur));
        break;
    case type_varstring:
        ret.set((const char *)cur);
        break;
    case type_varunicode:
        rtlUnicodeToCodepageX(resultLen, resultStr, rtlUnicodeStrlen((UChar const *)cur), (UChar const *)cur, "UTF-8");
        ret.setLen(resultStr, resultLen);
        break;
    default:
        UNIMPLEMENTED;
    }
    rtlFree(resultStr);
    return ret;
}

void CResultSetCursor::writeXmlText(IXmlWriter &writer, int columnIndex, const char *tag)
{
    if (!isValid())
        return;

    const char * name = (tag) ? tag : meta.meta->queryXmlTag(columnIndex);
    CResultSetColumnInfo & column = meta.columns.item(columnIndex);
    unsigned flags = column.flag;
    switch (flags)
    {
    case FVFFbeginif:
    case FVFFendif:
        return;
    case FVFFbeginrecord:
        {
            if (name && *name)
            {
                writer.outputBeginNested(name, false);
                const IntArray &attributes = meta.meta->queryAttrList(columnIndex);
                ForEachItemIn(ac, attributes)
                    writeXmlText(writer, attributes.item(ac), NULL);
            }
        }
        return;
    case FVFFendrecord:
        if (name && *name)
            writer.outputEndNested(name);
        return;
    }

    const byte * cur = getColumn(columnIndex);
    unsigned resultLen;
    char * resultStr = NULL;

    ITypeInfo & type = *column.type;
    unsigned size = type.getSize();
    unsigned len = UNKNOWN_LENGTH;
    switch (type.getTypeCode())
    {
    case type_boolean:
        writer.outputBool(*((byte *)cur) != 0, name);
        break;
    case type_int:
        {
            __int64 value = getIntFromInt(type, cur, isMappedIndexField(columnIndex));
            if (type.isSigned())
                writer.outputInt((__int64) value, name);
            else
                writer.outputUInt((unsigned __int64) value, name);
            break;
        }
    case type_swapint:
        {
            __int64 value = getIntFromSwapInt(type, cur, isMappedIndexField(columnIndex));
            if (type.isSigned())
                writer.outputInt((__int64) value, name);
            else
                writer.outputUInt((unsigned __int64) value, name);
            break;
        }
    case type_packedint:
        {
            if (type.isSigned())
                writer.outputInt(rtlGetPackedSigned(cur), name);
            else
                writer.outputUInt(rtlGetPackedUnsigned(cur), name);
            break;
        }
    case type_decimal:
        if (type.isSigned())
            writer.outputDecimal(cur, size, type.getPrecision(), name);
        else
            writer.outputUDecimal(cur, size, type.getPrecision(), name);
        break;
    case type_real:
        if (size == 4)
            writer.outputReal(*(float *)cur, name);
        else
            writer.outputReal(*(double *)cur, name);
        break;
    case type_qstring:
        len = getLength(type, cur);
        rtlQStrToStrX(resultLen, resultStr, len, (const char *)cur);
        writer.outputString(resultLen, resultStr, name);
        break;
    case type_data:
        len = getLength(type, cur);
        writer.outputData(len, cur, name);
        break;
    case type_string:
        len = getLength(type, cur);
        if (meta.isEBCDIC(columnIndex))
        {
            rtlEStrToStrX(resultLen, resultStr, len, (const char *)cur);
            writer.outputString(resultLen, resultStr, name);
        }
        else
            writer.outputString(len, (const char *)cur, name);
        break;
    case type_unicode:
        len = getLength(type, cur);
        writer.outputUnicode(len, (UChar const *)cur, name);
        break;
    case type_varstring:
        if (meta.isEBCDIC(columnIndex))
        {
            rtlStrToEStrX(resultLen, resultStr, strlen((const char *)cur), (const char *)cur);
            writer.outputString(resultLen, resultStr, name);
        }
        else
            writer.outputString(strlen((const char *)cur), (const char *)cur, name);
        break;
    case type_varunicode:
        writer.outputUnicode(rtlUnicodeStrlen((UChar const *)cur), (UChar const *)cur, name);
        break;
    case type_utf8:
        len = getLength(type, cur);
        writer.outputUtf8(len, (const char *)cur, name);
        break;
    case type_table:
    case type_groupedtable:
        {
            writer.outputBeginNested(name, false);
            Owned<IResultSetCursor> childCursor = getChildren(columnIndex);
            childCursor->beginWriteXmlRows(writer);
            ForEach(*childCursor)
                childCursor->writeXmlRow(writer);
            childCursor->endWriteXmlRows(writer);
            writer.outputEndNested(name);
        }
        break;
    case type_set:
        {
            writer.outputBeginNested(name, false);
            if (getIsAll(columnIndex))
                writer.outputSetAll();
            else
            {
                Owned<IResultSetCursor> childCursor = getChildren(columnIndex);
                childCursor->beginWriteXmlRows(writer);
                ForEach(*childCursor)
                    childCursor->writeXmlItem(writer);
                childCursor->endWriteXmlRows(writer);
            }
            writer.outputEndNested(name);
        }
        break;
    default:
        UNIMPLEMENTED;
    }
    rtlFree(resultStr);
}

IStringVal & CResultSetCursor::getXml(IStringVal &ret, int columnIndex)
{
    Owned<CommonXmlWriter> writer = CreateCommonXmlWriter(XWFexpandempty);
    writeXmlText(*writer, columnIndex);
    ret.set(writer->str());
    return ret;
}

IStringVal & CResultSetCursor::getXmlItem(IStringVal & ret)
{
    Owned<CommonXmlWriter> writer = CreateCommonXmlWriter(XWFexpandempty);
    writeXmlText(*writer, 0, meta.meta->queryXmlTag());
    ret.set(writer->str());
    return ret;
}

void CResultSetCursor::writeXmlItem(IXmlWriter &writer)
{
    writeXmlText(writer, 0, meta.meta->queryXmlTag());
}

IStringVal & CResultSetCursor::getXmlRow(IStringVal &ret)
{
    Owned<CommonXmlWriter> writer = CreateCommonXmlWriter(XWFexpandempty);
    writeXmlRow(*writer);
    ret.set(writer->str());
    return ret;
}

void CResultSetCursor::beginWriteXmlRows(IXmlWriter & writer)
{
    const char *rowtag = meta.meta->queryXmlTag();
    if (rowtag && *rowtag)
        writer.outputBeginArray(rowtag);
}

void CResultSetCursor::endWriteXmlRows(IXmlWriter & writer)
{
    const char *rowtag = meta.meta->queryXmlTag();
    if (rowtag && *rowtag)
        writer.outputEndArray(rowtag);
}

void CResultSetCursor::writeXmlRow(IXmlWriter &writer)
{
    StringBuffer temp;
    const char *rowtag = meta.meta->queryXmlTag();
    if (rowtag && *rowtag)
    {
        writer.outputBeginNested(rowtag, false);
        const IntArray &attributes = meta.meta->queryAttrList();
        ForEachItemIn(ac, attributes)
            writeXmlText(writer, attributes.item(ac), NULL);
    }
    unsigned numColumns = meta.getColumnCount();
    unsigned ignoreNesting = 0;
    for (unsigned col = 0; col < numColumns; col++)
    {
        unsigned flags = meta.columns.item(col).flag;
        const char *tag = meta.meta->queryXmlTag(col);
        if (tag && *tag=='@')
            continue;
        switch (flags)
        {
        case FVFFbeginif:
            if (ignoreNesting || !getBoolean(col))
                ignoreNesting++;
            break;
        case FVFFendif:
            if (ignoreNesting)
                ignoreNesting--;
            break;
        case FVFFbeginrecord:
            if (ignoreNesting)
                ignoreNesting++;
            else
                writeXmlText(writer, col);
            break;
        case FVFFendrecord:
            if (ignoreNesting)
                ignoreNesting--;
            else
                writeXmlText(writer, col);
            break;
        case FVFFnone:
        case FVFFvirtual:
        case FVFFdataset:
        case FVFFset:
            if (ignoreNesting == 0)
                writeXmlText(writer, col);
            break;
        }
    }
    assertex(ignoreNesting == 0);
    writer.outputEndNested(rowtag);
}

bool CResultSetCursor::isAfterLast() const
{
    return (curRowData.length() == 0) && (getCurRow() != BEFORE_FIRST_ROW);
}


bool CResultSetCursor::isBeforeFirst() const
{
    return getCurRow() == BEFORE_FIRST_ROW;
}


bool CResultSetCursor::isFirst() const
{
    return (getCurRow() == 0);
}


bool CResultSetCursor::isLast() const
{
    if (curRowData.length() == 0)
        return false;

    __int64 numRows = getNumRows();
    if (numRows != UNKNOWN_NUM_ROWS)
        return getCurRow() == numRows+1;

    MemoryBuffer temp;
    return !resultSet->getRow(temp, translateRow(getCurRow()+1));
}


bool CResultSetCursor::isValid() const
{
    return (curRowData.length() != 0);
}


bool CResultSetCursor::isNull(int columnIndex) const
{
    //MORE: There needs to be some projected extra field to
    return false;
}

bool CResultSetCursor::last()
{
    return absolute(getNumRows()-1);
}


bool CResultSetCursor::next()
{
    return absolute(getCurRow()+1);
}


bool CResultSetCursor::previous()
{
    return absolute(getCurRow()-1);
}


bool CResultSetCursor::relative(__int64 rows)
{
    if (getCurRow() + rows < 0)
    {
        beforeFirst();
        return false;
    }
    else
        return absolute (getCurRow() + rows);
}


void CResultSetCursor::serialize(IDataVal & d)
{
    MemoryBuffer buffer;

    resultSet->serialize(buffer);
    serializeType(buffer);
    serialize(buffer);

    d.setLen(buffer.toByteArray(), buffer.length());
}

void CResultSetCursor::serializeType(MemoryBuffer & buffer)
{
    buffer.append((unsigned)-1);
}

void CResultSetCursor::serialize(MemoryBuffer & buffer)
{
    buffer.append(getCurRow());
}

void CResultSetCursor::setFetchSize(int rows)
{
}


bool CResultSetCursor::supportsRandomSeek() const
{
    return meta.meta->supportsRandomSeek();
}


//---------------------------------------------------------------------------

bool IndirectResultSetCursor::absolute(__int64 row) 
{ 
    return queryBase()->absolute(row); 
}
void IndirectResultSetCursor::afterLast() 
{ 
    queryBase()->afterLast(); 
}
void IndirectResultSetCursor::beforeFirst() 
{ 
    queryBase()->beforeFirst(); 
}
bool IndirectResultSetCursor::fetch(__int64 fileoffset) 
{
    return queryBase()->fetch(fileoffset);
}
bool IndirectResultSetCursor::first()
{
    return queryBase()->first();
}
bool IndirectResultSetCursor::getBoolean(int columnIndex)
{
    return queryBase()->getBoolean(columnIndex);
}
IDataVal & IndirectResultSetCursor::getBytes(IDataVal &d, int columnIndex)
{
    return queryBase()->getBytes(d, columnIndex);
}
IResultSetCursor * IndirectResultSetCursor::getChildren(int columnIndex) const
{
    return queryBase()->getChildren(columnIndex);
}
xdouble IndirectResultSetCursor::getDouble(int columnIndex)
{
    return queryBase()->getDouble(columnIndex);
}
int IndirectResultSetCursor::getFetchSize() const
{
    return queryBase()->getFetchSize();
}
bool IndirectResultSetCursor::getIsAll(int columnIndex) const
{
    return queryBase()->getIsAll(columnIndex);
}
__int64 IndirectResultSetCursor::getInt(int columnIndex)
{
    return queryBase()->getInt(columnIndex);
}
IDataVal & IndirectResultSetCursor::getRaw(IDataVal &d, int columnIndex)
{
    return queryBase()->getRaw(d, columnIndex);
}
IDataVal & IndirectResultSetCursor::getRawRow(IDataVal &d)
{
    return queryBase()->getRawRow(d);
}
__int64 IndirectResultSetCursor::getNumRows() const
{
    return queryBase()->getNumRows();
}
IStringVal & IndirectResultSetCursor::getString(IStringVal & ret, int columnIndex)
{
    return queryBase()->getString(ret, columnIndex);
}
bool IndirectResultSetCursor::isAfterLast() const
{
    return queryBase()->isAfterLast();
}
bool IndirectResultSetCursor::isBeforeFirst() const
{
    return queryBase()->isBeforeFirst();
}
bool IndirectResultSetCursor::isFirst() const
{
    return queryBase()->isFirst();
}
bool IndirectResultSetCursor::isLast() const
{
    return queryBase()->isLast();
}
bool IndirectResultSetCursor::isNull(int columnIndex) const
{
    return queryBase()->isNull(columnIndex);
}
bool IndirectResultSetCursor::isValid() const
{
    return queryBase()->isValid();
}
bool IndirectResultSetCursor::last()
{
    return queryBase()->last();
}
bool IndirectResultSetCursor::next()
{
    return queryBase()->next();
}
bool IndirectResultSetCursor::previous()
{
    return queryBase()->previous();
}
INewResultSet * IndirectResultSetCursor::queryResultSet()
{
    return queryBase()->queryResultSet();
}
bool IndirectResultSetCursor::relative(__int64 rows)
{
    return queryBase()->relative(rows);
}
void IndirectResultSetCursor::serialize(IDataVal & d)
{
    queryBase()->serialize(d);
}
IStringVal & IndirectResultSetCursor::getDisplayText(IStringVal &ret, int columnIndex)
{
    return queryBase()->getDisplayText(ret, columnIndex);
}
IStringVal & IndirectResultSetCursor::getXml(IStringVal & ret, int columnIndex)
{
    return queryBase()->getXml(ret, columnIndex);
}
IStringVal & IndirectResultSetCursor::getXmlRow(IStringVal &ret)
{
    return queryBase()->getXmlRow(ret);
}
IStringVal & IndirectResultSetCursor::getXmlItem(IStringVal &ret)
{
    return queryBase()->getXmlItem(ret);
}
void IndirectResultSetCursor::beginWriteXmlRows(IXmlWriter & writer)
{
    return queryBase()->beginWriteXmlRows(writer);
}
void IndirectResultSetCursor::writeXmlRow(IXmlWriter &writer)
{
    return queryBase()->writeXmlRow(writer);
}
void IndirectResultSetCursor::endWriteXmlRows(IXmlWriter & writer)
{
    return queryBase()->endWriteXmlRows(writer);
}
void IndirectResultSetCursor::writeXmlItem(IXmlWriter &writer)
{
    return queryBase()->writeXmlItem(writer);
}
void IndirectResultSetCursor::noteRelatedFileChanged()
{
    queryBase()->noteRelatedFileChanged();
}

//---------------------------------------------------------------------------

bool NotifyingResultSetCursor::absolute(__int64 row)
{
    bool ret = IndirectResultSetCursor::absolute(row);
    notifyChanged();
    return ret;
}
void NotifyingResultSetCursor::afterLast()
{
    IndirectResultSetCursor::afterLast();
    notifyChanged();
}
void NotifyingResultSetCursor::beforeFirst()
{
    IndirectResultSetCursor::beforeFirst();
    notifyChanged();
}
bool NotifyingResultSetCursor::fetch(__int64 fileoffset)
{
    bool ret = IndirectResultSetCursor::fetch(fileoffset);
    notifyChanged();
    return ret;
}
bool NotifyingResultSetCursor::first()
{
    bool ret = IndirectResultSetCursor::first();
    notifyChanged();
    return ret;
}
bool NotifyingResultSetCursor::last()
{
    bool ret = IndirectResultSetCursor::last();
    notifyChanged();
    return ret;
}
bool NotifyingResultSetCursor::next()
{
    bool ret = IndirectResultSetCursor::next();
    notifyChanged();
    return ret;
}
bool NotifyingResultSetCursor::previous()
{
    bool ret = IndirectResultSetCursor::previous();
    notifyChanged();
    return ret;
}
bool NotifyingResultSetCursor::relative(__int64 rows)
{
    bool ret = IndirectResultSetCursor::relative(rows);
    notifyChanged();
    return ret;
}

void NotifyingResultSetCursor::noteRelatedFileChanged()
{
    IndirectResultSetCursor::noteRelatedFileChanged();
    notifyChanged();
}

void NotifyingResultSetCursor::notifyChanged()
{
    ForEachItemIn(i, dependents)
        dependents.item(i).noteRelatedFileChanged();
}

//---------------------------------------------------------------------------

DelayedFilteredResultSetCursor::DelayedFilteredResultSetCursor(INewResultSet * _resultSet)
{
    filtered.setown(_resultSet->createFiltered());
}

void DelayedFilteredResultSetCursor::clearCursor()
{
    cursor.clear();
    resultSet.clear();
}

void DelayedFilteredResultSetCursor::clearFilters()
{
    clearCursor();
    filtered->clearFilters();
}


void DelayedFilteredResultSetCursor::ensureFiltered()
{
}


void DelayedFilteredResultSetCursor::noteRelatedFileChanged()
{
    //Don't create a cursor, just to tell the class that the cursor is no longer valid!
    if (cursor)
        IndirectResultSetCursor::noteRelatedFileChanged();
}

IExtendedResultSetCursor * DelayedFilteredResultSetCursor::queryBase()
{
    //NB: Not thread safe - but none of the interface is
    if (!cursor)
    {
        ensureFiltered();
        //MORE: should possibly have the ability to create a null dataset at this point.
        resultSet.setown(filtered->create());
        cursor.setown(static_cast<IExtendedResultSetCursor *>(resultSet->createCursor()));
    }

    return cursor;
}

//---------------------------------------------------------------------------

//was using function templates that didn't use their types in their arguments, but
//VC++ fails to process them correctly.
template <class KEY> int qsortCompare(const void * _left, const void * _right) { return 0; }
typedef int (*qsortFunc)(const void *, const void *);


struct StringKeyElement
{
    unsigned index;
    StringAttr value;
};

int qsortCompare_StringKeyElement(const void * _left, const void * _right)
{
    const StringKeyElement * left = (const StringKeyElement *)_left;
    const StringKeyElement * right = (const StringKeyElement *)_right;
    int i = strcmp(left->value, right->value);
    if (i != 0)
        return i;
    if (left->index < right->index) return -1;
    assertex(left->index > right->index);
    return +1;
}
qsortFunc qsortCompare(StringKeyElement *) { return qsortCompare_StringKeyElement; }


void extractKey(StringKeyElement & element, CResultSetCursor * cursor, unsigned column)
{
    StringAttrAdaptor adaptor(element.value);
    cursor->getString(adaptor, column);
}

struct IntegerKeyElement
{
    unsigned index;
    __int64 value;
};

int qsortCompare_IntegerKeyElement(const void * _left, const void * _right)
{
    const IntegerKeyElement * left = (const IntegerKeyElement *)_left;
    const IntegerKeyElement * right = (const IntegerKeyElement *)_right;
    if (left->value < right->value) return -1;
    if (left->value > right->value) return +1;
    if (left->index < right->index) return -1;
    assertex(left->index > right->index);
    return +1;
}
qsortFunc qsortCompare(IntegerKeyElement *) { return qsortCompare_IntegerKeyElement; }

void extractKey(IntegerKeyElement & element, CResultSetCursor * cursor, unsigned column)
{
    element.value = cursor->getInt(column);
}

struct UnsignedKeyElement
{
    unsigned index;
    unsigned __int64 value;
};

int qsortCompare_UnsignedKeyElement(const void * _left, const void * _right)
{
    const UnsignedKeyElement * left = (const UnsignedKeyElement *)_left;
    const UnsignedKeyElement * right = (const UnsignedKeyElement *)_right;
    if (left->value < right->value) return -1;
    if (left->value > right->value) return +1;
    if (left->index < right->index) return -1;
    assertex(left->index > right->index);
    return +1;
}
qsortFunc qsortCompare(UnsignedKeyElement *) { return qsortCompare_UnsignedKeyElement; }


void extractKey(UnsignedKeyElement & element, CResultSetCursor * cursor, unsigned column)
{
    element.value = (unsigned __int64)cursor->getInt(column);
}


struct RealKeyElement
{
    unsigned index;
    double value;
};

int qsortCompare_RealKeyElement(const void * _left, const void * _right)
{
    const RealKeyElement * left = (const RealKeyElement *)_left;
    const RealKeyElement * right = (const RealKeyElement *)_right;
    if (left->value < right->value) return -1;
    if (left->value > right->value) return +1;
    if (left->index < right->index) return -1;
    assertex(left->index > right->index);
    return +1;
}
qsortFunc qsortCompare(RealKeyElement *) { return qsortCompare_RealKeyElement; }

void extractKey(RealKeyElement & element, CResultSetCursor * cursor, unsigned column)
{
    element.value = cursor->getDouble(column);
}


template <class KEYELEMENT>
void buildCursorIndex(unsigned & numElements, unsigned * & elements, CResultSetCursor * cursor, unsigned column, bool descend, KEYELEMENT *)
{
    __int64 max64 = cursor->getNumRows();
    assertex(max64 <= MAX_SORT_ELEMENTS);

    unsigned max = (unsigned)max64;
    KEYELEMENT * keys = new KEYELEMENT[max];
    for (unsigned idx=0; idx < max; idx++)
    {
        cursor->absolute(idx);
        keys[idx].index = idx;
        extractKey(keys[idx], cursor, column);
    }

    qsort(keys, max, sizeof(KEYELEMENT), qsortCompare((KEYELEMENT *)0));
    numElements = max;
    elements = (unsigned *)malloc(max * sizeof(unsigned));

    if (descend)
    {
        for (unsigned idx=0; idx < max; idx++)
            elements[max-idx-1] = keys[idx].index;
    }
    else
    {
        for (unsigned idx=0; idx < max; idx++)
            elements[idx] = keys[idx].index;
    }
    delete [] keys;
}

CResultSetSortedCursor::CResultSetSortedCursor(const CResultSetMetaData & _meta, IExtendedNewResultSet * _resultSet, unsigned _column, bool _desc) : CResultSetCursor(_meta, _resultSet)
{
    numEntries = 0;
    elements = NULL;
    lastRow = 0;
    column = _column;
    desc = _desc;
    buildIndex();
}

CResultSetSortedCursor::CResultSetSortedCursor(const CResultSetMetaData & _meta, IExtendedNewResultSet * _resultSet, unsigned _column, bool _desc, MemoryBuffer & buffer) : CResultSetCursor(_meta, _resultSet)
{ 
    numEntries = 0;
    elements = NULL;
    lastRow = 0;
    column = _column;
    desc = _desc;
    buildIndex();

    buffer.read(curRow);
    buffer.read(lastRow);
    absolute(lastRow);
}


CResultSetSortedCursor::~CResultSetSortedCursor()
{
    free(elements);
}


void CResultSetSortedCursor::buildIndex()
{
    Owned<CResultSetCursor> cursor = new CResultSetCursor(meta, resultSet);
    switch (meta.getColumnDisplayType(column))
    {
    case TypeBoolean:
    case TypeInteger:
        buildCursorIndex<IntegerKeyElement>(numEntries, elements, cursor, column, desc, (IntegerKeyElement*)0);
        break;
    case TypeUnsignedInteger:
        buildCursorIndex<UnsignedKeyElement>(numEntries, elements, cursor, column, desc, (UnsignedKeyElement*)0);
        break;
    case TypeReal:
        buildCursorIndex<RealKeyElement>(numEntries, elements, cursor, column, desc, (RealKeyElement*)0);
        break;
    case TypeString:
    case TypeData:
    case TypeUnicode:       //MORE!
        buildCursorIndex<StringKeyElement>(numEntries, elements, cursor, column, desc, (StringKeyElement*)0);
        break;
    default:
        UNIMPLEMENTED;  // Should have been translated to one of the above by this point...
    }
}

bool CResultSetSortedCursor::absolute(__int64 row)
{
    lastRow = row;
    return CResultSetCursor::absolute(translateRow(row));
}

__int64 CResultSetSortedCursor::getCurRow() const
{
    return lastRow;
}

__int64 CResultSetSortedCursor::translateRow(__int64 row) const
{
    if ((row >= 0) && (row < numEntries))
        return elements[row];
    return row;
}

void CResultSetSortedCursor::serializeType(MemoryBuffer & buffer)
{
    buffer.append((unsigned)column);
    buffer.append(desc);
}

void CResultSetSortedCursor::serialize(MemoryBuffer & buffer)
{
    CResultSetCursor::serialize(buffer);
    buffer.append(lastRow);
}


//---------------------------------------------------------------------------

inline byte hex2digit(char c)
{
    if (c >= 'a')
        return (c - 'a' + 10);
    else if (c >= 'A')
        return (c - 'A' + 10);
    return (c - '0');
}

inline byte getHexPair(const char * s)
{
    return hex2digit(s[0]) << 4 | hex2digit(s[1]);
}


static unsigned getSubstringMatchLength(size32_t len, const void * data)
{
    const char * inbuff = (const char *)data;
    unsigned trimLen = rtlTrimStrLen(len, inbuff);
    if (trimLen && (inbuff[trimLen-1] == '*'))
        return rtlUtf8Length(trimLen-1, inbuff);
    return FullStringMatch;
}

void CColumnFilter::addValue(unsigned sizeText, const char * text)
{
    unsigned lenText = rtlUtf8Length(sizeText, text);
    unsigned size = type->getSize();
    MemoryAttrItem * next = new MemoryAttrItem;
    type_t tc = type->getTypeCode();
    if (isMappedIndexField)
    {
        if (__BYTE_ORDER  == __LITTLE_ENDIAN)
        {
            if (tc == type_int)
                tc = type_swapint;
        }
        else
        {
            if (tc == type_swapint)
                tc = type_int;
        }
    }

    //sublen is the number of characters (not the size)
    subLen = getSubstringMatchLength(sizeText, text);
    switch (tc)
    {
    case type_void:
    case type_set:
    case type_table:
    case type_groupedtable:
        break;
    case type_boolean:
        {
            byte value = rtlCsvStrToBool(lenText, text);
            next->set(sizeof(value), &value);
            break;
        }
        break;
    case type_int:
        {
            __int64 value = type->isSigned() ? rtlStrToInt8(lenText, text) : (__int64)rtlStrToUInt8(lenText, text);
            if (isMappedIndexField && type->isSigned())
                value += getIntBias(size);
            if (__BYTE_ORDER  == __LITTLE_ENDIAN)
                next->set(size, &value);
            else
                next->set(size, ((const byte *)&value)+(sizeof(value)-size));
            break;
        }
    case type_swapint:
        {
            __int64 value = type->isSigned() ? rtlStrToInt8(lenText, text) : (__int64)rtlStrToUInt8(lenText, text);
            if (isMappedIndexField && type->isSigned())
                value += getIntBias(size);
            _rev8((char *)&value);
            if (__BYTE_ORDER  == __LITTLE_ENDIAN)
                next->set(size, ((const byte *)&value)+(sizeof(value)-size));
            else
                next->set(size, &value);
            break;
        }
    case type_packedint:
        {
            void * target = next->allocate(size);
            if (type->isSigned())
                rtlSetPackedSigned(target, rtlStrToInt8(lenText, text));
            else
                rtlSetPackedUnsigned(target, rtlStrToUInt8(lenText, text));
            break;
        }
    case type_decimal:
        {
            void * target = next->allocate(size);
            DecLock();
            rtlDecPushUtf8(lenText, text);
            if (type->isSigned())
                DecPopDecimal(target, size, type->getPrecision());
            else
                DecPopUDecimal(target, size, type->getPrecision());
            DecUnlock();
            break;
        }
    case type_real:
        {
            if (size == 4)
            {
                float value = (float)rtlStrToReal(lenText, text);
                next->set(sizeof(value), &value);
            }
            else
            {
                double value = rtlStrToReal(lenText, text);
                next->set(sizeof(value), &value);
            }
            break;
        }
    case type_qstring:
        {
            if (lenText > subLen) lenText = subLen;
            char * target = (char *)next->allocate(rtlQStrSize(lenText));
            rtlStrToQStr(lenText, target, lenText, text);
            break;
        }
        break;
    case type_data:
        {
            if (lenText > subLen) lenText = subLen;
            if (subLen != FullStringMatch)
                subLen /= 2;
            unsigned max = lenText/2;
            char * target = (char *)next->allocate(max);
            for (unsigned i=0; i<max; i++)
                target[i] = getHexPair(text+i*2);
            break;
        }
    case type_string:
        {
            if (lenText > subLen) lenText = subLen;
            char * target = (char *)next->allocate(lenText);
            rtlUtf8ToStr(lenText, target, lenText, text);
            break;
        }
    case type_unicode:
        {
            if (lenText > subLen) lenText = subLen;
            UChar * target = (UChar *)next->allocate(lenText*2);
            rtlUtf8ToUnicode(lenText, target, lenText, text);
            break;
        }
        break;
    case type_varstring:
        {
            if (lenText > subLen) lenText = subLen;
            next->set(lenText+1, text);
            break;
        }
    case type_varunicode:
        {
            UChar * target = (UChar *)next->allocate(lenText*2+2);
            rtlUtf8ToUnicode(lenText, target, lenText, text);
            target[lenText] = 0;
            break;
        }
        break;
    case type_utf8:
        {
            if (lenText > subLen) sizeText = rtlUtf8Size(subLen, text);
            next->set(sizeText, text);
            //Should it be utf8 or ascii coming in?
            //char * target = (char *)next->allocate(lenText*4);
            //rtlStrToUtf8(lenText, target, lenText, text);
            break;
        }
        break;
    default:
        UNIMPLEMENTED;
    }
    if (next->length())
        values.append(*next);
    else
        next->Release();
}

void CColumnFilter::deserialize(MemoryBuffer & buffer)
{
    unsigned num;
    buffer.read(num);
    for (unsigned i= 0; i < num; i++)
    {
        MemoryAttrItem * next = new MemoryAttrItem;
        ::deserialize(buffer, *next);
        values.append(*next);
    }
}


bool CColumnFilter::optimizeFilter(IFvDataSource * dataSource)
{
    if (values.ordinality() == 0)
        return true;
    optimized = true;
    ForEachItemIn(i, values)
    {
        MemoryAttr & cur = values.item(i);
        if (!dataSource->addFilter(whichColumn, subLen, cur.length(), cur.get()))
            optimized = false;
    }
    return optimized;
}


bool CColumnFilter::matches(const byte * rowValue, unsigned valueSize, const byte * value)
{
    unsigned size = type->getSize();
    unsigned len;
    switch (type->getTypeCode())
    {
    case type_void:
    case type_set:
    case type_table:
    case type_groupedtable:
        return true;
    case type_boolean:
    case type_int:
    case type_swapint:
    case type_packedint:
        return memcmp(rowValue, value, size) == 0;
    case type_decimal:
        if (type->isSigned())
            return DecCompareDecimal(size, rowValue, value) == 0;
        return DecCompareUDecimal(size, rowValue, value) == 0;
    case type_real:
        if (size == 4)
            return *(float *)rowValue == *(float *)value;
        return *(double *)rowValue == *(double *)value;
    case type_qstring:
        len = getLength(*type, rowValue);
        if ((subLen != FullStringMatch) && (len > subLen))
            len = subLen;
        return rtlCompareQStrQStr(len, rowValue, rtlQStrLength(valueSize), value) == 0;
    case type_data:
        len = getLength(*type, rowValue);
        if ((subLen != FullStringMatch) && (len > subLen))
            len = subLen;
        return rtlCompareDataData(len, (const char *)rowValue, valueSize, (const char *)value) == 0;
    case type_string:
        len = getLength(*type, rowValue);
        if ((subLen != FullStringMatch) && (len > subLen))
            len = subLen;
        return rtlCompareStrStr(len, (const char *)rowValue, valueSize, (const char *)value) == 0;
    case type_unicode:
        len = getLength(*type, rowValue);
        return rtlCompareUnicodeUnicode(len, (const UChar *)rowValue, valueSize/2, (const UChar *)value, "") == 0;
    case type_utf8:
        len = getLength(*type, rowValue);
        if ((subLen != FullStringMatch) && (len > subLen))
            len = subLen;
        return rtlCompareUtf8Utf8(len, (const char *)rowValue, rtlUtf8Length(valueSize, value), (const char *)value, "") == 0;
    case type_varstring:
        return strcmp((const char *)rowValue, (const char *)value) != 0;
    case type_varunicode:
        return rtlCompareVUnicodeVUnicode((const UChar *)rowValue, (const UChar *)value, "") == 0;
    default:
        UNIMPLEMENTED;
    }
}

bool CColumnFilter::isValid(const byte * rowValue, const unsigned * offsets)
{
    if (optimized || values.ordinality() == 0)
        return true;

    const byte * columnValue = rowValue + offsets[whichColumn];
    ForEachItemIn(i, values)
    {
        MemoryAttr & cur = values.item(i);
        if (matches(columnValue, cur.length(), (const byte *)cur.get()))
            return true;
    }

    return false;
}

void CColumnFilter::serialize(MemoryBuffer & buffer)
{
    buffer.append((unsigned)values.ordinality());
    ForEachItemIn(i, values)
        ::serialize(buffer, values.item(i));
}


//---------------------------------------------------------------------------

CFilteredResultSet::CFilteredResultSet(IExtendedNewResultSet * _other, ColumnFilterArray & _filters) : CIndirectResultSet(_other)
{
    appendArray(filters, _filters);
    initExtra();
}

#if 0
CFilteredResultSet::CFilteredResultSet(const CResultSetMetaData & _meta, CResultSet * _resultSet, MemoryBuffer & buffer) : CResultSetCursor(_meta, _resultSet, false)
{ 
    unsigned numFilters;
    buffer.read(numFilters);
    for (unsigned i=0; i< numFilters; i++)
    {
        CColumnFilter & next = * new CColumnFilter(meta.queryType(i));
        next.deserialize(buffer);
        filters.append(next);
    }
    initExtra();
    buffer.read(curRow);
    buffer.read(lastRow);
    absolute(lastRow);
}
#endif

CFilteredResultSet::~CFilteredResultSet()
{
    delete [] offsets;
}

bool CFilteredResultSet::getRow(MemoryBuffer & out, __int64 row)
{
    __int64 newRow = translateRow(row);
    if (newRow < 0)
        return false;
    return CIndirectResultSet::getRow(out, newRow);
}


bool CFilteredResultSet::getRawRow(MemoryBuffer & out, __int64 row)
{
    __int64 newRow = translateRow(row);
    if (newRow < 0)
        return false;
    return CIndirectResultSet::getRawRow(out, newRow);
}

__int64 CFilteredResultSet::getNumRows() const
{
    if (readAll)
        return validPositions.ordinality();
    return UNKNOWN_NUM_ROWS;
}

bool CFilteredResultSet::rowMatchesFilter(const byte * row)
{
    if (!meta.isFixedSize())
        meta.calcFieldOffsets(row, offsets);

    ForEachItemIn(i, filters)
    {
        if (!filters.item(i).isValid(row, offsets))
            return false;
    }
    return true;
}


__int64 CFilteredResultSet::translateRow(__int64 row)
{
    if (row < 0)
        return row;
    if (row > MAX_FILTER_ELEMENTS)
        return AFTER_LAST_ROW;      // would it be better to throw an error?
    if (!readAll && (row >= validPositions.ordinality()))
    {
        unsigned __int64 nextPos = 0;
        if (validPositions.ordinality())
            nextPos = validPositions.tos()+1;

        MemoryBuffer tempBuffer;
        unsigned startTime = msTick();
        while (row >= validPositions.ordinality())
        {
            if (!CIndirectResultSet::getRow(tempBuffer.clear(), nextPos))
            {
                readAll = true;
                break;
            }
            if (rowMatchesFilter((byte *)tempBuffer.toByteArray()))
            {
                validPositions.append(nextPos);
            }
            else
            {
                unsigned timeTaken = msTick() -  startTime;
                if (timeTaken > MAX_SKIP_TIME)
                    throwError1(FVERR_FilterTooRestrictive, timeTaken/1000);
            }
            nextPos++;
        }
    }

    if ((unsigned)row < validPositions.ordinality())
        return validPositions.item((unsigned)row);
    return AFTER_LAST_ROW;
}


void CFilteredResultSet::serialize(MemoryBuffer & buffer)
{
    CIndirectResultSet::serialize(buffer);
    buffer.append((unsigned)filters.ordinality());
    ForEachItemIn(i, filters)
        filters.item(i).serialize(buffer);
}


void CFilteredResultSet::initExtra()
{
    readAll = false;
    offsets = new unsigned[meta.getColumnCount()+1];
    if (meta.isFixedSize())
        meta.calcFieldOffsets(NULL, offsets);
}



//---------------------------------------------------------------------------

CFetchFilteredResultSet::CFetchFilteredResultSet(IExtendedNewResultSet * _parent, const CStrColumnFilter & _filter) : CIndirectResultSet(_parent)
{
    ForEachItemIn(i, _filter.values)
    {
        const MemoryAttrItem & value = _filter.values.item(i);
        unsigned __int64 offset = rtlStrToUInt8(value.length(), static_cast<const char *>(value.get()));
        validOffsets.append(offset);
    }
}

bool CFetchFilteredResultSet::getRow(MemoryBuffer & out, __int64 row)
{
    if (!validOffsets.isItem((unsigned)row))
        return false;

    return CIndirectResultSet::fetch(out, validOffsets.item((unsigned)row));
}


bool CFetchFilteredResultSet::getRawRow(MemoryBuffer & out, __int64 row)
{
    if (!validOffsets.isItem((unsigned)row))
        return false;

    return CIndirectResultSet::fetchRaw(out, validOffsets.item((unsigned)row));
}

__int64 CFetchFilteredResultSet::getNumRows() const
{
    return validOffsets.ordinality();
}

void CFetchFilteredResultSet::serialize(MemoryBuffer & buffer)
{
    CIndirectResultSet::serialize(buffer);
    buffer.append((unsigned)validOffsets.ordinality());
    ForEachItemIn(i, validOffsets)
        buffer.append(validOffsets.item(i));
}


//---------------------------------------------------------------------------


CFilteredResultSetBuilder::CFilteredResultSetBuilder(IExtendedNewResultSet * _resultSet)
{
    resultSet.set(_resultSet);
}

INewResultSet * CFilteredResultSetBuilder::create()
{
    Linked<IExtendedNewResultSet> baseResultSet = resultSet;

    const CResultSetMetaData & meta = static_cast<const CResultSetMetaData &>(resultSet->getMetaData());

    //Check for fetch filter, and if present apply that first
    unsigned numColumns = meta.getColumnCount();
    if (filters.isItem(numColumns-1) && meta.isVirtual(numColumns-1))
    {
        CStrColumnFilter & cur = filters.item(numColumns-1);
        if (cur.values.ordinality())
        {
            baseResultSet.setown(new CFetchFilteredResultSet(resultSet, cur));
            cur.values.kill();
        }
    }

    Owned<IExtendedNewResultSet> cloned = baseResultSet->cloneForFilter();
    IFvDataSource * dataSource = cloned ? cloned->queryDataSource() : NULL;

    ColumnFilterArray rawFilters;
    ForEachItemIn(columnIndex, filters)
    {
        CStrColumnFilter & cur = filters.item(columnIndex);
        if (cur.values.ordinality())
        {
            Owned<CColumnFilter> next = new CColumnFilter(columnIndex, meta.queryType(columnIndex), resultSet->isMappedIndexField(columnIndex));
            ForEachItemIn(j, cur.values)
            {
                MemoryAttrItem & curValue = cur.values.item(j);
                next->addValue(curValue.length(), reinterpret_cast<const char *>(curValue.get()));
            }

            if (!cloned || !next->optimizeFilter(dataSource))
                rawFilters.append(*next.getClear());
        }
    }

    if (cloned)
    {
        dataSource->applyFilter();
        if (rawFilters.ordinality() == 0)
            return cloned.getClear();
        return new CFilteredResultSet(cloned, rawFilters);
    }
    if (rawFilters.ordinality() == 0)
        return baseResultSet.getClear();
    return new CFilteredResultSet(baseResultSet, rawFilters);
}

void CFilteredResultSetBuilder::addFilter(unsigned columnIndex, const char * value)
{
    addFilter(columnIndex, strlen(value), value);
}

void CFilteredResultSetBuilder::addFilter(unsigned columnIndex, unsigned len, const char * value)
{
    assertex(columnIndex < (unsigned)resultSet->getMetaData().getColumnCount());

    while (filters.ordinality() <= columnIndex)
        filters.append(*new CStrColumnFilter());

    filters.item(columnIndex).addValue(len, value);
}

void CFilteredResultSetBuilder::addNaturalFilter(unsigned columnIndex, unsigned len, const char * value)
{
    const CResultSetMetaData & meta = static_cast<const CResultSetMetaData &>(resultSet->getMetaData());
    assertex(columnIndex < (unsigned)meta.getColumnCount());

    const CResultSetColumnInfo & column = meta.queryColumn(columnIndex);
    if (column.setTransforms.ordinality())
    {
        MemoryAttr source(len, value);
        MemoryAttr translated;
        translateValue(translated, source, column.setTransforms);
        addFilter(columnIndex, translated.length(), static_cast<const char *>(translated.get()));
    }
    else
        addFilter(columnIndex, len, value);
}

void CFilteredResultSetBuilder::clearFilter(unsigned columnIndex)
{
    assertex(columnIndex < (unsigned)resultSet->getMetaData().getColumnCount());

    if (filters.isItem(columnIndex))
        filters.item(columnIndex).clear();
}

void CFilteredResultSetBuilder::clearFilters()
{
    filters.kill();
}


//---------------------------------------------------------------------------

CResultSetFactoryBase::CResultSetFactoryBase(const char * _username, const char * _password)
{
    username.set(_username);
    password.set(_password);
}

CResultSetFactoryBase::CResultSetFactoryBase(ISecManager &secmgr, ISecUser &secuser)
{
    secMgr.set(&secmgr);
    secUser.set(&secuser);
    username.set(secuser.getName());
    password.set(secuser.credentials().getPassword());
}

//---------------------------------------------------------------------------

CResultSetFactory::CResultSetFactory(const char * _username, const char * _password) : CResultSetFactoryBase(_username, _password)
{
}

CResultSetFactory::CResultSetFactory(ISecManager &secmgr, ISecUser &secuser) : CResultSetFactoryBase(secmgr, secuser)
{
}

IDistributedFile * CResultSetFactory::lookupLogicalName(const char * logicalName)
{
    Owned<IUserDescriptor> udesc;
    if(username != NULL && *username != '\0')
    {
        udesc.setown(createUserDescriptor());
        udesc->set(username, password);
    }

    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, udesc.get());
    if (!df)
        throwError1(FVERR_CouldNotResolveX, logicalName);
    return df.getClear();
}


INewResultSet * CResultSetFactory::createNewResultSet(IConstWUResult * wuResult, const char * wuid)
{
    Owned<IFvDataSource> ds = createDataSource(wuResult, wuid, username, password);
    if (ds)
        return createResultSet(ds, (wuResult->getResultFormat() == ResultFormatXml));
    return NULL;
}

INewResultSet * CResultSetFactory::createNewFileResultSet(const char * logicalName, const char * cluster)
{
    Owned<IDistributedFile> df = lookupLogicalName(logicalName);
    Owned<IFvDataSource> ds = createFileDataSource(df, logicalName, cluster, username, password);
    if (ds)
    {
        Owned<CResultSet> result = createResultSet(ds, false);
        result->setColumnMapping(df);
        return result.getClear();
    }
    return NULL;
}

INewResultSet * CResultSetFactory::createNewResultSet(const char * wuid, unsigned sequence, const char * name)
{
    Owned<IConstWUResult> wuResult = (secMgr) ? secResolveResult(*secMgr, *secUser, wuid, sequence, name) : resolveResult(wuid, sequence, name);
    return (wuResult) ? createNewResultSet(wuResult, wuid) : NULL;
}

INewResultSet * CResultSetFactory::createNewFileResultSet(const char * logicalFile)
{
    return createNewFileResultSet(logicalFile, NULL);
}

CResultSet * CResultSetFactory::createResultSet(IFvDataSource * ds, bool _useXPath)
{
    //MORE: Save in a hash table, which times out after a certain period...
    return new CResultSet(ds, _useXPath);
}

IResultSetMetaData * CResultSetFactory::createResultSetMeta(IConstWUResult * wuResult)
{
    return new CResultSetMetaData(createMetaData(wuResult), false);
}

IResultSetMetaData * CResultSetFactory::createResultSetMeta(const char * wuid, unsigned sequence, const char * name)
{
    Owned<IConstWUResult> wuResult = (secMgr) ? secResolveResult(*secMgr, *secUser, wuid, sequence, name) : resolveResult(wuid, sequence, name);
    return (wuResult) ? createResultSetMeta(wuResult) : NULL;
}


//---------------------------------------------------------------------------

CRemoteResultSetFactory::CRemoteResultSetFactory(const char * remoteServer, const char * _username, const char * _password) : CResultSetFactoryBase(_username, _password)
{
    serverEP.set(remoteServer);
}

CRemoteResultSetFactory::CRemoteResultSetFactory(const char * remoteServer, ISecManager &secmgr, ISecUser &secuser) : CResultSetFactoryBase(secmgr, secuser)
{
    serverEP.set(remoteServer);
}

INewResultSet * CRemoteResultSetFactory::createNewResultSet(IConstWUResult * wuResult, const char * wuid)
{
    SCMStringBuffer name;
    wuResult->getResultName(name);
    Owned<IFvDataSource> ds = createRemoteDataSource(serverEP, username, password, wuid, wuResult->getResultSequence(), name.length() ? name.str() : NULL);
    if (ds)
        return new CResultSet(ds, false);
    return NULL;
}

INewResultSet * CRemoteResultSetFactory::createNewFileResultSet(const char * logicalName, const char * cluster)
{
    Owned<IFvDataSource> ds = createRemoteFileDataSource(serverEP, username, password, logicalName);
    if (ds)
        return new CResultSet(ds, false);
    return NULL;
}

INewResultSet* CRemoteResultSetFactory::createNewResultSet(const char * wuid, unsigned sequence, const char * name)
{
    Owned<IFvDataSource> ds = createRemoteDataSource(serverEP, username, password, wuid, sequence, name);
    if (ds)
        return new CResultSet(ds, false);
    return NULL;
}

INewResultSet* CRemoteResultSetFactory::createNewFileResultSet(const char * logicalName)
{
    Owned<IFvDataSource> ds = createRemoteFileDataSource(serverEP, username, password, logicalName);
    if (ds)
        return new CResultSet(ds, false);
    return NULL;
}

IResultSetMetaData * CRemoteResultSetFactory::createResultSetMeta(IConstWUResult * wuResult)
{
    UNIMPLEMENTED;
}

IResultSetMetaData * CRemoteResultSetFactory::createResultSetMeta(const char * wuid, unsigned sequence, const char * name)
{
    UNIMPLEMENTED;
}

//---------------------------------------------------------------------------

INewResultSet* createNewResultSet(IResultSetFactory & factory, IStringVal & error, IConstWUResult * wuResult, const char * wuid)
{
    try
    {
        return factory.createNewResultSet(wuResult, wuid);
    }
    catch (IException * e)
    {
        StringBuffer s;
        error.set(e->errorMessage(s).str());
        e->Release();
        return NULL;
    }
}

INewResultSet* createNewFileResultSet(IResultSetFactory & factory, IStringVal & error, const char * logicalFile, const char * cluster)
{
    try
    {
        return factory.createNewFileResultSet(logicalFile, cluster);
    }
    catch (IException * e)
    {
        StringBuffer s;
        error.set(e->errorMessage(s).str());
        e->Release();
        return NULL;
    }
}


INewResultSet* createNewResultSetSeqName(IResultSetFactory & factory, IStringVal & error, const char * wuid, unsigned sequence, const char * name)
{
    try
    {
        return factory.createNewResultSet(wuid, sequence, name);
    }
    catch (IException * e)
    {
        StringBuffer s;
        error.set(e->errorMessage(s).str());
        e->Release();
        return NULL;
    }
}

//---------------------------------------------------------------------------

//MORE: There should be an option to create a proxy-based IFileView so that
//i) formatting etc. can be done remotely
//ii) cacching and sort order calculation can be done remotely.
// Should still cache the last n rows locally (raw/non-raw).

IResultSetFactory * getResultSetFactory(const char * username, const char * password)
{
    return new CResultSetFactory(username, password);
}

IResultSetFactory * getSecResultSetFactory(ISecManager *secmgr, ISecUser *secuser, const char *username, const char *password)
{
    if (secmgr)
        return new CResultSetFactory(*secmgr, *secuser);
    return getResultSetFactory(username, password);
}

IResultSetFactory * getRemoteResultSetFactory(const char * remoteServer, const char * username, const char * password)
{
    return new CRemoteResultSetFactory(remoteServer, username, password);
}

int findResultSetColumn(const INewResultSet * results, const char * columnName)
{
    const IResultSetMetaData & meta = results->getMetaData();
    SCMStringBuffer s;
    for(int i = 0; i < meta.getColumnCount(); i++)
    {
        s.clear();
        if(!stricmp(columnName, meta.getColumnLabel(s, i).str()))
            return i;
    }
    return -1;
}


extern FILEVIEW_API unsigned getResultCursorXml(IStringVal & ret, IResultSetCursor * cursor, const char * name, unsigned start, unsigned count, const char * schemaName, const IProperties *xmlns)
{
    Owned<CommonXmlWriter> writer = CreateCommonXmlWriter(XWFexpandempty);
    unsigned rc = writeResultCursorXml(*writer, cursor, name, start, count, schemaName, xmlns);
    ret.set(writer->str());
    return rc;

}

extern FILEVIEW_API unsigned getResultXml(IStringVal & ret, INewResultSet * result, const char* name,unsigned start, unsigned count, const char * schemaName, const IProperties *xmlns)
{
    Owned<IResultSetCursor> cursor = result->createCursor();
    return getResultCursorXml(ret, cursor, name, start, count, schemaName, xmlns);
}

extern FILEVIEW_API unsigned getResultJSON(IStringVal & ret, INewResultSet * result, const char* name,unsigned start, unsigned count, const char * schemaName)
{
    Owned<IResultSetCursor> cursor = result->createCursor();
    Owned<CommonJsonWriter> writer = new CommonJsonWriter(0);
    writer->outputBeginRoot();
    unsigned rc = writeResultCursorXml(*writer, cursor, name, start, count, schemaName);
    writer->outputEndRoot();
    ret.set(writer->str());
    return rc;
}

extern FILEVIEW_API unsigned writeResultCursorXml(IXmlWriter & writer, IResultSetCursor * cursor, const char * name, unsigned start, unsigned count, const char * schemaName, const IProperties *xmlns)
{
    if (schemaName)
    {
        writer.outputBeginNested("XmlSchema", false);
        writer.outputUtf8(strlen(schemaName), schemaName, "@name");
        SCMStringBuffer xsd;
        const IResultSetMetaData & meta = cursor->queryResultSet()->getMetaData();
        meta.getXmlXPathSchema(xsd, false);
        writer.outputInlineXml(xsd.str());
        writer.outputEndNested("XmlSchema");
    }

    writer.outputBeginDataset(name, true);
    if (schemaName)
        writer.outputCString(schemaName, "@xmlSchema");
    if (xmlns)
    {
        Owned<IPropertyIterator> it = const_cast<IProperties*>(xmlns)->getIterator();
        ForEach(*it)
        {
            const char *name = it->getPropKey();
            writer.outputXmlns(name,const_cast<IProperties*>(xmlns)->queryProp(name));
        }
    }
    cursor->beginWriteXmlRows(writer);
    unsigned c=0;
    for(bool ok=cursor->absolute(start);ok;ok=cursor->next())
    {
        cursor->writeXmlRow(writer);

        c++;
        if(count && c>=count)
            break;
    }
    cursor->endWriteXmlRows(writer);
    writer.outputEndDataset(name);
    return c;
}

extern FILEVIEW_API unsigned writeResultXml(IXmlWriter & writer, INewResultSet * result, const char* name,unsigned start, unsigned count, const char * schemaName, const IProperties *xmlns)
{
    Owned<IResultSetCursor> cursor = result->createCursor();
    return writeResultCursorXml(writer, cursor, name, start, count, schemaName, xmlns);
}

extern FILEVIEW_API unsigned getResultCursorBin(MemoryBuffer & ret, IResultSetCursor * cursor, unsigned start, unsigned count)
{
    const IResultSetMetaData & meta = cursor->queryResultSet()->getMetaData();
    unsigned numCols = meta.getColumnCount();

    unsigned c=0;
    for(bool ok=cursor->absolute(start);ok;ok=cursor->next())
    {
        for (unsigned col=0; col < numCols; col++)
            cursor->getRaw(MemoryBuffer2IDataVal(ret), col);

        c++;
        if(count && c>=count)
            break;
    }
    return c;
}

extern FILEVIEW_API unsigned getResultBin(MemoryBuffer & ret, INewResultSet * result, unsigned start, unsigned count)
{
    Owned<IResultSetCursor> cursor = result->createCursor();
    return getResultCursorBin(ret, cursor, start, count);
}

inline const char *getSeverityTagname(WUExceptionSeverity severity, unsigned flags)
{
    if (flags & WorkUnitXML_SeverityTags)
    {
        switch (severity)
        {
        case ExceptionSeverityInformation:
            return "Info";
        case ExceptionSeverityWarning:
            return "Warning";
        case ExceptionSeverityError:
        default:
            break;
        }
    }
    return "Exception";
}

extern FILEVIEW_API void writeFullWorkUnitResults(const char *username, const char *password, const IConstWorkUnit *cw, IXmlWriter &writer, unsigned flags, WUExceptionSeverity minSeverity, const char *rootTag)
{
    SCMStringBuffer wuid;
    cw->getWuid(wuid);

    if (rootTag && *rootTag && !(flags & WorkUnitXML_NoRoot))
        writer.outputBeginNested(rootTag, true);

    Owned<IConstWUExceptionIterator> exceptions = &cw->getExceptions();
    ForEach(*exceptions)
    {
        WUExceptionSeverity severity = exceptions->query().getSeverity();
        if (severity>=minSeverity)
        {
            SCMStringBuffer src, msg;
            exceptions->query().getExceptionSource(src);
            exceptions->query().getExceptionMessage(msg);
            writer.outputBeginNested(getSeverityTagname(severity, flags), false);
            writer.outputCString(src.str(), "Source");
            writer.outputCString(msg.str(), "Message");
            writer.outputEndNested(getSeverityTagname(severity, flags));
        }
    }

    Owned<IResultSetFactory> factory = getResultSetFactory(username, password);
    switch (cw->getState())
    {
        case WUStateCompleted:
        case WUStateWait:
        {
            Owned<IConstWUResultIterator> results = &cw->getResults();
            ForEach(*results)
            {
                IConstWUResult &ds = results->query();
                if (ds.getResultSequence()>=0)
                {
                    SCMStringBuffer name;
                    ds.getResultName(name);
                    Owned<INewResultSet> nr = factory->createNewResultSet(&ds, wuid.str());
                    const IProperties *xmlns = ds.queryXmlns();
                    writeResultXml(writer, nr.get(), name.str(), 0, 0, (flags & WorkUnitXML_InclSchema) ? name.str() : NULL, xmlns);
                }
            }
        }
        break;

        case WUStateAborted:
            writer.outputBeginNested("Exception", false);
            writer.outputCString("System", "Source");
            writer.outputCString("Query aborted by operator", "Message");
            writer.outputEndNested("Exception");
            break;
    }
    if (rootTag && *rootTag && !(flags & WorkUnitXML_NoRoot))
        writer.outputEndNested(rootTag);
}

extern FILEVIEW_API IStringVal& getFullWorkUnitResultsXML(const char *username, const char *password, const IConstWorkUnit *cw, IStringVal &str, unsigned flags, WUExceptionSeverity minSeverity)
{
    SCMStringBuffer wuid;
    cw->getWuid(wuid);

    Owned<CommonXmlWriter> writer = CreateCommonXmlWriter(XWFexpandempty);
    writeFullWorkUnitResults(username, password, cw, *writer, flags, minSeverity, "Result");

    const char *xml = writer->str();
    unsigned len = writer->length();
    if (len && xml[len-1]=='\n')
        len--;
    str.setLen(xml, len);
    return str;
}

extern FILEVIEW_API IStringVal& getFullWorkUnitResultsJSON(const char *username, const char *password, const IConstWorkUnit *cw, IStringVal &str, unsigned flags, WUExceptionSeverity minSeverity)
{
    SCMStringBuffer wuid;
    cw->getWuid(wuid);

    Owned<CommonJsonWriter> writer = new CommonJsonWriter(0);
    writer->outputBeginRoot();
    writeFullWorkUnitResults(username, password, cw, *writer, flags, minSeverity, "Results");
    writer->outputEndRoot();
    str.set(writer->str());
    return str;
}
