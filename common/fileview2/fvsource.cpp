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
#include "eclrtl.hpp"
#include "rtlds_imp.hpp"

#include "fvresultset.ipp"

#include "fileview.hpp"
#include "fvsource.ipp"
#include "fverror.hpp"
#include "hqlerror.hpp"
#include "eclhelper.hpp"
#include "hqlattr.hpp"
#include "hqlutil.hpp"

#include "fvdatasource.hpp"

#define MAX_RECORD_SIZE         4096

inline void appendNextXpathName(StringBuffer &s, const char *&xpath)
{
    while (*xpath && !strchr("*[/", *xpath))
        s.append(*xpath++);
    xpath = strchr(xpath, '/');
}

void splitXmlTagNamesFromXPath(const char *xpath, StringAttr &inner, StringAttr *outer=NULL)
{
    if (!xpath)
        return;

    StringBuffer s1;
    StringBuffer s2;

    appendNextXpathName(s1, xpath);
    if (outer && xpath)
        appendNextXpathName(s2, ++xpath);
    if (xpath) //xpath too deep
        return;

    if (!s2.length())
        inner.set(s1.str());
    else
    {
        inner.set(s2.str());
        outer->set(s1.str());
    }
    if (!inner.get())
        inner.set("");
    if (outer && !outer->get())
        outer->set("");
}

DataSourceMetaItem::DataSourceMetaItem(unsigned _flags, const char * _name, const char * _xpath, ITypeInfo * _type)
{
    flags = _flags;
    name.set(_name);
    type.set(_type);
    xpath.set(_xpath);
    splitXmlTagNamesFromXPath(_xpath, tagname);
    hasMixedContent = false;
}

DataSourceMetaItem::DataSourceMetaItem(unsigned _flags, MemoryBuffer & in)
{
    flags = _flags;
    in.read(hasMixedContent);
    in.read(name);
    in.read(xpath);
    type.setown(deserializeType(in));
    splitXmlTagNamesFromXPath(xpath.get(), tagname);
}


void DataSourceMetaItem::serialize(MemoryBuffer & out) const
{
    out.append(flags);
    out.append(hasMixedContent);
    out.append(name);
    out.append(xpath);
    type->serialize(out);
}

//---------------------------------------------------------------------------

DataSourceDatasetItem::DataSourceDatasetItem(const char * _name, const char * _xpath, IHqlExpression * expr) : DataSourceMetaItem(FVFFdataset, NULL, NULL, NULL), record(expr->queryRecord(), 0, true, false, false)
{
    type.setown(makeTableType(NULL));
    name.set(_name);
    xpath.set(_xpath);
    splitXmlTagNamesFromXPath(_xpath, record.tagname, &tagname);
    if (!record.tagname.length())
        record.tagname.set("Row");
}

DataSourceDatasetItem::DataSourceDatasetItem(unsigned flags, MemoryBuffer & in) : DataSourceMetaItem(FVFFdataset, NULL, NULL, NULL), record(in)
{
    type.setown(makeTableType(NULL));
    in.read(name);
}

void DataSourceDatasetItem::serialize(MemoryBuffer & out) const
{
    out.append(flags);
    record.serialize(out);
    out.append(name);
}

//---------------------------------------------------------------------------

DataSourceSetItem::DataSourceSetItem(const char * _name, const char * _xpath, ITypeInfo * _type) : DataSourceMetaItem(FVFFset, _name, NULL, _type)
{
    createChild();
    xpath.set(_xpath);
    StringBuffer attr;
    splitXmlTagNamesFromXPath(_xpath, record.tagname, &tagname);
    if (!_xpath)
        record.tagname.set("Item");
}

DataSourceSetItem::DataSourceSetItem(unsigned flags, MemoryBuffer & in) : DataSourceMetaItem(flags, in)
{
    createChild();
    splitXmlTagNamesFromXPath(xpath.get(), record.tagname, &tagname);
    if (!record.tagname.get())
        record.tagname.set("Item");
}

void DataSourceSetItem::createChild()
{
    ITypeInfo * childType = type->queryChildType()->queryPromotedType();
    record.addSimpleField("Item", NULL, childType);
}

void DataSourceSetItem::serialize(MemoryBuffer & out) const
{
    out.append(flags);
    record.serialize(out);
    out.append(name);
}

//---------------------------------------------------------------------------

DataSourceMetaData::DataSourceMetaData(IHqlExpression * _record, byte _numFieldsToIgnore, bool _randomIsOk, bool _isGrouped, unsigned _keyedSize)
{
    init();
    numFieldsToIgnore = _numFieldsToIgnore;
    randomIsOk = _randomIsOk;
    isStoredFixedWidth = true;
    //MORE: Blobs aren't handled correctly in indexes....
    maxRecordSize = ::getMaxRecordSize(_record, MAX_RECORD_SIZE);
    keyedSize = _keyedSize;

    gatherFields(_record, false, &hasMixedContent);
    if (_isGrouped)
    {
        Owned<ITypeInfo> type = makeBoolType();
        addSimpleField("__groupfollows__", NULL, type);
        maxRecordSize++;
    }
    gatherAttributes();

    if (isStoredFixedWidth)
        assertex(minRecordSize == maxRecordSize);
}


DataSourceMetaData::DataSourceMetaData()
{
    init();
    randomIsOk = true;
    isStoredFixedWidth = true;
}


DataSourceMetaData::DataSourceMetaData(type_t typeCode)
{
    init();
    OwnedITypeInfo type;
    if (typeCode == type_unicode)
        type.setown(makeUnicodeType(UNKNOWN_LENGTH, 0));
    else
        type.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL));
    fields.append(*new DataSourceMetaItem(FVFFnone, "line", NULL, type));
}

void DataSourceMetaData::init()
{
    keyedSize = 0;
    minRecordSize = 0;
    maxRecordSize = 0;
    bitsRemaining = 0;
    numVirtualFields = 0;
    isStoredFixedWidth = false;
    randomIsOk = false;
    numFieldsToIgnore = 0;
    hasMixedContent = false;
}

DataSourceMetaData::DataSourceMetaData(MemoryBuffer & buffer)
{
    numVirtualFields = 0;
    buffer.read(numFieldsToIgnore);
    buffer.read(randomIsOk);
    buffer.read(hasMixedContent);
    buffer.read(isStoredFixedWidth);
    buffer.read(minRecordSize);
    buffer.read(keyedSize);
    buffer.read(maxRecordSize);

    unsigned numFields;
    buffer.read(numFields);
    for (unsigned idx=0; idx < numFields; idx++)
    {
        byte flags;
        buffer.read(flags);
        if (flags == FVFFdataset)
            fields.append(*new DataSourceDatasetItem(flags, buffer));
        else if (flags == FVFFset)
            fields.append(*new DataSourceSetItem(flags, buffer));
        else
            fields.append(*new DataSourceMetaItem(flags, buffer));
        if (flags == FVFFvirtual)
            ++numVirtualFields;
    }
    gatherAttributes();
}


void DataSourceMetaData::addFileposition()
{
    addVirtualField("__fileposition__", NULL, makeIntType(8, false));
}

void DataSourceMetaData::addSimpleField(const char * name, const char * xpath, ITypeInfo * type)
{
    ITypeInfo * promoted = type->queryPromotedType();
    unsigned size = promoted->getSize();
    unsigned thisBits = 0;
    if (size == UNKNOWN_LENGTH)
    {
        isStoredFixedWidth = false;
        switch (type->getTypeCode())
        {
        case type_set:
            minRecordSize += sizeof(bool) + sizeof(size32_t);
            break;
        case type_varstring:
            minRecordSize += 1;
            break;
        case type_packedint:
            minRecordSize += 1;
            break;
        case type_varunicode:
            minRecordSize += sizeof(UChar);
            break;
        default:
            minRecordSize += sizeof(size32_t);
            break;
        }
    }
    else if (type->getTypeCode() == type_bitfield)
    {
        thisBits = type->getBitSize();
        if (thisBits > bitsRemaining)
        {
            size = type->queryChildType()->getSize();
            minRecordSize += size;
            bitsRemaining = size * 8;
        }
        bitsRemaining -= thisBits;
    }
    else
        minRecordSize += size;
    if (thisBits == 0)
        bitsRemaining = 0;
    fields.append(*new DataSourceMetaItem(FVFFnone, name, xpath, type));
}


void DataSourceMetaData::addVirtualField(const char * name, const char * xpath, ITypeInfo * type)
{
    fields.append(*new DataSourceMetaItem(FVFFvirtual, name, xpath, type));
    ++numVirtualFields;
}


void DataSourceMetaData::extractKeyedInfo(UnsignedArray & offsets, TypeInfoArray & types)
{
    unsigned curOffset = 0;
    ForEachItemIn(i, fields)
    {
        if (curOffset >= keyedSize)
            break;

        DataSourceMetaItem & cur = fields.item(i);
        switch (cur.flags)
        {
        case FVFFnone:
            {
                offsets.append(curOffset);
                types.append(*LINK(cur.type));
                unsigned size = cur.type->getSize();
                assertex(size != UNKNOWN_LENGTH);
                curOffset += size;
                break;
            }
        case FVFFbeginrecord:
        case FVFFendrecord:
            break;
        default:
            throwUnexpected();
        }
    }
    offsets.append(curOffset);
    assertex(curOffset == keyedSize);
}

//MORE: Really this should create no_selects for the sub records, but pass on that for the moment.
void DataSourceMetaData::gatherFields(IHqlExpression * expr, bool isConditional, bool *pMixedContent)
{
    switch (expr->getOperator())
    {
    case no_record:
        gatherChildFields(expr, isConditional, pMixedContent);
        break;
    case no_ifblock:
        {
            OwnedITypeInfo boolType = makeBoolType();
            OwnedITypeInfo voidType = makeVoidType();
            isStoredFixedWidth = false;
            fields.append(*new DataSourceMetaItem(FVFFbeginif, NULL, NULL, boolType));
            gatherChildFields(expr->queryChild(1), true, pMixedContent);
            fields.append(*new DataSourceMetaItem(FVFFendif, NULL, NULL, voidType));
            break;
        }
    case no_field:
        {
            if (expr->hasAttribute(__ifblockAtom))
                break;
            Linked<ITypeInfo> type = expr->queryType();
            IAtom * name = expr->queryName();
            IHqlExpression * nameAttr = expr->queryAttribute(namedAtom);
            StringBuffer outname;
            if (nameAttr && nameAttr->queryChild(0)->queryValue())
                nameAttr->queryChild(0)->queryValue()->getStringValue(outname);
            else
                outname.append(name).toLowerCase();

            StringBuffer xpathtext;
            const char * xpath = NULL;
            IHqlExpression * xpathAttr = expr->queryAttribute(xpathAtom);
            if (xpathAttr && xpathAttr->queryChild(0)->queryValue())
                xpath = xpathAttr->queryChild(0)->queryValue()->getStringValue(xpathtext);

            if (isKey() && expr->hasAttribute(blobAtom))
                type.setown(makeIntType(8, false));
            type_t tc = type->getTypeCode();
            if (tc == type_row)
            {
                OwnedITypeInfo voidType = makeVoidType();
                Owned<DataSourceMetaItem> begin = new DataSourceMetaItem(FVFFbeginrecord, outname, xpath, voidType);
                //inherit mixed content from child row with xpath('')
                bool *pItemMixedContent = (pMixedContent && xpath && !*xpath) ? pMixedContent : &(begin->hasMixedContent);
                fields.append(*begin.getClear());
                gatherChildFields(expr->queryRecord(), isConditional, pItemMixedContent);
                fields.append(*new DataSourceMetaItem(FVFFendrecord, outname, xpath, voidType));
            }
            else if ((tc == type_dictionary) || (tc == type_table) || (tc == type_groupedtable))
            {
                isStoredFixedWidth = false;
                Owned<DataSourceDatasetItem> ds = new DataSourceDatasetItem(outname, xpath, expr);
                if (pMixedContent && xpath && !*xpath)
                    *pMixedContent = ds->queryChildMeta()->hasMixedContent;
                fields.append(*ds.getClear());
            }
            else if (tc == type_set)
            {
                isStoredFixedWidth = false;
                if (pMixedContent && xpath && !*xpath)
                    *pMixedContent = true;
                fields.append(*new DataSourceSetItem(outname, xpath, type));
            }
            else
            {
                if (type->getTypeCode() == type_alien)
                {
                    IHqlAlienTypeInfo * alien = queryAlienType(type);
                    type.set(alien->queryPhysicalType());
                }
                if (pMixedContent && xpath && !*xpath)
                    *pMixedContent = true;
                addSimpleField(outname, xpath, type);
            }
            break;
        }
    }
}


void DataSourceMetaData::gatherChildFields(IHqlExpression * expr, bool isConditional, bool *pMixedContent)
{
    bitsRemaining = 0;
    ForEachChild(idx, expr)
        gatherFields(expr->queryChild(idx), isConditional, pMixedContent);
}

unsigned DataSourceMetaData::numKeyedColumns() const
{
    unsigned count = 0;
    unsigned curOffset = 0;
    ForEachItemIn(i, fields)
    {
        if (curOffset >= keyedSize)
            break;

        DataSourceMetaItem & cur = fields.item(i);
        switch (cur.flags)
        {
        case FVFFnone:
            {
                unsigned size = cur.type->getSize();
                assertex(size != UNKNOWN_LENGTH);
                curOffset += size;
                count++;
                break;
            }
        default:
            throwUnexpected();
        }
    }
    return count;
    assertex(curOffset == keyedSize);
}

unsigned DataSourceMetaData::numColumns() const
{
    return fields.ordinality() - numFieldsToIgnore;
}

ITypeInfo * DataSourceMetaData::queryType(unsigned column) const
{
    return fields.item(column).type;
}

unsigned DataSourceMetaData::queryFieldFlags(unsigned column) const
{
    return fields.item(column).flags;
}

bool DataSourceMetaData::mixedContent(unsigned column) const
{
    return fields.item(column).hasMixedContent;
}

bool DataSourceMetaData::mixedContent() const
{
    return hasMixedContent;
}

const char * DataSourceMetaData::queryName(unsigned column) const
{
    return fields.item(column).name;
}

const char * DataSourceMetaData::queryXPath(unsigned column) const
{
    return fields.item(column).xpath;
}

const char * DataSourceMetaData::queryXmlTag(unsigned column) const
{
    DataSourceMetaItem &item = fields.item(column);
    return (item.tagname.get()) ? item.tagname.get() : item.name.get();
}

const char *DataSourceMetaData::queryXmlTag() const
{
    return (tagname.get()) ? tagname.get() : "Row";
}

void DataSourceMetaData::gatherNestedAttributes(DataSourceMetaItem &rec, aindex_t &idx)
{
    aindex_t numItems = fields.ordinality();
    while (++idx < numItems)
    {
        DataSourceMetaItem &item = fields.item(idx);
        if (item.flags==FVFFendrecord)
            return;
        else if (item.flags==FVFFbeginrecord)
            gatherNestedAttributes((!item.tagname.get() || *item.tagname.get()) ? item : rec, idx);
        else if (item.isXmlAttribute())
            rec.nestedAttributes.append(idx);
    }
}

void DataSourceMetaData::gatherAttributes()
{
    aindex_t numItems = fields.ordinality();
    for (aindex_t idx = 0; idx < numItems; ++idx)
    {
        DataSourceMetaItem &item = fields.item(idx);
        if (item.flags==FVFFbeginrecord)
        {
            if (!item.tagname.get() || *item.tagname.get())
                gatherNestedAttributes(item, idx);
        }
        else if (item.isXmlAttribute())
            attributes.append(idx);
    }
}

const IntArray &DataSourceMetaData::queryAttrList(unsigned column) const
{
    return fields.item(column).nestedAttributes;
}

const IntArray &DataSourceMetaData::queryAttrList() const
{
    return attributes;
}


IFvDataSourceMetaData * DataSourceMetaData::queryChildMeta(unsigned column) const
{
    return fields.item(column).queryChildMeta();
}


IFvDataSource * DataSourceMetaData::createChildDataSource(unsigned column, unsigned len, const void * data)
{
    DataSourceMetaData * childMeta = fields.item(column).queryChildMeta();
    if (childMeta)
        return new NestedDataSource(*childMeta, len, data);
    return NULL;
}


bool DataSourceMetaData::supportsRandomSeek() const
{
    return randomIsOk;
}


void DataSourceMetaData::serialize(MemoryBuffer & buffer) const
{
    //NB: Update NullDataSourceMeta if this changes....
    buffer.append(numFieldsToIgnore);
    buffer.append(randomIsOk);
    buffer.append(hasMixedContent);
    buffer.append(isStoredFixedWidth);
    buffer.append(minRecordSize);
    buffer.append(keyedSize);
    buffer.append(maxRecordSize);

    unsigned numFields = fields.ordinality();
    buffer.append(numFields);
    for (unsigned idx=0; idx < numFields; idx++)
    {
        fields.item(idx).serialize(buffer);
    }
}

size32_t DataSourceMetaData::getRecordSize(const void *rec)
{
    if (isStoredFixedWidth)
        return minRecordSize;
    if (!rec)
        return maxRecordSize;

    const byte * data = (const byte *)rec;
    unsigned curOffset = 0;
    unsigned bitsRemaining = 0;
    unsigned max = fields.ordinality() - numVirtualFields;
    for (unsigned idx=0; idx < max; idx++)
    {
        ITypeInfo & type = *fields.item(idx).type;
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
                size = *((unsigned *)cur)*2 + sizeof(unsigned);
                break;
            case type_utf8:
                size = sizeof(unsigned) + rtlUtf8Size(*(unsigned *)cur, cur+sizeof(unsigned));
                break;
            case type_varstring:
                size = strlen((char *)cur)+1;
                break;
            case type_varunicode:
                size = (rtlUnicodeStrlen((UChar *)cur)+1)*2;
                break;
            case type_packedint:
                size = rtlGetPackedSize(cur);
                break;
            default:
                UNIMPLEMENTED;
            }
        }

        if (type.getTypeCode() == type_bitfield)
        {
            unsigned thisBits = type.getBitSize();
            if (thisBits > bitsRemaining)
            {
                size = type.queryChildType()->getSize();
                bitsRemaining = size * 8;
            }
            else
                size = 0;

            bitsRemaining -= thisBits;
        }
        else
            bitsRemaining = 0;

        curOffset += size;
    }
    return curOffset;
}


size32_t DataSourceMetaData::getFixedSize() const
{
    if (isStoredFixedWidth)
        return minRecordSize;
    return 0;
}


size32_t DataSourceMetaData::getMinRecordSize() const
{
    return minRecordSize;
}

IFvDataSourceMetaData * deserializeDataSourceMeta(MemoryBuffer & in)
{
    return new DataSourceMetaData(in);
}

//---------------------------------------------------------------------------

RowBlock::RowBlock(MemoryBuffer & _buffer, __int64 _start, __int64 _startOffset)
{
    buffer.swapWith(_buffer);
    start = _start;
    startOffset = _startOffset;
    numRows = 0;
}

RowBlock::RowBlock(__int64 _start, __int64 _startOffset)
{
    start = _start;
    startOffset = _startOffset;
    numRows = 0;
}

void RowBlock::getNextStoredOffset(__int64 & row, offset_t & offset)
{
    row = getNextRow();
    offset = startOffset + buffer.length();
}

FixedRowBlock::FixedRowBlock(MemoryBuffer & _buffer, __int64 _start, __int64 _startOffset, size32_t _fixedRecordSize) : RowBlock(_buffer, _start, _startOffset)
{
    if (_fixedRecordSize == 0) _fixedRecordSize = 1;
    fixedRecordSize = _fixedRecordSize;
    numRows = buffer.length() / fixedRecordSize;
}

const void * FixedRowBlock::fetchRow(__int64 offset, size32_t & len)
{
    __int64 maxOffset = startOffset + buffer.length();
    if (offset < startOffset || offset >= maxOffset)
        return NULL;
    len = fixedRecordSize;
    return buffer.toByteArray() + (offset - startOffset);
}

const void * FixedRowBlock::getRow(__int64 row, size32_t & len, unsigned __int64 & rowOffset)
{
    if (row < start || row >= start + numRows)
        return NULL;

    unsigned index = (unsigned)(row - start);
    unsigned blockOffset = index * fixedRecordSize;
    len = fixedRecordSize;
    rowOffset = startOffset + blockOffset;
    return buffer.toByteArray() + blockOffset;
}


VariableRowBlock::VariableRowBlock(MemoryBuffer & _buffer, __int64 _start, __int64 _startOffset, IRecordSizeEx * recordSize, bool isLast) : RowBlock(_buffer, _start, _startOffset)
{
    const char * buff = buffer.toByteArray();
    unsigned cur = 0;
    unsigned max = buffer.length();
    unsigned maxCur = max;
    if (!isLast)
        maxCur -= recordSize->getRecordSize(NULL);
    while (cur < maxCur)
    {
        rowIndex.append(cur);
        cur += recordSize->getRecordSize(max-cur, buff + cur);
        if (cur > max)
            throwError(FVERR_RowTooLarge);
    }
    buffer.setLength(cur);
    rowIndex.append(cur);
    numRows = rowIndex.ordinality()-1;
}

VariableRowBlock::VariableRowBlock(MemoryBuffer & inBuffer, __int64 _start) : RowBlock(_start, 0)
{
    inBuffer.read(numRows);
    for (unsigned row = 0; row < numRows; row++)
    {
        unsigned thisLength;
        rowIndex.append(buffer.length());
        inBuffer.read(thisLength);
        buffer.append(thisLength, inBuffer.readDirect(thisLength));
    }
    rowIndex.append(buffer.length());
}

const void * VariableRowBlock::fetchRow(__int64 offset, size32_t & len)
{
    __int64 maxOffset = startOffset + buffer.length();
    if (offset < startOffset || offset >= maxOffset)
        return NULL;

    size32_t rowOffset = (size32_t)(offset - startOffset);
    unsigned pos = rowIndex.find(rowOffset);
    if (pos == NotFound)
        return NULL;
    len = rowIndex.item(pos+1)-rowOffset;
    return buffer.toByteArray() + rowOffset;
}

const void * VariableRowBlock::getRow(__int64 row, size32_t & len, unsigned __int64 & rowOffset)
{
    if (row < start || row >= start + numRows)
        return NULL;

    unsigned index = (unsigned)(row - start);
    unsigned blockOffset = rowIndex.item(index);
    len = rowIndex.item(index+1) - blockOffset;
    rowOffset = startOffset + blockOffset;
    return buffer.toByteArray() + blockOffset;
}

//---------------------------------------------------------------------------

offset_t calculateNextOffset(const char * data, unsigned len)
{
    offset_t offset = *(offset_t *)(data + len - sizeof(offset_t) - sizeof(unsigned short));
    return offset + *(unsigned short*)(data + len - sizeof(unsigned short));
}


void FilePosFixedRowBlock::getNextStoredOffset(__int64 & row, offset_t & offset)
{
    row = start + numRows;
    offset = calculateNextOffset(buffer.toByteArray(), buffer.length());
}

void FilePosVariableRowBlock::getNextStoredOffset(__int64 & row, offset_t & offset)
{
    row = start + numRows;
    offset = calculateNextOffset(buffer.toByteArray(), buffer.length());
}


//---------------------------------------------------------------------------

static int rowCacheId;

void RowCache::addRowsOwn(RowBlock * rows)
{
    if (allRows.ordinality() == MaxBlocksCached)
        makeRoom();

    unsigned newPos = getInsertPosition(rows->getStartRow());
    allRows.add(*rows, newPos);
    ages.add(++rowCacheId, newPos);
}

bool RowCache::getCacheRow(__int64 row, RowLocation & location)
{
    unsigned numRows = allRows.ordinality();
    if (numRows == 0)
        return false;

    const RowBlock & first = allRows.item(0);
    if (row < first.getStartRow())
    {
        location.bestRow = 0;
        location.bestOffset = 0;
        return false;
    }

    unsigned best = getBestRow(row);
    ages.replace(++rowCacheId, best);
    RowBlock & cur = allRows.item(best);
    location.matchRow = cur.getRow(row, location.matchLength, location.bestOffset);
    if (location.matchRow)
        return true;
    if (location.bestRow < cur.getNextRow())
    {
        cur.getNextStoredOffset(location.bestRow, location.bestOffset);
    }
    return false;
}


//Find the rowBlock that contains the expected row.
//Return the *previous* RowBlock if no match is found.
unsigned RowCache::getBestRow(__int64 row)
{
    unsigned max = allRows.ordinality();
    int start = 0;
    int end = max;
    while (end - start > 1)
    {
        int mid = (start + end) >> 1;
        RowBlock & cur = allRows.item(mid);
        if (row >= cur.getStartRow())
            start = mid;
        else
            end = mid;
    }
    assertex(row >= allRows.item(start).getStartRow());
    assertex(start != max);
    return start;
}


unsigned RowCache::getInsertPosition(__int64 row)
{
    unsigned max = allRows.ordinality();
    int start = 0;
    int end = max;
    while (end - start > 1)
    {
        int mid = (start + end) >> 1;
        RowBlock & cur = allRows.item(mid);
        if (row >= cur.getStartRow())
            start = mid;
        else
            end = mid;
    }
    if (start != max)
    {
        if (row >= allRows.item(start).getStartRow())
            start++;
    }
    assertex(start == max || (row < allRows.item(start).getStartRow()));
    return start;
}


void RowCache::makeRoom()
{
#if 0
    //For testing the caching by throwing away random blocks
    while (allRows.ordinality() > MinBlocksCached)
    {
        unsigned index = getRandom() % allRows.ordinality();
        allRows.remove(index);
        ages.remove(index);
    }
    return;
#endif

    unsigned numToFree = allRows.ordinality() - MinBlocksCached;
    RowBlock * * oldestRow = new RowBlock * [numToFree];
    __int64 * oldestAge = new __int64[numToFree];
    unsigned numGot = 0;

    ForEachItemIn(idx, ages)
    {
        __int64 curAge = ages.item(idx);
        unsigned compare = numGot;
        while (compare != 0)
        {
            if (curAge >= oldestAge[compare-1])
                break;
            compare--;
        }
        if (compare < numToFree)
        {
            unsigned copySize = numGot - compare;
            if (numGot == numToFree)
                copySize--;         //NB: Cannot go negative because compare < numToFree, numToFree == numGot => compare < numGot

            if (copySize)
            {
                memmove(oldestAge + compare + 1, oldestAge + compare, copySize * sizeof(*oldestAge));
                memmove(oldestRow + compare + 1, oldestRow + compare, copySize * sizeof(*oldestRow));
            }
            oldestAge[compare] = curAge;
            oldestRow[compare] = &allRows.item(idx);
            if (numGot != numToFree)
                numGot++;
        }
    }

    unsigned max = allRows.ordinality();
    unsigned i;
    for (i = 0; i < max; )
    {
        for (unsigned j = 0; j < numGot; j++)
        {
            if (oldestRow[j] == &allRows.item(i))
            {
                allRows.remove(i);
                ages.remove(i);
                max--;
                goto testNext;
            }
        }
        i++;
testNext: ;
    }

    delete [] oldestRow;
    delete [] oldestAge;
}


//---------------------------------------------------------------------------

FVDataSource::FVDataSource()
{
    transformer = NULL;
    extraFieldsSize = 0;
    openCount = 0;
    appendFileposition = false;
}


FVDataSource::~FVDataSource()
{
    //ensure all refs into the dll are cleared before it is unloaded
    returnedRecordSize.clear();
}

void FVDataSource::addFileposition()
{
    appendFileposition = true;
    transformedMeta->addFileposition();     // This may modify the other metas as well!
}

void FVDataSource::copyRow(MemoryBuffer & out, const void * src, size32_t length)
{
    if (transformer)
    {
        unsigned curLen = out.length();
        size32_t maxSize = transformedMeta->getMaxRecordSize();
        void * target = out.reserve(maxSize);
        //MORE: won't cope with dynamic maxlengths
        RtlStaticRowBuilder rowBuilder(target, maxSize);
        unsigned copied = transformer(rowBuilder, (const byte *)src);
        out.rewrite(curLen + copied);
    }
    else
        out.append(length, src);
}


bool FVDataSource::fetchRow(MemoryBuffer & out, __int64 offset)
{
    if (!transformer)
        return fetchRawRow(out, offset);

    MemoryBuffer temp;
    if (fetchRowData(temp, offset))
    {
        copyRow(out, temp.toByteArray(), temp.length());
        if (appendFileposition)
            out.append(offset);
        return true;
    }
    return false;
}

bool FVDataSource::fetchRawRow(MemoryBuffer & out, __int64 offset)
{
    if (fetchRowData(out, offset))
    {
        if (appendFileposition)
            out.append(offset);
        return true;
    }
    return false;
}

bool FVDataSource::getRow(MemoryBuffer & out, __int64 row)
{
    size32_t length;
    const void * data;
    unsigned __int64 offset = 0;
    if (getRowData(row, length, data, offset))
    {
        copyRow(out, data, length);
        if (appendFileposition)
            out.append(offset);
        return true;
    }
    return false;
}

bool FVDataSource::getRawRow(MemoryBuffer & out, __int64 row)
{
    size32_t length;
    const void * data;
    unsigned __int64 offset = 0;
    if (getRowData(row, length, data, offset))
    {
        out.append(length-extraFieldsSize, data);
        if (appendFileposition)
            out.append(offset);
        return true;
    }
    return false;
}


void FVDataSource::loadDll(const char * wuid)
{
    //MORE: This code should be commoned up and available in the work unit code or something...
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid, false);

    //Plugins should already be loaded when they were registered with the ViewTransformerRegistry
    //Something like the following code could be used to check the plugin version...
#if 0
    Owned<IConstWUPluginIterator> plugins = &wu->getPlugins();
    SafePluginMap * map = NULL;
    ForEach(*plugins)
    {
        IConstWUPlugin &thisplugin = plugins->query();
        SCMStringBuffer name, version;
        thisplugin.getPluginName(name);
        thisplugin.getPluginVersion(version);
        Owned<ILoadedDllEntry> loadedPlugin = map->getPluginDll(name.str(), version.str(), true);
        if (!loadedPlugin)
            break;
    }
#endif

    Owned<IConstWUQuery> q = wu->getQuery();
    SCMStringBuffer dllname;
    q->getQueryDllName(dllname);
    // MORE - leaks....
    loadedDll.setown(queryDllServer().loadDll(dllname.str(), DllLocationAnywhere));
}



IFvDataSourceMetaData * FVDataSource::queryMetaData()
{
    return transformedMeta;
}


bool FVDataSource::setReturnedInfoFromResult()
{
    SCMStringBuffer s;
    wuResult->getResultEclSchema(s);
    returnedRecord.setown(parseQuery(s.str()));
    if (!returnedRecord)
        throw MakeStringException(ERR_FILEVIEW_FIRST+4, "Could not process result schema [%s]", s.str());

    bool isKey = false;
    bool isGrouped = false;     // this isn't strictly true...it could be true for an internal result, but no current flag to test
    returnedMeta.setown(new DataSourceMetaData(returnedRecord, 0, true, isGrouped, 0));
    transformedRecord.setown(getFileViewerRecord(returnedRecord, isKey));

    if (!transformedRecord)
    {
        //No transformations needed, so don't need to loaded the dll containing the transform functions etc.
        transformedRecord.set(returnedRecord);
        returnedRecordSize.set(returnedMeta);
    }
    else
    {
        loadDll(wuid);
        s.clear();
        wuResult->getResultRecordSizeEntry(s);
        if (s.length())
        {
            typedef IRecordSize * (* recSizeFunc)();
            recSizeFunc func = (recSizeFunc)loadedDll->getEntry(s.str());
            Owned<IRecordSize> createdRecordSize = func();
            returnedRecordSize.setown(new RecordSizeToEx(createdRecordSize));
        }

        s.clear();
        wuResult->getResultTransformerEntry(s);
        if (s.length())
            transformer = (rowTransformFunction)loadedDll->getEntry(s.str());
    }
    transformedMeta.setown(new DataSourceMetaData(transformedRecord, 0, true, isGrouped, 0));
    return (returnedRecordSize != NULL);
}

//---------------------------------------------------------------------------

__int64 PagedDataSource::numRows(bool force)
{
    if (force && (totalRows == UNKNOWN_NUM_ROWS))
    {
        //MORE: Need to go and work it out - however painful...
    }
    return totalRows;
}


bool PagedDataSource::getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset)
{
    if ((row < 0) || ((unsigned __int64)row > totalRows))
        return false;

    RowLocation location;
    loop
    {
        if (cache.getCacheRow(row, location))
        {
            length = location.matchLength;
            data = location.matchRow;
            offset = location.bestOffset;
            return true;
        }

        improveLocation(row, location);

        if (!loadBlock(location.bestRow, location.bestOffset))
            return false;
    }
}


void PagedDataSource::improveLocation(__int64 row, RowLocation & location)
{
}

//---------------------------------------------------------------------------

NestedDataSource::NestedDataSource(DataSourceMetaData & meta, unsigned len, const void * data)
{
    returnedMeta.set(&meta);
    returnedRecordSize.set(returnedMeta);
    transformedMeta.set(returnedMeta);
    totalSize = len;

    MemoryBuffer temp;
    temp.append(len, data);
    if (returnedMeta->isFixedSize())
        rows.setown(new FixedRowBlock(temp, 0, 0, returnedMeta->fixedSize()));
    else
        rows.setown(new VariableRowBlock(temp, 0, 0, returnedRecordSize, true));
}

bool NestedDataSource::init()
{
    return true;
}


__int64 NestedDataSource::numRows(bool force)
{
    return rows->getNextRow() - rows->getStartRow();
}


bool NestedDataSource::getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset)
{
    data = rows->getRow(row, length, offset);
    return (data != NULL);
}

//---------------------------------------------------------------------------

NullDataSource::NullDataSource(IHqlExpression * _record, bool _isGrouped, unsigned _keyedSize)
: meta(_record, 0, true, _isGrouped, _keyedSize)
{
}

FailureDataSource::FailureDataSource(IHqlExpression * _record, IException * _error, bool _isGrouped, unsigned _keyedSize) 
: NullDataSource(_record, _isGrouped, _keyedSize), error(_error)
{
}

//---------------------------------------------------------------------------

IHqlExpression * parseQuery(const char * text)
{
    MultiErrorReceiver errs;
    OwnedHqlExpr ret = ::parseQuery(text, &errs);
    if (errs.errCount() == 0)
        return ret.getClear();

    for (unsigned i=0; i < errs.errCount(); i++)
    {
        StringBuffer msg;
        PrintLog("%d %s", errs.item(i)->getLine(), errs.item(i)->errorMessage(msg).str());
    }
    return NULL;
}


IFvDataSourceMetaData * createMetaData(IConstWUResult * wuResult)
{
    SCMStringBuffer s;
    wuResult->getResultEclSchema(s);
    OwnedHqlExpr record = parseQuery(s.str());
    if (!record)
        throw MakeStringException(ERR_FILEVIEW_FIRST+4, "Could not process result schema [%s]", s.str());

    OwnedHqlExpr simplifiedRecord = getFileViewerRecord(record, false);
    bool isGrouped = false;     // more not sure this is strictly true...
    if (!simplifiedRecord)
        return new DataSourceMetaData(record, 0, true, isGrouped, 0);
    return new DataSourceMetaData(simplifiedRecord, 0, true, isGrouped, 0);
}
