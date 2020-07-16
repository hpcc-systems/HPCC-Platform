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

#include "platform.h"
#include <algorithm>

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jlog.hpp"

#include "csvsplitter.hpp"
#include "thorherror.h"
#include "thorxmlread.hpp"
#include "thorcommon.ipp"
#include "eclrtl.hpp"

#include "jptree.ipp"

#define XMLTAG_CONTENT "<>"

//=====================================================================================================
XmlColumnIterator::XmlColumnIterator(IPropertyTreeIterator * _iter) : iter(_iter)
{
}

IColumnProvider * XmlColumnIterator::first()
{
    if (!iter->first())
        return NULL;

    setCurrent();
    return cur;
}

IColumnProvider * XmlColumnIterator::next()
{
    if (!iter->next())
        return NULL;

    setCurrent();
    return cur;
}

void XmlColumnIterator::setCurrent()
{
    Owned<IPropertyTree> curTree = &iter->get();
    cur.setown(new XmlDatasetColumnProvider);
    cur->setRow(curTree);
}

//=====================================================================================================

static void decodeHexPairs(const char *input, unsigned inputLen, void * outData, unsigned outLen)
{
    byte * tgt = (byte *)outData;
    while (inputLen >= 2)
    {
        if (outLen-- == 0)
            return;
        byte high = hex2num(*input++);
        *tgt++ = (high << 4) | hex2num(*input++);
        inputLen -= 2;
    }
    if (outLen)
        memset(outData, 0, outLen);
}

static void decodeHexPairsX(const char *input, unsigned inputLen, void *&outData, unsigned &outLen)
{
    if (inputLen<2)
    {
        outLen = 0;
        outData = NULL;
        return;
    }
    outLen = inputLen/2;
    outData = malloc(outLen);
    char *tgt = (char *)outData;
    for (;;)
    {
        byte high = hex2num(*input++);
        *tgt++ = (high << 4) | hex2num(*input++);
        inputLen -= 2;
        if (inputLen<2) break;
    }
}

//=====================================================================================================

bool XmlDatasetColumnProvider::getBool(const char * name)
{
    return row->getPropBool(name, 0);
}

__int64 XmlDatasetColumnProvider::getInt(const char * name)
{
    return row->getPropInt64(name, 0);
}

__uint64 XmlDatasetColumnProvider::getUInt(const char * name)
{
    return readUInt(name, 0);
}

void XmlDatasetColumnProvider::getData(size32_t len, void * target, const char * name)
{
    const char *hexPairSequence = row->queryProp(name);
    if (!hexPairSequence)
        memset(target, 0, len);
    else
        decodeHexPairs(hexPairSequence, (size32_t)strlen(hexPairSequence), target, len);
}

void XmlDatasetColumnProvider::getDataX(size32_t & len, void * & target, const char * name)
{
    const char *hexPairSequence = row->queryProp(name);
    if (!hexPairSequence)
    {
        len = 0;
        target = NULL;
        return;
    }
    decodeHexPairsX(hexPairSequence, (size32_t)strlen(hexPairSequence), target, len);
}

void XmlDatasetColumnProvider::getDataRaw(size32_t len, void * target, const char * name)
{ 
    const char *hexPairSequence = row->queryProp(name);
    if (!hexPairSequence)
        memset(target, 0, len);
    else
    {
        size32_t dLen = (size32_t)strlen(hexPairSequence);
        memcpy(target, hexPairSequence, dLen);
        if (dLen < len)
            memset((byte*)target+dLen, 0, len - dLen);
    }
}

void XmlDatasetColumnProvider::getDataRawX(size32_t & len, void * & target, const char * name)
{ 
    const char *hexPairSequence = row->queryProp(name);
    if (!hexPairSequence)
    {
        len = 0;
        target = NULL;
        return;
    }
    len = (size32_t)strlen(hexPairSequence);
    target = malloc(len);
    memcpy(target, hexPairSequence, len);   
}

void XmlDatasetColumnProvider::getQString(size32_t len, char * target, const char * name)
{
    // You could argue that it should convert from UTF8 to ascii first but it's a no-op for any char that QString supports, and it's ok to be undefined for any char that it doesn't
    const char * value = row->queryProp(name);
    size32_t lenValue = value ? (size32_t)strlen(value) : 0;
    rtlStrToQStr(len, target, lenValue, value);
}

void XmlDatasetColumnProvider::getString(size32_t len, char * target, const char * name)
{
    const char * value = row->queryProp(name);
    size32_t utf8bytes = value ? (size32_t)strlen(value) : 0;
    if (utf8bytes)
        rtlUtf8ToStr(len, target, rtlUtf8Length(utf8bytes, value), value);
    else
        memset(target, ' ', len);
}

void XmlDatasetColumnProvider::getStringX(size32_t & len, char * & target, const char * name)
{
    const char * value = row->queryProp(name);
    size32_t utf8bytes = value ? (size32_t)strlen(value) : 0;
    if (utf8bytes)
        rtlUtf8ToStrX(len, target, rtlUtf8Length(utf8bytes, value), value);
    else
    {
        len = 0;
        target = NULL;
    }
}

void XmlDatasetColumnProvider::getUnicodeX(size32_t & len, UChar * & target, const char * name)
{
    const char * text = row->queryProp(name);
    if (text)
        rtlCodepageToUnicodeX(len, target, (size32_t)strlen(text), text, "utf-8");
    else
    {
        len = 0;
        target = NULL;
    }
}

void XmlDatasetColumnProvider::getUtf8X(size32_t & len, char * & target, const char * path)
{
    const char * value = row->queryProp(path);
    size32_t size = value ? (size32_t)strlen(value) : 0;
    target = (char *)malloc(size);
    memcpy_iflen(target, value, size);
    len = rtlUtf8Length(size, target);
}

bool XmlDatasetColumnProvider::getIsSetAll(const char * path)
{
    StringBuffer fullpath;
    fullpath.append(path).append("/All");
    return row->hasProp(fullpath.str());
}

IColumnProviderIterator * XmlDatasetColumnProvider::getChildIterator(const char * path)
{
    return new XmlColumnIterator(row->getElements(path));
}

bool XmlDatasetColumnProvider::readBool(const char * path, bool _default)
{
    return row->getPropBool(path, _default);
}

void XmlDatasetColumnProvider::readData(size32_t len, void * target, const char * path, size32_t _lenDefault, const void * _default)
{
    const char *hexPairSequence = row->queryProp(path);
    if (hexPairSequence)
        decodeHexPairs(hexPairSequence, (size32_t)strlen(hexPairSequence), target, len);
    else
        rtlDataToData(len, target, _lenDefault, _default);
}

void XmlDatasetColumnProvider::readDataX(size32_t & len, void * & target, const char * path, size32_t _lenDefault, const void * _default)
{
    const char *hexPairSequence = row->queryProp(path);
    if (hexPairSequence)
        decodeHexPairsX(hexPairSequence, (size32_t)strlen(hexPairSequence), target, len);
    else
        rtlStrToDataX(len, target, _lenDefault, _default);
}

void XmlDatasetColumnProvider::readDataRaw(size32_t len, void * target, const char * path, size32_t _lenDefault, const void * _default)
{
    rtlDataToData(len, target, _lenDefault, _default);
}

void XmlDatasetColumnProvider::readDataRawX(size32_t & len, void * & target, const char * path, size32_t _lenDefault, const void * _default)
{
    rtlStrToDataX(len, target, _lenDefault, _default);
}

__int64 XmlDatasetColumnProvider::readInt(const char * path, __int64 _default)
{
    return row->getPropInt64(path, _default);
}

__uint64 XmlDatasetColumnProvider::readUInt(const char * path, __uint64 _default)
{
    const char *val = row->queryProp(path);
    if (val && *val)
        return strtoull(val, nullptr, 10);
    else
        return _default;
}

void XmlDatasetColumnProvider::readQString(size32_t len, char * target, const char * path, size32_t _lenDefault, const char * _default)
{
    const char * value = row->queryProp(path);
    if (value)
        rtlStrToQStr(len, target, (size32_t)strlen(value), value);              // more: could process utf8, but characters would be lost anyway.  At worse will mean extra blanks.
    else
        rtlQStrToQStr(len, target, _lenDefault, _default);
}

void XmlDatasetColumnProvider::readString(size32_t len, char * target, const char * path, size32_t _lenDefault, const char * _default)
{
    const char * value = row->queryProp(path);
    if (value)
        rtlUtf8ToStr(len, target, rtlUtf8Length((size32_t)strlen(value), value), value);
    else
        rtlStrToStr(len, target, _lenDefault, _default);
}

void XmlDatasetColumnProvider::readStringX(size32_t & len, char * & target, const char * path, size32_t _lenDefault, const char * _default)
{
    const char * value = row->queryProp(path);
    if (value)
        rtlUtf8ToStrX(len, target, rtlUtf8Length((size32_t)strlen(value), value), value);
    else
        rtlStrToStrX(len, target, _lenDefault, _default);
}

void XmlDatasetColumnProvider::readUnicodeX(size32_t & len, UChar * & target, const char * path, size32_t _lenDefault, const UChar * _default)
{
    const char * text = row->queryProp(path);
    if (text)
        rtlCodepageToUnicodeX(len, target, (size32_t)strlen(text), text, "utf-8");
    else
        rtlUnicodeToUnicodeX(len, target, _lenDefault, _default);
}

bool XmlDatasetColumnProvider::readIsSetAll(const char * path, bool _default)
{
    if (row->hasProp(path))
        return getIsSetAll(path);
    return _default;
}

void XmlDatasetColumnProvider::readUtf8X(size32_t & len, char * & target, const char * path, size32_t _lenDefault, const char * _default)
{
    const char * value = row->queryProp(path);
    if (value)
        rtlUtf8ToUtf8X(len, target, rtlUtf8Length((size32_t)strlen(value), value), value);
    else
        rtlUtf8ToUtf8X(len, target, _lenDefault, _default);
}

const char *XmlDatasetColumnProvider::readRaw(const char * path, size32_t &sz) const
{
    const char *value = row->queryProp(path);
    sz = value ? strlen(value) : 0;
    return value;
}

//=====================================================================================================

bool XmlSetColumnProvider::getBool(const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    return row->getPropBool(NULL, 0);
}

__int64 XmlSetColumnProvider::getInt(const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    return row->getPropInt64(NULL, 0);
}

__uint64 XmlSetColumnProvider::getUInt(const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    //MORE: Note nullptr is passed in all of these XmlSetColumnProvider::get functions
    //The code generator incorrectly generates "value" as the name to read.  Really it should be fixed there.
    return readUInt(nullptr, 0);
}

void XmlSetColumnProvider::getData(size32_t len, void * target, const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char *hexPairSequence = row->queryProp(NULL);
    if (!hexPairSequence)
        memset(target, 0, len);
    else
        decodeHexPairs(hexPairSequence, (size32_t)strlen(hexPairSequence), target, len);
}

void XmlSetColumnProvider::getDataX(size32_t & len, void * & target, const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char *hexPairSequence = row->queryProp(NULL);
    if (!hexPairSequence)
    {
        len = 0;
        target = NULL;
        return;
    }
    decodeHexPairsX(hexPairSequence, (size32_t)strlen(hexPairSequence), target, len);
}

void XmlSetColumnProvider::getDataRaw(size32_t len, void * target, const char * name)
{ 
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char *hexPairSequence = row->queryProp(NULL);
    if (!hexPairSequence)
        memset(target, 0, len);
    else
    {
        size32_t dLen = strlen(hexPairSequence);
        memcpy(target, hexPairSequence, dLen);
        if (dLen < len)
            memset((byte*)target+dLen, 0, len - dLen);
    }
}

void XmlSetColumnProvider::getDataRawX(size32_t & len, void * & target, const char * name)
{ 
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char *hexPairSequence = row->queryProp(NULL);
    if (!hexPairSequence)
    {
        len = 0;
        target = NULL;
        return;
    }
    len = (size32_t)strlen(hexPairSequence);
    target = malloc(len);
    memcpy(target, hexPairSequence, len);   
}

void XmlSetColumnProvider::getQString(size32_t len, char * target, const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char * value = row->queryProp(NULL);
    unsigned lenValue = value ? (size32_t)strlen(value) : 0;
    rtlStrToQStr(len, target, lenValue, value);
}

void XmlSetColumnProvider::getString(size32_t len, char * target, const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char * value = row->queryProp(NULL);
    if (value)
        rtlVStrToStr(len, target, value);
    else
        memset(target, ' ', len);
}

void XmlSetColumnProvider::getStringX(size32_t & len, char * & target, const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char * value = row->queryProp(NULL);
    len = value ? (size32_t)strlen(value) : 0;
    target = (char *)malloc(len);
    memcpy_iflen(target, value, len);
    //MORE: utf8->ascii?
}

void XmlSetColumnProvider::getUnicodeX(size32_t & len, UChar * & target, const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char * text = row->queryProp(NULL);
    if (text)
        rtlCodepageToUnicodeX(len, target, (size32_t)strlen(text), text, "utf-8");
    else
    {
        len = 0;
        target = NULL;
    }
}

void XmlSetColumnProvider::getUtf8X(size32_t & len, char * & target, const char * name)
{
#ifdef _DEBUG
    assertex(stricmp(name, "value")==0);
#endif
    const char * value = row->queryProp(NULL);
    size32_t size = value ? (size32_t)strlen(value) : 0;
    target = (char *)malloc(size);
    memcpy_iflen(target, value, size);
    len = rtlUtf8Length(size, value);
}


bool XmlSetColumnProvider::getIsSetAll(const char * path)
{
    UNIMPLEMENTED;
    StringBuffer fullpath;
    fullpath.append(path).append("/All");
    return row->hasProp(fullpath.str());
}

IColumnProviderIterator * XmlSetColumnProvider::getChildIterator(const char * path)
{
    UNIMPLEMENTED;
    return new XmlColumnIterator(row->getElements(path));
}

bool XmlSetColumnProvider::readBool(const char * path, bool _default)
{
    return row->getPropBool(NULL, _default);
}

void XmlSetColumnProvider::readData(size32_t len, void * target, const char * path, size32_t _lenDefault, const void * _default)
{
    const char *hexPairSequence = row->queryProp(NULL);
    if (hexPairSequence)
        decodeHexPairs(hexPairSequence, (size32_t)strlen(hexPairSequence), target, len);
    else
        rtlDataToData(len, target, _lenDefault, _default);
}

void XmlSetColumnProvider::readDataX(size32_t & len, void * & target, const char * path, size32_t _lenDefault, const void * _default)
{
    const char *hexPairSequence = row->queryProp(NULL);
    if (hexPairSequence)
        decodeHexPairsX(hexPairSequence, (size32_t)strlen(hexPairSequence), target, len);
    else
        rtlStrToDataX(len, target, _lenDefault, _default);
}

void XmlSetColumnProvider::readDataRaw(size32_t len, void * target, const char * path, size32_t _lenDefault, const void * _default)
{ 
    rtlDataToData(len, target, _lenDefault, _default);
}

void XmlSetColumnProvider::readDataRawX(size32_t & len, void * & target, const char * path, size32_t _lenDefault, const void * _default)
{ 
    rtlDataToData(len, target, _lenDefault, _default);
}

__int64 XmlSetColumnProvider::readInt(const char * path, __int64 _default)
{
    return row->getPropInt64(NULL, _default);
}

__uint64 XmlSetColumnProvider::readUInt(const char * path, __uint64 _default)
{
    const char *val = row->queryProp(path);
    if (val && *val)
        return strtoull(val, nullptr, 10);
    else
        return _default;
}

void XmlSetColumnProvider::readQString(size32_t len, char * target, const char * path, size32_t _lenDefault, const char * _default)
{
    const char * value = row->queryProp(NULL);
    if (value)
        rtlStrToQStr(len, target, (size32_t)strlen(value), value);              // more: could process utf8, but characters would be lost anyway.  At worse will mean extra blanks.
    else
        rtlQStrToQStr(len, target, _lenDefault, _default);
}

void XmlSetColumnProvider::readString(size32_t len, char * target, const char * path, size32_t _lenDefault, const char * _default)
{
    const char * value = row->queryProp(NULL);
    if (value)
        rtlUtf8ToStr(len, target, rtlUtf8Length((size32_t)strlen(value), value), value);
    else
        rtlStrToStr(len, target, _lenDefault, _default);
}

void XmlSetColumnProvider::readStringX(size32_t & len, char * & target, const char * path, size32_t _lenDefault, const char * _default)
{
    const char * value = row->queryProp(NULL);
    if (value)
        rtlUtf8ToStrX(len, target, rtlUtf8Length((size32_t)strlen(value), value), value);
    else
        rtlStrToStrX(len, target, _lenDefault, _default);
}

void XmlSetColumnProvider::readUnicodeX(size32_t & len, UChar * & target, const char * path, size32_t _lenDefault, const UChar * _default)
{
    const char * text = row->queryProp(NULL);
    if (text)
        rtlCodepageToUnicodeX(len, target, (size32_t)strlen(text), text, "utf-8");
    else
        rtlUnicodeToUnicodeX(len, target, _lenDefault, _default);
}

bool XmlSetColumnProvider::readIsSetAll(const char * path, bool _default)
{
    throwUnexpected();
    if (row->hasProp(NULL))
        return getIsSetAll(path);
    return _default;
}

void XmlSetColumnProvider::readUtf8X(size32_t & len, char * & target, const char * path, size32_t _lenDefault, const char * _default)
{
    const char * value = row->queryProp(NULL);
    if (value)
        rtlUtf8ToUtf8X(len, target, rtlUtf8Length((size32_t)strlen(value), value), value);
    else
        rtlUtf8ToUtf8X(len, target, _lenDefault, _default);
}

IDataVal & CXmlToRawTransformer::transform(IDataVal & result, size32_t len, const void * text, bool isDataSet)
{
    // MORE - should redo using a pull parser sometime
    Owned<IPropertyTree> root = createPTreeFromXMLString(len, (const char *)text, ipt_fast, xmlReadFlags);
    return transformTree(result, *root, isDataSet);
}

IDataVal & CXmlToRawTransformer::transformTree(IDataVal & result, IPropertyTree &root, bool isDataSet)
{
    unsigned minRecordSize = rowTransformer->queryRecordSize()->getMinRecordSize();
    Owned <XmlColumnProvider> columns;
    Owned<IPropertyTreeIterator> rows;
    StringBuffer decodedXML;
    Owned<IPropertyTree> decodedTree;
    MemoryBuffer raw;
    size32_t curLength = 0;
    if (isDataSet)
    {
        columns.setown(new XmlDatasetColumnProvider);
        if (root.hasProp("Row"))
            rows.setown(root.getElements("Row"));
        else
        {
            // HACK for Gordon to work around WSDL issues
            const char *body = root.queryProp(NULL);
            if (body)
            {
                while(isspace(*body))
                    body++;
                if (strncmp(body, "<Row", 4)==0)
                {
                    try
                    {
                        decodedXML.append("<root>").append(body).append("</root>");
                        decodedTree.setown(createPTreeFromXMLString(decodedXML.str(), ipt_caseInsensitive|ipt_fast));
                        rows.setown(decodedTree->getElements("Row"));
                    }
                    catch (IException *E)
                    {
                        EXCLOG(E);
                        E->Release();
                    }
                    catch (...)
                    {
                        ERRLOG(0, "Unexpected exception decoding XML for dataset");
                    }
                }
            }
        }
    }
    else
    {
        columns.setown(new XmlSetColumnProvider);
        rows.setown(root.getElements("string"));
        ForEach(*rows)
        {
            columns->setRow(&rows->query());
            NullDiskCallback dummyCallback;
            MemoryBufferBuilder rowBuilder(raw, minRecordSize);
            size32_t thisSize = rowTransformer->transform(rowBuilder, columns, &dummyCallback);
            curLength += thisSize;
            rowBuilder.finishRow(thisSize);
        }
        rows.setown(root.getElements("Item"));
    }

    if (rows)
    {
        ForEach(*rows)
        {
            columns->setRow(&rows->query());
            NullDiskCallback dummyCallback;
            MemoryBufferBuilder rowBuilder(raw, minRecordSize);
            size32_t thisSize = rowTransformer->transform(rowBuilder, columns, &dummyCallback);
            curLength += thisSize;
            rowBuilder.finishRow(thisSize);
        }
    }
    result.setLen(raw.toByteArray(), curLength);
    return result;
}

size32_t createRowFromXml(ARowBuilder & rowBuilder, size32_t size, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    Owned<IPropertyTree> root = createPTreeFromXMLString(size, utf8, ipt_fast, stripWhitespace ? ptr_ignoreWhiteSpace : ptr_none);
    if (!root)
    {
        throwError(THORCERR_InvalidXmlFromXml);
        return 0;
    }
    Owned <XmlColumnProvider> columns = new XmlDatasetColumnProvider;
    columns->setRow(root);
    NullDiskCallback dummyCallback;
    return xmlTransformer->transform(rowBuilder, columns, &dummyCallback);
}

const void * createRowFromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    RtlDynamicRowBuilder rowBuilder(rowAllocator);
    size32_t newSize = createRowFromXml(rowBuilder, rtlUtf8Size(len, utf8), utf8, xmlTransformer, stripWhitespace);
    return rowBuilder.finalizeRowClear(newSize);
}

size32_t createRowFromJson(ARowBuilder & rowBuilder, size32_t size, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    Owned<IPropertyTree> root = createPTreeFromJSONString(size, utf8, ipt_fast, stripWhitespace ? ptr_ignoreWhiteSpace : ptr_none);
    if (!root)
    {
        throwError(THORCERR_InvalidJsonFromJson);
        return 0;
    }
    Owned <XmlColumnProvider> columns = new XmlDatasetColumnProvider;
    columns->setRow(root);
    NullDiskCallback dummyCallback;
    return xmlTransformer->transform(rowBuilder, columns, &dummyCallback);
}

const void * createRowFromJson(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    RtlDynamicRowBuilder rowBuilder(rowAllocator);
    size32_t newSize = createRowFromJson(rowBuilder, rtlUtf8Size(len, utf8), utf8, xmlTransformer, stripWhitespace);
    return rowBuilder.finalizeRowClear(newSize);
}

//=====================================================================================================

IDataVal & CCsvToRawTransformer::transform(IDataVal & result, size32_t len, const void * text, bool isDataSet)
{
    CSVSplitter csvSplitter;

    csvSplitter.init(rowTransformer->getMaxColumns(), rowTransformer->queryCsvParameters(), NULL, NULL, NULL, NULL);

    size32_t minRecordSize = rowTransformer->queryRecordSize()->getMinRecordSize();
    const byte *finger = (const byte *) text;
    MemoryBuffer raw;
    size32_t curLength = 0;
    while (len)
    {
        unsigned thisLineLength = csvSplitter.splitLine(len, finger);
        finger += thisLineLength;
        len -= thisLineLength;

        MemoryBufferBuilder rowBuilder(raw, minRecordSize);
        unsigned thisSize = rowTransformer->transform(rowBuilder, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData(), 0);
        curLength += thisSize;
        rowBuilder.finishRow(thisSize);
    }
    result.setLen(raw.toByteArray(), curLength);
    return result;
}

//=====================================================================================================

extern thorhelper_decl IXmlToRawTransformer * createXmlRawTransformer(IXmlToRowTransformer * xmlTransformer, PTreeReaderOptions xmlReadFlags)
{
    if (xmlTransformer)
        return new CXmlToRawTransformer(*xmlTransformer, xmlReadFlags);
    return NULL;
}

extern thorhelper_decl ICsvToRawTransformer * createCsvRawTransformer(ICsvToRowTransformer * csvTransformer)
{
    if (csvTransformer)
        return new CCsvToRawTransformer(*csvTransformer);
    return NULL;
}

bool isContentXPath(const char *xpath, StringBuffer &head)
{
    if (xpath)
    {
        unsigned l = (size32_t)strlen(xpath);
        if (l >= 2)
        {
            const char *x = xpath+l-2;
            if ((x[0] == '<') && (x[1] == '>'))
            {
                head.append((size32_t)(x-xpath), xpath);
                return true;
            }
        }
    }
    return false;
}

class CXPath
{
    int topQualifier;
    BoolArray simpleQualifier;
    StringArray nodes, qualifierStack;
    StringAttr xpathstr;

    bool testForSimpleQualifier(const char *qualifier)
    {
        // first char always '['
        return ('@' == qualifier[1]);
    }
public:
    CXPath(const char *path, bool ignoreNameSpaces)
    {
        topQualifier = -1;
        if (!path) return;
        xpathstr.set(path);
        if (path && '/'==*path)
        {
            if ('/' == *(path+1))
                throw MakeStringException(0, "// unsupported here");
            path++;
        }
        for (;;)
        {
            const char *startQ = strchr(path, '[');
            const char *nextSep;
            for (;;)
            {
                nextSep = strchr(path, '/');
                if (startQ && (!nextSep || startQ < nextSep))
                    break;

                StringAttr node;
                unsigned l = nextSep ? (size32_t)(nextSep-path) : (size32_t)strlen(path);
                if (!l) break;
                if (ignoreNameSpaces)
                {
                    const char *colon = path;
                    const char *end = path+l+1;
                    do
                    {
                        if (':' == *colon++)
                        {
                            l -= colon-path;
                            path = colon;
                            break;
                        }

                    }
                    while (colon != end);
                }
                StringBuffer wildRemoved;
                node.set(path, l);
                const char *c = node.get();
                while (*c) { if ('*' != *c) wildRemoved.append(*c); c++; }
                if (wildRemoved.length() && !validateXMLTag(wildRemoved.str()))
                    throw MakeStringException(0, "Invalid node syntax %s in path %s", node.get(), path);
                nodes.append(node);
                qualifierStack.append(""); // no qualifier for this segment.
                simpleQualifier.append(true); // not used
                if (!nextSep) break;
                path = nextSep+1;
            }
            if (!nextSep && !startQ)
                break;

            const char *endQ = strchr(startQ, ']'); // escaped '[]' chars??
            assertex(endQ);
            unsigned l=startQ-path;
            if (ignoreNameSpaces)
            {
                const char *colon = path;
                const char *end = path+l+1;
                do
                {
                    if (':' == *colon++)
                    {
                        l -= colon-path;
                        path = colon;
                        break;
                    }

                }
                while (colon != end);
            }
            StringAttr node(path, l);
            nodes.append(node);

            StringAttr qualifier(startQ, endQ-startQ+1);
            qualifierStack.append(qualifier);
            bool simple = testForSimpleQualifier(qualifier);
            simpleQualifier.append(simple);
            if (-1 == topQualifier && !simple) topQualifier = qualifierStack.ordinality()-1;
            path = nextSep+1;
            if (!nextSep) break;
        }
    }
    bool toQualify(unsigned which, bool simple)
    {
        return (which < queryDepth() && *qualifierStack.item(which) && simple==querySimpleQualifier(which));
    }

    inline unsigned queryDepth()
    {
        return nodes.ordinality();
    }
    inline const char *queryNode(unsigned which)
    {
        return nodes.item(which);
    }
    inline bool querySimpleQualifier(unsigned which)
    {
        return simpleQualifier.item(which);
    }
    bool match(unsigned level, const char *tag)
    {
        const char *nodeTag = queryNode(level);
        if (strchr(nodeTag, '*'))
            return WildMatch(tag, strlen(tag), nodeTag, strlen(nodeTag), false);
        else
            return (0 == strcmp(nodeTag, tag));
    }
    bool qualify(IPropertyTree &tree, unsigned depth)
    {
        const char *qualifier = qualifierStack.item(depth);
        if (qualifier && '\0' != *qualifier)
        {
            const char *q = qualifier;
            bool numeric = true;
            for (;;)
            {
                if ('\0' == *q) break;
                else if (!isdigit(*q)) { numeric = false; break; }
                else q++;
            }
            if (numeric) throw MakeStringException(0, "Unsupported index qualifier: %s", qualifier);
            Owned<IPropertyTreeIterator> matchIter = tree.getElements(qualifier);
            if (!matchIter->first())
                return false;
        }
        return true;
    }
    inline int queryHighestQualifier() { return topQualifier; }
    const char *queryXPathStr() { return xpathstr; }
};

class CProspectiveMatch : public CInterface
{
public:
    CProspectiveMatch(IPropertyTree *_parent, IPropertyTree *_node, MemoryBuffer *_content=NULL) : parent(_parent), node(_node), content(_content) { }
    ~CProspectiveMatch() { if (content) delete content; }
    IPropertyTree *parent, *node;
    MemoryBuffer *content;
};
typedef CIArrayOf<CProspectiveMatch> CProcespectiveMatchArray;
class CParseStackInfo : public CInterface
{
public:
    CParseStackInfo() : keep(false), nodeMade(false), keptForQualifier(false), iPTMade(NULL), startOffset(0), prospectiveMatches(NULL) { }
    ~CParseStackInfo()
    {
        if (prospectiveMatches)
            delete prospectiveMatches;
    }
    inline void reset()
    {
        keep = nodeMade = keptForQualifier = false;
        startOffset = 0;
        if (prospectiveMatches)
            prospectiveMatches->kill();
        iPTMade = NULL;
    }
    bool keep, nodeMade, keptForQualifier;
    offset_t startOffset;
    IPropertyTree *iPTMade;
    CProcespectiveMatchArray *prospectiveMatches;
};

class CMarkReadBase : public CInterface
{
public:
    virtual void reset() = 0;
    virtual void mark(offset_t offset) = 0;
    virtual void getMarkTo(offset_t offset, MemoryBuffer &mb) = 0;
    virtual void closeMark() = 0;
};

class CMarkRead : public CMarkReadBase
{
    const void *buffer;
    offset_t startOffset;
    unsigned bufLen;
    bool marking;
public:
    CMarkRead(const void *_buffer, unsigned _bufLen) : buffer(_buffer), bufLen(_bufLen)
    {
        reset();
    }
    virtual void reset()
    {
        marking = false;
        startOffset = 0;
    }
    virtual void mark(offset_t offset)
    {
        assertex(!marking);
        marking = true;
        if (offset >= bufLen)
            throw MakeStringException(0, "start offset past end of input string");
        startOffset = offset;
    }
    virtual void getMarkTo(offset_t offset, MemoryBuffer &mb)
    {
        assertex(marking);
        marking = true;
        if (offset < startOffset)
            throw MakeStringException(0, "end offset proceeds start offset");
        if (offset > bufLen)
            throw MakeStringException(0, "end offset past end of input string");
        mb.append((size32_t)(offset-startOffset), ((char*)buffer)+startOffset);
        marking = false;
    }
    virtual void closeMark()
    {
        marking = false;
    }
};

class CMarkReadStream : implements ISimpleReadStream, public CMarkReadBase
{
    ISimpleReadStream &stream;
    offset_t readOffset, markingOffset;
    byte *buf, *bufPtr, *bufOther, *bufLowerHalf, *bufUpperHalf;
    size32_t remaining, bufSize;
    MemoryBuffer markBuffer;
    bool marking;
public:
    IMPLEMENT_IINTERFACE;

    CMarkReadStream(ISimpleReadStream &_stream) : stream(_stream), readOffset(0)
    {
        bufSize = 0x8000/2;
        bufLowerHalf = buf = (byte *)malloc(bufSize*2);
        bufUpperHalf = bufLowerHalf+bufSize;
        reset();
    }
    ~CMarkReadStream()
    {
        free(bufLowerHalf); // pointer to whole buf in fact
        stream.Release();
    }

    virtual void reset()
    {
        remaining = 0;
        buf = bufPtr = bufLowerHalf;
        bufOther = NULL;
        readOffset = markingOffset = 0;
        marking = false;
        markBuffer.resetBuffer();
    }
    virtual void mark(offset_t offset)
    {
        assertex(!marking);
        marking=true;
        markingOffset = offset;
        offset_t from = readOffset-(bufPtr-buf);
        if (offset < from)
        {
            if (!bufOther)
                throw MakeStringException(0, "Not enough buffered to mark!");
            from -= bufSize;
            if (offset < from)
                throw MakeStringException(0, "Not enough buffered to mark!");
            size32_t a = (size32_t)(offset-from);
            markBuffer.append(bufSize-a, bufOther+a);
        }
    }
    virtual void getMarkTo(offset_t offset, MemoryBuffer &mb)
    {
        assertex(marking);
        size32_t markSize = (size32_t)(offset-markingOffset);
        int d = markSize-markBuffer.length();
        if (d < 0)
            markBuffer.setLength(markSize);
        else if (d > 0)
        {
            offset_t from = readOffset-(bufPtr-buf);
            size32_t o = 0;
            if (markingOffset>from)
                o = (size32_t)(markingOffset-from);
            markBuffer.append(d, buf+o);
        }
        mb.clear();
        mb.swapWith(markBuffer);
        marking = false;
    }
    virtual void closeMark()
    {
        if (marking)
        {
            markBuffer.clear();
            marking = false;
        }
    }

// ISimpleReadStream
    virtual size32_t read(size32_t len, void * data)
    {
        unsigned r = 0;
        if (!remaining)
        {
            size32_t bufSpace = bufSize-(bufPtr-buf);
            if (bufSpace)
            {
                remaining = stream.read(bufSpace, bufPtr);
                if (remaining)
                {
                    bufSpace -= remaining;
                    r = std::min(len, remaining);
                    memcpy(data, bufPtr, r);
                    remaining -= r;
                    len -= r;
                    bufPtr += r;
                    data = (byte *)data + r;
                    readOffset += r;
                }
                else
                    return 0;
            }
            if (!bufSpace && !remaining)
            {
                if (marking && bufOther)
                {
                    offset_t from = readOffset-(bufPtr-buf);
                    int d = (int)(markingOffset-from);
                    if (d>0)
                        markBuffer.append(bufSize-d, buf+d);
                    else
                        markBuffer.append(bufSize, buf);
                }
                if (buf==bufLowerHalf)
                {
                    buf = bufUpperHalf;
                    bufOther = bufLowerHalf;
                }
                else
                {
                    buf = bufLowerHalf;
                    bufOther = bufUpperHalf;
                }
                bufPtr = buf;
            }
            if (!len) return r;
            if (!remaining)
            {
                remaining = stream.read(bufSize, buf);
                if (!remaining)
                    return r;
            }
        }
        unsigned r2 = std::min(len, remaining);
        memcpy(data, bufPtr, r2);
        remaining -= r2;
        bufPtr += r2;
        readOffset += r2;
        return r + r2;
    }
};

// could contain a IPT, but convenient and efficient to derive impl.
class CPTreeWithOffsets : public LocalPTree
{
public:
    CPTreeWithOffsets(const char *name) : LocalPTree(name) { startOffset = endOffset = 0; }

    offset_t startOffset, endOffset;
};

class COffsetNodeCreator : implements IPTreeNodeCreator, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    COffsetNodeCreator() { }
    virtual IPropertyTree *create(const char *tag) { return new CPTreeWithOffsets(tag); }
};

class thorhelper_decl CColumnIterator : implements IColumnProviderIterator, public CInterface
{
    Linked<IColumnProvider> parent;
    Linked<IPropertyTree> root, matchNode;
    MemoryBuffer * contentMb;
    offset_t contentStartOffset;
    void *utf8Translator;
    Linked<IPropertyTreeIterator> iter;
    Owned<IColumnProvider> cur;
    StringAttr xpath;

public:
    CColumnIterator(IColumnProvider *_parent, void *_utf8Translator, IPropertyTree *_root, IPropertyTree *_matchNode, IPropertyTreeIterator * _iter, MemoryBuffer *_contentMb, offset_t _contentStartOffset, const char *_xpath) : parent(_parent), root(_root), matchNode(_matchNode), iter(_iter), utf8Translator(_utf8Translator), xpath(_xpath), contentStartOffset(_contentStartOffset) { contentMb = _contentMb; }
    IMPLEMENT_IINTERFACE;

    IColumnProvider * first()
    {
        if (!iter->first())
            return NULL;

        setCurrent();
        return cur;
    }

    IColumnProvider * next()
    {
        if (!iter->next())
            return NULL;

        setCurrent();
        return cur;
    }

    void setCurrent();
};

class CColumnProvider : implements IColumnProvider, public CInterface
{
    Linked<IPropertyTree> root, node;
    MemoryBuffer contentMb;
    bool content;
    offset_t contentStartOffset;
    void *utf8Translator;
    CriticalSection crit;
    MemoryBuffer tmpMb;
    MemoryBuffer sharedResult;
    StringAttr xpath;

    void cnv2Latin1(unsigned length, const void *data, MemoryBuffer &mb)
    {
        void *target = mb.reserveTruncate(length);
        if (length == 0)
            return;
        bool f;
        unsigned rl = rtlCodepageConvert(utf8Translator, length, (char *)target, length, (const char *)data, f);
        if (f)
        {
            StringBuffer errMsg("Failure translating utf-8, matching element '");
            errMsg.append(xpath).append("' data: '");
            if (length>100)
            {
                appendDataAsHex(errMsg, 100, data);
                errMsg.append("<TRUNCATED>");
            }
            else
                appendDataAsHex(errMsg, length, data);
            errMsg.append("'");
            throw MakeStringExceptionDirect(0, errMsg.str());
        } else if (length > rl)
            mb.setLength(rl);
    }

public:
    IMPLEMENT_IINTERFACE;

    CColumnProvider(void *_utf8Translator, IPropertyTree *_root, IPropertyTree *_node, MemoryBuffer *_contentMb, bool ownContent, offset_t _contentStartOffset, const char *_xpath) : root(_root), node(_node), utf8Translator(_utf8Translator), contentStartOffset(_contentStartOffset), xpath(_xpath)
    {
        if (_contentMb)
        {
            content = true;
            if (ownContent)
                contentMb.swapWith(*_contentMb);
            else
                contentMb.setBuffer(_contentMb->length(), (void *)_contentMb->toByteArray());
        }
        else
            content = false;
    }
    bool contentRequest(const char *path, size32_t &offset, size32_t &length)
    {
        StringBuffer subPath;
        if (isContentXPath(path, subPath))
        {
            assertex(content);
            if (subPath.length())
            {
                if ('/' == *path && '/' != *(path+1))
                    throw MakeStringException(0, "Cannot extract xml text from absolute path specification: %s", path);
                CPTreeWithOffsets *subTree = (CPTreeWithOffsets *)node->queryPropTree(subPath.str());
                if (subTree)
                {
                    offset = (size32_t)(subTree->startOffset-contentStartOffset);
                    length = (size32_t)(subTree->endOffset-subTree->startOffset);
                }
                else
                {
                    offset = 0;
                    length = 0;
                }
            }
            else
            {
                CPTreeWithOffsets *_node = (CPTreeWithOffsets *)node.get();
                if (contentStartOffset != _node->startOffset)
                { // must be child
                    offset = (size32_t)(_node->startOffset-contentStartOffset);
                    length = (size32_t)(_node->endOffset-_node->startOffset);
                }
                else
                {
                    offset = 0;
                    length = contentMb.length();
                }
            }
            return true;
        }
        return false;
    }
    inline bool hasProp(const char * path)
    {
        if (path && '/' == *path && '/' != *(path+1))
            return root->hasProp(path+1);
        else 
            return node->hasProp(path);
    }
    inline const char * queryProp(const char * path)
    {
        if (path && '/' == *path && '/' != *(path+1))
            return root->queryProp(path+1);
        else 
            return node->queryProp(path);
    }
    inline bool getPropBin(const char * path, MemoryBuffer & mb)
    {
        if (path && '/' == *path && '/' != *(path+1)) 
            return root->getPropBin(path+1, mb);
        else 
            return node->getPropBin(path, mb);
    }

// IColumnProvider
    void getData(size32_t len, void * data, const char * path)
    {
        readData(len, data, path, 0, NULL);
    }
    void getDataX(size32_t & len, void * & data, const char * path)
    {
        readDataX(len, data, path, 0, NULL);
    }
    void getDataRaw(size32_t len, void * data, const char * path)
    { 
        readDataRaw(len, data, path, 0, NULL);
    }
    void getDataRawX(size32_t & len, void * & data, const char * path)
    { 
        readDataRawX(len, data, path, 0, NULL);
    }
    bool getBool(const char * path)
    {
        return readBool(path, false);
    }
    __int64 getInt(const char * path)
    {
        return readInt(path, 0);
    }
    __uint64 getUInt(const char * path)
    {
        return readUInt(path, 0);
    }
    void getQString(size32_t len, char * text, const char * path)
    {
        readQString(len, text, path, 0, NULL);
    }
    void getString(size32_t len, char * text, const char * path)
    {
        readString(len, text, path, 0, NULL);
    }
    void getStringX(size32_t & len, char * & text, const char * path)
    {
        readStringX(len, text, path, 0, NULL);
    }
    void getUnicodeX(size32_t & len, UChar * & text, const char * path)
    {
        readUnicodeX(len, text, path, 0, NULL);
    }
    void getUtf8X(size32_t & len, char * & text, const char * path)
    {
        readUtf8X(len, text, path, 0, NULL);
    }
    bool getIsSetAll(const char * path)
    {
        return readIsSetAll(path, false);
    }
    IColumnProviderIterator * getChildIterator(const char * path)
    {
        Owned<IPropertyTreeIterator> iter;
        if (path && '/' == *path && '/' != *(path+1)) 
            iter.setown(root->getElements(path+1));
        else
            iter.setown(node->getElements(path));
        return new CColumnIterator(this, utf8Translator, root, node, iter, content ? &contentMb : NULL, contentStartOffset, xpath);
    }

    //

    virtual void readData(size32_t len, void * data, const char * path, size32_t _lenDefault, const void * _default)
    {
        CriticalBlock b(crit);
        sharedResult.clear();
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
        {
            cnv2Latin1(length, contentMb.toByteArray()+offset, sharedResult);
        }
        else
        {
            if (!getPropBin(path, tmpMb.clear()))
            { 
                rtlStrToData(len, data, _lenDefault, _default);
                return; 
            }
            cnv2Latin1(tmpMb.length(), tmpMb.toByteArray(), sharedResult);
        }
        decodeHexPairs((const char *)sharedResult.toByteArray(), sharedResult.length(), data, len);
    }
    virtual void readDataX(size32_t & len, void * & data, const char * path, size32_t _lenDefault, const void * _default)
    {
        CriticalBlock b(crit);
        sharedResult.clear();
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
        {
            cnv2Latin1(length, contentMb.toByteArray()+offset, sharedResult);
        }
        else
        {
            if (!getPropBin(path, tmpMb.clear()))
            { 
                rtlStrToDataX(len, data, _lenDefault, _default);
                return; 
            }
            cnv2Latin1(tmpMb.length(), tmpMb.toByteArray(), sharedResult);
        }
        decodeHexPairsX((const char *)sharedResult.toByteArray(), sharedResult.length(), data, len);
    }
    virtual void readDataRaw(size32_t len, void * data, const char * path, size32_t _lenDefault, const void * _default)
    {
        CriticalBlock b(crit);
        sharedResult.clear();
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
        {
            cnv2Latin1(length, contentMb.toByteArray()+offset, sharedResult);
        }
        else
        {
            if (!getPropBin(path, tmpMb.clear()))
            { 
                rtlStrToData(len, data, _lenDefault, _default);
                return; 
            }
        }
        memcpy(data, sharedResult.toByteArray(), sharedResult.length());
        if (len < sharedResult.length())
            memset((byte*)data + sharedResult.length(), 0, len-sharedResult.length());
    }
    virtual void readDataRawX(size32_t & len, void * & data, const char * path, size32_t _lenDefault, const void * _default)
    {
        CriticalBlock b(crit);
        sharedResult.clear();
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
        {
            cnv2Latin1(length, contentMb.toByteArray()+offset, sharedResult);
        }
        else
        {
            if (!getPropBin(path, tmpMb.clear()))
            { 
                rtlStrToDataX(len, data, _lenDefault, _default);
                return; 
            }
        }
        len = tmpMb.length();
        if (len)
        {
            data = malloc(len);
            memcpy(data, tmpMb.toByteArray(), len);
        }
        else
            data = NULL;
    }
    virtual bool readBool(const char * path, bool _default)
    {
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
            throw MakeStringException(0, "Attempting to extract xml content text as boolean");

        const char *str = queryProp(path);
        if (!str) return _default;
        return strToBool(str);
    }
    virtual __int64 readInt(const char * path, __int64 _default)
    {
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
            throw MakeStringException(0, "Attempting to extract xml content text as integer");

        const char *str = queryProp(path);
        if (!str) return _default;
        return _atoi64(str);
    }
    virtual __uint64 readUInt(const char * path, __uint64 _default)
    {
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
            throw MakeStringException(0, "Attempting to extract xml content text as integer");

        const char *str = queryProp(path);
        if (!str) return _default;
        return strtoull(str, nullptr, 10);
    }
    virtual void readQString(size32_t len, char * text, const char * path, size32_t _lenDefault, const char * _default)
    {
        CriticalBlock b(crit);
        sharedResult.clear();
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
        {
            cnv2Latin1(length, contentMb.toByteArray()+offset, sharedResult);
        }
        else
        {
            const char *str = queryProp(path);
            if (str)
                cnv2Latin1((size32_t)strlen(str), str, sharedResult);
            else
            {
                rtlQStrToQStr(len, text, _lenDefault, _default);
                return;
            }
        }
        rtlStrToQStr(len, text, sharedResult.length(), sharedResult.toByteArray());
    }
    virtual void readString(size32_t len, char * text, const char * path, size32_t _lenDefault, const char * _default)
    {
        CriticalBlock b(crit);
        sharedResult.clear();
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
        {
            cnv2Latin1(length, contentMb.toByteArray()+offset, sharedResult);
        }
        else
        {
            const char *str = queryProp(path);
            if (str)
                cnv2Latin1((size32_t)strlen(str), str, sharedResult);
            else
            {
                rtlStrToStr(len, text, _lenDefault, _default);
                return;
            }
        }
        rtlStrToStr(len, text, sharedResult.length(), sharedResult.toByteArray());
    }
    virtual void readStringX(size32_t & len, char * & text, const char * path, size32_t _lenDefault, const char * _default)
    {
        MemoryBuffer result;
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
        {
            if (length)
                cnv2Latin1(length, contentMb.toByteArray()+offset, result);
        }
        else
        {
            const char *str = queryProp(path);
            if (str)
                cnv2Latin1((size32_t)strlen(str), str, result);
            else
            {
                rtlStrToStrX(len, text, _lenDefault, _default);
                return;
            }
        }
        len = result.length();
        text = (char *) result.detach();
    }
    virtual void readUnicodeX(size32_t & len, UChar * & text, const char * path, size32_t _lenDefault, const UChar * _default)
    {
        size32_t offset = 0;
        size32_t length = 0;
        if (contentRequest(path, offset, length))
        {
            rtlCodepageToUnicodeX(len, text, length, contentMb.toByteArray()+offset, "utf-8");
        }
        else
        {
            CriticalBlock b(crit);
            const char *tmpPtr = queryProp(path);
            if (tmpPtr)
                rtlCodepageToUnicodeX(len, text, strlen(tmpPtr), tmpPtr, "utf-8");
            else
                rtlUnicodeToUnicodeX(len, text, _lenDefault, _default);
        }
    }
    virtual void readUtf8X(size32_t & len, char * & text, const char * path, size32_t _lenDefault, const char * _default)
    {
        size32_t offset = 0;
        size32_t length = 0;
        size32_t size;
        if (contentRequest(path, offset, length))
        {
            rtlStrToStrX(size, text, length, contentMb.toByteArray()+offset);
        }
        else
        {
            CriticalBlock b(crit);
            const char *tmpPtr = queryProp(path);
            if (tmpPtr)
            {
                rtlStrToStrX(size, text, strlen(tmpPtr), tmpPtr);
            }
            else
            {
                rtlUtf8ToUtf8X(len, text, _lenDefault, _default);
                return;
            }
        }
        len = rtlUtf8Length(size, text);
    }
    virtual bool readIsSetAll(const char * path, bool _default)
    {
        if (hasProp(path))
        {
            StringBuffer fullpath;
            fullpath.append(path).append("/All");
            if (path && '/' == *path && '/' != *(path+1)) 
                return root->hasProp(fullpath.str()+1);
            else
                return node->hasProp(fullpath.str());
        }
        return _default;
    }
    virtual const char *readRaw(const char * path, size32_t &sz) const override
    {
        const char *value = node->queryProp(path);
        sz = value ? strlen(value) : 0;
        return value;
    }
};

void CColumnIterator::setCurrent()
{
    Owned<IPropertyTree> curTree = &iter->get();
    if (contentMb)
        cur.setown(new CColumnProvider(utf8Translator, root, curTree, contentMb, false, contentStartOffset, xpath));
    else
        cur.setown(new CColumnProvider(utf8Translator, root, curTree, NULL, false, 0, xpath));
}


class CXMLParse : implements IXMLParse, public CInterface
{
    IPullPTreeReader *xmlReader;
    StringAttr xpath;
    IXMLSelect *iXMLSelect;  // NOTE - not linked - creates circular links
    PTreeReaderOptions xmlOptions;
    bool step, contentRequired, isJson;

    //to make json file handling intuitive an array opening at root level is just ignored
    //but webservice calls map the entire response to a single row, and keeping the array works better
    bool keepRootArray;

    class CMakerBase : public CInterface, implements IPTreeMaker
    {
    protected:
        CXPath xpath;
        IXMLSelect *iXMLSelect;   // NOTE - not linked - creates circular links
        CICopyArrayOf<CParseStackInfo> stack, freeParseInfo;
        IPTreeMaker *maker;
        Linked<CMarkReadBase> marking;
        Owned<COffsetNodeCreator> nodeCreator;
        void *utf8Translator;
        unsigned level;
        bool contentRequired;
        unsigned lastMatchKeptLevel;
        IPropertyTree *lastMatchKeptNode, *lastMatchKeptNodeParent;

    public:
        IMPLEMENT_IINTERFACE;

        CMakerBase(const char *_xpath, IXMLSelect &_iXMLSelect, bool _contentRequired, bool ignoreNameSpaces) : xpath(_xpath, ignoreNameSpaces), iXMLSelect(&_iXMLSelect), contentRequired(_contentRequired)
        {
            lastMatchKeptLevel = 0;
            lastMatchKeptNode = lastMatchKeptNodeParent = NULL;
            maker = NULL;
            utf8Translator = NULL;
        }
        ~CMakerBase()
        {
            ForEachItemIn(i, stack)
                delete &stack.item(i);
            ForEachItemIn(i2, freeParseInfo)
                delete &freeParseInfo.item(i2);
            ::Release(maker);
            rtlCloseCodepageConverter(utf8Translator);
        }
        void init()
        {
            level = 0;
            nodeCreator.setown(new COffsetNodeCreator());
            maker = createRootLessPTreeMaker(ipt_none, NULL, nodeCreator);
            bool f;
            utf8Translator = rtlOpenCodepageConverter("utf-8", "latin1", f);
            if (f)
                throw MakeStringException(0, "Failed to initialize unicode utf-8 translator");
        }
        void setMarkingStream(CMarkReadBase &_marking) { marking.set(&_marking); }
        CXPath &queryXPath() { return xpath; }

// IPTreeMaker
        virtual void beginNode(const char *tag, bool arrayitem, offset_t startOffset)
        {
            if (lastMatchKeptNode && level == lastMatchKeptLevel)
            {
                // NB: could be passed to match objects for removal by match object,
                //     but dubious if useful for greater than one path to exist above match.
                if (lastMatchKeptNodeParent)
                    lastMatchKeptNodeParent->removeTree(lastMatchKeptNode);
                else
                    maker->reset();
                lastMatchKeptNode = NULL;
            }
            bool res = false;
            CParseStackInfo *stackInfo;
            if (freeParseInfo.ordinality())
            {
                stackInfo = &freeParseInfo.popGet();
                stackInfo->reset();
            }
            else
                stackInfo = new CParseStackInfo();
            stackInfo->startOffset = startOffset;
            if (!stack.ordinality())
            {
                if (0 == xpath.queryDepth() || xpath.match(0, tag))
                {
                    if (1 >= xpath.queryDepth())
                    {
                        if (contentRequired)
                        {
                            assertex(marking);
                            marking->mark(startOffset); // mark stream at tag start offset
                        }
                    }
                    res = true;
                }
            }
            else if (xpath.queryDepth())
            {
                if (stack.tos().keep)
                {
                    if (level >= xpath.queryDepth())
                        res = true;
                    else if (xpath.match(level, tag))
                    {
                        res = true;
                        if (level == xpath.queryDepth()-1)
                        {
                            if (contentRequired)
                            {
                                assertex(marking);
                                marking->mark(startOffset); // mark stream at tag start offset
                            }
                        }
                    }
                    else if (level > ((unsigned)xpath.queryHighestQualifier()))
                    {
                        stackInfo->keptForQualifier = true;
                        res = true; // construct content below qualified tag (!=simple) needed to qualify when back at topQ.
                    }
                }
            }
            else
                res = true;
            stackInfo->keep = res;
            stack.append(*stackInfo);
            if (res)
            {
                maker->beginNode(tag, false, startOffset);
                CPTreeWithOffsets *current = (CPTreeWithOffsets *)maker->queryCurrentNode();
                current->startOffset = startOffset;
                stackInfo->nodeMade = res;
                stackInfo->iPTMade = current;
            }
        }
        virtual void newAttribute(const char *tag, const char *value)
        {
            if (stack.tos().keep)
                maker->newAttribute(tag, value);
        }
        virtual void beginNodeContent(const char *tag)
        {
            // Can optimize qualifiers here that contain only attribute tests.
            bool &keep = stack.tos().keep;
            if (keep)
            {
                if (xpath.toQualify(level, true))
                {
                    IPropertyTree *currentNode = maker->queryCurrentNode();
                    keep = xpath.qualify(*currentNode, level);
                }
            }
            level++;
        }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            --level;
            CParseStackInfo &stackInfo = stack.tos();
            bool keep = stackInfo.keep;
            bool nodeMade = stackInfo.nodeMade;
            IPropertyTree *currentNode = maker->queryCurrentNode();
            if (nodeMade)
            {
                CPTreeWithOffsets *current = (CPTreeWithOffsets *)maker->queryCurrentNode();
                current->endOffset = endOffset;
                maker->endNode(tag, length, value, binary, endOffset);
            }
            if (keep)
            {
                if (!stackInfo.keptForQualifier)
                    if (xpath.toQualify(level, false))
                        keep = xpath.qualify(*currentNode, level);
            }

            bool matched = false;
            if (keep)
            {
                if (!stackInfo.keptForQualifier)
                {
                    if ((0 == xpath.queryDepth() && 0 == level) || level == xpath.queryDepth()-1)
                    {
                        unsigned topQ = xpath.queryHighestQualifier();
                        unsigned noHigherQualifiers = -1 == topQ || topQ >= level;
                        IPropertyTree *parent = stack.ordinality()>=2?stack.item(stack.ordinality()-2).iPTMade:NULL;
                        if (noHigherQualifiers)
                        {
                            MemoryBuffer mb;
                            MemoryBuffer *content;
                            if (contentRequired)
                            {
                                assertex(marking);
                                marking->getMarkTo(endOffset, mb);
                                content = &mb;
                            }
                            else
                                content = NULL;
                            CPTreeWithOffsets *currentNodeWO = (CPTreeWithOffsets *)currentNode;
                            Owned<CColumnProvider> provider = new CColumnProvider(utf8Translator, maker->queryRoot(), currentNode, content, true, currentNodeWO->startOffset, xpath.queryXPathStr());
                            iXMLSelect->match(*provider, stackInfo.startOffset, endOffset);
                            matched = true;
                        }
                        else
                        {
                            // only prospective match - depends on higher qualifiers being satisfied.
                            if (!stackInfo.prospectiveMatches)
                                stackInfo.prospectiveMatches = new CProcespectiveMatchArray;
                            MemoryBuffer *tagContent = NULL;
                            if (contentRequired)
                            {
                                tagContent = new MemoryBuffer;
                                marking->getMarkTo(endOffset, *tagContent);
                            }
                            stackInfo.prospectiveMatches->append(*new CProspectiveMatch(parent, currentNode, tagContent));
                        }
                    }
                    else if (stackInfo.prospectiveMatches && stackInfo.prospectiveMatches->ordinality() && level < xpath.queryDepth()-1)
                    {
                        unsigned topQ = xpath.queryHighestQualifier();
                        unsigned noHigherQualifiers = -1 == topQ || topQ >= level;
                        if (noHigherQualifiers)
                        {
                            ForEachItemIn(m, *stackInfo.prospectiveMatches)
                            {
                                CProspectiveMatch &prospectiveMatch = stackInfo.prospectiveMatches->item(m);
                                CPTreeWithOffsets *prospectiveNodeWO = (CPTreeWithOffsets *)prospectiveMatch.node;
                                Owned<CColumnProvider> provider = new CColumnProvider(utf8Translator, maker->queryRoot(), prospectiveMatch.node, prospectiveMatch.content, true, prospectiveNodeWO->startOffset, xpath.queryXPathStr());
                                // NB: caveat; if complex qualifiers on intermediate iterator nodes and fully qualified attributes
                                //             are access from this match, there are potential ambiguities in the lookup.
                                iXMLSelect->match(*provider, stackInfo.startOffset, endOffset);
                                matched = true;
                            }
                            stackInfo.prospectiveMatches->kill();
                            stackInfo.prospectiveMatches = NULL;
                        }
                    }
                    else
                    {
                        if (NULL == lastMatchKeptNode && level < xpath.queryDepth())
                            keep = false;
                    }
                }
            }
            else
            {
                if (contentRequired && ((0==level && 0==xpath.queryDepth()) || level == xpath.queryDepth()-1))
                {
                    assertex(marking);
                    marking->closeMark();
                }
            }
            freeParseInfo.append(stackInfo);
            if (keep && stackInfo.prospectiveMatches && stackInfo.prospectiveMatches->ordinality())
            {
                Linked<CParseStackInfo> childStackInfo = &stackInfo;
                stack.pop();
                if (stack.ordinality())
                {
                    CParseStackInfo &parentSI = stack.tos();
                    if (!parentSI.prospectiveMatches)
                        parentSI.prospectiveMatches = new CProcespectiveMatchArray;

                    ForEachItemIn(p, *stackInfo.prospectiveMatches)
                        parentSI.prospectiveMatches->append(*LINK(&stackInfo.prospectiveMatches->item(p)));
                }
            }
            else
                stack.pop();

            // Track last level kept
            if (lastMatchKeptNode || (keep && matched))
            {
                assertex(nodeMade);
                lastMatchKeptLevel = level;
                lastMatchKeptNode = currentNode;
                lastMatchKeptNodeParent = maker->queryCurrentNode();
            }
            else if (!keep && nodeMade)
            {
                IPropertyTree *parent = maker->queryCurrentNode();
                if (parent)
                    parent->removeTree(currentNode);
            }
            currentNode = NULL;
        }
        virtual IPropertyTree *queryRoot() { return maker->queryRoot(); }
        virtual IPropertyTree *queryCurrentNode() { return maker->queryCurrentNode(); }
        virtual void reset()
        {
            level = 0;
            ForEachItemIn(i, stack)
                delete &stack.item(i);
            ForEachItemIn(i2, freeParseInfo)
                delete &freeParseInfo.item(i2);
            stack.kill();
            freeParseInfo.kill();
            if (marking)
                marking->reset();
        }
        virtual IPropertyTree *create(const char *tag)
        {
            return nodeCreator->create(tag);
        }
    } *iXMLMaker;

    class CXMLMaker : public CMakerBase
    {
    public:
        CXMLMaker(const char *_xpath, IXMLSelect &_iXMLSelect, bool _contentRequired, bool ignoreNameSpaces) : CMakerBase(_xpath, _iXMLSelect, _contentRequired, ignoreNameSpaces)
        {
        }
    };

    class CJSONMaker : public CMakerBase
    {
    private:
        bool keepRootArray;
        bool inRootArray;
    public:
        CJSONMaker(const char *_xpath, IXMLSelect &_iXMLSelect, bool _contentRequired, bool ignoreNameSpaces, bool _keepRootArray) : CMakerBase(_xpath, _iXMLSelect, _contentRequired, ignoreNameSpaces), keepRootArray(_keepRootArray)
        {
            inRootArray = false;
        }

        bool checkRootArrayItem(const char *&tag)
        {
            if (!inRootArray)
                return false;
            if (stack.ordinality()!=1)
                return false;
            if (streq(tag, "__object__"))
                tag = "Row";  //unamed json root array [{},{}] will generate "Row"s
            return true;
        }
        bool checkSkipRoot(const char *&tag)
        {
            if (checkRootArrayItem(tag))
                return false;
            if (stack.ordinality()) //root level only
                return false;
            if (streq(tag, "__array__")) //xpath starts after root array
            {
                if (keepRootArray && !xpath.queryDepth())
                {
                    inRootArray = true;
                    return false;
                }
                return true;
            }
            if (streq(tag, "__object__") && xpath.queryDepth()) //empty xpath matches start object, otherwise skip, xpath starts immediately after
                return true;
            return false;
        }

        virtual void beginNode(const char *tag, bool arrayitem, offset_t startOffset) override
        {
            if (!checkSkipRoot(tag))
                CMakerBase::beginNode(tag, arrayitem, startOffset);
        }
        virtual void newAttribute(const char *tag, const char *value)
        {
            if (stack.ordinality() && stack.tos().keep)
                maker->newAttribute(tag, value);
        }
        virtual void beginNodeContent(const char *tag)
        {
            if (!checkSkipRoot(tag))
                CMakerBase::beginNodeContent(tag);
        }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            if (!checkSkipRoot(tag))
                CMakerBase::endNode(tag, length, value, binary, endOffset);
        }
    };

public:
    IMPLEMENT_IINTERFACE;

    CXMLParse(const char *fName, const char *_xpath, IXMLSelect &_iXMLSelect, PTreeReaderOptions _xmlOptions=ptr_none, bool _contentRequired=true, bool _step=true, bool _isJson=false, bool _keepRootArray=false) : xpath(_xpath), iXMLSelect(&_iXMLSelect), xmlOptions(_xmlOptions), contentRequired(_contentRequired), step(_step), isJson(_isJson), keepRootArray(_keepRootArray) { init(); go(fName); }
    CXMLParse(IFile &ifile, const char *_xpath, IXMLSelect &_iXMLSelect, PTreeReaderOptions _xmlOptions=ptr_none, bool _contentRequired=true, bool _step=true, bool _isJson=false, bool _keepRootArray=false) : xpath(_xpath), iXMLSelect(&_iXMLSelect), xmlOptions(_xmlOptions), contentRequired(_contentRequired), step(_step), isJson(_isJson), keepRootArray(_keepRootArray) { init(); go(ifile); }
    CXMLParse(IFileIO &fileio, const char *_xpath, IXMLSelect &_iXMLSelect, PTreeReaderOptions _xmlOptions=ptr_none, bool _contentRequired=true, bool _step=true, bool _isJson=false, bool _keepRootArray=false) : xpath(_xpath), iXMLSelect(&_iXMLSelect), xmlOptions(_xmlOptions), contentRequired(_contentRequired), step(_step), isJson(_isJson), keepRootArray(_keepRootArray) { init(); go(fileio); }
    CXMLParse(ISimpleReadStream &stream, const char *_xpath, IXMLSelect &_iXMLSelect, PTreeReaderOptions _xmlOptions=ptr_none, bool _contentRequired=true, bool _step=true, bool _isJson=false, bool _keepRootArray=false) : xpath(_xpath), iXMLSelect(&_iXMLSelect), xmlOptions(_xmlOptions), contentRequired(_contentRequired), step(_step), isJson(_isJson), keepRootArray(_keepRootArray) { init(); go(stream); }
    CXMLParse(const void *buffer, unsigned bufLen, const char *_xpath, IXMLSelect &_iXMLSelect, PTreeReaderOptions _xmlOptions=ptr_none, bool _contentRequired=true, bool _step=true, bool _isJson=false, bool _keepRootArray=false) : xpath(_xpath), iXMLSelect(&_iXMLSelect), xmlOptions(_xmlOptions), contentRequired(_contentRequired), step(_step), isJson(_isJson), keepRootArray(_keepRootArray) { init(); go(buffer, bufLen); }
    CXMLParse(const char *_xpath, IXMLSelect &_iXMLSelect, PTreeReaderOptions _xmlOptions=ptr_none, bool _contentRequired=true, bool _step=true, bool _isJson=false, bool _keepRootArray=false) : xpath(_xpath), iXMLSelect(&_iXMLSelect), xmlOptions(_xmlOptions), contentRequired(_contentRequired), step(_step), isJson(_isJson), keepRootArray(_keepRootArray) { init(); }
    ~CXMLParse()
    {
        ::Release(iXMLMaker);
        ::Release(xmlReader);
    }
    CMakerBase *createMaker()
    {
        bool ignoreNameSpaces = 0 != ((unsigned)xmlOptions & (unsigned)ptr_ignoreNameSpaces);
        if (isJson)
            return new CJSONMaker(xpath, *iXMLSelect, contentRequired, ignoreNameSpaces, keepRootArray);
        return new CXMLMaker(xpath, *iXMLSelect, contentRequired, ignoreNameSpaces);
    }
    void init()
    {
        xmlReader = NULL;
        iXMLMaker = createMaker();
        iXMLMaker->init();
    }

    void go(const char *fName)
    {
        OwnedIFile ifile = createIFile(fName);
        go(*ifile);
    }
    void go(IFile &file)
    {
        OwnedIFileIO ifileio = file.open(IFOread);
        if (!ifileio)
            throw MakeStringException(0, "Failed to open: %s", file.queryFilename());
        go(*ifileio);
    }
    void go(IFileIO &fileio)
    {
        Owned<IIOStream> stream = createIOStream(&fileio);
        go(*stream);
    }

    void go(ISimpleReadStream &stream)
    {
        if (contentRequired)
        {
            // only need marking stream if fetching xml text content.
            Owned<CMarkReadStream> markingStream = new CMarkReadStream(*LINK(&stream));
            iXMLMaker->setMarkingStream(*markingStream);
            if (isJson)
                xmlReader = createPullJSONStreamReader(*markingStream, *iXMLMaker, xmlOptions);
            else
                xmlReader = createPullXMLStreamReader(*markingStream, *iXMLMaker, xmlOptions);
        }
        else if (isJson)
            xmlReader = createPullJSONStreamReader(stream, *iXMLMaker, xmlOptions);
        else
            xmlReader = createPullXMLStreamReader(stream, *iXMLMaker, xmlOptions);
        if (!step)
        {
            xmlReader->load();
            xmlReader->Release();
            xmlReader = NULL;
        }
    }
    void go(const void *buffer, unsigned bufLen)
    {
        if (contentRequired)
        {
            Owned<CMarkReadBase> markingStream = new CMarkRead(buffer, bufLen);
            iXMLMaker->setMarkingStream(*markingStream);
        }
        if (isJson)
            xmlReader = createPullJSONBufferReader(buffer, bufLen, *iXMLMaker, xmlOptions);
        else
            xmlReader = createPullXMLBufferReader(buffer, bufLen, *iXMLMaker, xmlOptions);
        if (!step)
        {
            xmlReader->load();
            xmlReader->Release();
            xmlReader = NULL;
        }
    }
    void provideXML(const char *str)
    {
        if (contentRequired)
        {
            Owned<CMarkReadBase> markingStream = new CMarkRead(str, strlen(str));
            iXMLMaker->setMarkingStream(*markingStream);
        }
        if (isJson)
            xmlReader = createPullJSONStringReader(str, *iXMLMaker, xmlOptions);
        else
            xmlReader = createPullXMLStringReader(str, *iXMLMaker, xmlOptions);
        if (!step)
        {
            xmlReader->load();
            xmlReader->Release();
            xmlReader = NULL;
        }
    }

// IXMLParse
    virtual bool next()
    {
        return xmlReader->next();
    }

    virtual void reset()
    {
        iXMLMaker->reset();
        xmlReader->reset();
    }
};

IXMLParse *createXMLParse(const char *filename, const char *xpath, IXMLSelect &iselect, PTreeReaderOptions xmlOptions, bool contentRequired)
{
    return new CXMLParse(filename, xpath, iselect, xmlOptions, contentRequired);
}
IXMLParse *createXMLParse(ISimpleReadStream &stream, const char *xpath, IXMLSelect &iselect, PTreeReaderOptions xmlOptions, bool contentRequired)
{
    return new CXMLParse(stream, xpath, iselect, xmlOptions, contentRequired);
}

IXMLParse *createXMLParse(const void *buffer, unsigned bufLen, const char *xpath, IXMLSelect &iselect, PTreeReaderOptions xmlOptions, bool contentRequired)
{
    return new CXMLParse(buffer, bufLen, xpath, iselect, xmlOptions, contentRequired);
}

IXMLParse *createXMLParseString(const char *string, const char *xpath, IXMLSelect &iselect, PTreeReaderOptions xmlOptions, bool contentRequired)
{
    CXMLParse *parser = new CXMLParse(xpath, iselect, xmlOptions, contentRequired);
    parser->provideXML(string);
    return parser;
}

IXMLParse *createJSONParse(const char *filename, const char *xpath, IXMLSelect &iselect, PTreeReaderOptions xmlOptions, bool contentRequired)
{
    return new CXMLParse(filename, xpath, iselect, xmlOptions, contentRequired, true, true);
}
IXMLParse *createJSONParse(ISimpleReadStream &stream, const char *xpath, IXMLSelect &iselect, PTreeReaderOptions xmlOptions, bool contentRequired)
{
    return new CXMLParse(stream, xpath, iselect, xmlOptions, contentRequired, true, true);
}

IXMLParse *createJSONParse(const void *buffer, unsigned bufLen, const char *xpath, IXMLSelect &iselect, PTreeReaderOptions xmlOptions, bool contentRequired, bool keepRootArray)
{
    return new CXMLParse(buffer, bufLen, xpath, iselect, xmlOptions, contentRequired, true, true, keepRootArray);
}

IXMLParse *createJSONParseString(const char *string, const char *xpath, IXMLSelect &iselect, PTreeReaderOptions xmlOptions, bool contentRequired)
{
    CXMLParse *parser = new CXMLParse(xpath, iselect, xmlOptions, contentRequired, true, true);
    parser->provideXML(string);
    return parser;
}

