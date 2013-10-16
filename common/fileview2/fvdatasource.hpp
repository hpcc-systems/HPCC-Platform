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

#ifndef FVDATASOURCE_HPP
#define FVDATASOURCE_HPP

#include "fileview.hpp"
#include "deftype.hpp"

enum
{
    FVFFnone        = 0,
    FVFFbeginif     = 1,
    FVFFendif       = 2,
    FVFFbeginrecord = 3,
    FVFFendrecord   = 4,
    FVFFdataset     = 5,
    FVFFset         = 6,
    FVFFvirtual     = 7,
};

interface IFvDataSource;
interface IFvDataSourceMetaData : extends IInterface
{
    virtual unsigned numColumns() const = 0;
    virtual ITypeInfo * queryType(unsigned column) const = 0;
    virtual const char * queryName(unsigned column) const = 0;
    virtual const char * queryXPath(unsigned column) const = 0;
    virtual void serialize(MemoryBuffer & buffer) const = 0;
    virtual bool supportsRandomSeek() const = 0;
    virtual unsigned queryFieldFlags(unsigned column) const = 0;
    virtual IFvDataSourceMetaData * queryChildMeta(unsigned column) const = 0;
    virtual IFvDataSource * createChildDataSource(unsigned column, unsigned len, const void * data) = 0;
    virtual unsigned numKeyedColumns() const = 0;
    
    inline bool isVirtual(unsigned column) const { return queryFieldFlags(column) == FVFFvirtual; }
    virtual const char *queryXmlTag(unsigned column) const = 0;
    virtual const char *queryXmlTag() const = 0;
    virtual const IntArray &queryAttrList() const = 0;
    virtual const IntArray &queryAttrList(unsigned column) const = 0;
    virtual bool mixedContent(unsigned column) const = 0;
    virtual bool mixedContent() const = 0;
};

IFvDataSourceMetaData * deserializeDataSourceMeta(MemoryBuffer & in);

#define BEFORE_FIRST_ROW    ((__int64)-1)
#define AFTER_LAST_ROW      ((__int64)-2)

interface IFvDataSource : extends IInterface
{
    virtual IFvDataSourceMetaData * queryMetaData() = 0;
    virtual void applyFilter() { }
    virtual IFvDataSource * cloneForFilter() { return NULL; }
    virtual __int64 numRows(bool force = false) = 0;
    virtual bool fetchRow(MemoryBuffer & out, __int64 offset) = 0;
    virtual bool fetchRawRow(MemoryBuffer & out, __int64 offset) = 0;
    virtual bool getRow(MemoryBuffer & out, __int64 row) = 0;
    virtual bool getRawRow(MemoryBuffer & out, __int64 row) = 0;
    virtual bool addFilter(unsigned offset, unsigned matchLen, unsigned len, const void * data) { return false; }       // more: support ranges
    virtual void onClose() = 0;
    virtual void onOpen() = 0;
    virtual bool isIndex() = 0;
};

//not public outside of the dll
class ADataSource : public CInterface, implements IFvDataSource
{
public:
    IMPLEMENT_IINTERFACE

    virtual bool init() = 0;
};

IFvDataSource * createDataSource(IConstWUResult * wuResult, const char * wuid, const char * username, const char * password);
IFvDataSource * createFileDataSource(IDistributedFile * df, const char * logicalName, const char * cluster, const char * username, const char * password);
IFvDataSource * createFileDataSource(const char * logicalName, const char * cluster, const char * username, const char * password);
IFvDataSource * createRemoteDataSource(const SocketEndpoint & ep, const char * username, const char * password, const char * wuid, unsigned sequence, const char * name);
IFvDataSource * createRemoteFileDataSource(const SocketEndpoint & ep, const char * username, const char * password, const char * logicalName);
IFvDataSourceMetaData * createMetaData(IConstWUResult * wuResult);


IConstWUResult * resolveResult(const char * wuid, unsigned sequence, const char * name);
IConstWUResult * secResolveResult(ISecManager &secmgr, ISecUser &secuser, const char * wuid, unsigned sequence, const char * name);

#endif
