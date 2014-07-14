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

#ifndef FILEVIEW_INCL
#define FILEVIEW_INCL

#ifdef _WIN32
    #ifdef FILEVIEW2_EXPORTS
        #define FILEVIEW_API __declspec(dllexport)
    #else
        #define FILEVIEW_API __declspec(dllimport)
    #endif
#else
    #define FILEVIEW_API
#endif

#include "workunit.hpp"

#define UNKNOWN_NUM_ROWS    (I64C(0x7FFFFFFFFFFFFFFF))

enum DisplayType
{
    TypeBoolean = 0,
    TypeInteger = 1,
    TypeUnsignedInteger = 2,
    TypeReal = 3,
    TypeString = 4,
    TypeData = 5,
    TypeUnicode = 6,
    TypeUnknown = 7,
    TypeBeginIfBlock = 8,
    TypeEndIfBlock = 9,
    TypeBeginRecord = 10,
    TypeEndRecord = 11,
    TypeSet = 12,
    TypeDataset = 13
};



enum ResultSetType
{
    TYPE_FORWARD_ONLY = 0,
    TYPE_SCROLL_INSENSITIVE = 1,
    TYPE_SCROLL_SENSITIVE = 2
};


interface IXmlWriter;

interface IResultSetMetaData : extends IInterface
{
    virtual IResultSetMetaData * getChildMeta(int column) const = 0;
    virtual int getColumnCount() const = 0;
    virtual DisplayType getColumnDisplayType(int column) const = 0;
    virtual IStringVal & getColumnLabel(IStringVal & s, int column) const = 0;
    virtual IStringVal & getColumnEclType(IStringVal & s, int column) const = 0;
    virtual IStringVal & getColumnXmlType(IStringVal & s, int column) const = 0;
    virtual bool isSigned(int column) const = 0;
    virtual bool isEBCDIC(int column) const = 0;
    virtual bool isBigEndian(int column) const = 0;
    virtual unsigned getColumnRawType(int column) const = 0;
    virtual unsigned getColumnRawSize(int column) const = 0;
    virtual IStringVal & getXmlSchema(IStringVal & s, bool addHeader) const = 0;
    virtual unsigned getNumKeyedColumns() const = 0;
    virtual IStringVal & getXmlXPathSchema(IStringVal & str, bool addHeader) const = 0;
    virtual bool hasGetTranslation(int column) const = 0;
    virtual bool hasSetTranslation(int column) const = 0;
    virtual IStringVal & getNaturalColumnLabel(IStringVal & s, int column) const = 0;
    virtual bool isVirtual(int column) const = 0;
};


typedef double xdouble;
interface INewResultSet;
interface IXmlWriter;
interface IResultSetCursor : extends IInterface
{
    virtual bool absolute(__int64 row) = 0;
    virtual void afterLast() = 0;
    virtual void beforeFirst() = 0;
    virtual bool fetch(__int64 fileoffset) = 0;
    virtual bool first() = 0;
    virtual bool getBoolean(int columnIndex) = 0;
    virtual IDataVal & getBytes(IDataVal & d, int columnIndex) = 0;
    virtual xdouble getDouble(int columnIndex) = 0;
    virtual int getFetchSize() const = 0;
    virtual IResultSetCursor * getChildren(int columnIndex) const = 0;
    virtual bool getIsAll(int columnIndex) const = 0;
    virtual __int64 getInt(int columnIndex) = 0;
    virtual IDataVal & getRaw(IDataVal & d, int columnIndex) = 0;
    virtual IDataVal & getRawRow(IDataVal & d) = 0;
    virtual IStringVal & getString(IStringVal & ret, int columnIndex) = 0;
    virtual bool isAfterLast() const = 0;
    virtual bool isBeforeFirst() const = 0;
    virtual bool isFirst() const = 0;
    virtual bool isLast() const = 0;
    virtual bool isNull(int columnIndex) const = 0;
    virtual bool isValid() const = 0;
    virtual bool last() = 0;
    virtual bool next() = 0;
    virtual bool previous() = 0;
    virtual INewResultSet * queryResultSet() = 0;
    virtual bool relative(__int64 rows) = 0;
    virtual void serialize(IDataVal & d) = 0;
    virtual IStringVal & getDisplayText(IStringVal & ret, int columnIndex) = 0;
    virtual IStringVal & getXml(IStringVal & ret, int columnIndex) = 0;
    virtual IStringVal & getXmlRow(IStringVal & ret) = 0;
    virtual IStringVal & getXmlItem(IStringVal & ret) = 0;
    virtual void beginWriteXmlRows(IXmlWriter & writer) = 0;
    virtual void writeXmlRow(IXmlWriter & writer) = 0;
    virtual void endWriteXmlRows(IXmlWriter & writer) = 0;
    virtual void writeXmlItem(IXmlWriter & writer) = 0;
    virtual __int64 getNumRows() const = 0;
};


interface INewResultSet;

interface IResultSetFilter : extends IInterface
{
    virtual void clearFilter(unsigned columnIndex) = 0;
    virtual void addFilter(unsigned columnIndex, const char * value) = 0;
    virtual void addFilter(unsigned columnIndex, unsigned length, const char * utf8Value) = 0;
    virtual void addNaturalFilter(unsigned columnIndex, unsigned length, const char * utf8Value) = 0;
    virtual void clearFilters() = 0;
};


interface IFilteredResultSet : extends IResultSetFilter
{
    virtual INewResultSet * create() = 0;
};



//Following interface is stateless, and can be shared...
interface INewResultSet : extends IInterface
{
    virtual IResultSetCursor * createCursor() = 0;
    virtual IResultSetCursor * createCursor(IDataVal & buffer) = 0;
    virtual IFilteredResultSet * createFiltered() = 0;
    virtual IResultSetCursor * createSortedCursor(unsigned column, bool descend) = 0;
    virtual const IResultSetMetaData & getMetaData() const = 0;
    virtual __int64 getNumRows() const = 0;
    virtual bool supportsRandomSeek() const = 0;
};



interface IResultSetFactory : extends IInterface
{
    virtual INewResultSet * createNewResultSet(IConstWUResult * wuResult, const char * wuid) = 0;
    virtual INewResultSet * createNewFileResultSet(const char * logicalFile, const char * cluster) = 0;
    virtual INewResultSet * createNewResultSet(const char * wuid, unsigned sequence, const char * name) = 0;
    virtual INewResultSet * createNewFileResultSet(const char * logicalFile) = 0;
    virtual IResultSetMetaData * createResultSetMeta(IConstWUResult * wuResult) = 0;
    virtual IResultSetMetaData * createResultSetMeta(const char * wuid, unsigned sequence, const char * name) = 0;
};


//provided to wrap the exceptions for clarion....
extern FILEVIEW_API INewResultSet* createNewResultSet(IResultSetFactory & factory, IStringVal & error, IConstWUResult * wuResult, const char * wuid);
extern FILEVIEW_API INewResultSet* createNewFileResultSet(IResultSetFactory & factory, IStringVal & error, const char * logicalFile, const char * queue, const char * cluster);
extern FILEVIEW_API INewResultSet* createNewResultSetSeqName(IResultSetFactory & factory, IStringVal & error, const char * wuid, unsigned sequence, const char * name);


extern FILEVIEW_API IResultSetFactory * getResultSetFactory(const char * username, const char * password);
extern FILEVIEW_API IResultSetFactory * getSecResultSetFactory(ISecManager *secmgr, ISecUser *secuser, const char * username, const char * password);

extern FILEVIEW_API IResultSetFactory * getRemoteResultSetFactory(const char * remoteServer, const char * username, const char * password);
extern FILEVIEW_API IResultSetFactory * getSecRemoteResultSetFactory(const char * remoteServer, ISecManager &secmgr, ISecUser &secuser);

//Formatting applied remotely, so it can be accessed between different operating systems...
extern FILEVIEW_API IResultSetFactory * getRemoteResultSetFactory(const char * remoteServer, const char * username, const char * password);
extern FILEVIEW_API int findResultSetColumn(const INewResultSet * results, const char * columnName);

extern FILEVIEW_API unsigned getResultCursorXml(IStringVal & ret, IResultSetCursor * cursor, const char * name, unsigned start=0, unsigned count=0, const char * schemaName=NULL, const IProperties *xmlns=NULL);
extern FILEVIEW_API unsigned getResultXml(IStringVal & ret, INewResultSet * cursor,  const char* name, unsigned start=0, unsigned count=0, const char * schemaName=NULL, const IProperties *xmlns=NULL);
extern FILEVIEW_API unsigned getResultJSON(IStringVal & ret, INewResultSet * cursor,  const char* name, unsigned start=0, unsigned count=0, const char * schemaName=NULL);
extern FILEVIEW_API unsigned writeResultCursorXml(IXmlWriter & writer, IResultSetCursor * cursor, const char * name, unsigned start=0, unsigned count=0, const char * schemaName=NULL, const IProperties *xmlns = NULL);
extern FILEVIEW_API unsigned writeResultXml(IXmlWriter & writer, INewResultSet * cursor,  const char* name, unsigned start=0, unsigned count=0, const char * schemaName=NULL, const IProperties *xmlns = NULL);

extern FILEVIEW_API unsigned getResultCursorBin(MemoryBuffer & ret, IResultSetCursor * cursor, unsigned start=0, unsigned count=0);
extern FILEVIEW_API unsigned getResultBin(MemoryBuffer & ret, INewResultSet * cursor, unsigned start=0, unsigned count=0);

#define WorkUnitXML_InclSchema      0x0001
#define WorkUnitXML_NoRoot          0x0002
#define WorkUnitXML_SeverityTags    0x0004

extern FILEVIEW_API void writeFullWorkUnitResults(const char *username, const char *password, const IConstWorkUnit *cw, IXmlWriter &writer, unsigned flags, WUExceptionSeverity minSeverity, const char *rootTag);
extern FILEVIEW_API IStringVal& getFullWorkUnitResultsXML(const char *user, const char *pw, const IConstWorkUnit *wu, IStringVal &str, unsigned flags=0, WUExceptionSeverity minSeverity=ExceptionSeverityInformation);
extern FILEVIEW_API IStringVal& getFullWorkUnitResultsJSON(const char *user, const char *pw, const IConstWorkUnit *wu, IStringVal &str, unsigned flags=0, WUExceptionSeverity minSeverity=ExceptionSeverityInformation);

extern FILEVIEW_API void startRemoteDataSourceServer(const char * queue, const char * cluster);
extern FILEVIEW_API void stopRemoteDataSourceServer();

#endif
