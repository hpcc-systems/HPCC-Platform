/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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
#include "sqlite3.h"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield_imp.hpp"
#include "nbcd.hpp"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif


#define UNSUPPORTED throw MakeStringException(-1, "UNSUPPORTED feature at %s(%d)", __FILE__, __LINE__)

static const char * compatibleVersions[] = {
    "SqLite3 Embed Helper 1.0.0",
    NULL };

static const char *version = "SqLite3 Embed Helper 1.0.0";

extern "C" EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = version;
    pb->moduleName = "sqlite3";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "SqLite3 Embed Helper";
    return true;
}

namespace sqlite3embed {

// Use class OwnedStatement for a sqlite3 stmt object that needs to be released cleanly

class OwnedStatement
{
    sqlite3_stmt *ptr;
public:
    inline OwnedStatement() : ptr(NULL)     {}
    inline OwnedStatement(sqlite3_stmt *_ptr) : ptr(_ptr) {}
    inline ~OwnedStatement()                { clear(); }
    inline sqlite3_stmt * get() const           { return ptr; }
    inline sqlite3_stmt * getClear()            { sqlite3_stmt *ret = ptr; ptr = NULL; return ret; }
    inline sqlite3_stmt * operator -> () const { return ptr; }
    inline operator sqlite3_stmt *() const    { return ptr; }
    inline void clear()                         { sqlite3_finalize(ptr); ptr = NULL; }
    inline void setown(sqlite3_stmt *_ptr)      { clear(); ptr = _ptr; }
    inline sqlite3_stmt **ref()                  { return &ptr; }
};

// Conversions from SqLite3 values to ECL data

static void checkSqliteError(int rc)
{
    if (rc != SQLITE_OK)
        rtlFail(rc, "SqLite3 error");
}

static void typeError(const char *expected, const RtlFieldInfo *field) __attribute__((noreturn));

static void typeError(const char *expected, const RtlFieldInfo *field)
{
    VStringBuffer msg("sqlite3: type mismatch - %s expected", expected);
    if (field)
        msg.appendf(" for field %s", field->name->str());
    rtlFail(0, msg.str());
}

static bool getBooleanResult(const RtlFieldInfo *field, sqlite3_value *val)
{
    assertex(val);
    if (sqlite3_value_type(val) != SQLITE_INTEGER)
        typeError("boolean", field);
    return sqlite3_value_int64(val) != 0;
}

static void getDataResult(const RtlFieldInfo *field, sqlite3_value *val, size32_t &chars, void * &result)
{
    assertex(val);
    if (sqlite3_value_type(val) != SQLITE_BLOB)
        typeError("blob", field);
    const void *blob = sqlite3_value_blob(val);
    int bytes = sqlite3_value_bytes(val);
    rtlStrToDataX(chars, result, bytes, blob);
}

static double getRealResult(const RtlFieldInfo *field, sqlite3_value *val)
{
    assertex(val);
    if (sqlite3_value_type(val) != SQLITE_FLOAT)
        typeError("real", field);
    return sqlite3_value_double(val);
}

static __int64 getSignedResult(const RtlFieldInfo *field, sqlite3_value *val)
{
    assertex(val);
    if (sqlite3_value_type(val) != SQLITE_INTEGER)
        typeError("integer", field);
    return sqlite3_value_int64(val);
}

static unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, sqlite3_value *val)
{
    assertex(val);
    if (sqlite3_value_type(val) != SQLITE_INTEGER)
        typeError("integer", field);
    return (unsigned __int64) sqlite3_value_int64(val);
}

static void getStringResult(const RtlFieldInfo *field, sqlite3_value *val, size32_t &chars, char * &result)
{
    assertex(val);
    if (sqlite3_value_type(val) != SQLITE_TEXT)
        typeError("string", field);
    const char *text = (const char *) sqlite3_value_text(val);
    int bytes = sqlite3_value_bytes(val);
    unsigned numchars = rtlUtf8Length(bytes, text);
    rtlUtf8ToStrX(chars, result, numchars, text);
}

static void getUTF8Result(const RtlFieldInfo *field, sqlite3_value *val, size32_t &chars, char * &result)
{
    assertex(val);
    if (sqlite3_value_type(val) != SQLITE_TEXT)
        typeError("string", field);
    const char *text = (const char *) sqlite3_value_text(val);
    int bytes = sqlite3_value_bytes(val);
    unsigned numchars = rtlUtf8Length(bytes, text);
    rtlUtf8ToUtf8X(chars, result, numchars, text);
}

static void getUnicodeResult(const RtlFieldInfo *field, sqlite3_value *val, size32_t &chars, UChar * &result)
{
    assertex(val);
    if (sqlite3_value_type(val) != SQLITE_TEXT)
        typeError("string", field);
    const UChar *text = (const UChar *) sqlite3_value_text16(val);
    int bytes = sqlite3_value_bytes(val);
    unsigned numchars = bytes / sizeof(UChar);
    rtlUnicodeToUnicodeX(chars, result, numchars, text);
}

// A SqLite3RowBuilder object is used to construct an ECL row from a sqlite result

class SqLite3RowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    SqLite3RowBuilder(sqlite3_stmt *_stmt)
    : stmt(_stmt), val(NULL), colIdx(0)
    {
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return sqlite3embed::getBooleanResult(field, val);
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        nextField(field);
        sqlite3embed::getDataResult(field, val, len, result);
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return sqlite3embed::getRealResult(field, val);
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return sqlite3embed::getSignedResult(field, val);
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        nextField(field);
        return sqlite3embed::getUnsignedResult(field, val);
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        nextField(field);
        sqlite3embed::getStringResult(field, val, chars, result);
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        nextField(field);
        sqlite3embed::getUTF8Result(field, val, chars, result);
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        nextField(field);
        sqlite3embed::getUnicodeResult(field, val, chars, result);
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        nextField(field);
        double ret = sqlite3embed::getRealResult(field, val);
        value.setReal(ret);
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        UNSUPPORTED;
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        UNSUPPORTED;
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        UNSUPPORTED;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
protected:
    void nextField(const RtlFieldInfo * field)
    {
        val = sqlite3_column_value(stmt, colIdx++);
    }
    sqlite3_value *val;
    sqlite3_stmt *stmt;
    unsigned colIdx;
};

// A SqLite3 function that returns a dataset will return a SqLite3RowStream object that can be
// interrogated to return each row of the result in turn

class SqLite3RowStream : public CInterfaceOf<IRowStream>
{
public:
    SqLite3RowStream(sqlite3_stmt *_stmt, IEngineRowAllocator *_resultAllocator)
    : stmt(_stmt), resultAllocator(_resultAllocator)
    {
        assertex(stmt);
    }
    virtual const void *nextRow()
    {
        if (!stmt)
            return NULL;
        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_ROW)
        {
            stmt.clear();
            return NULL;
        }
        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        SqLite3RowBuilder sqliteRowBuilder(stmt);
        const RtlTypeInfo *typeInfo = resultAllocator->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, sqliteRowBuilder);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop()
    {
        resultAllocator.clear();
        stmt.clear();
    }

protected:
    Linked<IEngineRowAllocator> resultAllocator;
    OwnedStatement stmt;
};

// Each call to a SqLite3 function will use a new SqLite3EmbedFunctionContext object

class SqLite3EmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    SqLite3EmbedFunctionContext(const char *options) : db(NULL)
    {
        const char *dbname = NULL;
        StringArray opts;
        opts.appendList(options, ",");
        ForEachItemIn(idx, opts)
        {
            const char *opt = opts.item(idx);
            const char *val = strchr(opt, '=');
            if (val)
            {
                StringBuffer optName(val-opt, opt);
                val++;
                if (stricmp(optName, "file")==0)
                    dbname = val;   // Note that lifetime of val is adequate for this to be safe
            }
        }
        if (!dbname)
            rtlFail(0, "sqlite3: no filename specified");
        int rc = sqlite3_open(dbname, &db);
        checkSqliteError(rc);
    }
    ~SqLite3EmbedFunctionContext()
    {
        if (db)
            sqlite3_close(db);
    }

    virtual bool getBooleanResult()
    {
        return sqlite3embed::getBooleanResult(NULL, getScalarResult());
    }
    virtual void getDataResult(size32_t &len, void * &result)
    {
        sqlite3embed::getDataResult(NULL, getScalarResult(), len, result);
    }
    virtual double getRealResult()
    {
        return sqlite3embed::getRealResult(NULL, getScalarResult());
    }
    virtual __int64 getSignedResult()
    {
        return sqlite3embed::getSignedResult(NULL, getScalarResult());
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        return sqlite3embed::getUnsignedResult(NULL, getScalarResult());
    }
    virtual void getStringResult(size32_t &chars, char * &result)
    {
        sqlite3embed::getStringResult(NULL, getScalarResult(), chars, result);
    }
    virtual void getUTF8Result(size32_t &chars, char * &result)
    {
        sqlite3embed::getUTF8Result(NULL, getScalarResult(), chars, result);
    }
    virtual void getUnicodeResult(size32_t &chars, UChar * &result)
    {
        sqlite3embed::getUnicodeResult(NULL, getScalarResult(), chars, result);
    }
    virtual void getDecimalResult(Decimal &value)
    {
        double ret = sqlite3embed::getRealResult(NULL, getScalarResult());
        value.setReal(ret);
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
    {
        UNSUPPORTED;
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        return new SqLite3RowStream(stmt.getClear(), _resultAllocator);
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        RtlDynamicRowBuilder rowBuilder(_resultAllocator);
        SqLite3RowBuilder sqliteRowBuilder(stmt);
        const RtlTypeInfo *typeInfo = _resultAllocator->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, sqliteRowBuilder);
        return (byte *) rowBuilder.finalizeRowClear(len);
    }
    virtual size32_t getTransformResult(ARowBuilder & rowBuilder)
    {
        SqLite3RowBuilder sqliteRowBuilder(stmt);
        const RtlTypeInfo *typeInfo = rowBuilder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        return typeInfo->build(rowBuilder, 0, &dummyField, sqliteRowBuilder);
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, byte *val)
    {
        UNSUPPORTED;  // Not sure it ever makes sense
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        UNSUPPORTED;   // Would need some thought, if it ever made sense
    }

    virtual void bindBooleanParam(const char *name, bool val)
    {
        checkSqliteError(sqlite3_bind_int64(stmt, findParameter(name), val ? 1 : 0));
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        checkSqliteError(sqlite3_bind_blob(stmt, findParameter(name), val, len, SQLITE_TRANSIENT));
    }
    virtual void bindRealParam(const char *name, double val)
    {
        checkSqliteError(sqlite3_bind_double(stmt, findParameter(name), val));
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        checkSqliteError(sqlite3_bind_int64(stmt, findParameter(name), val));
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        checkSqliteError(sqlite3_bind_int64(stmt, findParameter(name), val));
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        size32_t utf8chars;
        char *utf8;
        rtlStrToUtf8X(utf8chars, utf8, len, val);
        checkSqliteError(sqlite3_bind_text(stmt, findParameter(name), utf8, rtlUtf8Size(len, utf8), rtlFree));
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        bindStringParam(name, strlen(val), val);
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        size32_t sizeBytes = rtlUtf8Size(chars, val);
        checkSqliteError(sqlite3_bind_text(stmt, findParameter(name), val, sizeBytes, SQLITE_TRANSIENT)); // NOTE - requires size in bytes not chars
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        size32_t sizeBytes = chars * sizeof(UChar);
        checkSqliteError(sqlite3_bind_text16(stmt, findParameter(name), val, sizeBytes, SQLITE_TRANSIENT)); // NOTE - requires size in bytes not chars
    }
    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, void *setData)
    {
        UNSUPPORTED;
    }

    virtual void importFunction(size32_t lenChars, const char *text)
    {
        UNSUPPORTED;
    }
    virtual void compileEmbeddedScript(size32_t len, const char *script)
    {
        int rc = sqlite3_prepare_v2(db, script, len, stmt.ref(), NULL);
        checkSqliteError(rc);
    }
    virtual void callFunction()
    {
        assertex(stmt);
        int rc = sqlite3_reset(stmt);
        checkSqliteError(rc);
    }
protected:
    sqlite3_value *getScalarResult()
    {
        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_ROW || sqlite3_column_count(stmt) != 1)
            typeError("scalar", NULL);
        return sqlite3_column_value(stmt, 0);
    }
    inline int findParameter(const char *name)
    {
        VStringBuffer pname(":%s", name);
        int idx = sqlite3_bind_parameter_index(stmt, pname);
        if (!idx)
        {
            VStringBuffer msg("sqlite3: bound parameter %s not found", name);
            rtlFail(0, msg.str());
        }
        return idx;
    }
    OwnedStatement stmt;
    sqlite3 *db;
};

class SqLite3EmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(bool isImport, const char *options)
    {
        if (isImport)
            UNSUPPORTED;
        else
            return new SqLite3EmbedFunctionContext(options);
    }
};

extern IEmbedContext* getEmbedContext()
{
    return new SqLite3EmbedContext();
}

extern bool syntaxCheck(const char *script)
{
    return true; // MORE
}

} // namespace
